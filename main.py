"""
NOTE: verify Upstox's intraday endpoint/interval support against current
docs before relying on this — API versions change. This pulls 1-minute
candles and resamples to 3-minute locally.

NOTE: the daily-candle endpoint used for Camarilla R3/S3 pivots is built
by analogy with the intraday endpoint below — verify the exact path
against current Upstox docs if pivot values look wrong.

This script runs two scans back-to-back in the SAME 3-minute workflow
run (no separate cron job / workflow):
  Step 1: the existing ~50 F&O stock/index/commodity scan (unchanged).
  Step 2: a new Nifty 500 (all ~500 stocks, cash/equity) scan, using the
          exact same EMA9/20 + RSI + volume + trend + strong-candle
          strategy, fetched in small batches to avoid Upstox rate-limits,
          with its own separate state file so it can never collide with
          the existing scan's alert de-dup state.

75-min informative trend (added): every fetched instrument's raw 1-min
data is ALSO resampled to 75-min locally (no extra API call) and passed
through strategy.get_75min_trend_info(), which is filter-free and never
blocks a signal from firing. The result is attached to each signal dict
as signal["trend_75min"] before send_alert() so it shows up as a
purely-informational block in the Telegram message.
"""

import sys
import json
import time
import datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

import config
import instruments
import state
from strategy import check_signals, debug_ema_gap, get_75min_trend_info
from telegram_notifier import send_alert
from indicators import calculate_r3_s3

UPSTOX_INTRADAY_URL = "https://api.upstox.com/v2/historical-candle/intraday/{instrument_key}/1minute"
UPSTOX_DAILY_URL = "https://api.upstox.com/v2/historical-candle/{instrument_key}/day/{to_date}/{from_date}"

# NOTE: verify these two paths/params against current Upstox docs before
# relying on them — used only to compute PCR (Put-Call Ratio) for
# indices, an informational-only field, so a failure here never blocks
# an alert (see fetch_pcr's try/except).
UPSTOX_OPTION_CONTRACTS_URL = "https://api.upstox.com/v2/option/contract"
UPSTOX_OPTION_CHAIN_URL = "https://api.upstox.com/v2/option/chain"

PIVOT_CACHE_FILE = "pivot_cache.json"
PCR_EXPIRY_CACHE_FILE = "pcr_expiry_cache.json"

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))

# Stock / index cash-market session (NSE).
STOCK_SESSION_START = dt.time(9, 15)
STOCK_SESSION_END = dt.time(15, 30)

# Commodity (MCX) session — matches the stock session window.
COMMODITY_SESSION_START = dt.time(9, 15)
COMMODITY_SESSION_END = dt.time(15, 30)


def _now_ist():
    return dt.datetime.now(IST)


def _in_stock_session(now_ist):
    return STOCK_SESSION_START <= now_ist.time() <= STOCK_SESSION_END


def _in_commodity_session(now_ist):
    return COMMODITY_SESSION_START <= now_ist.time() <= COMMODITY_SESSION_END


def fetch_1min_candles(instrument_key):
    headers = {"Authorization": f"Bearer {config.UPSTOX_ACCESS_TOKEN}"}
    url = UPSTOX_INTRADAY_URL.format(instrument_key=instrument_key)
    resp = requests.get(url, headers=headers, timeout=20)
    resp.raise_for_status()
    payload = resp.json()
    candles = payload.get("data", {}).get("candles", [])
    if not candles:
        return None

    df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df


def resample_3min(df):
    df = df.set_index("timestamp")
    out = df.resample("3min").agg({
        "open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum",
    }).dropna().reset_index()
    return out


def resample_75min(df):
    """
    Same resampling as resample_3min, just on a 75-minute bucket.
    Used only for the informative 75-min trend check — never affects
    the 3-min signal-firing logic itself.
    """
    df = df.set_index("timestamp")
    out = df.resample("75min").agg({
        "open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum",
    }).dropna().reset_index()
    return out


def drop_unclosed_candle(df, now_ist, candle_minutes=3):
    """
    The last row of a resample can be a still-forming candle if the
    current 1-min data doesn't yet cover the full bucket (e.g. it's
    14:01 and we only have 14:00-14:01 of data resampled into a "14:00"
    candle). Using that incomplete candle as the latest candle produces
    EMA values that don't match the final, fully-closed EMA — which is
    exactly the kind of mismatch between the alert and the chart.

    A candle starting at `ts` with bucket size `candle_minutes` is only
    closed once now_ist >= ts + candle_minutes. Drop the last row if it
    isn't closed yet. candle_minutes defaults to 3 (3-min scan); pass 75
    when checking the 75-min resample.
    """
    if df.empty:
        return df

    last_ts = df.iloc[-1]["timestamp"]
    if last_ts.tzinfo is None:
        last_ts = last_ts.tz_localize(IST)

    candle_close_time = last_ts + dt.timedelta(minutes=candle_minutes)
    if now_ist < candle_close_time:
        df = df.iloc[:-1].reset_index(drop=True)

    return df


def fetch_prev_day_ohlc(instrument_key):
    """
    Fetches the most recent completed trading day's High, Low, Close for
    an instrument (used to compute Camarilla R3/S3). Looks back up to 10
    calendar days so weekends/holidays don't cause a miss.
    """
    headers = {"Authorization": f"Bearer {config.UPSTOX_ACCESS_TOKEN}"}
    today = _now_ist().date()
    from_date = today - dt.timedelta(days=10)
    to_date = today - dt.timedelta(days=1)

    url = UPSTOX_DAILY_URL.format(
        instrument_key=instrument_key,
        to_date=to_date.isoformat(),
        from_date=from_date.isoformat(),
    )
    resp = requests.get(url, headers=headers, timeout=20)
    resp.raise_for_status()
    payload = resp.json()
    candles = payload.get("data", {}).get("candles", [])
    if not candles:
        return None

    candles_sorted = sorted(candles, key=lambda c: c[0])
    last = candles_sorted[-1]
    return {"high": last[2], "low": last[3], "close": last[4]}


def load_pivot_cache():
    try:
        with open(PIVOT_CACHE_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"date": None, "pivots": {}}


def save_pivot_cache(cache):
    with open(PIVOT_CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)


def _load_pcr_expiry_cache():
    try:
        with open(PCR_EXPIRY_CACHE_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"date": None, "expiries": {}}


def _save_pcr_expiry_cache(cache):
    with open(PCR_EXPIRY_CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)


def get_nearest_option_expiry(instrument_key):
    """
    Returns the nearest (>= today) expiry date string for the given
    index's option chain, using Upstox's option-contracts endpoint.
    Cached once per calendar day, since expiries don't change intraday.
    Returns None if the lookup fails for any reason.
    """
    cache = _load_pcr_expiry_cache()
    today_str = _now_ist().date().isoformat()
    if cache.get("date") != today_str:
        cache = {"date": today_str, "expiries": {}}

    if instrument_key in cache["expiries"]:
        return cache["expiries"][instrument_key]

    headers = {"Authorization": f"Bearer {config.UPSTOX_ACCESS_TOKEN}"}
    resp = requests.get(
        UPSTOX_OPTION_CONTRACTS_URL,
        headers=headers,
        params={"instrument_key": instrument_key},
        timeout=20,
    )
    resp.raise_for_status()
    payload = resp.json()
    contracts = payload.get("data", [])
    if not contracts:
        return None

    today = dt.date.today()
    expiries = {c.get("expiry") for c in contracts if c.get("expiry")}
    upcoming = sorted(e for e in expiries if dt.date.fromisoformat(e) >= today)
    if not upcoming:
        return None

    nearest = upcoming[0]
    cache["expiries"][instrument_key] = nearest
    _save_pcr_expiry_cache(cache)
    return nearest


def fetch_pcr(instrument_key):
    """
    Computes Put-Call Ratio (total Put OI / total Call OI) for an
    index's nearest-expiry option chain. Informational only — returns
    None (never raises) if anything about the lookup fails, so a PCR
    fetch problem can never block or delay an alert from being sent.
    """
    try:
        expiry = get_nearest_option_expiry(instrument_key)
        if not expiry:
            return None

        headers = {"Authorization": f"Bearer {config.UPSTOX_ACCESS_TOKEN}"}
        resp = requests.get(
            UPSTOX_OPTION_CHAIN_URL,
            headers=headers,
            params={"instrument_key": instrument_key, "expiry_date": expiry},
            timeout=20,
        )
        resp.raise_for_status()
        payload = resp.json()
        chain = payload.get("data", [])
        if not chain:
            return None

        total_call_oi = 0
        total_put_oi = 0
        for row in chain:
            call_md = (row.get("call_options") or {}).get("market_data") or {}
            put_md = (row.get("put_options") or {}).get("market_data") or {}
            total_call_oi += call_md.get("oi", 0) or 0
            total_put_oi += put_md.get("oi", 0) or 0

        if total_call_oi <= 0:
            return None

        return round(total_put_oi / total_call_oi, 2)
    except Exception as e:
        print(f"PCR fetch failed for {instrument_key}: {e}")
        return None


def build_pivot_levels(watchlist):
    """
    Returns {symbol: {"r3": ..., "s3": ...}} for every symbol in the
    watchlist. Daily OHLC is only fetched once per calendar day per
    symbol — results are cached in pivot_cache.json.
    """
    cache = load_pivot_cache()
    today_str = _now_ist().date().isoformat()

    if cache.get("date") != today_str:
        cache = {"date": today_str, "pivots": {}}

    missing = {sym: key for sym, key in watchlist.items() if sym not in cache["pivots"]}

    if missing:
        print(f"Fetching daily OHLC for {len(missing)} instrument(s) to build pivot levels...")

        def _fetch_pivot(symbol, instrument_key):
            ohlc = fetch_prev_day_ohlc(instrument_key)
            if ohlc is None:
                return symbol, None
            r3, s3 = calculate_r3_s3(ohlc["high"], ohlc["low"], ohlc["close"])
            return symbol, {"r3": r3, "s3": s3}

        with ThreadPoolExecutor(max_workers=config.FETCH_WORKERS) as pool:
            futures = {
                pool.submit(_fetch_pivot, symbol, instrument_key): symbol
                for symbol, instrument_key in missing.items()
            }
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    sym, levels = future.result()
                    if levels is not None:
                        cache["pivots"][sym] = levels
                except Exception as e:
                    print(f"Error fetching pivot for {symbol}: {e}")

        save_pivot_cache(cache)

    return cache["pivots"]


def build_watchlist(now_ist=None):
    now_ist = now_ist or _now_ist()
    watchlist = {}

    if _in_stock_session(now_ist):
        watchlist.update(instruments.resolve_indices(config.INDICES))
        fo_watch = None if config.USE_FULL_FO_LIST else config.FO_STOCK_WATCHLIST
        watchlist.update(instruments.resolve_fo_stock_list(fo_watch))

    if _in_commodity_session(now_ist):
        watchlist.update(instruments.resolve_mcx_nearest_futures(config.COMMODITIES))

    return watchlist


def _fetch_and_resample_one(symbol, instrument_key, now_ist):
    """
    Returns (symbol, df3, df75) — df3 drives the actual 3-min signal
    logic (unchanged); df75 is the same raw 1-min data resampled to
    75-min, used only for the informative trend check. Both come from a
    single API call, so this adds no extra Upstox requests.
    """
    raw = fetch_1min_candles(instrument_key)
    if raw is None or len(raw) < 30:
        return symbol, None, None
    df3 = resample_3min(raw)
    df3 = drop_unclosed_candle(df3, now_ist, candle_minutes=3)
    df75 = resample_75min(raw)
    df75 = drop_unclosed_candle(df75, now_ist, candle_minutes=75)
    return symbol, df3, df75


def fetch_all(watchlist, now_ist, workers):
    """
    Fetches + resamples 1-min candles -> 3-min (and 75-min) for every
    instrument in watchlist, concurrently (up to `workers` threads).
    Returns {symbol: (df3, df75)}, skipping any instrument whose fetch
    failed or that doesn't have enough history yet.
    """
    dfs = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_fetch_and_resample_one, symbol, instrument_key, now_ist): symbol
            for symbol, instrument_key in watchlist.items()
        }
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                sym, df3, df75 = future.result()
                if df3 is not None:
                    dfs[sym] = (df3, df75)
            except Exception as e:
                print(f"Error fetching {symbol}: {e}")
    return dfs


def _chunked(items_dict, size):
    items = list(items_dict.items())
    for i in range(0, len(items), size):
        yield dict(items[i:i + size])


def fetch_all_batched(watchlist, now_ist, batch_size, workers, delay_seconds):
    """
    Same as fetch_all, but splits watchlist into sequential batches of
    `batch_size` with a `delay_seconds` pause between batches, so a
    500-instrument scan doesn't fire hundreds of concurrent requests at
    Upstox in one burst and trip rate-limiting (429s).
    """
    dfs = {}
    batches = list(_chunked(watchlist, batch_size))
    total = len(batches)
    for i, batch in enumerate(batches, start=1):
        print(f"  [Nifty500] batch {i}/{total} ({len(batch)} instruments)...", flush=True)
        dfs.update(fetch_all(batch, now_ist, workers))
        if i < total:
            time.sleep(delay_seconds)
    return dfs


# ---------------------------------------------------------------------
# Step 1: existing ~50 F&O stock/index/commodity scan (unchanged logic)
# ---------------------------------------------------------------------

def run_fo_scan(now_ist):
    watchlist = build_watchlist(now_ist)
    print(f"Scanning {len(watchlist)} instruments...")

    pivots = build_pivot_levels(watchlist)

    saved_state = state.load_state()
    alerts_sent = 0

    # Used to set the "F&O: Yes/No" flag on stock signals only — indices
    # (NIFTY 50, SENSEX...) and commodities (GOLD, SILVER...) aren't
    # stocks, so they never get this flag (see non_stock_symbols below).
    fno_underlyings = instruments.get_fno_underlyings()
    non_stock_symbols = set(config.INDICES.keys()) | set(config.COMMODITIES.keys())
    index_symbols = set(config.INDICES.keys())
    pcr_cache_this_run = {}

    dfs = fetch_all(watchlist, now_ist, config.FETCH_WORKERS)

    for symbol, (df3, df75) in dfs.items():
        try:
            levels = pivots.get(symbol)
            r3 = levels["r3"] if levels else None
            s3 = levels["s3"] if levels else None

            require_trend = symbol not in index_symbols
            signals = check_signals(df3, symbol, r3=r3, s3=s3, require_trend_confirmation=require_trend)
            for signal in signals:
                if state.already_alerted(saved_state, symbol, signal["direction"], signal["candle_time"]):
                    continue

                if symbol not in non_stock_symbols:
                    signal["is_fno"] = symbol.upper() in fno_underlyings

                if symbol in index_symbols:
                    # PCR is informational only — fetch_pcr() never
                    # raises, so a failed/blocked fetch just means no
                    # PCR line on this alert, nothing more.
                    if symbol not in pcr_cache_this_run:
                        pcr_cache_this_run[symbol] = fetch_pcr(watchlist[symbol])
                    signal["pcr"] = pcr_cache_this_run[symbol]

                # 75-min informative trend — never blocks the alert;
                # get_75min_trend_info returns None if there isn't
                # enough 75-min history yet, in which case no 75-min
                # block is shown on the message.
                if df75 is not None:
                    signal["trend_75min"] = get_75min_trend_info(df75, symbol)

                send_alert(signal)
                state.mark_alerted(saved_state, symbol, signal["direction"], signal["candle_time"])
                alerts_sent += 1

        except Exception as e:
            print(f"Error on {symbol}: {e}")

    # Debug visibility: show the instruments whose EMA9/EMA20 are
    # currently closest together, even though none of them crossed this
    # run. Helps confirm the scanner is working when 0 alerts fire.
    gaps = []
    for symbol, (df3, df75) in dfs.items():
        try:
            g = debug_ema_gap(df3, symbol)
            if g is not None:
                gaps.append(g)
        except Exception:
            pass

    if gaps:
        gaps.sort(key=lambda g: g["gap_pct"])
        print("Closest to an EMA9/EMA20 cross this run (top 5):")
        for g in gaps[:5]:
            print(
                f"  {g['symbol']}: EMA9={g['ema_fast']} EMA20={g['ema_slow']} "
                f"gap={g['gap_pct']}%  {g['leaning']}"
            )

    state.save_state(saved_state)
    print(f"Done. {alerts_sent} alert(s) sent.")
    return alerts_sent


# ---------------------------------------------------------------------
# Step 2: new Nifty 500 (all ~500 stocks, cash/equity) scan
# ---------------------------------------------------------------------

def run_nifty500_scan(now_ist):
    if not _in_stock_session(now_ist):
        # Nifty 500 is a cash/equity scan only — no commodity leg here.
        return 0

    watchlist = instruments.resolve_nifty500_list()
    if not watchlist:
        print("[Nifty500] Watchlist empty (fetch failed and no cache) — skipping this run.")
        return 0

    print(f"[Nifty500] Scanning {len(watchlist)} instruments in batches of {config.NIFTY500_BATCH_SIZE}...")

    fno_underlyings = instruments.get_fno_underlyings()

    pivots = {}
    if config.NIFTY500_INCLUDE_PIVOTS:
        pivots = build_pivot_levels(watchlist)

    dfs = fetch_all_batched(
        watchlist,
        now_ist,
        batch_size=config.NIFTY500_BATCH_SIZE,
        workers=config.NIFTY500_FETCH_WORKERS,
        delay_seconds=config.NIFTY500_BATCH_DELAY_SECONDS,
    )

    # Separate state file — never touches/overwrites the F&O scan's
    # alert_state.json.
    saved_state = state.load_state(config.NIFTY500_STATE_FILE)
    alerts_sent = 0

    for symbol, (df3, df75) in dfs.items():
        try:
            levels = pivots.get(symbol)
            r3 = levels["r3"] if levels else None
            s3 = levels["s3"] if levels else None

            signals = check_signals(df3, symbol, r3=r3, s3=s3)
            for signal in signals:
                if state.already_alerted(saved_state, symbol, signal["direction"], signal["candle_time"]):
                    continue

                # F&O flag: tells the Telegram message whether this
                # Nifty 500 stock also trades in F&O, or is cash-only.
                signal["is_fno"] = symbol.upper() in fno_underlyings

                # 75-min informative trend — same as Step 1, never
                # blocks the alert.
                if df75 is not None:
                    signal["trend_75min"] = get_75min_trend_info(df75, symbol)

                send_alert(signal)
                state.mark_alerted(saved_state, symbol, signal["direction"], signal["candle_time"])
                alerts_sent += 1

        except Exception as e:
            print(f"[Nifty500] Error on {symbol}: {e}")

    state.save_state(saved_state, config.NIFTY500_STATE_FILE)
    print(f"[Nifty500] Done. {alerts_sent} alert(s) sent.")
    return alerts_sent


def run():
    if not config.UPSTOX_ACCESS_TOKEN:
        print("UPSTOX_ACCESS_TOKEN not set — aborting.")
        sys.exit(1)

    now_ist = _now_ist()
    if not (_in_stock_session(now_ist) or _in_commodity_session(now_ist)):
        print(f"Outside all trading sessions ({now_ist.strftime('%H:%M')} IST) — skipping.")
        return

    # ---- Step 1: existing ~50 F&O stock/index/commodity scan ----
    run_fo_scan(now_ist)

    # ---- Step 2: Nifty 500 cash/equity scan (new) ----
    run_nifty500_scan(now_ist)


if __name__ == "__main__":
    run()
