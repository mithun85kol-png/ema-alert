"""
NOTE: verify Upstox's intraday endpoint/interval support against current
docs before relying on this — API versions change. This pulls 1-minute
candles and resamples to 5-minute locally.

NOTE: the daily-candle endpoint used for Camarilla R3/S3 pivots is built
by analogy with the intraday endpoint below — verify the exact path
against current Upstox docs if pivot values look wrong.
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
from strategy import check_signal
from telegram_notifier import send_alert
from indicators import calculate_r3_s3

UPSTOX_INTRADAY_URL = "https://api.upstox.com/v2/historical-candle/intraday/{instrument_key}/1minute"
UPSTOX_DAILY_URL = "https://api.upstox.com/v2/historical-candle/{instrument_key}/day/{to_date}/{from_date}"

PIVOT_CACHE_FILE = "pivot_cache.json"

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))

# Stock / index cash-market session (NSE).
STOCK_SESSION_START = dt.time(9, 15)
STOCK_SESSION_END = dt.time(15, 30)

# Commodity (MCX) session — runs later into the evening.
COMMODITY_SESSION_START = dt.time(9, 0)
COMMODITY_SESSION_END = dt.time(23, 30)


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


def resample_5min(df):
    df = df.set_index("timestamp")
    out = df.resample("5min").agg({
        "open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum",
    }).dropna().reset_index()
    return out


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

    # Upstox candles are [timestamp, open, high, low, close, volume, oi]
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


def build_pivot_levels(watchlist):
    """
    Returns {symbol: {"r3": ..., "s3": ...}} for every symbol in the
    watchlist. Daily OHLC is only fetched once per calendar day per
    symbol — results are cached in pivot_cache.json so we don't hit
    Upstox's daily-candle endpoint on every 5-minute run.
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


def run():
    if not config.UPSTOX_ACCESS_TOKEN:
        print("UPSTOX_ACCESS_TOKEN not set — aborting.")
        sys.exit(1)

    now_ist = _now_ist()
    if not (_in_stock_session(now_ist) or _in_commodity_session(now_ist)):
        print(f"Outside all trading sessions ({now_ist.strftime('%H:%M')} IST) — skipping.")
        return

    watchlist = build_watchlist(now_ist)
    print(f"Scanning {len(watchlist)} instruments...")

    pivots = build_pivot_levels(watchlist)

    saved_state = state.load_state()
    alerts_sent = 0

    # Phase 1: fetch + resample every instrument in parallel (bounded pool
    # so we don't hammer Upstox's API all at once).
    dfs = {}

    def _fetch_one(symbol, instrument_key):
        raw = fetch_1min_candles(instrument_key)
        if raw is None or len(raw) < 30:
            return symbol, None
        return symbol, resample_5min(raw)

    with ThreadPoolExecutor(max_workers=config.FETCH_WORKERS) as pool:
        futures = {
            pool.submit(_fetch_one, symbol, instrument_key): symbol
            for symbol, instrument_key in watchlist.items()
        }
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                sym, df5 = future.result()
                if df5 is not None:
                    dfs[sym] = df5
            except Exception as e:
                print(f"Error fetching {symbol}: {e}")

    # Phase 2: run the signal check per instrument.
    for symbol, df5 in dfs.items():
        try:
            levels = pivots.get(symbol)
            r3 = levels["r3"] if levels else None
            s3 = levels["s3"] if levels else None

            signal = check_signal(df5, symbol, r3=r3, s3=s3)
            if signal is None:
                continue

            if state.already_alerted(saved_state, symbol, signal["direction"], signal["candle_time"]):
                continue

            send_alert(signal)
            state.mark_alerted(saved_state, symbol, signal["direction"], signal["candle_time"])
            alerts_sent += 1

        except Exception as e:
            print(f"Error on {symbol}: {e}")

    state.save_state(saved_state)
    print(f"Done. {alerts_sent} alert(s) sent.")


if __name__ == "__main__":
    run()
