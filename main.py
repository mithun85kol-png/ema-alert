"""
NOTE: verify Upstox's intraday endpoint/interval support against current
docs before relying on this — API versions change. This pulls 1-minute
candles and resamples to 3-minute locally.

NOTE: the daily-candle endpoint used for Camarilla R3/S3 pivots is built
by analogy with the intraday endpoint below — verify the exact path
against current Upstox docs if pivot values look wrong.

This script scans the ~50 F&O stock/index/commodity watchlist on every
workflow run (the workflow itself can still run as often as every few
minutes — see "when alerts actually fire" below).

75-min PRIMARY/ALERTING signal (FLIPPED from the previous 3-min-primary
design): the main EMA9/EMA20 crossover check (strategy.check_signals —
EMA50 trend agreement mandatory for stocks/commodities, informational
only for indices; volume is informational only, never gating) now runs
on df75 — 1-min data (cached multi-day history + today's live data)
resampled to 75-min candles. This is the timeframe that actually
decides whether/when a Telegram alert is sent. The old design made
75-min a MANDATORY gate on top of a 3-min primary cross, which meant an
alert only fired once a 3-min cross AND a fresh 75-min cross both
lined up — causing alerts to arrive well after the 75-min cross had
actually happened. That gate is gone: a 75-min cross (+ trend
agreement) now fires the alert directly, as soon as that 75-min candle
closes.

df3 (3-min) is still built for every instrument; strategy.get_3min_trend_info(df3, symbol) is
computed for every qualifying 75-min signal and attached as
signal["trend_3min"], showing:
  - whether EMA9/20 has crossed on the 3-min chart recently (and how
    many 3-min candles ago), or hasn't crossed within the lookback
    window at all
  - how close EMA9/20 currently are to crossing on the 3-min chart
    (gap_pct — smaller = closer to a cross)
Purely informational — it never gates or blocks the 75-min alert.

COMMODITIES (GOLD/SILVER/CRUDEOIL etc.): follow the exact same rule as
stocks — the 75-min loop is their only alert (EMA9/20 cross + mandatory
EMA50 trend agreement; volume is informational only), with 3-min shown
as context on the alert. There is no separate standalone commodity-only
alert (a 15-min standalone version used to exist here; it remains
removed). df15 is still resampled per-instrument but is no longer read
anywhere.

WHEN ALERTS ACTUALLY FIRE (75-min primary signal):
- A 75-min candle closes 5 times a day during the trading session
  (09:15, 10:30, 11:45, 13:00, 14:15 IST — the 15:30 close is a short
  partial bar). drop_unclosed_candle(..., candle_minutes=75) makes
  sure a still-forming 75-min bar is never evaluated, so a signal can
  only appear right after a 75-min candle closes.
- The workflow still runs every 1-3 minutes; a run simply finds "no new
  closed 75-min candle" and sends nothing until one actually closes —
  so alerts are inherently much less frequent than under the old
  3-min-primary design, by design.
- check_signals() re-checks the last config.PRIMARY_LOOKBACK_CANDLES
  (2) closed 75-min candles (not just the newest), so a delayed/skipped
  run still catches a cross that closed while nothing was running.
- Every qualifying cross (EMA9/20 cross + EMA50 trend agreement for
  stocks/commodities, informational-only for indices; volume shown but
  informational only) fires — no further gate on top.

Sector index trend (added): every stock alert with a config.
STOCK_SECTOR_MAP entry gets a "Sector: ..." line showing whether its
sector index (e.g. NIFTY BANK for HDFCBANK/ICICIBANK, NIFTY IT for
TCS/INFY) is currently in an UPTREND or DOWNTREND (still computed on
3-min sector data, for near-real-time context), and whether that
agrees with the stock's own EMA50 trend. Computed once per run via
fetch_sector_trends() (below) — informational only, never blocks a
stock's alert, and stocks with no sector mapping simply don't get this
line.

Retry/backoff (added): every Upstox HTTP call goes through
_request_with_retry(), which retries on 429 (rate-limit) and transient
5xx errors a few times with a short increasing wait (config.py:
UPSTOX_MAX_RETRIES / UPSTOX_RETRY_BACKOFF_BASE_SECONDS) before giving
up on that one instrument. This means a brief rate-limit no longer
silently drops a symbol from the scan.

Fetch-failure summary (added): symbols that still fail after retries
are counted and, if the total meets config.FAILURE_ALERT_MIN_COUNT, a
single short Telegram message is sent at the end of the run summarizing
how many symbols were skipped and why — so a persistent Upstox problem
is visible instead of only living in the GitHub Actions log.
"""

import sys
import json
import time
import random
import threading
import datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

import config
import instruments
import state
import delivery_data
from strategy import check_signals, debug_ema_gap, get_3min_trend_info, get_sector_trend
from telegram_notifier import send_alert
from indicators import calculate_r3_s3

UPSTOX_INTRADAY_URL = "https://api.upstox.com/v2/historical-candle/intraday/{instrument_key}/1minute"
UPSTOX_DAILY_URL = "https://api.upstox.com/v2/historical-candle/{instrument_key}/day/{to_date}/{from_date}"

# NOTE: verify against current Upstox docs — same historical-candle
# family as UPSTOX_DAILY_URL above, just with a 1-minute interval.
# Used only to warm up EMA9/EMA20 on the 75-min timeframe (see
# build_hist1min_cache below); a failure here degrades gracefully to
# "not enough 75-min history yet" rather than blocking the 3-min scan.
UPSTOX_HISTORICAL_1MIN_URL = "https://api.upstox.com/v2/historical-candle/{instrument_key}/1minute/{to_date}/{from_date}"

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

# Commodity (MCX) session — extended to match MCX's evening session so
# the standalone 15-min EMA cross alert (below) keeps firing up to
# 11 PM IST, not just the cash-market window.
COMMODITY_SESSION_START = dt.time(9, 15)
COMMODITY_SESSION_END = dt.time(23, 0)


def _now_ist():
    return dt.datetime.now(IST)


def _in_stock_session(now_ist):
    return STOCK_SESSION_START <= now_ist.time() <= STOCK_SESSION_END


def _in_commodity_session(now_ist):
    return COMMODITY_SESSION_START <= now_ist.time() <= COMMODITY_SESSION_END


# ---------------------------------------------------------------------
# Retry/backoff wrapper for Upstox HTTP calls
# ---------------------------------------------------------------------

def _request_with_retry(method, url, **kwargs):
    """
    Thin wrapper around requests.get/... that retries on 429
    (rate-limited) and transient 5xx responses, with a short increasing
    wait between attempts. Raises the final error (via raise_for_status)
    if every attempt fails, exactly like a plain requests call would —
    callers don't need to change their exception handling.

    Total attempts = 1 + config.UPSTOX_MAX_RETRIES.
    Wait before attempt N (N>=2) = config.UPSTOX_RETRY_BACKOFF_BASE_SECONDS * (N-1),
    plus up to 50% random jitter on top. The jitter matters when many
    worker threads hit the same endpoint concurrently and all get
    429'd together (thundering herd) — without jitter every thread
    retries at the exact same instant and collides again; with it,
    retries spread out over time instead of re-syncing.
    """
    last_exc = None
    attempts = 1 + config.UPSTOX_MAX_RETRIES

    for attempt in range(1, attempts + 1):
        try:
            resp = method(url, **kwargs)
        except requests.exceptions.RequestException as e:
            # Network-level failure (timeout, connection error, etc.) —
            # also worth retrying, same backoff schedule.
            last_exc = e
            if attempt < attempts:
                wait = config.UPSTOX_RETRY_BACKOFF_BASE_SECONDS * attempt
                wait += random.uniform(0, wait * 0.5)
                time.sleep(wait)
                continue
            raise

        if resp.status_code in config.UPSTOX_RETRY_STATUS_CODES and attempt < attempts:
            wait = config.UPSTOX_RETRY_BACKOFF_BASE_SECONDS * attempt
            wait += random.uniform(0, wait * 0.5)
            time.sleep(wait)
            continue

        # Either success, or a non-retryable error, or we're out of
        # retries — let the caller's raise_for_status() surface it.
        return resp

    # Should be unreachable, but just in case.
    if last_exc:
        raise last_exc


def _get_with_retry(url, **kwargs):
    return _request_with_retry(requests.get, url, **kwargs)


def fetch_1min_candles(instrument_key):
    headers = {"Authorization": f"Bearer {config.UPSTOX_ACCESS_TOKEN}"}
    url = UPSTOX_INTRADAY_URL.format(instrument_key=instrument_key)
    resp = _get_with_retry(url, headers=headers, timeout=20)
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


def resample_15min(df):
    """
    Resamples 1-minute candles to 15-minute bars for the standalone
    commodity EMA9/EMA20 cross alert (config.COMMODITIES only — see
    run_fo_scan). No per-day origin alignment needed: the session start
    09:15 IST is already a clean multiple of 15 minutes (555 min from
    midnight / 15 = 37), same reasoning as resample_3min.
    """
    df = df.set_index("timestamp")
    out = df.resample("15min").agg({
        "open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum",
    }).dropna().reset_index()
    return out


def resample_75min(df):
    """
    Resamples 1-minute candles to 75-minute bars, aligning each trading
    day's bins independently to the session start (09:15 IST). This
    matters once historical (pre-today) data is combined with today's
    data (see build_hist1min_cache / _fetch_and_resample_one below) —
    without per-day alignment, bars could blend minutes from the end of
    one session with the start of the next, or drift out of sync with
    the visual 75-min chart.
    """
    if df.empty:
        return df

    df = df.copy()
    ts = pd.to_datetime(df["timestamp"])
    if ts.dt.tz is None:
        ts = ts.dt.tz_localize(IST)
    else:
        ts = ts.dt.tz_convert(IST)
    df["timestamp"] = ts
    df = df.set_index("timestamp")

    out_frames = []
    for date, day_df in df.groupby(df.index.date):
        day_start = dt.datetime.combine(date, STOCK_SESSION_START, tzinfo=IST)
        day_out = day_df.resample("75min", origin=day_start).agg({
            "open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum",
        }).dropna()
        out_frames.append(day_out)

    if not out_frames:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

    out = pd.concat(out_frames).reset_index().rename(columns={"index": "timestamp"})
    return out.sort_values("timestamp").reset_index(drop=True)


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
    resp = _get_with_retry(url, headers=headers, timeout=20)
    resp.raise_for_status()
    payload = resp.json()
    candles = payload.get("data", {}).get("candles", [])
    if not candles:
        return None

    candles_sorted = sorted(candles, key=lambda c: c[0])
    last = candles_sorted[-1]
    return {"high": last[2], "low": last[3], "close": last[4]}


class _RateLimiter:
    """
    Thread-safe rate limiter: makes callers (across any number of
    worker threads) block so that no more than `max_per_second` calls
    actually go out in any 1-second window. Unlike per-thread
    sleep/backoff, this caps the TRUE combined outbound rate no matter
    how many threads are calling it concurrently — which is what a
    server-side rate limit (Upstox's 429) actually cares about.
    """

    def __init__(self, max_per_second):
        self._lock = threading.Lock()
        self._min_interval = 1.0 / max_per_second
        self._next_slot = 0.0

    def wait(self):
        with self._lock:
            now = time.monotonic()
            start = max(now, self._next_slot)
            self._next_slot = start + self._min_interval
            sleep_for = start - now
        if sleep_for > 0:
            time.sleep(sleep_for)


# Caps outbound requests to Upstox's historical-candle endpoint (used
# only by fetch_historical_1min, for the once-daily 75-min-warmup
# fetch) to config.HIST_FETCH_MAX_PER_SECOND combined across every
# worker thread — see build_hist1min_cache, which also uses a smaller
# dedicated worker pool (config.HIST_FETCH_WORKERS) for this same
# reason. This endpoint was getting mass 429'd when all
# config.FETCH_WORKERS (30) threads hit it at once for ~214
# instruments.
_hist_fetch_limiter = _RateLimiter(config.HIST_FETCH_MAX_PER_SECOND)


def fetch_historical_1min(instrument_key, days_back):
    """
    Fetches PRE-TODAY 1-minute candles for the last `days_back` calendar
    days via Upstox's historical-candle endpoint. Used only to warm up
    EMA9/EMA20 on the 75-min timeframe (a single trading day alone only
    yields ~5 bars — nowhere near enough). Returns the raw candle list
    (JSON-serializable) or None on failure.

    Rate-limited via _hist_fetch_limiter (see below) — this endpoint
    has a tighter Upstox rate limit than the intraday one, and with
    config.FETCH_WORKERS (30) threads all calling it at once for the
    ~214-instrument watchlist, it was returning mass 429s even with
    per-call retries (all 30 threads retry in lockstep and collide
    again). The limiter caps the actual outbound request rate across
    ALL threads combined, so retries aren't needed nearly as often.
    """
    _hist_fetch_limiter.wait()
    headers = {"Authorization": f"Bearer {config.UPSTOX_ACCESS_TOKEN}"}
    today = _now_ist().date()
    to_date = today - dt.timedelta(days=1)
    from_date = to_date - dt.timedelta(days=days_back)

    url = UPSTOX_HISTORICAL_1MIN_URL.format(
        instrument_key=instrument_key,
        to_date=to_date.isoformat(),
        from_date=from_date.isoformat(),
    )
    resp = _get_with_retry(url, headers=headers, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    return payload.get("data", {}).get("candles", [])


def load_hist1min_cache():
    try:
        with open(config.HIST_1MIN_CACHE_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"date": None, "data": {}}


def save_hist1min_cache(cache):
    with open(config.HIST_1MIN_CACHE_FILE, "w") as f:
        json.dump(cache, f)


def build_hist1min_cache(watchlist):
    """
    Ensures every symbol in watchlist has PRE-TODAY 1-min history cached
    for 75-min EMA warmup — 75-min is now the PRIMARY/ALERTING
    timeframe, so this cache is what makes the 75-min signal actually
    usable (without it, a single trading day alone only yields ~5 bars,
    nowhere near enough to warm up EMA9/EMA20/EMA50). Fetched once per
    calendar day per symbol (not on every 3-min run) — cache is keyed
    by date and reset when the date rolls over, same pattern as
    build_pivot_levels(). A symbol that fails to fetch simply has no
    cached history that day, which degrades gracefully: its 75-min bars
    just won't have enough bars to warm up yet (see
    strategy.check_signals — it returns [] until there's enough
    history), never a crash.
    """
    cache = load_hist1min_cache()
    today_str = _now_ist().date().isoformat()

    if cache.get("date") != today_str:
        cache = {"date": today_str, "data": {}}

    missing = {sym: key for sym, key in watchlist.items() if sym not in cache["data"]}

    if missing:
        print(f"Fetching {config.HISTORICAL_1MIN_LOOKBACK_DAYS}-day 1-min history for {len(missing)} instrument(s) (75-min warmup)...")

        def _fetch_one_hist(symbol, instrument_key):
            try:
                candles = fetch_historical_1min(instrument_key, config.HISTORICAL_1MIN_LOOKBACK_DAYS)
                return symbol, candles
            except Exception as e:
                print(f"Historical 1-min fetch failed for {symbol}: {e}")
                return symbol, None

        with ThreadPoolExecutor(max_workers=config.HIST_FETCH_WORKERS) as pool:
            futures = {
                pool.submit(_fetch_one_hist, symbol, instrument_key): symbol
                for symbol, instrument_key in missing.items()
            }
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    sym, candles = future.result()
                    if candles:
                        cache["data"][sym] = candles
                except Exception as e:
                    print(f"Error caching history for {symbol}: {e}")

        save_hist1min_cache(cache)

    return cache["data"]


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
    resp = _get_with_retry(
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
        resp = _get_with_retry(
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
            return symbol, {"r3": r3, "s3": s3, "prev_close": ohlc["close"]}

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


def _fetch_and_resample_one(symbol, instrument_key, now_ist, hist_candles=None):
    """
    Returns (symbol, df3, df75, df15) — df3 drives the actual 3-min
    signal logic (unchanged); df75 is used for the informative 75-min
    trend check attached to 3-min alerts; df15 is used for the
    standalone 15-min commodity EMA cross alert.

    df75 combines cached PRE-TODAY 1-min history (hist_candles, from
    build_hist1min_cache — enough days to warm up EMA9/EMA20) with
    today's fresh 1-min data, deduped and resampled together. Without
    hist_candles, df75 falls back to today-only data (same as before) —
    which will almost never have enough bars to warm up EMA9/EMA20, so
    the 75-min features simply stay dormant for that symbol until
    history is available, never a crash.

    Raises on a genuine fetch failure (after retries are exhausted in
    fetch_1min_candles) so the caller (fetch_all) can distinguish
    "failed to fetch" from "fetched fine, just not enough history yet".
    """
    raw = fetch_1min_candles(instrument_key)
    if raw is None or len(raw) < 30:
        return symbol, None, None, None
    df3 = resample_3min(raw)
    df3 = drop_unclosed_candle(df3, now_ist, candle_minutes=3)

    df15 = resample_15min(raw)
    df15 = drop_unclosed_candle(df15, now_ist, candle_minutes=15)

    if hist_candles:
        hist_df = pd.DataFrame(
            hist_candles,
            columns=["timestamp", "open", "high", "low", "close", "volume", "oi"],
        )
        hist_df["timestamp"] = pd.to_datetime(hist_df["timestamp"])
        combined_75 = pd.concat([hist_df, raw], ignore_index=True)
        combined_75 = combined_75.drop_duplicates(subset="timestamp").sort_values("timestamp").reset_index(drop=True)
    else:
        combined_75 = raw

    df75 = resample_75min(combined_75)
    df75 = drop_unclosed_candle(df75, now_ist, candle_minutes=75)
    return symbol, df3, df75, df15


def fetch_all(watchlist, now_ist, workers, hist_cache=None):
    """
    Fetches + resamples 1-min candles -> 3-min (and 75-min) for every
    instrument in watchlist, concurrently (up to `workers` threads).

    hist_cache: {symbol: [candle, ...]} of cached pre-today 1-min
    history (see build_hist1min_cache), passed through to each
    instrument's 75-min resample so EMA9/EMA20 there is properly warmed
    up. Optional — omitting it just means 75-min features stay dormant.

    Returns (dfs, failed_symbols):
      dfs            -- {symbol: (df3, df75, df15)} for every instrument
                         that fetched successfully (with enough history).
      failed_symbols -- list of symbols whose fetch raised an exception
                         even after the retry/backoff in
                         fetch_1min_candles was exhausted. These are the
                         ones genuinely skipped this run (as opposed to
                         a symbol that fetched fine but had too little
                         history, which is not counted as a failure).
    """
    hist_cache = hist_cache or {}
    dfs = {}
    failed_symbols = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                _fetch_and_resample_one, symbol, instrument_key, now_ist, hist_cache.get(symbol)
            ): symbol
            for symbol, instrument_key in watchlist.items()
        }
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                sym, df3, df75, df15 = future.result()
                if df3 is not None:
                    dfs[sym] = (df3, df75, df15)
            except Exception as e:
                print(f"Error fetching {symbol}: {e}")
                failed_symbols.append(symbol)
    return dfs, failed_symbols


# ---------------------------------------------------------------------
# Sector index trend (added) — informational context for stock alerts
# ---------------------------------------------------------------------

def fetch_sector_trends(now_ist):
    """
    Fetches today's 1-min data for every sector index in
    config.SECTOR_INDICES, resamples to 3-min, and reads each one's
    current EMA50 trend via strategy.get_sector_trend(). Sector indices
    never fire their own alert — this purely produces a lookup table
    that individual stock alerts attach themselves to (via
    config.STOCK_SECTOR_MAP), same pattern as build_pivot_levels().

    Returns {display_name: "UPTREND"/"DOWNTREND"/None}. None means
    either the fetch failed or the index doesn't have ~50 3-min bars
    yet today to warm up EMA50 — a stock whose sector maps to None
    simply gets no sector-trend line on its alert (see
    telegram_notifier.py), never an error.

    Resolving the sector index names to instrument_keys reuses
    instruments.py's in-process instrument-master cache, so this incurs
    no extra download after the first index/stock resolution earlier in
    the same run.
    """
    sector_watchlist = instruments.resolve_indices(config.SECTOR_INDICES)
    trends = {}
    if not sector_watchlist:
        return trends

    def _fetch_one_sector(display, instrument_key):
        try:
            raw = fetch_1min_candles(instrument_key)
            if raw is None or len(raw) < 30:
                return display, None
            df3 = resample_3min(raw)
            df3 = drop_unclosed_candle(df3, now_ist, candle_minutes=3)
            return display, get_sector_trend(df3)
        except Exception as e:
            print(f"Sector index fetch failed for {display}: {e}")
            return display, None

    with ThreadPoolExecutor(max_workers=config.FETCH_WORKERS) as pool:
        futures = {
            pool.submit(_fetch_one_sector, display, key): display
            for display, key in sector_watchlist.items()
        }
        for future in as_completed(futures):
            display, trend = future.result()
            trends[display] = trend

    return trends


# ---------------------------------------------------------------------
# Fetch-failure summary alert
# ---------------------------------------------------------------------

def send_telegram_text(text):
    """
    Sends a plain text message to Telegram directly (bypasses
    telegram_notifier.send_alert, which expects a full signal dict).
    Used only for the end-of-run fetch-failure summary. Never raises —
    a failed summary send should not affect the run's exit status.
    """
    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        print("Telegram not configured, skipping summary send:", text)
        return
    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={
            "chat_id": config.TELEGRAM_CHAT_ID,
            "text": text,
        }, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print("Telegram summary send failed:", e)


def maybe_send_failure_summary(fo_failed, fo_total):
    total_failed = len(fo_failed)
    if total_failed < config.FAILURE_ALERT_MIN_COUNT:
        return

    lines = [
        f"⚠️ Scan warning: {total_failed} instrument(s) failed to fetch this run "
        f"(after retries) and were skipped.",
        f"F&O/Index/Commodity scan: {len(fo_failed)}/{fo_total} failed",
        "They will be retried automatically on the next 3-min run.",
    ]

    send_telegram_text("\n".join(lines))


# ---------------------------------------------------------------------
# ~50 F&O stock/index/commodity scan
# ---------------------------------------------------------------------

def run_fo_scan(now_ist):
    watchlist = build_watchlist(now_ist)
    print(f"Scanning {len(watchlist)} instruments...")

    pivots = build_pivot_levels(watchlist)

    # Previous trading day's NSE delivery % per stock (see
    # delivery_data.py). Cached on disk, so this only actually
    # downloads the NSE bhavcopy once per calendar day, not once per
    # run. {} on failure — never blocks the scan, just means no
    # "Delivery %" line on alerts for the day.
    delivery_map = delivery_data.get_delivery_data(now_ist.date())

    saved_state = state.load_state()
    alerts_sent = 0

    # Used to set the "F&O: Yes/No" flag on stock signals only — indices
    # (NIFTY 50, SENSEX...) and commodities (GOLD, SILVER...) aren't
    # stocks, so they never get this flag (see non_stock_symbols below).
    fno_underlyings = instruments.get_fno_underlyings()
    non_stock_symbols = set(config.INDICES.keys()) | set(config.COMMODITIES.keys())
    index_symbols = set(config.INDICES.keys())
    pcr_cache_this_run = {}

    hist_cache = build_hist1min_cache(watchlist)

    # Run the main stock/index/commodity fetch and the sector-index
    # fetch AT THE SAME TIME (not one after another) — they're
    # completely independent of each other. This means the sector
    # feature adds ~zero extra wait before alerts go out: the sector
    # fetch is only ~14 lightweight index calls and finishes well
    # before the much bigger stock fetch does, so the overall run time
    # is still just however long the stock fetch alone takes. A sector
    # fetch failure/timeout is also fully isolated — wrapped in
    # try/except below — so it can never delay or block the main
    # stock alerts even in the worst case.
    with ThreadPoolExecutor(max_workers=2) as outer_pool:
        main_future = outer_pool.submit(fetch_all, watchlist, now_ist, config.FETCH_WORKERS, hist_cache)
        sector_future = outer_pool.submit(fetch_sector_trends, now_ist)
        dfs, failed_symbols = main_future.result()
        try:
            sector_trends = sector_future.result()
        except Exception as e:
            print(f"Sector trend fetch failed this run (non-blocking): {e}")
            sector_trends = {}

    for symbol, (df3, df75, df15) in dfs.items():
        try:
            levels = pivots.get(symbol)
            r3 = levels["r3"] if levels else None
            s3 = levels["s3"] if levels else None
            prev_close = levels.get("prev_close") if levels else None

            # PRIMARY/ALERTING signal — 75-min EMA9/20 cross + EMA50
            # trend agreement (mandatory for stocks/commodities,
            # informational-only for indices). Nothing to evaluate if
            # df75 isn't warmed up yet for this symbol (see
            # build_hist1min_cache) — degrades gracefully, never a
            # crash.
            if df75 is None:
                continue

            require_trend = symbol not in index_symbols
            signals = check_signals(
                df75, symbol, r3=r3, s3=s3,
                lookback=config.PRIMARY_LOOKBACK_CANDLES,
                require_trend_confirmation=require_trend,
                prev_close=prev_close,
            )

            if not signals:
                continue

            # 3-min context — informational only, attached to every
            # qualifying 75-min signal below. None if df3 isn't
            # available/warmed up; the alert still fires either way,
            # just without this block on the message.
            info3 = get_3min_trend_info(df3, symbol) if df3 is not None else None

            for signal in signals:
                if state.already_alerted(saved_state, symbol, signal["direction"], signal["candle_time"]):
                    continue

                if info3 is not None:
                    signal["trend_3min"] = info3

                if symbol not in non_stock_symbols:
                    signal["is_fno"] = symbol.upper() in fno_underlyings

                    # Delivery % — stocks only, previous trading day's
                    # NSE delivery percentage. Omitted if the symbol
                    # isn't in the bhavcopy (e.g. new listing) or the
                    # fetch failed for the day.
                    deliv = delivery_map.get(symbol.upper())
                    if deliv is not None:
                        signal["delivery_pct"] = deliv

                    # Sector index trend — stocks only (indices/
                    # commodities aren't in STOCK_SECTOR_MAP, so this is
                    # a no-op for them even if reached).
                    sector_name = config.STOCK_SECTOR_MAP.get(symbol.upper())
                    if sector_name:
                        signal["sector_index"] = sector_name
                        signal["sector_trend"] = sector_trends.get(sector_name)

                if symbol in index_symbols:
                    # PCR is informational only — fetch_pcr() never
                    # raises, so a failed/blocked fetch just means no
                    # PCR line on this alert, nothing more.
                    if symbol not in pcr_cache_this_run:
                        pcr_cache_this_run[symbol] = fetch_pcr(watchlist[symbol])
                    signal["pcr"] = pcr_cache_this_run[symbol]

                send_alert(signal)
                state.mark_alerted(saved_state, symbol, signal["direction"], signal["candle_time"])
                alerts_sent += 1

        except Exception as e:
            print(f"Error on {symbol}: {e}")

    # ---- 3-min: informational-only (no separate alert) ----
    # There is no standalone 3-min alert anymore. 3-min context
    # (EMA9/20 bias, candles since last 3-min cross, and how close to a
    # cross right now) is attached to every 75-min alert above via
    # signal["trend_3min"] (set from get_3min_trend_info(df3, ...)), so
    # you can see the shorter-term picture without a second, separate
    # ping.

    # ---- 15-min standalone commodity alert: REMOVED ----
    # Commodities (GOLD/SILVER/CRUDEOIL etc.) no longer get a separate
    # 15-min EMA cross alert. They now follow the exact same rule as
    # stocks: the 75-min loop above is their ONLY alert (EMA9/20 cross +
    # mandatory EMA50 trend agreement, volume informational only — see
    # strategy.check_signals / require_trend_confirmation, which is
    # True for commodities), with 3-min shown purely as informational
    # context via signal["trend_3min"], same as every other instrument.
    # df15 is still resampled per-instrument (see _fetch_and_resample_one)
    # but is no longer read anywhere in this function.

    # Debug visibility: show the instruments whose EMA9/EMA20 (on the
    # 75-min timeframe, same one the actual signal now uses) are
    # currently closest together, even though none of them crossed this
    # run. Helps confirm the scanner is working when 0 alerts fire.
    gaps = []
    for symbol, (df3, df75, df15) in dfs.items():
        try:
            g = debug_ema_gap(df75, symbol) if df75 is not None else None
            if g is not None:
                gaps.append(g)
        except Exception:
            pass

    if gaps:
        gaps.sort(key=lambda g: g["gap_pct"])
        print("Closest to an EMA9/EMA20 cross this run (top 5, 75-min):")
        for g in gaps[:5]:
            print(
                f"  {g['symbol']}: EMA9={g['ema_fast']} EMA20={g['ema_slow']} "
                f"gap={g['gap_pct']}%  {g['leaning']}"
            )

    state.save_state(saved_state)
    if failed_symbols:
        print(f"{len(failed_symbols)} instrument(s) failed to fetch this run: {failed_symbols}")
    print(f"Done. {alerts_sent} alert(s) sent.")
    return alerts_sent, failed_symbols, len(watchlist)


def run():
    if not config.UPSTOX_ACCESS_TOKEN:
        print("UPSTOX_ACCESS_TOKEN not set — aborting.")
        sys.exit(1)

    now_ist = _now_ist()
    if not (_in_stock_session(now_ist) or _in_commodity_session(now_ist)):
        print(f"Outside all trading sessions ({now_ist.strftime('%H:%M')} IST) — skipping.")
        return

    _, fo_failed, fo_total = run_fo_scan(now_ist)

    # ---- Fetch-failure visibility: one summary alert if enough
    # instruments failed to fetch this run ----
    maybe_send_failure_summary(fo_failed, fo_total)


if __name__ == "__main__":
    run()
