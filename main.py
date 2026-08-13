
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

75-min PRIMARY/ALERTING signal for STOCKS/COMMODITIES/CASH (FLIPPED
from the previous 3-min-primary design): the main EMA9/EMA50 crossover
check (strategy.check_signals — EMA9/50 cross; volume increase over
the previous candle is now MANDATORY, require_volume_increase=True)
now runs on df75 — 1-min data (cached multi-day history + today's live
data) resampled to 75-min candles. This is the timeframe that decides
whether/when a STOCK/COMMODITY Telegram alert is sent. The old design
made 75-min a MANDATORY gate on top of a 3-min primary cross, which
meant an alert only fired once a 3-min cross AND a fresh 75-min cross
both lined up — causing alerts to arrive well after the 75-min cross
had actually happened. That gate is gone: a 75-min cross (+ trend
agreement) now fires the alert directly, as soon as that 75-min candle
closes.

INDICES (NIFTY 50, NIFTY BANK, SENSEX): completely separate rule, not
tied to df75 at all, and checked by its OWN dedicated 5-min cron
trigger (SCAN_MODE=index -> run_fo_scan(index_only=True) — see run()),
fully independent of the 75-min F&O/Nifty500/commodity scan. An index
alert fires on a PURE EMA9/20 crossover on the 5-min chart (df5) — no
EMA50 trend requirement, no 75-min involvement.
strategy.check_signals(df5, ..., require_trend_confirmation=False) is
called directly on df5, scanning the trailing
config.INDEX_ALERT_LOOKBACK_CANDLES closed 5-min candles. The
resulting signal is tagged timeframe="5-min" (telegram_notifier uses
this to label the message correctly) and does NOT get a trend_3min
context block, since the alert itself already is the short-timeframe
signal.

df3 (3-min) is still built for every instrument. For STOCKS/COMMODITIES,
strategy.get_3min_trend_info(df3, symbol) is computed for every
qualifying 75-min signal and attached as signal["trend_3min"], showing:
  - whether EMA9/20 has crossed on the 3-min chart recently (and how
    many 3-min candles ago), or hasn't crossed within the lookback
    window at all
  - how close EMA9/20 currently are to crossing on the 3-min chart
    (gap_pct — smaller = closer to a cross)
Purely informational — it never gates or blocks the 75-min alert. (Not
attached for indices — see above.)

COMMODITIES (GOLD/SILVER/CRUDEOIL etc.): follow the exact same rule as
stocks — the 75-min loop is their only alert (EMA9/50 cross, mandatory
volume increase over the previous candle), with 3-min shown
as context on the alert. There is no separate standalone commodity-only
alert (a 15-min standalone version used to exist here; it remains
removed). df15 is still resampled per-instrument but is no longer read
anywhere.

WHEN ALERTS ACTUALLY FIRE:
- STOCKS/COMMODITIES (75-min): a 75-min candle closes 5 times a day
  during the trading session (09:15, 10:30, 11:45, 13:00, 14:15 IST —
  the 15:30 close is a short partial bar). drop_unclosed_candle(...,
  candle_minutes=75) makes sure a still-forming 75-min bar is never
  evaluated, so a signal can only appear right after a 75-min candle
  closes. check_signals() re-checks the last
  config.PRIMARY_LOOKBACK_CANDLES (2) closed 75-min candles (not just
  the newest), so a delayed/skipped run still catches a cross that
  closed while nothing was running.
- INDICES (5-min): a 5-min candle closes every 5 minutes during the
  session, checked by its own dedicated 5-min cron trigger
  (SCAN_MODE=index). check_signals() re-checks the last
  config.INDEX_ALERT_LOOKBACK_CANDLES closed 5-min candles, so a
  delayed/skipped run still catches up. No trend/gate condition beyond
  the plain EMA9/20 cross itself.
- The workflow still runs every 1-3 minutes; a run simply finds "no new
  closed candle (in the relevant timeframe)" and sends nothing until
  one actually closes.

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
import os
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
import bulk_block_data
import corporate_actions
from strategy import check_signals, debug_ema_gap, get_3min_trend_info, get_sector_trend, passes_confluence_filter
from telegram_notifier import send_alert
from indicators import calculate_r3_s3

UPSTOX_INTRADAY_URL = "https://api.upstox.com/v2/historical-candle/intraday/{instrument_key}/1minute"
UPSTOX_DAILY_URL = "https://api.upstox.com/v2/historical-candle/{instrument_key}/day/{to_date}/{from_date}"

# Upstox's v2 historical-candle endpoint is DEPRECATED, and its
# 1-minute interval was also hard-capped at "final month leading up
# to to_date" — a 35-day request (config.HISTORICAL_1MIN_LOOKBACK_DAYS)
# was silently outside that window, which is what caused the
# "400 Client Error: Bad Request" spam for every instrument. Moved to
# the v3 endpoint (unit/interval as separate path segments); v3's
# 1-15 minute interval range is capped at "1 month" too, so
# HISTORICAL_1MIN_LOOKBACK_DAYS was also reduced (see config.py) to
# stay safely inside that window.
UPSTOX_HISTORICAL_1MIN_URL = "https://api.upstox.com/v3/historical-candle/{instrument_key}/minutes/1/{to_date}/{from_date}"

# NOTE: verify these two paths/params against current Upstox docs before
# relying on them — used to compute PCR and Call/Put writing buildup
# for indices AND F&O stocks (informational-only fields), so a failure
# here never blocks an alert (see fetch_option_chain_snapshot's
# try/except).
UPSTOX_OPTION_CONTRACTS_URL = "https://api.upstox.com/v2/option/contract"
UPSTOX_OPTION_CHAIN_URL = "https://api.upstox.com/v2/option/chain"

PIVOT_CACHE_FILE = "pivot_cache.json"
PCR_EXPIRY_CACHE_FILE = "pcr_expiry_cache.json"

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))

# Stock / index cash-market session (NSE).
STOCK_SESSION_START = dt.time(9, 15)
STOCK_SESSION_END = dt.time(15, 30)

# Commodity (MCX) session — MCX's evening session actually runs until
# 11:30 PM IST (was incorrectly set to 23:00 here, which cut off the
# last 30 minutes of commodity scanning/alerting every day).
COMMODITY_SESSION_START = dt.time(9, 15)
COMMODITY_SESSION_END = dt.time(23, 30)


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
    """
    Rate-limited via _intraday_fetch_limiter (defined below) — this
    endpoint has its own Upstox budget (25/sec, 250/min, 1000/30min)
    separate from the historical/pivot endpoints. With FETCH_WORKERS
    (30) threads calling it unrated for the combined ~714-instrument
    F&O+Nifty500 watchlist, a single run could burst past the 250/min
    cap and get mass 429'd (instruments silently skipped for that run).
    The limiter caps the true combined outbound rate across all worker
    threads so nothing gets dropped — see _intraday_fetch_limiter.
    """
    _intraday_fetch_limiter.wait()
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


def resample_5min(df):
    """
    Resamples 1-minute candles to 5-minute bars — drives the standalone
    index (NIFTY 50 / NIFTY BANK / SENSEX) EMA9/EMA20 cross alert (see
    run_fo_scan, index_only mode). No per-day origin alignment needed:
    the session start 09:15 IST (555 min from midnight) is already a
    clean multiple of 5 minutes, same reasoning as resample_3min.
    """
    df = df.set_index("timestamp")
    out = df.resample("5min").agg({
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
    _pivot_fetch_limiter.wait()
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

# Caps outbound requests to Upstox's daily-candle endpoint (used only by
# fetch_prev_day_ohlc, for R3/S3 pivot levels — see build_pivot_levels).
# This was fine unrated at config.FETCH_WORKERS (30) concurrency back
# when it only covered the ~214-instrument F&O/index/commodity list,
# but started returning mass 429s once the Nifty 500 scan added ~500
# more names hitting the same endpoint in the same run (same "thundering
# herd" issue HIST_FETCH_MAX_PER_SECOND already solves for the
# historical 1-min endpoint above — retries alone don't help because
# every thread retries in lockstep and collides again).
_pivot_fetch_limiter = _RateLimiter(config.PIVOT_FETCH_MAX_PER_SECOND)

# Caps outbound requests to Upstox's intraday-candle endpoint (used by
# fetch_1min_candles — the hot path called on EVERY scan run for every
# instrument in the watchlist, up to ~714 combined for F&O+Nifty500).
# This endpoint has its own separate Upstox budget (25/sec, 250/min,
# 1000/30min). Unrated at FETCH_WORKERS (30) concurrency, a single run
# could burst past the 250/min cap and get mass 429'd — this is what
# caused the "224 instruments failed to fetch" warning. Capping the
# combined outbound rate at config.INTRADAY_FETCH_MAX_PER_SECOND
# (4/sec = 240/min, safely under the 250/min cap) means the ~714-call
# fetch now takes ~3 minutes instead of ~30 seconds, but nothing gets
# dropped — every instrument gets fetched and checked every run.
_intraday_fetch_limiter = _RateLimiter(config.INTRADAY_FETCH_MAX_PER_SECOND)


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
    headers = {
        "Authorization": f"Bearer {config.UPSTOX_ACCESS_TOKEN}",
        "Accept": "application/json",
    }
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


def fetch_option_chain_snapshot(instrument_key):
    """
    Fetches the full option chain for the given underlying's nearest
    expiry and reduces it to what's needed for (a) PCR and (b) Call/Put
    writing-buildup detection (see compute_oi_buildup / update_oi_buildup
    / get_stock_oi_buildup below):
      - pcr: total Put OI / total Call OI across the whole chain
      - strikes: a per-strike {call_oi, call_ltp, put_oi, put_ltp} snapshot,
        restricted to strikes within config.OI_STRIKE_RANGE_PCT of the
        current spot price — far OTM/ITM strikes have thin, noisy OI
        that isn't useful for a writing-buildup read.
    Informational only — returns None (never raises) if anything about
    the lookup fails, so a failed fetch can never block or delay an
    alert from being sent.

    NOTE: assumes each option-chain row has a "strike_price", an
    "underlying_spot_price", and call_options/put_options.market_data.
    {oi, ltp} — verify these field names against current Upstox docs if
    this starts returning empty/wrong data. Also assumes the endpoint
    accepts an F&O stock's NSE_EQ (cash) instrument_key as the
    underlying the same way it accepts an index's NSE_INDEX key —
    verify this specifically for stocks if it starts returning empty
    data for them.
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

        spot = None
        total_call_oi = 0
        total_put_oi = 0
        strikes = {}

        for row in chain:
            call_md = (row.get("call_options") or {}).get("market_data") or {}
            put_md = (row.get("put_options") or {}).get("market_data") or {}
            call_oi = call_md.get("oi", 0) or 0
            put_oi = put_md.get("oi", 0) or 0
            total_call_oi += call_oi
            total_put_oi += put_oi

            if spot is None:
                spot = row.get("underlying_spot_price")

            strike_price = row.get("strike_price")
            if strike_price is None:
                continue
            strikes[str(strike_price)] = {
                "call_oi": call_oi,
                "call_ltp": call_md.get("ltp", 0) or 0,
                "put_oi": put_oi,
                "put_ltp": put_md.get("ltp", 0) or 0,
            }

        if total_call_oi <= 0 or not spot:
            return None

        pcr = round(total_put_oi / total_call_oi, 2)

        band = float(spot) * config.OI_STRIKE_RANGE_PCT / 100
        near_strikes = {
            k: v for k, v in strikes.items()
            if abs(float(k) - float(spot)) <= band
        }

        return {"expiry": expiry, "spot": spot, "pcr": pcr, "strikes": near_strikes}
    except Exception as e:
        print(f"Option chain fetch failed for {instrument_key}: {e}")
        return None


def compute_oi_buildup(prev_strikes, curr_strikes, min_change_pct):
    """
    Compares two option-chain strike snapshots (see
    fetch_option_chain_snapshot) and classifies Call/Put WRITING
    (short buildup) per strike, the options-chain equivalent of "OI up,
    price down" for a stock/future:
      Call writing = Call OI up by >= min_change_pct AND Call LTP flat/down
      Put writing  = Put OI up by >= min_change_pct AND Put LTP flat/down
    Sums the OI added under "writing" on each side across every strike
    in the snapshot (also tracks the opposite — OI up + price up — as
    call_buying_oi/put_buying_oi, informational, not used for the bias
    call below). Whichever side added meaningfully more writing OI is
    called the dominant one:
      more Call writing -> BEARISH (call writers expect price to stay
        below their strike, so they're comfortable selling calls there)
      more Put writing  -> BULLISH (put writers expect price to stay
        above their strike, so they're comfortable selling puts there)
    A <20% edge either way is called NEUTRAL (mixed/no clear side).

    Returns None if there isn't enough overlapping/valid strike data to
    compare (e.g. first snapshot ever for this instrument, or every
    strike's OI change was under min_change_pct).
    """
    call_writing_oi = 0
    put_writing_oi = 0
    call_buying_oi = 0
    put_buying_oi = 0
    compared = 0

    for strike, curr in curr_strikes.items():
        prev = prev_strikes.get(strike)
        if not prev:
            continue

        prev_call_oi = prev.get("call_oi", 0)
        prev_put_oi = prev.get("put_oi", 0)

        if prev_call_oi > 0:
            call_oi_change_pct = (curr["call_oi"] - prev_call_oi) / prev_call_oi * 100
            if call_oi_change_pct >= min_change_pct:
                compared += 1
                oi_added = curr["call_oi"] - prev_call_oi
                if curr["call_ltp"] <= prev.get("call_ltp", 0):
                    call_writing_oi += oi_added
                else:
                    call_buying_oi += oi_added

        if prev_put_oi > 0:
            put_oi_change_pct = (curr["put_oi"] - prev_put_oi) / prev_put_oi * 100
            if put_oi_change_pct >= min_change_pct:
                compared += 1
                oi_added = curr["put_oi"] - prev_put_oi
                if curr["put_ltp"] <= prev.get("put_ltp", 0):
                    put_writing_oi += oi_added
                else:
                    put_buying_oi += oi_added

    if compared == 0 or (call_writing_oi == 0 and put_writing_oi == 0):
        return None

    if call_writing_oi > put_writing_oi * 1.2:
        bias, note = "BEARISH", "Call writing dominant"
    elif put_writing_oi > call_writing_oi * 1.2:
        bias, note = "BULLISH", "Put writing dominant"
    else:
        bias, note = "NEUTRAL", "Mixed / no clear side"

    return {
        "bias": bias,
        "note": note,
        "call_writing_oi": int(call_writing_oi),
        "put_writing_oi": int(put_writing_oi),
        "call_buying_oi": int(call_buying_oi),
        "put_buying_oi": int(put_buying_oi),
    }


def _load_oi_buildup_cache():
    try:
        with open(config.OI_BUILDUP_CACHE_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"date": None, "snapshots": {}}


def _save_oi_buildup_cache(cache):
    with open(config.OI_BUILDUP_CACHE_FILE, "w") as f:
        json.dump(cache, f)


def update_oi_buildup(index_watchlist):
    """
    Runs once per scan for every index in index_watchlist
    ({display: instrument_key}) — independent of whether any alert
    fires this run, same pattern as fetch_sector_trends. Fetches each
    index's current option-chain snapshot, compares it against the
    PREVIOUS RUN's snapshot (cached in config.OI_BUILDUP_CACHE_FILE —
    roughly one run apart, ~3 min) to detect Call/Put writing buildup
    (see compute_oi_buildup), then overwrites the cache with the
    current snapshot for next run's comparison. Cache resets when the
    calendar date rolls over, same pattern as build_pivot_levels().

    Returns {display: {"pcr": float_or_None, "buildup": dict_or_None}}.
    buildup is None if there's no previous snapshot yet for today (e.g.
    first run after market open) or nothing crossed the noise threshold
    this run. Never raises — a failed fetch for one index just means
    that index gets no PCR/buildup line on its alert this run.
    """
    cache = _load_oi_buildup_cache()
    today_str = _now_ist().date().isoformat()
    if cache.get("date") != today_str:
        cache = {"date": today_str, "snapshots": {}}

    results = {}
    for display, instrument_key in index_watchlist.items():
        snap = fetch_option_chain_snapshot(instrument_key)
        if snap is None:
            results[display] = {"pcr": None, "buildup": None}
            continue

        prev_entry = cache["snapshots"].get(display)
        buildup = None
        if prev_entry and prev_entry.get("expiry") == snap["expiry"]:
            buildup = compute_oi_buildup(
                prev_entry["strikes"], snap["strikes"], config.OI_BUILDUP_MIN_CHANGE_PCT
            )

        results[display] = {"pcr": snap["pcr"], "buildup": buildup}
        cache["snapshots"][display] = {"expiry": snap["expiry"], "strikes": snap["strikes"]}

    _save_oi_buildup_cache(cache)
    return results


def _load_stock_oi_cache():
    try:
        with open(config.STOCK_OI_BUILDUP_CACHE_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_stock_oi_cache(cache):
    with open(config.STOCK_OI_BUILDUP_CACHE_FILE, "w") as f:
        json.dump(cache, f)


def get_stock_oi_buildup(symbol, instrument_key):
    """
    ON-DEMAND Call/Put writing-buildup check for an F&O STOCK — unlike
    update_oi_buildup() (indices, which snapshots every run regardless
    of whether an alert fires), this only runs when an alert is ABOUT
    to fire for `symbol`. Re-fetching option chains for all ~180 F&O
    stocks every run would multiply Upstox API calls far past what the
    existing scan already uses, and Upstox's shared "Other APIs" rate
    limit is already tight at the current scale — so this trades
    continuous tracking for only paying the extra API cost on the (much
    rarer) alerts that actually fire.

    Because of that, the comparison "previous snapshot" here is
    whenever this stock LAST fired an alert (could be minutes, hours,
    or days ago) — NOT a fixed ~3-min run-to-run window like the index
    version. The buildup reading reflects "what changed in this stock's
    option OI since the last time we alerted on it", and since_hours
    tells you how old that comparison point is, so the Telegram message
    doesn't imply a tight window it isn't.

    Returns {"pcr": float_or_None, "buildup": dict_or_None,
    "since_hours": float_or_None} — buildup/since_hours are None if
    there's no usable previous snapshot yet for this stock (its first
    alert since a contract rollover, or ever). Returns None only if the
    option-chain fetch itself failed. Never raises.
    """
    snap = fetch_option_chain_snapshot(instrument_key)
    if snap is None:
        return None

    cache = _load_stock_oi_cache()
    prev = cache.get(symbol)

    buildup = None
    since_hours = None
    if prev and prev.get("expiry") == snap["expiry"]:
        buildup = compute_oi_buildup(prev["strikes"], snap["strikes"], config.OI_BUILDUP_MIN_CHANGE_PCT)
        try:
            prev_time = dt.datetime.fromisoformat(prev["snapshot_at"])
            since_hours = round((_now_ist() - prev_time).total_seconds() / 3600, 1)
        except Exception:
            since_hours = None

    cache[symbol] = {
        "expiry": snap["expiry"],
        "strikes": snap["strikes"],
        "snapshot_at": _now_ist().isoformat(),
    }
    _save_stock_oi_cache(cache)

    return {"pcr": snap["pcr"], "buildup": buildup, "since_hours": since_hours}


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

        with ThreadPoolExecutor(max_workers=config.PIVOT_FETCH_WORKERS) as pool:
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
    Returns (symbol, df3, df5, df75, df15) — df3 is kept purely as
    informational 3-min context attached to 75-min stock/commodity
    alerts (get_3min_trend_info); df5 drives the standalone index
    (NIFTY 50/BANK/SENSEX) EMA9/EMA20 cross alert; df75 is the primary
    75-min signal timeframe for stocks/commodities/cash; df15 is
    resampled but currently unused (kept for reference).

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

    df5 = resample_5min(raw)
    df5 = drop_unclosed_candle(df5, now_ist, candle_minutes=5)

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
    return symbol, df3, df5, df75, df15


def fetch_all(watchlist, now_ist, workers, hist_cache=None):
    """
    Fetches + resamples 1-min candles -> 3-min (and 75-min) for every
    instrument in watchlist, concurrently (up to `workers` threads).

    hist_cache: {symbol: [candle, ...]} of cached pre-today 1-min
    history (see build_hist1min_cache), passed through to each
    instrument's 75-min resample so EMA9/EMA20 there is properly warmed
    up. Optional — omitting it just means 75-min features stay dormant.

    Returns (dfs, failed_symbols):
      dfs            -- {symbol: (df3, df5, df75, df15)} for every instrument
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
                sym, df3, df5, df75, df15 = future.result()
                if df3 is not None:
                    dfs[sym] = (df3, df5, df75, df15)
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


def maybe_send_failure_summary(fo_failed, fo_total, n500_failed=None, n500_total=0):
    n500_failed = n500_failed or []
    total_failed = len(fo_failed) + len(n500_failed)
    if total_failed < config.FAILURE_ALERT_MIN_COUNT:
        return

    lines = [
        f"⚠️ Scan warning: {total_failed} instrument(s) failed to fetch this run "
        f"(after retries) and were skipped.",
        f"F&O/Index/Commodity scan: {len(fo_failed)}/{fo_total} failed",
    ]
    if n500_total:
        lines.append(f"Nifty 500 cash scan: {len(n500_failed)}/{n500_total} failed")
    lines.append("They will be retried automatically on the next run.")

    send_telegram_text("\n".join(lines))


# ---------------------------------------------------------------------
# ~50 F&O stock/index/commodity scan
# ---------------------------------------------------------------------

def run_fo_scan(now_ist, index_only=False):
    """
    index_only=True restricts the watchlist to just the 3 index
    instruments (NIFTY 50 / NIFTY BANK / SENSEX) — used by the
    dedicated 5-min index-only cron trigger so it stays a cheap ~3-call
    run, completely independent of the full F&O/commodity 75-min scan.
    """
    watchlist = build_watchlist(now_ist)
    if index_only:
        watchlist = {sym: watchlist[sym] for sym in config.INDICES.keys() if sym in watchlist}
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

    hist_cache = build_hist1min_cache(watchlist)

    index_watchlist_this_run = {sym: watchlist[sym] for sym in index_symbols if sym in watchlist}

    # Run the main stock/index/commodity fetch, the sector-index fetch,
    # and the index option-chain (PCR + Call/Put writing buildup) fetch
    # AT THE SAME TIME (not one after another) — all three are
    # completely independent of each other. This means the sector and
    # option-chain features add ~zero extra wait before alerts go out:
    # both are only a handful of lightweight calls and finish well
    # before the much bigger stock fetch does, so the overall run time
    # is still just however long the stock fetch alone takes. A
    # sector/option-chain fetch failure/timeout is also fully isolated
    # — wrapped in try/except below — so it can never delay or block
    # the main stock alerts even in the worst case.
    with ThreadPoolExecutor(max_workers=3) as outer_pool:
        main_future = outer_pool.submit(fetch_all, watchlist, now_ist, config.FETCH_WORKERS, hist_cache)
        sector_future = outer_pool.submit(fetch_sector_trends, now_ist)
        oi_future = outer_pool.submit(update_oi_buildup, index_watchlist_this_run)
        dfs, failed_symbols = main_future.result()
        try:
            sector_trends = sector_future.result()
        except Exception as e:
            print(f"Sector trend fetch failed this run (non-blocking): {e}")
            sector_trends = {}
        try:
            oi_data = oi_future.result()
        except Exception as e:
            print(f"Option-chain (PCR/OI buildup) fetch failed this run (non-blocking): {e}")
            oi_data = {}

    for symbol, (df3, df5, df75, df15) in dfs.items():
        try:
            levels = pivots.get(symbol)
            r3 = levels["r3"] if levels else None
            s3 = levels["s3"] if levels else None
            prev_close = levels.get("prev_close") if levels else None

            # ALERTING SIGNAL. Indices and stocks/commodities now follow
            # completely separate rules:
            #   - Indices (NIFTY 50, NIFTY BANK, SENSEX): a PURE 5-min
            #     EMA9/20 crossover — no EMA50 trend requirement, no
            #     75-min gate at all. check_signals() runs directly on
            #     df5 with require_trend_confirmation=False. Checked by
            #     a dedicated 5-min cron trigger (index_only=True), fully
            #     separate from the 75-min stock/commodity runs.
            #   - Stocks/commodities: unchanged — 75-min EMA9/20 cross +
            #     mandatory EMA50 trend agreement, on df75.
            if symbol in index_symbols:
                if df5 is None:
                    continue
                signals = check_signals(
                    df5, symbol, r3=r3, s3=s3,
                    lookback=config.INDEX_ALERT_LOOKBACK_CANDLES,
                    require_trend_confirmation=False,
                    prev_close=prev_close,
                )
                for sig in signals:
                    sig["timeframe"] = "5-min"
            else:
                if df75 is None:
                    continue
                signals = check_signals(
                    df75, symbol, r3=r3, s3=s3,
                    lookback=config.PRIMARY_LOOKBACK_CANDLES,
                    require_trend_confirmation=False,
                    prev_close=prev_close,
                    ema_fast=config.EMA_FAST,
                    ema_slow=config.PRIMARY_EMA_SLOW,
                    require_volume_increase=False,
                )
                for sig in signals:
                    sig["timeframe"] = "75-min"

            if not signals:
                continue

            # 15-min context (get_3min_trend_info, now run on df15 —
            # per request, 2026-08-12, changed from the 3-min chart to
            # the 15-min chart) is only meaningful as supporting context
            # UNDERNEATH a 75-min alert — for indices the alert itself
            # now IS the 3-min signal, so there's nothing extra to
            # attach there.
            info3 = None
            if symbol not in index_symbols and df15 is not None:
                info3 = get_3min_trend_info(df15, symbol)

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

                    # Bulk/Block deal (added) — stocks only, the SINGLE
                    # most recent Bulk/Block deal for this symbol
                    # within the trailing lookback window, if any (see
                    # bulk_block_data.py). Informational only, fetched
                    # on-demand right here since this alert is about to
                    # fire anyway — a failed/empty fetch never blocks
                    # the alert, just means no Bulk/Block line on it.
                    last_deal = bulk_block_data.get_last_deal_for_symbol(symbol)
                    if last_deal:
                        signal["last_bulk_block_deal"] = last_deal

                    # PCR + Call/Put writing buildup — F&O stocks only
                    # (cash-only stocks have no option chain), fetched
                    # ON-DEMAND right here rather than every run (see
                    # get_stock_oi_buildup) since this alert is about to
                    # fire anyway. Informational only — a failed/slow
                    # fetch never blocks the alert, just means no PCR/
                    # buildup line on it.
                    if signal["is_fno"]:
                        oi = get_stock_oi_buildup(symbol, watchlist[symbol])
                        if oi:
                            signal["pcr"] = oi["pcr"]
                            if oi["buildup"]:
                                signal["oi_buildup"] = oi["buildup"]
                                signal["oi_buildup_since_hours"] = oi["since_hours"]

                if symbol in index_symbols:
                    # PCR + Call/Put writing buildup — informational
                    # only, computed once per run for every index
                    # regardless of whether an alert fires (see
                    # update_oi_buildup above), so this is just a
                    # lookup here. Missing/failed fetch this run simply
                    # means no PCR/buildup line on the alert.
                    oi = oi_data.get(symbol)
                    if oi:
                        signal["pcr"] = oi["pcr"]
                        if oi["buildup"]:
                            signal["oi_buildup"] = oi["buildup"]
                else:
                    # Confluence "High R:R" filter (added) — never
                    # applied to indices (see above). Combines fields
                    # the signal already carries (risk_reward, RSI,
                    # sector_trend, oi_buildup — all set above) into a
                    # single quality gate; see
                    # strategy.passes_confluence_filter for the exact
                    # rules. A signal that fails this is simply not
                    # sent (and not marked alerted, so it's re-checked
                    # — and can still pass later once RSI/sector/OI
                    # shift — on the next run within the lookback
                    # window).
                    if config.CONFLUENCE_FILTER_ENABLED and not passes_confluence_filter(signal):
                        continue
                    signal["confluence_passed"] = True

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
    for symbol, (df3, df5, df75, df15) in dfs.items():
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


# ---------------------------------------------------------------------
# Nifty 500 cash-stock scan (added)
# ---------------------------------------------------------------------
# Same signal logic/conditions as the F&O 75-min flow above — EMA cross
# + mandatory EMA50 trend agreement on df75, with df3 shown as
# informational 3-min context — just on the full Nifty 500 constituent
# list (cash/EQ, fetched live from NSE — see
# instruments.resolve_nifty500_stocks) and EMA9/21
# (config.NIFTY500_EMA_FAST/EMA_SLOW) instead of EMA9/20. No PCR/OI
# writing-buildup here (cash-focused scan; most Nifty 500 names have no
# option chain anyway) — the "F&O: Yes/No" flag on the alert still
# tells you whether a given name also happens to be F&O-eligible.
# Deduped independently from the F&O scan (state key suffixed "::N500"),
# so a stock that's in both lists can alert separately under each
# EMA pair without one suppressing the other.

def build_nifty500_watchlist(now_ist=None):
    now_ist = now_ist or _now_ist()
    if not _in_stock_session(now_ist):
        return {}
    return instruments.resolve_nifty500_stocks()


def run_nifty500_scan(now_ist):
    watchlist = build_nifty500_watchlist(now_ist)
    if not watchlist:
        print("Nifty 500 scan: empty watchlist (outside session or list unavailable) — skipping.")
        return 0, [], 0

    print(f"Scanning {len(watchlist)} Nifty 500 cash stocks (EMA{config.NIFTY500_EMA_FAST}/{config.NIFTY500_EMA_SLOW})...")

    pivots = build_pivot_levels(watchlist)
    delivery_map = delivery_data.get_delivery_data(now_ist.date())
    saved_state = state.load_state()
    alerts_sent = 0

    fno_underlyings = instruments.get_fno_underlyings()
    hist_cache = build_hist1min_cache(watchlist)

    with ThreadPoolExecutor(max_workers=2) as outer_pool:
        main_future = outer_pool.submit(fetch_all, watchlist, now_ist, config.FETCH_WORKERS, hist_cache)
        sector_future = outer_pool.submit(fetch_sector_trends, now_ist)
        dfs, failed_symbols = main_future.result()
        try:
            sector_trends = sector_future.result()
        except Exception as e:
            print(f"Sector trend fetch failed this run (non-blocking): {e}")
            sector_trends = {}

    for symbol, (df3, df5, df75, df15) in dfs.items():
        try:
            levels = pivots.get(symbol)
            r3 = levels["r3"] if levels else None
            s3 = levels["s3"] if levels else None
            prev_close = levels.get("prev_close") if levels else None

            if df75 is None:
                continue
            signals = check_signals(
                df75, symbol, r3=r3, s3=s3,
                lookback=config.PRIMARY_LOOKBACK_CANDLES,
                require_trend_confirmation=False,
                prev_close=prev_close,
                ema_fast=config.NIFTY500_EMA_FAST,
                ema_slow=config.NIFTY500_EMA_SLOW,
                require_volume_increase=False,
            )
            for sig in signals:
                sig["timeframe"] = "75-min"

            if not signals:
                continue

            # info3 now runs on df15 (15-min candles) instead of df3 —
            # per request, 2026-08-12.
            info3 = None
            if df15 is not None:
                info3 = get_3min_trend_info(
                    df15, symbol,
                    ema_fast=config.NIFTY500_EMA_FAST,
                    ema_slow=config.NIFTY500_EMA_SLOW,
                )

            state_symbol = f"{symbol}::N500"

            for signal in signals:
                if state.already_alerted(saved_state, state_symbol, signal["direction"], signal["candle_time"]):
                    continue

                if info3 is not None:
                    signal["trend_3min"] = info3

                signal["is_fno"] = symbol.upper() in fno_underlyings

                deliv = delivery_map.get(symbol.upper())
                if deliv is not None:
                    signal["delivery_pct"] = deliv

                last_deal = bulk_block_data.get_last_deal_for_symbol(symbol)
                if last_deal:
                    signal["last_bulk_block_deal"] = last_deal

                sector_name = config.STOCK_SECTOR_MAP.get(symbol.upper())
                if sector_name:
                    signal["sector_index"] = sector_name
                    signal["sector_trend"] = sector_trends.get(sector_name)

                if config.CONFLUENCE_FILTER_ENABLED and not passes_confluence_filter(signal):
                    continue
                signal["confluence_passed"] = True

                send_alert(signal)
                state.mark_alerted(saved_state, state_symbol, signal["direction"], signal["candle_time"])
                alerts_sent += 1

        except Exception as e:
            print(f"Error on {symbol} (Nifty 500 scan): {e}")

    state.save_state(saved_state)
    if failed_symbols:
        print(f"{len(failed_symbols)} Nifty 500 instrument(s) failed to fetch this run: {failed_symbols}")
    print(f"Nifty 500 scan done. {alerts_sent} alert(s) sent.")
    return alerts_sent, failed_symbols, len(watchlist)


def run():
    # TEMP DEBUG — remove once we confirm why "Run scan" finished in ~1s
    # with zero stdout beyond the auto-generated env dump: this print
    # (with explicit flush) tells us whether run() is even being
    # entered, and if so, whether the trading-session gate below is
    # what's causing the early return.
    print(f"[debug] run() started, token_set={bool(config.UPSTOX_ACCESS_TOKEN)}", flush=True)

    if not config.UPSTOX_ACCESS_TOKEN:
        print("UPSTOX_ACCESS_TOKEN not set — aborting.", flush=True)
        sys.exit(1)

    now_ist = _now_ist()
    print(
        f"[debug] now_ist={now_ist.isoformat()} "
        f"in_stock_session={_in_stock_session(now_ist)} "
        f"in_commodity_session={_in_commodity_session(now_ist)}",
        flush=True,
    )
    # SCAN_MODE=index -> dedicated lightweight run for the 5-min index
    # (NIFTY 50/BANK/SENSEX) alert only — set by the "mode" input on the
    # index-only cron trigger (see scan.yml). Completely independent of
    # the full 75-min F&O/Nifty500/commodity scan below: only ~3 API
    # calls, safe to run every 5 minutes without touching rate limits.
    mode = os.environ.get("SCAN_MODE", "full")
    if mode == "index":
        if not _in_stock_session(now_ist):
            print(f"Outside stock session ({now_ist.strftime('%H:%M')} IST) — index-only scan skipping.", flush=True)
            return
        run_fo_scan(now_ist, index_only=True)
        return

    if not (_in_stock_session(now_ist) or _in_commodity_session(now_ist)):
        print(f"Outside all trading sessions ({now_ist.strftime('%H:%M')} IST) — skipping.", flush=True)
        return

    _, fo_failed, fo_total = run_fo_scan(now_ist)

    # ---- Corporate-action alerts (Dividend/Bonus/Buyback/Order Win) ----
    # Same watchlist, same Telegram channel as the EMA alerts. Cheap to
    # call every run -- corporate_actions.check_and_alert() only does
    # real work (NSE fetch) once per calendar day; every other call
    # this same day is a no-op (see its own cache check).
    try:
        corporate_actions.check_and_alert(now_ist)
    except Exception as e:
        print(f"Corporate-action check failed this run (non-blocking): {e}", flush=True)

    # ---- Nifty 500 cash-stock scan (same conditions, EMA9/21) ----
    n500_failed, n500_total = [], 0
    if _in_stock_session(now_ist):
        try:
            _, n500_failed, n500_total = run_nifty500_scan(now_ist)
        except Exception as e:
            print(f"Nifty 500 scan failed this run (non-blocking): {e}", flush=True)

    # ---- Fetch-failure visibility: one summary alert if enough
    # instruments failed to fetch this run ----
    maybe_send_failure_summary(fo_failed, fo_total, n500_failed, n500_total)


if __name__ == "__main__":
    run()
