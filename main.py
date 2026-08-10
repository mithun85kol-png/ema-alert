

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

75-min PRIMARY/ALERTING signal for STOCKS/COMMODITIES ONLY (FLIPPED
from the previous 3-min-primary design): the main EMA9/EMA20 crossover
check (strategy.check_signals — EMA9/20 cross + mandatory EMA50 trend
agreement; volume is informational only, never gating) now runs
on df75 — 1-min data (cached multi-day history + today's live data)
resampled to 75-min candles. This is the timeframe that decides
whether/when a STOCK/COMMODITY Telegram alert is sent. The old design
made 75-min a MANDATORY gate on top of a 3-min primary cross, which
meant an alert only fired once a 3-min cross AND a fresh 75-min cross
both lined up — causing alerts to arrive well after the 75-min cross
had actually happened. That gate is gone: a 75-min cross (+ trend
agreement) now fires the alert directly, as soon as that 75-min candle
closes.

INDICES (NIFTY 50, NIFTY BANK, SENSEX): completely separate rule, not
tied to df75 at all. An index alert fires on a PURE EMA9/20 crossover
on the 3-min chart (df3) — no EMA50 trend requirement, no 75-min
involvement. strategy.check_signals(df3, ..., require_trend_confirmation
=False) is called directly on df3, scanning the trailing
config.INDEX_3MIN_ALERT_LOOKBACK_CANDLES closed 3-min candles. The
resulting signal is tagged timeframe="3-min" (telegram_notifier uses
this to label the message correctly) and does NOT get a trend_3min
context block, since the alert itself already is the 3-min signal.

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
stocks — the 75-min loop is their only alert (EMA9/20 cross + mandatory
EMA50 trend agreement; volume is informational only), with 3-min shown
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
- INDICES (3-min): a 3-min candle closes far more often — every 3
  minutes during the session. check_signals() re-checks the last
  config.INDEX_3MIN_ALERT_LOOKBACK_CANDLES closed 3-min candles, so a
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
            futur
