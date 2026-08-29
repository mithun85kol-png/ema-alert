
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

PRIMARY/ALERTING timeframe for STOCKS/COMMODITIES/CASH is controlled
by ONE setting: config.PRIMARY_TIMEFRAME ("15min" or "75min"). Flip
that value alone to switch which timeframe fires alerts — nothing
else in this file needs to change. The main EMA crossover check
(strategy.check_signals) runs on whichever df main.py picks based on
that setting: df15 (EMA9/20) when "15min", df75 (EMA9/50) when
"75min". The OTHER timeframe is shown as informational-only context
under every alert (signal["trend_3min"] / signal["info_timeframe_label"]).
Three gating conditions on the primary-timeframe crossing candle are
each independently toggleable in config.py: REQUIRE_TREND_CONFIRMATION
(EMA50 trend agreement), REQUIRE_STRONG_CANDLE (candle body strength),
and REQUIRE_VOLUME_CONFIRMATION (volume > previous candle). All three
are mandatory (True) by default.

INDICES (NIFTY 50, NIFTY BANK, SENSEX): completely separate rule, not
tied to df75 at all, and checked by its OWN dedicated cron trigger
(SCAN_MODE=index -> run_fo_scan(index_only=True) — see run()), fully
independent of the 75-min F&O/Nifty500/commodity scan. An index alert
fires on a PURE EMA9/20 crossover on the 15-min chart (df15) — no
EMA50 trend requirement, no 75-min involvement. (CHANGED from 5-min to
15-min, per request — reuses the same df15 already resampled for
stocks/commodities.)
strategy.check_signals(df15, ..., require_trend_confirmation=False) is
called directly on df15, scanning the trailing
config.INDEX_ALERT_LOOKBACK_CANDLES closed 15-min candles. The
resulting signal is tagged timeframe="15-min" (telegram_notifier uses
this to label the message correctly) and does NOT get a trend_3min
context block, since the alert itself already is the short-timeframe
signal.

3-min (df3) processing has been removed entirely (2026-08-13) — it was
being resampled/fetched for every instrument on every run but nothing
actually read it any more (the informational context below had already
moved to df15). This was pure wasted work on every run. df15 (15-min)
is what's built for every instrument now. For STOCKS/COMMODITIES,
strategy.get_3min_trend_info(df15, symbol) is computed for every
qualifying 75-min signal and attached as signal["trend_3min"], showing:
  - whether EMA9/20 has crossed on the 15-min chart recently (and how
    many 15-min candles ago), or hasn't crossed within the lookback
    window at all
  - how close EMA9/20 currently are to crossing on the 15-min chart
    (gap_pct — smaller = closer to a cross)
Purely informational — it never gates or blocks the 75-min alert. (Not
attached for indices — see above.)

COMMODITIES (GOLD/SILVER/CRUDEOIL etc.): follow the exact same rule as
stocks — the 75-min loop is their only alert (EMA9/50 cross, mandatory
volume increase over the previous candle), with 15-min shown
as context on the alert. There is no separate standalone commodity-only
alert (a 15-min standalone version used to exist here; it remains
removed).

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
- INDICES (15-min): a 15-min candle closes 5 times a day during the
  session (09:15, 09:30, ... — same cadence as the stock/commodity
  15-min primary), checked by its own dedicated cron trigger
  (SCAN_MODE=index). check_signals() re-checks the last
  config.INDEX_ALERT_LOOKBACK_CANDLES closed 15-min candles, so a
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
try:
    import corporate_actions
except ImportError:
    # corporate_actions.py (Dividend/Bonus/Buyback/Order Win alerts)
    # isn't present in this checkout. Never let a missing/optional
    # module take down the whole scan -- same "fail silently, never
    # block the main alert path" pattern as bulk_block_data.py /
    # delivery_data.py. If you have this file, just add it back to
    # the repo root and this feature resumes automatically.
    corporate_actions = None
from strategy import check_signals, debug_ema_gap, get_3min_trend_info, get_sector_trend, passes_confluence_filter, compute_smart_money_signal, check_breakout_scan, check_consolidation_breakout_scan, compute_consolidation_window, check_consolidation_breakout_live, compute_session_vwap, check_trendline_scan, get_opening_candle_bias, compute_intraday_checklist, get_opening_candle_buy_sell_estimate, compute_trading_score, passes_alert_gate, compute_near_high_score, compute_daily_score_scan
from telegram_notifier import send_alert, send_ema_cross_report, send_breakout_alert, send_consolidation_breakout_summary, send_trendline_alert, send_opening_bias_report, send_daily_score_report
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


def build_chart_link(symbol, timeframe_label=None):
    """
    TradingView deep link for `symbol` — tapping it in the Telegram
    alert opens that symbol's live chart directly (TradingView app if
    installed, else browser; no login/API key needed). Per request
    (2026-08-18), the chart ALWAYS opens on the 15-min interval,
    regardless of which timeframe the alert itself fired on — so
    timeframe_label is accepted for backward compatibility but no
    longer affects the link.
    Checks config.TRADINGVIEW_SYMBOL_OVERRIDES first (indices/MCX
    commodity futures, which don't chart correctly as plain
    "NSE:{symbol}"), otherwise defaults to "NSE:{symbol}" — correct
    for the vast majority of stocks (F&O watchlist + Nifty 500).
    """
    tv_symbol = config.TRADINGVIEW_SYMBOL_OVERRIDES.get(symbol, f"NSE:{symbol}")
    url = f"https://www.tradingview.com/chart/?symbol={tv_symbol}&interval=15"
    return url


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


def fetch_daily_history(instrument_key, days_back=40):
    """
    Fetches up to `days_back` calendar days of completed daily candles
    (strictly before today) for one instrument, via the same
    Upstox daily-candle endpoint used by fetch_prev_day_ohlc — reuses
    the same rate limiter (_pivot_fetch_limiter) since it's the same
    Upstox endpoint/budget. Returns a list of {"date", "open", "high",
    "low", "close", "volume"} sorted oldest -> newest, or [] on
    failure/no data. (open/high/low added 2026-08-18 for
    run_breakout_scan's Rows 9-12, which need the daily range, not just
    close/volume — existing callers that only read "close"/"volume"
    are unaffected.)
    """
    headers = {"Authorization": f"Bearer {config.UPSTOX_ACCESS_TOKEN}"}
    today = _now_ist().date()
    from_date = today - dt.timedelta(days=days_back)
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
        return []

    candles_sorted = sorted(candles, key=lambda c: c[0])
    return [
        {"date": c[0], "open": c[1], "high": c[2], "low": c[3], "close": c[4], "volume": c[5]}
        for c in candles_sorted
    ]


MOMENTUM_VOLUME_CACHE_FILE = "momentum_volume_cache.json"

# How many trailing COMPLETED trading days count as "last four weeks"
# for the Momentum check (5 trading days/week x 4 weeks). Today's own
# (still-forming) day is never included.
MOMENTUM_LOOKBACK_TRADING_DAYS = 20

# How many trading days back (from the most recent completed day) to
# compare against for the Volume Spike check.
VOLUME_SPIKE_LOOKBACK_TRADING_DAYS = 5

# EMA50 x EMA200 cross on DAILY candles — informational only, added
# per request. The classic "Golden Cross" (EMA50 crosses above EMA200
# = long-term bullish) / "Death Cross" (crosses below = long-term
# bearish). Computed on daily closes (not intraday), since that's what
# these periods conventionally mean. This needs much deeper daily
# history than the Momentum/Volume Spike checks above (EMA200 wants
# 200+ daily closes to warm up), so fetch_daily_history below is now
# called with DAILY_HISTORY_LOOKBACK_DAYS instead of its old default of
# 40 -- ONE fetch per symbol still covers Momentum/Volume Spike AND
# this, no extra API calls added.
EMA50_PERIOD = 50
EMA200_PERIOD = 200
# 320 calendar days -> roughly 210-220 trading days after weekends/
# holidays, comfortably covering EMA200's 200-candle warmup plus a
# buffer for cross-detection to look back into. NOTE: an EMA seeded
# from a fixed starting point only fully converges to the "true"
# infinite-history EMA after several multiples of its own period,
# so with ~220 candles feeding a 200-period EMA this is a solid
# approximation for an informational tag, not laboratory-precise --
# bump this constant higher (and, if needed, Upstox's date range) for
# tighter precision.
DAILY_HISTORY_LOOKBACK_DAYS = 320

# "Last High" 1-6 month lines (added, per request) — highest DAILY HIGH
# over the trailing N months, ~21 trading days/month. Purely
# informational, computed alongside Momentum/Volume Spike/EMA50-200 in
# build_momentum_volume_data below (same cached daily-history fetch, no
# extra API calls). A given month is simply omitted if there isn't
# enough daily history yet for that symbol (e.g. recent listing).
MONTHLY_HIGH_LOOKBACKS_TRADING_DAYS = {1: 21, 2: 42, 3: 63, 4: 84, 5: 105, 6: 126}


def _compute_ema50_200_cross(history):
    """
    history: the same oldest->newest list of {"date","close","volume"}
    fetch_daily_history returns. Returns None if there isn't enough of
    it yet to warm up EMA200 (len < EMA200_PERIOD + 5); otherwise
    {"bias", "ema50", "ema200", "cross_date"} — cross_date is the date
    (string, "YYYY-MM-DD") of the most recent EMA50/200 cross found
    anywhere in the supplied history, or None if the whole window was
    one-sided (no cross happened within the available history).
    """
    if len(history) < EMA200_PERIOD + 5:
        return None

    closes = [day["close"] for day in history]
    dates = [day["date"] for day in history]
    ema50 = pd.Series(closes).ewm(span=EMA50_PERIOD, adjust=False).mean()
    ema200 = pd.Series(closes).ewm(span=EMA200_PERIOD, adjust=False).mean()

    bias = "BULLISH" if ema50.iloc[-1] > ema200.iloc[-1] else "BEARISH"

    cross_date = None
    for i in range(len(closes) - 1, 0, -1):
        prev_diff = ema50.iloc[i - 1] - ema200.iloc[i - 1]
        curr_diff = ema50.iloc[i] - ema200.iloc[i]
        if (prev_diff <= 0 < curr_diff) or (prev_diff >= 0 > curr_diff):
            cross_date = str(dates[i])[:10]
            break

    return {
        "bias": bias,
        "ema50": round(float(ema50.iloc[-1]), 2),
        "ema200": round(float(ema200.iloc[-1]), 2),
        "cross_date": cross_date,
    }


def load_momentum_volume_cache():
    try:
        with open(MOMENTUM_VOLUME_CACHE_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"date": None, "data": {}}


def save_momentum_volume_cache(cache):
    with open(MOMENTUM_VOLUME_CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)


def load_daily_score_report_state():
    """
    Dedup state for send_daily_score_report (added, per request) —
    just the set of symbols included in the LAST message actually
    sent, plus that message's date. Resets automatically each new
    calendar day (a stale yesterday's list would be misleading).
    """
    try:
        with open(config.DAILY_SCORE_REPORT_STATE_FILE, "r") as f:
            data = json.load(f)
        if data.get("date") != _now_ist().date().isoformat():
            return {"date": _now_ist().date().isoformat(), "last_sent_symbols": []}
        return data
    except (FileNotFoundError, json.JSONDecodeError):
        return {"date": _now_ist().date().isoformat(), "last_sent_symbols": []}


def save_daily_score_report_state(symbols):
    with open(config.DAILY_SCORE_REPORT_STATE_FILE, "w") as f:
        json.dump({"date": _now_ist().date().isoformat(), "last_sent_symbols": sorted(symbols)}, f, indent=2)


def build_momentum_volume_data(watchlist):
    """
    Returns {symbol: {"four_week_high_close": ..., "prev_day_volume":
    ..., "volume_5day_ago": ..., "ema_cross": ...}} for every symbol in
    the watchlist — the raw numbers behind the three informational tags
    on the alert:

      - Momentum: today's alert close > four_week_high_close (the
        highest DAILY CLOSE over the last MOMENTUM_LOOKBACK_TRADING_DAYS
        completed trading days, i.e. the last ~4 weeks — today's own
        candle is never included).
      - Volume Spike: prev_day_volume (the most recent COMPLETED
        trading day's total volume) > volume_5day_ago (total volume
        VOLUME_SPIKE_LOOKBACK_TRADING_DAYS completed trading days
        before that).
      - EMA50/200 cross (added): "ema_cross" is None or
        {"bias","ema50","ema200","cross_date"} from
        _compute_ema50_200_cross — the daily-timeframe Golden/Death
        Cross, purely informational, same as the two above.

    Daily OHLC/volume is only fetched once per calendar day per symbol
    — results are cached in momentum_volume_cache.json, same pattern
    as build_pivot_levels/pivot_cache.json. Now fetches
    DAILY_HISTORY_LOOKBACK_DAYS (320) calendar days per symbol instead
    of the old 40, since EMA50/200 needs much deeper history than
    Momentum/Volume Spike alone did — still just one fetch per symbol
    per day, serving all three tags.
    """
    cache = load_momentum_volume_cache()
    today_str = _now_ist().date().isoformat()

    if cache.get("date") != today_str:
        cache = {"date": today_str, "data": {}}

    missing = {sym: key for sym, key in watchlist.items() if sym not in cache["data"]}

    if missing:
        print(f"Fetching daily history for {len(missing)} instrument(s) for Momentum/Volume Spike checks...")

        def _fetch_one(symbol, instrument_key):
            history = fetch_daily_history(instrument_key, days_back=DAILY_HISTORY_LOOKBACK_DAYS)
            if len(history) < MOMENTUM_LOOKBACK_TRADING_DAYS + 1:
                # Not enough daily history yet (new listing, fetch
                # partially failed, etc.) — both checks simply stay
                # unavailable for this symbol today, same graceful
                # degradation as pivots/delivery%/sector when data is
                # missing.
                return symbol, None

            lookback_window = history[-MOMENTUM_LOOKBACK_TRADING_DAYS:]
            four_week_high_close = max(day["close"] for day in lookback_window)

            prev_day_volume = history[-1]["volume"]
            volume_5day_ago = None
            if len(history) >= VOLUME_SPIKE_LOOKBACK_TRADING_DAYS + 1:
                volume_5day_ago = history[-(VOLUME_SPIKE_LOOKBACK_TRADING_DAYS + 1)]["volume"]

            # EMA50/200 (added, see _compute_ema50_200_cross above) —
            # None if this symbol's history is shorter than EMA200
            # needs; doesn't affect Momentum/Volume Spike above, which
            # have their own, much shallower requirement.
            ema_cross = _compute_ema50_200_cross(history)

            # "Last High" 1-6 month (added, per request) — highest
            # DAILY HIGH over the trailing N months (~21 trading
            # days/month), using the same `history` already fetched
            # above. A given month key is simply omitted if this
            # symbol doesn't have that much daily history yet.
            multi_month_highs = {}
            for months, days_needed in MONTHLY_HIGH_LOOKBACKS_TRADING_DAYS.items():
                if len(history) >= days_needed:
                    window = history[-days_needed:]
                    multi_month_highs[months] = max(day["high"] for day in window)

            # Consolidation window (added, per request — Consolidation
            # Breakout in the main real-time scan too) — computed once
            # here from the SAME `history` already fetched above, zero
            # extra API calls. None if the trailing
            # CONSOLIDATION_LOOKBACK_DAYS window wasn't actually tight
            # (or there isn't enough history) — see
            # strategy.compute_consolidation_window.
            consolidation_window = compute_consolidation_window(history)

            return symbol, {
                "four_week_high_close": four_week_high_close,
                "prev_day_volume": prev_day_volume,
                "volume_5day_ago": volume_5day_ago,
                "ema_cross": ema_cross,
                "multi_month_highs": multi_month_highs,
                "consolidation_window": consolidation_window,
            }

        with ThreadPoolExecutor(max_workers=config.PIVOT_FETCH_WORKERS) as pool:
            futures = {
                pool.submit(_fetch_one, symbol, instrument_key): symbol
                for symbol, instrument_key in missing.items()
            }
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    sym, data = future.result()
                    if data is not None:
                        cache["data"][sym] = data
                except Exception as e:
                    print(f"Error fetching Momentum/Volume Spike data for {symbol}: {e}")

        save_momentum_volume_cache(cache)

    return cache["data"]


def build_todays_ema_cross_list(watchlist, now_ist):
    """
    RE-ADDED (was missing from this checkout — see chat) — the
    standalone "EMA50/200 (Golden/Death Cross) + Delivery%" report
    (SCAN_MODE=ema_cross_report), separate from the regular alert scan.
    Returns a list of {symbol, bias, ema50, ema200, cross_date,
    delivery_pct}, sorted by symbol, for every watchlist symbol whose
    EMA50/200 cross happened on the LATEST trading day actually present
    in its own fetched daily history (cross_date == latest_date — see
    _compute_ema50_200_cross) — i.e. a FRESH cross, not one from
    weeks/months ago that just happens to still be the most recent one
    on record.

    IMPORTANT CAVEAT (read before relying on the open vs close run
    showing different things): fetch_daily_history always stops at
    YESTERDAY — today's own daily candle only exists once today's
    session has closed AND Upstox has finalized it (usually
    evening/next morning), never mid-day. So a run of this report at
    MARKET OPEN and one at MARKET CLOSE on the same calendar day will
    almost always show the exact same list — both are really reporting
    "as of yesterday's close", since today's close isn't known yet even
    at 15:30. This isn't a bug; it's just what "daily EMA cross" data
    can ever mean before today's candle exists.
    """
    momentum_volume = build_momentum_volume_data(watchlist)
    delivery_map = delivery_data.get_delivery_data(now_ist.date()) or {}

    crosses = []
    for symbol, mv in momentum_volume.items():
        ema_cross = mv.get("ema_cross") if mv else None
        if not ema_cross or not ema_cross.get("cross_date"):
            continue
        if ema_cross["cross_date"] != ema_cross["latest_date"]:
            continue
        crosses.append({
            "symbol": symbol,
            "bias": ema_cross["bias"],
            "ema50": ema_cross["ema50"],
            "ema200": ema_cross["ema200"],
            "cross_date": ema_cross["cross_date"],
            "delivery_pct": delivery_map.get(symbol),
        })

    return sorted(crosses, key=lambda c: c["symbol"])


def run_ema_cross_report(now_ist):
    """
    RE-ADDED (was missing — see chat) — SCAN_MODE=ema_cross_report
    entry point (see run()). Builds the full stock/commodity/Nifty500
    watchlist, finds fresh EMA50/200 crosses (see
    build_todays_ema_cross_list above), and sends ONE consolidated
    Telegram message via telegram_notifier.send_ema_cross_report —
    always sends something (even "no crosses today"), so a scheduled
    run being silent never looks like it might have just failed.
    """
    watchlist = build_watchlist(now_ist)
    crosses = build_todays_ema_cross_list(watchlist, now_ist)
    send_ema_cross_report(crosses, now_ist)


def build_todays_opening_bias_list(now_ist):
    """
    NEW (per request) — the "dala"/consolidated list of every F&O stock
    whose TODAY's first 15-min candle is Open==Low (bullish) or
    Open==High (bearish). SCAN_MODE=opening_bias_report entry point
    (see run_opening_bias_report / run()).

    Deliberately F&O-stocks-only (not indices, not commodities, not the
    wider Nifty 500 list) — matches "FNO stock" in the request. Reuses
    the exact same get_opening_candle_bias() used inline on every
    regular alert, so this list can never disagree with what an
    individual alert would show for the same symbol.

    Returns (bullish, bearish, no_data) — three lists of symbols,
    each sorted alphabetically. no_data means today's first 15-min
    candle hasn't closed yet (before ~09:30 IST) or the fetch failed
    for that symbol this run — kept separate from "neutral" (a candle
    that DID close but wasn't a clean Open==Low/Open==High) since only
    no_data is worth flagging back to you; a real neutral candle is
    just... not on either list, same as it wouldn't show on an
    individual alert either.
    """
    fo_watch = None if config.USE_FULL_FO_LIST else config.FO_STOCK_WATCHLIST
    watchlist = instruments.resolve_fo_stock_list(fo_watch)

    dfs, failed_symbols = fetch_all(watchlist, now_ist, config.FETCH_WORKERS)

    bullish, bearish, no_data = [], [], []
    for symbol in watchlist:
        if symbol not in dfs:
            no_data.append(symbol)
            continue
        _df5, _df75, df15 = dfs[symbol]
        if df15 is None:
            no_data.append(symbol)
            continue
        bias = get_opening_candle_bias(df15, symbol)
        if bias == "BULLISH":
            bullish.append(symbol)
        elif bias == "BEARISH":
            bearish.append(symbol)
        # bias is None (genuinely neutral candle) -> on neither list,
        # not counted as no_data either.

    return sorted(bullish), sorted(bearish), sorted(no_data)


def run_opening_bias_report(now_ist):
    """
    NEW (per request) — SCAN_MODE=opening_bias_report entry point.
    Meant to run shortly after 09:30 IST (once today's first 15-min
    candle has actually closed) via its own cron trigger in scan.yml.
    Sends ONE consolidated Telegram message listing every F&O stock
    with Open==Low and every one with Open==High — always sends
    something (even an empty-both-lists message), same always-report
    principle as run_ema_cross_report.

    STARTUP DELAY (per request): the cron trigger itself fires at
    exactly 09:30:00 IST, but the 09:15-09:30 candle isn't reliably
    available from Upstox the instant it closes -- there's typically a
    short broker-side finalization lag. Sleeping 15s here (so the
    actual fetch happens ~09:30:15) gives that lag room without having
    to fiddle with cron-job.org's schedule, which only supports
    minute-level granularity anyway (no ":15" second offset). now_ist
    is deliberately re-read via _now_ist() AFTER the sleep, so the
    session-gate check right below and the report's own printed
    timestamp both reflect the real (post-sleep) clock time, not the
    stale pre-sleep one passed in from run().
    """
    time.sleep(15)
    now_ist = _now_ist()

    if not _in_stock_session(now_ist):
        print(f"Outside stock session ({now_ist.strftime('%H:%M')} IST) — opening-bias report skipping.", flush=True)
        return
    bullish, bearish, no_data = build_todays_opening_bias_list(now_ist)
    send_opening_bias_report(bullish, bearish, no_data, now_ist)


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
    Returns (symbol, df5, df75, df15) — df5 is kept for today's session
    OHLCV/VWAP aggregation (build_todays_daily_bar) only; the standalone
    index (NIFTY 50/BANK/SENSEX) EMA9/EMA20 cross alert now runs on
    df15 instead (CHANGED from 5-min to 15-min, per request). df75 is
    the primary 75-min signal timeframe for stocks/commodities/cash;
    df15 also doubles as the informational trend context attached to
    75-min alerts (get_3min_trend_info). (3-min/df3 was removed
    2026-08-13 — it had become dead weight, resampled every run for
    every instrument but no longer read anywhere.)

    df75 combines cached PRE-TODAY 1-min history (hist_candles, from
    build_hist1min_cache — enough days to warm up EMA9/EMA20) with
    today's fresh 1-min data, deduped and resampled together. df15 is
    now built from that same combined data (fixed 2026-08-13 — see
    below). Without hist_candles, both fall back to today-only data —
    which will almost never have enough bars to warm up EMA9/EMA20, so
    the 75-min/15-min features simply stay dormant for that symbol
    until history is available, never a crash.

    Raises on a genuine fetch failure (after retries are exhausted in
    fetch_1min_candles) so the caller (fetch_all) can distinguish
    "failed to fetch" from "fetched fine, just not enough history yet".
    """
    raw = fetch_1min_candles(instrument_key)
    if raw is None:
        return symbol, None, None, None
    # BUG FIX (found via opening_bias_report showing mass "No data" at
    # 09:31 IST, even after the len(raw)<30-without-hist_candles fix
    # below): this used to be `len(raw) < 30`, which rejected EVERY
    # symbol whenever today's session was under ~30 minutes old -- at
    # 09:31, today's raw candles are only ~16 rows (09:15-09:31). But
    # opening_bias_report calls fetch_all() WITHOUT a hist_cache at
    # all (it only needs today's very first 15-min candle, not
    # multi-day EMA warmup), so the "only skip the len check when
    # hist_candles is available" version still rejected everything for
    # that caller. A 15-min candle only needs 15 one-min bars, so 15
    # (not 30, not conditional on hist_candles) is the real minimum --
    # this now works correctly whether or not hist_candles was passed.
    if len(raw) < 15:
        return symbol, None, None, None
    df5 = resample_5min(raw)
    df5 = drop_unclosed_candle(df5, now_ist, candle_minutes=5)

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

    # df15 (15-min informational trend context, get_3min_trend_info)
    # is built from the SAME combined multi-day history as df75, not
    # just today's candles. Fixed 2026-08-13: EMA20 warmup needs 22
    # bars -- on today-only data that's 22 x 15min = 330 minutes,
    # i.e. almost the entire trading session, so the "15-min:" line
    # on an alert was silently missing for most of the day (only
    # appearing near the close) until it had enough history. Using
    # combined_75 here gives it the same multi-day warmup df75 already
    # gets, so it's available from the first alert of the day.
    df15 = resample_15min(combined_75)
    df15 = drop_unclosed_candle(df15, now_ist, candle_minutes=15)

    df75 = resample_75min(combined_75)
    df75 = drop_unclosed_candle(df75, now_ist, candle_minutes=75)
    return symbol, df5, df75, df15


def fetch_all(watchlist, now_ist, workers, hist_cache=None):
    """
    Fetches + resamples 1-min candles -> 5-min/15-min/75-min for every
    instrument in watchlist, concurrently (up to `workers` threads).

    hist_cache: {symbol: [candle, ...]} of cached pre-today 1-min
    history (see build_hist1min_cache), passed through to each
    instrument's 75-min resample so EMA9/EMA20 there is properly warmed
    up. Optional — omitting it just means 75-min features stay dormant.

    Returns (dfs, failed_symbols):
      dfs            -- {symbol: (df5, df75, df15)} for every instrument
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
                sym, df5, df75, df15 = future.result()
                if df5 is not None:
                    dfs[sym] = (df5, df75, df15)
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

    # Momentum (price vs last 4 weeks' high) / Volume Spike (daily
    # volume vs 5 trading days ago) — see build_momentum_volume_data.
    # Cached on disk, same once-per-day pattern as pivots above.
    momentum_volume = build_momentum_volume_data(watchlist)

    # Previous trading day's NSE delivery % per stock (see
    # delivery_data.py). Cached on disk, so this only actually
    # downloads the NSE bhavcopy once per calendar day, not once per
    # run. {} on failure — never blocks the scan, just means no
    # "Delivery %" line on alerts for the day.
    delivery_map = delivery_data.get_delivery_data(now_ist.date())

    saved_state = state.load_state()
    alerts_sent = 0
    daily_score_report_hits = []
    # Consolidation Breakout — LIVE hits collected across this whole
    # run (added, per request — "je single alert ache ota summary kore
    # dao"), sent as ONE batched message at the end instead of one
    # message per stock. See send_consolidation_breakout_summary.
    consolidation_live_hits = []

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

    for symbol, (df5, df75, df15) in dfs.items():
        try:
            # De-overlap fix (per request, to guarantee no duplicate
            # index alerts): NIFTY 50/BANK/SENSEX are fully owned by
            # the dedicated index-only cron (index_only=True). Without
            # this skip, the separate "full" (75-min stock/commodity)
            # cron ALSO evaluates these same 3 symbols every run (since
            # build_watchlist() always includes them), so two
            # independently-scheduled runs could both see an
            # un-alerted candle at nearly the same moment and both fire
            # before either commits alert_state.json — a real (if rare)
            # double-send window. Skipping indices here whenever this
            # is NOT the dedicated index_only run closes that window:
            # each index candle is now only ever evaluated by ONE cron
            # trigger, never two.
            if symbol in index_symbols and not index_only:
                continue

            # Consolidation Breakout — LIVE (added, per request:
            # "emergency alert e add kora jay na", i.e. put it in the
            # main real-time scan too, not just the once/day
            # standalone run_consolidation_breakout_scan). Uses the
            # SAME once-per-day cached daily history as the
            # Momentum/Volume Spike tags above (see
            # build_momentum_volume_data / compute_consolidation_window)
            # — zero extra API calls — checked against TODAY'S CURRENT
            # price/volume-so-far every scan cycle instead of waiting
            # for today's close. See strategy.check_consolidation_
            # breakout_live. Runs independently of the EMA-cross signal
            # below (no cross needs to have happened), same spirit as
            # the Trendline Break check further down.
            if config.CONSOLIDATION_BREAKOUT_LIVE_ENABLED and df15 is not None and len(df15) > 0 and df5 is not None and len(df5) > 0:
                cw = momentum_volume.get(symbol, {}).get("consolidation_window")
                if cw:
                    current_close = float(df15.iloc[-1]["close"])
                    current_volume_so_far = float(df5["volume"].sum())
                    live_signal = check_consolidation_breakout_live(symbol, current_close, current_volume_so_far, cw)
                    if live_signal is not None:
                        live_signal["date"] = now_ist.date().isoformat()
                        cb_state_symbol = f"{symbol}::CONSOLIDATION_BREAKOUT_LIVE"
                        if not state.already_alerted(saved_state, cb_state_symbol, live_signal["direction"], live_signal["date"]):
                            live_signal["chart_link"] = build_chart_link(symbol, "15-min")
                            state.mark_alerted(saved_state, cb_state_symbol, live_signal["direction"], live_signal["date"])
                            consolidation_live_hits.append(live_signal)

            levels = pivots.get(symbol)
            r3 = levels["r3"] if levels else None
            s3 = levels["s3"] if levels else None
            prev_close = levels.get("prev_close") if levels else None

            # ALERTING SIGNAL. Indices and stocks/commodities now follow
            # completely separate rules:
            #   - Indices (NIFTY 50, NIFTY BANK, SENSEX): a PURE 15-min
            #     EMA9/20 crossover — no EMA50 trend requirement, no
            #     75-min gate at all. check_signals() runs directly on
            #     df15 with require_trend_confirmation=False. Checked by
            #     a dedicated cron trigger (index_only=True), fully
            #     separate from the 75-min stock/commodity runs.
            #     (CHANGED from 5-min to 15-min, per request — df15 is
            #     the same 15-min resample already computed for
            #     stocks/commodities in _fetch_and_resample_one, so
            #     this reuses it rather than fetching anything extra.)
            #   - Stocks/commodities: PRIMARY/ALERTING timeframe is
            #     whichever config.PRIMARY_TIMEFRAME currently selects
            #     ("15min" or "75min") — flip that ONE setting to
            #     switch which timeframe fires alerts; nothing else in
            #     this file needs to change. Trend/strong-candle/volume
            #     gating on the primary signal is likewise controlled
            #     by config.REQUIRE_TREND_CONFIRMATION /
            #     REQUIRE_STRONG_CANDLE / REQUIRE_VOLUME_CONFIRMATION.
            if symbol in index_symbols:
                if df15 is None:
                    continue
                signals = check_signals(
                    df15, symbol, r3=r3, s3=s3,
                    lookback=config.INDEX_ALERT_LOOKBACK_CANDLES,
                    require_trend_confirmation=False,
                    prev_close=prev_close,
                    require_macd_cross=config.REQUIRE_MACD_CROSS,
                    require_rsi_confirmation=config.REQUIRE_RSI_CONFIRMATION,
                )
                for sig in signals:
                    sig["timeframe"] = "15-min"
                info_df, info_label = None, None

                # Trendline Break (added, per request) — standalone,
                # does NOT require an EMA cross; checked every run on
                # the same df15 already used above, on the latest
                # closed candle only (see strategy.check_trendline_scan).
                # Gated by config.ENABLE_TRENDLINE_ALERTS (per request
                # — these are now off entirely; flip that flag back to
                # True to re-enable without touching this code).
                if config.ENABLE_TRENDLINE_ALERTS:
                    tl_signal = check_trendline_scan(df15, symbol)
                    if tl_signal is not None:
                        tl_state_symbol = f"{symbol}::TRENDLINE::{tl_signal['direction']}"
                        if not state.in_cooldown(saved_state, tl_state_symbol, tl_signal["direction"], tl_signal["candle_time"], config.TRENDLINE_COOLDOWN_MINUTES):
                            tl_signal["chart_link"] = build_chart_link(symbol, "15-min")
                            state.mark_alerted(saved_state, tl_state_symbol, tl_signal["direction"], tl_signal["candle_time"])
                            try:
                                send_trendline_alert(tl_signal)
                                alerts_sent += 1
                            except Exception as e:
                                print(f"send_trendline_alert failed for {symbol}: {e}")
            else:
                if config.PRIMARY_TIMEFRAME == "15min":
                    primary_df, primary_label = df15, "15-min"
                    primary_ema_fast, primary_ema_slow = config.EMA_FAST, config.EMA_SLOW
                    info_df, info_label = df75, "75-min"
                else:
                    primary_df, primary_label = df75, "75-min"
                    primary_ema_fast, primary_ema_slow = config.EMA_FAST, config.PRIMARY_EMA_SLOW_75MIN
                    info_df, info_label = df15, "15-min"

                if primary_df is None:
                    continue
                signals = check_signals(
                    primary_df, symbol, r3=r3, s3=s3,
                    lookback=config.PRIMARY_LOOKBACK_CANDLES,
                    require_trend_confirmation=config.REQUIRE_TREND_CONFIRMATION,
                    prev_close=prev_close,
                    ema_fast=primary_ema_fast,
                    ema_slow=primary_ema_slow,
                    require_volume_increase=config.REQUIRE_VOLUME_CONFIRMATION,
                    require_strong_candle=config.REQUIRE_STRONG_CANDLE,
                    require_macd_cross=config.REQUIRE_MACD_CROSS,
                    require_rsi_confirmation=config.REQUIRE_RSI_CONFIRMATION,
                )
                for sig in signals:
                    sig["timeframe"] = primary_label

                # Trendline Break (added, per request) — standalone,
                # does NOT require an EMA cross; checked every run on
                # the same primary_df already fetched above (whichever
                # timeframe config.PRIMARY_TIMEFRAME currently selects),
                # on the latest closed candle only (see
                # strategy.check_trendline_scan). Gated by
                # config.ENABLE_TRENDLINE_ALERTS — see matching comment
                # in the 15-min branch above.
                if config.ENABLE_TRENDLINE_ALERTS:
                    tl_signal = check_trendline_scan(primary_df, symbol)
                    if tl_signal is not None:
                        tl_state_symbol = f"{symbol}::TRENDLINE::{tl_signal['direction']}"
                        if not state.in_cooldown(saved_state, tl_state_symbol, tl_signal["direction"], tl_signal["candle_time"], config.TRENDLINE_COOLDOWN_MINUTES):
                            tl_signal["chart_link"] = build_chart_link(symbol, primary_label)
                            state.mark_alerted(saved_state, tl_state_symbol, tl_signal["direction"], tl_signal["candle_time"])
                            try:
                                send_trendline_alert(tl_signal)
                                alerts_sent += 1
                            except Exception as e:
                                print(f"send_trendline_alert failed for {symbol}: {e}")

                # "Perfect Daily Score" F&O report (added, per request)
                # — checked on EVERY F&O stock's latest closed
                # primary_df candle, completely independent of
                # `signals` above (an EMA cross is NOT required).
                # Reuses primary_df already in memory — no extra
                # fetch. See strategy.compute_daily_score_scan and
                # telegram_notifier.send_daily_score_report. The
                # actual send (with change-detection dedup) happens
                # once after this whole per-symbol loop finishes, not
                # here — this just collects candidates.
                if config.DAILY_SCORE_REPORT_ENABLED and symbol.upper() in fno_underlyings:
                    ds_hit = compute_daily_score_scan(primary_df, symbol)
                    if ds_hit is not None and ds_hit["score"] >= config.DAILY_SCORE_REPORT_MIN_SCORE:
                        # Chart link (added, per request) — same
                        # TradingView 15-min deep link every other
                        # alert type already gets (see build_chart_link
                        # above / send_alert / send_breakout_alert /
                        # send_trendline_alert), just wired in here too
                        # so each row in the Daily Score report is
                        # tappable.
                        ds_hit["chart_link"] = build_chart_link(symbol)
                        ds_hit["is_fno"] = True
                        daily_score_report_hits.append(ds_hit)

            if not signals:
                continue

            # Informational context block, on whichever timeframe is
            # NOT primary right now (see info_df/info_label above).
            # get_3min_trend_info() returns cross_time (the exact
            # candle timestamp of the last cross, if any within the
            # lookback window) — telegram_notifier shows this so the
            # line carries a real timestamp, not just "N candles ago".
            # Only meaningful under a stock/commodity alert — indices
            # have nothing extra to attach.
            info3 = None
            if symbol not in index_symbols and info_df is not None:
                info3 = get_3min_trend_info(info_df, symbol)

            # Opening 15-min candle bias (added, per request) — see
            # strategy.get_opening_candle_bias. Computed once per
            # symbol per run on df15 (always available regardless of
            # which timeframe the alert itself fired on) and attached
            # to every signal below.
            opening_bias = get_opening_candle_bias(df15, symbol)

            # BUG FIX (found 2026-08-27): this `mv` lookup was missing
            # entirely from run_fo_scan -- every signal that reached
            # the `if mv is not None:` block below (momentum/volume
            # spike attachment) was crashing with NameError, silently
            # swallowed by the per-symbol try/except at the bottom of
            # this loop (`Error on {symbol}: name 'mv' is not
            # defined` in the logs), so NO F&O/commodity/index
            # EMA-cross alert could ever actually get sent.
            # run_nifty500_scan already had this line; it was only
            # missing here. Moved above the per-signal loop (not
            # per-signal) since it's a cheap dict lookup keyed only on
            # symbol -- matches run_nifty500_scan's placement.
            mv = momentum_volume.get(symbol)

            for signal in signals:
                if state.already_alerted(saved_state, symbol, signal["direction"], signal["candle_time"]):
                    continue

                if info3 is not None:
                    signal["trend_3min"] = info3
                    signal["info_timeframe_label"] = info_label

                # Always attach (even when None/"neutral") so every
                # alert carries this line — see telegram_notifier for
                # how a None/neutral reading is displayed.
                signal["opening_candle_bias"] = opening_bias

                # Momentum / Volume Spike / EMA50-200 / multi-month
                # highs — moved UP (per request, 2026-08-27) to before
                # compute_intraday_checklist(), since the checklist
                # now uses signal["momentum"] and signal["volume_spike"]
                # (see the checklist's own docstring for why). Previously
                # this ran after the checklist, so those two fields
                # were always None/missing at checklist time.
                signal["chart_link"] = build_chart_link(symbol, signal.get("timeframe"))
                if mv is not None:
                    signal["momentum"] = signal["close"] > mv["four_week_high_close"]
                    signal["four_week_high_close"] = mv["four_week_high_close"]
                    if mv["volume_5day_ago"] is not None:
                        signal["volume_spike"] = mv["prev_day_volume"] > mv["volume_5day_ago"]
                        # Volume Spike % (added, per request, 2026-08-27
                        # — alert gate case 6): numeric version of the
                        # same comparison, for the ">500%" gate check
                        # in strategy.passes_alert_gate. Only computed
                        # when volume_5day_ago > 0 (can't divide by 0).
                        if mv["volume_5day_ago"] > 0:
                            signal["volume_spike_pct"] = (
                                (mv["prev_day_volume"] - mv["volume_5day_ago"])
                                / mv["volume_5day_ago"] * 100
                            )
                    if mv.get("ema_cross") is not None:
                        signal["ema_cross"] = mv["ema_cross"]
                    if mv.get("multi_month_highs"):
                        signal["multi_month_highs"] = mv["multi_month_highs"]
                        signal["near_high"] = compute_near_high_score(signal)

                # 15-Minute Intraday Trade Checklist (added, per
                # request) — purpose-built for entry timing. Needs
                # opening_candle_bias, opening_range_breakout, momentum
                # and volume_spike (all set above by this point) —
                # must be present before this call.
                signal["intraday_checklist"] = compute_intraday_checklist(signal)

                # 1st 15-min Buy/Sell volume ESTIMATE (added, per
                # request) — see strategy.get_opening_candle_buy_sell_estimate
                # docstring for the approximation used.
                signal["opening_buy_sell"] = get_opening_candle_buy_sell_estimate(df15, symbol)

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
                    # "Smart Money Entry" 🐋 (added) — purely
                    # informational, like everything else on this
                    # alert now (OI buildup included, per request) —
                    # never blocks or filters anything. Just a
                    # 0-7 score + a plain-language reasons line, built
                    # entirely from fields already attached to this
                    # signal above — no extra fetch, no extra API call.
                    # See strategy.compute_smart_money_signal.
                    smart_money = compute_smart_money_signal(signal)
                    if smart_money:
                        signal["smart_money"] = smart_money

                    # Volume Spike gate (CHANGED, per request — was
                    # informational-only before). Only blocks on an
                    # explicit False; None (data missing/not fetched
                    # yet for this symbol) still passes through, so a
                    # daily-history fetch hiccup never silently eats a
                    # real signal. See config.REQUIRE_VOLUME_SPIKE.
                    if config.REQUIRE_VOLUME_SPIKE and signal.get("volume_spike") is False:
                        continue

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

                # Trading Score (added, per request — "sob miliye ekta
                # trading score generate koro") — ONE combined /10
                # score rolling up Buy/Sell Score + Daily Score +
                # Smart Money (when present). Computed BEFORE the
                # Alert Gate now (moved, 2026-08-27), since the gate
                # itself needs it — see strategy.compute_trading_score.
                signal["trading_score"] = compute_trading_score(signal)

                # Alert Gate (SIMPLIFIED, per request, 2026-08-28) —
                # single condition: Trading Score GOOD or STRONG
                # (>= config.QUALITY_GATE_MIN_TRADING_SCORE). See
                # config.py's "Alert Gate" comment and
                # strategy.passes_alert_gate. Not marked alerted on
                # failure, so it's re-checked next run.
                if config.QUALITY_GATE_ENABLED:
                    gate_passed, gate_reasons = passes_alert_gate(signal)
                    if not gate_passed:
                        continue
                    signal["alert_gate_reasons"] = gate_reasons

                # Same-direction cooldown (added, per request) — blocks
                # only if this exact (symbol, direction) already
                # alerted within config.SAME_DIRECTION_COOLDOWN_MINUTES,
                # even on a genuinely new/different candle. This is
                # separate from state.already_alerted() above, which
                # only catches the exact-same-candle case.
                if state.in_cooldown(saved_state, symbol, signal["direction"], signal["candle_time"], config.SAME_DIRECTION_COOLDOWN_MINUTES):
                    continue

                # Mark BEFORE sending: if send_alert() raises after the
                # Telegram API call already succeeded (e.g. a parsing
                # or logging error on our side, post-send), the alert
                # still went out, so we must not leave this candle
                # unmarked -- an unmarked candle gets re-alerted every
                # future run until fixed. Marking first means a
                # send failure costs at most one missed alert, never
                # an infinite duplicate loop.
                state.mark_alerted(saved_state, symbol, signal["direction"], signal["candle_time"])
                try:
                    send_alert(signal)
                    alerts_sent += 1
                except Exception as e:
                    import traceback
                    print(f"send_alert failed for {symbol} (state already marked, won't re-send): {e}")
                    traceback.print_exc()

        except Exception as e:
            print(f"Error on {symbol}: {e}")

    # ---- 15-min: informational-only (no separate alert) ----
    # There is no standalone 15-min alert. 15-min context (EMA9/20
    # bias, candles since last 15-min cross, and how close to a cross
    # right now) is attached to every 75-min alert above via
    # signal["trend_3min"] (set from get_3min_trend_info(df15, ...) —
    # the key name "trend_3min" is legacy and kept as-is so
    # telegram_notifier.py doesn't need touching), so you can see the
    # shorter-term picture without a second, separate ping. (3-min/df3
    # itself was removed 2026-08-13 — see _fetch_and_resample_one.)

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
    for symbol, (df5, df75, df15) in dfs.items():
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

    # "Perfect Daily Score" F&O report (added, per request) — combined
    # FNO+CASH send happens once in run(), after run_nifty500_scan also
    # finishes below — see run() for the actual sort/dedup/send logic.
    # This function only collects its (F&O-tagged) share.

    # Consolidation Breakout — LIVE, batched send (added, per request —
    # "je single alert ache ota summary kore dao"). Every hit collected
    # during the loop above goes out as ONE message, not one per stock.
    if consolidation_live_hits:
        send_consolidation_breakout_summary(consolidation_live_hits, now_ist)
        alerts_sent += len(consolidation_live_hits)

    state.save_state(saved_state)
    if failed_symbols:
        print(f"{len(failed_symbols)} instrument(s) failed to fetch this run: {failed_symbols}")
    print(f"Done. {alerts_sent} alert(s) sent.")
    return alerts_sent, failed_symbols, len(watchlist), daily_score_report_hits


# ---------------------------------------------------------------------
# Nifty 500 cash-stock scan (added)
# ---------------------------------------------------------------------
# Same signal logic/conditions as the F&O 75-min flow above — EMA cross
# + mandatory EMA50 trend agreement on df75, with df15 shown as
# informational 15-min context — just on the full Nifty 500 constituent
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
        return 0, [], 0, []

    print(f"Scanning {len(watchlist)} Nifty 500 cash stocks (EMA{config.NIFTY500_EMA_FAST}/{config.NIFTY500_EMA_SLOW})...")

    pivots = build_pivot_levels(watchlist)

    # Momentum (price vs last 4 weeks' high) / Volume Spike (daily
    # volume vs 5 trading days ago) — see build_momentum_volume_data.
    momentum_volume = build_momentum_volume_data(watchlist)

    delivery_map = delivery_data.get_delivery_data(now_ist.date())
    saved_state = state.load_state()
    alerts_sent = 0
    daily_score_report_hits = []
    # Consolidation Breakout — LIVE hits collected across this whole
    # run (see matching comment in run_fo_scan above), sent as ONE
    # batched message at the end.
    consolidation_live_hits = []

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

    for symbol, (df5, df75, df15) in dfs.items():
        # Skip F&O-underlying symbols entirely here -- they're already
        # scanned (both directions) by run_fo_scan() above, under a
        # separate dedup key ("SYMBOL" vs this scan's "SYMBOL::N500").
        # Without this, any stock that's in BOTH the F&O watchlist AND
        # the Nifty 500 universe (most large-caps) would fire the SAME
        # crossover twice -- once from each scan -- since neither scan's
        # dedup state knows about the other's. (2026-08-18 fix)
        if symbol.upper() in fno_underlyings:
            continue
        try:
            # Consolidation Breakout — LIVE (added, per request — see
            # matching comment in run_fo_scan above). Uses the SAME
            # once-per-day cached daily history as the Momentum/Volume
            # Spike tags above (build_momentum_volume_data), zero extra
            # API calls. Same dedup-key suffix convention as this
            # scan's own EMA-cross state ("::N500" style) — here
            # "::CONSOLIDATION_BREAKOUT_LIVE::N500" — so it never
            # collides with run_fo_scan's own state key for the same
            # symbol.
            if config.CONSOLIDATION_BREAKOUT_LIVE_ENABLED and df15 is not None and len(df15) > 0 and df5 is not None and len(df5) > 0:
                cw = momentum_volume.get(symbol, {}).get("consolidation_window")
                if cw:
                    current_close = float(df15.iloc[-1]["close"])
                    current_volume_so_far = float(df5["volume"].sum())
                    live_signal = check_consolidation_breakout_live(symbol, current_close, current_volume_so_far, cw)
                    if live_signal is not None:
                        live_signal["date"] = now_ist.date().isoformat()
                        cb_state_symbol = f"{symbol}::CONSOLIDATION_BREAKOUT_LIVE::N500"
                        if not state.already_alerted(saved_state, cb_state_symbol, live_signal["direction"], live_signal["date"]):
                            live_signal["chart_link"] = build_chart_link(symbol, "15-min")
                            state.mark_alerted(saved_state, cb_state_symbol, live_signal["direction"], live_signal["date"])
                            consolidation_live_hits.append(live_signal)

            levels = pivots.get(symbol)
            r3 = levels["r3"] if levels else None
            s3 = levels["s3"] if levels else None
            prev_close = levels.get("prev_close") if levels else None

            # PRIMARY/ALERTING timeframe here follows config.PRIMARY_TIMEFRAME
            # too, same as run_fo_scan above — only the EMA pair differs
            # (9/21 for Nifty 500 cash, vs 9/20 for F&O). Note: on
            # "15min", the primary EMA pair is still NIFTY500_EMA_FAST/
            # SLOW (9/21) — only the 75-min branch has a fixed EMA9/50
            # pairing (PRIMARY_EMA_SLOW_75MIN), same as run_fo_scan.
            if config.PRIMARY_TIMEFRAME == "15min":
                primary_df, primary_label = df15, "15-min"
                primary_ema_fast, primary_ema_slow = config.NIFTY500_EMA_FAST, config.NIFTY500_EMA_SLOW
                info_df, info_label = df75, "75-min"
            else:
                primary_df, primary_label = df75, "75-min"
                primary_ema_fast, primary_ema_slow = config.NIFTY500_EMA_FAST, config.PRIMARY_EMA_SLOW_75MIN
                info_df, info_label = df15, "15-min"

            if primary_df is None:
                continue
            signals = check_signals(
                primary_df, symbol, r3=r3, s3=s3,
                lookback=config.PRIMARY_LOOKBACK_CANDLES,
                require_trend_confirmation=config.REQUIRE_TREND_CONFIRMATION,
                prev_close=prev_close,
                ema_fast=primary_ema_fast,
                ema_slow=primary_ema_slow,
                require_volume_increase=config.REQUIRE_VOLUME_CONFIRMATION,
                require_strong_candle=config.REQUIRE_STRONG_CANDLE,
                require_macd_cross=config.REQUIRE_MACD_CROSS,
                require_rsi_confirmation=config.REQUIRE_RSI_CONFIRMATION,
            )
            for sig in signals:
                sig["timeframe"] = primary_label

            # Nifty 500 cash-stock scan: BEARISH alerts are not wanted
            # here (cash-only stocks, no shorting use case for most
            # subscribers) — only BULLISH crossovers are sent. This
            # does NOT affect the F&O scan above (run_fo_scan), which
            # still sends both directions.
            signals = [sig for sig in signals if sig["direction"] == "BULLISH"]

            # Trendline Break (added, per request) — standalone, does
            # NOT require an EMA cross, and NOT restricted to BULLISH
            # only (unlike the EMA-cross filter just above) — a
            # descending-resistance break is inherently bullish and an
            # ascending-support break is inherently bearish, so both
            # directions are meaningful trendline signals in their own
            # right. Checked on the same primary_df already fetched
            # above, latest closed candle only (see
            # strategy.check_trendline_scan). Safe from double-firing
            # with run_fo_scan's own trendline check since F&O symbols
            # are skipped entirely at the top of this loop. Gated by
            # config.ENABLE_TRENDLINE_ALERTS — see matching comment in
            # run_fo_scan above.
            if config.ENABLE_TRENDLINE_ALERTS:
                tl_signal = check_trendline_scan(primary_df, symbol)
                if tl_signal is not None:
                    tl_state_symbol = f"{symbol}::TRENDLINE::{tl_signal['direction']}"
                    if not state.in_cooldown(saved_state, tl_state_symbol, tl_signal["direction"], tl_signal["candle_time"], config.TRENDLINE_COOLDOWN_MINUTES):
                        tl_signal["chart_link"] = build_chart_link(symbol, primary_label)
                        state.mark_alerted(saved_state, tl_state_symbol, tl_signal["direction"], tl_signal["candle_time"])
                        try:
                            send_trendline_alert(tl_signal)
                            alerts_sent += 1
                        except Exception as e:
                            print(f"send_trendline_alert failed for {symbol}: {e}")

            # "Perfect Daily Score" report (added, per request) — CASH
            # side. Every symbol reaching this loop body is guaranteed
            # NOT in fno_underlyings (see the skip at the top of this
            # loop), so every hit collected here is a genuine cash-only
            # stock — tagged accordingly below, no dedup needed against
            # run_fo_scan's F&O-tagged hits. See the matching comment
            # in run_fo_scan above for the full mechanism (this just
            # collects; the combined FNO+CASH send happens once, after
            # both scans finish, in run()).
            if config.DAILY_SCORE_REPORT_ENABLED:
                ds_hit = compute_daily_score_scan(primary_df, symbol)
                if ds_hit is not None and ds_hit["score"] >= config.DAILY_SCORE_REPORT_MIN_SCORE:
                    ds_hit["chart_link"] = build_chart_link(symbol)
                    ds_hit["is_fno"] = False
                    daily_score_report_hits.append(ds_hit)

            if not signals:
                continue

            # Informational context block, on whichever timeframe is
            # NOT primary right now — real cross timestamp via
            # cross_time, same as run_fo_scan above.
            info3 = None
            if info_df is not None:
                info3 = get_3min_trend_info(
                    info_df, symbol,
                    ema_fast=config.NIFTY500_EMA_FAST,
                    ema_slow=config.NIFTY500_EMA_SLOW,
                )

            state_symbol = f"{symbol}::N500"

            # Opening 15-min candle bias (added, per request) — same
            # as run_fo_scan, see strategy.get_opening_candle_bias.
            opening_bias = get_opening_candle_bias(df15, symbol)

            for signal in signals:
                if state.already_alerted(saved_state, state_symbol, signal["direction"], signal["candle_time"]):
                    continue

                if info3 is not None:
                    signal["trend_3min"] = info3
                    signal["info_timeframe_label"] = info_label

                # Always attach (even when None/"neutral") — see
                # run_fo_scan above / telegram_notifier for display.
                signal["opening_candle_bias"] = opening_bias

                # Momentum / Volume Spike — moved UP (see matching
                # comment in run_fo_scan above) to before the checklist,
                # since it now uses signal["momentum"]/["volume_spike"].
                signal["chart_link"] = build_chart_link(symbol, signal.get("timeframe"))
                mv = momentum_volume.get(symbol)
                if mv is not None:
                    signal["momentum"] = signal["close"] > mv["four_week_high_close"]
                    signal["four_week_high_close"] = mv["four_week_high_close"]
                    if mv["volume_5day_ago"] is not None:
                        signal["volume_spike"] = mv["prev_day_volume"] > mv["volume_5day_ago"]
                        # Volume Spike % — see the matching comment in
                        # run_fo_scan above (alert gate case 6).
                        if mv["volume_5day_ago"] > 0:
                            signal["volume_spike_pct"] = (
                                (mv["prev_day_volume"] - mv["volume_5day_ago"])
                                / mv["volume_5day_ago"] * 100
                            )
                    if mv.get("ema_cross") is not None:
                        signal["ema_cross"] = mv["ema_cross"]
                    if mv.get("multi_month_highs"):
                        signal["multi_month_highs"] = mv["multi_month_highs"]
                        signal["near_high"] = compute_near_high_score(signal)

                # 15-Minute Intraday Trade Checklist — see the matching
                # comment in run_fo_scan above.
                signal["intraday_checklist"] = compute_intraday_checklist(signal)

                # 1st 15-min Buy/Sell volume ESTIMATE — see the
                # matching comment in run_fo_scan above.
                signal["opening_buy_sell"] = get_opening_candle_buy_sell_estimate(df15, symbol)

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

                # "Smart Money Entry" 🐋 (added) — informational only,
                # see the matching comment in run_fo_scan above. Note:
                # this scan never fetches OI buildup (cash-only Nifty
                # 500 stocks), so that one dimension just won't
                # contribute a point here — everything else still can.
                smart_money = compute_smart_money_signal(signal)
                if smart_money:
                    signal["smart_money"] = smart_money

                # Volume Spike gate — see the matching comment in
                # run_fo_scan() above.
                if config.REQUIRE_VOLUME_SPIKE and signal.get("volume_spike") is False:
                    continue

                if config.CONFLUENCE_FILTER_ENABLED and not passes_confluence_filter(signal):
                    continue
                signal["confluence_passed"] = True

                # Trading Score — see the matching comment in
                # run_fo_scan() above.
                signal["trading_score"] = compute_trading_score(signal)

                # Alert Gate (SIMPLIFIED, per request, 2026-08-28) —
                # see the matching comment in run_fo_scan() above.
                if config.QUALITY_GATE_ENABLED:
                    gate_passed, gate_reasons = passes_alert_gate(signal)
                    if not gate_passed:
                        continue
                    signal["alert_gate_reasons"] = gate_reasons

                # Same-direction cooldown — see the matching comment in
                # run_fo_scan() above.
                if state.in_cooldown(saved_state, state_symbol, signal["direction"], signal["candle_time"], config.SAME_DIRECTION_COOLDOWN_MINUTES):
                    continue

                # Mark BEFORE sending -- see the matching comment in
                # run_fo_scan() above for why.
                state.mark_alerted(saved_state, state_symbol, signal["direction"], signal["candle_time"])
                try:
                    send_alert(signal)
                    alerts_sent += 1
                except Exception as e:
                    import traceback
                    print(f"send_alert failed for {symbol} (state already marked, won't re-send): {e}")
                    traceback.print_exc()

        except Exception as e:
            print(f"Error on {symbol} (Nifty 500 scan): {e}")

    # Consolidation Breakout — LIVE, batched send (see matching comment
    # in run_fo_scan above).
    if consolidation_live_hits:
        send_consolidation_breakout_summary(consolidation_live_hits, now_ist)
        alerts_sent += len(consolidation_live_hits)

    state.save_state(saved_state)
    if failed_symbols:
        print(f"{len(failed_symbols)} Nifty 500 instrument(s) failed to fetch this run: {failed_symbols}")
    print(f"Nifty 500 scan done. {alerts_sent} alert(s) sent.")
    return alerts_sent, failed_symbols, len(watchlist), daily_score_report_hits


def build_todays_daily_bar(df5, today_date_str):
    """
    Aggregates today's own 5-min intraday candles (df5, already fetched
    by fetch_all for the current session) into a single daily-candle-
    shaped dict — {"date","open","high","low","close","volume","vwap"}
    — for run_breakout_scan. This is "today's row" that combines with
    fetch_daily_history's pre-today history to give the scan a
    complete, up-to-the-moment daily series without a separate API
    call. vwap is today's cumulative session VWAP (see
    strategy.compute_session_vwap) — None if df5 is empty/all-zero-
    volume, in which case Row 13 (Close > VWAP) will just fail
    gracefully for that symbol.

    Returns None if df5 is None or empty (nothing to aggregate yet —
    e.g. very early in the session, or fetch failed for this symbol).
    """
    if df5 is None or len(df5) == 0:
        return None
    return {
        "date": today_date_str,
        "open": float(df5.iloc[0]["open"]),
        "high": float(df5["high"].max()),
        "low": float(df5["low"].min()),
        "close": float(df5.iloc[-1]["close"]),
        "volume": float(df5["volume"].sum()),
        "vwap": compute_session_vwap(df5),
    }


def run_breakout_scan(now_ist):
    """
    Standalone daily breakout screener (added 2026-08-18) — the
    12-condition Chartink-style scan (Row 2/Market Cap omitted, see
    chat) from strategy.check_breakout_scan(). Own SCAN_MODE
    ("breakout_scan"), own cron trigger — meant to run once per day,
    at/after market close (so today's daily candle is fully formed).
    Runs across the same Nifty 500 cash universe as run_nifty500_scan,
    reusing build_nifty500_watchlist. Sends at most ONE alert per
    symbol per day (dedup key includes today's date, not a candle
    time — see state_symbol below).
    """
    watchlist = build_nifty500_watchlist(now_ist)
    if not watchlist:
        print("Breakout scan: empty watchlist (outside session or list unavailable) — skipping.")
        return 0, [], 0

    print(f"Breakout scan: scanning {len(watchlist)} Nifty 500 stocks...")

    saved_state = state.load_state()
    alerts_sent = 0
    today_str = now_ist.date().isoformat()

    hist_cache = build_hist1min_cache(watchlist)
    dfs, failed_symbols = fetch_all(watchlist, now_ist, config.FETCH_WORKERS, hist_cache)

    for symbol, (df5, df75, df15) in dfs.items():
        try:
            instrument_key = watchlist[symbol]
            history = fetch_daily_history(instrument_key, days_back=config.BREAKOUT_HISTORY_LOOKBACK_DAYS)
            if not history:
                continue

            today_bar = build_todays_daily_bar(df5, today_str)
            if today_bar is None:
                continue

            signal = check_breakout_scan(history, today_bar, symbol)
            if signal is None:
                continue

            state_symbol = f"{symbol}::BREAKOUT"
            if state.already_alerted(saved_state, state_symbol, "BULLISH", today_str):
                continue

            signal["chart_link"] = build_chart_link(symbol)

            state.mark_alerted(saved_state, state_symbol, "BULLISH", today_str)
            try:
                send_breakout_alert(signal)
                alerts_sent += 1
            except Exception as e:
                import traceback
                print(f"send_breakout_alert failed for {symbol} (state already marked, won't re-send): {e}")
                traceback.print_exc()

        except Exception as e:
            print(f"Error on {symbol} (Breakout scan): {e}")

    state.save_state(saved_state)
    if failed_symbols:
        print(f"{len(failed_symbols)} Breakout-scan instrument(s) failed to fetch this run: {failed_symbols}")
    print(f"Breakout scan done. {alerts_sent} alert(s) sent.")
    return alerts_sent, failed_symbols, len(watchlist)


def run_consolidation_breakout_scan(now_ist):
    """
    Standalone Consolidation Breakout scan (added, per request —
    "Consolidation dhorte parbe?"). Own SCAN_MODE
    ("consolidation_breakout_scan"), meant to run once per day at/after
    market close, same as run_breakout_scan above — reuses the exact
    same building blocks (Nifty 500 watchlist, fetch_all,
    fetch_daily_history, build_todays_daily_bar) so today's daily bar
    is only ever assembled once in practice even though both scans
    call build_todays_daily_bar independently (cheap, pure computation
    over already-fetched df5 — no extra API calls). Direction-agnostic
    (fires BULLISH or BEARISH — see check_consolidation_breakout_scan),
    so the dedup key includes direction, unlike run_breakout_scan's
    hardcoded "BULLISH".
    """
    watchlist = build_nifty500_watchlist(now_ist)
    if not watchlist:
        print("Consolidation breakout scan: empty watchlist (outside session or list unavailable) — skipping.")
        return 0, [], 0

    print(f"Consolidation breakout scan: scanning {len(watchlist)} Nifty 500 stocks...")

    saved_state = state.load_state()
    alerts_sent = 0
    today_str = now_ist.date().isoformat()
    # Batched send (per request — "je single alert ache ota summary
    # kore dao") — every hit this run goes out as ONE message.
    hits = []

    hist_cache = build_hist1min_cache(watchlist)
    dfs, failed_symbols = fetch_all(watchlist, now_ist, config.FETCH_WORKERS, hist_cache)

    for symbol, (df5, df75, df15) in dfs.items():
        try:
            instrument_key = watchlist[symbol]
            history = fetch_daily_history(instrument_key, days_back=config.BREAKOUT_HISTORY_LOOKBACK_DAYS)
            if not history:
                continue

            today_bar = build_todays_daily_bar(df5, today_str)
            if today_bar is None:
                continue

            signal = check_consolidation_breakout_scan(history, today_bar, symbol)
            if signal is None:
                continue

            state_symbol = f"{symbol}::CONSOLIDATION_BREAKOUT"
            if state.already_alerted(saved_state, state_symbol, signal["direction"], today_str):
                continue

            signal["chart_link"] = build_chart_link(symbol)

            state.mark_alerted(saved_state, state_symbol, signal["direction"], today_str)
            hits.append(signal)

        except Exception as e:
            print(f"Error on {symbol} (Consolidation breakout scan): {e}")

    if hits:
        send_consolidation_breakout_summary(hits, now_ist)
        alerts_sent += len(hits)

    state.save_state(saved_state)
    if failed_symbols:
        print(f"{len(failed_symbols)} Consolidation-breakout-scan instrument(s) failed to fetch this run: {failed_symbols}")
    print(f"Consolidation breakout scan done. {alerts_sent} alert(s) sent.")
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

    # SCAN_MODE=ema_cross_report -> the standalone "EMA50/200
    # (Golden/Death Cross) + Delivery%" report (RE-ADDED, was missing —
    # see chat), fully separate from the alert scan — no session gate,
    # since it can usefully run right at/just before market open too.
    # See run_ema_cross_report / build_todays_ema_cross_list above for
    # what "today" actually means here. IMPORTANT: without this check,
    # SCAN_MODE=ema_cross_report was silently falling through to the
    # normal full-scan path below (since it isn't "index" either) --
    # meaning the two new cron-job.org triggers were firing a full
    # 75-min alert scan instead of the intended lightweight report.
    if mode == "ema_cross_report":
        run_ema_cross_report(now_ist)
        return

    # SCAN_MODE=opening_bias_report -> NEW (per request) standalone
    # "F&O Opening 15-min Bias" report — one consolidated list of every
    # F&O stock whose first 15-min candle today was Open==Low or
    # Open==High. Meant to run once, shortly after 09:30 IST, via its
    # own cron trigger (see scan.yml). See run_opening_bias_report /
    # build_todays_opening_bias_list above.
    if mode == "opening_bias_report":
        run_opening_bias_report(now_ist)
        return

    # SCAN_MODE=breakout_scan -> the standalone 12-condition daily
    # breakout screener (added 2026-08-18), fully separate from the
    # EMA-cross alert scan above — see run_breakout_scan. Meant to run
    # once/day via its own cron trigger, at/after market close.
    if mode == "breakout_scan":
        run_breakout_scan(now_ist)
        return

    # SCAN_MODE=consolidation_breakout_scan -> the standalone
    # Consolidation Breakout scan (added, per request), fully separate
    # from both the EMA-cross alert scan and the 12-condition breakout
    # scan above. Meant to run once/day via its own cron trigger,
    # at/after market close, same as breakout_scan.
    if mode == "consolidation_breakout_scan":
        if config.CONSOLIDATION_BREAKOUT_SCAN_ENABLED:
            run_consolidation_breakout_scan(now_ist)
        else:
            print("Consolidation breakout scan disabled (config.CONSOLIDATION_BREAKOUT_SCAN_ENABLED=False) — skipping.", flush=True)
        return

    if not (_in_stock_session(now_ist) or _in_commodity_session(now_ist)):
        print(f"Outside all trading sessions ({now_ist.strftime('%H:%M')} IST) — skipping.", flush=True)
        return

    _, fo_failed, fo_total, fo_ds_hits = run_fo_scan(now_ist)

    # ---- Corporate-action alerts (Dividend/Bonus/Buyback/Order Win) ----
    # Same watchlist, same Telegram channel as the EMA alerts. Cheap to
    # call every run -- corporate_actions.check_and_alert() only does
    # real work (NSE fetch) once per calendar day; every other call
    # this same day is a no-op (see its own cache check).
    if corporate_actions is not None:
        try:
            corporate_actions.check_and_alert(now_ist)
        except Exception as e:
            print(f"Corporate-action check failed this run (non-blocking): {e}", flush=True)

    # ---- Standalone Bulk/Block Deal alert (added, per request) ----
    # Market-wide (every symbol, same as the NSE Bulk Deals page),
    # dedup'd against its own persisted state file — see
    # bulk_block_data.check_and_alert. Non-blocking, same pattern as
    # corporate_actions above: a bad fetch here must never take down
    # the main EMA-cross scan.
    try:
        bulk_block_data.check_and_alert(now_ist)
    except Exception as e:
        print(f"Bulk/Block deal check failed this run (non-blocking): {e}", flush=True)

    # ---- Nifty 500 cash-stock scan (same conditions, EMA9/21) ----
    n500_failed, n500_total, n500_ds_hits = [], 0, []
    if _in_stock_session(now_ist):
        try:
            _, n500_failed, n500_total, n500_ds_hits = run_nifty500_scan(now_ist)
        except Exception as e:
            print(f"Nifty 500 scan failed this run (non-blocking): {e}", flush=True)

    # "Perfect Daily Score" report (FNO+CASH combined, per request,
    # 2026-08-28) — moved here from inside run_fo_scan so it can cover
    # BOTH scans' hits in ONE message: F&O stocks (tagged "FNO",
    # collected by run_fo_scan above) plus pure cash-only Nifty 500
    # stocks (tagged "CASH", collected by run_nifty500_scan above — that
    # scan already skips anything in fno_underlyings, so there's no
    # overlap/double-count between the two lists). Sent only if the SET
    # of qualifying symbols has changed since the last message actually
    # sent today (see load_daily_score_report_state), so a stock
    # holding 8/8 across many candles in a row doesn't repeat this
    # every scan cycle. Sorted highest score first, then FNO before
    # CASH, then alphabetically.
    daily_score_report_hits = fo_ds_hits + n500_ds_hits
    if config.DAILY_SCORE_REPORT_ENABLED and daily_score_report_hits:
        daily_score_report_hits.sort(key=lambda h: (-h["score"], not h["is_fno"], h["symbol"]))
        current_symbols = {h["symbol"] for h in daily_score_report_hits}
        ds_report_state = load_daily_score_report_state()
        if current_symbols != set(ds_report_state.get("last_sent_symbols", [])):
            try:
                send_daily_score_report(daily_score_report_hits, now_ist)
                save_daily_score_report_state(current_symbols)
            except Exception as e:
                print(f"send_daily_score_report failed: {e}")

    # ---- Fetch-failure visibility: one summary alert if enough
    # instruments failed to fetch this run ----
    maybe_send_failure_summary(fo_failed, fo_total, n500_failed, n500_total)


if __name__ == "__main__":
    run()
