
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
import urllib.parse
import json
import time
import random
import threading
import datetime as dt
import xml.etree.ElementTree as ET
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import re
import requests

import config
import instruments
import state
import delivery_data
import bulk_block_data
import groww_links
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
from strategy import check_signals, debug_ema_gap, get_3min_trend_info, get_sector_trend, passes_confluence_filter, compute_smart_money_signal, check_breakout_scan, check_consolidation_breakout_scan, compute_consolidation_window, check_consolidation_breakout_live, compute_session_vwap, check_trendline_scan, check_liquidity_sweep_scan, check_ma_envelope, get_opening_candle_bias, compute_intraday_checklist, get_opening_candle_buy_sell_estimate, compute_trading_score, passes_alert_gate, compute_near_high_score, compute_daily_score_scan, get_monthly_rsi_info, check_swing_entry_scan, check_daily_ema50_macd_scan, compute_position_info, check_monthly_macd_rsi_scan, check_daily_macd_rsi_scan, get_daily_support_info, compute_alert_strength
from telegram_notifier import _sector_label, send_alert_stats_report, send_oversold_report, send_oversold_eod_report, flush_pending_alerts, send_alert, send_ema_cross_report, send_breakout_alert, send_consolidation_breakout_summary, send_trendline_alert, send_liquidity_sweep_alert, send_ma_envelope_alert, send_opening_bias_report, send_sector_performance_report, send_eod_alert_report, send_daily_score_report, send_trading_score_summary, send_mode_failure_notice, send_top_movers_report, send_swing_entry_summary
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
# 15:40 not 15:30 (2026-10-21): the 15:15-15:30 candle closes AT 15:30:00 and the
# cron run for it only starts a minute or two later, so a 15:30 cutoff skipped
# the last candle of the day for every stock.
STOCK_SESSION_END = dt.time(15, 40)

# Commodity (MCX) session — MCX's evening session actually runs until
# 11:30 PM IST (was incorrectly set to 23:00 here, which cut off the
# last 30 minutes of commodity scanning/alerting every day).
COMMODITY_SESSION_START = dt.time(9, 15)
COMMODITY_SESSION_END = dt.time(23, 40)   # same reason: the 23:15-23:30 candle needs a late-starting run


def _now_ist():
    return dt.datetime.now(IST)


def _in_stock_session(now_ist):
    return STOCK_SESSION_START <= now_ist.time() <= STOCK_SESSION_END


def _in_commodity_session(now_ist):
    return COMMODITY_SESSION_START <= now_ist.time() <= COMMODITY_SESSION_END


_groww_name_cache = {}


def _lookup_company_name(symbol):
    """
    Company name for `symbol` from Upstox's NSE_EQ instrument master —
    only used as a LAST-resort hint by groww_links.get_url (the real
    Groww slug comes from Groww's own search, see groww_links.py).
    Cached per-process. None for indices/MCX commodities.
    """
    if symbol not in _groww_name_cache:
        names = instruments.resolve_stock_names([symbol])
        _groww_name_cache[symbol] = names.get(symbol)
    return _groww_name_cache[symbol]


def build_chart_link(symbol, timeframe_label=None):
    """
    Chart link for `symbol` — tapping it in the Telegram alert opens
    that symbol on Groww. timeframe_label is accepted for backward
    compatibility but doesn't affect the link.

    FIXED (2026-10-19 — "sob chart groww te khulche na"): all the
    resolution logic moved to groww_links.get_url, which now covers
    stocks (Groww's real search_id slug, not a guess from the company
    name), indices and MCX commodities (config.GROWW_FIXED_URLS).
    TradingView is only the fallback when Groww has nothing for the
    symbol — the Telegram label then says "TradingView", so you can
    see at a glance which links still need a GROWW_SYMBOL_OVERRIDES
    entry.
    """
    # Indices / commodities never need a company name; skip the master
    # lookup unless the symbol could actually be a plain stock.
    name = None
    if symbol not in config.GROWW_FIXED_URLS and symbol not in config.GROWW_SYMBOL_OVERRIDES:
        try:
            name = _lookup_company_name(symbol)
        except Exception:
            name = None

    url = groww_links.get_url(symbol, name)
    if url:
        return url

    # Last resort (per request 2026-10-20 — "sob chart groww te koro"): still a
    # Groww page (its search), never TradingView. Add the stock to
    # config.GROWW_SYMBOL_OVERRIDES to make it open the chart directly.
    return "https://groww.in/search?q=" + urllib.parse.quote(symbol)


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


def resample_75min(df, rule="75min"):
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
        day_out = day_df.resample(rule, origin=day_start).agg({
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


def fetch_stock_news(symbol, max_headlines=2):
    """
    Fetch recent headlines for a symbol via Google News RSS (added,
    per request, 2026-09-10 — "alert er sathe stock news paoa jabe?").
    No API key needed, no extra dependency (stdlib xml.etree only).

    Only headline TEXT is returned (never full article body) — this
    keeps the alert message short and avoids reproducing copyrighted
    article content. Results can be noisy/loosely matched since this
    is an unfiltered public search, not a curated financial news feed.

    On ANY failure (network, timeout, parsing, no results) this
    returns [] silently and logs to stdout — news is a nice-to-have
    add-on, never allowed to block or break the main MACD alert.
    Called only for symbols that are actually about to alert (see
    main.py's send_alert call sites), not for every scanned symbol.
    """
    try:
        query = quote(f"{symbol} share")
        url = f"https://news.google.com/rss/search?q={query}&hl=en-IN&gl=IN&ceid=IN:en"
        resp = requests.get(url, timeout=8)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        headlines = []
        for item in root.findall(".//item"):
            title_el = item.find("title")
            if title_el is not None and title_el.text:
                headlines.append(title_el.text.strip())
            if len(headlines) >= max_headlines:
                break
        return headlines
    except Exception as e:
        print(f"fetch_stock_news failed for {symbol}: {e}")
        return []


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
            # "high"/"low" added (per request, 2026-09-05) so the
            # Liquidity Sweep scan can check the previous day's
            # high/low without a second daily-OHLC fetch — see
            # strategy.check_liquidity_sweep_scan / main.py's callers.
            return symbol, {"r3": r3, "s3": s3, "prev_close": ohlc["close"], "high": ohlc["high"], "low": ohlc["low"]}

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


def fetch_daily_history(instrument_key, days_back=40, include_today=False):
    """
    Fetches up to `days_back` calendar days of completed daily candles
    for one instrument, via the same Upstox daily-candle endpoint used
    by fetch_prev_day_ohlc — reuses the same rate limiter
    (_pivot_fetch_limiter) since it's the same Upstox endpoint/budget.
    Returns a list of {"date", "open", "high", "low", "close",
    "volume"} sorted oldest -> newest, or [] on failure/no data.
    (open/high/low added 2026-08-18 for run_breakout_scan's Rows 9-12,
    which need the daily range, not just close/volume — existing
    callers that only read "close"/"volume" are unaffected.)

    include_today (added, per request — "bikale aajker cross chai"):
    by default (False) the request stops at YESTERDAY, same as always
    — this is what every other caller (Momentum/Volume Spike/EMA50-200
    "as of yesterday", pivots, breakout scans) wants, since it's a
    stable, always-available range. Pass True (used only by
    build_todays_ema_cross_list_evening) to instead request up through
    TODAY — Upstox simply won't return today's row if it hasn't been
    finalized yet by call time, so this is safe either way; it never
    raises just because today isn't there yet.
    """
    headers = {"Authorization": f"Bearer {config.UPSTOX_ACCESS_TOKEN}"}
    today = _now_ist().date()
    from_date = today - dt.timedelta(days=days_back)
    to_date = today if include_today else today - dt.timedelta(days=1)

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
        # BUG FIX (found 2026-08-29 — see chat): build_todays_ema_cross_
        # list compares ema_cross["cross_date"] against
        # ema_cross["latest_date"] to decide "is this cross FRESH (on
        # the most recent available daily candle) or old". That key was
        # never actually set here, so that comparison raised a
        # KeyError the moment any symbol had a real cross_date. Masked
        # until now because a SEPARATE bug (see build_watchlist call in
        # run_ema_cross_report) meant the watchlist passed in was
        # always empty at the time this report actually runs, so the
        # loop body — including this comparison — was simply never
        # reached. dates[-1] is the most recent date in the supplied
        # history, i.e. "yesterday" per fetch_daily_history's contract.
        "latest_date": str(dates[-1])[:10],
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

    # Entries cached before 2026-10-19 have no month_high/month_low;
    # treat them as missing so they get refetched once (same cost as the
    # normal first-run-of-the-day rebuild).
    missing = {
        sym: key for sym, key in watchlist.items()
        if sym not in cache["data"] or "month_high" not in cache["data"][sym]
    }

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

            # 1-month average DAILY volume (ADDED, per request,
            # 2026-09-01 — "volume alert tao kore dao same [as
            # delivery]") — same trailing ~20-trading-day window as
            # four_week_high_close above, zero extra API calls (reuses
            # `history` already fetched for this symbol today).
            volume_avg_1m = sum(day["volume"] for day in lookback_window) / len(lookback_window)

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

            # 1-MONTH RANGE (ADDED, 2026-10-19 — "one month position"):
            # highest HIGH / lowest LOW over the last 21 completed
            # trading days (same 21-day "1M" window as multi_month_highs
            # above), from the `history` already fetched — no extra API
            # call. The alert shows where the close sits inside it.
            month_window = history[-MOMENTUM_LOOKBACK_TRADING_DAYS:]   # 20 trading days (2026-10-24)
            month_high = max(day["high"] for day in month_window)
            month_low = min(day["low"] for day in month_window)

            return symbol, {
                "month_high": month_high,
                "month_low": month_low,
                "four_week_high_close": four_week_high_close,
                "prev_day_volume": prev_day_volume,
                "volume_5day_ago": volume_5day_ago,
                "volume_avg_1m": volume_avg_1m,
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
    delivery_pct, chart_link, screener_link}, sorted by symbol, for every watchlist symbol whose
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
        try:
            ema_cross = mv.get("ema_cross") if mv else None
            if not ema_cross or not ema_cross.get("cross_date"):
                continue
            if ema_cross["cross_date"] != ema_cross.get("latest_date"):
                continue
            crosses.append({
                "symbol": symbol,
                "bias": ema_cross["bias"],
                "ema50": ema_cross["ema50"],
                "ema200": ema_cross["ema200"],
                "cross_date": ema_cross["cross_date"],
                "delivery_pct": delivery_map.get(symbol),
                # Chart + Quarterly Results links (added, per request —
                # "Etet chart link ar result add kore dao") — same
                # build_chart_link (now Groww, falls back to
                # TradingView) and plain screener.in URL the main
                # per-symbol alert already uses.
                "chart_link": build_chart_link(symbol),
                "screener_link": f"https://www.screener.in/company/{symbol}/consolidated/",
            })
        except Exception as e:
            # Safety net (added 2026-08-29, see chat) — one malformed
            # entry must never take down the whole report the way the
            # missing "latest_date" key silently did before (it just
            # happened to never be reached due to a separate bug — see
            # build_full_watchlist_no_session_gate above). Skip just
            # this symbol, not the entire scheduled run.
            print(f"Error building EMA cross entry for {symbol}: {e}")

    return sorted(crosses, key=lambda c: c["symbol"])


def run_ema_cross_report(now_ist):
    """
    RE-ADDED (was missing — see chat) — SCAN_MODE=ema_cross_report
    entry point (see run()). MORNING report — builds the full
    stock/commodity/Nifty500 watchlist, finds crosses on the most
    recent COMPLETED daily candle (yesterday's close — see
    build_todays_ema_cross_list above), and sends ONE consolidated
    Telegram message via telegram_notifier.send_ema_cross_report —
    always sends something (even "no crosses today"), so a scheduled
    run being silent never looks like it might have just failed.

    Labeled "Yesterday's Close" (added, per request — "sokale gotokal
    ... cross chai") since that's genuinely the freshest data available
    at a morning run — today's own candle doesn't exist yet. See
    run_ema_cross_report_evening below for the "today's own candle"
    version.
    """
    watchlist = build_full_watchlist_no_session_gate()
    crosses = build_todays_ema_cross_list(watchlist, now_ist)
    send_ema_cross_report(crosses, now_ist, period_label="Yesterday's Close")


def build_todays_ema_cross_list_evening(watchlist, now_ist):
    """
    EVENING counterpart to build_todays_ema_cross_list above (added,
    per request — "bikale aajker cross chai"). Does its OWN per-symbol
    daily-history fetch with include_today=True (see
    fetch_daily_history), completely separate from
    build_momentum_volume_data's cache — that cache is built once per
    calendar day and shared by every other feature (Momentum/Volume
    Spike/Near-High/the morning report above), so reusing it here would
    either serve stale pre-today data (if the morning run already
    populated it) or require invalidating it mid-day (which would
    force every OTHER feature to refetch too, wasting API calls for no
    reason). A dedicated fetch here costs one extra daily-history call
    per watchlist symbol, once/day (this report's own cron trigger),
    which is an acceptable trade for genuinely fresh same-day data.

    IMPORTANT CAVEAT: Upstox does not guarantee today's daily candle is
    finalized by any particular evening time — if it isn't yet by the
    time this runs, that symbol's fetch simply won't include today's
    row, and it's silently excluded from this list (same as any other
    missing-data case elsewhere in this file) rather than crashing or
    reporting a false negative. An empty result on a run soon after
    market close more often means "Upstox hasn't finalized today's
    candles yet" than "genuinely zero crosses across the whole
    watchlist" — if this keeps showing empty, try moving the cron
    trigger later in the evening.

    Filters specifically for cross_date == TODAY's calendar date (not
    just "latest available", unlike the morning version) — an exact
    match, since "today" has a precise, unambiguous meaning here.
    """
    delivery_map = delivery_data.get_delivery_data(now_ist.date()) or {}
    today_str = now_ist.date().isoformat()

    def _fetch_one(symbol, instrument_key):
        try:
            history = fetch_daily_history(instrument_key, days_back=DAILY_HISTORY_LOOKBACK_DAYS, include_today=True)
            ema_cross = _compute_ema50_200_cross(history)
            return symbol, ema_cross
        except Exception as e:
            print(f"Evening EMA cross fetch failed for {symbol}: {e}")
            return symbol, None

    crosses = []
    with ThreadPoolExecutor(max_workers=config.PIVOT_FETCH_WORKERS) as pool:
        futures = {
            pool.submit(_fetch_one, symbol, instrument_key): symbol
            for symbol, instrument_key in watchlist.items()
        }
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                sym, ema_cross = future.result()
                if not ema_cross or not ema_cross.get("cross_date"):
                    continue
                if ema_cross["cross_date"] != today_str:
                    continue
                crosses.append({
                    "symbol": sym,
                    "bias": ema_cross["bias"],
                    "ema50": ema_cross["ema50"],
                    "ema200": ema_cross["ema200"],
                    "cross_date": ema_cross["cross_date"],
                    "delivery_pct": delivery_map.get(sym),
                    # Chart + Quarterly Results links — see the matching
                    # comment in build_todays_ema_cross_list above.
                    "chart_link": build_chart_link(sym),
                    "screener_link": f"https://www.screener.in/company/{sym}/consolidated/",
                })
            except Exception as e:
                print(f"Error building evening EMA cross entry for {symbol}: {e}")

    return sorted(crosses, key=lambda c: c["symbol"])


def run_ema_cross_report_evening(now_ist):
    """
    EVENING counterpart to run_ema_cross_report above (added, per
    request). SCAN_MODE=ema_cross_report_evening entry point (see
    run()) — needs its OWN separate cron-job.org trigger (later in the
    evening, e.g. after 18:00 IST, to give Upstox time to finalize the
    day's daily candles — see build_todays_ema_cross_list_evening's
    docstring caveat).
    """
    watchlist = build_full_watchlist_no_session_gate()
    crosses = build_todays_ema_cross_list_evening(watchlist, now_ist)
    send_ema_cross_report(crosses, now_ist, period_label="Today's Close")


def _opening_rows(df15, now_ist, n_candles):
    """Today's 15-min candles that fall INSIDE the opening window (first `n_candles` x 15 min from 09:15)."""
    rows = df15[df15["timestamp"].dt.date == now_ist.date()]
    if rows.empty:
        return rows
    end = rows.iloc[0]["timestamp"] + pd.Timedelta(minutes=15 * n_candles)
    return rows[rows["timestamp"] < end]


def _opening_candle_reliable(df15, now_ist, n_candles=1):
    """True only if today's opening window (1st 15-min candle, or the first `n_candles` 15-min
    candles: 2 = 30 min, 4 = 1 hour) is complete enough to judge Open==Low / Open==High: it exists
    for TODAY, starts at 09:15, its 1-min data starts at 09:15 (real open) and runs to the end of the
    window (so the high/low are not missing the last minutes because of data lag)."""
    try:
        if df15 is None or df15.empty:
            return False
        rows = _opening_rows(df15, now_ist, n_candles)
        if len(rows) < n_candles:
            return False
        t0 = rows.iloc[0]["timestamp"]
        if (t0.hour, t0.minute) != (9, 15):
            return False
        fm = df15.attrs.get("first_minute")
        lm = df15.attrs.get("last_minute")
        if fm is not None and (fm.hour, fm.minute) != (9, 15):
            return False
        # last 1-min bar must be within 2 minutes of the window end (09:28 / 09:43 / 10:13)
        end_min = 9 * 60 + 15 + 15 * n_candles - 2
        if lm is not None and (lm.hour * 60 + lm.minute) < end_min:
            return False
        return True
    except Exception:
        return False


def _opening_window(df15, now_ist, n_candles):
    """(open, high, low, close) of today's first `n_candles` 15-min candles combined."""
    rows = _opening_rows(df15, now_ist, n_candles)
    return (float(rows.iloc[0]["open"]), float(rows["high"].max()),
            float(rows["low"].min()), float(rows.iloc[-1]["close"]))


def build_todays_opening_bias_list(now_ist, n_candles=1):
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

    Returns (bullish, bearish, no_data, pct_changes):
      bullish, bearish, no_data -- three lists of symbols, each sorted
        alphabetically, same as before. no_data means today's first
        15-min candle hasn't closed yet (before ~09:30 IST) or the
        fetch failed for that symbol this run — kept separate from
        "neutral" (a candle that DID close but wasn't a clean
        Open==Low/Open==High) since only no_data is worth flagging
        back to you; a real neutral candle is just... not on either
        list, same as it wouldn't show on an individual alert either.
      pct_changes -- {symbol: pct} (ADDED, per request, 2026-09-01 —
        "stocke er pase change ana jabe?") -- % change of that first
        15-min candle's CLOSE vs the previous trading day's close
        (build_pivot_levels' prev_close, same source every other
        alert's "Day Change %" already uses), for every symbol that
        made either list. A symbol only ends up missing from
        pct_changes if its pivot prev_close itself is unavailable --
        quietly omitted (line just won't show a %), not an error.
    """
    fo_watch = None if config.USE_FULL_FO_LIST else config.FO_STOCK_WATCHLIST
    watchlist = instruments.resolve_fo_stock_list(fo_watch)
    pivots = build_pivot_levels(watchlist)

    dfs, failed_symbols = fetch_all(watchlist, now_ist, config.FETCH_WORKERS)

    bullish, bearish, no_data = [], [], []
    pct_changes = {}
    all_pct = {}
    for symbol in watchlist:
        if symbol not in dfs:
            no_data.append(symbol)
            continue
        _df5, _df75, df15 = dfs[symbol]
        if df15 is None or not _opening_candle_reliable(df15, now_ist, n_candles):
            no_data.append(symbol)
            continue
        _o, _h, _l, _c = _opening_window(df15, now_ist, n_candles)
        # opening-window change for EVERY stock (sector performance report)
        _pc = (pivots.get(symbol) or {}).get("prev_close")
        if _pc and not df15.empty:
            all_pct[symbol] = (_c - _pc) / _pc * 100.0
        if n_candles == 1:
            bias = get_opening_candle_bias(df15, symbol)
        else:
            _eps = config.OPENING_CANDLE_EPSILON
            _eq_l, _eq_h = abs(_o - _l) <= _eps, abs(_o - _h) <= _eps
            bias = "BULLISH" if (_eq_l and not _eq_h) else ("BEARISH" if (_eq_h and not _eq_l) else None)
        if bias == "BULLISH":
            bullish.append(symbol)
        elif bias == "BEARISH":
            bearish.append(symbol)
        else:
            # bias is None (genuinely neutral candle) -> on neither
            # list, not counted as no_data either -- skip the pct
            # lookup too, it wouldn't be shown anywhere.
            continue

        prev_close = (pivots.get(symbol) or {}).get("prev_close")
        if prev_close and not df15.empty:
            pct_changes[symbol] = (_c - prev_close) / prev_close * 100.0

    global _LAST_ALL_PCT
    _LAST_ALL_PCT = all_pct
    return sorted(bullish), sorted(bearish), sorted(no_data), pct_changes


_LAST_ALL_PCT = {}


def run_opening_bias_report(now_ist, hour=False, sleep_first=True, window_min=None):
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
    # window_min: 15 (default) / 30 / 60.  `hour=True` is the old way of saying 60.
    if window_min is None:
        window_min = 60 if hour else 15
    n_c = max(1, int(window_min) // 15)
    label = {15: "15-min", 30: "30-min", 60: "1-hour"}.get(int(window_min), f"{int(window_min)}-min")
    if sleep_first:
        time.sleep(15)
    now_ist = _now_ist()

    if not _in_stock_session(now_ist):
        print(f"Outside stock session ({now_ist.strftime('%H:%M')} IST) — opening-bias report skipping.", flush=True)
        return
    bullish, bearish, no_data, pct_changes = build_todays_opening_bias_list(now_ist, n_c)
    if len(no_data) >= 10:
        # Upstox usually just hasn't published the last minutes of the candle
        # yet — wait a little and look once more rather than report a wrong list.
        print(f"{len(no_data)} symbols not ready at {now_ist.strftime('%H:%M:%S')} — retrying once in 25s.", flush=True)
        time.sleep(25)
        now_ist = _now_ist()
        bullish, bearish, no_data, pct_changes = build_todays_opening_bias_list(now_ist, n_c)

    # Chart links (added, per request) — same Groww chart link
    # link every other alert type already gets (see build_chart_link).
    # Built here (not inside build_todays_opening_bias_list) since
    # build_chart_link lives in main.py, not strategy.py/telegram_
    # notifier.py — keeps that function's return signature unchanged
    # for anything else that might call it.
    bullish_links = {sym: build_chart_link(sym) for sym in bullish}
    bearish_links = {sym: build_chart_link(sym) for sym in bearish}

    send_opening_bias_report(bullish, bearish, no_data, now_ist, bullish_links, bearish_links, pct_changes, label=label)
    try:
        try:
            _wl = instruments.resolve_fo_stock_list(None if config.USE_FULL_FO_LIST else config.FO_STOCK_WATCHLIST)
            _m1 = build_1month_changes({s: _wl[s] for s in _LAST_ALL_PCT if s in _wl})
        except Exception as e:
            print(f"1-month sector data failed: {e}", flush=True)
            _m1 = {"__day__": {}}
        send_sector_performance_report(_LAST_ALL_PCT, now_ist, _m1, label=label)
    except Exception as e:
        print(f"Sector performance report failed: {e}", flush=True)


def build_1month_changes(watchlist):
    """{symbol: % change over ~1 month} = last completed daily close vs the close ~30 days
    earlier. Failures are skipped quietly (sector just averages the rest)."""
    out = {}
    day_out = {}

    def _one(sym, key):
        h = fetch_daily_history(key, days_back=35, include_today=False)
        if len(h) < 10:
            return sym, None
        first, last, prev = float(h[0]["close"]), float(h[-1]["close"]), float(h[-2]["close"])
        if prev:
            day_out[sym] = (last - prev) / prev * 100.0   # last trading day
        return sym, ((last - first) / first * 100.0 if first else None)

    with ThreadPoolExecutor(max_workers=config.PIVOT_FETCH_WORKERS) as pool:
        futs = [pool.submit(_one, s, k) for s, k in watchlist.items()]
        for f in as_completed(futs):
            try:
                s, v = f.result()
                if v is not None:
                    out[s] = v
            except Exception as e:
                print(f"1M change failed: {e}", flush=True)
    out["__day__"] = day_out
    return out


def build_todays_top_movers_1min(now_ist):
    """
    ADDED (per request, 2026-08-31 — "arek ta alert dao top gainer
    looser in 1st 1 minit") — ranks every F&O stock by % change of
    TODAY's first 1-minute candle's close (09:15:00-09:16:00 IST)
    versus the previous trading day's close (build_pivot_levels'
    prev_close — same source every other alert already uses for
    "Day Change %").

    Same F&O-stocks-only universe as build_todays_opening_bias_list
    (not indices, not commodities, not the wider Nifty 500 list).

    Fetches the FULL day's 1-min series per symbol (fetch_1min_candles
    -- Upstox's intraday endpoint doesn't offer a "just the first
    candle" filter) and takes row 0, which is today's very first
    1-min candle since the endpoint only ever returns today's data.

    Returns (gainers, losers) -- each a list of up to
    config.TOP_MOVERS_COUNT dicts {symbol, close, prev_close, pct},
    sorted by pct (gainers descending, losers ascending -- i.e. most
    negative first). A symbol is skipped (not counted as a mover, not
    an error) if its 1-min fetch fails or pivot prev_close is missing
    -- both already-existing "quietly skip, don't crash the whole
    report" patterns used elsewhere in this file.
    """
    fo_watch = None if config.USE_FULL_FO_LIST else config.FO_STOCK_WATCHLIST
    watchlist = instruments.resolve_fo_stock_list(fo_watch)
    pivots = build_pivot_levels(watchlist)

    def _fetch_first_candle_close(symbol, instrument_key):
        try:
            df1 = fetch_1min_candles(instrument_key)
            if df1 is None or df1.empty:
                return symbol, None
            return symbol, float(df1.iloc[0]["close"])
        except Exception as e:
            print(f"Top movers 1-min fetch failed for {symbol}: {e}")
            return symbol, None

    first_closes = {}
    with ThreadPoolExecutor(max_workers=config.FETCH_WORKERS) as pool:
        futures = {
            pool.submit(_fetch_first_candle_close, symbol, instrument_key): symbol
            for symbol, instrument_key in watchlist.items()
        }
        for future in as_completed(futures):
            symbol, close = future.result()
            if close is not None:
                first_closes[symbol] = close

    movers = []
    for symbol, close in first_closes.items():
        levels = pivots.get(symbol)
        prev_close = levels.get("prev_close") if levels else None
        if not prev_close:
            continue
        pct = (close - prev_close) / prev_close * 100.0
        movers.append({"symbol": symbol, "close": close, "prev_close": prev_close, "pct": pct})

    gainers = sorted(movers, key=lambda m: m["pct"], reverse=True)[:config.TOP_MOVERS_COUNT]
    losers = sorted(movers, key=lambda m: m["pct"])[:config.TOP_MOVERS_COUNT]
    return gainers, losers


def run_top_movers_1min(now_ist):
    """
    ADDED (per request, 2026-08-31) — SCAN_MODE=top_movers_1min entry
    point. Meant to run via its own cron-job.org trigger shortly after
    09:16 IST (once today's first 1-min candle has closed).

    STARTUP DELAY: same reasoning as run_opening_bias_report -- the
    cron trigger fires at a fixed minute, but the just-closed 1-min
    candle isn't reliably available from Upstox the instant it closes.
    Sleeping 10s here gives that lag room. now_ist is re-read via
    _now_ist() AFTER the sleep for the same reason (session gate +
    printed/report timestamp should reflect the real post-sleep clock).
    """
    time.sleep(10)
    now_ist = _now_ist()

    if not _in_stock_session(now_ist):
        print(f"Outside stock session ({now_ist.strftime('%H:%M')} IST) — top-movers report skipping.", flush=True)
        return

    gainers, losers = build_todays_top_movers_1min(now_ist)
    gainer_links = {m["symbol"]: build_chart_link(m["symbol"]) for m in gainers}
    loser_links = {m["symbol"]: build_chart_link(m["symbol"]) for m in losers}
    send_top_movers_report(gainers, losers, now_ist, gainer_links, loser_links)


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


def build_full_watchlist_no_session_gate():
    """
    BUG FIX (found 2026-08-29 — see chat): same instrument universe as
    build_watchlist above (indices + F&O stocks + commodities), but
    WITHOUT its session-time gates. run_ema_cross_report is explicitly
    meant to run before market open (its own docstring: "no session
    gate, since it can usefully run right at/just before market open
    too") — but it was calling build_watchlist(now_ist), which
    internally only populates anything once _in_stock_session/
    _in_commodity_session is true (from 09:15 IST). Since the cron
    trigger fires at ~07:48 IST, build_watchlist was silently
    returning {} every single time, so build_todays_ema_cross_list had
    nothing to scan and "No fresh Golden/Death Cross today" was really
    reporting "checked zero symbols", not "checked everything, found
    nothing".
    """
    watchlist = {}
    watchlist.update(instruments.resolve_indices(config.INDICES))
    fo_watch = None if config.USE_FULL_FO_LIST else config.FO_STOCK_WATCHLIST
    watchlist.update(instruments.resolve_fo_stock_list(fo_watch))
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

    # Remember which 1-min bars today's data really starts/ends with, so the
    # 1st-15-min Open=High/Open=Low check can refuse to judge a candle that is
    # missing its 09:15 minute or its last minutes (Upstox publish lag).
    try:
        _last_day = raw["timestamp"].iloc[-1].date()
        _today_raw = raw[raw["timestamp"].dt.date == _last_day]
        df15.attrs["first_minute"] = _today_raw["timestamp"].iloc[0]
        df15.attrs["last_minute"] = _today_raw["timestamp"].iloc[-1]
    except Exception:
        pass

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

_daily_history_cache = {}
_monthly_history_cache = {}


def _attach_daily_support(signal, symbol, instrument_key):
    """signal["daily_support"] = daily MACD/RSI/volume support (see
    strategy.get_daily_support_info). One daily-history fetch per symbol per run.
    Never raises, never blocks."""
    try:
        if symbol not in _daily_history_cache:
            _daily_history_cache[symbol] = fetch_daily_history(instrument_key, days_back=260, include_today=False)
        signal["daily_support"] = get_daily_support_info(_daily_history_cache[symbol], signal["direction"])
    except Exception as e:
        print(f"Daily support info failed for {symbol}: {e}", flush=True)
        signal["daily_support"] = None
    # Monthly RSI (shown in the compact alert next to Daily RSI). Never blocks.
    try:
        if symbol not in _monthly_history_cache:
            _monthly_history_cache[symbol] = fetch_daily_history(
                instrument_key, days_back=config.MONTHLY_RSI_HISTORY_LOOKBACK_DAYS, include_today=True)
        mh = _monthly_history_cache[symbol]
        info = get_monthly_rsi_info(mh) if mh else None
        if info:
            signal["monthly_rsi"] = info
        # 52-week high / low from the same long daily history (last 365 days)
        if mh:
            cutoff = (_now_ist().date() - dt.timedelta(days=365)).isoformat()
            rows = [r for r in mh if str(r["date"])[:10] >= cutoff]
            if len(rows) >= 20:
                hi = max(float(r["high"]) for r in rows)
                lo = min(float(r["low"]) for r in rows)
                px = float(signal.get("close") or rows[-1]["close"])
                hi, lo = max(hi, px), min(lo, px)   # today's price can set a new high/low
                signal["week52"] = {
                    "high": hi, "low": lo,
                    "pct_from_high": (px - hi) / hi * 100.0 if hi else None,
                    "pct_from_low": (px - lo) / lo * 100.0 if lo else None,
                }
    except Exception as e:
        print(f"Monthly RSI failed for {symbol}: {e}", flush=True)


_NIFTY_CACHE = {"t": 0.0, "df": None}


def _nifty_trend_at(candle_time):
    """'UP' / 'DOWN' / 'FLAT' = where NIFTY's close sits vs the average typical price of today's 15-min
    candles up to `candle_time`. None if the data is unavailable (never blocks). The Nifty 1-min data is
    fetched at most once every 2 minutes per run."""
    try:
        now = time.time()
        if _NIFTY_CACHE["df"] is None or now - _NIFTY_CACHE["t"] > 120:
            sym = getattr(config, "MARKET_TREND_SYMBOL", "NIFTY 50")
            key = instruments.resolve_indices({sym: config.INDICES[sym]}).get(sym)
            raw = fetch_1min_candles(key) if key else None
            df = None
            if raw is not None and not raw.empty:
                ts = pd.to_datetime(raw["timestamp"])
                ts = ts.dt.tz_convert(IST) if ts.dt.tz is not None else ts.dt.tz_localize(IST)
                raw = raw.assign(timestamp=ts.dt.tz_localize(None))
                df = resample_15min(raw)
            _NIFTY_CACHE.update({"t": now, "df": df})
        df = _NIFTY_CACHE["df"]
        if df is None or df.empty:
            return None
        ct = pd.Timestamp(candle_time)
        if ct.tzinfo is not None:
            ct = ct.tz_convert(IST).tz_localize(None)
        rows = df[(df["timestamp"].dt.date == ct.date()) & (df["timestamp"] <= ct)]
        if rows.empty:
            return None
        avg = float(((rows["high"] + rows["low"] + rows["close"]) / 3.0).mean())
        last = float(rows.iloc[-1]["close"])
        flat = float(getattr(config, "MARKET_TREND_FLAT_PCT", 0.03)) / 100.0
        if last > avg * (1 + flat):
            return "UP"
        if last < avg * (1 - flat):
            return "DOWN"
        return "FLAT"
    except Exception as e:
        print(f"Nifty trend check failed: {e}", flush=True)
        return None


def _daily_support_blocks(signal):
    """True if this signal must be blocked by the stock-only gates (cash-bearish, Monthly RSI,
    optional daily support). Commodities and indices are exempt: a nearest-expiry futures
    contract has too little history for a Monthly RSI, so the "no Monthly RSI -> block" rule
    was silently killing every commodity alert."""
    if signal.get("symbol") in (set(config.COMMODITIES) | set(config.INDICES)):
        return False
    # No alerts for candles that START before ALERT_NO_ALERT_BEFORE (09:15 / 09:30 candles are noise)
    try:
        nb = str(getattr(config, "ALERT_NO_ALERT_BEFORE", "") or "")
        if nb:
            hh, mm = (int(x) for x in nb.split(":"))
            if pd.Timestamp(signal["candle_time"]).time() < dt.time(hh, mm):
                print(f"[early-candle] {signal.get('symbol')} {signal['direction']} skipped (candle before {nb})", flush=True)
                return True
    except Exception as e:
        print(f"early-candle check failed: {e}", flush=True)
    # Cash-only stocks: no bearish alerts (F&O stocks keep both directions)
    if getattr(config, "CASH_STOCKS_BULLISH_ONLY", False) and signal.get("is_fno") is False and signal["direction"] == "BEARISH":
        print(f"[cash-bearish] {signal.get('symbol')} BEARISH skipped (cash stock)", flush=True)
        return True
    # Monthly RSI against the direction -> block (missing data never blocks)
    if getattr(config, "ALERT_REQUIRE_MONTHLY_RSI", False):
        mr = (signal.get("monthly_rsi") or {}).get("rsi")
        if mr is None and getattr(config, "MONTHLY_RSI_BLOCK_IF_MISSING", False):
            print(f"[monthly-rsi] {signal.get('symbol')} {signal['direction']} skipped (no monthly RSI data)", flush=True)
            return True
        if mr is not None:
            bull = signal["direction"] == "BULLISH"
            if (bull and mr < config.MONTHLY_RSI_BULL_MIN) or (not bull and mr > config.MONTHLY_RSI_BEAR_MAX):
                print(f"[monthly-rsi] {signal.get('symbol')} {signal['direction']} skipped (monthly RSI {mr})", flush=True)
                return True
    # Market trend: don't fight NIFTY (bullish while Nifty is below its intraday average, bearish while above)
    trend = _nifty_trend_at(signal["candle_time"])
    signal["nifty_trend"] = trend
    if getattr(config, "MARKET_TREND_FILTER", False) and trend:
        if (signal["direction"] == "BULLISH" and trend == "DOWN") or (signal["direction"] == "BEARISH" and trend == "UP"):
            print(f"[market-trend] {signal.get('symbol')} {signal['direction']} skipped (Nifty {trend})", flush=True)
            return True
    if not getattr(config, "ALERT_REQUIRE_DAILY_SUPPORT", False):
        return False
    ds = signal.get("daily_support")
    if not ds:
        return False   # missing data never blocks
    return not (ds["macd_ok"] and ds["rsi_ok"] and ds["vol_ok"] is not False)


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
    # 1-month per-symbol average delivery % (ADDED, per request,
    # 2026-09-01 — "delivery ota last one month average theke koto
    # besi kore dite parbe?") — also cached, once per calendar day.
    delivery_avg_map = delivery_data.get_delivery_avg_1m(now_ist.date())

    saved_state = state.load_state()
    alerts_sent = 0
    daily_score_report_hits = []
    trading_score_summary_hits = []
    # Consolidation Breakout — LIVE hits collected across this whole
    # run (added, per request — "je single alert ache ota summary kore
    # dao"), sent as ONE batched message at the end instead of one
    # message per stock. See send_consolidation_breakout_summary.
    consolidation_live_hits = []
    orb_hits = {w: [] for w in _orb_windows()}
    bb_hits = {"break": [], "reentry": []}
    move15_hits = []

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
            if symbol in index_symbols and not index_only and not getattr(config, "INDEX_ALERTS_IN_FULL_RUN", False):
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
            # Opening-range-break alerts (1st 15-min / 30-min / 1-hour, price above/below VWAP) — stocks only.
            _orb_scan_symbol(symbol, df5, df15, prev_close, now_ist, saved_state, watchlist.get(symbol), fno_underlyings, orb_hits)
            _bb_scan_symbol(symbol, df15, now_ist, saved_state, fno_underlyings, bb_hits, watchlist.get(symbol), levels, df5)
            _move15_scan_symbol(symbol, df15, prev_close, now_ist, saved_state, watchlist.get(symbol), fno_underlyings, move15_hits)
            # ONLY-SELECTED-ALERTS MODE (2026-11-21): when MACD15_ALERT_ENABLED is False the 15-min MACD alert is sent for commodities only (MACD15_ALERT_COMMODITIES)
            if not _macd15_allowed(symbol):
                continue
            # prev_day_high/prev_day_low (added, per request, 2026-09-05)
            # — feeds the Liquidity Sweep scan below.
            prev_day_high = levels.get("high") if levels else None
            prev_day_low = levels.get("low") if levels else None

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
                    require_vwap_confirmation=config.REQUIRE_VWAP_CONFIRMATION,
                    require_macd_divergence=config.REQUIRE_MACD_DIVERGENCE,
                    today=now_ist.date(),
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

                # Liquidity Sweep (added, per request, 2026-09-05) —
                # standalone, does NOT require a MACD cross; checked
                # every run on the same df15 already used above, on
                # the latest closed candle only (see
                # strategy.check_liquidity_sweep_scan). Checks BOTH
                # a rolling 15-min swing high/low AND the previous
                # day's high/low (pivots.get, may be missing — that's
                # fine, the prev-day check just skips itself then).
                if config.ENABLE_LIQUIDITY_SWEEP_ALERTS:
                    ls_signals = check_liquidity_sweep_scan(
                        df15, symbol,
                        prev_day_high=prev_day_high,
                        prev_day_low=prev_day_low,
                    )
                    for ls_signal in ls_signals:
                        ls_state_symbol = f"{symbol}::SWEEP::{ls_signal['direction']}"
                        if not state.in_cooldown(saved_state, ls_state_symbol, ls_signal["direction"], ls_signal["candle_time"], config.LIQUIDITY_SWEEP_COOLDOWN_MINUTES):
                            ls_signal["chart_link"] = build_chart_link(symbol, "15-min")
                            state.mark_alerted(saved_state, ls_state_symbol, ls_signal["direction"], ls_signal["candle_time"])
                            try:
                                send_liquidity_sweep_alert(ls_signal)
                                alerts_sent += 1
                            except Exception as e:
                                print(f"send_liquidity_sweep_alert failed for {symbol}: {e}")

                # Moving Average Envelope (added, per request,
                # 2026-09-09) — standalone, does NOT require a MACD
                # cross; reuses the daily EMA200 already computed for
                # the EMA50/200 cross feature (momentum_volume dict,
                # built once for the whole watchlist above) — no extra
                # daily-history fetch. See strategy.check_ma_envelope.
                if config.ENABLE_MA_ENVELOPE_ALERTS:
                    mv_env = momentum_volume.get(symbol)
                    ema200_val = mv_env["ema_cross"]["ema200"] if mv_env and mv_env.get("ema_cross") else None
                    env_signal = check_ma_envelope(df15, symbol, ema200_val)
                    if env_signal is not None:
                        env_state_symbol = f"{symbol}::MAENV::{env_signal['direction']}"
                        if not state.in_cooldown(saved_state, env_state_symbol, env_signal["direction"], env_signal["candle_time"], config.MA_ENVELOPE_COOLDOWN_MINUTES):
                            env_signal["chart_link"] = build_chart_link(symbol, "15-min")
                            state.mark_alerted(saved_state, env_state_symbol, env_signal["direction"], env_signal["candle_time"])
                            try:
                                send_ma_envelope_alert(env_signal)
                                alerts_sent += 1
                            except Exception as e:
                                print(f"send_ma_envelope_alert failed for {symbol}: {e}")
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
                    require_vwap_confirmation=config.REQUIRE_VWAP_CONFIRMATION,
                    require_macd_divergence=config.REQUIRE_MACD_DIVERGENCE,
                    today=now_ist.date(),
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

                # Liquidity Sweep — see the matching comment in the
                # index branch above. Checked on primary_df (whichever
                # timeframe config.PRIMARY_TIMEFRAME currently selects).
                if config.ENABLE_LIQUIDITY_SWEEP_ALERTS:
                    ls_signals = check_liquidity_sweep_scan(
                        primary_df, symbol,
                        prev_day_high=prev_day_high,
                        prev_day_low=prev_day_low,
                    )
                    for ls_signal in ls_signals:
                        ls_state_symbol = f"{symbol}::SWEEP::{ls_signal['direction']}"
                        if not state.in_cooldown(saved_state, ls_state_symbol, ls_signal["direction"], ls_signal["candle_time"], config.LIQUIDITY_SWEEP_COOLDOWN_MINUTES):
                            ls_signal["chart_link"] = build_chart_link(symbol, primary_label)
                            state.mark_alerted(saved_state, ls_state_symbol, ls_signal["direction"], ls_signal["candle_time"])
                            try:
                                send_liquidity_sweep_alert(ls_signal)
                                alerts_sent += 1
                            except Exception as e:
                                print(f"send_liquidity_sweep_alert failed for {symbol}: {e}")

                # Moving Average Envelope — see the matching comment
                # in the index branch above. Checked on primary_df.
                if config.ENABLE_MA_ENVELOPE_ALERTS:
                    mv_env = momentum_volume.get(symbol)
                    ema200_val = mv_env["ema_cross"]["ema200"] if mv_env and mv_env.get("ema_cross") else None
                    env_signal = check_ma_envelope(primary_df, symbol, ema200_val)
                    if env_signal is not None:
                        env_state_symbol = f"{symbol}::MAENV::{env_signal['direction']}"
                        if not state.in_cooldown(saved_state, env_state_symbol, env_signal["direction"], env_signal["candle_time"], config.MA_ENVELOPE_COOLDOWN_MINUTES):
                            env_signal["chart_link"] = build_chart_link(symbol, primary_label)
                            state.mark_alerted(saved_state, env_state_symbol, env_signal["direction"], env_signal["candle_time"])
                            try:
                                send_ma_envelope_alert(env_signal)
                                alerts_sent += 1
                            except Exception as e:
                                print(f"send_ma_envelope_alert failed for {symbol}: {e}")

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
                # Screener.in link (added, per request, 2026-09-10 —
                # "shudhu Screener.in link jog koro") — company
                # fundamentals (quarterly results, ratios, etc), one
                # tap away, instead of scraping numbers from NSE's
                # session-protected API (unreliable from GitHub
                # Actions — see the matching note in bulk_block_data.py
                # for why that approach was avoided there too). Real
                # stocks only, never indices/commodities.
                if symbol not in non_stock_symbols:
                    signal["screener_link"] = f"https://www.screener.in/company/{symbol}/consolidated/"
                if mv is not None:
                    signal["momentum"] = signal["close"] > mv["four_week_high_close"]
                    signal["four_week_high_close"] = mv["four_week_high_close"]
                    # Day Volume vs 1-month average (ADDED, per request,
                    # 2026-09-01 — "volume alert tao kore dao same [as
                    # delivery]") — same "today's own value vs this
                    # symbol's own trailing 1-month average" comparison
                    # as Delivery %, but for the previous COMPLETED
                    # trading day's total volume (mv["prev_day_volume"])
                    # vs mv["volume_avg_1m"] — NOT the crossing candle's
                    # own (much smaller) 15-min volume, which is a
                    # different number already shown separately as
                    # "Volume: ..." on the alert.
                    if mv.get("volume_avg_1m"):
                        signal["prev_day_volume"] = mv["prev_day_volume"]
                        signal["volume_avg_1m"] = mv["volume_avg_1m"]
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

                # Volume Support gate (ADDED, per request, 2026-09-05)
                # — MANDATORY: the previous COMPLETED trading day's
                # total volume (signal["prev_day_volume"]) must exceed
                # this symbol's trailing 1-month average daily volume
                # (signal["volume_avg_1m"]), or the signal is rejected
                # outright. Not marked alerted on failure, so it's
                # re-checked next run within the lookback window.
                # Missing daily data (volume_avg_1m never set above)
                # never blocks — same missing-data-never-blocks rule
                # used everywhere else in this file.
                if config.REQUIRE_VOLUME_1M_SUPPORT and signal.get("volume_avg_1m") and signal.get("prev_day_volume") is not None:
                    if signal["prev_day_volume"] <= signal["volume_avg_1m"]:
                        continue

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

                # 1st 15-min / 75-min / 1-month position lines (ADDED,
                # 2026-10-19) — see strategy.compute_position_info.
                signal["positions"] = compute_position_info(df15, df75, signal, mv)
                signal["prev_day"] = prev_day_info(df15, now_ist, signal.get("close"))
                # EMA 9/20 cross — informational, on 15-min and 75-min (2026-10-21)
                _attach_daily_support(signal, symbol, watchlist[symbol])
                for _k, _d in (("ema920_15", df15), ("ema920_75", df75)):
                    try:
                        signal[_k] = get_3min_trend_info(_d, symbol, ema_fast=config.EMA_FAST, ema_slow=config.EMA_SLOW) if _d is not None else None
                    except Exception as _e:
                        print(f"EMA9/20 info failed for {symbol}: {_e}")
                        signal[_k] = None

                if symbol not in non_stock_symbols:
                    signal["is_fno"] = symbol.upper() in fno_underlyings

                    # Delivery % — stocks only, previous trading day's
                    # NSE delivery percentage. Omitted if the symbol
                    # isn't in the bhavcopy (e.g. new listing) or the
                    # fetch failed for the day.
                    deliv = delivery_map.get(symbol.upper())
                    if deliv is not None:
                        signal["delivery_pct"] = deliv
                    deliv_avg = delivery_avg_map.get(symbol.upper())
                    if deliv_avg is not None:
                        signal["delivery_avg_1m"] = deliv_avg

                    # Delivery Support gate (ADDED, per request,
                    # 2026-09-09 — "delivery ar volume ta base koro...
                    # jodi negative hoy to alert asbe na"): MANDATORY —
                    # previous day's delivery % must exceed this
                    # symbol's trailing 1-month average delivery %, or
                    # the signal is rejected outright (not marked
                    # alerted, so it's re-checked next run). Same
                    # missing-data-never-blocks rule as
                    # REQUIRE_VOLUME_1M_SUPPORT above — stocks with no
                    # bhavcopy delivery data (delivery_avg_1m never
                    # set) simply skip this check.
                    if config.REQUIRE_DELIVERY_1M_SUPPORT and signal.get("delivery_avg_1m") and signal.get("delivery_pct") is not None:
                        if signal["delivery_pct"] <= signal["delivery_avg_1m"]:
                            continue

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
                    last_deal = None if config.COMPACT_ALERT_FORMAT else bulk_block_data.get_last_deal_for_symbol(symbol)
                    if last_deal:
                        signal["last_bulk_block_deal"] = last_deal

                    # Monthly RSI(14) — informational only (CHANGED,
                    # per request — "amar ager je alert ache tate
                    # monthly rsi ta add kore dao": folded into this
                    # existing alert instead of being kept as its own
                    # separate scan). Needs a much longer daily
                    # history than momentum_volume's 320-day cache
                    # above provides, so fetched fresh, on-demand,
                    # right here — same "already firing anyway"
                    # pattern as Bulk/Block deal just above. A
                    # failed/short fetch never blocks the alert, just
                    # means no Monthly RSI line on it.
                    monthly_history = [] if config.COMPACT_ALERT_FORMAT else fetch_daily_history(
                        watchlist[symbol],
                        days_back=config.MONTHLY_RSI_HISTORY_LOOKBACK_DAYS,
                        include_today=True,
                    )
                    if monthly_history:
                        monthly_rsi_info = get_monthly_rsi_info(monthly_history)
                        if monthly_rsi_info:
                            signal["monthly_rsi"] = monthly_rsi_info

                    # PCR + Call/Put writing buildup — F&O stocks only
                    # (cash-only stocks have no option chain), fetched
                    # ON-DEMAND right here rather than every run (see
                    # get_stock_oi_buildup) since this alert is about to
                    # fire anyway. Informational only — a failed/slow
                    # fetch never blocks the alert, just means no PCR/
                    # buildup line on it.
                    if signal["is_fno"] and not config.COMPACT_ALERT_FORMAT:
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

                # Signal STRENGTH (2026-10-21, "false signal na ase") — counts how many
                # independent confirmations agree with the cross; weak ones are dropped
                # (not marked alerted). config.ALERT_MIN_STRENGTH_RATIO = 0 sends all.
                signal["strength"] = compute_alert_strength(signal)
                if signal["strength"]["mandatory_failed"] or not signal["strength"]["meets_min"] or _daily_support_blocks(signal) or _macd_bb_blocks(signal, df15, now_ist, levels):
                    print(f"[strength] {symbol} {signal['direction']} {signal['candle_time']} skipped "
                          f"({signal['strength']['passed']}/{signal['strength']['total']}, need {signal['strength']['need']}, failed must: {signal['strength']['mandatory_failed']})", flush=True)
                    continue

                # Same-direction cooldown (added, per request) — blocks
                # only if this exact (symbol, direction) already
                # alerted within config.SAME_DIRECTION_COOLDOWN_MINUTES,
                # even on a genuinely new/different candle. This is
                # separate from state.already_alerted() above, which
                # only catches the exact-same-candle case.
                if state.in_cooldown(saved_state, symbol, signal["direction"], signal["candle_time"], config.SAME_DIRECTION_COOLDOWN_MINUTES):
                    continue
                if getattr(config, "ALERT_ONCE_PER_DAY", False) and state.alerted_today(
                        saved_state, symbol, signal["direction"], signal["candle_time"],
                        getattr(config, "ALERT_ONCE_PER_DAY_ANY_DIRECTION", False)):
                    # already alerted today -> only repeat if the score has clearly improved
                    _prev = state.last_alert_score(saved_state, symbol, signal["direction"], str(signal["candle_time"])[:10])
                    _now = (signal.get("strength") or {}).get("passed")
                    if (not getattr(config, "ALERT_REALERT_IF_SCORE_UP", False) or _prev is None or _now is None
                            or _now - _prev < getattr(config, "ALERT_REALERT_MIN_IMPROVEMENT", 2)):
                        continue
                    signal["score_up_from"] = _prev

                # Mark BEFORE sending: if send_alert() raises after the
                # Telegram API call already succeeded (e.g. a parsing
                # or logging error on our side, post-send), the alert
                # still went out, so we must not leave this candle
                # unmarked -- an unmarked candle gets re-alerted every
                # future run until fixed. Marking first means a
                # send failure costs at most one missed alert, never
                # an infinite duplicate loop.
                if signal.get("is_fno") and getattr(config, "ATM_OPTION_INFO_ENABLED", True):
                    try:
                        signal["option_text"] = get_atm_option_text(symbol, watchlist.get(symbol), now_ist.date(), short=False)
                    except Exception:
                        signal["option_text"] = ""

                state.mark_alerted(saved_state, symbol, signal["direction"], signal["candle_time"])
                try:
                    signal["alert_count"] = state.alert_day_streak(saved_state, symbol, signal["direction"], signal["candle_time"])
                except Exception:
                    pass
                _log_alert_for_eod(saved_state, signal, symbol, watchlist.get(symbol))
                # Stock News (added, per request, 2026-09-10) — fetched
                # only now, for symbols actually about to alert, not
                # for every scanned symbol. See fetch_stock_news above.
                if config.ENABLE_STOCK_NEWS and not config.COMPACT_ALERT_FORMAT:
                    signal["news"] = fetch_stock_news(symbol, config.STOCK_NEWS_MAX_HEADLINES)
                try:
                    send_alert(signal)
                    alerts_sent += 1
                    # Trading Score Summary (added, per request) —
                    # collect every stock whose alert actually went
                    # out this run (i.e. passed the EMA-cross gate +
                    # Trading Score GOOD/STRONG gate), for the
                    # combined summary message sent once after this
                    # whole scan finishes (see run()).
                    trading_score_summary_hits.append({
                        "symbol": symbol,
                        "direction": signal["direction"],
                        "trading_score": signal.get("trading_score"),
                        "chart_link": signal.get("chart_link"),
                        "is_fno": symbol.upper() in fno_underlyings,
                    })
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

    for _w, _hits in orb_hits.items():
        if _hits:
            send_orb_summary(_hits, now_ist, _w)
            alerts_sent += len(_hits)
    for _k, _hits in bb_hits.items():
        if _hits:
            send_bb_summary(_hits, now_ist, _k)
            alerts_sent += len(_hits)
    if move15_hits:
        send_move15_summary(move15_hits, now_ist)
        alerts_sent += len(move15_hits)

    state.save_state(saved_state)
    if failed_symbols:
        print(f"{len(failed_symbols)} instrument(s) failed to fetch this run: {failed_symbols}")
    print(f"Done. {alerts_sent} alert(s) sent.")
    return alerts_sent, failed_symbols, len(watchlist), daily_score_report_hits, trading_score_summary_hits


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
        return 0, [], 0, [], []

    print(f"Scanning {len(watchlist)} Nifty 500 cash stocks (EMA{config.NIFTY500_EMA_FAST}/{config.NIFTY500_EMA_SLOW})...")

    pivots = build_pivot_levels(watchlist)

    # Momentum (price vs last 4 weeks' high) / Volume Spike (daily
    # volume vs 5 trading days ago) — see build_momentum_volume_data.
    momentum_volume = build_momentum_volume_data(watchlist)

    delivery_map = delivery_data.get_delivery_data(now_ist.date())
    delivery_avg_map = delivery_data.get_delivery_avg_1m(now_ist.date())
    saved_state = state.load_state()
    alerts_sent = 0
    daily_score_report_hits = []
    trading_score_summary_hits = []
    # Consolidation Breakout — LIVE hits collected across this whole
    # run (see matching comment in run_fo_scan above), sent as ONE
    # batched message at the end.
    consolidation_live_hits = []
    orb_hits = {w: [] for w in _orb_windows()}
    bb_hits = {"break": [], "reentry": []}
    move15_hits = []

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
            # Opening-range-break alerts (1st 15-min / 30-min / 1-hour, price above/below VWAP) — stocks only.
            _orb_scan_symbol(symbol, df5, df15, prev_close, now_ist, saved_state, watchlist.get(symbol), fno_underlyings, orb_hits)
            _bb_scan_symbol(symbol, df15, now_ist, saved_state, fno_underlyings, bb_hits, watchlist.get(symbol), levels, df5)
            _move15_scan_symbol(symbol, df15, prev_close, now_ist, saved_state, watchlist.get(symbol), fno_underlyings, move15_hits)
            # ONLY-SELECTED-ALERTS MODE (2026-11-21): when MACD15_ALERT_ENABLED is False the 15-min MACD alert is sent for commodities only (MACD15_ALERT_COMMODITIES)
            if not _macd15_allowed(symbol):
                continue
            # prev_day_high/prev_day_low (added, per request, 2026-09-05)
            # — feeds the Liquidity Sweep scan below.
            prev_day_high = levels.get("high") if levels else None
            prev_day_low = levels.get("low") if levels else None

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
                require_vwap_confirmation=config.REQUIRE_VWAP_CONFIRMATION,
                require_macd_divergence=config.REQUIRE_MACD_DIVERGENCE,
                today=now_ist.date(),
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

            # Liquidity Sweep — see the matching comment in run_fo_scan
            # above. Safe from double-firing with run_fo_scan's own
            # sweep check since F&O symbols are skipped entirely at
            # the top of this loop.
            if config.ENABLE_LIQUIDITY_SWEEP_ALERTS:
                ls_signals = check_liquidity_sweep_scan(
                    primary_df, symbol,
                    prev_day_high=prev_day_high,
                    prev_day_low=prev_day_low,
                )
                for ls_signal in ls_signals:
                    ls_state_symbol = f"{symbol}::SWEEP::{ls_signal['direction']}"
                    if not state.in_cooldown(saved_state, ls_state_symbol, ls_signal["direction"], ls_signal["candle_time"], config.LIQUIDITY_SWEEP_COOLDOWN_MINUTES):
                        ls_signal["chart_link"] = build_chart_link(symbol, primary_label)
                        state.mark_alerted(saved_state, ls_state_symbol, ls_signal["direction"], ls_signal["candle_time"])
                        try:
                            send_liquidity_sweep_alert(ls_signal)
                            alerts_sent += 1
                        except Exception as e:
                            print(f"send_liquidity_sweep_alert failed for {symbol}: {e}")

            # Moving Average Envelope — see the matching comment in
            # run_fo_scan above.
            if config.ENABLE_MA_ENVELOPE_ALERTS:
                mv_env = momentum_volume.get(symbol)
                ema200_val = mv_env["ema_cross"]["ema200"] if mv_env and mv_env.get("ema_cross") else None
                env_signal = check_ma_envelope(primary_df, symbol, ema200_val)
                if env_signal is not None:
                    env_state_symbol = f"{symbol}::MAENV::{env_signal['direction']}"
                    if not state.in_cooldown(saved_state, env_state_symbol, env_signal["direction"], env_signal["candle_time"], config.MA_ENVELOPE_COOLDOWN_MINUTES):
                        env_signal["chart_link"] = build_chart_link(symbol, primary_label)
                        state.mark_alerted(saved_state, env_state_symbol, env_signal["direction"], env_signal["candle_time"])
                        try:
                            send_ma_envelope_alert(env_signal)
                            alerts_sent += 1
                        except Exception as e:
                            print(f"send_ma_envelope_alert failed for {symbol}: {e}")

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
                # Screener.in link — see the matching comment in
                # run_fo_scan above. Every symbol here is a real stock
                # (this scan never covers indices/commodities), so no
                # non_stock_symbols check needed.
                signal["screener_link"] = f"https://www.screener.in/company/{symbol}/consolidated/"
                mv = momentum_volume.get(symbol)
                if mv is not None:
                    signal["momentum"] = signal["close"] > mv["four_week_high_close"]
                    signal["four_week_high_close"] = mv["four_week_high_close"]
                    if mv.get("volume_avg_1m"):
                        signal["prev_day_volume"] = mv["prev_day_volume"]
                        signal["volume_avg_1m"] = mv["volume_avg_1m"]
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

                # Volume Support gate — see the matching comment in
                # run_fo_scan above (config.REQUIRE_VOLUME_1M_SUPPORT).
                if config.REQUIRE_VOLUME_1M_SUPPORT and signal.get("volume_avg_1m") and signal.get("prev_day_volume") is not None:
                    if signal["prev_day_volume"] <= signal["volume_avg_1m"]:
                        continue

                # 15-Minute Intraday Trade Checklist — see the matching
                # comment in run_fo_scan above.
                signal["intraday_checklist"] = compute_intraday_checklist(signal)

                # 1st 15-min Buy/Sell volume ESTIMATE — see the
                # matching comment in run_fo_scan above.
                signal["opening_buy_sell"] = get_opening_candle_buy_sell_estimate(df15, symbol)

                # 1st 15-min / 75-min / 1-month position lines (ADDED,
                # 2026-10-19) — see strategy.compute_position_info.
                signal["positions"] = compute_position_info(df15, df75, signal, mv)
                signal["prev_day"] = prev_day_info(df15, now_ist, signal.get("close"))
                # EMA 9/20 cross — informational, on 15-min and 75-min (2026-10-21)
                _attach_daily_support(signal, symbol, watchlist[symbol])
                for _k, _d in (("ema920_15", df15), ("ema920_75", df75)):
                    try:
                        signal[_k] = get_3min_trend_info(_d, symbol, ema_fast=config.EMA_FAST, ema_slow=config.EMA_SLOW) if _d is not None else None
                    except Exception as _e:
                        print(f"EMA9/20 info failed for {symbol}: {_e}")
                        signal[_k] = None

                signal["is_fno"] = symbol.upper() in fno_underlyings

                deliv = delivery_map.get(symbol.upper())
                if deliv is not None:
                    signal["delivery_pct"] = deliv
                deliv_avg = delivery_avg_map.get(symbol.upper())
                if deliv_avg is not None:
                    signal["delivery_avg_1m"] = deliv_avg

                # Delivery Support gate — see the matching comment in
                # run_fo_scan above (config.REQUIRE_DELIVERY_1M_SUPPORT).
                if config.REQUIRE_DELIVERY_1M_SUPPORT and signal.get("delivery_avg_1m") and signal.get("delivery_pct") is not None:
                    if signal["delivery_pct"] <= signal["delivery_avg_1m"]:
                        continue

                last_deal = None if config.COMPACT_ALERT_FORMAT else bulk_block_data.get_last_deal_for_symbol(symbol)
                if last_deal:
                    signal["last_bulk_block_deal"] = last_deal

                # Monthly RSI(14) — see the matching comment in
                # run_fo_scan above. Every symbol here is a real
                # stock (this scan never covers indices/commodities),
                # so no non_stock_symbols check needed.
                monthly_history = [] if config.COMPACT_ALERT_FORMAT else fetch_daily_history(
                    watchlist[symbol],
                    days_back=config.MONTHLY_RSI_HISTORY_LOOKBACK_DAYS,
                    include_today=True,
                )
                if monthly_history:
                    monthly_rsi_info = get_monthly_rsi_info(monthly_history)
                    if monthly_rsi_info:
                        signal["monthly_rsi"] = monthly_rsi_info

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

                # Signal STRENGTH (2026-10-21, "false signal na ase") — counts how many
                # independent confirmations agree with the cross; weak ones are dropped
                # (not marked alerted). config.ALERT_MIN_STRENGTH_RATIO = 0 sends all.
                signal["strength"] = compute_alert_strength(signal)
                if signal["strength"]["mandatory_failed"] or not signal["strength"]["meets_min"] or _daily_support_blocks(signal) or _macd_bb_blocks(signal, df15, now_ist, levels):
                    print(f"[strength] {symbol} {signal['direction']} {signal['candle_time']} skipped "
                          f"({signal['strength']['passed']}/{signal['strength']['total']}, need {signal['strength']['need']}, failed must: {signal['strength']['mandatory_failed']})", flush=True)
                    continue

                # Same-direction cooldown — see the matching comment in
                # run_fo_scan() above.
                if state.in_cooldown(saved_state, state_symbol, signal["direction"], signal["candle_time"], config.SAME_DIRECTION_COOLDOWN_MINUTES):
                    continue
                if getattr(config, "ALERT_ONCE_PER_DAY", False) and state.alerted_today(
                        saved_state, state_symbol, signal["direction"], signal["candle_time"],
                        getattr(config, "ALERT_ONCE_PER_DAY_ANY_DIRECTION", False)):
                    # already alerted today -> only repeat if the score has clearly improved
                    _prev = state.last_alert_score(saved_state, symbol, signal["direction"], str(signal["candle_time"])[:10])
                    _now = (signal.get("strength") or {}).get("passed")
                    if (not getattr(config, "ALERT_REALERT_IF_SCORE_UP", False) or _prev is None or _now is None
                            or _now - _prev < getattr(config, "ALERT_REALERT_MIN_IMPROVEMENT", 2)):
                        continue
                    signal["score_up_from"] = _prev

                # Mark BEFORE sending -- see the matching comment in
                # run_fo_scan() above for why.
                if signal.get("is_fno") and getattr(config, "ATM_OPTION_INFO_ENABLED", True):
                    try:
                        signal["option_text"] = get_atm_option_text(symbol, watchlist.get(symbol), now_ist.date(), short=False)
                    except Exception:
                        signal["option_text"] = ""
                state.mark_alerted(saved_state, state_symbol, signal["direction"], signal["candle_time"])
                try:
                    signal["alert_count"] = state.alert_day_streak(saved_state, state_symbol, signal["direction"], signal["candle_time"])
                except Exception:
                    pass
                _log_alert_for_eod(saved_state, signal, symbol, watchlist.get(symbol))
                # Stock News — see the matching comment in run_fo_scan
                # above.
                if config.ENABLE_STOCK_NEWS and not config.COMPACT_ALERT_FORMAT:
                    signal["news"] = fetch_stock_news(symbol, config.STOCK_NEWS_MAX_HEADLINES)
                try:
                    send_alert(signal)
                    alerts_sent += 1
                    # Trading Score Summary — see the matching comment
                    # in run_fo_scan() above. Every symbol reaching
                    # this loop is guaranteed NOT F&O (see the skip at
                    # the top of this loop), so is_fno is always False
                    # here — same "no overlap" guarantee as the Daily
                    # Score Report's CASH half.
                    trading_score_summary_hits.append({
                        "symbol": symbol,
                        "direction": signal["direction"],
                        "trading_score": signal.get("trading_score"),
                        "chart_link": signal.get("chart_link"),
                        "is_fno": False,
                    })
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

    for _w, _hits in orb_hits.items():
        if _hits:
            send_orb_summary(_hits, now_ist, _w)
            alerts_sent += len(_hits)
    for _k, _hits in bb_hits.items():
        if _hits:
            send_bb_summary(_hits, now_ist, _k)
            alerts_sent += len(_hits)
    if move15_hits:
        send_move15_summary(move15_hits, now_ist)
        alerts_sent += len(move15_hits)

    state.save_state(saved_state)
    if failed_symbols:
        print(f"{len(failed_symbols)} Nifty 500 instrument(s) failed to fetch this run: {failed_symbols}")
    print(f"Nifty 500 scan done. {alerts_sent} alert(s) sent.")
    return alerts_sent, failed_symbols, len(watchlist), daily_score_report_hits, trading_score_summary_hits


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


def run_swing_entry_scan(now_ist):
    """
    Standalone Swing-Trade ENTRY Confluence Score screener (added,
    per request — "ekta perfect swing trade er alert ready kore
    dao... sob combine kore ekta confluence score"). NOT financial
    advice — a mechanical, rules-based technical screener; see
    strategy.check_swing_entry_scan / compute_swing_confluence_score
    for exactly what the 5 components are and how entry/stop/target
    are derived. Own SCAN_MODE ("swing_entry_scan"), meant to run
    once/day at/after market close (so today's daily candle is
    settled).

    Same Nifty 500 cash universe as the other once/day scans
    (build_nifty500_watchlist), and — unlike run_breakout_scan —
    needs only DAILY candles (fetch_daily_history), no intraday
    fetch/resample.

    Dedup key is (symbol, "SWING_ENTRY", date) — today's date string
    — so this fires at most once per stock per day, which combined
    with check_swing_entry_scan's own "fresh cross only" logic means
    a stock that stays in the qualifying zone for a week is only
    reported on the day it first crossed in.
    """
    watchlist = build_nifty500_watchlist(now_ist)
    if not watchlist:
        print("Swing entry scan: empty watchlist (outside session or list unavailable) — skipping.")
        return 0, [], 0

    print(f"Swing entry scan: scanning {len(watchlist)} Nifty 500 stocks...")

    saved_state = state.load_state()
    hits = []
    failed_symbols = []

    for symbol, instrument_key in watchlist.items():
        try:
            history = fetch_daily_history(
                instrument_key,
                days_back=config.SWING_HISTORY_LOOKBACK_DAYS,
                include_today=True,
            )
            if not history:
                failed_symbols.append(symbol)
                continue

            signal = check_swing_entry_scan(history, symbol)
            if signal is None:
                continue

            state_symbol = f"{symbol}::SWING_ENTRY"
            if state.already_alerted(saved_state, state_symbol, "BULLISH", signal["date"]):
                continue

            signal["chart_link"] = build_chart_link(symbol)
            state.mark_alerted(saved_state, state_symbol, "BULLISH", signal["date"])
            hits.append(signal)

        except Exception as e:
            print(f"Error on {symbol} (Swing entry scan): {e}")
            failed_symbols.append(symbol)

    if hits:
        send_swing_entry_summary(hits, now_ist)

    state.save_state(saved_state)
    if failed_symbols:
        print(f"{len(failed_symbols)} Swing-entry-scan instrument(s) failed to fetch this run: {failed_symbols}")
    print(f"Swing entry scan done. {len(hits)} stock(s) reported.")
    return len(hits), failed_symbols, len(watchlist)


def _tv_daily_link(symbol):
    """TradingView DAILY chart link (interval=D) — used by the daily
    EMA50 scan alert alongside the Groww link, so a Groww outage never
    leaves the alert without a working chart."""
    tv_symbol = config.TRADINGVIEW_SYMBOL_OVERRIDES.get(symbol, f"NSE:{symbol}")
    return f"https://www.tradingview.com/chart/?symbol={tv_symbol}&interval=D"


def _send_telegram_chunks(lines, header, max_chars=3800):
    """Sends `lines` as one or more Telegram messages (each under
    Telegram's 4096-char cap), repeating `header` on each part."""
    parts, cur = [], header
    for ln in lines:
        if len(cur) + len(ln) + 2 > max_chars and cur != header:
            parts.append(cur)
            cur = header
        cur += "\n\n" + ln
    parts.append(cur)
    for part in parts:
        send_telegram_text(part)


def send_daily_ema50_summary(hits, now_ist):
    hits = sorted(hits, key=lambda h: -(h.get("volume_vs_20d_avg") or 0))
    header = f"\U0001F4C8 DAILY 50-EMA CROSS + MACD CROSS + RSI>{config.DAILY_EMA50_RSI_MIN}  ({now_ist.strftime('%d %b %Y')})\n{len(hits)} stock(s)"
    lines = []
    for h in hits:
        chg = f"{h['day_change_pct']:+.2f}%" if h.get("day_change_pct") is not None else "-"
        vol = f"{h['volume_vs_20d_avg']:.1f}x" if h.get("volume_vs_20d_avg") is not None else "-"
        when = "today" if h["macd_cross_days_ago"] == 0 else f"{h['macd_cross_days_ago']}d ago"
        lines.append(
            f"\U0001F7E2 {h['symbol']}  Close {h['close']} ({chg})\n"
            f"EMA50 {h['ema50']} | RSI {h['rsi']} | MACD cross: {when}\n"
            f"Volume vs 20d avg: {vol}\n"
            f"TradingView (1D): {h['tv_link']}\n"
            f"Groww: {h['chart_link']}"
        )
    _send_telegram_chunks(lines, header)


def run_daily_ema50_scan(now_ist):
    """
    Daily 50-EMA cross + MACD cross + RSI>50 scan (added, per request
    2026-10-09). Own SCAN_MODE ("daily_ema50_scan"), meant to run ONCE a
    day after the close (~16:00-16:30 IST) so today's daily candle is
    final. See strategy.check_daily_ema50_macd_scan for the exact rules
    and the DAILY_EMA50_* block in config.py for every knob.

    Universe: F&O stocks + Nifty 500 (union), deliberately WITHOUT the
    session-time gate build_nifty500_watchlist has (that one returns {}
    after 15:30, which is exactly when this scan should run).

    Safety: if too many symbols have no candle dated TODAY yet (ran too
    early, holiday, Upstox lag), it sends a "not ready" notice and
    exits without marking anything alerted, instead of scanning stale
    data. Dedup key is (symbol::DAILY_EMA50, BULLISH, today's date).
    """
    today_str = now_ist.date().isoformat()
    watchlist = {}
    watchlist.update(instruments.resolve_fo_stock_list(None))
    watchlist.update(instruments.resolve_nifty500_stocks())
    if not watchlist:
        print("Daily EMA50 scan: empty watchlist — skipping.")
        send_telegram_text("Daily EMA50 scan: stock list unavailable, scan skipped.")
        return 0, [], 0

    print(f"Daily EMA50 scan: scanning {len(watchlist)} stocks...", flush=True)

    def _fetch_one(symbol, instrument_key):
        return symbol, fetch_daily_history(
            instrument_key,
            days_back=config.DAILY_EMA50_HISTORY_LOOKBACK_DAYS,
            include_today=True,
        )

    histories, failed_symbols = {}, []
    with ThreadPoolExecutor(max_workers=config.PIVOT_FETCH_WORKERS) as pool:
        futures = {pool.submit(_fetch_one, sym, key): sym for sym, key in watchlist.items()}
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                _, hist = fut.result()
                if hist:
                    histories[sym] = hist
                else:
                    failed_symbols.append(sym)
            except Exception as e:
                print(f"Fetch failed for {sym} (Daily EMA50 scan): {e}")
                failed_symbols.append(sym)

    # stale-data guard: last candle must be dated today
    fresh = {s: h for s, h in histories.items() if str(h[-1]["date"])[:10] == today_str}
    if not histories or (len(histories) - len(fresh)) / len(histories) > config.DAILY_EMA50_STALE_ABORT_FRACTION:
        msg = (f"Daily EMA50 scan: today's ({today_str}) daily candle isn't available yet for "
               f"{len(histories) - len(fresh)}/{len(histories)} stocks (market holiday, or ran too early). "
               f"Nothing scanned — run again a bit later.")
        print(msg, flush=True)
        send_telegram_text(msg)
        return 0, failed_symbols, len(watchlist)

    saved_state = state.load_state()
    hits = []
    already_sent = 0
    for symbol, history in fresh.items():
        try:
            signal = check_daily_ema50_macd_scan(history, symbol)
            if signal is None:
                continue
            state_symbol = f"{symbol}::DAILY_EMA50"
            if state.already_alerted(saved_state, state_symbol, "BULLISH", today_str):
                already_sent += 1
                continue
            signal["chart_link"] = build_chart_link(symbol)
            signal["tv_link"] = _tv_daily_link(symbol)
            state.mark_alerted(saved_state, state_symbol, "BULLISH", today_str)
            hits.append(signal)
        except Exception as e:
            print(f"Error on {symbol} (Daily EMA50 scan): {e}")
            failed_symbols.append(symbol)

    if hits:
        send_daily_ema50_summary(hits, now_ist)
    elif config.DAILY_EMA50_SEND_EMPTY_NOTICE:
        extra = f" ({already_sent} qualifying stock(s) were already alerted earlier today.)" if already_sent else ""
        send_telegram_text(
            f"Daily 50-EMA + MACD + RSI scan ({now_ist.strftime('%d %b %Y')}): no NEW stock qualified. "
            f"{len(fresh)} stocks scanned.{extra}"
        )

    state.save_state(saved_state)
    if failed_symbols:
        print(f"{len(failed_symbols)} Daily-EMA50-scan instrument(s) failed: {failed_symbols}")
    print(f"Daily EMA50 scan done. {len(hits)} stock(s) reported.", flush=True)
    return len(hits), failed_symbols, len(watchlist)


def send_monthly_macd_rsi_summary(hits, now_ist):
    hits = sorted(hits, key=lambda h: (h["direction"] != "BULLISH", h["symbol"]))
    header = f"\U0001F5D3 1-MONTH MACD + RSI CROSS  ({now_ist.strftime('%d %b %Y')})\n{len(hits)} symbol(s)"
    lines = []
    for h in hits:
        icon = "\U0001F7E2\u2B06\uFE0F" if h["direction"] == "BULLISH" else "\U0001F534\u2B07\uFE0F"
        chg = f"{h['month_change_pct']:+.2f}%" if h.get("month_change_pct") is not None else "-"
        note = " (month so far - can still flip)" if h["in_progress"] else " (month closed)"
        sym = f"<a href=\"{h['chart_link']}\">{h['symbol']}</a>" if h.get("chart_link") else h["symbol"]
        lines.append(
            f"{icon} <b>{sym}</b> {h['direction']} - {h['month']}{note}\n"
            f"Close {h['close']} | Month change {chg}\n"
            f"MACD {h['macd_line']} / Signal {h['macd_signal']} | RSI {h['rsi']}"
        )
    parts, cur = [], header
    for ln in lines:
        if len(cur) + len(ln) + 2 > 3800 and cur != header:
            parts.append(cur); cur = header
        cur += "\n\n" + ln
    parts.append(cur)
    for part in parts:
        _send_html_text(part)


def _send_html_text(text):
    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={"chat_id": config.TELEGRAM_CHAT_ID, "text": text,
                                     "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print("Telegram send failed (monthly):", e)


def run_monthly_macd_rsi_scan(now_ist):
    """
    1-month MACD cross + RSI alert (ADDED 2026-10-19 — "15 minite eo cross
    e asbe abar 1 month eo asbe"). See strategy.check_monthly_macd_rsi_scan
    and the MONTHLY_ALERT_* block in config.py. Universe: F&O stocks +
    indices (+ Nifty 500 if config.MONTHLY_ALERT_UNIVERSE == "fo+n500").
    One combined message; each (symbol, direction, month) alerted once.
    """
    watchlist = {}
    watchlist.update(instruments.resolve_fo_stock_list(None))
    watchlist.update(instruments.resolve_indices(config.INDICES))
    if config.MONTHLY_ALERT_UNIVERSE == "fo+n500":
        watchlist.update(instruments.resolve_nifty500_stocks())
    if not watchlist:
        print("Monthly scan: empty watchlist - skipping.", flush=True)
        return 0, [], 0

    print(f"Monthly MACD+RSI scan: {len(watchlist)} instruments...", flush=True)

    def _fetch_one(symbol, key):
        return symbol, fetch_daily_history(key, days_back=config.MONTHLY_RSI_HISTORY_LOOKBACK_DAYS, include_today=True)

    histories, failed = {}, []
    with ThreadPoolExecutor(max_workers=config.PIVOT_FETCH_WORKERS) as pool:
        futures = {pool.submit(_fetch_one, s, k): s for s, k in watchlist.items()}
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                _, hist = fut.result()
                if hist:
                    histories[sym] = hist
                else:
                    failed.append(sym)
            except Exception as e:
                print(f"Fetch failed for {sym} (monthly scan): {e}")
                failed.append(sym)

    saved_state = state.load_state()
    hits = []
    for symbol, history in histories.items():
        try:
            sig = check_monthly_macd_rsi_scan(history, symbol)
            if sig is None:
                continue
            state_symbol = f"{symbol}::MONTHLY"
            if state.already_alerted(saved_state, state_symbol, sig["direction"], sig["month"]):
                continue
            sig["chart_link"] = build_chart_link(symbol)
            state.mark_alerted(saved_state, state_symbol, sig["direction"], sig["month"])
            hits.append(sig)
        except Exception as e:
            print(f"Error on {symbol} (monthly scan): {e}")
            failed.append(symbol)

    if hits:
        send_monthly_macd_rsi_summary(hits, now_ist)
    state.save_state(saved_state)
    print(f"Monthly scan done. {len(hits)} new cross(es).", flush=True)
    return len(hits), failed, len(watchlist)


def maybe_run_monthly_scan_once_a_day(now_ist):
    """Called from every normal run; does real work only on the first run
    of the day at/after config.MONTHLY_ALERT_RUN_AFTER IST (weekdays)."""
    if not config.MONTHLY_ALERT_ENABLED or now_ist.weekday() >= 5:
        return
    hh, mm = (int(x) for x in config.MONTHLY_ALERT_RUN_AFTER.split(":"))
    if now_ist.time() < dt.time(hh, mm):
        return
    today = now_ist.date().isoformat()
    st = state.load_state()
    if state.already_alerted(st, "MONTHLY_SCAN", "DONE", today):
        return
    state.mark_alerted(st, "MONTHLY_SCAN", "DONE", today)  # mark first: a crash must not retry every 3 min
    state.save_state(st)
    run_monthly_macd_rsi_scan(now_ist)


def _fetch_daily_histories(watchlist, days_back, include_today=True):
    """{symbol: daily history list}, [failed symbols] — threaded, rate-limited."""
    def _one(symbol, key):
        return symbol, fetch_daily_history(key, days_back=days_back, include_today=include_today)

    histories, failed = {}, []
    with ThreadPoolExecutor(max_workers=config.PIVOT_FETCH_WORKERS) as pool:
        futures = {pool.submit(_one, s, k): s for s, k in watchlist.items()}
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                _, hist = fut.result()
                if hist:
                    histories[sym] = hist
                else:
                    failed.append(sym)
            except Exception as e:
                print(f"Fetch failed for {sym} (daily scan): {e}")
                failed.append(sym)
    return histories, failed


def send_daily_macd_rsi_summary(hits, now_ist):
    """ONE combined message (split only if it exceeds Telegram's size limit):
    bullish first, then bearish, strongest first inside each group."""
    hits = sorted(hits, key=lambda h: (h["direction"] != "BULLISH", -h["passed"] / max(1, h["total"]), h["symbol"]))
    nb = sum(1 for h in hits if h["direction"] == "BULLISH")
    cdate = dt.date.fromisoformat(hits[0]["date"]).strftime("%d %b %Y") if hits else now_ist.strftime("%d %b %Y")
    counts = f"\U0001F7E2 {nb} bullish" + ("" if config.DAILY_ALERT_BULLISH_ONLY else f"   \U0001F534 {len(hits) - nb} bearish")
    header = (f"\U0001F4C5 <b>DAILY MACD + RSI CROSS (UP)</b>  (candle of {cdate})\n{counts}"
              if config.DAILY_ALERT_BULLISH_ONLY else
              f"\U0001F4C5 <b>DAILY MACD + RSI CROSS</b>  (candle of {cdate})\n{counts}")
    lines = []
    for h in hits:
        icon = "\U0001F7E2\u2B06\uFE0F" if h["direction"] == "BULLISH" else "\U0001F534\u2B07\uFE0F"
        ratio = h["passed"] / max(1, h["total"])
        fire = " \U0001F525" if ratio >= 0.75 else ""
        chg = f" ({h['day_change_pct']:+.2f}%)" if h.get("day_change_pct") is not None else ""
        sym = h["symbol"]
        links = []
        if h.get("chart_link"):
            links.append(f"\U0001F4C8 <a href=\"{h['chart_link']}\">Chart</a>")
        if h.get("results_link"):
            links.append(f"\U0001F4CA <a href=\"{h['results_link']}\">Results</a>")
        link_line = ("  ·  ".join(links) + "\n") if links else ""
        extra = ""
        if h.get("cross_days_ago"):
            extra += f" | cross {h['cross_days_ago']}d ago"
        if h.get("volume_vs_avg_pct") is not None:
            extra += f" | Vol {h['volume_vs_avg_pct']:+.0f}%"
        if h.get("delivery_vs_avg_pct") is not None:
            extra += f" | Deliv {h['delivery_vs_avg_pct']:+.0f}%"
        missing = [n for n, ok in h["checks"] if not ok]
        miss = ("\n\u274C " + ", ".join(missing)) if missing else ""
        vd = []
        _lbl = "RSI crossed 55" if h.get("trigger") == "RSI" else None
        if h.get("trigger") == "RSI" and h["direction"] != "BULLISH":
            _lbl = f"RSI crossed {config.ALERT_STRONG_RSI_BEARISH} (down)"
        if _lbl:
            vd.append("\U0001F500 " + _lbl + (f" {h['cross_days_ago']}d ago" if h.get("cross_days_ago") else " today"))
        elif h.get("cross_days_ago"):
            vd.append(f"cross {h['cross_days_ago']}d ago")
        if h.get("volume_vs_avg_pct") is not None:
            vd.append(f"Vol {h['volume_vs_avg_pct']:+.0f}%")
        if h.get("delivery_vs_avg_pct") is not None:
            vd.append(f"Deliv {h['delivery_vs_avg_pct']:+.0f}%")
        vd_line = (" | ".join(vd) + "\n") if vd else ""
        # order (2026-10-22): volume/delivery on top, MACD+RSI info at the very bottom
        lines.append(
            f"{icon} <b>{sym}</b>  {h['close']}{chg}  \U0001F4AA {h['passed']}/{h['total']}{fire}\n"
            f"{vd_line}"
            f"{link_line}"
            f"{miss.lstrip(chr(10)) + chr(10) if miss else ''}"
            f"MACD {h['macd_line']} / {h['macd_signal']} | RSI {h['rsi']}"
        )
    parts, cur = [], header
    for ln in lines:
        if len(cur) + len(ln) + 2 > 3800 and cur != header:
            parts.append(cur); cur = header
        cur += "\n\n" + ln
    parts.append(cur)
    for part in parts:
        _send_html_text(part)


def run_daily_macd_rsi_scan(now_ist, notify_if_stale=True):
    """
    DAILY MACD+RSI alert (ADDED 2026-10-22). Universe: F&O stocks + indices
    (+ Nifty 500 if config.DAILY_ALERT_UNIVERSE == "fo+n500"). One combined
    message; each (symbol, direction, date) alerted once. See
    strategy.check_daily_macd_rsi_scan and the DAILY_ALERT_* block in config.py.
    """
    today_str = now_ist.date().isoformat()
    watchlist = {}
    watchlist.update(instruments.resolve_fo_stock_list(None))
    watchlist.update(instruments.resolve_indices(config.INDICES))
    if config.DAILY_ALERT_UNIVERSE == "fo+n500":
        watchlist.update(instruments.resolve_nifty500_stocks())
    if not watchlist:
        print("Daily MACD+RSI scan: empty watchlist - skipping.", flush=True)
        return 0, [], 0
    print(f"Daily MACD+RSI scan: {len(watchlist)} instruments...", flush=True)

    # Morning run (the default, before 15:45) judges the LAST COMPLETED daily candle
    # (yesterday / last trading day) — never today's still-forming one. A run at/after
    # 15:45 judges today's own candle instead.
    use_today = now_ist.time() >= dt.time(15, 45)
    histories, failed = _fetch_daily_histories(
        watchlist, config.DAILY_ALERT_HISTORY_LOOKBACK_DAYS, include_today=use_today)
    if not use_today:   # safety: drop a partial row for today if the API sent one anyway
        histories = {s: [r for r in h if str(r["date"])[:10] < today_str] for s, h in histories.items()}
        histories = {s: h for s, h in histories.items() if h}
    latest = max((str(h[-1]["date"])[:10] for h in histories.values()), default=None)
    fresh = {s: h for s, h in histories.items() if str(h[-1]["date"])[:10] == latest}
    stale = (not histories) or (use_today and latest != today_str) or \
            (len(histories) - len(fresh)) / len(histories) > config.DAILY_ALERT_STALE_ABORT_FRACTION
    if stale:
        msg = (f"Daily MACD+RSI scan: the latest daily candle ({latest}) isn't available for "
               f"{len(histories) - len(fresh)}/{len(histories)} symbols (holiday, or ran too early) - nothing scanned.")
        print(msg, flush=True)
        if notify_if_stale:
            send_telegram_text(msg)
        return 0, failed, len(watchlist)

    try:
        delivery_map = delivery_data.get_delivery_data(now_ist.date()) or {}
        delivery_avg_map = delivery_data.get_delivery_avg_1m(now_ist.date()) or {}
    except Exception as e:
        print(f"Daily scan: delivery data unavailable ({e})", flush=True)
        delivery_map, delivery_avg_map = {}, {}

    saved_state = state.load_state()
    hits, below_bar = [], 0
    for symbol, history in fresh.items():
        try:
            sig = check_daily_macd_rsi_scan(
                history, symbol,
                delivery_pct=delivery_map.get(symbol.upper()),
                delivery_avg_1m=delivery_avg_map.get(symbol.upper()),
            )
            if sig is None:
                continue
            if config.DAILY_ALERT_BULLISH_ONLY and sig["direction"] != "BULLISH":
                continue
            if not sig["meets_min"]:
                below_bar += 1
                continue
            state_symbol = f"{symbol}::DAILY"
            if state.already_alerted(saved_state, state_symbol, sig["direction"], sig["date"]):
                continue
            sig["chart_link"] = build_chart_link(symbol)
            if symbol not in set(config.INDICES.keys()):
                sig["results_link"] = f"https://www.screener.in/company/{symbol}/consolidated/"
            state.mark_alerted(saved_state, state_symbol, sig["direction"], sig["date"])
            hits.append(sig)
        except Exception as e:
            print(f"Error on {symbol} (daily scan): {e}")
            failed.append(symbol)

    if hits:
        send_daily_macd_rsi_summary(hits, now_ist)
    elif getattr(config, "DAILY_ALERT_NOTIFY_WHEN_EMPTY", False):
        try:
            send_telegram_text(
                f"\U0001F4C5 <b>DAILY MACD + RSI</b>: scan chalechhe ({len(fresh)} symbol) — "
                f"aj kono bullish alert nei. {below_bar} ta cross bar-er niche."
            )
        except Exception as e:
            print(f"Daily empty note failed: {e}", flush=True)
    state.save_state(saved_state)
    print(f"Daily scan done. {len(hits)} alert(s); {below_bar} cross(es) below the strength bar.", flush=True)
    return len(hits), failed, len(watchlist)


def _log_alert_for_eod(st, signal, symbol, instrument_key):
    """Remember this alert (stocks only) for the end-of-day summary. Never raises."""
    try:
        if not instrument_key or not str(instrument_key).startswith("NSE_EQ"):
            return
        stt = signal.get("strength") or {}
        state.log_alert(st, {
            "symbol": symbol,
            "direction": signal["direction"],
            "time": str(signal["candle_time"]),
            "price": float(signal["close"]),
            "key": instrument_key,
            "score": f"{stt.get('passed')}/{stt.get('total')}" if stt.get("total") else "",
            "fno": bool(signal.get("is_fno")),
            "day": signal.get("alert_count"),
            "nifty": signal.get("nifty_trend"),
            "sector": _sector_label(symbol) or "",
        })
    except Exception as e:
        print(f"EOD log failed for {symbol}: {e}", flush=True)


def _log_orb15_for_eod(st, hit, symbol, instrument_key):
    """Remember this ORB alert (stocks only) for the day-end summary + stats. Never raises."""
    try:
        if not instrument_key or not str(instrument_key).startswith("NSE_EQ"):
            return
        state.log_alert(st, {
            "symbol": symbol, "direction": hit["direction"], "time": str(hit["candle_time"]),
            "price": float(hit["close"]), "key": instrument_key, "score": "", "type": hit.get("log_type") or f"ORB{hit.get('window', 15)}",
            "fno": bool(hit.get("is_fno")), "day": None, "nifty": None,
            "vol": round(hit["vol_ratio"], 2) if hit.get("vol_ratio") else None,
            "sector": _sector_label(symbol) or "",
        })
    except Exception as e:
        print(f"ORB15 log failed for {symbol}: {e}", flush=True)


def _eod_one(entry):
    """Move of one alerted stock since the alert (uses today's 1-min candles)."""
    df = fetch_1min_candles(entry["key"])
    if df is None or df.empty:
        return None
    ts = df["timestamp"]
    try:
        ts = ts.dt.tz_convert(IST)
    except TypeError:
        ts = ts.dt.tz_localize(IST)
    ts = ts.dt.tz_localize(None)
    price = float(entry["price"])
    t0 = pd.Timestamp(str(entry["time"])[:19]) + pd.Timedelta(minutes=entry.get("t_offset_min", 15))   # 15-min candle has closed
    after = df[ts >= t0]
    last = float(df.iloc[-1]["close"])
    bull = entry["direction"] == "BULLISH"
    sign = 1.0 if bull else -1.0
    pnl = sign * (last - price) / price * 100.0
    if after.empty:
        best = worst = 0.0
    elif bull:
        best = (float(after["high"].max()) - price) / price * 100.0
        worst = (float(after["low"].min()) - price) / price * 100.0
    else:
        best = (price - float(after["low"].min())) / price * 100.0
        worst = (price - float(after["high"].max())) / price * 100.0
    # first-touch outcome: +TARGET% before -STOP% (both in the same minute counts as STOP)
    outcome = "NONE"
    tgt = float(getattr(config, "EOD_TARGET_PCT", 0.5)) / 100.0
    stp = float(getattr(config, "EOD_STOP_PCT", 0.5)) / 100.0
    for _, row in after.iterrows():
        hi, lo = float(row["high"]), float(row["low"])
        if bull:
            hit_t, hit_s = hi >= price * (1 + tgt), lo <= price * (1 - stp)
        else:
            hit_t, hit_s = lo <= price * (1 - tgt), hi >= price * (1 + stp)
        if hit_s:
            outcome = "STOP"
            break
        if hit_t:
            outcome = "TARGET"
            break
    out = dict(entry)
    out.update({"last": last, "pnl": pnl, "best": max(best, 0.0), "worst": min(worst, 0.0), "outcome": outcome})
    return out


def run_alert_stats(now_ist):
    """Win-rate tables from the stored day-end outcomes (see config.ALERT_STATS_*)."""
    st = state.load_state()
    log = st.get("_alerts_log") if isinstance(st.get("_alerts_log"), dict) else {}
    rows = []
    for day, entries in sorted(log.items()):
        for e in entries or []:
            if e.get("outcome") in ("TARGET", "STOP", "NONE") and e.get("pnl") is not None:
                rows.append(dict(e, _day=day))
    send_alert_stats_report(rows, now_ist)


def run_eod_alert_report(now_ist):
    """End-of-day summary of every stock alerted today (see config.EOD_REPORT_*)."""
    today = now_ist.date().isoformat()
    entries = state.get_alert_log(state.load_state(), today)
    results = []
    if entries:
        with ThreadPoolExecutor(max_workers=8) as pool:
            futs = {pool.submit(_eod_one, e): e for e in entries}
            for f in as_completed(futs):
                e = futs[f]
                try:
                    r = f.result()
                except Exception as ex:
                    print(f"EOD fetch failed for {e.get('symbol')}: {ex}", flush=True)
                    r = None
                if r:
                    results.append(r)
                else:
                    results.append(dict(e, last=None, pnl=None, best=None, worst=None))
    send_eod_alert_report(results, now_ist)
    try:
        _send_eod_chart_list(results, now_ist)
    except Exception as e:
        print(f"EOD chart list failed (non-blocking): {e}", flush=True)
    try:
        _st = state.load_state()
        if state.update_alert_outcomes(_st, today, results):
            state.save_state(_st)
    except Exception as e:
        print(f"Saving alert outcomes failed: {e}", flush=True)


def maybe_run_eod_report_once_a_day(now_ist):
    """First run of the day at/after config.EOD_REPORT_RUN_AFTER (weekdays) sends the summary."""
    if not getattr(config, "EOD_REPORT_ENABLED", False) or now_ist.weekday() >= 5:
        return
    hh, mm = (int(x) for x in config.EOD_REPORT_RUN_AFTER.split(":"))
    if now_ist.time() < dt.time(hh, mm):
        return
    today = now_ist.date().isoformat()
    st = state.load_state()
    if state.already_alerted(st, "EOD_REPORT", "DONE", today):
        return
    state.mark_alerted(st, "EOD_REPORT", "DONE", today)   # mark first: a crash must not retry every run
    state.save_state(st)
    run_eod_alert_report(now_ist)
    if getattr(config, "ALERT_STATS_ENABLED", False) and (getattr(config, "ALERT_STATS_WEEKDAY", 4) is None or now_ist.weekday() == getattr(config, "ALERT_STATS_WEEKDAY", 4)):
        try:
            run_alert_stats(now_ist)
        except Exception as e:
            print(f"Alert stats failed: {e}", flush=True)


# ---------- Daily RSI < 30 (oversold) alert + day-end performance (ADDED 2026-11-15) ----------
def _rsi_series(closes, period=14):
    s = pd.Series([float(x) for x in closes])
    delta = s.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    ag = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    al = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    return 100 - 100 / (1 + ag / al)


def _hourly_rsi(instrument_key, now_ist, today_df=None):
    """RSI(14) of the last CLOSED 1-hour bar (09:15-anchored bars), or None. Info only."""
    try:
        raw = fetch_historical_1min(instrument_key, config.OVERSOLD_HOURLY_HISTORY_DAYS) or []
        frames = []
        if raw:
            h = pd.DataFrame(raw).iloc[:, :6]
            h.columns = ["timestamp", "open", "high", "low", "close", "volume"]
            frames.append(h)
        if today_df is not None and not today_df.empty:
            frames.append(today_df[["timestamp", "open", "high", "low", "close", "volume"]])
        if not frames:
            return None
        df = pd.concat(frames, ignore_index=True)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(IST)
        df = df.drop_duplicates("timestamp").sort_values("timestamp")
        bars = resample_75min(df, "60min")
        if bars.empty:
            return None
        last_start = bars["timestamp"].iloc[-1]
        if last_start + pd.Timedelta(minutes=60) > pd.Timestamp(now_ist):   # still forming -> drop
            bars = bars.iloc[:-1]
        if len(bars) < 20:
            return None
        return round(float(_rsi_series(bars["close"].tolist()).iloc[-1]), 1)
    except Exception as e:
        print(f"1H RSI failed ({instrument_key}): {e}", flush=True)
        return None


def run_oversold_report(now_ist):
    """ONE list: every stock with Daily RSI(14) < OVERSOLD_DAILY_RSI_MAX on the last completed day,
    with 1H RSI + Monthly RSI alongside. Saved to state for the day-end performance report."""
    today_str = now_ist.date().isoformat()
    watchlist = {}
    fo = instruments.resolve_fo_stock_list(None)
    watchlist.update(fo)
    if config.OVERSOLD_UNIVERSE == "fo+n500":
        watchlist.update(instruments.resolve_nifty500_stocks())
    if not watchlist:
        print("Oversold scan: empty watchlist.", flush=True)
        return 0
    print(f"Oversold scan: {len(watchlist)} stocks...", flush=True)
    histories, failed = _fetch_daily_histories(watchlist, config.OVERSOLD_HISTORY_LOOKBACK_DAYS, include_today=False)
    histories = {s: [r for r in h if str(r["date"])[:10] < today_str] for s, h in histories.items()}
    histories = {s: h for s, h in histories.items() if len(h) >= 40}
    if not histories:
        print("Oversold scan: no daily history - skipping.", flush=True)
        return 0
    latest = max(str(h[-1]["date"])[:10] for h in histories.values())

    hits = []
    for sym, h in histories.items():
        try:
            if str(h[-1]["date"])[:10] != latest:
                continue                      # stale / suspended stock
            rs = _rsi_series([r["close"] for r in h])
            d = rs.iloc[-1]
            if pd.isna(d) or d >= config.OVERSOLD_DAILY_RSI_MAX:
                continue
            streak = 0
            for v in reversed(rs.tolist()):
                if pd.notna(v) and v < config.OVERSOLD_DAILY_RSI_MAX:
                    streak += 1
                else:
                    break
            hits.append({"symbol": sym, "key": watchlist[sym], "is_fno": sym in fo,
                         "d_rsi": round(float(d), 1), "streak": streak,
                         "prev_close": float(h[-1]["close"])})
        except Exception as e:
            print(f"Oversold calc failed {sym}: {e}", flush=True)
    hits.sort(key=lambda x: x["d_rsi"])
    hits = hits[:config.OVERSOLD_MAX_STOCKS]
    print(f"Oversold scan: {len(hits)} below RSI {config.OVERSOLD_DAILY_RSI_MAX}.", flush=True)

    def _enrich(hit):
        today_df = None
        try:
            today_df = fetch_1min_candles(hit["key"])
        except Exception as e:
            print(f"Oversold intraday failed {hit['symbol']}: {e}", flush=True)
        hit["has_today"] = False
        hit["price"] = hit["prev_close"]
        if today_df is not None and not today_df.empty:
            ts = pd.to_datetime(today_df["timestamp"])
            ts = ts.dt.tz_convert(IST) if ts.dt.tz is not None else ts.dt.tz_localize(IST)
            if str(ts.iloc[-1].date()) == today_str:
                hit["has_today"] = True
                hit["price"] = float(today_df.iloc[-1]["close"])
        hit["chg"] = (hit["price"] - hit["prev_close"]) / hit["prev_close"] * 100.0 if hit["prev_close"] else None
        hit["h_rsi"] = _hourly_rsi(hit["key"], now_ist, today_df if hit["has_today"] else None)
        hit["m_rsi"] = None
        try:
            mh = fetch_daily_history(hit["key"], days_back=config.MONTHLY_RSI_HISTORY_LOOKBACK_DAYS, include_today=True)
            info = get_monthly_rsi_info(mh) if mh else None
            hit["m_rsi"] = round(float(info["rsi"]), 1) if info else None
        except Exception as e:
            print(f"Oversold monthly RSI failed {hit['symbol']}: {e}", flush=True)
        hit["chart_link"] = build_chart_link(hit["symbol"])
        hit["screener_link"] = f"https://www.screener.in/company/{hit['symbol']}/consolidated/"
        return hit

    if hits:
        with ThreadPoolExecutor(max_workers=6) as pool:
            hits = list(pool.map(_enrich, hits))
        if not any(h["has_today"] for h in hits):
            print("Oversold scan: no live candles for today (holiday / feed not ready) - nothing sent.", flush=True)
            return 0

    send_oversold_report(hits, now_ist, latest)

    try:
        st = state.load_state()
        stamp = now_ist.strftime("%Y-%m-%d %H:%M:%S")
        for h in hits:
            state.log_alert(st, {"symbol": h["symbol"], "direction": "BULLISH", "time": stamp,
                                 "price": h["price"], "key": h["key"], "fno": h["is_fno"],
                                 "prev_close": h["prev_close"], "d_rsi": h["d_rsi"],
                                 "h_rsi": h["h_rsi"], "m_rsi": h["m_rsi"], "streak": h["streak"],
                                 "t_offset_min": 0}, log_key="_oversold_log")
        state.save_state(st)
    except Exception as e:
        print(f"Oversold log failed: {e}", flush=True)
    return len(hits)


def run_oversold_eod_report(now_ist):
    today = now_ist.date().isoformat()
    entries = state.get_alert_log(state.load_state(), today, log_key="_oversold_log")
    if not entries:
        print("Oversold EOD: nothing was alerted today - skipping.", flush=True)
        return
    results = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = {pool.submit(_eod_one, e): e for e in entries}
        for f in as_completed(futs):
            e = futs[f]
            try:
                r = f.result()
            except Exception as ex:
                print(f"Oversold EOD fetch failed {e.get('symbol')}: {ex}", flush=True)
                r = None
            if r:
                pc = r.get("prev_close")
                r["day_chg"] = (r["last"] - pc) / pc * 100.0 if pc else None
                results.append(r)
            else:
                results.append(dict(e, last=None, pnl=None, best=None, worst=None, day_chg=None))
    send_oversold_eod_report(results, now_ist)


def maybe_run_opening_1h_once_a_day(now_ist):
    """1st 1-hour (09:15-10:15) Open=Low/High + sector report. Runs from a normal run, once a day,
    first run at/after OPENING_1H_RUN_AFTER (weekdays, before 12:00)."""
    if not getattr(config, "OPENING_1H_REPORT_ENABLED", False) or now_ist.weekday() >= 5:
        return
    hh, mm = (int(x) for x in config.OPENING_1H_RUN_AFTER.split(":"))
    if not (dt.time(hh, mm) <= now_ist.time() <= dt.time(12, 0)):
        return
    today = now_ist.date().isoformat()
    st = state.load_state()
    if state.already_alerted(st, "OPEN1H_REPORT", "DONE", today):
        return
    state.mark_alerted(st, "OPEN1H_REPORT", "DONE", today)   # mark first: a crash must not retry every run
    state.save_state(st)
    run_opening_bias_report(now_ist, hour=True, sleep_first=False)


def maybe_run_opening_15m_once_a_day(now_ist):
    """1st 15-min (09:15-09:30) Open=Low/High + sector report, once a day from a normal run (ADDED 2026-11-21).
    Shares the OPEN15M_REPORT marker with SCAN_MODE=opening_bias_report so it never goes out twice."""
    if not getattr(config, "OPENING_15M_REPORT_ENABLED", False) or now_ist.weekday() >= 5:
        return
    hh, mm = (int(x) for x in config.OPENING_15M_RUN_AFTER.split(":"))
    if not (dt.time(hh, mm) <= now_ist.time() <= dt.time(12, 0)):
        return
    today = now_ist.date().isoformat()
    st = state.load_state()
    if state.already_alerted(st, "OPEN15M_REPORT", "DONE", today):
        return
    state.mark_alerted(st, "OPEN15M_REPORT", "DONE", today)
    state.save_state(st)
    run_opening_bias_report(now_ist, window_min=15, sleep_first=False)


def maybe_run_opening_30m_once_a_day(now_ist):
    """1st 30-min (09:15-09:45) Open=Low/High + sector report. Runs from a normal run, once a day,
    first run at/after OPENING_30M_RUN_AFTER (weekdays, before 12:00)."""
    if not getattr(config, "OPENING_30M_REPORT_ENABLED", False) or now_ist.weekday() >= 5:
        return
    hh, mm = (int(x) for x in config.OPENING_30M_RUN_AFTER.split(":"))
    if not (dt.time(hh, mm) <= now_ist.time() <= dt.time(12, 0)):
        return
    today = now_ist.date().isoformat()
    st = state.load_state()
    if state.already_alerted(st, "OPEN30M_REPORT", "DONE", today):
        return
    state.mark_alerted(st, "OPEN30M_REPORT", "DONE", today)   # mark first: a crash must not retry every run
    state.save_state(st)
    run_opening_bias_report(now_ist, window_min=30, sleep_first=False)


def maybe_run_oversold_once_a_day(now_ist):
    """Morning list (first run at/after OVERSOLD_RUN_AFTER, before the close) + day-end performance."""
    if now_ist.weekday() >= 5:
        return
    today = now_ist.date().isoformat()
    if getattr(config, "OVERSOLD_ALERT_ENABLED", False):
        hh, mm = (int(x) for x in config.OVERSOLD_RUN_AFTER.split(":"))
        if dt.time(hh, mm) <= now_ist.time() <= dt.time(13, 0):
            st = state.load_state()
            if not state.already_alerted(st, "OVERSOLD_SCAN", "DONE", today):
                state.mark_alerted(st, "OVERSOLD_SCAN", "DONE", today)   # mark first: a crash must not retry every run
                state.save_state(st)
                try:
                    run_oversold_report(now_ist)
                except Exception as e:
                    print(f"run_oversold_report failed: {e}", flush=True)
                    send_mode_failure_notice("oversold_report", e, now_ist)
    if getattr(config, "OVERSOLD_EOD_ENABLED", False):
        hh, mm = (int(x) for x in config.OVERSOLD_EOD_RUN_AFTER.split(":"))
        if now_ist.time() >= dt.time(hh, mm):
            st = state.load_state()
            if not state.already_alerted(st, "OVERSOLD_EOD", "DONE", today):
                state.mark_alerted(st, "OVERSOLD_EOD", "DONE", today)
                state.save_state(st)
                try:
                    run_oversold_eod_report(now_ist)
                except Exception as e:
                    print(f"run_oversold_eod_report failed: {e}", flush=True)
                    send_mode_failure_notice("oversold_eod", e, now_ist)


def maybe_run_daily_scan_once_a_day(now_ist):
    """Called from every normal run; does real work only on the first run of
    the day at/after config.DAILY_ALERT_RUN_AFTER IST (weekdays)."""
    if not config.DAILY_ALERT_ENABLED or now_ist.weekday() >= 5:
        return
    hh, mm = (int(x) for x in config.DAILY_ALERT_RUN_AFTER.split(":"))
    if now_ist.time() < dt.time(hh, mm):
        return
    today = now_ist.date().isoformat()
    st = state.load_state()
    if state.already_alerted(st, "DAILY_SCAN", "DONE", today):
        return
    state.mark_alerted(st, "DAILY_SCAN", "DONE", today)  # mark first: a crash must not retry every run
    state.save_state(st)
    run_daily_macd_rsi_scan(now_ist, notify_if_stale=True)


# ---------- Daily Bollinger band report (ADDED 2026-11-22) ----------
def run_daily_bb_report(now_ist):
    """Every morning: F&O stocks whose LAST COMPLETED daily candle closed ABOVE the daily upper Bollinger band
    (20, 2) with daily RSI > DAILY_BB_RSI_MIN (65). One summary with sector + Chart, strongest RSI first.
    (DAILY_BB_INCLUDE_LOWER = also the mirror list: close below the lower band with RSI < 100 - 65.) Never raises."""
    import html as _html
    from telegram_notifier import _post_html_chunks
    try:
        today_str = now_ist.date().isoformat()
        watchlist = dict(instruments.resolve_fo_stock_list(None))
        if getattr(config, "DAILY_BB_UNIVERSE", "fo") == "fo+n500":
            watchlist.update(instruments.resolve_nifty500_stocks())
        if not watchlist:
            print("Daily BB report: empty watchlist - skipping.", flush=True)
            return True
        use_today = now_ist.time() >= dt.time(15, 45)
        histories, failed = _fetch_daily_histories(watchlist, int(getattr(config, "DAILY_BB_HISTORY_DAYS", 180)),
                                                   include_today=use_today)
        if not use_today:   # never judge today's still-forming candle
            histories = {s: [r for r in h if str(r["date"])[:10] < today_str] for s, h in histories.items()}
            histories = {s: h for s, h in histories.items() if h}
        latest = max((str(h[-1]["date"])[:10] for h in histories.values()), default=None)
        if not latest:
            print("Daily BB report: no daily data - nothing sent.", flush=True)
            return False
        if use_today:
            n_today = sum(1 for h in histories.values() if str(h[-1]["date"])[:10] == today_str)
            if latest != today_str or n_today < 0.7 * len(histories):
                print(f"Daily BB report (evening): today's daily candle not ready yet ({n_today}/{len(histories)}) - will retry next run.", flush=True)
                return False
        period, k = int(getattr(config, "BB_PERIOD", 20)), float(getattr(config, "DAILY_BB_STD", 2.0))
        rsi_min = float(getattr(config, "DAILY_BB_RSI_MIN", 65))
        lower_on = getattr(config, "DAILY_BB_INCLUDE_LOWER", False)
        ups, lows = [], []
        for sym, h in histories.items():
            try:
                if str(h[-1]["date"])[:10] != latest or len(h) < period + 15:
                    continue
                cl = pd.Series([float(r["close"]) for r in h])
                ma, sd = cl.rolling(period).mean(), cl.rolling(period).std(ddof=0)
                up, lo = float((ma + k * sd).iloc[-1]), float((ma - k * sd).iloc[-1])
                rsi = float(_rsi_series(cl.tolist(), 14).iloc[-1])
                close, prev = float(cl.iloc[-1]), float(cl.iloc[-2])
                vols = [float(r.get("volume") or 0) for r in h]
                vavg = sum(vols[-21:-1]) / 20.0 if len(vols) >= 21 else 0
                row = {"symbol": sym, "close": close, "chg": (close - prev) / prev * 100.0 if prev else 0.0,
                       "rsi": rsi, "vol": (vols[-1] / vavg) if vavg > 0 else None}
                _o = [float(r.get("open") or r["close"]) for r in h[-2:]]
                _h2 = float(h[-1].get("high") or close)
                _l2 = float(h[-1].get("low") or close)
                _rng = _h2 - _l2
                _need = float(getattr(config, "BB_CANDLE_OUTSIDE_PCT", 80)) / 100.0
                _same = getattr(config, "BB_PREV_SAME_COLOR", True)
                _vol_ok = row["vol"] is None or row["vol"] >= float(getattr(config, "BB_MIN_VOL_RATIO", 1.5))
                _rsi_up = rsi > rsi_min
                _rsi_lo = rsi < (100 - rsi_min)
                if _rng > 0 and len(_o) == 2:
                    _out_up = (_h2 - max(_l2, up)) / _rng
                    _out_lo = (min(_h2, lo) - _l2) / _rng
                    if (close > up and close > _o[1] and _out_up + 1e-9 >= _need and _vol_ok and _rsi_up
                            and (not _same or float(h[-2]["close"]) > float(h[-2].get("open") or h[-2]["close"]))):
                        row.update(band=up, away=(close / up - 1) * 100.0, out_pct=_out_up * 100.0)
                        ups.append(row)
                    elif (lower_on and close < lo and close < _o[1] and _out_lo + 1e-9 >= _need and _vol_ok and _rsi_lo
                            and (not _same or float(h[-2]["close"]) < float(h[-2].get("open") or h[-2]["close"]))):
                        row.update(band=lo, away=(close / lo - 1) * 100.0, out_pct=_out_lo * 100.0)
                        lows.append(row)
            except Exception as e:
                print(f"Daily BB: {sym} failed: {e}", flush=True)
        day_txt = dt.date.fromisoformat(latest).strftime("%d %b")
        when = "AJKER (day-end)" if use_today else "AGER DIN"
        head = [f"\U0001F4CA <b>DAILY BOLLINGER \u2014 {when} \u2014 UPPER BAND CLOSE</b>  {now_ist.strftime('%d %b %H:%M')}",
                f"<i>{day_txt} er daily candle upper band (20, {k:g}) er 80%+ bahire, ager candle o green, volume bhalo \u00b7 RSI &gt; {rsi_min:g} \u00b7 {len(ups)} ta stock</i>"]
        body = []

        def _lines(rows, icon, word):
            for i, r in enumerate(sorted(rows, key=lambda r: -r["rsi"] if icon == "\U0001F7E2" else r["rsi"]), 1):
                sec = _sector_label(r["symbol"]) or "Other"
                body.append(f"{i}. <b>{_html.escape(r['symbol'])}</b> ({_html.escape(sec)}) "
                            f"<a href=\"{build_chart_link(r['symbol'])}\">Chart</a>\n"
                            f"    {r['close']:g} ({r['chg']:+.2f}%) \u00b7 {word} band {r['band']:.2f} ({r['away']:+.1f}%) "
                            f"\u00b7 RSI {r['rsi']:.1f}" + (f" \u00b7 Vol {r['vol']:.1f}x" if r.get("vol") else "")
                            + (f" \u00b7 candle {r['out_pct']:.0f}% bahire" if r.get("out_pct") is not None else ""))

        _lines(ups, "\U0001F7E2", "upper")
        if lows:
            body.append("")
            body.append(f"\U0001F534 <b>LOWER BAND CLOSE</b> ({len(lows)})")
            _lines(lows, "\U0001F534", "lower")
        if not ups and not lows:
            if not getattr(config, "DAILY_BB_NOTIFY_WHEN_EMPTY", True):
                return True
            body.append(f"{day_txt} e kono F&amp;O stock upper band er upore close kore ni (RSI &gt; {rsi_min:g}).")
        _post_html_chunks(head, body, "daily_bb")
        print(f"Daily BB report done: {len(ups)} upper, {len(lows)} lower, {len(failed)} fetch failures.", flush=True)
        return True
    except Exception as e:
        print(f"Daily BB report failed: {e}", flush=True)
        return True   # do not retry a crashing report every run


def maybe_run_daily_bb_once_a_day(now_ist):
    """Twice a day (weekdays): MORNING (first run at/after DAILY_BB_RUN_AFTER, before 12:00) = previous trading day's
    daily close; EVENING (first run at/after DAILY_BB_EVENING_RUN_AFTER, until DAILY_BB_EVENING_UNTIL) = today's close.
    The evening one is retried every run until today's daily candle is available."""
    if not getattr(config, "DAILY_BB_ENABLED", False) or now_ist.weekday() >= 5:
        return
    today = now_ist.date().isoformat()
    hh, mm = (int(x) for x in config.DAILY_BB_RUN_AFTER.split(":"))
    if dt.time(hh, mm) <= now_ist.time() <= dt.time(12, 0):
        st = state.load_state()
        if not state.already_alerted(st, "DAILY_BB", "DONE", today):
            state.mark_alerted(st, "DAILY_BB", "DONE", today)   # mark first: a crash must not retry every run
            state.save_state(st)
            run_daily_bb_report(now_ist)
    if getattr(config, "DAILY_BB_EVENING_ENABLED", True):
        eh, em = (int(x) for x in config.DAILY_BB_EVENING_RUN_AFTER.split(":"))
        uh, um = (int(x) for x in config.DAILY_BB_EVENING_UNTIL.split(":"))
        if dt.time(eh, em) <= now_ist.time() <= dt.time(uh, um):
            st = state.load_state()
            if not state.already_alerted(st, "DAILY_BB_EVE", "DONE", today):
                if run_daily_bb_report(now_ist):
                    st = state.load_state()
                    state.mark_alerted(st, "DAILY_BB_EVE", "DONE", today)
                    state.save_state(st)


index_symbols_orb = set(config.INDICES.keys())


def prev_day_info(df15, now_ist, price=None):
    """Previous trading day's O/H/L/C from the 15-min history (None if the history has no earlier day).
    where = where `price` sits vs that day's range: ABOVE_HIGH / BELOW_LOW / INSIDE. Never raises."""
    try:
        if df15 is None or df15.empty:
            return None
        ts = df15["timestamp"]
        prior = df15[ts.dt.date < now_ist.date()]
        if prior.empty:
            return None
        d = prior[prior["timestamp"].dt.date == prior["timestamp"].dt.date.iloc[-1]]
        o, h, l, c = float(d.iloc[0]["open"]), float(d["high"].max()), float(d["low"].min()), float(d.iloc[-1]["close"])
        where = None
        if price:
            where = "ABOVE_HIGH" if price > h else ("BELOW_LOW" if price < l else "INSIDE")
        return {"date": str(d.iloc[-1]["timestamp"].date()), "open": o, "high": h, "low": l, "close": c,
                "chg_pct": (c - o) / o * 100.0 if o else None, "where": where}
    except Exception:
        return None


def prev_day_text(pd_info):
    """One short HTML-safe line: 'Ager din: 🟢 +1.2% · H 150 · L 145 · C 148 · aj ager diner high er upore'."""
    if not pd_info:
        return ""
    ch = pd_info.get("chg_pct")
    icon = "\U0001F7E2" if (ch or 0) >= 0 else "\U0001F534"
    w = {"ABOVE_HIGH": " \u00b7 aj ager diner high er UPORE", "BELOW_LOW": " \u00b7 aj ager diner low er NICHE",
         "INSIDE": " \u00b7 aj ager diner range er bhitore"}.get(pd_info.get("where"), "")
    return (f"Ager din: {icon} {ch:+.2f}% \u00b7 H {pd_info['high']:g} \u00b7 L {pd_info['low']:g} \u00b7 C {pd_info['close']:g}{w}"
            if ch is not None else "")


def _orb_windows():
    """{window_minutes: settings} of the enabled opening-range-break alerts (config.ORB_WINDOWS)."""
    if not getattr(config, "ORB15_MOVE_ALERT_ENABLED", False):
        return {}
    return dict(getattr(config, "ORB_WINDOWS", {15: {"min_pct": 2.0, "min_vol_ratio": 2.0}}))


def check_orb_break(df5, df15, symbol, prev_close, now_ist, window_min):
    """Opening-range break of the first `window_min` minutes (15 / 30 / 60) from 09:15 on 15-MIN candles (ADDED 2026-11-20).
    BULLISH: the latest CLOSED 15-min candle AFTER the window closes above the window's high AND above the session
    VWAP AND the day change vs previous close >= min_pct AND (optional) the window's volume is >= min_vol_ratio x the
    same window's average volume of the previous days. BEARISH mirrors it (below the low and below VWAP).
    Evaluates the CURRENT state, so a missed run still catches it. Returns a dict or None. Never raises."""
    try:
        cfg = _orb_windows().get(window_min)
        if cfg is None or not prev_close:
            return None
        # ALL on 15-MIN candles (changed 2026-11-21): range = first window of 15-min candles, break = latest
        # CLOSED 15-min candle after the window, VWAP = session VWAP of today's 15-min candles up to it.
        if df15 is None or df15.empty:
            return None
        t15 = df15["timestamp"]
        today = df15[t15.dt.date == now_ist.date()]
        if today.empty:
            return None
        first_ts = today.iloc[0]["timestamp"]
        if (first_ts.hour, first_ts.minute) != (9, 15):
            return None
        win_end = first_ts + pd.Timedelta(minutes=window_min)
        win = today[today["timestamp"] < win_end]
        after = today[today["timestamp"] >= win_end]
        if after.empty or len(win) < window_min // 15:
            return None                                  # window not finished / no 15-min candle closed after it yet
        hi, lo = float(win["high"].max()), float(win["low"].min())
        last = today.iloc[-1]
        close = float(last["close"])
        vwap = compute_session_vwap(today)
        if getattr(config, "ORB_REQUIRE_VWAP", True) and not vwap:
            return None
        pct = (close - float(prev_close)) / float(prev_close) * 100.0
        thr = float(cfg.get("min_pct", 2.0))
        if close > hi and (thr <= 0 or pct >= thr) and (not getattr(config, "ORB_REQUIRE_VWAP", True) or close > vwap):
            direction = "BULLISH"
        elif (close < lo and (thr <= 0 or pct <= -thr) and getattr(config, "ORB15_BEARISH_ENABLED", True)
              and (not getattr(config, "ORB_REQUIRE_VWAP", True) or close < vwap)):
            direction = "BEARISH"
        else:
            return None
        # EMA50 (15-min closes, incl. prior days from the history cache) must be on the right side too
        ema50 = None
        if getattr(config, "ORB_REQUIRE_EMA", True):
            per = int(getattr(config, "ORB_EMA_PERIOD", 50))
            cl = df15["close"].astype(float)
            if len(cl) < per + 5:
                return None                               # not enough history to trust the EMA
            ema50 = float(cl.ewm(span=per, adjust=False).mean().iloc[-1])
            if (direction == "BULLISH" and close <= ema50) or (direction == "BEARISH" and close >= ema50):
                return None
        # "good volume": the window's volume vs the same window on the previous days (from the 15-min history)
        vol_ratio = None
        try:
            n_c = window_min // 15
            def _win_vol(day_df):
                if len(day_df) < n_c:
                    return None
                head = day_df.iloc[:n_c]
                if (head.iloc[0]["timestamp"].hour, head.iloc[0]["timestamp"].minute) != (9, 15):
                    return None
                return float(head["volume"].sum())
            t15 = df15["timestamp"]
            sums = []
            for _d, g in df15[t15.dt.date < now_ist.date()].groupby(t15.dt.date):
                v = _win_vol(g)
                if v is not None:
                    sums.append(v)
            sums = sums[-int(getattr(config, "ORB15_VOL_LOOKBACK_DAYS", 10)):]
            tv = _win_vol(df15[t15.dt.date == now_ist.date()])
            if tv is not None and len(sums) >= int(getattr(config, "ORB15_VOL_MIN_DAYS", 3)) and sum(sums) > 0:
                vol_ratio = tv / (sum(sums) / len(sums))
        except Exception:
            vol_ratio = None
        min_ratio = float(cfg.get("min_vol_ratio", 1.5)) or 1.5   # volume is MANDATORY (0 in config still means 1.5x)
        if min_ratio > 0:
            if vol_ratio is None:
                if not getattr(config, "ORB15_VOL_MISSING_PASSES", False):
                    return None                          # no history to prove "good volume"
            elif vol_ratio < min_ratio:
                return None
        return {"symbol": symbol, "direction": direction, "close": close, "pct": pct, "window": window_min,
                "range_high": hi, "range_low": lo, "vwap": float(vwap) if vwap else None, "ema50": ema50, "prev_day": prev_day_info(df15, now_ist, close),
                "candle_time": str(last["timestamp"]), "date": now_ist.date().isoformat(), "vol_ratio": vol_ratio}
    except Exception as e:
        print(f"ORB{window_min} check failed for {symbol}: {e}", flush=True)
        return None


def _orb_scan_symbol(symbol, df5, df15, prev_close, now_ist, saved_state, instrument_key, fno_underlyings, orb_hits):
    """Run every enabled ORB window for one stock; new hits go into orb_hits[window] (once per day per
    window per direction) and into the day-end log. Never raises."""
    try:
        if symbol in index_symbols_orb or symbol in config.COMMODITIES:
            return
        for w in _orb_windows():
            hit = check_orb_break(df5, df15, symbol, prev_close, now_ist, w)
            if hit is None:
                continue
            hit["is_fno"] = symbol.upper() in fno_underlyings
            if hit["is_fno"] is False and getattr(config, "CASH_STOCKS_BULLISH_ONLY", False) and hit["direction"] == "BEARISH":
                continue
            key = f"{symbol}::ORB{w}_MOVE"
            if state.already_alerted(saved_state, key, hit["direction"], hit["date"]):
                continue
            hit["chart_link"] = build_chart_link(symbol, "15-min")
            state.mark_alerted(saved_state, key, hit["direction"], hit["date"])
            orb_hits.setdefault(w, []).append(hit)
            _log_orb15_for_eod(saved_state, hit, symbol, instrument_key)
    except Exception as e:
        print(f"ORB scan failed for {symbol}: {e}", flush=True)


def send_orb_summary(hits, now_ist, window_min):
    """ONE Telegram message per window with every new opening-range break (price beyond the range AND on the
    right side of VWAP) of this run: sector, F&O/Cash, Chart link, % move, level, VWAP, volume ratio."""
    import html as _html
    from telegram_notifier import _sector_label
    if not hits:
        return
    name = {15: "1st 15-MIN", 30: "1st 30-MIN", 60: "1st 1-HOUR"}.get(window_min, f"1st {window_min}-MIN")
    short = {15: "15m", 30: "30m", 60: "1h"}.get(window_min, f"{window_min}m")
    cfg = _orb_windows().get(window_min, {})
    out = [f"\U0001F680 <b>{name} ORB BREAK + VWAP</b>  {now_ist.strftime('%H:%M')}",
           f"<i>{short} range todar + VWAP + EMA50 er sothik side"
           + (f" · {cfg.get('min_pct'):g}%+ move" if cfg.get("min_pct") else "")
           + (f" · volume {cfg.get('min_vol_ratio'):g}x+" if cfg.get("min_vol_ratio") else "") + "</i>"]
    for direction, icon, word, vw in (("BULLISH", "\U0001F7E2", f"above {short} high", "above"), ("BEARISH", "\U0001F534", f"below {short} low", "below")):
        grp = sorted((h for h in hits if h["direction"] == direction), key=lambda h: -abs(h["pct"]))
        if not grp:
            continue
        out.append(f"\n{icon} <b>{direction}</b> ({len(grp)})")
        for h in grp:
            lvl = h["range_high"] if direction == "BULLISH" else h["range_low"]
            sec = _sector_label(h["symbol"]) or "Other"
            kind = "F&O" if h.get("is_fno") else "Cash"
            out.append(f"{icon} <b>{_html.escape(h['symbol'])}</b> ({_html.escape(sec)}) [{kind}] "
                       f"<a href=\"{h.get('chart_link', '')}\">Chart</a>\n"
                       f"    {h['close']:g} ({h['pct']:+.2f}%) \u00b7 {word} {lvl:g}"
                       + (f" \u00b7 VWAP {h['vwap']:.2f} \u2705" if h.get("vwap") else "")
                       + (f" \u00b7 EMA50 {h['ema50']:.2f} \u2705" if h.get("ema50") else "")
                       + (f" \u00b7 Vol {h['vol_ratio']:.1f}x" if h.get("vol_ratio") else "")
)
    text = "\n".join(out)
    for i in range(0, len(text), 3900):
        try:
            requests.post(f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage",
                          data={"chat_id": config.TELEGRAM_CHAT_ID, "text": text[i:i + 3900], "parse_mode": "HTML",
                                "disable_web_page_preview": True}, timeout=20)
        except Exception as e:
            print(f"ORB{window_min} summary send failed: {e}", flush=True)


# ---------- Bollinger Band close + RSI 70/30 + Supertrend alert (ADDED 2026-11-21) ----------
def _supertrend(df, period=10, mult=3.0):
    """Standard Supertrend (Wilder ATR). Returns (line, direction) as lists; direction 1 = uptrend (line below
    price = support), -1 = downtrend (line above price = resistance)."""
    h, l, c = df["high"].astype(float).tolist(), df["low"].astype(float).tolist(), df["close"].astype(float).tolist()
    n = len(c)
    tr = [h[0] - l[0]] + [max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1])) for i in range(1, n)]
    atr = [None] * n
    if n >= period:
        atr[period - 1] = sum(tr[:period]) / period
        for i in range(period, n):
            atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period
    line, dirn = [None] * n, [0] * n
    fub = flb = None
    d = 1
    for i in range(n):
        if atr[i] is None:
            continue
        mid = (h[i] + l[i]) / 2.0
        ub, lb = mid + mult * atr[i], mid - mult * atr[i]
        if fub is None:
            fub, flb = ub, lb
            d = 1 if c[i] >= mid else -1
        else:
            fub = ub if (ub < fub or c[i - 1] > fub) else fub
            flb = lb if (lb > flb or c[i - 1] < flb) else flb
            if d == 1 and c[i] < flb:
                d = -1
            elif d == -1 and c[i] > fub:
                d = 1
        line[i], dirn[i] = (flb if d == 1 else fub), d
    return line, dirn


def _bb_eval(df15):
    """Indicator columns for the last candles: bb upper/lower, rsi, supertrend line + direction. None if too little data."""
    p = int(getattr(config, "BB_PERIOD", 20))
    if df15 is None or len(df15) < max(p, int(getattr(config, "BB_MIN_BARS", 30))):
        return None
    d = df15.reset_index(drop=True).copy()
    cl = d["close"].astype(float)
    ma = cl.rolling(p).mean()
    sd = cl.rolling(p).std(ddof=0)
    k = float(getattr(config, "BB_STD", 2.0))
    d["bb_up"], d["bb_lo"] = ma + k * sd, ma - k * sd
    d["rsi"] = _rsi_series(cl.tolist(), int(getattr(config, "BB_RSI_PERIOD", 14))).values
    ema = lambda s, n: s.ewm(span=n, adjust=False).mean()
    d["macd"] = ema(cl, 12) - ema(cl, 26)
    d["macd_sig"] = ema(d["macd"], 9)
    d["vol_avg"] = d["volume"].astype(float).shift(1).rolling(20, min_periods=10).mean()
    d["st_line"], d["st_dir"] = _supertrend(d, int(getattr(config, "ST_PERIOD", 10)), float(getattr(config, "ST_MULT", 3.0)))
    return d



def _macd15_allowed(symbol):
    """Which symbols may send the 15-min MACD/score alert (2026-11-24): stocks (MACD15_ALERT_ENABLED), commodities
    (MACD15_ALERT_COMMODITIES), indices only if MACD15_ALERT_INDICES (indices keep the standalone Bollinger alert)."""
    if symbol in config.INDICES:
        return bool(getattr(config, "MACD15_ALERT_INDICES", False))
    if symbol in config.COMMODITIES:
        return bool(getattr(config, "MACD15_ALERT_COMMODITIES", True) or getattr(config, "MACD15_ALERT_ENABLED", True))
    return bool(getattr(config, "MACD15_ALERT_ENABLED", True))


def _bb_candle_check(d, i, bull):
    """Bollinger condition on candle i of the _bb_eval frame d (2026-11-24):
      * >= BB_CANDLE_OUTSIDE_PCT % of the candle's range (low..high) lies outside the band AND it closes outside
        (bullish: above the upper band; bearish: below the lower band)
      * previous candle has the SAME colour (green->green / red->red)  [BB_PREV_SAME_COLOR]
      * good volume: candle volume >= BB_MIN_VOL_RATIO x average of the last 20 candles (skipped if no volume data)
    Returns a dict (RSI / R1-S1 are added by the caller as information only) or None."""
    try:
        if i < 1 or i >= len(d):
            return None
        r, p = d.iloc[i], d.iloc[i - 1]
        band = r["bb_up"] if bull else r["bb_lo"]
        if pd.isna(band):
            return None
        o, h, l, c = float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"])
        band = float(band)
        if bull and not (c > band and c > o):
            return None
        if (not bull) and not (c < band and c < o):
            return None
        rng = h - l
        if rng <= 0:
            return None
        out = (h - max(l, band)) / rng if bull else (min(h, band) - l) / rng
        need = float(getattr(config, "BB_CANDLE_OUTSIDE_PCT", 80)) / 100.0
        if out + 1e-9 < need:
            return None
        if getattr(config, "BB_PREV_SAME_COLOR", True):
            po, pc = float(p["open"]), float(p["close"])
            if (bull and not pc > po) or ((not bull) and not pc < po):
                return None
        va, vol = r.get("vol_avg"), float(r["volume"] or 0)
        vol_ratio = None
        if va is not None and not pd.isna(va) and float(va) > 0 and vol > 0:
            vol_ratio = vol / float(va)
            if vol_ratio < float(getattr(config, "BB_MIN_VOL_RATIO", 1.5)):
                return None
        return {"candle_time": str(r["timestamp"]), "outside_pct": out * 100.0, "band": band, "vol_ratio": vol_ratio,
                "rsi": None if pd.isna(r["rsi"]) else float(r["rsi"]), "close": c}
    except Exception:
        return None


def _macd_bb_blocks(signal, df15, now_ist, levels=None):
    """True (= block the MACD alert) when MACD_REQUIRE_BB is on and no Bollinger-qualified candle exists among the
    last MACD_BB_LOOKBACK closed candles up to the signal candle. On success signal['bb_info'] is filled for the
    message (RSI and R1/S1 are INFORMATION only). Missing data never crashes; too little data = blocked."""
    try:
        if not getattr(config, "MACD_REQUIRE_BB", True):
            return False
        d = _bb_eval(df15)
        if d is None:
            print(f"[bb-gate] {signal.get('symbol')} skipped (not enough candles for Bollinger)", flush=True)
            return True
        bull = signal["direction"] == "BULLISH"
        ct = str(signal["candle_time"])
        idx = d.index[d["timestamp"].astype(str) <= ct]
        if len(idx) == 0:
            return True
        last = int(idx[-1])
        look = max(1, int(getattr(config, "MACD_BB_LOOKBACK", 3)))
        hit = None
        for i in range(last, max(last - look, 0), -1):
            hit = _bb_candle_check(d, i, bull)
            if hit:
                break
        if not hit:
            print(f"[bb-gate] {signal.get('symbol')} {signal['direction']} {ct} skipped (no BB-qualified candle in last {look})", flush=True)
            return True
        r1 = s1 = None
        pdi = prev_day_info(df15, now_ist)
        _h = (pdi or {}).get("high") or (levels or {}).get("high")
        _l = (pdi or {}).get("low") or (levels or {}).get("low")
        _c = (pdi or {}).get("close") or (levels or {}).get("prev_close")
        if _h and _l and _c:
            _p = (float(_h) + float(_l) + float(_c)) / 3.0
            r1, s1 = 2 * _p - float(_l), 2 * _p - float(_h)
        px = hit["close"]
        hit["r1s1_ok"] = None if (r1 is None or s1 is None) else ((px > r1) if bull else (px < s1))
        hit["r1"], hit["s1"] = r1, s1
        hit["std"] = float(getattr(config, "BB_STD", 1.2))
        signal["bb_info"] = hit
        return False
    except Exception as e:
        print(f"[bb-gate] failed for {signal.get('symbol')}: {e}", flush=True)
        return True


def _bb_times(st, key, direction, day):
    return sorted(t for t in state._get_times(st, f"{key}:{direction}") if str(t).startswith(day))


def _bb_scan_symbol(symbol, df15, now_ist, saved_state, fno_underlyings, bb_hits, instrument_key=None, levels=None, df5=None):
    """BREAK: candle CLOSES above the upper Bollinger band with RSI >= 70 and Supertrend support (uptrend, close above
    the line), or below the lower band with RSI <= 30 and Supertrend resistance (BEARISH). RE-ENTRY: later, when the
    same stock's candle closes back INSIDE the band after a BREAK alert today. Last 2 closed candles are checked (catch-up).
    State is per (symbol, direction) lists of candle times, so merge_state.py unions them safely. Never raises."""
    try:
        if not getattr(config, "BB_ALERT_ENABLED", False):
            return
        is_index, is_comm = symbol in index_symbols_orb, symbol in config.COMMODITIES
        if (is_index and not getattr(config, "BB_INCLUDE_INDICES", True)) or (is_comm and not getattr(config, "BB_INCLUDE_COMMODITIES", True)):
            return
        if not (is_index or (is_comm and getattr(config, "BB_STANDALONE_COMMODITIES", False))
                or (not is_index and not is_comm and getattr(config, "BB_ALERT_STOCKS", False))):
            return   # 2026-11-24: stocks / commodities get Bollinger as a condition INSIDE the 15-min MACD alert instead
        d = _bb_eval(df15)
        if d is None:
            return
        day = now_ist.date().isoformat()
        today_idx = [i for i in range(len(d)) if str(d.iloc[i]["timestamp"]).startswith(day)]
        is_fno = symbol.upper() in fno_underlyings
        # classic floor pivots from the previous day's H/L/C (15-min history first, pivot cache as fallback)
        r1 = s1 = None
        pdi = prev_day_info(df15, now_ist)
        _h = (pdi or {}).get("high") or (levels or {}).get("high")
        _l = (pdi or {}).get("low") or (levels or {}).get("low")
        _c = (pdi or {}).get("close") or (levels or {}).get("prev_close")
        if _h and _l and _c:
            _p = (float(_h) + float(_l) + float(_c)) / 3.0
            r1, s1 = 2 * _p - float(_l), 2 * _p - float(_h)
        need_pivot = getattr(config, "BB_REQUIRE_R1S1", True)
        kind_tag = "Index" if is_index else ("Commodity" if is_comm else ("F&O" if is_fno else "Cash"))
        rsi_hi, rsi_lo = float(getattr(config, "BB_RSI_UPPER", 70)), float(getattr(config, "BB_RSI_LOWER", 30))
        need_st = getattr(config, "BB_REQUIRE_SUPERTREND", True)
        # units to evaluate: each of the last 2 closed 15-min candles, plus (BB_FIRST2_5MIN_ENABLED) the state of that
        # 15-min candle after its FIRST and SECOND 5-min candle (the 3rd one = the 15-min close itself, already covered).
        units = []
        for i in today_idx[-2:]:
            units.append((d.iloc[i], None))
            if getattr(config, "BB_FIRST2_5MIN_ENABLED", True) and df5 is not None and not df5.empty:
                try:
                    bar_start = pd.Timestamp(d.iloc[i]["timestamp"])
                    c5 = df5[(df5["timestamp"] >= bar_start) & (df5["timestamp"] < bar_start + pd.Timedelta(minutes=10))]
                    hist = df15[df15["timestamp"] < bar_start].tail(160)
                    for k in range(1, len(c5) + 1):
                        part = c5.iloc[:k]
                        syn = {"timestamp": bar_start, "open": float(part.iloc[0]["open"]), "high": float(part["high"].max()),
                               "low": float(part["low"].min()), "close": float(part.iloc[-1]["close"]),
                               "volume": float(part["volume"].sum())}
                        sd = _bb_eval(pd.concat([hist, pd.DataFrame([syn])], ignore_index=True))
                        if sd is not None:
                            units.append((sd.iloc[-1], str(part.iloc[-1]["timestamp"])))
                except Exception as e:
                    print(f"BB 5-min partial failed for {symbol}: {e}", flush=True)
        for r, five_min in units:
            if pd.isna(r["bb_up"]) or pd.isna(r["rsi"]):
                continue
            ct = str(r["timestamp"])
            close, rsi = float(r["close"]), float(r["rsi"])
            stl, std_ = r["st_line"], r["st_dir"]
            for direction in ("BULLISH", "BEARISH"):
                if direction == "BEARISH" and (not getattr(config, "BB_BEARISH_ENABLED", True)
                                               or (is_fno is False and not is_index and not is_comm
                                                   and getattr(config, "CASH_STOCKS_BULLISH_ONLY", False))):
                    continue
                bull = direction == "BULLISH"
                brk = _bb_times(saved_state, f"{symbol}::BB_BREAK", direction, day)
                rev = _bb_times(saved_state, f"{symbol}::BB_REENTRY", direction, day)
                active = bool(brk) and (not rev or max(brk) > max(rev))
                outside = close > float(r["bb_up"]) if bull else close < float(r["bb_lo"])
                qualifies = (five_min is None and _bb_candle_check(d, int(r.name), bull) is not None
                             and ((float(r["macd"]) > float(r["macd_sig"])) if bull else (float(r["macd"]) < float(r["macd_sig"]))))
                _info_only = (is_index and "index" in getattr(config, "BB_RSI_INFO_ONLY_KINDS", ())) or \
                             (is_comm and "commodity" in getattr(config, "BB_RSI_INFO_ONLY_KINDS", ()))
                rsi_ok = (rsi >= rsi_hi if bull else rsi <= rsi_lo) if (getattr(config, "BB_RSI_REQUIRED", False) and not _info_only) else True
                st_ok = True
                if need_st:
                    st_ok = (stl is not None and not pd.isna(stl) and ((std_ == 1 and close > float(stl)) if bull
                                                                       else (std_ == -1 and close < float(stl))))
                pv_ok = True
                if need_pivot:
                    pv_ok = (r1 is not None and close > r1) if bull else (s1 is not None and close < s1)
                # extra info (informational, never blocks): MACD side, candle volume vs recent average, last-1-month trend
                _va = r.get("vol_avg")
                vol_ratio = (float(r["volume"]) / float(_va)) if (_va is not None and not pd.isna(_va) and float(_va) > 0 and five_min is None) else None
                _ts = pd.Timestamp(r["timestamp"])
                _win = d[(d["timestamp"] >= _ts - pd.Timedelta(days=30)) & (d["timestamp"] <= _ts)]
                m_pct = m_days = m_above = None
                if len(_win) >= 20:
                    _b = float(_win.iloc[0]["close"])
                    if _b:
                        m_pct = (close - _b) / _b * 100.0
                        m_days = (_ts - pd.Timestamp(_win.iloc[0]["timestamp"])).days
                        m_above = close > float(_win["close"].mean())
                base = {"symbol": symbol, "direction": direction, "r1": r1, "s1": s1, "instrument_key": instrument_key,
                        "macd": float(r["macd"]), "macd_sig": float(r["macd_sig"]), "vol_ratio": vol_ratio,
                        "month_pct": m_pct, "month_days": m_days, "month_above_avg": m_above, "close": close,
                        "day_pct": ((close - float(_c)) / float(_c) * 100.0) if _c else None, "rsi": rsi, "is_fno": is_fno, "kind": kind_tag,
                        "bb_up": float(r["bb_up"]), "bb_lo": float(r["bb_lo"]),
                        "st_line": None if (stl is None or pd.isna(stl)) else float(stl), "candle_time": ct, "five_min": five_min}
                if outside and qualifies and rsi_ok and st_ok and pv_ok and not active:
                    if state.already_alerted(saved_state, f"{symbol}::BB_BREAK", direction, ct):
                        continue
                    state.mark_alerted(saved_state, f"{symbol}::BB_BREAK", direction, ct)
                    base["chart_link"] = build_chart_link(symbol, "15-min")
                    bb_hits["break"].append(base)
                    base["log_type"] = "BB"
                    _log_orb15_for_eod(saved_state, base, symbol, instrument_key)
                elif active and five_min is None and ct > max(brk) and not outside:
                    # candle closed back INSIDE the band: re-arm (a new alert needs a fresh qualifying break after this).
                    # The re-entry MESSAGE itself is only sent when BB_REENTRY_ALERT is on.
                    if state.already_alerted(saved_state, f"{symbol}::BB_REENTRY", direction, ct):
                        continue
                    state.mark_alerted(saved_state, f"{symbol}::BB_REENTRY", direction, ct)
                    if not getattr(config, "BB_REENTRY_ALERT", False):
                        continue
                    b = d[d["timestamp"].astype(str) == max(brk)]
                    base["break_time"] = max(brk)
                    base["break_price"] = float(b.iloc[0]["close"]) if not b.empty else None
                    base["chart_link"] = build_chart_link(symbol, "15-min")
                    bb_hits["reentry"].append(base)
    except Exception as e:
        print(f"BB scan failed for {symbol}: {e}", flush=True)


_ATM_OPT_CACHE = {}


def get_atm_option_text(symbol, instrument_key, today=None, short=False):
    """'🎯 Lot 550 · Exp 24 Dec · ATM 450: CE 12.5 (IV 28.4%) · PE 11.8 (IV 27.9%) · 1 lot ≈ ₹6,875 (CE)' for an F&O stock
    / index from Upstox's option-contract + option-chain endpoints (nearest expiry, strike closest to spot).
    Returns "" if anything is missing (never raises, never blocks the alert)."""
    try:
        if not getattr(config, "ATM_OPTION_INFO_ENABLED", True) or not instrument_key:
            return ""
        if symbol in _ATM_OPT_CACHE:
            return _ATM_OPT_CACHE[symbol][1 if short else 0]
        text = ""
        short_txt = ""
        try:
            today = today or _now_ist().date()
            headers = {"Authorization": f"Bearer {config.UPSTOX_ACCESS_TOKEN}"}
            r = _get_with_retry(UPSTOX_OPTION_CONTRACTS_URL, headers=headers, params={"instrument_key": instrument_key}, timeout=15)
            r.raise_for_status()
            contracts = r.json().get("data") or []
            exps = sorted({c["expiry"] for c in contracts if c.get("expiry") and dt.date.fromisoformat(str(c["expiry"])[:10]) >= today})
            if exps:
                expiry = str(exps[0])[:10]
                lot = next((int(c["lot_size"]) for c in contracts if str(c.get("expiry"))[:10] == expiry and c.get("lot_size")), None)
                if not lot:
                    lot = (instruments.get_fno_lot_sizes() or {}).get(symbol.upper())
                r2 = _get_with_retry(UPSTOX_OPTION_CHAIN_URL, headers=headers,
                                     params={"instrument_key": instrument_key, "expiry_date": expiry}, timeout=15)
                r2.raise_for_status()
                chain = [row for row in (r2.json().get("data") or []) if row.get("strike_price") is not None]
                if chain:
                    spot = next((float(row["underlying_spot_price"]) for row in chain if row.get("underlying_spot_price")), None)
                    if spot:
                        row = min(chain, key=lambda x: abs(float(x["strike_price"]) - spot))
                        def _side(o):
                            o = o or {}
                            ltp = (o.get("market_data") or {}).get("ltp")
                            iv = (o.get("option_greeks") or {}).get("iv")
                            return ltp, iv
                        (cl, civ), (pl, piv) = _side(row.get("call_options")), _side(row.get("put_options"))
                        def _fmt(tag, ltp, iv):
                            if not ltp:
                                return f"{tag} -"
                            return f"{tag} {float(ltp):g}" + (f" (IV {float(iv):.1f}%)" if iv else "")
                        strike = float(row["strike_price"])
                        parts = []
                        if lot:
                            parts.append(f"Lot {int(lot)}")
                        parts.append(f"Exp {dt.date.fromisoformat(expiry).strftime('%d %b')}")
                        parts.append(f"ATM {strike:g}: {_fmt('CE', cl, civ)} \u00b7 {_fmt('PE', pl, piv)}")
                        if lot and cl:
                            parts.append(f"1 lot CE \u2248 \u20b9{float(cl) * int(lot):,.0f}")
                        text = "\U0001F3AF " + " \u00b7 ".join(parts)
                        # Max Call OI (resistance) / Max Put OI (support) strikes of the same expiry, with price, OI and
                        # the change vs the previous day's closing OI
                        if getattr(config, "OPTION_MAX_OI_ENABLED", True):
                            def _oi_txt(v):
                                v = float(v or 0)
                                return f"{v / 1e5:.2f}L" if v >= 1e5 else (f"{v / 1e3:.1f}K" if v >= 1e3 else f"{v:g}")

                            def _best(side):
                                best = None
                                for rw in chain:
                                    md = (rw.get(side) or {}).get("market_data") or {}
                                    oi = float(md.get("oi") or 0)
                                    if oi > 0 and (best is None or oi > best[1]):
                                        best = (float(rw["strike_price"]), oi, md.get("ltp"), md.get("prev_oi"))
                                return best

                            def _fmt_oi(tag, b):
                                if not b:
                                    return None
                                s = f"Max {tag} OI {b[0]:g}: {_oi_txt(b[1])}"
                                if b[3]:
                                    s += f" ({(b[1] - float(b[3])) / float(b[3]) * 100:+.0f}% vs kal)"
                                if b[2]:
                                    s += f" \u00b7 {'CE' if tag == 'Call' else 'PE'} price {float(b[2]):g}"
                                return s
                            tc = sum(float((rw.get("call_options") or {}).get("market_data", {}).get("oi") or 0) for rw in chain)
                            tp = sum(float((rw.get("put_options") or {}).get("market_data", {}).get("oi") or 0) for rw in chain)
                            _bc, _bp = _best("call_options"), _best("put_options")
                            short_txt = (f"Lot {int(lot)} \u00b7 " if lot else "") + f"ATM {strike:g} CE {float(cl or 0):g} PE {float(pl or 0):g}"
                            if civ or piv:
                                short_txt += f" (IV {float(civ or piv):.0f}%)"
                            if _bc:
                                short_txt += f" \u00b7 Call OI {_bc[0]:g}"
                            if _bp:
                                short_txt += f" \u00b7 Put OI {_bp[0]:g}"
                            bits = [x for x in (_fmt_oi("Call", _bc), _fmt_oi("Put", _bp)) if x]
                            if bits:
                                if tc > 0:
                                    bits.append(f"PCR {tp / tc:.2f}")
                                text += "\n    \U0001F4CD " + " \u00b7 ".join(bits)
        except Exception as e:
            print(f"[atm-option] {symbol}: {type(e).__name__}: {e}", flush=True)
            text = ""
        _ATM_OPT_CACHE[symbol] = (text, short_txt or text)
        return short_txt or text if short else text
    except Exception:
        return ""


_CORP_EVENT_CACHE = {}


def get_corp_event_text(symbol, today=None):
    """'🗓 Result ~28 Nov (6d) · Dividend ex-date 25 Nov' from Yahoo Finance (yfinance, "SYMBOL.NS"), or "" when
    nothing upcoming / not available. Best effort: Yahoo's Indian dates are sometimes estimates or missing, so a
    missing line does NOT mean 'no result/dividend'. Cached per run; never raises; hard 8-sec per-symbol cap."""
    try:
        if not getattr(config, "CORP_EVENTS_ENABLED", True):
            return ""
        if symbol in _CORP_EVENT_CACHE:
            return _CORP_EVENT_CACHE[symbol]
        text = ""
        try:
            import yfinance as yf
            import concurrent.futures as _cf
            today = today or _now_ist().date()

            def _fetch():
                return yf.Ticker(f"{symbol}.NS").calendar

            with _cf.ThreadPoolExecutor(max_workers=1) as ex:
                cal = ex.submit(_fetch).result(timeout=8)

            def _as_dates(v):
                out = []
                for x in (v if isinstance(v, (list, tuple)) else [v]):
                    try:
                        if x is None or (isinstance(x, float) and x != x):
                            continue
                        out.append(pd.Timestamp(x).date())
                    except Exception:
                        continue
                return out

            if cal is not None and hasattr(cal, "to_dict") and not isinstance(cal, dict):
                cal = {k: list(v.values()) if isinstance(v, dict) else v for k, v in cal.to_dict().items()}
            cal = cal or {}
            parts = []
            win_r = int(getattr(config, "CORP_EVENTS_RESULT_DAYS", 45))
            win_d = int(getattr(config, "CORP_EVENTS_DIVIDEND_DAYS", 30))
            er = sorted(d for d in _as_dates(cal.get("Earnings Date")) if 0 <= (d - today).days <= win_r)
            if er:
                parts.append(f"Result ~{er[0].strftime('%d %b')} ({(er[0] - today).days}d)")
            xd = sorted(d for d in _as_dates(cal.get("Ex-Dividend Date")) if -3 <= (d - today).days <= win_d)
            if xd:
                lab = "today" if xd[0] == today else (f"{(xd[0] - today).days}d" if xd[0] > today else "just passed")
                parts.append(f"Dividend ex-date {xd[0].strftime('%d %b')} ({lab})")
            if parts:
                text = "\U0001F5D3 " + " \u00b7 ".join(parts)
        except Exception as e:
            print(f"[corp-events] {symbol}: {type(e).__name__}", flush=True)
            text = ""
        _CORP_EVENT_CACHE[symbol] = text
        return text
    except Exception:
        return ""


def send_bb_summary(hits, now_ist, kind):
    """'break' = 15-min candle outside the Bollinger band (same look as the MACD alert: one short line per stock,
    tap to open the details; F&O stocks show Lot, ATM CE/PE + IV, max Call/Put OI strikes). 'reentry' = back inside."""
    import html as _html
    from telegram_notifier import _sector_label
    if not hits:
        return
    mk = lambda ok: "\u2705" if ok else "\u274C"
    if kind != "break":
        out = [f"\U0001F501 <b>BOLLINGER BAND RE-ENTRY</b>  {now_ist.strftime('%H:%M')}",
               "<i>ager alert howa stock ebar band er bhitore fire close korlo</i>"]
        for h in sorted(hits, key=lambda h: h["symbol"]):
            bull = h["direction"] == "BULLISH"
            band = h["bb_up"] if bull else h["bb_lo"]
            bp = h.get("break_price")
            mv = ""
            if bp:
                pnl = (h["close"] - bp) / bp * 100.0 * (1 if bull else -1)
                mv = f" \u00b7 alert {str(h.get('break_time'))[11:16]} @ {bp:g} \u2192 {pnl:+.2f}%"
            out.append(f"{'🟢' if bull else '🔴'} <b>{_html.escape(h['symbol'])}</b> {h['close']:g} \u00b7 {'upper' if bull else 'lower'} band {band:.2f} er bhitore \u00b7 RSI {h['rsi']:.1f}{mv}")
        text = "\n".join(out)
        try:
            requests.post(f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage",
                          data={"chat_id": config.TELEGRAM_CHAT_ID, "text": text[:3900], "parse_mode": "HTML",
                                "disable_web_page_preview": True}, timeout=20)
        except Exception as e:
            print(f"BB summary send failed: {e}", flush=True)
        return
    nb = sum(1 for h in hits if h["direction"] == "BULLISH")
    ns = len(hits) - nb
    ct0 = str(hits[0]["candle_time"])[11:16]
    cnt = " \u00b7 ".join(x for x in ((f"\U0001F7E2 {nb} bullish" if nb else ""), (f"\U0001F534 {ns} bearish" if ns else "")) if x)
    header = f"\U0001F4C8 <b>15-MIN BB ALERTS</b> ({ct0})\n{cnt}   <i>{_html.escape(BUILD_TAG_SHORT())}</i>"
    items = []
    for h in sorted(hits, key=lambda h: (h["direction"] != "BULLISH", h["symbol"])):
        try:
            bull = h["direction"] == "BULLISH"
            icon = "\U0001F7E2" if bull else "\U0001F534"
            kindtxt = h.get("kind") or ("F&O" if h.get("is_fno") else "Cash")
            band = h["bb_up"] if bull else h["bb_lo"]
            sec = "" if kindtxt in ("Index", "Commodity") else f" <i>({_html.escape(_sector_label(h['symbol']) or 'Other')})</i>"
            links = [f"\U0001F4C8 <a href=\"{h.get('chart_link', '')}\">Chart</a>"]
            if kindtxt in ("F&O", "Cash"):
                links.append(f"\U0001F4CA <a href=\"https://www.screener.in/company/{urllib.parse.quote(h['symbol'])}/consolidated/\">Screener</a>")
            mv = f" ({h['day_pct']:+.2f}%)" if h.get("day_pct") is not None else ""
            top = (f"{icon} <b>{_html.escape(h['symbol'])}</b>{sec}  " + "  \u00b7  ".join(links)
                   + f"\nRSI {h['rsi']:.1f} \u00b7 {h['close']:g}{mv} \u00b7 \U0001F4CABB\u2705")
            body = [f"{h['candle_time'][:10]} {str(h['candle_time'])[11:16]}  |  {kindtxt}" + ("  \u2705" if kindtxt == "F&O" else "")]
            if h.get("outside_pct") is not None:
                body.append(f"\u2705 BB({getattr(config, 'BB_STD', 1.2):g}): candle {h['outside_pct']:.0f}% {'upper' if bull else 'lower'} band {band:.2f} er bahire \u00b7 ager candle {'green' if bull else 'red'}")
            else:
                body.append(f"\u2705 BB close {'upper' if bull else 'lower'} band {band:.2f} er bahire")
            if h.get("vol_ratio") is not None:
                body.append(f"\u2705 Volume {h['vol_ratio']:.1f}x (20-candle avg er)")
            rsi_must = kindtxt in ("F&O", "Cash") and getattr(config, "BB_RSI_REQUIRED", False)
            _ri = mk(True) if rsi_must else "\u2139\ufe0f"
            body.append(f"{_ri} RSI {h['rsi']:.1f}" + (f" (must {getattr(config, 'BB_RSI_UPPER', 70):g}/{getattr(config, 'BB_RSI_LOWER', 30):g})" if rsi_must else " (info)"))
            body.append(f"\u2705 MACD {h['macd']:.2f} / {h['macd_sig']:.2f}")
            if bull and h.get("r1"):
                body.append(f"{mk(h['close'] > h['r1'])} R1 {h['r1']:.2f} (info)")
            elif (not bull) and h.get("s1"):
                body.append(f"{mk(h['close'] < h['s1'])} S1 {h['s1']:.2f} (info)")
            if h.get("st_line"):
                body.append(f"{mk((h['close'] > h['st_line']) == bull)} Supertrend {h['st_line']:.2f} (info)")
            if h.get("month_pct") is not None:
                body.append(f"\U0001F4C5 1 mash: {h['month_pct']:+.1f}% ({h.get('month_days')} din)")
            if h.get("is_fno") and kindtxt == "F&O" and getattr(config, "BB_MSG_OPTIONS", "full") != "off":
                ao = get_atm_option_text(h["symbol"], h.get("instrument_key"), now_ist.date(), short=False)
                if ao:
                    body.append(ao)
            if kindtxt in ("F&O", "Cash") and getattr(config, "BB_MSG_EVENTS", True):
                ev = get_corp_event_text(h["symbol"], now_ist.date())
                if ev:
                    body.append(ev)
            items.append(f"{top}\n<blockquote expandable>" + "\n".join(body) + "</blockquote>")
        except Exception as e:
            print(f"BB message build failed for {h.get('symbol')}: {e}", flush=True)
    groups, cur, size = [], [], 0
    for it in items:
        if cur and size + len(it) + 400 > 3800:
            groups.append(cur); cur, size = [], 0
        cur.append(it); size += len(it) + 2
    if cur:
        groups.append(cur)
    for g in groups:
        try:
            requests.post(f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage",
                          data={"chat_id": config.TELEGRAM_CHAT_ID, "text": header + "\n" + "\n".join(g), "parse_mode": "HTML",
                                "disable_web_page_preview": True}, timeout=20)
        except Exception as e:
            print(f"BB summary send failed: {e}", flush=True)


def BUILD_TAG_SHORT():
    try:
        from telegram_notifier import BUILD_TAG
        return BUILD_TAG.split(" ")[0]
    except Exception:
        return ""


# ---------- 1st 15-min candle 2% move alert (ADDED 2026-11-23) ----------
def _move15_scan_symbol(symbol, df15, prev_close, now_ist, saved_state, instrument_key, fno_underlyings, hits):
    """Stocks only: today's FIRST 15-min candle (09:15-09:30) closed >= MOVE15_MIN_PCT % above / below the previous
    day's close. Once per stock per day per direction; picked up by any run until MOVE15_UNTIL. Never raises."""
    try:
        if not getattr(config, "MOVE15_ALERT_ENABLED", False) or not prev_close:
            return
        if symbol in index_symbols_orb or symbol in config.COMMODITIES:
            return
        hh, mm = (int(x) for x in str(getattr(config, "MOVE15_UNTIL", "10:30")).split(":"))
        if now_ist.time() > dt.time(hh, mm):
            return
        if df15 is None or df15.empty:
            return
        ts = df15["timestamp"]
        today = df15[ts.dt.date == now_ist.date()]
        if today.empty:
            return
        first = today.iloc[0]
        t0 = pd.Timestamp(first["timestamp"])
        if (t0.hour, t0.minute) != (9, 15):
            return
        close = float(first["close"])
        pct = (close - float(prev_close)) / float(prev_close) * 100.0
        thr = float(getattr(config, "MOVE15_MIN_PCT", 2.0))
        if pct >= thr:
            direction = "BULLISH"
        elif pct <= -thr:
            direction = "BEARISH"
        else:
            return
        is_fno = symbol.upper() in fno_underlyings
        if direction == "BEARISH" and (not getattr(config, "MOVE15_BEARISH_ENABLED", True)
                                       or (not is_fno and getattr(config, "CASH_STOCKS_BULLISH_ONLY", False))):
            return
        day = now_ist.date().isoformat()
        if state.already_alerted(saved_state, f"{symbol}::MOVE15", direction, day):
            return
        state.mark_alerted(saved_state, f"{symbol}::MOVE15", direction, day)
        hit = {"symbol": symbol, "direction": direction, "close": close, "pct": pct, "is_fno": is_fno,
               "open": float(first["open"]), "candle_time": str(t0) , "chart_link": build_chart_link(symbol, "15-min"),
               "log_type": "MOVE15"}
        hits.append(hit)
        logh = dict(hit, candle_time=str(t0 + pd.Timedelta(seconds=1)))   # distinct log key from a BB alert on the same candle
        _log_orb15_for_eod(saved_state, logh, symbol, instrument_key)
    except Exception as e:
        print(f"MOVE15 scan failed for {symbol}: {e}", flush=True)


def send_move15_summary(hits, now_ist):
    """ONE message: stocks whose first 15-min candle moved >= MOVE15_MIN_PCT % (vs previous close)."""
    import html as _html
    from telegram_notifier import _sector_label
    if not hits:
        return
    thr = float(getattr(config, "MOVE15_MIN_PCT", 2.0))
    out = [f"\U0001F680 <b>1st 15-MIN {thr:g}%+ MOVE</b>  {now_ist.strftime('%H:%M')}"]
    for direction, icon in (("BULLISH", "\U0001F7E2"), ("BEARISH", "\U0001F534")):
        grp = sorted((h for h in hits if h["direction"] == direction), key=lambda h: -abs(h["pct"]))
        if not grp:
            continue
        out.append(f"\n{icon} <b>{direction}</b> ({len(grp)})")
        for h in grp:
            sec = _sector_label(h["symbol"]) or "Other"
            out.append(f"{icon} <b>{_html.escape(h['symbol'])}</b> ({_html.escape(sec)}) [{'F&amp;O' if h['is_fno'] else 'Cash'}] "
                       f"<a href=\"{h['chart_link']}\">Chart</a>\n    {h['close']:g} ({h['pct']:+.2f}%)")
    text = "\n".join(out)
    for i in range(0, len(text), 3900):
        try:
            requests.post(f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage",
                          data={"chat_id": config.TELEGRAM_CHAT_ID, "text": text[i:i + 3900], "parse_mode": "HTML",
                                "disable_web_page_preview": True}, timeout=20)
        except Exception as e:
            print(f"MOVE15 summary send failed: {e}", flush=True)


def _sector_chart_label(sym):
    from telegram_notifier import _sector_label
    return (_sector_label(sym) or "Other").strip() or "Other"


def run_day_movers_report(now_ist):
    """TOP N GAINERS / LOSERS of the day (F&O stocks): latest price vs previous close, with sector + Chart.
    Works any time (also after the close). Triggered by maybe_run_day_movers_once_a_day or SCAN_MODE=day_movers."""
    import html as _html
    n = int(getattr(config, "DAY_MOVERS_COUNT", 10))
    fo_watch = None if config.USE_FULL_FO_LIST else config.FO_STOCK_WATCHLIST
    watchlist = instruments.resolve_fo_stock_list(fo_watch)
    pivots = build_pivot_levels(watchlist)
    dfs, _failed = fetch_all(watchlist, now_ist, config.FETCH_WORKERS)
    movers = []
    for sym, tup in dfs.items():
        try:
            df15 = tup[2]
            prev = (pivots.get(sym) or {}).get("prev_close")
            if df15 is None or df15.empty or not prev:
                continue
            today = df15[df15["timestamp"].dt.date == now_ist.date()]
            if today.empty:
                continue
            px = float(today.iloc[-1]["close"])
            movers.append({"symbol": sym, "close": px, "pct": (px - float(prev)) / float(prev) * 100.0})
        except Exception:
            continue
    if not movers:
        print("[day_movers] no data - nothing sent", flush=True)
        return
    gainers = sorted(movers, key=lambda x: -x["pct"])[:n]
    losers = sorted(movers, key=lambda x: x["pct"])[:n]
    out = [f"\U0001F4CA <b>TOP {n} GAINERS / LOSERS</b>  {now_ist.strftime('%d %b %H:%M')}  (F&amp;O, vs prev close)"]
    for title, icon, rows in (("GAINERS", "\U0001F7E2", gainers), ("LOSERS", "\U0001F534", losers)):
        out.append(f"\n{icon} <b>{title}</b>")
        for i, r in enumerate(rows, 1):
            out.append(f"{i}. <b>{_html.escape(r['symbol'])}</b> ({_html.escape(_sector_chart_label(r['symbol']))}) "
                       f"<b>{r['pct']:+.2f}%</b> \u00b7 {r['close']:g} "
                       f"<a href=\"{build_chart_link(r['symbol'])}\">Chart</a>")
    try:
        requests.post(f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage",
                      data={"chat_id": config.TELEGRAM_CHAT_ID, "text": "\n".join(out)[:4000], "parse_mode": "HTML",
                            "disable_web_page_preview": True}, timeout=20)
    except Exception as e:
        print(f"[day_movers] send failed: {e}", flush=True)


def maybe_run_day_movers_once_a_day(now_ist):
    """Auto top-N gainers/losers at config.DAY_MOVERS_AUTO_TIMES (first run at/after each time, weekdays)."""
    if not getattr(config, "DAY_MOVERS_ENABLED", False) or now_ist.weekday() >= 5:
        return
    today = now_ist.date().isoformat()
    for hhmm in getattr(config, "DAY_MOVERS_AUTO_TIMES", ()):
        hh, mm = (int(x) for x in hhmm.split(":"))
        start = now_ist.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if not (start <= now_ist <= start + dt.timedelta(minutes=int(getattr(config, "DAY_MOVERS_WINDOW_MIN", 90)))):
            continue
        st = state.load_state()
        if state.already_alerted(st, f"DAYMOVERS_{hhmm}", "DONE", today):
            continue
        state.mark_alerted(st, f"DAYMOVERS_{hhmm}", "DONE", today)
        state.save_state(st)
        run_day_movers_report(now_ist)


def run_top5_report(now_ist, window):
    """TOP 5 GAINERS / LOSERS (F&O stocks) for window 'day' (previous trading day: its close vs the day before),
    '15' / '30' / '60' (price at the close of the first 15 / 30 / 60 minutes of today vs previous close). ADDED 2026-11-24."""
    import html as _html
    n = int(getattr(config, "TOP5_COUNT", 5))
    fo_watch = None if config.USE_FULL_FO_LIST else config.FO_STOCK_WATCHLIST
    watchlist = instruments.resolve_fo_stock_list(fo_watch)
    pivots = build_pivot_levels(watchlist)
    dfs, _failed = fetch_all(watchlist, now_ist, config.FETCH_WORKERS)
    rows = []
    for sym, tup in dfs.items():
        try:
            df15 = tup[2]
            if df15 is None or df15.empty:
                continue
            dts = df15["timestamp"].dt.date
            if window == "day":
                days = sorted(d for d in set(dts) if d < now_ist.date())
                if len(days) < 2:
                    continue
                c1 = float(df15[dts == days[-1]].iloc[-1]["close"])
                c0 = float(df15[dts == days[-2]].iloc[-1]["close"])
                px, base = c1, c0
            else:
                mins = int(window)
                t0 = pd.Timestamp(now_ist.date().isoformat() + " 09:15:00")
                if df15["timestamp"].dt.tz is not None:
                    t0 = t0.tz_localize(df15["timestamp"].dt.tz)
                win = df15[(df15["timestamp"] >= t0) & (df15["timestamp"] < t0 + pd.Timedelta(minutes=mins))]
                if len(win) < mins // 15:
                    continue
                base = (pivots.get(sym) or {}).get("prev_close")
                if not base:
                    continue
                px = float(win.iloc[-1]["close"])
            rows.append({"symbol": sym, "close": px, "pct": (px - float(base)) / float(base) * 100.0})
        except Exception:
            continue
    if not rows:
        print(f"[top5:{window}] no data - nothing sent", flush=True)
        return
    label = {"day": "LAST DAY", "15": "1st 15-MIN", "30": "1st 30-MIN", "60": "1st 1-HOUR"}[window]
    sub = "prev day close vs day before" if window == "day" else "vs prev close"
    out = [f"\U0001F3C6 <b>TOP {n} GAINERS / LOSERS \u2014 {label}</b>  {now_ist.strftime('%d %b %H:%M')}  (F&amp;O, {sub})"]
    for title, icon, sel in (("GAINERS", "\U0001F7E2", sorted(rows, key=lambda x: -x["pct"])[:n]),
                             ("LOSERS", "\U0001F534", sorted(rows, key=lambda x: x["pct"])[:n])):
        out.append(f"\n{icon} <b>{title}</b>")
        for i, r in enumerate(sel, 1):
            out.append(f"{i}. <b>{_html.escape(r['symbol'])}</b> ({_html.escape(_sector_chart_label(r['symbol']))}) "
                       f"<b>{r['pct']:+.2f}%</b> \u00b7 {r['close']:g} "
                       f"<a href=\"{build_chart_link(r['symbol'])}\">Chart</a>")
    try:
        requests.post(f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage",
                      data={"chat_id": config.TELEGRAM_CHAT_ID, "text": "\n".join(out)[:4000], "parse_mode": "HTML",
                            "disable_web_page_preview": True}, timeout=20)
    except Exception as e:
        print(f"[top5:{window}] send failed: {e}", flush=True)


def maybe_run_top5_once_a_day(now_ist):
    """Top-5 gainers/losers for last day / 15 / 30 / 60 min, each once a day at the first run at/after its time (until 12:00)."""
    if not getattr(config, "TOP5_ENABLED", False) or now_ist.weekday() >= 5:
        return
    today = now_ist.date().isoformat()
    for window, hhmm in getattr(config, "TOP5_SCHEDULE", {}).items():
        hh, mm = (int(x) for x in hhmm.split(":"))
        if not (dt.time(hh, mm) <= now_ist.time() <= dt.time(12, 0)):
            continue
        st = state.load_state()
        if state.already_alerted(st, f"TOP5_{window}", "DONE", today):
            continue
        state.mark_alerted(st, f"TOP5_{window}", "DONE", today)   # mark first: a crash must not retry every run
        state.save_state(st)
        try:
            run_top5_report(now_ist, window)
        except Exception as e:
            print(f"Top5 {window} failed: {e}", flush=True)


def _send_eod_chart_list(results, now_ist):
    """2nd day-end message: every alerted stock with Chart link + result (ADDED 2026-11-19)."""
    import html as _html
    if not getattr(config, "EOD_CHART_LIST_ENABLED", False) or not results:
        return
    out = [f"\U0001F4C8 <b>DAY-END CHARTS</b>  {now_ist.strftime('%d %b %Y')}"]
    for direction, icon in (("BULLISH", "\U0001F7E2"), ("BEARISH", "\U0001F534")):
        rows = sorted((r for r in results if r.get("direction") == direction),
                      key=lambda r: (r.get("pnl") is None, -(r.get("pnl") or 0)))
        if not rows:
            continue
        out.append(f"\n{icon} <b>{direction}</b> ({len(rows)})")
        for r in rows:
            pnl = r.get("pnl")
            res = "\u26AA" if pnl is None else ("\u2705" if pnl > 0 else "\u274C")
            pct = "" if pnl is None else f" <b>{pnl:+.2f}%</b>"
            out.append(f"{res} <b>{_html.escape(r['symbol'])}</b> ({_html.escape(_sector_chart_label(r['symbol']))}){pct} "
                       f"<a href=\"{build_chart_link(r['symbol'])}\">Chart</a>")
    text = "\n".join(out)
    for i in range(0, len(text), 3800):
        try:
            requests.post(f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage",
                          data={"chat_id": config.TELEGRAM_CHAT_ID, "text": text[i:i + 3800], "parse_mode": "HTML",
                                "disable_web_page_preview": True}, timeout=20)
        except Exception as e:
            print(f"EOD chart list send failed: {e}", flush=True)


def run_stock_list_report(now_ist):
    """SCAN_MODE=stock_list — ALL stocks, SECTOR WISE, to Telegram (ADDED 2026-11-17).
    Every F&O stock (+ the rest of Nifty 500 as CASH when config.STOCK_LIST_INCLUDE_NIFTY500).
    Each line: symbol [FNO · lot qty] or [CASH], Chart link (Groww) and Screener.in link.
    Works any time of day (no market-session check). Sent in several messages (Telegram's 4096 limit)."""
    import html as _html
    from telegram_notifier import _sector_label
    fo = instruments.resolve_fo_stock_list(None if config.USE_FULL_FO_LIST else config.FO_STOCK_WATCHLIST)
    universe = {s: "FNO" for s in fo}
    if getattr(config, "STOCK_LIST_INCLUDE_NIFTY500", True):
        try:
            for s in instruments.resolve_nifty500_stocks():
                universe.setdefault(s, "CASH")
        except Exception as e:
            print(f"Nifty 500 list failed (continuing with F&O only): {e}", flush=True)
    lots = instruments.get_fno_lot_sizes()
    syms = sorted(universe)
    print(f"[stock_list] {len(syms)} stocks ({sum(1 for v in universe.values() if v == 'FNO')} FNO)", flush=True)

    def _link(sym):
        try:
            return sym, build_chart_link(sym)
        except Exception:
            return sym, f"https://www.tradingview.com/chart/?symbol=NSE:{sym}"
    with ThreadPoolExecutor(max_workers=8) as pool:
        links = dict(pool.map(_link, syms))

    by_sector = {}
    for sym in syms:
        sec = (_sector_label(sym) or "Other").strip() or "Other"
        by_sector.setdefault(sec, []).append(sym)

    def _line(sym):
        if universe[sym] == "FNO":
            lot = lots.get(sym)
            tag = f"FNO \u00b7 lot {lot}" if lot else "FNO"
        else:
            tag = "CASH"
        return (f"\u2022 <b>{_html.escape(sym)}</b> [{tag}] "
                f"<a href=\"{links[sym]}\">Chart</a> \u00b7 "
                f"<a href=\"https://www.screener.in/company/{sym}/consolidated/\">Screener</a>")

    n_fno = sum(1 for v in universe.values() if v == "FNO")
    head = (f"\U0001F4CB <b>ALL STOCKS \u2014 sector wise</b>  {now_ist.strftime('%d %b %Y')}\n"
            f"FNO {n_fno} \u00b7 CASH {len(universe) - n_fno} \u00b7 Sectors {len(by_sector)}\n"
            f"FNO = F&O stock (lot qty shown)  \u00b7  CASH = cash only")
    msgs, cur = [], head
    LIMIT = 3800
    for sec in sorted(by_sector, key=lambda x: (x == "Other", x)):
        members = sorted(by_sector[sec], key=lambda x: (universe[x] != "FNO", x))
        pending = f"\n\n\U0001F3ED <b>{_html.escape(sec)}</b> ({len(members)})"
        for sym in members:
            line = _line(sym)
            add = (pending + "\n" if pending else "\n") + line
            if len(cur) + len(add) > LIMIT:
                msgs.append(cur)
                cur = ""
                add = f"\U0001F3ED <b>{_html.escape(sec)}</b> (contd)\n{line}"
            cur += add
            pending = None
    if cur.strip():
        msgs.append(cur)
    print(f"[stock_list] sending {len(msgs)} messages", flush=True)
    for i, m in enumerate(msgs, 1):
        for attempt in range(3):
            try:
                r = requests.post(f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage",
                                  data={"chat_id": config.TELEGRAM_CHAT_ID, "text": m, "parse_mode": "HTML",
                                        "disable_web_page_preview": True}, timeout=20)
                if r.status_code == 429:
                    time.sleep(int(r.json().get("parameters", {}).get("retry_after", 5)) + 1)
                    continue
                r.raise_for_status()
                break
            except Exception as e:
                print(f"[stock_list] message {i}/{len(msgs)} failed (try {attempt + 1}): {e}", flush=True)
                time.sleep(2)
        time.sleep(1.1)


def _run_telegram_test():
    """SCAN_MODE=telegram_test: send ONE test message to TELEGRAM_CHAT_ID and exit. Works any time
    (no market-session or Upstox check). Prints Telegram's answer in the log so a failure is visible."""
    tok, chat = config.TELEGRAM_BOT_TOKEN, str(config.TELEGRAM_CHAT_ID or "")
    print(f"[telegram_test] token_set={bool(tok)} chat_id_set={bool(chat)} "
          f"chat_type={'private' if chat and not chat.startswith('-') else ('group' if chat else 'none')} "
          f"chat_id_ends_with=...{chat[-4:] if chat else ''}", flush=True)
    if not tok or not chat:
        print("[telegram_test] TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID secret missing - nothing sent.", flush=True)
        sys.exit(1)
    try:
        from telegram_notifier import BUILD_TAG
    except Exception:
        BUILD_TAG = "unknown"
    now_ist = _now_ist()
    text = (f"\u2705 <b>Test message from GitHub</b>\n"
            f"Time: {now_ist.strftime('%Y-%m-%d %H:%M')} IST\n"
            f"Code version: <b>{BUILD_TAG.split(' ')[0]}</b>\n"
            f"Alerts will come to this chat.")
    r = requests.post(f"https://api.telegram.org/bot{tok}/sendMessage",
                      data={"chat_id": chat, "text": text, "parse_mode": "HTML"}, timeout=20)
    print(f"[telegram_test] Telegram answered HTTP {r.status_code}: {r.text[:300]}", flush=True)
    if r.status_code != 200:
        sys.exit(1)


def _run_inner():
    if os.environ.get("SCAN_MODE", "full") == "telegram_test":
        _run_telegram_test()
        return
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
    if mode not in getattr(config, "ENABLED_SCAN_MODES", {mode}):
        print(f"SCAN_MODE={mode} is switched off (config.ENABLED_SCAN_MODES) — nothing sent.", flush=True)
        return
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
        # try/except ADDED (per request, 2026-08-30 — "run korlam,
        # kichu ashe ni") — this call had NO error handling before, so
        # any unhandled exception (NSE fetch blocked, Upstox token
        # issue, etc.) killed the process with zero Telegram output —
        # looked exactly like "nothing happened" instead of an actual,
        # visible failure. Now a crash at least sends a short warning
        # instead of dying silently.
        try:
            run_ema_cross_report(now_ist)
        except Exception as e:
            print(f"run_ema_cross_report failed: {e}", flush=True)
            send_mode_failure_notice("ema_cross_report", e, now_ist)
        return

    # SCAN_MODE=ema_cross_report_evening -> NEW (per request, 2026-08-29
    # — "sokale gotokal, bikale aajker cross chai") — the EVENING
    # counterpart, reporting crosses on TODAY's own daily candle
    # specifically (not just "whatever's latest", like the morning
    # version above). Needs a separate cron-job.org trigger, later in
    # the evening — see run_ema_cross_report_evening's docstring for
    # why timing matters here (Upstox's same-day candle finalization
    # isn't instant).
    if mode == "ema_cross_report_evening":
        try:
            run_ema_cross_report_evening(now_ist)
        except Exception as e:
            print(f"run_ema_cross_report_evening failed: {e}", flush=True)
            send_mode_failure_notice("ema_cross_report_evening", e, now_ist)
        return

    # SCAN_MODE=opening_bias_report -> NEW (per request) standalone
    # "F&O Opening 15-min Bias" report — one consolidated list of every
    # F&O stock whose first 15-min candle today was Open==Low or
    # Open==High. Meant to run once, shortly after 09:30 IST, via its
    # own cron trigger (see scan.yml). See run_opening_bias_report /
    # build_todays_opening_bias_list above.
    if mode == "opening_bias_report":
        try:
            _st15 = state.load_state()
            if state.already_alerted(_st15, "OPEN15M_REPORT", "DONE", now_ist.date().isoformat()):
                print("1st 15-min opening report already sent today — skipping.", flush=True)
                return
            state.mark_alerted(_st15, "OPEN15M_REPORT", "DONE", now_ist.date().isoformat())
            state.save_state(_st15)
            run_opening_bias_report(now_ist)
        except Exception as e:
            print(f"run_opening_bias_report failed: {e}", flush=True)
            send_mode_failure_notice("opening_bias_report", e, now_ist)
        return

    if mode == "top_movers_1min":
        # ADDED (per request, 2026-08-31 — "arek ta alert dao top
        # gainer looser in 1st 1 minit"). Own SCAN_MODE, own cron
        # trigger — see run_top_movers_1min's docstring for timing.
        try:
            run_top_movers_1min(now_ist)
        except Exception as e:
            print(f"run_top_movers_1min failed: {e}", flush=True)
            send_mode_failure_notice("top_movers_1min", e, now_ist)
        return

    # SCAN_MODE=breakout_scan -> the standalone 12-condition daily
    # breakout screener (added 2026-08-18), fully separate from the
    # EMA-cross alert scan above — see run_breakout_scan. Meant to run
    # once/day via its own cron trigger, at/after market close.
    if mode == "breakout_scan":
        try:
            run_breakout_scan(now_ist)
        except Exception as e:
            print(f"run_breakout_scan failed: {e}", flush=True)
            send_mode_failure_notice("breakout_scan", e, now_ist)
        return

    # SCAN_MODE=consolidation_breakout_scan -> the standalone
    # Consolidation Breakout scan (added, per request), fully separate
    # from both the EMA-cross alert scan and the 12-condition breakout
    # scan above. Meant to run once/day via its own cron trigger,
    # at/after market close, same as breakout_scan.
    if mode == "consolidation_breakout_scan":
        if config.CONSOLIDATION_BREAKOUT_SCAN_ENABLED:
            try:
                run_consolidation_breakout_scan(now_ist)
            except Exception as e:
                print(f"run_consolidation_breakout_scan failed: {e}", flush=True)
                send_mode_failure_notice("consolidation_breakout_scan", e, now_ist)
        else:
            print("Consolidation breakout scan disabled (config.CONSOLIDATION_BREAKOUT_SCAN_ENABLED=False) — skipping.", flush=True)
        return

    # SCAN_MODE=swing_entry_scan -> the standalone Swing-Trade Entry
    # Confluence Score screener (added, per request — "ekta perfect
    # swing trade er alert... sob combine kore ekta confluence
    # score"). NOT financial advice — see
    # strategy.check_swing_entry_scan's docstring. Own cron trigger,
    # meant to run once/day at/after market close (same timing as
    # breakout_scan/consolidation_breakout_scan).
    if mode == "swing_entry_scan":
        try:
            run_swing_entry_scan(now_ist)
        except Exception as e:
            print(f"run_swing_entry_scan failed: {e}", flush=True)
            send_mode_failure_notice("swing_entry_scan", e, now_ist)
        return

    # SCAN_MODE=daily_ema50_scan -> Daily 50-EMA cross + MACD cross +
    # RSI>50 scan (added, per request 2026-10-09). Own cron trigger,
    # once/day after the close (~16:00-16:30 IST). See
    # run_daily_ema50_scan.
    if mode == "daily_ema50_scan":
        if config.DAILY_EMA50_SCAN_ENABLED:
            try:
                run_daily_ema50_scan(now_ist)
            except Exception as e:
                print(f"run_daily_ema50_scan failed: {e}", flush=True)
                send_mode_failure_notice("daily_ema50_scan", e, now_ist)
        else:
            print("Daily EMA50 scan disabled (config.DAILY_EMA50_SCAN_ENABLED=False) — skipping.", flush=True)
        return

    if mode == "daily_scan":
        if config.DAILY_ALERT_ENABLED:
            try:
                _st = state.load_state()   # mark today DONE so the normal-run hook doesn't repeat it
                state.mark_alerted(_st, "DAILY_SCAN", "DONE", now_ist.date().isoformat())
                state.save_state(_st)
                run_daily_macd_rsi_scan(now_ist)
            except Exception as e:
                print(f"run_daily_macd_rsi_scan failed: {e}", flush=True)
                send_mode_failure_notice("daily_scan", e, now_ist)
        return

    if mode == "daily_bb":
        try:
            _stb = state.load_state()
            state.mark_alerted(_stb, "DAILY_BB", "DONE", now_ist.date().isoformat())
            state.save_state(_stb)
            run_daily_bb_report(now_ist)
        except Exception as e:
            print(f"run_daily_bb_report failed: {e}", flush=True)
            send_mode_failure_notice("daily_bb", e, now_ist)
        return

    if mode == "day_movers":
        try:
            run_day_movers_report(now_ist)
        except Exception as e:
            print(f"run_day_movers_report failed: {e}", flush=True)
            send_mode_failure_notice("day_movers", e, now_ist)
        return

    if mode == "stock_list":
        try:
            run_stock_list_report(now_ist)
        except Exception as e:
            print(f"run_stock_list_report failed: {e}", flush=True)
            send_mode_failure_notice("stock_list", e, now_ist)
        return

    if mode == "opening_bias_report_30m":
        try:
            run_opening_bias_report(now_ist, window_min=30)
        except Exception as e:
            print(f"run_opening_bias_report (30m) failed: {e}", flush=True)
            send_mode_failure_notice("opening_bias_report_30m", e, now_ist)
        return

    if mode == "opening_bias_report_1h":
        try:
            run_opening_bias_report(now_ist, hour=True)
        except Exception as e:
            print(f"run_opening_bias_report (1h) failed: {e}", flush=True)
            send_mode_failure_notice("opening_bias_report_1h", e, now_ist)
        return

    if mode == "oversold_report":
        try:
            run_oversold_report(now_ist)
        except Exception as e:
            print(f"run_oversold_report failed: {e}", flush=True)
            send_mode_failure_notice("oversold_report", e, now_ist)
        return

    if mode == "oversold_eod":
        try:
            run_oversold_eod_report(now_ist)
        except Exception as e:
            print(f"run_oversold_eod_report failed: {e}", flush=True)
            send_mode_failure_notice("oversold_eod", e, now_ist)
        return

    if mode == "alert_stats":
        try:
            run_alert_stats(now_ist)
        except Exception as e:
            print(f"run_alert_stats failed: {e}", flush=True)
            send_mode_failure_notice("alert_stats", e, now_ist)
        return

    if mode == "eod_report":
        try:
            run_eod_alert_report(now_ist)
        except Exception as e:
            print(f"run_eod_alert_report failed: {e}", flush=True)
            send_mode_failure_notice("eod_report", e, now_ist)
        return

    if mode == "monthly_scan":
        if config.MONTHLY_ALERT_ENABLED:
            try:
                run_monthly_macd_rsi_scan(now_ist)
            except Exception as e:
                print(f"run_monthly_macd_rsi_scan failed: {e}", flush=True)
                send_mode_failure_notice("monthly_scan", e, now_ist)
        return

    # Daily MACD+RSI alert: first run of the day from 08:00 IST (the 09:00 cron run
    # is before the trading session, so this sits ahead of the session gate).
    try:
        maybe_run_daily_scan_once_a_day(now_ist)
    except Exception as e:
        print(f"Daily scan failed this run (non-blocking): {e}", flush=True)
        try:
            send_mode_failure_notice("daily_scan", e, now_ist)
        except Exception:
            pass

    # Daily Bollinger band report (first run at/after DAILY_BB_RUN_AFTER, ~09:01).
    try:
        maybe_run_daily_bb_once_a_day(now_ist)
    except Exception as e:
        print(f"Daily BB report failed this run (non-blocking): {e}", flush=True)

    # End-of-day summary of today's alerts (first run at/after 15:40 IST).
    try:
        maybe_run_eod_report_once_a_day(now_ist)
    except Exception as e:
        print(f"EOD report failed this run (non-blocking): {e}", flush=True)

    # Top 10 gainers / losers (auto times in config.DAY_MOVERS_AUTO_TIMES).
    try:
        maybe_run_day_movers_once_a_day(now_ist)
    except Exception as e:
        print(f"Day movers report failed this run (non-blocking): {e}", flush=True)

    # Top-5 gainers / losers: last day, 1st 15 / 30 / 60 min.
    try:
        maybe_run_top5_once_a_day(now_ist)
    except Exception as e:
        print(f"Top5 report failed this run (non-blocking): {e}", flush=True)

    # 1st 15-min Open=Low/High + sector report (~09:31).
    try:
        maybe_run_opening_15m_once_a_day(now_ist)
    except Exception as e:
        print(f"Opening 15M report failed this run (non-blocking): {e}", flush=True)

    # 1st 30-min Open=Low/High + sector report (~09:47).
    try:
        maybe_run_opening_30m_once_a_day(now_ist)
    except Exception as e:
        print(f"Opening 30M report failed this run (non-blocking): {e}", flush=True)

    # 1st 1-hour Open=Low/High + sector report (~10:17).
    try:
        maybe_run_opening_1h_once_a_day(now_ist)
    except Exception as e:
        print(f"Opening 1H report failed this run (non-blocking): {e}", flush=True)

    # Daily RSI < 30 list (morning, ~9:30) + its day-end performance report.
    try:
        maybe_run_oversold_once_a_day(now_ist)
    except Exception as e:
        print(f"Oversold alert failed this run (non-blocking): {e}", flush=True)

    if not (_in_stock_session(now_ist) or _in_commodity_session(now_ist)):
        print(f"Outside all trading sessions ({now_ist.strftime('%H:%M')} IST) — skipping.", flush=True)
        return

    _, fo_failed, fo_total, fo_ds_hits, fo_ts_hits = run_fo_scan(now_ist)

    # 1-month MACD+RSI alert: once a day, non-blocking (see config.MONTHLY_ALERT_*)
    try:
        maybe_run_monthly_scan_once_a_day(now_ist)
    except Exception as e:
        print(f"Monthly scan failed this run (non-blocking): {e}", flush=True)

    # ---- Corporate-action alerts (Dividend/Bonus/Buyback/Order Win) ----
    # Same watchlist, same Telegram channel as the EMA alerts. Cheap to
    # call every run -- corporate_actions.check_and_alert() only does
    # real work (NSE fetch) once per calendar day; every other call
    # this same day is a no-op (see its own cache check).
    if corporate_actions is not None and getattr(config, "CORPORATE_ACTIONS_ALERT_ENABLED", True):
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
    n500_failed, n500_total, n500_ds_hits, n500_ts_hits = [], 0, [], []
    if _in_stock_session(now_ist):
        try:
            _, n500_failed, n500_total, n500_ds_hits, n500_ts_hits = run_nifty500_scan(now_ist)
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

    # Trading Score Summary (added, per request — "trading score
    # good/strong hole eksathe stock summary dao with chart") — ONE
    # combined message listing every stock whose individual EMA-cross
    # alert actually went out THIS run (i.e. already passed both the
    # EMA-cross gate and the Trading Score GOOD/STRONG gate — see
    # strategy.passes_alert_gate). This is IN ADDITION to each
    # symbol's own detailed alert, not a replacement. No separate
    # dedup needed: each hit here already represents a freshly-sent,
    # cooldown-cleared alert this exact run (see state.mark_alerted
    # upstream), so there's nothing to compare against a "last sent"
    # state file — if the list is non-empty, it's genuinely new this
    # run. Simply not sent on a run with zero qualifying alerts.
    trading_score_summary_hits = fo_ts_hits + n500_ts_hits
    if config.TRADING_SCORE_SUMMARY_ENABLED and trading_score_summary_hits:
        trading_score_summary_hits.sort(
            key=lambda h: (-(h["trading_score"]["score"] if h["trading_score"] else 0), not h["is_fno"], h["symbol"])
        )
        try:
            send_trading_score_summary(trading_score_summary_hits, now_ist)
        except Exception as e:
            print(f"send_trading_score_summary failed: {e}")

    # ---- Fetch-failure visibility: one summary alert if enough
    # instruments failed to fetch this run ----
    maybe_send_failure_summary(fo_failed, fo_total, n500_failed, n500_total)


def _upstox_token_ok():
    """False only when Upstox clearly rejects the token (401). Network trouble => True (don't block)."""
    try:
        r = requests.get("https://api.upstox.com/v2/market-quote/ltp",
                         params={"instrument_key": "NSE_INDEX|Nifty 50"},
                         headers={"Authorization": f"Bearer {config.UPSTOX_ACCESS_TOKEN}", "Accept": "application/json"},
                         timeout=15)
        return r.status_code != 401
    except Exception as e:
        print(f"Token check skipped ({e})", flush=True)
        return True


def _notify_token_expired():
    """Telegram note (max once per 60 min, remembered in alert_state.json)."""
    try:
        st = state.load_state()
        now = dt.datetime.now()
        last = st.get("_token_notice")
        if last:
            try:
                if (now - dt.datetime.fromisoformat(last)).total_seconds() < 3600:
                    return
            except Exception:
                pass
        st["_token_notice"] = now.isoformat()
        state.save_state(st)
        requests.post(f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage",
                      data={"chat_id": config.TELEGRAM_CHAT_ID, "parse_mode": "HTML",
                            "text": "⚠️ <b>UPSTOX TOKEN EXPIRED</b>\nAj kono alert ashbe na. Notun token toiri kore GitHub → Settings → Secrets → <b>UPSTOX_TOKEN</b> update koro."},
                      timeout=15)
    except Exception as e:
        print(f"Token notice failed: {e}", flush=True)


def run():
    if not _upstox_token_ok():
        print("Upstox token rejected (401) — skipping run.", flush=True)
        _notify_token_expired()
        return
    try:
        _run_inner()
    finally:
        # send the run's queued 15-min alerts as one combined message
        try:
            flush_pending_alerts()
        except Exception as e:
            print(f"flush_pending_alerts failed: {e}", flush=True)


if __name__ == "__main__":
    run()
