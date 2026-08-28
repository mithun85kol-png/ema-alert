"""
Central config for the EMA 9/20 alert bot.

Fill in UPSTOX_ACCESS_TOKEN (or Analytics Token, as you were using before)
and TELEGRAM values as environment variables / GitHub Secrets:
  UPSTOX_ACCESS_TOKEN
  TELEGRAM_BOT_TOKEN
  TELEGRAM_CHAT_ID
"""

import os

# ---------- Auth ----------
UPSTOX_ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# ---------- Timeframe ----------
CANDLE_INTERVAL = "3minute"   # Upstox intraday candle unit
LOOKBACK_CANDLES = 60         # enough history for EMA20 + RSI14 + volume avg to warm up

# ---------- Fetching (Step 1: ~50 F&O stocks/index/commodity scan) ----------
# Raised 15 -> 30. This scan only covers ~50 instruments so it was never
# the bottleneck, but a bit more headroom costs nothing and keeps this
# scan comfortably fast even if Upstox is briefly slow to respond.
FETCH_WORKERS = 30

# ---------- Historical 1-min fetch (75-min EMA warmup, main.py's
# build_hist1min_cache / fetch_historical_1min) ----------
# Deliberately much gentler than FETCH_WORKERS above. This hits
# Upstox's historical-candle endpoint once per symbol per day for the
# full ~214-instrument watchlist, and that endpoint has a noticeably
# tighter rate limit than the intraday one — running it at
# FETCH_WORKERS (30) concurrency caused mass "429 Too Many Requests"
# errors (visible in scan logs) even with per-call retries, because
# all 30 threads retried in lockstep and collided again. A smaller
# worker pool + a hard combined-rate cap (see main.py's _RateLimiter /
# _hist_fetch_limiter) fixes this. This step is not latency-sensitive
# (once a day, not on the 3-min hot path), so trading some speed here
# for reliability is free.
HIST_FETCH_WORKERS = 6
HIST_FETCH_MAX_PER_SECOND = 4   # hard cap on combined outbound rate to this endpoint, across all HIST_FETCH_WORKERS threads

# ---------- Daily-candle fetch for R3/S3 pivots (main.py's
# fetch_prev_day_ohlc / build_pivot_levels) ----------
# Same reasoning/pattern as HIST_FETCH_WORKERS/HIST_FETCH_MAX_PER_SECOND
# above, for Upstox's daily-candle endpoint. Was safe to run unrated at
# FETCH_WORKERS (30) concurrency while this only covered the
# ~214-instrument F&O/index/commodity list, but started returning mass
# "429 Too Many Requests" once the Nifty 500 scan added ~500 more names
# hitting this same endpoint in the same run.
PIVOT_FETCH_WORKERS = 6
PIVOT_FETCH_MAX_PER_SECOND = 4

# Caps outbound requests to Upstox's intraday-candle endpoint (used by
# fetch_1min_candles — the per-run hot path for every instrument in the
# watchlist, up to ~714 combined for F&O+Nifty500). Same reasoning as
# HIST_FETCH_MAX_PER_SECOND/PIVOT_FETCH_MAX_PER_SECOND above: 4/sec
# sustained = 240/min, safely under Upstox's 250/min cap for this
# endpoint. Without this, FETCH_WORKERS (30) threads firing unrated
# would burst well past 250/min for the combined F&O+Nifty500 watchlist,
# causing instruments to be silently skipped for the run (fixed
# 2026-08-12 after fetch-failure warnings on ~45% of the Nifty 500
# list). Trade-off: the full ~714-instrument fetch now takes ~3 minutes
# instead of ~30 seconds, but nothing gets dropped.
INTRADAY_FETCH_MAX_PER_SECOND = 4

# ---------- Upstox request retry/backoff (applies to ALL Upstox calls:
# intraday candles, daily OHLC for pivots) ----------
# If Upstox returns 429 (rate-limited) or a transient 5xx, retry a few
# times with a short increasing wait instead of dropping the instrument
# from this run. Keeps a temporary rate-limit from silently skipping a
# stock -- worst case it costs an extra second or two on that symbol,
# it does not get permanently missed.
UPSTOX_MAX_RETRIES = 3          # total attempts = 1 initial + this many retries... see main.py (attempts = MAX_RETRIES)
UPSTOX_RETRY_STATUS_CODES = (429, 500, 502, 503, 504)
UPSTOX_RETRY_BACKOFF_BASE_SECONDS = 0.6   # attempt 1 wait ~0.6s, attempt 2 ~1.2s, attempt 3 ~1.8s

# ---------- Indicator settings ----------
EMA_FAST = 9
EMA_SLOW = 20

# EMA9/EMA50 cross on 75-min candles — the 75-min F&O stock/commodity
# alert's crossover pair (replaces the old 9/20 pair). EMA_FAST (9)
# above is reused as the fast leg; this is only the new slow leg.
# Used whenever config.PRIMARY_TIMEFRAME == "75min" (see main.py).
# Trend/volume/strong-candle gating on this pair is controlled by
# REQUIRE_TREND_CONFIRMATION / REQUIRE_VOLUME_CONFIRMATION /
# REQUIRE_STRONG_CANDLE below, same as the 15-min primary pair.
PRIMARY_EMA_SLOW_75MIN = 50
RSI_PERIOD = 14
RSI_BULLISH_MIN = 55          # unused by current strategy.py (informational-only design); kept for reference
RSI_BEARISH_MAX = 45          # unused by current strategy.py (informational-only design); kept for reference
VOLUME_AVG_PERIOD = 20
VOLUME_MULTIPLIER = 1.3       # unused by current strategy.py (informational-only design); kept for reference
STRONG_CANDLE_BODY_RATIO = 0.30  # unused by current strategy.py (strong-candle filter removed — plain cross + mandatory volume now); kept for reference

# ---------- Primary/alerting timeframe toggle (stocks/commodities) ----------
# Set to "15min" or "75min" — see main.py's module docstring for the
# full explanation. "75min" is the current/previous default behavior
# (EMA9 x EMA50 on 75-min candles); "15min" switches to EMA9 x EMA20
# on 15-min candles instead, with 75-min shown as informational
# context under the alert. Flip this ONE setting to switch; nothing
# else needs to change.
PRIMARY_TIMEFRAME = "15min"

# Three independently toggleable gating conditions on the primary-
# timeframe crossing candle (see strategy.check_signals /
# strategy._evaluate_candle). All three True/mandatory matches the
# strategy.py docstring's current design: a plain EMA cross that
# agrees with the EMA50 trend AND has rising volume on the crossing
# candle. REQUIRE_STRONG_CANDLE is False because that filter was
# removed from strategy.py (kept here only so main.py's call doesn't
# need an unconditional True/False literal baked in).
REQUIRE_TREND_CONFIRMATION = True
# CHANGED (per request): only EMA cross + EMA50 trend agreement should
# gate an alert now — volume confirmation on the crossing candle is no
# longer mandatory (still computed/shown on the alert as before, just
# doesn't block).
REQUIRE_VOLUME_CONFIRMATION = False
REQUIRE_STRONG_CANDLE = False

# ADDED (per request): two more mandatory gating conditions on top of
# EMA cross + EMA50 trend above — see strategy._evaluate_candle /
# strategy.check_signals (require_macd_cross / require_rsi_confirmation
# params).
#   REQUIRE_MACD_CROSS: a RECENT MACD line/signal crossover (within
#   MACD_DIVERGENCE_LOOKBACK_CANDLES below) matching the EMA cross's
#   direction must exist, or the alert is rejected.
#   REQUIRE_RSI_CONFIRMATION: RSI(14) > 50 for a bullish cross, < 50
#   for a bearish cross, or the alert is rejected.
# Both apply to every scan (indices, F&O stocks/commodities, Nifty
# 500) — unlike REQUIRE_TREND_CONFIRMATION/REQUIRE_VOLUME_CONFIRMATION
# above, these two are NOT skipped for indices.
REQUIRE_MACD_CROSS = True
REQUIRE_RSI_CONFIRMATION = True

# CHANGED (per request): Volume Spike was purely informational before
# (shown on every alert, never blocked anything). Now it's a BLOCKING
# condition on the F&O stocks/commodities and Nifty 500 stock alerts
# (never applied to indices -- same precedent as the confluence filter
# below, which also skips indices): the crossing signal is only sent
# if yesterday's completed daily volume also exceeded the volume from
# 5 trading days before that (see main.py's build_momentum_volume_data
# / signal["volume_spike"]). If the daily-history fetch for this
# symbol failed or didn't have enough days yet today (volume_spike is
# None, not False), this does NOT block the alert -- only an explicit
# "No" blocks it, so a data-fetch hiccup never silently swallows a
# real signal. Set back to False to make it informational-only again.
# CHANGED (per request): back to informational-only — only EMA cross +
# EMA50 trend agreement should gate an alert now, nothing else.
REQUIRE_VOLUME_SPIKE = False

# Minimum distance between EMA9 and EMA20 at the moment of crossover,
# as a % of close price. unused by current strategy.py (gap filter
# removed — any qualifying cross is eligible regardless of gap size);
# kept for reference.
MIN_EMA_CROSS_GAP_PCT = 0.05

# How many of the most recent CLOSED candles to re-check for a cross on
# every run (not just the single latest one). This is what lets the bot
# "catch up" if a scheduled run is skipped or delayed — any cross that
# happened on an in-between candle still gets alerted on the next run,
# instead of silently disappearing. Each candle already alerted is
# never re-sent (see state.py). Still used by the unused/dormant
# check_signals_15min() — kept for reference.
CROSS_LOOKBACK_CANDLES = 5

# ---------- 75-min = PRIMARY/ALERTING timeframe ----------
# 75-min is what decides whether/when an alert fires; the EMA9/50
# cross + trend-agreement conditions in strategy.py run on 75-min
# candles. Catch-up window: how many of the most recent CLOSED 75-min
# candles to re-check on every run, in case a run was skipped/delayed.
# Widened 2 -> 6 (per Mithun's instruction, 2026-08-14) so a full
# trading session's worth of 75-min closes (~5-6 closes between 09:15
# and 15:30) is always covered even after a longer gap in runs — no
# close is ever silently missed. This only widens the RE-CHECK window;
# it can never cause a duplicate alert, since state.already_alerted
# dedupes strictly on (symbol, direction, exact candle_time) regardless
# of how many past candles get re-scanned.
PRIMARY_LOOKBACK_CANDLES = 6

# How many trailing 15-min candles the informational context block
# (strategy.get_3min_trend_info, called on df15 — per request,
# 2026-08-12, changed from the 3-min chart to the 15-min chart) scans
# to report "a cross happened N candles ago" and how close EMA9/20
# currently are to crossing on the 15-min chart. 5 candles x 15-min =
# 75 minutes. Purely informational — attached to every 75-min alert but
# never blocks it.
INFO_3MIN_LOOKBACK_CANDLES = 5

# ---------- Index-only pure 15-min EMA cross alert ----------
# INDICES (NIFTY 50, NIFTY BANK, SENSEX) don't use the 75-min flow at
# all. main.py calls strategy.check_signals() directly on df15
# (CHANGED from df5/5-min to df15/15-min, per request) with
# require_trend_confirmation=False — a plain EMA9/20 cross on the
# 15-min chart, no trend requirement, no 75-min gate. This is how many
# trailing CLOSED 15-min candles are re-checked on every run (catch-up
# window, same idea as PRIMARY_LOOKBACK_CANDLES below but for the
# index's 15-min alert specifically).
INDEX_ALERT_LOOKBACK_CANDLES = 8  # 8 x 15-min = 120 min lookback
# (safety buffer for the dedicated index-only cron job — covers
# several missed/delayed runs, not just the exact interval gap)

# Informational trade-plan fields (computed in strategy.py, currently
# not shown in the Telegram message — see telegram_notifier.py):
# Stop Loss = EMA20 (on the cross candle), Target = highest high
# (bullish) / lowest low (bearish) over the trailing window below.
# 5 candles x 3-min = 15 minutes.
TARGET_LOOKBACK_CANDLES = 5

# ---------- MACD (added) — informational only, never blocks a signal ----------
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

# How many trailing candles to scan for a MACD bullish/bearish
# divergence (the window is split into two halves; price extremes and
# MACD-line values in each half are compared — see
# strategy._detect_macd_divergence). Now computed on 75-min candles (the
# primary timeframe): 8 candles x 75-min = 600 minutes (~2 trading
# days).
MACD_DIVERGENCE_LOOKBACK_CANDLES = 8

# ---------- Indices (cash/index segment, no expiry) ----------
INDICES = {
    "NIFTY 50": "Nifty 50",
    "NIFTY BANK": "Nifty Bank",
    "SENSEX": "SENSEX",
}

# ---------- MCX Commodities (nearest-expiry futures) ----------
COMMODITIES = {
    "GOLD": "GOLD",
    "SILVER": "SILVER",
    "CRUDEOIL": "CRUDEOIL",
}

# ---------- TradingView chart-link symbol overrides ----------
# build_chart_link() in main.py defaults to "NSE:{symbol}" for plain
# stocks, which is correct for the F&O watchlist + Nifty 500. Indices
# and MCX commodity futures don't chart correctly under that default,
# so they're overridden here to the TradingView symbol that actually
# renders their chart. Add an entry here for any future index/
# commodity that also charts wrong under "NSE:{symbol}".
TRADINGVIEW_SYMBOL_OVERRIDES = {
    "NIFTY 50": "NSE:NIFTY",
    "NIFTY BANK": "NSE:BANKNIFTY",
    "SENSEX": "BSE:SENSEX",
    "GOLD": "MCX:GOLD1!",
    "SILVER": "MCX:SILVER1!",
    "CRUDEOIL": "MCX:CRUDEOIL1!",
}


# ---------- F&O stocks ----------
USE_FULL_FO_LIST = True

FO_STOCK_WATCHLIST = [
    "RELIANCE", "HDFCBANK", "ICICIBANK", "INFY", "TCS",
    "SBIN", "AXISBANK", "KOTAKBANK", "TATAMOTORS", "BAJFINANCE",
]

# ---------- Nifty 500 cash-stock scan (added) ----------
# Runs the exact same signal logic as the F&O 75-min flow (EMA cross +
# mandatory EMA50 trend agreement, RSI/volume/VWAP/MACD/pivot as
# informational context only — see strategy.check_signals) but on the
# full Nifty 500 constituent list (cash/EQ segment) and with EMA9/21
# instead of EMA9/20. The Nifty 500 list itself is fetched live from
# NSE's archives (see instruments.resolve_nifty500_stocks) rather than
# hardcoded here, since NSE rebalances the index periodically and a
# static list here would silently go stale. A stock that happens to
# also be F&O-eligible still gets scanned here too (with 9/21) in
# addition to the F&O scan (with 9/20) — they're independent alerts,
# deduped separately (see main.py's run_nifty500_scan).
NIFTY500_EMA_FAST = 9
NIFTY500_EMA_SLOW = 50   # was 21 — now EMA9 x EMA50 on 75-min, same as the F&O scan

# ---------- Sector Index mapping (added) ----------
# Maps a display name (used on the alert message and as the key of
# STOCK_SECTOR_MAP's values below) to the search name
# instruments.resolve_indices() looks up in the Upstox instrument
# master (NSE_INDEX segment, matched against 'name' or
# 'trading_symbol') — same resolution mechanism as INDICES above, just
# a separate dict so sector indices don't get mixed into the main
# scan/alert loop (they never fire their own EMA-cross alert; they
# only supply a trend reading to related stocks' alerts).
SECTOR_INDICES = {
    "NIFTY BANK": "Nifty Bank",
    "NIFTY PSU BANK": "Nifty PSU Bank",
    "NIFTY IT": "Nifty IT",
    "NIFTY AUTO": "Nifty Auto",
    "NIFTY PHARMA": "Nifty Pharma",
    "NIFTY FMCG": "Nifty FMCG",
    "NIFTY METAL": "Nifty Metal",
    "NIFTY ENERGY": "Nifty Energy",
    "NIFTY OIL AND GAS": "Nifty Oil & Gas",
    "NIFTY REALTY": "Nifty Realty",
    "NIFTY FIN SERVICE": "Nifty Financial Services",
    "NIFTY MEDIA": "Nifty Media",
    "NIFTY CONSUMER DURABLES": "NIFTY CONSR DURBL",
    "NIFTY INFRA": "Nifty Infrastructure",
}

# Maps each stock's NSE trading symbol (upper-case) to the sector index
# display name above (a key of SECTOR_INDICES) it belongs to. A stock
# with no entry here simply gets no sector-trend line on its alert —
# never an error, just gracefully omitted (see main.py). NOT exhaustive
# — the full F&O list runs to 180+ names; this covers the most liquid/
# commonly traded F&O stocks across the major sectors. Extend freely;
# any symbol left unmapped just degrades gracefully.
STOCK_SECTOR_MAP = {
    # Banking (private)
    "HDFCBANK": "NIFTY BANK", "ICICIBANK": "NIFTY BANK", "AXISBANK": "NIFTY BANK",
    "KOTAKBANK": "NIFTY BANK", "INDUSINDBK": "NIFTY BANK", "FEDERALBNK": "NIFTY BANK",
    "IDFCFIRSTB": "NIFTY BANK", "BANDHANBNK": "NIFTY BANK", "AUBANK": "NIFTY BANK",
    # Banking (PSU)
    "SBIN": "NIFTY PSU BANK", "PNB": "NIFTY PSU BANK", "BANKBARODA": "NIFTY PSU BANK",
    "CANBK": "NIFTY PSU BANK", "UNIONBANK": "NIFTY PSU BANK",
    # IT
    "TCS": "NIFTY IT", "INFY": "NIFTY IT", "WIPRO": "NIFTY IT", "HCLTECH": "NIFTY IT",
    "TECHM": "NIFTY IT", "LTIM": "NIFTY IT", "MPHASIS": "NIFTY IT", "COFORGE": "NIFTY IT",
    "PERSISTENT": "NIFTY IT",
    # Auto
    "TATAMOTORS": "NIFTY AUTO", "MARUTI": "NIFTY AUTO", "M&M": "NIFTY AUTO",
    "BAJAJ-AUTO": "NIFTY AUTO", "EICHERMOT": "NIFTY AUTO", "HEROMOTOCO": "NIFTY AUTO",
    "TVSMOTOR": "NIFTY AUTO", "ASHOKLEY": "NIFTY AUTO", "BHARATFORG": "NIFTY AUTO",
    "MOTHERSON": "NIFTY AUTO",
    # Pharma
    "SUNPHARMA": "NIFTY PHARMA", "CIPLA": "NIFTY PHARMA", "DRREDDY": "NIFTY PHARMA",
    "DIVISLAB": "NIFTY PHARMA", "AUROPHARMA": "NIFTY PHARMA", "LUPIN": "NIFTY PHARMA",
    "BIOCON": "NIFTY PHARMA", "TORNTPHARM": "NIFTY PHARMA", "ALKEM": "NIFTY PHARMA",
    # FMCG
    "HINDUNILVR": "NIFTY FMCG", "ITC": "NIFTY FMCG", "NESTLEIND": "NIFTY FMCG",
    "BRITANNIA": "NIFTY FMCG", "TATACONSUM": "NIFTY FMCG", "DABUR": "NIFTY FMCG",
    "GODREJCP": "NIFTY FMCG", "MARICO": "NIFTY FMCG", "COLPAL": "NIFTY FMCG",
    "VBL": "NIFTY FMCG",
    # Metal
    "TATASTEEL": "NIFTY METAL", "JSWSTEEL": "NIFTY METAL", "HINDALCO": "NIFTY METAL",
    "VEDL": "NIFTY METAL", "JINDALSTEL": "NIFTY METAL", "SAIL": "NIFTY METAL",
    "NMDC": "NIFTY METAL", "NATIONALUM": "NIFTY METAL", "HINDCOPPER": "NIFTY METAL",
    # Energy / Oil & Gas
    "RELIANCE": "NIFTY ENERGY", "POWERGRID": "NIFTY ENERGY", "NTPC": "NIFTY ENERGY",
    "ADANIGREEN": "NIFTY ENERGY", "TATAPOWER": "NIFTY ENERGY", "ADANIENSOL": "NIFTY ENERGY",
    "ONGC": "NIFTY OIL AND GAS", "IOC": "NIFTY OIL AND GAS", "BPCL": "NIFTY OIL AND GAS",
    "GAIL": "NIFTY OIL AND GAS",
    # Realty
    "DLF": "NIFTY REALTY", "GODREJPROP": "NIFTY REALTY", "OBEROIRLTY": "NIFTY REALTY",
    "PHOENIXLTD": "NIFTY REALTY", "PRESTIGE": "NIFTY REALTY", "LODHA": "NIFTY REALTY",
    # Financial Services (NBFC / insurance / broking — non-bank)
    "BAJFINANCE": "NIFTY FIN SERVICE", "BAJAJFINSV": "NIFTY FIN SERVICE",
    "HDFCLIFE": "NIFTY FIN SERVICE", "SBILIFE": "NIFTY FIN SERVICE",
    "ICICIGI": "NIFTY FIN SERVICE", "ICICIPRULI": "NIFTY FIN SERVICE",
    "SHRIRAMFIN": "NIFTY FIN SERVICE", "CHOLAFIN": "NIFTY FIN SERVICE",
    "MUTHOOTFIN": "NIFTY FIN SERVICE", "PFC": "NIFTY FIN SERVICE", "RECLTD": "NIFTY FIN SERVICE",
    # Media
    "SUNTV": "NIFTY MEDIA", "ZEEL": "NIFTY MEDIA", "PVRINOX": "NIFTY MEDIA",
    # Consumer Durables
    "TITAN": "NIFTY CONSUMER DURABLES", "HAVELLS": "NIFTY CONSUMER DURABLES",
    "VOLTAS": "NIFTY CONSUMER DURABLES", "CROMPTON": "NIFTY CONSUMER DURABLES",
    # Infra / Cement / Capital goods / Ports (grouped under Infra)
    "LT": "NIFTY INFRA", "ULTRACEMCO": "NIFTY INFRA", "GRASIM": "NIFTY INFRA",
    "SHREECEM": "NIFTY INFRA", "AMBUJACEM": "NIFTY INFRA", "ACC": "NIFTY INFRA",
    "ADANIPORTS": "NIFTY INFRA",
}

# ---------- 75-min timeframe warmup (now the PRIMARY/alerting timeframe) ----------
# 75-min is now what the EMA9/20 cross + EMA50 trend-agreement
# conditions actually run on (see strategy.check_signals, called with
# df75 in main.py). It needs EMA_SLOW=20, RSI_PERIOD=14,
# VOLUME_AVG_PERIOD=20, MACD_SLOW=26, and the 50-period trend EMA all
# warmed up — i.e. ~50+ bars minimum on the 75-min timeframe. At ~5
# bars/trading day, that's ~10-12 trading days (~14-17 calendar days
# with weekends). 28 calendar days gives comfortable margin (including
# a holiday-heavy stretch) while staying safely inside Upstox's max
# retrieval limit for the 1-15 minute interval band on the v3
# historical-candle endpoint, which is capped at "1 month leading up
# to to_date" — the previous value of 35 exceeded that cap and was
# why every instrument's 1-min warmup fetch was failing with
# "400 Bad Request". This many calendar days of PRE-TODAY 1-minute
# history are fetched once per day (cached) and combined with today's
# intraday data before resampling to 75-min.
HISTORICAL_1MIN_LOOKBACK_DAYS = 28
HIST_1MIN_CACHE_FILE = "historical_1min_cache.json"

# ---------- VWAP Momentum scan (replica of the Chartink "BUY VWAP EMA
# 9,20 RSI" screener) — a separate, standalone BUY-only alert. Runs on
# df5 (5-min candles, already fetched for the index alert — no extra
# API cost) for F&O futures-segment stocks only. Fires once when ALL
# of the below newly become true on a closed 5-min candle:
#   1. 5-min RSI(14) > VWAP_MOMENTUM_RSI_MIN
#   2. Day's Close / Day's Open > VWAP_MOMENTUM_DAY_CHANGE_MIN
#   3. Day's VWAP < the 5-min candle's Close
#   4. Day's cumulative Volume >= VWAP_MOMENTUM_MIN_VOLUME
#   5. Day's Close > VWAP_MOMENTUM_MIN_PRICE
# See strategy.check_vwap_momentum() for the transition-only firing
# logic (doesn't re-alert every run while conditions stay true).
VWAP_MOMENTUM_ENABLED = True
VWAP_MOMENTUM_RSI_PERIOD = 14
VWAP_MOMENTUM_RSI_MIN = 60
VWAP_MOMENTUM_DAY_CHANGE_MIN = 1.002
VWAP_MOMENTUM_MIN_VOLUME = 2_000_000
VWAP_MOMENTUM_MIN_PRICE = 350

# ---------- Confluence "High R:R" filter (added) ----------
# A secondary filter layered ONLY on top of already-qualifying 75-min
# EMA9/50 signals (stocks/commodities/Nifty 500) — never applied to
# index or VWAP-momentum alerts, which have their own separate rules.
# Combines fields the signal ALREADY carries (no new indicator or
# extra API call) into a single "is this a genuinely good risk:reward
# setup" gate, meant to cut the daily count down to only a handful
# (roughly 5/day) of higher-quality setups with a comparatively small
# stop and a bigger target. See strategy.passes_confluence_filter().
# CHANGED (per request): master flag now False too (was True but every
# sub-check underneath was already False, i.e. already a no-op) — only
# EMA cross + EMA50 trend agreement should gate an alert now.
CONFLUENCE_FILTER_ENABLED = False

# Each check below can be toggled independently without touching
# strategy.py — set back to True later to re-enable a specific check.
# Per Mithun's instruction (2026-08-12): only the OI buildup check is
# active for now; risk:reward / RSI band / sector trend are paused.
CONFLUENCE_CHECK_RISK_REWARD = False
CONFLUENCE_CHECK_RSI_BAND = False
CONFLUENCE_CHECK_SECTOR_TREND = False
# CHANGED (per request): OI buildup is informational only now, same as
# Momentum/Volume Spike/Delivery%/Sector/Bulk-Deal — it still shows on
# every alert ("OI Buildup: ..." line, unaffected by this) but no
# longer BLOCKS a signal from being sent. With all four confluence
# checks now False, passes_confluence_filter() always returns True —
# the "🎯 High R:R" tag on every alert is effectively vestigial at this
# point (kept as-is; say the word if you'd rather it just stop showing
# rather than showing on every alert).
CONFLUENCE_CHECK_OI_BUILDUP = False

CONFLUENCE_MIN_RISK_REWARD = 3.0
CONFLUENCE_RSI_BULLISH_MIN = 50
CONFLUENCE_RSI_BULLISH_MAX = 70
CONFLUENCE_RSI_BEARISH_MIN = 30
CONFLUENCE_RSI_BEARISH_MAX = 50

# ---------- Alert Gate (SIMPLIFIED, per request, 2026-08-28) ----------
# CHANGED from an OR-of-7 gate down to a SINGLE condition: the alert
# sends (after a valid EMA cross, as always — strong candle + volume
# mandatory, EMA50 trend agreement) if, and only if, Trading Score is
# GOOD or STRONG (i.e. >= QUALITY_GATE_MIN_TRADING_SCORE, out of the
# label bands 8-10 STRONG / 6-7.9 GOOD / 4-5.9 MODERATE / <4 WEAK — see
# strategy.compute_trading_score for the bands and
# strategy.passes_alert_gate for the exact implementation).
#
# The previous 6 other cases (Daily Score alone, Buy/Sell Score alone,
# Smart Money alone, the "combined" case, Bulk/Block deal, Volume
# Spike) are REMOVED from the gate decision per request — Trading
# Score already rolls all of those up into one number (see
# compute_trading_score's docstring), so gating on it alone is now the
# only thing that matters. Daily Score, Buy/Sell Score, Smart Money
# etc. are still computed and still SHOWN on every alert message (and
# still feed INTO Trading Score) — only the gate's OWN decision logic
# changed, not what data reaches Telegram.
#
# A signal that fails is simply not sent (and not marked alerted, so
# it's re-checked next run). Set QUALITY_GATE_ENABLED = False to go
# back to sending every qualifying EMA-cross signal regardless of
# score.
QUALITY_GATE_ENABLED = True
QUALITY_GATE_MIN_TRADING_SCORE = 6    # trading_score.score must be >= 6 (GOOD or STRONG band)

# ---------- "Near N-month High" Trading Score component (added, per
# request) ----------
# Close counts as "near" a high if it's within this % of ANY of the
# 1-6 month highs already computed in main.py's build_momentum_volume_data
# (signal["multi_month_highs"]) — see strategy.compute_near_high_score.
# Feeds into compute_trading_score as a 4th equally-weighted component,
# same normalize-to-/10 treatment as checklist/daily_score/smart_money.
# ---------- "Perfect Daily Score" F&O report (added, per request)
# ----------
# ONE combined Telegram message listing every F&O stock whose Daily
# Score (see strategy.compute_daily_score_scan) is currently at/above
# this threshold on its latest closed primary-timeframe candle —
# completely independent of whether that candle also had an EMA
# cross. Checked every scan run, but only actually SENT when the set
# of qualifying symbols has changed since the last time it was sent
# (see main.py's daily_score_report_state.json) — so a stock sitting
# at 8/8 for hours doesn't repeat the same message every 15 minutes.
DAILY_SCORE_REPORT_ENABLED = True
DAILY_SCORE_REPORT_MIN_SCORE = 8   # out of 8 — score must be >= this
DAILY_SCORE_REPORT_STATE_FILE = "daily_score_report_state.json"

NEAR_HIGH_THRESHOLD_PCT = 5.0

# ---------- "Smart Money Entry" 🐋 tag (added) ----------
# PURELY INFORMATIONAL — like every other tag on the alert now
# (Momentum, Volume Spike, EMA50/200, OI Buildup) this never blocks or
# skips a signal. Scores up to 9 independent, already-computed
# dimensions (OI buildup, volume vs previous candle, delivery %, VWAP
# cushion, a same-direction Bulk/Block deal, a matching Marubozu,
# Momentum, Volume Spike, EMA50/200 bias) — see
# strategy.compute_smart_money_signal for the exact rules. The tag
# only shows once at least this many points are scored (out of
# whichever of the 9 dimensions had data available that run — a
# missing field, e.g. no OI data on the Nifty 500 cash scan, is
# skipped rather than counted against it).
SMART_MONEY_MIN_SCORE = 5
SMART_MONEY_DELIVERY_THRESHOLD = 50.0
SMART_MONEY_VWAP_MIN_PCT = 0.3
SMART_MONEY_DEAL_LOOKBACK_DAYS = 3

# ---------- Trade Signal Score gate — REMOVED (per request) ----------
# compute_trade_score() was deleted from strategy.py and main.py no
# longer calls it; MIN_TRADE_SCORE / MIN_TRADE_SCORE_FACTORS are gone.
# The 15-Minute Intraday Trade Checklist (BUY/SELL Setup line on the
# alert) is the only score left — see config.OPENING_CANDLE_* below
# and strategy.compute_intraday_checklist.

# ---------- Same-direction alert cooldown (added, per request) ----------
# Applies ONLY to F&O stocks/commodities and Nifty 500 stock alerts
# (same scope as the Trade Score gate above -- index alerts are meant
# to be fast/frequent and keep their own separate, already-existing
# lookback logic untouched). Even if a genuinely NEW EMA cross fires
# on a later candle, the alert is suppressed if the same
# (symbol, direction) already alerted within this many minutes -- this
# is what stops a whipsaw-y stock from re-alerting the same direction
# again too soon (see state.in_cooldown). Set to 0 to disable and go
# back to alerting on every new qualifying candle regardless of how
# recently the same symbol+direction last fired.
SAME_DIRECTION_COOLDOWN_MINUTES = 60

# ---------- State / alert de-dup ----------
STATE_FILE = "alert_state.json"
DEDUPE_MINUTES = 30

# Nifty 500 constituent list, fetched from NSE archives and cached here
# (see instruments.resolve_nifty500_stocks) — refetched at most once per
# calendar day, same pattern as the other daily caches above.
NIFTY500_LIST_CACHE_FILE = "nifty500_list_cache.json"

# ---------- Call/Put writing buildup (added) — informational only ----------
# Strike band around the current spot price to include when comparing
# option-chain OI snapshots (see main.py's fetch_option_chain_snapshot).
# Strikes far OTM/ITM have thin, noisy OI that isn't useful for a
# writing-buildup read.
OI_STRIKE_RANGE_PCT = 3.0

# Minimum OI change (%) on a strike, between two snapshots, before it
# counts as a genuine buildup rather than noise (see main.py's
# compute_oi_buildup).
OI_BUILDUP_MIN_CHANGE_PCT = 3.0

# Indices: snapshotted every run regardless of whether an alert fires
# (see main.py's update_oi_buildup) — comparison is always ~one run
# apart (~3 min).
OI_BUILDUP_CACHE_FILE = "oi_buildup_cache.json"

# F&O stocks: snapshotted ON-DEMAND only when an alert is about to fire
# for that stock (see main.py's get_stock_oi_buildup) — re-fetching
# option chains for all ~180 F&O stocks every run would multiply
# Upstox API calls far past what the existing scan already uses.
# Comparison is therefore "since this stock's last alert", which can be
# minutes to days, not a fixed run-to-run window.
STOCK_OI_BUILDUP_CACHE_FILE = "oi_buildup_stock_cache.json"

# ---------- Fetch-failure visibility ----------
# After a run, if the number of instruments that failed to fetch (even
# after retries) is >= this many, a single short Telegram warning is
# sent summarizing the count -- so a persistent rate-limit or outage
# shows up as a visible signal instead of silently vanishing into the
# GitHub Actions log. Set to a high number (e.g. 9999) to disable.
FAILURE_ALERT_MIN_COUNT = 5

# ---------- Breakout Scan (added 2026-08-18) — Chartink-style 13-row
# screener, minus Row 2 (Market Cap — no data source in this bot; see
# chat). Runs on daily candles, once per day (own SCAN_MODE, own cron
# trigger — see scan.yml), across the Nifty 500 cash universe. See
# strategy.check_breakout_scan() for exactly how each row maps to a
# condition. Every threshold below is independently editable without
# touching strategy.py.
BREAKOUT_SCAN_ENABLED = True

# Row 1 — price floor
BREAKOUT_MIN_PRICE = 100

# Row 3 — turnover floor (₹100 crore = 1,000,000,000; use the full
# 10-digit number, NOT 100000000 which is only ₹10 crore)
BREAKOUT_MIN_TURNOVER = 1_000_000_000

# Row 4 — volume spike: today's volume vs the 20-day average volume AS
# OF YESTERDAY (today's own volume is never part of the baseline it's
# being compared to)
BREAKOUT_VOLUME_SMA_PERIOD = 20
BREAKOUT_VOLUME_SPIKE_MULTIPLIER = 2.5

# Rows 5/6 — trend filter (today's close vs its own 50/200-day SMA,
# both including today)
BREAKOUT_SMA_SHORT_PERIOD = 50
BREAKOUT_SMA_LONG_PERIOD = 200

# Rows 7/8 — RSI band
BREAKOUT_RSI_PERIOD = 14
BREAKOUT_RSI_MIN = 60
BREAKOUT_RSI_MAX = 75

# Row 9 — near 52-week high: today's close >= this fraction of the
# highest daily HIGH over the trailing BREAKOUT_NEAR_HIGH_LOOKBACK_DAYS
# days AS OF YESTERDAY (excludes today's own high, same "offset 1 day
# ago" reasoning as Row 4)
BREAKOUT_NEAR_HIGH_LOOKBACK_DAYS = 250
BREAKOUT_NEAR_HIGH_PCT = 0.97

# Row 10 — new breakout high: today's close > the highest daily HIGH
# over the trailing BREAKOUT_NEW_HIGH_LOOKBACK_DAYS days AS OF
# YESTERDAY (excludes today's own high — without this exclusion the
# row can never fire, since today's close can never beat a max that
# already includes today's own high)
BREAKOUT_NEW_HIGH_LOOKBACK_DAYS = 20

# Row 11 — tight base / volatility contraction: the trailing
# BREAKOUT_TIGHT_BASE_LOOKBACK_DAYS days' (high-low) range, INCLUDING
# today, must be under this fraction of today's close
BREAKOUT_TIGHT_BASE_LOOKBACK_DAYS = 10
BREAKOUT_TIGHT_BASE_MAX_RANGE_PCT = 0.08

# Row 12 — enough volatility to realistically move: today's ATR(14)
# (including today) must exceed this fraction of today's close
BREAKOUT_ATR_PERIOD = 14
BREAKOUT_ATR_MIN_PCT = 0.025

# Row 13 — bulls in charge: today's close vs today's cumulative session
# VWAP (needs today's own 5-min intraday candles — see
# run_breakout_scan)

# How many calendar days of pre-today daily history to fetch per
# symbol. Must comfortably exceed BREAKOUT_NEAR_HIGH_LOOKBACK_DAYS
# (250 calendar-day lookback needs ~250 trading days, which is
# ~350 calendar days including weekends/holidays); 380 gives margin.
BREAKOUT_HISTORY_LOOKBACK_DAYS = 380
BREAKOUT_HISTORY_CACHE_FILE = "breakout_history_cache.json"


# ---------- Opening 15-min candle bias (added, per request) ----------
# Looks at the FIRST 15-min candle of today's session (09:15 candle)
# for each symbol. If that candle's Open == Low (price never traded
# below the open through the whole first 15 minutes), it's flagged
# BULLISH. If Open == High (price never traded above the open), it's
# flagged BEARISH. Neither -> omitted (price moved both above and
# below open in the first 15 min, which is the normal/common case).
# Purely informational, shown as an extra line on every alert for that
# symbol for the rest of the day, regardless of which timeframe the
# alert itself fired on. See strategy.get_opening_candle_bias.
OPENING_CANDLE_BIAS_ENABLED = True
# Equality tolerance (in price points, not %) — real market data can
# have tiny floating-point noise even when two prices are "the same"
# tick, so exact == is avoided. 0.01 = 1 paisa, safely inside NSE's
# tick size (0.05) for virtually every stock/index.
OPENING_CANDLE_EPSILON = 0.01

# ---- Trendline Break scan (added, per request) ----
# Master on/off switch for the standalone Trendline Break Telegram
# alerts (send_trendline_alert). Set to False to stop these messages
# entirely without touching main.py / the scan logic — the scan still
# runs internally (cheap), it just won't send anything to Telegram.
# Flip back to True any time to re-enable.
ENABLE_TRENDLINE_ALERTS = False
# Diagonal trendline break — connects the last 2 confirmed swing highs
# (descending -> resistance line) or last 2 confirmed swing lows
# (ascending -> support line) and flags the candle where price closes
# through that line. See strategy.detect_trendline_break.
#
# How many candles back to look for swing points.
TRENDLINE_LOOKBACK_CANDLES = 50
# A candle counts as a confirmed swing high/low only if it's the
# highest/lowest point among this many candles on BOTH sides of it
# (a "fractal" pivot) — higher = fewer, more significant swings.
TRENDLINE_SWING_STRENGTH = 3
# Standalone Trendline Break alert dedup — same-symbol/direction
# re-alerts are suppressed within this cooldown, same idea as
# config.SAME_DIRECTION_COOLDOWN_MINUTES for the EMA cross alert.
TRENDLINE_COOLDOWN_MINUTES = 75

