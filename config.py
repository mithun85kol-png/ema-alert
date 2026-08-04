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
RSI_PERIOD = 14
RSI_BULLISH_MIN = 55          # unused by current strategy.py (informational-only design); kept for reference
RSI_BEARISH_MAX = 45          # unused by current strategy.py (informational-only design); kept for reference
VOLUME_AVG_PERIOD = 20
VOLUME_MULTIPLIER = 1.3       # unused by current strategy.py (informational-only design); kept for reference
STRONG_CANDLE_BODY_RATIO = 0.30  # candle body must be >= 30% of the candle's high-low range

# Minimum distance between EMA9 and EMA20 at the moment of crossover,
# as a % of close price. Filters out "whipsaw" crosses where the two
# lines are essentially touching (e.g. 0.005% apart) — those aren't
# visible as a real cross on the chart and are just noise.
MIN_EMA_CROSS_GAP_PCT = 0.05

# How many of the most recent CLOSED candles to re-check for a cross on
# every run (not just the single latest one). This is what lets the bot
# "catch up" if a scheduled run is skipped or delayed — any cross that
# happened on an in-between candle still gets alerted on the next run,
# instead of silently disappearing. 5 candles x 3-min = 15 minutes of
# buffer. Raise this if your workflow sometimes has longer gaps between
# runs; each candle already alerted is never re-sent (see state.py).
CROSS_LOOKBACK_CANDLES = 5

# Informational trade-plan fields added to every alert: Stop Loss = EMA20
# (on the cross candle), Target = highest high (bullish) / lowest low
# (bearish) over the trailing window below. 5 candles x 3-min = 15
# minutes, matching "entry on cross, target = last 15-min high/low".
TARGET_LOOKBACK_CANDLES = 5

# ---------- MACD (added) — informational only, never blocks a signal ----------
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

# How many trailing candles to scan for a MACD bullish/bearish
# divergence (the window is split into two halves; price extremes and
# MACD-line values in each half are compared — see
# strategy._detect_macd_divergence). 20 candles x 3-min = 60 minutes.
MACD_DIVERGENCE_LOOKBACK_CANDLES = 20

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

# ---------- F&O stocks ----------
USE_FULL_FO_LIST = True

FO_STOCK_WATCHLIST = [
    "RELIANCE", "HDFCBANK", "ICICIBANK", "INFY", "TCS",
    "SBIN", "AXISBANK", "KOTAKBANK", "TATAMOTORS", "BAJFINANCE",
]

# ---------- 75-min timeframe warmup ----------
# A single trading day only produces ~5 bars on the 75-min timeframe
# (375-min session / 75min), nowhere near the 22 needed to warm up
# EMA9/EMA20. This many calendar days of PRE-TODAY 1-minute history are
# fetched once per day (cached) and combined with today's intraday data
# before resampling to 75-min, so EMA9/EMA20 on that timeframe actually
# has enough bars to be meaningful.
HISTORICAL_1MIN_LOOKBACK_DAYS = 10
HIST_1MIN_CACHE_FILE = "historical_1min_cache.json"

# How many trailing 75-min candles get_75min_trend_info() checks for a
# recent EMA9/20 cross (informational block on every 3-min alert). 5
# candles ≈ one trading day. If no cross falls within this window, the
# info block still shows the current bias, just without a "crossed N
# candle(s) ago" note.
TREND_75MIN_LOOKBACK_CANDLES = 5

# ---------- State / alert de-dup ----------
STATE_FILE = "alert_state.json"
DEDUPE_MINUTES = 30

# ---------- Fetch-failure visibility ----------
# After a run, if the number of instruments that failed to fetch (even
# after retries) is >= this many, a single short Telegram warning is
# sent summarizing the count -- so a persistent rate-limit or outage
# shows up as a visible signal instead of silently vanishing into the
# GitHub Actions log. Set to a high number (e.g. 9999) to disable.
FAILURE_ALERT_MIN_COUNT = 5
