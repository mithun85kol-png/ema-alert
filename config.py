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

# ---------- Fetching ----------
FETCH_WORKERS = 15            # concurrent fetch threads; tune down if Upstox rate-limits (429s)

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

# ---------- State / alert de-dup ----------
STATE_FILE = "alert_state.json"
DEDUPE_MINUTES = 30

# ==========================================================================
# ---------- Nifty 500 (cash/equity) scan — Step 2, runs alongside ----------
# ==========================================================================
#
# This is a second, independent scan of all ~500 Nifty 500 stocks (cash/
# equity, not F&O), using the exact same EMA9/20 + RSI + volume + trend +
# strong-candle strategy as the existing F&O scan. It runs in the SAME
# 3-minute workflow run as the existing scan (no separate cron job).

# Official NSE archive CSV listing all current Nifty 500 constituents.
# NSE reconstitutes this list only twice a year (Jan 31 / Jul 31 cutoff),
# so we don't need to re-download it every 3-minute run — see
# NIFTY500_CACHE_MAX_AGE_DAYS below.
NIFTY500_CSV_URL = "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv"
NIFTY500_SYMBOLS_CACHE_FILE = "nifty500_symbols_cache.json"
NIFTY500_CACHE_MAX_AGE_DAYS = 7

# Separate state file so Nifty 500 alert de-dup never touches/overwrites
# the existing F&O scan's alert_state.json.
NIFTY500_STATE_FILE = "alert_state_nifty500.json"

# Fetching ~500 instruments' 1-min candles in one go risks Upstox
# rate-limiting (429s). Instead this scan fetches in smaller sequential
# batches, with a short pause between batches, and a smaller thread pool
# per batch than the main F&O scan uses.
NIFTY500_BATCH_SIZE = 40           # instruments per batch
NIFTY500_FETCH_WORKERS = 8         # concurrent threads *within* a batch
NIFTY500_BATCH_DELAY_SECONDS = 2   # pause between batches

# R3/S3 Camarilla pivots are skipped for the Nifty 500 scan by default —
# computing them requires one extra daily-candle API call per stock, which
# means ~500 extra calls before the scan even starts. Flip to True if you
# want pivot proximity notes on Nifty 500 alerts too (first run of the day
# will be slower while the pivot cache warms up).
NIFTY500_INCLUDE_PIVOTS = False
