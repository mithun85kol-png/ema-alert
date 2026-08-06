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
STRONG_CANDLE_BODY_RATIO = 0.30  # unused by current strategy.py (strong-candle filter removed — plain cross + mandatory volume now); kept for reference

# Minimum distance between EMA9 and EMA20 at the moment of crossover,
# as a % of close price. unused by current strategy.py (gap filter
# removed — any qualifying cross is eligible regardless of gap size);
# kept for reference.
MIN_EMA_CROSS_GAP_PCT = 0.05

# How many of the most recent CLOSED candles to re-check for a cross on
# every run (not just the single latest one). This is what lets the bot
# "catch up" if a scheduled run is skipped or delayed — any cross that
# happened on an in-between candle still gets alerted on the next run,
# instead of silently disappearing. Applies to 3-min candles (the
# primary/alerting timeframe) — 5 candles x 3-min = 15 minutes, so a
# missed run still catches a cross from up to ~15 minutes earlier. Each
# candle already alerted is never re-sent (see state.py).
CROSS_LOOKBACK_CANDLES = 5

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

# ---------- 75-min timeframe warmup (informational context only) ----------
# 3-min is the PRIMARY/alerting timeframe again — 75-min is only used
# for strategy.get_75min_trend_info(), the informational context block
# attached to every 3-min alert (EMA9/20 bias + how close to a cross on
# the bigger timeframe). That helper only needs EMA9/EMA20 warmed up,
# i.e. EMA_SLOW + 2 = 22 bars minimum on the 75-min timeframe — roughly
# 4-5 TRADING days (~7-8 CALENDAR days with weekends). 25 calendar days
# is more than enough margin. This many calendar days of PRE-TODAY
# 1-minute history are fetched once per day (cached) and combined with
# today's intraday data before resampling to 75-min. (NOTE: verify
# Upstox's historical-candle endpoint allows a 25-day 1-minute range in
# one request against current docs — if it caps out lower, this fetch
# will need to be split into chunks.)
HISTORICAL_1MIN_LOOKBACK_DAYS = 25
HIST_1MIN_CACHE_FILE = "historical_1min_cache.json"

# How many trailing 75-min candles get_75min_trend_info() scans to
# report "a cross happened N candles ago" (None if no cross in this
# window). Used again now that 3-min is primary and every 3-min alert
# gets a 75-min context block — see strategy.get_75min_trend_info().
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
