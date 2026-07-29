"""
Central configuration for the EMA Alert Bot.
Loads secrets from .env and holds strategy parameters + watchlist groups.
"""
import os
import logging
from dotenv import load_dotenv

load_dotenv()

# ---------------- Upstox (Analytics Token) ----------------
UPSTOX_ANALYTICS_TOKEN = os.getenv("UPSTOX_ANALYTICS_TOKEN", "")
UPSTOX_BASE_URL = "https://api.upstox.com/v2"

# ---------------- Telegram ----------------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# ---------------- Strategy parameters ----------------
EMA_FAST = 9
EMA_SLOW = 20
RSI_PERIOD = 14

# NOTE: user wants stoploss placed at EMA20, and wants the alert to fire
# right on the crossing candle's close (not a few candles later) so the
# entry price stays close to EMA20 - keeping the stoploss distance small.
# Since the gap is measured on the cross candle itself, where EMA9/EMA20
# are ~equal by definition, this threshold stays near-zero (it only
# screens out literal ties/float noise). Real strength confirmation for
# this setup comes from VOLUME_CONFIRMATION_MULT and REQUIRE_RSI_MOMENTUM
# below instead, since both are measurable on the crossing candle itself
# - no need to wait for future candles, so the tight stoploss is preserved.
EMA_MIN_GAP_PCT = 0.01  # minimum EMA9/EMA20 separation (% of price), checked on the cross candle itself

TREND_EMA_PERIOD = 50  # longer EMA used as trend context (informational only, doesn't block signals)

# How many recent candles to scan for a crossover, instead of only
# comparing the last two candles. Protects against missed crosses when a
# scheduled run is delayed or skipped (e.g. GitHub Actions cron jitter).
CROSS_LOOKBACK_BARS = 3

# NEW - strength confirmation on the crossing candle itself (replaces the
# whipsaw filter, which needed a future candle and would have delayed the
# alert past the crossing candle):
# Crossing candle's volume must be at least this multiple of the previous
# candle's volume. 1.0 = no requirement (just needs to be >= prev volume).
# Raise toward 1.3-1.5 for stricter "real breakout" volume confirmation.
VOLUME_CONFIRMATION_MULT = 1.15

# If True, RSI must be rising for a BULLISH cross and falling for a
# BEARISH cross, measured on the crossing candle vs the previous one.
# Rejects crosses where momentum is already fading right as the EMAs
# cross.
REQUIRE_RSI_MOMENTUM = True

BB_LENGTH = 20
BB_MULT = 1.2

# Each group is checked on its own timeframe
INDEX_TIMEFRAME = 3
COMMODITY_TIMEFRAME = 5
STOCK_TIMEFRAMES = [5, 75]

BASE_CANDLE_INTERVAL = "1minute"
CANDLE_LOOKBACK_DAYS = 10  # enough warm-up data even for the 75-min stock timeframe

# Market hours (IST)
MARKET_OPEN = "09:15"
MARKET_CLOSE = "15:30"
COMMODITY_MARKET_CLOSE = "23:30"
TIMEZONE = "Asia/Kolkata"

STATE_FILE = "alert_state.json"

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# ---------------- Watchlists ----------------
INDEX_WATCHLIST = [
    {"symbol": "NIFTY50", "instrument_key": "NSE_INDEX|Nifty 50"},
    {"symbol": "BANKNIFTY", "instrument_key": "NSE_INDEX|Nifty Bank"},
    {"symbol": "SENSEX", "instrument_key": "BSE_INDEX|SENSEX"},
]

# MCX front-month contracts are resolved automatically every run
COMMODITY_SYMBOLS = ["GOLD", "SILVER", "CRUDEOIL"]

# All Nifty50 constituents (as of the last rebalance) + BDL. Instrument
# keys are resolved automatically by symbol every run - no ISIN needed.
# If a symbol here is outdated (index gets rebalanced twice a year, in
# Jan/Jul) it just gets skipped with a log warning, nothing breaks.
STOCK_SYMBOLS = [
    "RELIANCE", "BHARTIARTL", "HDFCBANK", "ICICIBANK", "SBIN", "TCS",
    "BAJFINANCE", "LT", "HINDUNILVR", "SUNPHARMA", "MARUTI", "INFY",
    "TITAN", "ADANIENT", "ADANIPORTS", "M&M", "KOTAKBANK", "AXISBANK",
    "ITC", "ULTRACEMCO", "HCLTECH", "NTPC", "ONGC", "BAJAJ-AUTO",
    "JSWSTEEL", "BAJAJFINSV", "BEL", "ETERNAL", "POWERGRID", "COALINDIA",
    "ASIANPAINT", "SHRIRAMFIN", "TATASTEEL", "HINDALCO", "GRASIM",
    "EICHERMOT", "INDIGO", "SBILIFE", "WIPRO", "JIOFIN", "TRENT",
    "TECHM", "APOLLOHOSP", "HDFCLIFE", "TATAMOTORS", "CIPLA",
    "TATACONSUM", "MAXHEALTH", "DRREDDY",
    "BDL",  # kept from earlier, not part of Nifty50 itself
]


def get_logger(name: str) -> logging.Logger:
    logging.basicConfig(
        level=LOG_LEVEL,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    return logging.getLogger(name)
