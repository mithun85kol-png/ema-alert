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

EMA_MIN_GAP_PCT = 0.5  # minimum EMA9/EMA20 separation (% of price) to count as a valid crossover
VOLUME_CONFIRMATION_REQUIRED = True  # signal candle volume must exceed previous candle volume
TREND_EMA_PERIOD = 50  # longer EMA used as trend context; crossovers must align with this trend

BB_LENGTH = 20
BB_MULT = 1.2

# Each group is checked on its own timeframe(s)
INDEX_TIMEFRAME = 3
COMMODITY_TIMEFRAME = 3

# STOCK group gets checked on BOTH timeframes, each firing its own alert.
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
