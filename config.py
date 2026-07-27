"""
Central configuration for the EMA Alert Bot.
Loads secrets from .env and holds strategy parameters + watchlist.
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
RSI_BULLISH_MIN = 50
RSI_BULLISH_MAX = 70
RSI_BEARISH_MAX = 50
RSI_BEARISH_MIN = 30

BB_LENGTH = 20
BB_MULT = 1.2

TIMEFRAME_MINUTES = 5
BASE_CANDLE_INTERVAL = "1minute"
CANDLE_LOOKBACK_DAYS = 5

# Market hours (IST)
MARKET_OPEN = "09:15"
MARKET_CLOSE = "15:30"
TIMEZONE = "Asia/Kolkata"

STATE_FILE = "alert_state.json"

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# ---------------- Watchlist ----------------
# GOLD and SILVER are resolved dynamically at runtime (see commodities.py)
# and appended to this list automatically - no need to add them here.
WATCHLIST = [
    {"symbol": "NIFTY50", "instrument_key": "NSE_INDEX|Nifty 50"},
    {"symbol": "BANKNIFTY", "instrument_key": "NSE_INDEX|Nifty Bank"},
    {"symbol": "RELIANCE", "instrument_key": "NSE_EQ|INE002A01018"},
    {"symbol": "KOTAKBANK", "instrument_key": "NSE_EQ|INE237A01036"},
    {"symbol": "BDL", "instrument_key": "NSE_EQ|INE171Z01026"},
    {"symbol": "SENSEX", "instrument_key": "BSE_INDEX|SENSEX"},
]


def get_logger(name: str) -> logging.Logger:
    logging.basicConfig(
        level=LOG_LEVEL,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    return logging.getLogger(name)
