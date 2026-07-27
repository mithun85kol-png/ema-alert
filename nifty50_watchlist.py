"""
Resolves Nifty 50 constituent stocks to Upstox instrument_keys at runtime
using the Instrument Search API (avoids hardcoding ISINs, which can change).
Used only for the extra EMA 50/200 (75-min) alert - does not touch the
main config.WATCHLIST used by the other strategies.
"""
from typing import Optional
import requests
import config

log = config.get_logger(__name__)

# Nifty 50 trading symbols. This is the index composition and is reviewed
# periodically by NSE (typically twice a year) - update this list if the
# index constituents change.
NIFTY50_SYMBOLS = [
    "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
    "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV", "BEL", "BHARTIARTL",
    "CIPLA", "COALINDIA", "DRREDDY", "EICHERMOT", "ETERNAL",
    "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE", "HEROMOTOCO",
    "HINDALCO", "HINDUNILVR", "ICICIBANK", "ITC", "INDUSINDBK",
    "INFY", "JSWSTEEL", "JIOFIN", "KOTAKBANK", "LT",
    "M&M", "MARUTI", "NTPC", "NESTLEIND", "ONGC",
    "POWERGRID", "RELIANCE", "SBILIFE", "SHRIRAMFIN", "SBIN",
    "SUNPHARMA", "TCS", "TATACONSUM", "TATAMOTORS", "TATASTEEL",
    "TECHM", "TITAN", "TRENT", "ULTRACEMCO", "WIPRO",
]

_cache: dict = {}


def _search_instrument_key(symbol: str) -> Optional[str]:
    """
    Looks up the NSE_EQ instrument_key for a trading symbol using Upstox's
    instrument search endpoint. Caches results in-process so we don't hit
    the API 50 times on every single run.

    NOTE: this assumes config.UPSTOX_ANALYTICS_TOKEN is a valid bearer
    token for api.upstox.com. If your upstox_client.py wraps auth/headers
    differently, swap the request below to use that client instead.
    """
    if symbol in _cache:
        return _cache[symbol]
    try:
        resp = requests.get(
            f"{config.UPSTOX_BASE_URL}/instruments/search",
            params={"query": symbol, "exchanges": "NSE", "segment": "EQ"},
            headers={
                "Authorization": f"Bearer {config.UPSTOX_ANALYTICS_TOKEN}",
                "Accept": "application/json",
            },
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json().get("data", [])
        for item in data:
            if item.get("trading_symbol") == symbol and item.get("segment") == "NSE_EQ":
                key = item.get("instrument_key")
                _cache[symbol] = key
                return key
        log.warning("No exact NSE_EQ match found for %s", symbol)
    except requests.RequestException as e:
        log.error("Instrument search failed for %s: %s", symbol, e)
    return None


def build_nifty50_watchlist() -> list:
    """Returns a list of {"symbol": ..., "instrument_key": ...} dicts for
    every Nifty 50 stock that could be resolved."""
    watchlist = []
    for symbol in NIFTY50_SYMBOLS:
        key = _search_instrument_key(symbol)
        if key:
            watchlist.append({"symbol": symbol, "instrument_key": key})
        else:
            log.warning("Skipping %s - could not resolve instrument_key", symbol)
    log.info("Resolved %d/%d Nifty50 instrument keys", len(watchlist), len(NIFTY50_SYMBOLS))
    return watchlist
