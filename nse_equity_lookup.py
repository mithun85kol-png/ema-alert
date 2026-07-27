"""
Resolves the STOCK watchlist (config.STOCK_SYMBOLS - all Nifty50
constituents + BDL) to Upstox instrument_keys at runtime using the
Instrument Search API (avoids hardcoding ISINs, which can change).

Used only by run_stocks.py (the 75-min EMA alert for the stock group) -
does not touch config.INDEX_WATCHLIST or commodities.py.
"""
from typing import Optional
import requests
import config

log = config.get_logger(__name__)

_cache: dict = {}


def _search_instrument_key(symbol: str) -> Optional[str]:
    """
    Looks up the NSE_EQ instrument_key for a trading symbol using Upstox's
    instrument search endpoint. Caches results in-process so we don't hit
    the API once per symbol on every single run.
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


def build_stock_watchlist(client=None) -> list:
    """
    Returns a list of {"symbol": ..., "instrument_key": ...} dicts for
    every symbol in config.STOCK_SYMBOLS that could be resolved.

    `client` (an UpstoxClient instance) is accepted for interface
    consistency with commodities.build_commodity_watchlist(client) and
    for future use, but instrument search currently goes straight to the
    Upstox REST API using config.UPSTOX_ANALYTICS_TOKEN.
    """
    watchlist = []
    for symbol in config.STOCK_SYMBOLS:
        key = _search_instrument_key(symbol)
        if key:
            watchlist.append({"symbol": symbol, "instrument_key": key})
        else:
            log.warning("Skipping %s - could not resolve instrument_key", symbol)
    log.info("Resolved %d/%d stock instrument keys", len(watchlist), len(config.STOCK_SYMBOLS))
    return watchlist
