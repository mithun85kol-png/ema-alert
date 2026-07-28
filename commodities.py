"""
Resolves the current front-month MCX MINI futures contract for a
commodity (GOLD MINI, SILVER MINI, CRUDE OIL MINI) using Upstox's
Instrument Search API. This runs every time the bot executes, so the
contract automatically rolls over to the next month's contract when the
current one expires - no manual updates needed.
"""
import config

log = config.get_logger(__name__)

# Internal display symbol used throughout the bot (state tracking, alert
# messages) -> exact "name" Upstox uses in its instrument master for the
# MINI contract of each commodity.
COMMODITY_SYMBOLS = ["GOLD", "SILVER", "CRUDEOIL"]

SYMBOL_TO_UPSTOX_NAME = {
    "GOLD": "GOLD MINI",
    "SILVER": "SILVER MINI",
    "CRUDEOIL": "CRUDE OIL MINI",
}


def resolve_front_month(client, underlying_symbol: str):
    upstox_name = SYMBOL_TO_UPSTOX_NAME.get(underlying_symbol, underlying_symbol)
    try:
        results = client.search_instruments(upstox_name)
    except Exception as e:
        log.error("Instrument search failed for %s: %s", underlying_symbol, e)
        return None

    # search_instruments() already filters the instrument master down to
    # exact name + FUT matches, so results here are already the correct
    # candidates - no extra filtering needed.
    candidates = results

    if not candidates:
        log.warning(
            "No MCX MINI futures contract found for %s (searched name=%r) - "
            "check log output of raw search results if this persists",
            underlying_symbol, upstox_name,
        )
        return None

    candidates.sort(key=lambda r: r.get("expiry", ""))
    chosen = candidates[0]
    log.info("%s (MINI) front-month contract: %s (expiry %s)", underlying_symbol,
              chosen.get("trading_symbol"), chosen.get("expiry"))
    return chosen.get("instrument_key")


def build_commodity_watchlist(client) -> list:
    items = []
    for symbol in COMMODITY_SYMBOLS:
        key = resolve_front_month(client, symbol)
        if key:
            items.append({"symbol": symbol, "instrument_key": key})
    return items
