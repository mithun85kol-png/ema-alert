"""
Resolves the current front-month MCX futures contract for a commodity
(e.g. GOLD, SILVER) using Upstox's Instrument Search API. This runs every
time the bot executes, so the contract automatically rolls over to the
next month's contract when the current one expires - no manual updates
needed.
"""
import config

log = config.get_logger(__name__)

COMMODITY_SYMBOLS = ["GOLD", "SILVER"]


def resolve_front_month(client, underlying_symbol: str):
    try:
        results = client.search_instruments(underlying_symbol)
    except Exception as e:
        log.error("Instrument search failed for %s: %s", underlying_symbol, e)
        return None

    candidates = [
        r for r in results
        if r.get("underlying_symbol") == underlying_symbol and r.get("instrument_type") == "FUT"
    ]
    if not candidates:
        log.warning("No MCX futures contract found for %s", underlying_symbol)
        return None

    candidates.sort(key=lambda r: r.get("expiry", ""))
    chosen = candidates[0]
    log.info("%s front-month contract: %s (expiry %s)", underlying_symbol,
              chosen.get("trading_symbol"), chosen.get("expiry"))
    return chosen.get("instrument_key")


def build_commodity_watchlist(client) -> list:
    items = []
    for symbol in COMMODITY_SYMBOLS:
        key = resolve_front_month(client, symbol)
        if key:
            items.append({"symbol": symbol, "instrument_key": key})
    return items
