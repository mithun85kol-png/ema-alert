"""
Resolves the current front-month MCX MINI futures contract for a
commodity (GOLD MINI, SILVER MINI, CRUDE OIL MINI) using Upstox's
Instrument Search API. This runs every time the bot executes, so the
contract automatically rolls over to the next month's contract when the
current one expires - no manual updates needed.

IMPORTANT - how MINI is actually identified:
MCX's instrument "name" field does NOT distinguish MINI from other
variants (regular, guinea, petal, ten-gram, micro, 100kg, etc) - it's
just the base commodity name ("GOLD", "SILVER", "CRUDE OIL") for all of
them. The variant is encoded in trading_symbol instead, as the FIRST
TOKEN (e.g. "GOLDM FUT 05 NOV 26" -> first token "GOLDM").

We match on the EXACT first token, not a prefix/substring, because some
variant names are prefix-extensions of the MINI ticker - e.g. SILVER's
MINI contract's first token is "SILVERM", but the MICRO contract's first
token is "SILVERMIC". A naive `.startswith("SILVERM")` check would wrongly
match both. Splitting on whitespace and comparing the first token exactly
avoids this.
"""
import config

log = config.get_logger(__name__)

COMMODITY_SYMBOLS = ["GOLD", "SILVER", "CRUDEOIL"]

# Base "name" field to search for in the instrument master (no "MINI"
# suffix - see module docstring for why).
SYMBOL_TO_BASE_NAME = {
    "GOLD": "GOLD",
    "SILVER": "SILVER",
    "CRUDEOIL": "CRUDE OIL",
}

# Expected first token of trading_symbol for the MINI variant of each
# commodity. If Upstox ever changes this convention, update here.
SYMBOL_TO_MINI_PREFIX = {
    "GOLD": "GOLDM",
    "SILVER": "SILVERM",
    "CRUDEOIL": "CRUDEOILM",
}


def _first_token(trading_symbol: str) -> str:
    return (trading_symbol or "").strip().split()[0].upper() if trading_symbol else ""


def resolve_front_month(client, underlying_symbol: str):
    base_name = SYMBOL_TO_BASE_NAME.get(underlying_symbol, underlying_symbol)
    mini_prefix = SYMBOL_TO_MINI_PREFIX.get(underlying_symbol)

    try:
        all_variants = client.search_instruments(base_name)
    except Exception as e:
        log.error("Instrument search failed for %s: %s", underlying_symbol, e)
        return None

    if not all_variants:
        log.warning(
            "No FUT contracts at all found under name=%r for %s - instrument "
            "master may be unavailable or the base name has changed.",
            base_name, underlying_symbol,
        )
        return None

    mini_candidates = [
        inst for inst in all_variants
        if _first_token(inst.get("trading_symbol")) == mini_prefix
    ]

    if not mini_candidates:
        # Help future debugging: show what variants DID exist so a naming
        # convention change is easy to spot from the logs.
        seen_prefixes = sorted(set(_first_token(inst.get("trading_symbol")) for inst in all_variants))
        log.warning(
            "No MCX MINI futures contract found for %s (expected trading_symbol "
            "starting with %r) - variants actually seen: %s",
            underlying_symbol, mini_prefix, seen_prefixes,
        )
        return None

    mini_candidates.sort(key=lambda r: r.get("expiry") or 0)
    chosen = mini_candidates[0]
    log.info("%s (MINI) front-month contract: %s (expiry %s, lot_size %s)",
              underlying_symbol, chosen.get("trading_symbol"), chosen.get("expiry"), chosen.get("lot_size"))
    return chosen.get("instrument_key")


def build_commodity_watchlist(client) -> list:
    items = []
    for symbol in COMMODITY_SYMBOLS:
        key = resolve_front_month(client, symbol)
        if key:
            items.append({"symbol": symbol, "instrument_key": key})
    return items
