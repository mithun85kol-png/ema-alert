"""
Dynamically resolves the full NSE F&O (stock derivatives) underlying
universe to Upstox instrument_keys, using Upstox's NSE instrument master
file (contains both NSE_EQ and NSE_FO segments in one file).

This replaces a hardcoded stock list: every run, the current set of
stocks with an active FUT contract is read directly off the exchange's
own instrument file, so additions/removals to the F&O list (NSE reviews
this roughly every 6 months) are picked up automatically with no manual
edits needed here.

Index futures (NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY, NIFTYNXT50,
SENSEX, BANKEX) use the same NSE_FO/FUT segment as stock futures, so
they are explicitly excluded here - those are already covered separately
by config.INDEX_WATCHLIST.
"""
import gzip
import io
import json
import requests
import config

log = config.get_logger(__name__)

NSE_INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"

INDEX_UNDERLYINGS = {
    "NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50",
    "SENSEX", "BANKEX",
}

_instrument_cache = None


def _load_nse_instruments() -> list:
    global _instrument_cache
    if _instrument_cache is not None:
        return _instrument_cache
    try:
        resp = requests.get(NSE_INSTRUMENTS_URL, timeout=60)
        resp.raise_for_status()
        with gzip.GzipFile(fileobj=io.BytesIO(resp.content)) as gz:
            data = json.loads(gz.read().decode("utf-8"))
        _instrument_cache = data
        log.info("Loaded NSE instrument master: %d instruments", len(data))
        return data
    except Exception as e:
        log.error("Failed to load NSE instrument master: %s", e)
        _instrument_cache = []
        return []


def build_fo_stock_watchlist() -> list:
    """
    Returns [{"symbol": ..., "instrument_key": ...}, ...] for every stock
    currently trading in the NSE F&O segment (index futures excluded).
    """
    instruments = _load_nse_instruments()
    if not instruments:
        return []

    fo_symbols = set()
    for inst in instruments:
        if inst.get("segment") != "NSE_FO" or inst.get("instrument_type") != "FUT":
            continue
        underlying = (inst.get("asset_symbol") or "").upper()
        if underlying and underlying not in INDEX_UNDERLYINGS:
            fo_symbols.add(underlying)

    eq_lookup = {}
    for inst in instruments:
        if inst.get("segment") == "NSE_EQ" and inst.get("instrument_type") == "EQ":
            ts = inst.get("trading_symbol")
            if ts:
                eq_lookup[ts.upper()] = inst.get("instrument_key")

    watchlist = []
    for symbol in sorted(fo_symbols):
        key = eq_lookup.get(symbol)
        if key:
            watchlist.append({"symbol": symbol, "instrument_key": key})
        else:
            log.warning("F&O stock %s: no matching NSE_EQ instrument_key found", symbol)

    log.info("Resolved %d/%d F&O stock instrument keys", len(watchlist), len(fo_symbols))
    return watchlist
