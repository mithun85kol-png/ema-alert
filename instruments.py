"""
Resolves human-readable names (NIFTY 50, GOLD, RELIANCE...) into the
instrument_key strings Upstox's API expects, using the daily instrument
master file Upstox publishes.

Also resolves the Nifty 500 constituent list (fetched from NSE's official
archive CSV, cached to disk) against that same instrument master, and
exposes the current set of F&O-eligible underlying stocks.
"""

import csv
import gzip
import io
import json
import datetime as dt

import requests

import config

INSTRUMENT_MASTER_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"

# NSE's static file server occasionally rejects requests with no
# browser-like User-Agent header.
NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/csv,application/csv,*/*",
}

_cache = {"loaded_at": None, "data": None}


def _load_master():
    if _cache["data"] is not None:
        return _cache["data"]

    print("Downloading instrument master...", flush=True)
    resp = requests.get(INSTRUMENT_MASTER_URL, timeout=30)
    resp.raise_for_status()
    print(f"Downloaded {len(resp.content)} bytes, parsing...", flush=True)
    with gzip.GzipFile(fileobj=io.BytesIO(resp.content)) as f:
        data = json.load(f)
    print(f"Parsed {len(data)} instruments.", flush=True)

    _cache["data"] = data
    _cache["loaded_at"] = dt.datetime.utcnow()
    return data


def _normalize(s):
    """Strip spaces/hyphens/etc so 'CRUDEOIL' matches 'CRUDE OIL'."""
    return "".join(ch for ch in s.upper() if ch.isalnum())


def resolve_indices(index_names):
    print(f"Resolving {len(index_names)} indices...", flush=True)
    master = _load_master()
    out = {}
    for display, search in index_names.items():
        found = False
        for row in master:
            if row.get("segment") in ("NSE_INDEX", "BSE_INDEX") and row.get("name", "").upper() == search.upper():
                out[display] = row["instrument_key"]
                found = True
                break
        if not found:
            print(f"  NOT RESOLVED (index): display='{display}' searched_name='{search}'", flush=True)
    print(f"Resolved {len(out)} indices.", flush=True)
    return out


def resolve_mcx_nearest_futures(commodity_names):
    print(f"Resolving {len(commodity_names)} commodities...", flush=True)
    master = _load_master()
    today = dt.date.today()
    out = {}

    for display, search in commodity_names.items():
        search_norm = _normalize(search)
        candidates = []
        for row in master:
            if row.get("segment") != "MCX_FO":
                continue
            if row.get("instrument_type") != "FUT":
                continue
            name_norm = _normalize(row.get("name", ""))
            if search_norm not in name_norm:
                continue
            expiry_ms = row.get("expiry")
            if not expiry_ms:
                continue
            expiry_date = dt.datetime.utcfromtimestamp(expiry_ms / 1000).date()
            if expiry_date >= today:
                candidates.append((expiry_date, row["instrument_key"]))

        if candidates:
            candidates.sort(key=lambda x: x[0])
            out[display] = candidates[0][1]
        else:
            print(f"  NOT RESOLVED (commodity): display='{display}' searched_name='{search}'", flush=True)

    print(f"Resolved {len(out)} commodities.", flush=True)
    return out


def resolve_fo_stock_list(watchlist=None):
    print(f"Resolving F&O stock list (watchlist={'full' if watchlist is None else len(watchlist)})...", flush=True)
    master = _load_master()

    if watchlist is None:
        underlyings = _fno_underlyings_from_master(master)
        print(f"Found {len(underlyings)} F&O underlyings.", flush=True)
    else:
        underlyings = {s.upper() for s in watchlist}

    out = {}
    for row in master:
        if row.get("segment") == "NSE_EQ" and row.get("trading_symbol", "").upper() in underlyings:
            out[row["trading_symbol"].upper()] = row["instrument_key"]

    print(f"Resolved {len(out)} F&O stocks.", flush=True)
    return out


def _fno_underlyings_from_master(master):
    underlyings = set()
    for row in master:
        if row.get("segment") == "NSE_FO" and row.get("instrument_type") in ("CE", "PE", "FUT"):
            sym = row.get("underlying_symbol") or row.get("name")
            if sym:
                underlyings.add(sym.upper())
    return underlyings


def get_fno_underlyings():
    """
    Returns the current set of NSE trading symbols that have F&O
    (futures/options) contracts on Upstox, derived from the instrument
    master already loaded for this run (no extra network call). Used to
    flag Nifty 500 signals as "in F&O" vs "cash only" in the Telegram
    alert.
    """
    master = _load_master()
    return _fno_underlyings_from_master(master)


# ---------------------------------------------------------------------
# Nifty 500 constituent list (fetched from NSE, cached to disk)
# ---------------------------------------------------------------------

def _load_nifty500_symbols_cache():
    try:
        with open(config.NIFTY500_SYMBOLS_CACHE_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def _save_nifty500_symbols_cache(symbols):
    payload = {"cached_at": dt.datetime.utcnow().isoformat(), "symbols": symbols}
    with open(config.NIFTY500_SYMBOLS_CACHE_FILE, "w") as f:
        json.dump(payload, f, indent=2)


def _fetch_nifty500_symbols_from_nse():
    resp = requests.get(config.NIFTY500_CSV_URL, headers=NSE_HEADERS, timeout=20)
    resp.raise_for_status()
    text = resp.content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    symbols = []
    for row in reader:
        sym = (row.get("Symbol") or "").strip().upper()
        if sym:
            symbols.append(sym)
    return symbols


def get_nifty500_symbols():
    """
    Returns the list of ~500 NSE trading symbols in the Nifty 500 index.

    Fetched from NSE's official archive CSV and cached to disk for
    config.NIFTY500_CACHE_MAX_AGE_DAYS days, since the index is only
    reconstituted twice a year — no need to hit NSE's servers every
    3-minute run. If the live fetch fails (NSE occasionally blocks/
    throttles non-browser requests) this falls back to a stale cache
    rather than crashing the whole scan; if there's no cache at all,
    it returns an empty list and the Nifty 500 step is skipped for
    that run only (the existing F&O scan is unaffected).
    """
    cache = _load_nifty500_symbols_cache()
    if cache:
        cached_at = dt.datetime.fromisoformat(cache["cached_at"])
        age = dt.datetime.utcnow() - cached_at
        if age < dt.timedelta(days=config.NIFTY500_CACHE_MAX_AGE_DAYS):
            return cache["symbols"]

    try:
        symbols = _fetch_nifty500_symbols_from_nse()
        if len(symbols) < 400:
            # NSE occasionally serves an HTML error/interstitial page
            # with a 200 status instead of the CSV — guard against
            # quietly caching garbage.
            raise ValueError(f"only got {len(symbols)} symbols, expected ~500")
        _save_nifty500_symbols_cache(symbols)
        print(f"Fetched {len(symbols)} Nifty 500 symbols from NSE (fresh).", flush=True)
        return symbols
    except Exception as e:
        print(f"Nifty 500 CSV fetch failed ({e}).", flush=True)
        if cache:
            print(f"Falling back to stale cached list from {cache['cached_at']}.", flush=True)
            return cache["symbols"]
        print("No cached Nifty 500 list available — skipping Nifty 500 scan this run.", flush=True)
        return []


def resolve_nifty500_list():
    """
    Returns {trading_symbol: instrument_key} for every Nifty 500 stock
    that Upstox's instrument master also lists on NSE_EQ. Symbols that
    don't resolve (rare naming mismatches) are skipped and logged, same
    pattern as resolve_fo_stock_list.
    """
    symbols = get_nifty500_symbols()
    if not symbols:
        return {}

    print(f"Resolving {len(symbols)} Nifty 500 symbols against Upstox instrument master...", flush=True)
    master = _load_master()
    wanted = set(symbols)

    out = {}
    for row in master:
        if row.get("segment") == "NSE_EQ" and row.get("trading_symbol", "").upper() in wanted:
            out[row["trading_symbol"].upper()] = row["instrument_key"]

    missing = wanted - set(out.keys())
    if missing:
        preview = sorted(missing)[:20]
        suffix = "..." if len(missing) > 20 else ""
        print(f"  {len(missing)} Nifty 500 symbol(s) NOT RESOLVED: {preview}{suffix}", flush=True)

    print(f"Resolved {len(out)} Nifty 500 stocks.", flush=True)
    return out
