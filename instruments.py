"""
Resolves human-readable names (NIFTY 50, GOLD, RELIANCE...) into the
instrument_key strings Upstox's API expects, using the daily instrument
master file Upstox publishes.

Also exposes the current set of F&O-eligible underlying stocks.
"""

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
        search_upper = search.upper()
        for row in master:
            if row.get("segment") not in ("NSE_INDEX", "BSE_INDEX"):
                continue
            # Some BSE_INDEX entries (e.g. SENSEX) don't have a 'name'
            # field that matches the display name exactly — fall back to
            # trading_symbol so those still resolve.
            name_match = row.get("name", "").upper() == search_upper
            symbol_match = row.get("trading_symbol", "").upper() == search_upper
            if name_match or symbol_match:
                out[display] = row["instrument_key"]
                found = True
                break
        if not found:
            print(f"  NOT RESOLVED (index): display='{display}' searched_name='{search}'", flush=True)
            if search_upper == "SENSEX":
                # Last-resort fallback: Upstox documents SENSEX's
                # instrument_key as the fixed string "BSE_INDEX|SENSEX"
                # (not derived from the master file's 'name'/
                # 'trading_symbol' fields), so use it directly rather
                # than silently dropping the index from the scan.
                out[display] = "BSE_INDEX|SENSEX"
                print(f"  Using known fallback instrument_key for '{display}': BSE_INDEX|SENSEX", flush=True)
                found = True
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
