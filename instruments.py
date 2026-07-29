"""
Resolves human-readable names (NIFTY 50, GOLD, RELIANCE...) into the
instrument_key strings Upstox's API expects, using the daily instrument
master file Upstox publishes.
"""

import gzip
import json
import io
import datetime as dt
import requests

import config

INSTRUMENT_MASTER_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"

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


def resolve_indices(index_names):
    print(f"Resolving {len(index_names)} indices...", flush=True)
    master = _load_master()
    out = {}
    for display, search in index_names.items():
        for row in master:
            if row.get("segment") in ("NSE_INDEX", "BSE_INDEX") and row.get("name", "").upper() == search.upper():
                out[display] = row["instrument_key"]
                break
    print(f"Resolved {len(out)} indices.", flush=True)
    return out


def resolve_mcx_nearest_futures(commodity_names):
    print(f"Resolving {len(commodity_names)} commodities...", flush=True)
    master = _load_master()
    today = dt.date.today()
    out = {}

    for display, search in commodity_names.items():
        candidates = []
        for row in master:
            if row.get("segment") != "MCX_FO":
                continue
            if row.get("instrument_type") != "FUT":
                continue
            name = row.get("name", "").upper()
            if search.upper() not in name:
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

    print(f"Resolved {len(out)} commodities.", flush=True)
    return out


def resolve_fo_stock_list(watchlist=None):
    print(f"Resolving F&O stock list (watchlist={'full' if watchlist is None else len(watchlist)})...", flush=True)
    master = _load_master()

    if watchlist is None:
        underlyings = set()
        for row in master:
            if row.get("segment") == "NSE_FO" and row.get("instrument_type") in ("CE", "PE", "FUT"):
                sym = row.get("underlying_symbol") or row.get("name")
                if sym:
                    underlyings.add(sym.upper())
        print(f"Found {len(underlyings)} F&O underlyings.", flush=True)
    else:
        underlyings = {s.upper() for s in watchlist}

    out = {}
    for row in master:
        if row.get("segment") == "NSE_EQ" and row.get("trading_symbol", "").upper() in underlyings:
            out[row["trading_symbol"].upper()] = row["instrument_key"]

    print(f"Resolved {len(out)} F&O stocks.", flush=True)
    return out


def get_sector_trend(symbol, sector_trend_cache):
    """
    Looks up which sector a stock belongs to (config.STOCK_SECTOR_MAP) and
    returns (sector_name, sector_trend) using a cache of already-computed
    sector trends (built once per run in main.py's compute_sector_trends()).

    Returns ("UNKNOWN", "UNKNOWN") if the symbol has no sector mapping
    (e.g. it's an index/commodity, or a stock not yet added to the map).
    Informational only — never blocks an alert.
    """
    sector_name = config.STOCK_SECTOR_MAP.get(symbol.upper())
    if sector_name is None:
        return "UNKNOWN", "UNKNOWN"
    return sector_name, sector_trend_cache.get(sector_name, "UNKNOWN")
