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

INSTRUMENT_MASTER_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"

_cache = {"loaded_at": None, "data": None}


def _load_master():
    if _cache["data"] is not None:
        return _cache["data"]

    resp = requests.get(INSTRUMENT_MASTER_URL, timeout=30)
    resp.raise_for_status()
    with gzip.GzipFile(fileobj=io.BytesIO(resp.content)) as f:
        data = json.load(f)

    _cache["data"] = data
    _cache["loaded_at"] = dt.datetime.utcnow()
    return data


def resolve_indices(index_names):
    master = _load_master()
    out = {}
    for display, search in index_names.items():
        for row in master:
            if row.get("segment") in ("NSE_INDEX", "BSE_INDEX") and row.get("name", "").upper() == search.upper():
                out[display] = row["instrument_key"]
                break
    return out


def resolve_mcx_nearest_futures(commodity_names):
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

    return out


def resolve_fo_stock_list(watchlist=None):
    master = _load_master()

    if watchlist is None:
        underlyings = set()
        for row in master:
            if row.get("segment") == "NSE_FO" and row.get("instrument_type") in ("CE", "PE", "FUT"):
                sym = row.get("underlying_symbol") or row.get("name")
                if sym:
                    underlyings.add(sym.upper())
    else:
        underlyings = {s.upper() for s in watchlist}

    out = {}
    for row in master:
        if row.get("segment") == "NSE_EQ" and row.get("trading_symbol", "").upper() in underlyings:
            out[row["trading_symbol"].upper()] = row["instrument_key"]

    return out
