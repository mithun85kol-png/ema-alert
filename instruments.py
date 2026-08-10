"""
Resolves human-readable names (NIFTY 50, GOLD, RELIANCE...) into the
instrument_key strings Upstox's API expects, using the daily instrument
master file Upstox publishes.

Also exposes the current set of F&O-eligible underlying stocks.
"""

import csv
import gzip
import io
import json
import os
import datetime as dt

import requests

import config

NIFTY500_LIST_URL = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"

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


def _normalize_index_name(s):
    """
    Like _normalize, but also treats '&' and the word 'AND' as
    equivalent (and drops both), so 'Nifty Oil & Gas' and
    'NIFTY OIL AND GAS' compare equal regardless of which spelling
    Upstox's master file or our own config.py happens to use.
    """
    s = s.upper().replace("&", " AND ")
    words = [w for w in s.split() if w != "AND"]
    return "".join(ch for ch in "".join(words) if ch.isalnum())


def resolve_indices(index_names):
    print(f"Resolving {len(index_names)} indices...", flush=True)
    master = _load_master()
    index_rows = [r for r in master if r.get("segment") in ("NSE_INDEX", "BSE_INDEX")]
    out = {}

    for display, search in index_names.items():
        found = False
        # Try both the configured display name and search name against
        # both 'name'/'trading_symbol'/'short_name' fields — whichever
        # side happens to match Upstox's actual spelling wins.
        candidates_to_try = [search, display]

        # Pass 1: exact (case-insensitive) match on name / trading_symbol / short_name
        for term in candidates_to_try:
            term_upper = term.upper()
            for row in index_rows:
                fields = (row.get("name", ""), row.get("trading_symbol", ""), row.get("short_name", ""))
                if term_upper in {f.upper() for f in fields if f}:
                    out[display] = row["instrument_key"]
                    found = True
                    break
            if found:
                break

        # Pass 2: normalized match, treating '&' and 'AND' as equivalent
        if not found:
            for term in candidates_to_try:
                term_norm = _normalize_index_name(term)
                for row in index_rows:
                    fields = (row.get("name", ""), row.get("trading_symbol", ""), row.get("short_name", ""))
                    if term_norm and term_norm in {_normalize_index_name(f) for f in fields if f}:
                        out[display] = row["instrument_key"]
                        found = True
                        break
                if found:
                    break

        # Pass 3: normalized substring containment (last resort before
        # giving up), e.g. search misses a word the master name has.
        if not found:
            for term in candidates_to_try:
                term_norm = _normalize_index_name(term)
                if not term_norm:
                    continue
                for row in index_rows:
                    name_norm = _normalize_index_name(row.get("name", ""))
                    if name_norm and (term_norm in name_norm or name_norm in term_norm):
                        out[display] = row["instrument_key"]
                        found = True
                        break
                if found:
                    break

        if not found:
            print(f"  NOT RESOLVED (index): display='{display}' searched_name='{search}'", flush=True)
            if search.upper() == "SENSEX":
                # Last-resort fallback: Upstox documents SENSEX's
                # instrument_key as the fixed string "BSE_INDEX|SENSEX"
                # (not derived from the master file's 'name'/
                # 'trading_symbol' fields), so use it directly rather
                # than silently dropping the index from the scan.
                out[display] = "BSE_INDEX|SENSEX"
                print(f"  Using known fallback instrument_key for '{display}': BSE_INDEX|SENSEX", flush=True)
                found = True
            else:
                # Diagnostic: print near-matching index names actually
                # present in the master file, so the correct search
                # string/trading_symbol can be copied straight into
                # config.py. Try whole-word overlap first; Upstox
                # sometimes abbreviates words (e.g. "CONSR" for
                # "CONSUMER", "DURBL" for "DURABLES"), so fall back to
                # a 4-character word-prefix match, which still catches
                # those cases.
                search_words = {w for w in search.upper().replace("&", " ").split() if w != "AND"}
                near = []
                for row in index_rows:
                    name = row.get("name", "")
                    if not name:
                        continue
                    name_words = {w for w in name.upper().replace("&", " ").split() if w != "AND"}
                    if search_words & name_words:
                        near.append((name, row.get("trading_symbol", "")))
                if not near:
                    search_prefixes = {w[:4] for w in search_words if len(w) >= 4}
                    for row in index_rows:
                        name = row.get("name", "")
                        symbol = row.get("trading_symbol", "")
                        combined_words = {w[:4] for w in f"{name} {symbol}".upper().replace("&", " ").split() if len(w) >= 4}
                        if search_prefixes & combined_words:
                            near.append((name, symbol))
                if near:
                    print(f"    Possible matches in master (name, trading_symbol): {near[:5]}", flush=True)

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


def _load_nifty500_cache():
    try:
        with open(config.NIFTY500_LIST_CACHE_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"date": None, "symbols": []}


def _save_nifty500_cache(cache):
    with open(config.NIFTY500_LIST_CACHE_FILE, "w") as f:
        json.dump(cache, f)


def fetch_nifty500_symbols():
    """
    Fetches the current Nifty 500 constituent list (trading symbols)
    from NSE's public archives, so the watchlist stays correct across
    NSE's periodic index rebalances instead of relying on a hardcoded
    list here. Cached to disk (config.NIFTY500_LIST_CACHE_FILE), so this
    is only actually downloaded once per calendar day, not once per run
    — same pattern as delivery_data.py's bhavcopy cache.

    Falls back to yesterday's cached list if today's fetch fails (NSE's
    archive server occasionally blocks/errors) rather than returning
    nothing, since the constituent list changes rarely. Returns []
    (never raises) only if there's no fetch AND no prior cache at all —
    callers should treat that as "Nifty 500 scan unavailable this run".
    """
    cache = _load_nifty500_cache()
    today_str = dt.date.today().isoformat()

    if cache.get("date") == today_str and cache.get("symbols"):
        return cache["symbols"]

    print("Fetching Nifty 500 constituent list from NSE...", flush=True)
    try:
        resp = requests.get(NIFTY500_LIST_URL, headers=NSE_HEADERS, timeout=20)
        resp.raise_for_status()
        reader = csv.DictReader(io.StringIO(resp.text))
        symbols = []
        for row in reader:
            sym = (row.get("Symbol") or row.get("SYMBOL") or "").strip().upper()
            if sym:
                symbols.append(sym)
        if symbols:
            print(f"Fetched {len(symbols)} Nifty 500 symbols.", flush=True)
            _save_nifty500_cache({"date": today_str, "symbols": symbols})
            return symbols
        print("Nifty 500 list fetch returned no symbols.", flush=True)
    except Exception as e:
        print(f"Nifty 500 list fetch failed: {e}", flush=True)

    if cache.get("symbols"):
        print(f"Using stale cached Nifty 500 list from {cache.get('date')} ({len(cache['symbols'])} symbols).", flush=True)
        return cache["symbols"]

    return []


def resolve_nifty500_stocks():
    """
    Resolves the live Nifty 500 constituent list (see
    fetch_nifty500_symbols) to {trading_symbol: instrument_key} via the
    Upstox NSE_EQ instrument master — same resolution mechanism
    resolve_fo_stock_list uses for an explicit watchlist. A constituent
    not found in the master (e.g. a very recent listing Upstox hasn't
    indexed yet) is simply omitted, never an error.
    """
    symbols = fetch_nifty500_symbols()
    if not symbols:
        print("Nifty 500 list unavailable — skipping Nifty 500 cash scan this run.", flush=True)
        return {}
    return resolve_fo_stock_list(symbols)


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
