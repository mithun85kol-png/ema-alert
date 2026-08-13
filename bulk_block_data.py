"""
Fetches the SINGLE MOST RECENT Bulk or Block deal for a symbol —
NSE's disclosure of a single-client, single-stock, single-day trade
big enough to cross NSE's threshold (bulk: >=0.5% of the company's
total listed shares in a day; block: a separate large-trade window,
minimum 5,00,000 shares or Rs 5 crore).

REWRITTEN 2026-08-13: the previous version called NSE's MAIN SITE API
(www.nseindia.com/api/historical/...), which sits behind NSE's
bot-protection and reliably gets blocked from GitHub Actions runners
(cloud/datacenter IPs) even with a browser-like User-Agent and a
same-site cookie warm-up — so this feature never actually produced a
"Last Bulk/Block Deal" line on any alert.

This version instead uses the same static-file archive host
(archives.nseindia.com) that delivery_data.py already uses
successfully from GitHub Actions for the bhavcopy — no bot-protection,
no session/cookie warm-up needed:
  https://archives.nseindia.com/content/equities/bulk.csv
  https://archives.nseindia.com/content/equities/block.csv
These two CSVs each hold a short trailing window of recent bulk/block
deals (not a full historical range you can query by date — NSE
refreshes them in place), which is exactly what "most recent deal for
this symbol" needs.

Fails silently (returns None) rather than ever raising, so a
broken/empty fetch here never blocks the alert itself — it just means
no "Last Bulk/Block Deal" line on that alert.
"""
import csv
import io
import datetime as dt

import requests

BULK_URL = "https://archives.nseindia.com/content/equities/bulk.csv"
BLOCK_URL = "https://archives.nseindia.com/content/equities/block.csv"

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "text/csv,*/*",
    "Referer": "https://www.nseindia.com/",
}

# {symbol: deal_or_None} -- reused for the rest of this process run so
# repeat lookups (or multiple alerts in one run) don't re-download.
_cache = {}

# The two CSVs themselves, downloaded at most once per process run
# (both scans in main.py run in the same process, so this is at most
# one download of each file per GitHub Actions run, not one per
# alerting symbol).
_bulk_rows = None
_block_rows = None


def _parse_bd_date(date_str):
    for fmt in ("%d-%b-%Y", "%d-%B-%Y"):
        try:
            return dt.datetime.strptime(date_str.strip(), fmt)
        except Exception:
            continue
    return dt.datetime.min


def _fetch_csv(url):
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=15)
        if resp.status_code != 200 or not resp.text.strip():
            return []
        return list(csv.DictReader(io.StringIO(resp.text)))
    except Exception as e:
        print(f"Bulk/Block deal CSV fetch failed ({url}): {e}")
        return []


def _get_rows(mode):
    """mode: 'bulk' or 'block'. Downloads once per process run, cached
    in the module-level _bulk_rows/_block_rows globals after that."""
    global _bulk_rows, _block_rows
    if mode == "bulk":
        if _bulk_rows is None:
            _bulk_rows = _fetch_csv(BULK_URL)
        return _bulk_rows
    else:
        if _block_rows is None:
            _block_rows = _fetch_csv(BLOCK_URL)
        return _block_rows


def _row_get(row, key):
    """NSE's CSV header spelling/spacing has shifted before (leading
    spaces, case) -- match loosely rather than assuming one exact
    key."""
    for actual_key in row.keys():
        if actual_key.strip().lower() == key.lower():
            v = row[actual_key]
            return v.strip() if v else v
    return None


def get_last_deal_for_symbol(symbol):
    """
    Returns the single most recent Bulk/Block deal for `symbol` found
    in NSE's current bulk.csv/block.csv archive snapshot, as
    {type, date, client, buy_sell, quantity, price}, or None if
    nothing found for that symbol / the fetch failed or came back
    empty.
    """
    symbol = symbol.upper()

    if symbol in _cache:
        return _cache[symbol]

    matches = []
    for mode, label in (("bulk", "Bulk"), ("block", "Block")):
        for row in _get_rows(mode):
            sym = (_row_get(row, "Symbol") or "").upper()
            if sym != symbol:
                continue
            matches.append({
                "type": label,
                "date": _row_get(row, "Date"),
                "client": _row_get(row, "Client Name"),
                "buy_sell": (_row_get(row, "Buy/Sell") or "").upper(),
                "quantity": _row_get(row, "Quantity Traded"),
                "price": _row_get(row, "Trade Price / Wght. Avg. Price"),
            })

    result = max(matches, key=lambda m: _parse_bd_date(m["date"] or "")) if matches else None
    _cache[symbol] = result
    return result
