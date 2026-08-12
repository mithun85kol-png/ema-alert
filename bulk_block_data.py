"""
Fetches the SINGLE MOST RECENT Bulk or Block deal for a symbol —
NSE's disclosure of a single-client, single-stock, single-day trade
big enough to cross NSE's threshold (bulk: >=0.5% of the company's
total listed shares in a day; block: a separate large-trade window,
minimum 5,00,000 shares or Rs 5 crore) — searched over a trailing
lookback window (config below), not just the current/most recent
trading day.

NOTE / RELIABILITY WARNING: unlike delivery_data.py's bhavcopy (a
static file on archives.nseindia.com, which has been reliable from
GitHub Actions), this uses NSE's MAIN SITE historical API
(www.nseindia.com/api/historical/...), which sits behind NSE's
bot-protection and is known to block requests from cloud/datacenter
IP ranges — including GitHub Actions runners — even with a
browser-like User-Agent and a same-site cookie warm-up (see
_get_session below). Verify the exact endpoint path/params against
NSE's current site if this stops returning data; this module is
written to fail silently (returns None) rather than ever raise, so a
broken/blocked fetch here never blocks the alert itself — it just
means no "Last Bulk/Block Deal" line on that alert.
"""
import datetime as dt

import requests

LOOKBACK_DAYS = 180  # how far back to search for the most recent deal

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}

# {symbol: (date_fetched, result_or_None)} — re-used for the rest of
# this process run once a symbol has been looked up, so a symbol that
# alerts more than once in one run doesn't re-hit NSE.
_cache = {}


def _get_session():
    """
    NSE's main site rejects a bare API call without first visiting a
    normal page to pick up session cookies (same reason every
    unofficial NSE-scraping library does this two-step warm-up).
    """
    s = requests.Session()
    s.headers.update(_HEADERS)
    try:
        s.get("https://www.nseindia.com/", timeout=10)
    except Exception:
        pass
    return s


def _fetch_historical(session, mode, from_str, to_str):
    """mode: "bulk-deals" or "block-deals"."""
    url = f"https://www.nseindia.com/api/historical/{mode}"
    try:
        resp = session.get(url, params={"from": from_str, "to": to_str}, timeout=15)
        if resp.status_code != 200:
            return []
        return resp.json().get("data", [])
    except Exception as e:
        print(f"NSE historical {mode} fetch failed: {e}")
        return []


def _parse_bd_date(date_str):
    try:
        return dt.datetime.strptime(date_str, "%d-%b-%Y")
    except Exception:
        return dt.datetime.min


def get_last_deal_for_symbol(symbol):
    """
    Returns the single most recent Bulk/Block deal for `symbol` within
    the trailing LOOKBACK_DAYS, as
    {type, date, client, buy_sell, quantity, price}, or None if
    nothing found in that window / the fetch failed or got blocked.
    """
    symbol = symbol.upper()
    today = dt.date.today()

    cached = _cache.get(symbol)
    if cached is not None and cached[0] == today:
        return cached[1]

    to_str = today.strftime("%d-%m-%Y")
    from_str = (today - dt.timedelta(days=LOOKBACK_DAYS)).strftime("%d-%m-%Y")

    session = _get_session()
    matches = []
    for mode, label in (("bulk-deals", "Bulk"), ("block-deals", "Block")):
        for r in _fetch_historical(session, mode, from_str, to_str):
            if (r.get("BD_SYMBOL") or "").strip().upper() == symbol:
                matches.append({
                    "type": label,
                    "date": r.get("BD_DT_DATE"),
                    "client": r.get("BD_CLIENT_NAME"),
                    "buy_sell": (r.get("BD_BUY_SELL") or "").strip().upper(),
                    "quantity": r.get("BD_QTY_TRD"),
                    "price": r.get("BD_TP_WATP"),
                })

    result = max(matches, key=lambda m: _parse_bd_date(m["date"])) if matches else None
    _cache[symbol] = (today, result)
    return result
