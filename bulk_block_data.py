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

ADDED 2026-08-29 (per request — "aro ekta alert chai", NSE Bulk Deals
page screenshot): a SEPARATE, standalone alert -- check_and_alert()
below -- that fires whenever a NEW bulk/block deal (any symbol, market
-wide, same as the NSE Bulk Deals page) shows up in these same two
CSVs, not just the enrichment line on an existing EMA-cross alert.
"""
import csv
import io
import os
import json
import hashlib
import datetime as dt

import requests

import config

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


def get_all_recent_deals():
    """
    Market-wide version of get_last_deal_for_symbol (added, per
    request) — EVERY row currently in bulk.csv + block.csv, for EVERY
    symbol, not just one. This is the same underlying archive snapshot
    (reuses _get_rows' once-per-process-run cache, so calling this
    alongside get_last_deal_for_symbol in the same run doesn't
    re-download), just returned in full rather than filtered/reduced
    to one symbol's latest. Each deal also includes "security_name"
    (not needed for the single-symbol lookup above, but wanted for the
    standalone alert below — matches the NSE Bulk Deals page's own
    SECURITY NAME column).
    """
    deals = []
    for mode, label in (("bulk", "Bulk"), ("block", "Block")):
        for row in _get_rows(mode):
            symbol = (_row_get(row, "Symbol") or "").upper()
            if not symbol:
                continue
            deals.append({
                "type": label,
                "date": _row_get(row, "Date"),
                "symbol": symbol,
                "security_name": _row_get(row, "Security Name"),
                "client": _row_get(row, "Client Name"),
                "buy_sell": (_row_get(row, "Buy/Sell") or "").upper(),
                "quantity": _row_get(row, "Quantity Traded"),
                "price": _row_get(row, "Trade Price / Wght. Avg. Price"),
            })
    return deals


def _deal_id(deal):
    """
    A stable dedup key for one deal row. NSE's CSVs don't give each
    row its own id, so this is built from every field that together
    identifies a specific disclosed trade — two genuinely different
    deals essentially never collide on all of type+date+symbol+
    client+buy_sell+quantity+price at once.
    """
    raw = "|".join(str(deal.get(k, "")) for k in
                    ("type", "date", "symbol", "client", "buy_sell", "quantity", "price"))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _load_alert_state():
    """
    Persisted across runs (unlike _cache/_bulk_rows/_block_rows above,
    which are only per-process) — {"seen": {deal_id: date_str, ...}}.
    Storing the deal's own date alongside each id (rather than just a
    bare set) is what lets _prune_alert_state below age entries out by
    real trade date instead of growing the file forever, since NSE's
    CSVs themselves only ever hold a short trailing window anyway.
    """
    path = config.BULK_DEAL_ALERT_STATE_FILE
    if not os.path.exists(path):
        return {"seen": {}}
    try:
        with open(path, "r") as f:
            data = json.load(f)
        if "seen" not in data:
            return {"seen": {}}
        return data
    except (json.JSONDecodeError, OSError):
        return {"seen": {}}


def _save_alert_state(state):
    with open(config.BULK_DEAL_ALERT_STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def _prune_alert_state(state, now_ist):
    """Drops any seen-id whose deal date is older than
    config.BULK_DEAL_ALERT_LOOKBACK_DAYS, so the state file doesn't
    grow forever."""
    cutoff = now_ist.date() - dt.timedelta(days=config.BULK_DEAL_ALERT_LOOKBACK_DAYS)
    kept = {}
    for deal_id, date_str in state.get("seen", {}).items():
        parsed = _parse_bd_date(date_str or "")
        if parsed != dt.datetime.min and parsed.date() >= cutoff:
            kept[deal_id] = date_str
    state["seen"] = kept


def check_and_alert(now_ist):
    """
    Standalone Bulk/Block Deal alert (added, per request) — fires once
    per NEW deal that shows up in NSE's bulk.csv/block.csv archive
    snapshot, market-wide (every symbol, not just the F&O/Nifty 500
    watchlist — same universe as the NSE Bulk Deals page itself).
    Meant to be called every run alongside corporate_actions.check_and
    _alert(), same non-blocking try/except pattern in main.run(): a
    failure here (bad CSV, network hiccup) must never take down the
    main EMA-cross scan.

    Dedup is against a persisted state file (config.
    BULK_DEAL_ALERT_STATE_FILE), NOT the in-memory alert_state.json
    used by the EMA-cross alerts — a deal has no "candle_time"/
    "direction" to fit that schema, so it gets its own tiny state file
    with its own id scheme (_deal_id) and its own pruning
    (_prune_alert_state), same overall shape as
    load_daily_score_report_state in main.py.
    """
    if not config.BULK_DEAL_ALERT_ENABLED:
        return

    # local import — avoids a circular import (telegram_notifier
    # doesn't import bulk_block_data, so this is safe, just kept local
    # so this module can still be imported standalone/tested without
    # needing the Telegram env vars set).
    from telegram_notifier import send_bulk_deal_alert

    deals = get_all_recent_deals()
    if not deals:
        return

    state = _load_alert_state()
    seen = state["seen"]

    new_deals = [d for d in deals if _deal_id(d) not in seen]
    if not new_deals:
        _prune_alert_state(state, now_ist)
        _save_alert_state(state)
        return

    # Oldest first, so if several new deals showed up since the last
    # run (e.g. bot was down, or first run of the day catching a
    # whole day's worth at once) they arrive in Telegram in a sensible
    # chronological order rather than reversed.
    new_deals.sort(key=lambda d: _parse_bd_date(d.get("date") or ""))

    for deal in new_deals:
        deal_id = _deal_id(deal)
        try:
            send_bulk_deal_alert(deal)
        except Exception as e:
            print(f"send_bulk_deal_alert failed for {deal.get('symbol')}: {e}", flush=True)
            # Not marked seen on a send failure — will retry next run,
            # same "never silently eat a real signal" principle as the
            # rest of this bot (see state.in_cooldown's docstring).
            continue
        seen[deal_id] = deal.get("date")

    _prune_alert_state(state, now_ist)
    _save_alert_state(state)
