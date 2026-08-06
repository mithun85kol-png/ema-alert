"""
Fetches previous trading day's delivery percentage from NSE's daily
bhavcopy (security-wise delivery position) and caches it in
delivery_cache.json, keyed by data-date. Stocks (cash EQ series) only —
indices and MCX commodities don't have a delivery %, callers simply
won't find those symbols in the returned map.
"""
import csv
import io
import json
import os
from datetime import timedelta

import requests

DELIVERY_CACHE_FILE = "delivery_cache.json"
NSE_BHAVCOPY_URL = "https://archives.nseindia.com/products/content/sec_bhavdata_full_{date}.csv"

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Referer": "https://www.nseindia.com/",
}


def _fetch_bhavcopy_for_date(date_obj):
    """
    Returns {symbol: delivery_pct} for one calendar date, or None if
    that date's file isn't available (weekend/holiday/not published
    yet/blocked).
    """
    date_str = date_obj.strftime("%d%m%Y")
    url = NSE_BHAVCOPY_URL.format(date=date_str)
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=15)
        if resp.status_code != 200 or not resp.text.strip():
            return None
        reader = csv.DictReader(io.StringIO(resp.text))
        data = {}
        for row in reader:
            series = (row.get(" SERIES") or row.get("SERIES") or "").strip()
            if series != "EQ":
                continue
            symbol = (row.get("SYMBOL") or "").strip()
            deliv_raw = (row.get(" DELIV_PER") or row.get("DELIV_PER") or "").strip()
            if not symbol or deliv_raw in ("", "-"):
                continue
            try:
                data[symbol] = float(deliv_raw)
            except ValueError:
                continue
        return data if data else None
    except Exception as e:
        print(f"Delivery bhavcopy fetch failed for {date_str}: {e}")
        return None


def _load_cache():
    if os.path.exists(DELIVERY_CACHE_FILE):
        try:
            with open(DELIVERY_CACHE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_cache(cache):
    with open(DELIVERY_CACHE_FILE, "w") as f:
        json.dump(cache, f)


def get_delivery_data(today_ist_date, lookback_days=6):
    """
    Returns {symbol: delivery_pct} for the most recent trading day's
    bhavcopy strictly before today_ist_date. Cached per data-date in
    delivery_cache.json so the file is downloaded at most once per
    calendar day, not once per run. Walks back up to lookback_days
    calendar days to auto-skip weekends/holidays.

    Returns {} (never raises) if nothing could be fetched in the
    lookback window — callers should treat that as "no delivery %
    available today", not an error.
    """
    cache = _load_cache()

    for back in range(1, lookback_days + 1):
        candidate = today_ist_date - timedelta(days=back)
        candidate_str = candidate.isoformat()

        if cache.get("date") == candidate_str and cache.get("data"):
            return cache["data"]

        data = _fetch_bhavcopy_for_date(candidate)
        if data:
            cache = {"date": candidate_str, "data": data}
            _save_cache(cache)
            return data

    print("Delivery data: no bhavcopy found in lookback window, skipping.")
    return {}
