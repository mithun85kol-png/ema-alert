"""
Groww chart / stock-page links for every alert.

WHY THIS FILE EXISTS (fix, 2026-10-19 — "sob chart groww te khulche na"):
the old build_chart_link() in main.py built a Groww URL by slugifying the
company's *current* legal name. That is unreliable, because Groww's URL
slug is its own "search_id", which is frozen at the company's ORIGINAL
name and does not follow renames/demergers. Real examples from Groww's
own stock list:
    TAAL Tech                        -> /stocks/taal-enterprises-ltd
    Tanla Platforms                  -> /stocks/tanla-solutions-ltd
    TARC                             -> /stocks/anant-raj-global-ltd
    Tata Motors Passenger Vehicles   -> /stocks/tata-motors-ltd
So a slug guessed from the name 404'd for every renamed stock and the
alert silently fell back to TradingView. Indices and MCX commodities
ALWAYS fell back to TradingView (they were never given a Groww link).

Resolution order for get_url(symbol, company_name):
  1. config.GROWW_FIXED_URLS      — full URLs for indices / MCX commodities
  2. config.GROWW_SYMBOL_OVERRIDES — hand-fixed {symbol: slug}
  3. groww_slug_cache.json         — slugs already resolved on an earlier run
  4. Groww's own search endpoint   — exact NSE-symbol match -> its real
                                     search_id (the slug). Saved to (3).
  5. name-based guess, only if the page is confirmed to exist
Returns None if none of these work, and the caller falls back to
TradingView (the Telegram label then honestly says "TradingView").

Never raises, never blocks an alert: every network call is short, and
after a few failures in one run Groww lookups are skipped for the rest of
that run.
"""
import json
import os
import re
import threading

import requests

import config

CACHE_FILE = getattr(config, "GROWW_SLUG_CACHE_FILE", "groww_slug_cache.json")

SEARCH_URL = "https://groww.in/v1/api/search/v1/entity"
STOCK_URL = "https://groww.in/stocks/{slug}"

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://groww.in/",
}

_lock = threading.Lock()
_cache = None            # {symbol: slug}, lazy-loaded from CACHE_FILE
_failed_this_run = set()  # symbols whose lookup already failed this run
_net_errors = 0          # consecutive-ish network failures this run
_MAX_NET_ERRORS = 3


# ---------------------------------------------------------------- cache

def _load_cache():
    global _cache
    if _cache is not None:
        return _cache
    try:
        with open(CACHE_FILE, "r") as f:
            data = json.load(f)
        _cache = data if isinstance(data, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        _cache = {}
    return _cache


def _save_cache():
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(_cache or {}, f, indent=2, sort_keys=True)
    except OSError as e:
        print(f"[groww-link] could not save {CACHE_FILE}: {e}")


# ------------------------------------------------------- Groww search API

def _iter_items(obj, depth=0):
    """Yields every dict that looks like a search hit, whatever the
    wrapper around it is ({"content": [...]}, {"data": {"content": ...}},
    a bare list...). Groww has reshaped this response before."""
    if depth > 4:
        return
    if isinstance(obj, list):
        for x in obj:
            if isinstance(x, dict):
                if any(k in x for k in ("search_id", "searchId", "slug")):
                    yield x
                else:
                    yield from _iter_items(x, depth + 1)
    elif isinstance(obj, dict):
        for k in ("content", "data", "results", "items", "entities", "stocks"):
            if k in obj:
                yield from _iter_items(obj[k], depth + 1)


def _item_symbol(item):
    for k in ("nse_scrip_code", "nseScripCode", "nse_symbol", "nseSymbol", "symbol", "id"):
        v = item.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip().upper()
    return None


def _item_is_stock(item):
    et = str(item.get("entity_type") or item.get("entityType") or "").lower()
    return (not et) or ("stock" in et) or ("equity" in et)


def _search_slug(symbol):
    """Groww's own search -> the real URL slug (search_id) for an NSE
    symbol, accepting ONLY an exact NSE-symbol match (never a fuzzy
    one, so a wrong company can't be linked). None if not found or the
    request failed."""
    global _net_errors
    if _net_errors >= _MAX_NET_ERRORS:
        return None
    try:
        resp = requests.get(
            SEARCH_URL,
            params={"app": "false", "page": 0, "q": symbol, "size": 15},
            headers=_HEADERS, timeout=5,
        )
        if resp.status_code != 200:
            _net_errors += 1
            print(f"[groww-link] search HTTP {resp.status_code} for {symbol}")
            return None
        _net_errors = 0
        want = symbol.upper()
        for item in _iter_items(resp.json()):
            if not _item_is_stock(item):
                continue
            if _item_symbol(item) != want:
                continue
            slug = item.get("search_id") or item.get("searchId") or item.get("slug")
            if isinstance(slug, str) and slug.strip():
                return slug.strip().strip("/")
        return None
    except Exception as e:
        _net_errors += 1
        print(f"[groww-link] search failed for {symbol}: {e}")
        return None


# ---------------------------------------------- name-based last resort

def slugify_name(name):
    """Best-effort 'Reliance Industries Limited' -> 'reliance-industries-ltd'.
    Only a LAST resort — see the module docstring for why it is wrong
    for renamed companies."""
    s = name.lower().replace("&", " ")
    s = re.sub(r"[.,]", "", s)
    s = re.sub(r"\blimited\b", "ltd", s)
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")


def _page_exists(url):
    """True / False if Groww clearly says the page exists / doesn't;
    None if we couldn't tell (network trouble)."""
    global _net_errors
    if _net_errors >= _MAX_NET_ERRORS:
        return None
    try:
        r = requests.get(url, headers=_HEADERS, timeout=5)
        if r.status_code == 404:
            return False
        if r.status_code != 200:
            return None
        m = re.search(r"<title>(.*?)</title>", r.text[:20000], re.I | re.S)
        title = (m.group(1) if m else "").lower()
        if "404" in title or "page not found" in title or "not found" in title:
            return False
        return True
    except Exception:
        _net_errors += 1
        return None


# ------------------------------------------------------------- public API

def get_url(symbol, company_name=None):
    """Groww URL for `symbol`, or None (caller then falls back to
    TradingView)."""
    symbol = symbol.upper()

    fixed = getattr(config, "GROWW_FIXED_URLS", {}).get(symbol)
    if fixed:
        return fixed

    override = getattr(config, "GROWW_SYMBOL_OVERRIDES", {}).get(symbol)
    if override:
        return STOCK_URL.format(slug=override)

    with _lock:
        cache = _load_cache()
        if symbol in cache:
            return STOCK_URL.format(slug=cache[symbol])
        if symbol in _failed_this_run:
            return None

    slug = _search_slug(symbol)
    if slug:
        with _lock:
            _load_cache()[symbol] = slug
            _save_cache()
        return STOCK_URL.format(slug=slug)

    if company_name:
        guess = slugify_name(company_name)
        url = STOCK_URL.format(slug=guess)
        if guess and _page_exists(url) is True:
            return url  # not cached: a name guess is never trusted long-term

    with _lock:
        _failed_this_run.add(symbol)
    print(f"[groww-link] no Groww page found for {symbol} -> Groww search link; "
          f"add it to config.GROWW_SYMBOL_OVERRIDES (slug = last part of its groww.in/stocks/... URL)")
    return None
