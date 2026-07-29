"""
NOTE: verify Upstox's intraday endpoint/interval support against current
docs before relying on this — API versions change. This pulls 1-minute
candles and resamples to 5-minute locally.
"""

import sys
import time
import datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

import config
import instruments
import state
from strategy import check_signal
from telegram_notifier import send_alert

UPSTOX_INTRADAY_URL = "https://api.upstox.com/v2/historical-candle/intraday/{instrument_key}/1minute"

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))

# Stock / index cash-market session (NSE).
STOCK_SESSION_START = dt.time(9, 15)
STOCK_SESSION_END = dt.time(15, 30)

# Commodity (MCX) session — runs later into the evening.
COMMODITY_SESSION_START = dt.time(9, 0)
COMMODITY_SESSION_END = dt.time(23, 30)


def _now_ist():
    return dt.datetime.now(IST)


def _in_stock_session(now_ist):
    return STOCK_SESSION_START <= now_ist.time() <= STOCK_SESSION_END


def _in_commodity_session(now_ist):
    return COMMODITY_SESSION_START <= now_ist.time() <= COMMODITY_SESSION_END


def fetch_1min_candles(instrument_key):
    headers = {"Authorization": f"Bearer {config.UPSTOX_ACCESS_TOKEN}"}
    url = UPSTOX_INTRADAY_URL.format(instrument_key=instrument_key)
    resp = requests.get(url, headers=headers, timeout=20)
    resp.raise_for_status()
    payload = resp.json()
    candles = payload.get("data", {}).get("candles", [])
    if not candles:
        return None

    df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df


def resample_5min(df):
    df = df.set_index("timestamp")
    out = df.resample("5min").agg({
        "open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum",
    }).dropna().reset_index()
    return out


def build_watchlist(now_ist=None):
    now_ist = now_ist or _now_ist()
    watchlist = {}

    if _in_stock_session(now_ist):
        watchlist.update(instruments.resolve_indices(config.INDICES))
        fo_watch = None if config.USE_FULL_FO_LIST else config.FO_STOCK_WATCHLIST
        watchlist.update(instruments.resolve_fo_stock_list(fo_watch))

    if _in_commodity_session(now_ist):
        watchlist.update(instruments.resolve_mcx_nearest_futures(config.COMMODITIES))

    return watchlist


def run():
    if not config.UPSTOX_ACCESS_TOKEN:
        print("UPSTOX_ACCESS_TOKEN not set — aborting.")
        sys.exit(1)

    now_ist = _now_ist()
    if not (_in_stock_session(now_ist) or _in_commodity_session(now_ist)):
        print(f"Outside all trading sessions ({now_ist.strftime('%H:%M')} IST) — skipping.")
        return

    watchlist = build_watchlist(now_ist)
    print(f"Scanning {len(watchlist)} instruments...")

    saved_state = state.load_state()
    alerts_sent = 0

    # Phase 1: fetch + resample every instrument in parallel (bounded pool
    # so we don't hammer Upstox's API all at once).
    dfs = {}

    def _fetch_one(symbol, instrument_key):
        raw = fetch_1min_candles(instrument_key)
        if raw is None or len(raw) < 30:
            return symbol, None
        return symbol, resample_5min(raw)

    with ThreadPoolExecutor(max_workers=config.FETCH_WORKERS) as pool:
        futures = {
            pool.submit(_fetch_one, symbol, instrument_key): symbol
            for symbol, instrument_key in watchlist.items()
        }
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                sym, df5 = future.result()
                if df5 is not None:
                    dfs[sym] = df5
            except Exception as e:
                print(f"Error fetching {symbol}: {e}")

    # Phase 2: run the signal check per instrument.
    for symbol, df5 in dfs.items():
        try:
            signal = check_signal(df5, symbol)
            if signal is None:
                continue

            if state.already_alerted(saved_state, symbol, signal["direction"], signal["candle_time"]):
                continue

            send_alert(signal)
            state.mark_alerted(saved_state, symbol, signal["direction"], signal["candle_time"])
            alerts_sent += 1

        except Exception as e:
            print(f"Error on {symbol}: {e}")

    state.save_state(saved_state)
    print(f"Done. {alerts_sent} alert(s) sent.")


if __name__ == "__main__":
    run()
