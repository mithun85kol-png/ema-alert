"""
NOTE: verify Upstox's intraday endpoint/interval support against current
docs before relying on this — API versions change. This pulls 1-minute
candles and resamples to 5-minute locally.
"""

import sys
import time
import pandas as pd
import requests

import config
import instruments
import state
from strategy import check_signal
from telegram_notifier import send_alert

UPSTOX_INTRADAY_URL = "https://api.upstox.com/v2/historical-candle/intraday/{instrument_key}/1minute"


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


def build_watchlist():
    watchlist = {}
    watchlist.update(instruments.resolve_indices(config.INDICES))
    watchlist.update(instruments.resolve_mcx_nearest_futures(config.COMMODITIES))

    fo_watch = None if config.USE_FULL_FO_LIST else config.FO_STOCK_WATCHLIST
    watchlist.update(instruments.resolve_fo_stock_list(fo_watch))
    return watchlist


def run():
    if not config.UPSTOX_ACCESS_TOKEN:
        print("UPSTOX_ACCESS_TOKEN not set — aborting.")
        sys.exit(1)

    watchlist = build_watchlist()
    print(f"Scanning {len(watchlist)} instruments...")

    saved_state = state.load_state()
    alerts_sent = 0

    for symbol, instrument_key in watchlist.items():
        try:
            raw = fetch_1min_candles(instrument_key)
            if raw is None or len(raw) < 30:
                continue
            df5 = resample_5min(raw)
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

        time.sleep(0.2)

    state.save_state(saved_state)
    print(f"Done. {alerts_sent} alert(s) sent.")


if __name__ == "__main__":
    run()
