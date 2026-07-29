"""
Indicator + confirmation helpers. All functions take a pandas DataFrame
with columns: open, high, low, close, volume (oldest row first).
"""

import pandas as pd


def add_emas(df, fast=9, slow=20):
    df["ema_fast"] = df["close"].ewm(span=fast, adjust=False).mean()
    df["ema_slow"] = df["close"].ewm(span=slow, adjust=False).mean()
    return df


def add_rsi(df, period=14):
    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, 1e-9)
    df["rsi"] = 100 - (100 / (1 + rs))
    return df


def add_volume_avg(df, period=20):
    df["vol_avg"] = df["volume"].rolling(period).mean()
    return df


def is_strong_candle(row, body_ratio_min=0.6, bullish=True):
    high, low, open_, close = row["high"], row["low"], row["open"], row["close"]
    candle_range = high - low
    if candle_range <= 0:
        return False
    body = abs(close - open_)
    if body / candle_range < body_ratio_min:
        return False
    if bullish:
        return close > open_
    return close < open_


def is_volume_confirmed(row, multiplier=1.3):
    if pd.isna(row.get("vol_avg")) or row["vol_avg"] == 0:
        return False
    return row["volume"] >= multiplier * row["vol_avg"]


def is_trend_confirmed(row, bullish=True):
    if pd.isna(row.get("rsi")):
        return False
    if bullish:
        return row["rsi"] >= 55
    return row["rsi"] <= 45
