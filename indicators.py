"""
Indicator helper functions for EMA cross alert system.
Provides EMA, RSI, volume average, candle strength, and candle
pattern detection utilities used by strategy.py.
"""

import pandas as pd


def add_emas(df, fast_period, slow_period):
    """
    Adds 'ema_fast' and 'ema_slow' columns based on the given periods.
    """
    df["ema_fast"] = df["close"].ewm(span=fast_period, adjust=False).mean()
    df["ema_slow"] = df["close"].ewm(span=slow_period, adjust=False).mean()
    return df


def add_ema50(df):
    """
    Adds 'ema_trend' column using a 50-period EMA (used for trend direction).
    """
    df["ema_trend"] = df["close"].ewm(span=50, adjust=False).mean()
    return df


def add_rsi(df, period):
    """
    Adds 'rsi' column using standard RSI calculation.
    """
    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()

    rs = avg_gain / avg_loss
    df["rsi"] = 100 - (100 / (1 + rs))
    return df


def add_volume_avg(df, period):
    """
    Adds 'vol_avg' column: rolling average volume over the given period.
    """
    df["vol_avg"] = df["volume"].rolling(window=period).mean()
    return df


def volume_vs_previous(df):
    """
    Returns % change of current candle's volume vs the previous candle's volume.
    """
    curr_vol = df.iloc[-1]["volume"]
    prev_vol = df.iloc[-2]["volume"]
    if prev_vol == 0:
        return None
    return round(((curr_vol - prev_vol) / prev_vol) * 100, 1)


def is_strong_candle(row, body_ratio_threshold, bullish=True):
    """
    Returns True if the candle's body is a strong proportion of its range,
    and the candle is in the expected direction (bullish/bearish).
    """
    high, low, open_, close = row["high"], row["low"], row["open"], row["close"]
    candle_range = high - low
    if candle_range <= 0:
        return False

    body = abs(close - open_)
    body_ratio = body / candle_range

    if body_ratio < body_ratio_threshold:
        return False

    if bullish:
        return close > open_
    else:
        return close < open_


def detect_candle_pattern(row):
    """
    Classifies a single candle as one of: Marubozu (Bullish/Bearish),
    Hammer, Inverted Hammer, Shooting Star, Doji, or Normal.
    Returns a short label string.
    """
    high, low, open_, close = row["high"], row["low"], row["open"], row["close"]
    candle_range = high - low
    if candle_range <= 0:
        return "N/A"

    body = abs(close - open_)
    body_ratio = body / candle_range
    upper_wick = high - max(open_, close)
    lower_wick = min(open_, close) - low

    # Doji: very small body relative to range
    if body_ratio < 0.1:
        return "Doji"

    # Marubozu: body takes up almost the whole range (tiny/no wicks)
    if body_ratio > 0.9:
        return "Bullish Marubozu" if close > open_ else "Bearish Marubozu"

    # Hammer: small body near top, long lower wick, little/no upper wick
    if lower_wick >= 2 * body and upper_wick <= body * 0.3:
        return "Hammer"

    # Shooting Star / Inverted Hammer: small body near bottom, long upper wick
    if upper_wick >= 2 * body and lower_wick <= body * 0.3:
        return "Shooting Star" if close < open_ else "Inverted Hammer"

    return "Normal"
