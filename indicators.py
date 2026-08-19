"""
Indicator helper functions for EMA cross alert system.
Provides EMA, RSI, volume average, candle strength, candle
pattern detection, and Camarilla pivot utilities used by strategy.py.
"""

import pandas as pd


def add_emas(df, fast_period, slow_period):
    df["ema_fast"] = df["close"].ewm(span=fast_period, adjust=False).mean()
    df["ema_slow"] = df["close"].ewm(span=slow_period, adjust=False).mean()
    return df


def add_ema50(df):
    df["ema_trend"] = df["close"].ewm(span=50, adjust=False).mean()
    return df


def add_macd(df, fast_period=12, slow_period=26, signal_period=9):
    """
    Adds 'macd_line', 'macd_signal', and 'macd_hist' columns.
    macd_line = EMA(fast) - EMA(slow) of close
    macd_signal = EMA(signal_period) of macd_line
    macd_hist = macd_line - macd_signal
    """
    ema_fast = df["close"].ewm(span=fast_period, adjust=False).mean()
    ema_slow = df["close"].ewm(span=slow_period, adjust=False).mean()
    df["macd_line"] = ema_fast - ema_slow
    df["macd_signal"] = df["macd_line"].ewm(span=signal_period, adjust=False).mean()
    df["macd_hist"] = df["macd_line"] - df["macd_signal"]
    return df


def add_rsi(df, period):
    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss
    df["rsi"] = 100 - (100 / (1 + rs))
    return df


def add_volume_avg(df, period):
    df["vol_avg"] = df["volume"].rolling(window=period).mean()
    return df


def volume_vs_previous(df):
    curr_vol = df.iloc[-1]["volume"]
    prev_vol = df.iloc[-2]["volume"]
    if prev_vol == 0:
        return None
    return round(((curr_vol - prev_vol) / prev_vol) * 100, 1)


def is_strong_candle(row, body_ratio_threshold, bullish=True):
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
    high, low, open_, close = row["high"], row["low"], row["open"], row["close"]
    candle_range = high - low
    if candle_range <= 0:
        return "N/A"
    body = abs(close - open_)
    body_ratio = body / candle_range
    upper_wick = high - max(open_, close)
    lower_wick = min(open_, close) - low
    if body_ratio < 0.1:
        return "Doji"
    if body_ratio > 0.9:
        return "Bullish Marubozu" if close > open_ else "Bearish Marubozu"
    if lower_wick >= 2 * body and upper_wick <= body * 0.3:
        return "Hammer"
    if upper_wick >= 2 * body and lower_wick <= body * 0.3:
        return "Shooting Star" if close < open_ else "Inverted Hammer"
    return "Normal"


def add_atr(df, period):
    """
    Adds an 'atr' column — Average True Range, Wilder-smoothed (same
    ewm(alpha=1/period) style as add_rsi above, the standard ATR
    convention). True Range for each row = max of:
      high - low
      abs(high - previous close)
      abs(low - previous close)
    First row has no previous close, so its True Range is just
    high - low (no different-day gap to measure yet).
    """
    prev_close = df["close"].shift(1)
    tr1 = df["high"] - df["low"]
    tr2 = (df["high"] - prev_close).abs()
    tr3 = (df["low"] - prev_close).abs()
    true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    true_range.iloc[0] = tr1.iloc[0]
    df["atr"] = true_range.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    return df


def calculate_r3_s3(prev_high, prev_low, prev_close):
    candle_range = prev_high - prev_low
    r3 = prev_close + candle_range * 1.1 / 4
    s3 = prev_close - candle_range * 1.1 / 4
    return round(r3, 2), round(s3, 2)
