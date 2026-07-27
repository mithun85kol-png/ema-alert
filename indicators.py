"""
Technical indicator helpers: candle resampling, EMA, RSI.
"""
import pandas as pd


def resample_candles(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.set_index("timestamp")
    ohlc = df.resample(f"{minutes}min", label="right", closed="right").agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    })
    ohlc = ohlc.dropna(subset=["open", "high", "low", "close"]).reset_index()
    return ohlc


def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()

    rs = avg_gain / avg_loss.replace(0, pd.NA)
    rsi_values = 100 - (100 / (1 + rs))
    rsi_values = rsi_values.fillna(50)
    return rsi_values


def add_indicators(df: pd.DataFrame, ema_fast: int, ema_slow: int, rsi_period: int) -> pd.DataFrame:
    df = df.copy()
    df["ema_fast"] = ema(df["close"], ema_fast)
    df["ema_slow"] = ema(df["close"], ema_slow)
    df["rsi"] = rsi(df["close"], rsi_period)
    return df
