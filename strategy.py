"""
EMA9/EMA20 intrabar cross: alert fires when the latest closed candle's
high-low range straddles the EMA20 line (i.e. price crossed through EMA20
somewhere within the candle, not just close-to-close), confirmed ONLY by
a strong candle in the cross direction. Fires for stocks, indices, and
commodities alike — no separate rule per instrument type.

RSI, EMA50-based trend, and volume-vs-previous-candle are attached to the
signal as informational fields only. They are shown in the alert message
but never block it from firing.
"""

import config
from indicators import (
    add_emas, add_rsi, add_volume_avg, add_ema50,
    is_strong_candle, volume_vs_previous, detect_candle_pattern,
)


def check_signal(df, symbol):
    if len(df) < max(config.EMA_SLOW, config.RSI_PERIOD, config.VOLUME_AVG_PERIOD, 50) + 2:
        return None

    df = add_emas(df, config.EMA_FAST, config.EMA_SLOW)
    df = add_rsi(df, config.RSI_PERIOD)
    df = add_volume_avg(df, config.VOLUME_AVG_PERIOD)
    df = add_ema50(df)

    curr = df.iloc[-1]
    prev = df.iloc[-2]

    ema_slow_now = curr["ema_slow"]

    # Intrabar cross: candle's high-low range straddles the EMA20 line,
    # meaning price crossed through EMA20 somewhere within this candle
    # (not just close vs previous close).
    straddles_ema = curr["low"] <= ema_slow_now <= curr["high"]

    bullish_cross = straddles_ema and curr["close"] > ema_slow_now and curr["open"] <= ema_slow_now
    bearish_cross = straddles_ema and curr["close"] < ema_slow_now and curr["open"] >= ema_slow_now

    if not (bullish_cross or bearish_cross):
        return None

    direction = "BULLISH" if bullish_cross else "BEARISH"
    bullish = bullish_cross

    if not is_strong_candle(curr, config.STRONG_CANDLE_BODY_RATIO, bullish=bullish):
        return None

    stock_trend = "BULLISH" if curr["close"] > curr["ema_trend"] else "BEARISH"
    vol_change_pct = volume_vs_previous(df)
    rsi_val = curr["rsi"] if not pd_isna(curr["rsi"]) else None

    cross_pattern = detect_candle_pattern(curr)
    prev_pattern = detect_candle_pattern(prev)

    return {
        "symbol": symbol,
        "direction": direction,
        "close": round(float(curr["close"]), 2),
        "rsi": round(float(rsi_val), 1) if rsi_val is not None else None,
        "volume": int(curr["volume"]),
        "vol_avg": round(float(curr["vol_avg"]), 0) if not pd_isna(curr["vol_avg"]) else None,
        "vol_change_pct": vol_change_pct,
        "ema_fast": round(float(curr["ema_fast"]), 2),
        "ema_slow": round(float(curr["ema_slow"]), 2),
        "ema_trend": round(float(curr["ema_trend"]), 2),
        "stock_trend": stock_trend,
        "candle_time": str(curr.get("timestamp", "")),
        "cross_candle_pattern": cross_pattern,
        "prev_candle_pattern": prev_pattern,
    }


def pd_isna(val):
    import pandas as pd
    return pd.isna(val)
