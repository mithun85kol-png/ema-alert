"""
EMA9 crossing EMA20 on the latest closed candle, confirmed by a strong
candle. RSI, EMA50 trend, sector performance, and volume-vs-previous
are attached as informational fields only — they do NOT block the alert.
"""

import config
from indicators import (
    add_emas, add_rsi, add_volume_avg, add_ema50,
    is_strong_candle, volume_vs_previous,
)
from instruments import get_sector_trend  # must exist in instruments.py


def check_signal(df, symbol):
    if len(df) < max(config.EMA_SLOW, config.RSI_PERIOD, config.VOLUME_AVG_PERIOD, 50) + 2:
        return None

    df = add_emas(df, config.EMA_FAST, config.EMA_SLOW)
    df = add_rsi(df, config.RSI_PERIOD)
    df = add_volume_avg(df, config.VOLUME_AVG_PERIOD)
    df = add_ema50(df)

    curr = df.iloc[-1]
    prev = df.iloc[-2]

    bullish_cross = prev["ema_fast"] <= prev["ema_slow"] and curr["ema_fast"] > curr["ema_slow"]
    bearish_cross = prev["ema_fast"] >= prev["ema_slow"] and curr["ema_fast"] < curr["ema_slow"]

    if not (bullish_cross or bearish_cross):
        return None

    direction = "BULLISH" if bullish_cross else "BEARISH"
    bullish = bullish_cross

    # Only condition that can block the alert: strong candle in the cross direction
    if not is_strong_candle(curr, config.STRONG_CANDLE_BODY_RATIO, bullish=bullish):
        return None

    # --- Informational fields only (never block the alert) ---
    stock_trend = "BULLISH" if curr["close"] > curr["ema_trend"] else "BEARISH"
    vol_change_pct = volume_vs_previous(df)
    sector_name, sector_trend = get_sector_trend(symbol)

    return {
        "symbol": symbol,
        "direction": direction,
        "close": round(float(curr["close"]), 2),
        "rsi": round(float(curr["rsi"]), 1),
        "volume": int(curr["volume"]),
        "vol_avg": round(float(curr["vol_avg"]), 0),
        "vol_change_pct": vol_change_pct,
        "ema_fast": round(float(curr["ema_fast"]), 2),
        "ema_slow": round(float(curr["ema_slow"]), 2),
        "ema_trend": round(float(curr["ema_trend"]), 2),
        "stock_trend": stock_trend,
        "sector": sector_name,
        "sector_trend": sector_trend,
        "candle_time": str(curr.get("timestamp", "")),
    }
