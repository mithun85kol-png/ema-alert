"""
EMA9 crossing EMA20 on the latest closed candle, confirmed by strong
candle + volume + RSI/trend. Returns None or a signal dict.
"""

import config
from indicators import add_emas, add_rsi, add_volume_avg, is_strong_candle, is_volume_confirmed, is_trend_confirmed


def check_signal(df, symbol):
    if len(df) < max(config.EMA_SLOW, config.RSI_PERIOD, config.VOLUME_AVG_PERIOD) + 2:
        return None

    df = add_emas(df, config.EMA_FAST, config.EMA_SLOW)
    df = add_rsi(df, config.RSI_PERIOD)
    df = add_volume_avg(df, config.VOLUME_AVG_PERIOD)

    curr = df.iloc[-1]
    prev = df.iloc[-2]

    bullish_cross = prev["ema_fast"] <= prev["ema_slow"] and curr["ema_fast"] > curr["ema_slow"]
    bearish_cross = prev["ema_fast"] >= prev["ema_slow"] and curr["ema_fast"] < curr["ema_slow"]

    if not (bullish_cross or bearish_cross):
        return None

    direction = "BULLISH" if bullish_cross else "BEARISH"
    bullish = bullish_cross

    if not is_strong_candle(curr, config.STRONG_CANDLE_BODY_RATIO, bullish=bullish):
        return None
    if not is_volume_confirmed(curr, config.VOLUME_MULTIPLIER):
        return None
    if not is_trend_confirmed(curr, bullish=bullish):
        return None

    return {
        "symbol": symbol,
        "direction": direction,
        "close": round(float(curr["close"]), 2),
        "rsi": round(float(curr["rsi"]), 1),
        "volume": int(curr["volume"]),
        "vol_avg": round(float(curr["vol_avg"]), 0),
        "ema_fast": round(float(curr["ema_fast"]), 2),
        "ema_slow": round(float(curr["ema_slow"]), 2),
        "candle_time": str(curr.get("timestamp", "")),
    }
