"""
EMA9/EMA20 line crossover with trend confirmation: alert fires only when
ALL of these hold on the latest closed candle —
  1. EMA9 crosses EMA20 (matching the visual cross point on the chart)
  2. The crossing candle is strong in the cross direction
  3. EMA9/EMA20 are a meaningful distance apart at the moment of
     crossover (filters out whipsaw crosses that are invisible on the
     chart, e.g. 0.005% apart)
  4. The cross direction agrees with the broader trend (EMA50) — a
     bullish cross only fires if the stock is in an uptrend, and a
     bearish cross only fires if the stock is in a downtrend
Fires for stocks, indices, and commodities alike — no separate rule per
instrument type.

RSI, volume-vs-previous-candle %, candle patterns, and Camarilla R3/S3
pivot proximity are attached to the signal as informational fields
only. They are shown in the alert message but never block it from
firing.
"""

import config
from indicators import (
    add_emas, add_rsi, add_volume_avg, add_ema50,
    is_strong_candle, volume_vs_previous, detect_candle_pattern,
)

# How close price needs to be to R3/S3 (as a % of price) to be flagged
# as "near" that level, rather than "mid-range".
PIVOT_PROXIMITY_PCT = 0.3


def check_signal(df, symbol, r3=None, s3=None):
    if len(df) < max(config.EMA_SLOW, config.RSI_PERIOD, config.VOLUME_AVG_PERIOD, 50) + 2:
        return None

    df = add_emas(df, config.EMA_FAST, config.EMA_SLOW)
    df = add_rsi(df, config.RSI_PERIOD)
    df = add_volume_avg(df, config.VOLUME_AVG_PERIOD)
    df = add_ema50(df)

    curr = df.iloc[-1]
    prev = df.iloc[-2]

    # Condition 1: true EMA9/EMA20 line crossover.
    bullish_cross = prev["ema_fast"] <= prev["ema_slow"] and curr["ema_fast"] > curr["ema_slow"]
    bearish_cross = prev["ema_fast"] >= prev["ema_slow"] and curr["ema_fast"] < curr["ema_slow"]

    if not (bullish_cross or bearish_cross):
        return None

    direction = "BULLISH" if bullish_cross else "BEARISH"
    bullish = bullish_cross

    # Condition 2: candle must be strong in the cross direction.
    if not is_strong_candle(curr, config.STRONG_CANDLE_BODY_RATIO, bullish=bullish):
        return None

    # Condition 3: EMA9/EMA20 must actually be a meaningful distance
    # apart on this candle, not just touching. Without this, sideways/
    # choppy price action produces "crosses" where the two lines are
    # 0.005-0.01% apart — mathematically a cross, but invisible on the
    # chart and not a real signal.
    close_price = float(curr["close"])
    ema_gap_pct = abs(float(curr["ema_fast"]) - float(curr["ema_slow"])) / close_price * 100
    if ema_gap_pct < config.MIN_EMA_CROSS_GAP_PCT:
        return None

    # Condition 4 (MANDATORY): cross direction must agree with the
    # broader trend (EMA50) — a bullish cross only fires in an uptrend,
    # a bearish cross only fires in a downtrend.
    stock_trend = "BULLISH" if curr["close"] > curr["ema_trend"] else "BEARISH"
    if direction != stock_trend:
        return None

    # Volume is informational only — computed for the message but does
    # not block a signal from firing.
    vol_change_pct = volume_vs_previous(df)
    rsi_val = curr["rsi"] if not pd_isna(curr["rsi"]) else None

    cross_pattern = detect_candle_pattern(curr)
    prev_pattern = detect_candle_pattern(prev)

    pivot_note = None
    if r3 is not None and s3 is not None:
        if close_price >= r3:
            pivot_note = f"Above R3 ({r3}) — resistance broken"
        elif close_price <= s3:
            pivot_note = f"Below S3 ({s3}) — support broken"
        else:
            dist_to_r3_pct = abs(r3 - close_price) / close_price * 100
            dist_to_s3_pct = abs(close_price - s3) / close_price * 100
            if dist_to_r3_pct <= PIVOT_PROXIMITY_PCT:
                pivot_note = f"Near R3 ({r3}), {dist_to_r3_pct:.2f}% away — near resistance"
            elif dist_to_s3_pct <= PIVOT_PROXIMITY_PCT:
                pivot_note = f"Near S3 ({s3}), {dist_to_s3_pct:.2f}% away — near support"
            else:
                pivot_note = "Mid-range (not near R3/S3)"

    return {
        "symbol": symbol,
        "direction": direction,
        "close": round(close_price, 2),
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
        "r3": r3,
        "s3": s3,
        "pivot_note": pivot_note,
    }


def debug_ema_gap(df, symbol):
    """
    Debug helper only — NOT used in the firing decision. Returns how far
    apart EMA9/EMA20 are on the latest closed candle, as a % of price, so
    you can see in the Action logs which instruments are "close" to a
    cross even when none has fired yet.
    """
    if len(df) < config.EMA_SLOW + 2:
        return None

    df = add_emas(df, config.EMA_FAST, config.EMA_SLOW)
    curr = df.iloc[-1]

    ema_fast = float(curr["ema_fast"])
    ema_slow = float(curr["ema_slow"])
    close_price = float(curr["close"])
    gap_pct = abs(ema_fast - ema_slow) / close_price * 100

    return {
        "symbol": symbol,
        "ema_fast": round(ema_fast, 2),
        "ema_slow": round(ema_slow, 2),
        "gap_pct": round(gap_pct, 3),
        "leaning": "BULLISH (EMA9 above)" if ema_fast > ema_slow else "BEARISH (EMA9 below)",
    }


def pd_isna(val):
    import pandas as pd
    return pd.isna(val)
