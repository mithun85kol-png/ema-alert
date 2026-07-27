"""
EMA9 retest strategy, evaluated on the same timeframe as the main EMA
strategy. After an EMA9/EMA20 crossover, this looks for the first
subsequent candle where price pulls back and touches the EMA9 line while
the trend (EMA9 vs EMA20 side) is still intact — a common continuation
confirmation pattern. Fires only once per crossover (the first retest).
"""
from dataclasses import dataclass
from typing import Optional
import pandas as pd

import config
from indicators import resample_candles, add_indicators

log = config.get_logger(__name__)

LOOKBACK_BARS = 15  # how far back to search for the triggering crossover


@dataclass
class RetestSignal:
    symbol: str
    direction: str
    candle_time: pd.Timestamp
    close: float
    ema_fast: float


def evaluate(symbol: str, raw_1min_df: pd.DataFrame) -> Optional[RetestSignal]:
    candles = resample_candles(raw_1min_df, config.TIMEFRAME_MINUTES)

    min_bars_needed = config.EMA_SLOW + LOOKBACK_BARS + 2
    if len(candles) < min_bars_needed:
        return None

    candles = add_indicators(candles, config.EMA_FAST, config.EMA_SLOW, config.RSI_PERIOD)

    last_idx = len(candles) - 1
    start = max(1, last_idx - LOOKBACK_BARS)

    cross_idx = None
    direction = None
    for i in range(last_idx, start - 1, -1):
        prev = candles.iloc[i - 1]
        cur = candles.iloc[i]
        if prev["ema_fast"] <= prev["ema_slow"] and cur["ema_fast"] > cur["ema_slow"]:
            cross_idx, direction = i, "BULLISH"
            break
        if prev["ema_fast"] >= prev["ema_slow"] and cur["ema_fast"] < cur["ema_slow"]:
            cross_idx, direction = i, "BEARISH"
            break

    if cross_idx is None or cross_idx == last_idx:
        return None  # no crossover found, or the crossover bar itself is "now" (that's the cross alert, not retest)

    segment = candles.iloc[cross_idx:last_idx + 1]
    if direction == "BULLISH" and not (segment["ema_fast"] > segment["ema_slow"]).all():
        return None  # trend broke before a retest happened
    if direction == "BEARISH" and not (segment["ema_fast"] < segment["ema_slow"]).all():
        return None

    between = candles.iloc[cross_idx + 1:last_idx]
    for _, row in between.iterrows():
        if row["low"] <= row["ema_fast"] <= row["high"]:
            return None  # already retested on an earlier candle, don't re-alert

    last = candles.iloc[last_idx]
    touched_now = last["low"] <= last["ema_fast"] <= last["high"]
    if not touched_now:
        return None

    return RetestSignal(symbol, direction, last["timestamp"], last["close"], last["ema_fast"])
