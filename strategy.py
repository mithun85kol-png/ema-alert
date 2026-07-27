"""
EMA 9/20 crossover strategy, evaluated on 5-minute candles.
RSI is still calculated and shown in the alert for reference, but is not
used as a filter — every crossover triggers an alert.
"""
from dataclasses import dataclass
from typing import Optional
import pandas as pd

import config
from indicators import resample_candles, add_indicators

log = config.get_logger(__name__)


@dataclass
class Signal:
    symbol: str
    direction: str
    candle_time: pd.Timestamp
    close: float
    ema_fast: float
    ema_slow: float
    rsi: float


def evaluate(symbol: str, raw_1min_df: pd.DataFrame) -> Optional[Signal]:
    candles = resample_candles(raw_1min_df, config.TIMEFRAME_MINUTES)

    min_bars_needed = config.EMA_SLOW + config.RSI_PERIOD + 2
    if len(candles) < min_bars_needed:
        log.debug("%s: not enough bars yet (%d/%d)", symbol, len(candles), min_bars_needed)
        return None

    candles = add_indicators(candles, config.EMA_FAST, config.EMA_SLOW, config.RSI_PERIOD)

    last = candles.iloc[-1]
    prev = candles.iloc[-2]

    crossed_up = prev["ema_fast"] <= prev["ema_slow"] and last["ema_fast"] > last["ema_slow"]
    crossed_down = prev["ema_fast"] >= prev["ema_slow"] and last["ema_fast"] < last["ema_slow"]

    if crossed_up:
        return Signal(symbol, "BULLISH", last["timestamp"], last["close"],
                      last["ema_fast"], last["ema_slow"], last["rsi"])

    if crossed_down:
        return Signal(symbol, "BEARISH", last["timestamp"], last["close"],
                      last["ema_fast"], last["ema_slow"], last["rsi"])

    return None
