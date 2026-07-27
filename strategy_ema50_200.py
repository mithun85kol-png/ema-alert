"""
EMA 50/200 crossover strategy on a 75-min timeframe. This is a separate,
additional alert from the main EMA 9/20 strategy in strategy.py - it does
not replace anything, it just runs alongside it.
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
    timeframe: int


def evaluate(symbol: str, raw_1min_df: pd.DataFrame) -> Optional[Signal]:
    tf = config.EMA50200_TIMEFRAME_MINUTES
    candles = resample_candles(raw_1min_df, tf)

    min_bars_needed = config.EMA50200_EMA_SLOW + config.RSI_PERIOD + 2
    if len(candles) < min_bars_needed:
        log.debug(
            "%s (%dm, EMA50/200): not enough bars yet (%d/%d)",
            symbol, tf, len(candles), min_bars_needed,
        )
        return None

    candles = add_indicators(
        candles, config.EMA50200_EMA_FAST, config.EMA50200_EMA_SLOW, config.RSI_PERIOD
    )

    last = candles.iloc[-1]
    prev = candles.iloc[-2]

    crossed_up = prev["ema_fast"] <= prev["ema_slow"] and last["ema_fast"] > last["ema_slow"]
    crossed_down = prev["ema_fast"] >= prev["ema_slow"] and last["ema_fast"] < last["ema_slow"]

    if crossed_up:
        return Signal(
            symbol, "BULLISH", last["timestamp"], last["close"],
            last["ema_fast"], last["ema_slow"], last["rsi"], tf,
        )
    if crossed_down:
        return Signal(
            symbol, "BEARISH", last["timestamp"], last["close"],
            last["ema_fast"], last["ema_slow"], last["rsi"], tf,
        )

    return None
