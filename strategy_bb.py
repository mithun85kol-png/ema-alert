"""
Bollinger Band re-entry strategy, evaluated on 5-minute candles.

Sell signal: price was above the upper band and the latest closed candle
             re-enters below the upper band, confirmed by a red (bearish)
             candle (close < open).
Buy signal:  price was below the lower band and the latest closed candle
             re-enters above the lower band, confirmed by a green (bullish)
             candle (close > open).
"""
from dataclasses import dataclass
from typing import Optional
import pandas as pd

import config
from indicators import resample_candles, bollinger_bands

log = config.get_logger(__name__)


@dataclass
class BBSignal:
    symbol: str
    direction: str
    candle_time: pd.Timestamp
    close: float
    open: float
    band_level: float


def evaluate(symbol: str, raw_1min_df: pd.DataFrame) -> Optional[BBSignal]:
    candles = resample_candles(raw_1min_df, config.TIMEFRAME_MINUTES)

    min_bars_needed = config.BB_LENGTH + 2
    if len(candles) < min_bars_needed:
        log.debug("%s: not enough bars yet for BB (%d/%d)", symbol, len(candles), min_bars_needed)
        return None

    candles = bollinger_bands(candles, config.BB_LENGTH, config.BB_MULT)

    last = candles.iloc[-1]
    prev = candles.iloc[-2]

    if pd.isna(prev["bb_upper"]) or pd.isna(last["bb_upper"]):
        return None

    is_red = last["close"] < last["open"]
    is_green = last["close"] > last["open"]

    if prev["close"] > prev["bb_upper"] and last["close"] < last["bb_upper"] and is_red:
        return BBSignal(symbol, "SELL", last["timestamp"], last["close"], last["open"], last["bb_upper"])

    if prev["close"] < prev["bb_lower"] and last["close"] > last["bb_lower"] and is_green:
        return BBSignal(symbol, "BUY", last["timestamp"], last["close"], last["open"], last["bb_lower"])

    return None
