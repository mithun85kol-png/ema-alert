"""
Bollinger Band re-entry strategy, evaluated on a caller-specified timeframe.

Sell signal: price was above the upper band and the latest closed candle
             re-enters and closes back inside, confirmed by a strong red
             (bearish) candle.
Buy signal:  price was below the lower band and the latest closed candle
             re-enters and closes back inside, confirmed by a strong green
             (bullish) candle.
"strong" candle = its body (open-close range) is at least as large as the
immediately preceding candle's body.
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
    timeframe: int


def evaluate(symbol: str, raw_1min_df: pd.DataFrame, timeframe_minutes: int) -> Optional[BBSignal]:
    candles = resample_candles(raw_1min_df, timeframe_minutes)

    min_bars_needed = config.BB_LENGTH + 2
    if len(candles) < min_bars_needed:
        log.debug("%s (%dm): not enough bars yet for BB (%d/%d)", symbol, timeframe_minutes, len(candles), min_bars_needed)
        return None

    candles = bollinger_bands(candles, config.BB_LENGTH, config.BB_MULT)

    last = candles.iloc[-1]
    prev = candles.iloc[-2]

    if pd.isna(prev["bb_upper"]) or pd.isna(last["bb_upper"]):
        return None

    prev_body = abs(prev["close"] - prev["open"])
    last_body = abs(last["close"] - last["open"])
    is_strong = last_body >= prev_body

    is_red = last["close"] < last["open"]
    is_green = last["close"] > last["open"]

    if prev["close"] > prev["bb_upper"] and last["close"] < last["bb_upper"] and is_red and is_strong:
        return BBSignal(symbol, "SELL", last["timestamp"], last["close"], last["open"], last["bb_upper"], timeframe_minutes)

    if prev["close"] < prev["bb_lower"] and last["close"] > last["bb_lower"] and is_green and is_strong:
        return BBSignal(symbol, "BUY", last["timestamp"], last["close"], last["open"], last["bb_lower"], timeframe_minutes)

    return None
