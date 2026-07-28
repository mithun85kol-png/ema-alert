"""
EMA 9/20 crossover strategy, evaluated on a caller-specified timeframe.
RSI is still calculated and shown in the alert for reference, but is not
used as a filter. Volume of the signal candle vs the previous candle is
also included for context.

Two filters are applied on top of the raw crossover:
1. Minimum-gap filter (config.EMA_MIN_GAP_PCT) - the EMA9/EMA20
   separation on the signal candle must be at least this % of price,
   otherwise the crossover is treated as noise and no alert is fired.
2. Trend filter (config.TREND_EMA_PERIOD) - a longer EMA (e.g. 50)
   is used as the market trend context. A BULLISH crossover only
   fires if price is above the trend EMA (uptrend), and a BEARISH
   crossover only fires if price is below it (downtrend). This filters
   out counter-trend signals.
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
    volume: float
    prev_volume: float


def evaluate(symbol: str, raw_1min_df: pd.DataFrame, timeframe_minutes: int) -> Optional[Signal]:
    candles = resample_candles(raw_1min_df, timeframe_minutes)

    trend_period = getattr(config, "TREND_EMA_PERIOD", 50)
    min_bars_needed = max(config.EMA_SLOW, trend_period) + config.RSI_PERIOD + 2
    if len(candles) < min_bars_needed:
        log.debug("%s (%dm): not enough bars yet (%d/%d)", symbol, timeframe_minutes, len(candles), min_bars_needed)
        return None

    candles = add_indicators(candles, config.EMA_FAST, config.EMA_SLOW, config.RSI_PERIOD)

    # Trend EMA computed locally so indicators.py doesn't need to change.
    candles["ema_trend"] = candles["close"].ewm(span=trend_period, adjust=False).mean()

    last = candles.iloc[-1]
    prev = candles.iloc[-2]

    crossed_up = prev["ema_fast"] <= prev["ema_slow"] and last["ema_fast"] > last["ema_slow"]
    crossed_down = prev["ema_fast"] >= prev["ema_slow"] and last["ema_fast"] < last["ema_slow"]

    if not (crossed_up or crossed_down):
        return None

    # Filter 1: minimum EMA9/EMA20 gap (ignore marginal/noisy crossovers)
    gap_pct = abs(last["ema_fast"] - last["ema_slow"]) / last["close"] * 100
    min_gap_pct = getattr(config, "EMA_MIN_GAP_PCT", 0.0)
    if gap_pct < min_gap_pct:
        log.debug(
            "%s (%dm): crossover ignored - gap too small (%.3f%% < %.3f%%)",
            symbol, timeframe_minutes, gap_pct, min_gap_pct,
        )
        return None

    # Filter 2: trend alignment (ignore counter-trend crossovers)
    uptrend = last["close"] > last["ema_trend"]
    if crossed_up and not uptrend:
        log.debug("%s (%dm): bullish crossover ignored - price below EMA%d (downtrend)",
                  symbol, timeframe_minutes, trend_period)
        return None
    if crossed_down and uptrend:
        log.debug("%s (%dm): bearish crossover ignored - price above EMA%d (uptrend)",
                  symbol, timeframe_minutes, trend_period)
        return None

    if crossed_up:
        return Signal(symbol, "BULLISH", last["timestamp"], last["close"],
                      last["ema_fast"], last["ema_slow"], last["rsi"], timeframe_minutes,
                      last["volume"], prev["volume"])

    if crossed_down:
        return Signal(symbol, "BEARISH", last["timestamp"], last["close"],
                      last["ema_fast"], last["ema_slow"], last["rsi"], timeframe_minutes,
                      last["volume"], prev["volume"])

    return None
