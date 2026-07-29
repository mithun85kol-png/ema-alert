"""
EMA 9/20 crossover strategy, evaluated on a caller-specified timeframe.
RSI is still calculated and shown in the alert for reference. Volume of
the signal candle vs the previous candle is also included for context.

DESIGN GOAL: fire the alert on the crossing candle's own close (not a
few candles later), so entry price stays close to EMA20 - this keeps a
stoploss placed at EMA20 as small as possible.

Because we fire immediately, there's no "future candle" to check for a
whipsaw reversal. Instead, strength is confirmed using data available on
the crossing candle itself:
1. Minimum-gap filter (config.EMA_MIN_GAP_PCT) - kept very small/near-zero
   on purpose, since EMA9/EMA20 are ~equal by definition at the moment of
   a cross. This just screens out literal ties/float noise, not real
   crossovers.
2. Volume confirmation (config.VOLUME_CONFIRMATION_MULT) - the crossing
   candle's volume must be at least this multiple of the previous
   candle's volume. Weak/choppy crosses tend to happen on unremarkable
   volume; a real breakout/breakdown usually shows a volume pickup right
   on the crossing candle.
3. RSI momentum confirmation - for a BULLISH cross, RSI must be rising
   (last > prev); for a BEARISH cross, RSI must be falling (last < prev).
   This rejects crosses where momentum is already fading on the very
   candle where the EMAs crossed.

Cross detection itself scans a small lookback window
(config.CROSS_LOOKBACK_BARS) instead of only the last two candles, so a
delayed/skipped scheduled run doesn't permanently miss a real cross.

Trend context (config.TREND_EMA_PERIOD) is still calculated and included
in every signal as informational context (UPTREND/DOWNTREND based on
price vs the longer EMA), but it does not block any crossover from firing.
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
    trend: str


def _find_recent_cross(candles: pd.DataFrame, lookback_bars: int):
    """
    Scan the last `lookback_bars` candles (excluding the very first row of
    the dataframe, since we need a "prev" for each candidate) for the most
    recent EMA9/EMA20 crossover. Returns (index_of_cross_candle, direction)
    or (None, None) if no cross is found in the window.

    Scans from most-recent backwards so that if there were somehow two
    crosses in the window (rare), we alert on the latest one.
    """
    n = len(candles)
    start = max(1, n - lookback_bars)

    for i in range(n - 1, start - 1, -1):
        last = candles.iloc[i]
        prev = candles.iloc[i - 1]

        crossed_up = prev["ema_fast"] <= prev["ema_slow"] and last["ema_fast"] > last["ema_slow"]
        crossed_down = prev["ema_fast"] >= prev["ema_slow"] and last["ema_fast"] < last["ema_slow"]

        if crossed_up:
            return i, "BULLISH"
        if crossed_down:
            return i, "BEARISH"

    return None, None


def evaluate(symbol: str, raw_1min_df: pd.DataFrame, timeframe_minutes: int) -> Optional[Signal]:
    candles = resample_candles(raw_1min_df, timeframe_minutes)

    trend_period = getattr(config, "TREND_EMA_PERIOD", 50)
    lookback_bars = getattr(config, "CROSS_LOOKBACK_BARS", 3)

    min_bars_needed = max(config.EMA_SLOW, trend_period) + config.RSI_PERIOD + lookback_bars
    if len(candles) < min_bars_needed:
        log.debug("%s (%dm): not enough bars yet (%d/%d)", symbol, timeframe_minutes, len(candles), min_bars_needed)
        return None

    candles = add_indicators(candles, config.EMA_FAST, config.EMA_SLOW, config.RSI_PERIOD)

    # Trend EMA computed locally so indicators.py doesn't need to change.
    candles["ema_trend"] = candles["close"].ewm(span=trend_period, adjust=False).mean()

    cross_idx, direction = _find_recent_cross(candles, lookback_bars)
    if cross_idx is None:
        return None

    cross_candle = candles.iloc[cross_idx]
    prev_candle = candles.iloc[cross_idx - 1]

    # Filter 1: minimum EMA9/EMA20 gap on the crossing candle itself.
    # Kept near-zero on purpose - see module docstring.
    gap_pct = abs(cross_candle["ema_fast"] - cross_candle["ema_slow"]) / cross_candle["close"] * 100
    min_gap_pct = getattr(config, "EMA_MIN_GAP_PCT", 0.0)
    if gap_pct < min_gap_pct:
        log.debug(
            "%s (%dm): crossover ignored - gap too small on cross candle (%.4f%% < %.4f%%)",
            symbol, timeframe_minutes, gap_pct, min_gap_pct,
        )
        return None

    # Filter 2: volume confirmation on the crossing candle itself.
    volume_mult = getattr(config, "VOLUME_CONFIRMATION_MULT", 1.0)
    prev_vol = prev_candle["volume"] if prev_candle["volume"] else 0
    if prev_vol > 0 and cross_candle["volume"] < prev_vol * volume_mult:
        log.debug(
            "%s (%dm): crossover ignored - weak volume on cross candle (%.0f < %.0f x %.2f)",
            symbol, timeframe_minutes, cross_candle["volume"], prev_vol, volume_mult,
        )
        return None

    # Filter 3: RSI momentum must agree with the cross direction.
    require_rsi_momentum = getattr(config, "REQUIRE_RSI_MOMENTUM", True)
    if require_rsi_momentum:
        if direction == "BULLISH" and not (cross_candle["rsi"] > prev_candle["rsi"]):
            log.debug("%s (%dm): bullish crossover ignored - RSI not rising on cross candle",
                      symbol, timeframe_minutes)
            return None
        if direction == "BEARISH" and not (cross_candle["rsi"] < prev_candle["rsi"]):
            log.debug("%s (%dm): bearish crossover ignored - RSI not falling on cross candle",
                      symbol, timeframe_minutes)
            return None

    # Trend context (informational only - does NOT block the signal)
    uptrend = cross_candle["close"] > cross_candle["ema_trend"]
    trend_label = "UPTREND" if uptrend else "DOWNTREND"
    if direction == "BULLISH" and not uptrend:
        log.debug("%s (%dm): bullish crossover fired counter-trend (price below EMA%d)",
                  symbol, timeframe_minutes, trend_period)
    if direction == "BEARISH" and uptrend:
        log.debug("%s (%dm): bearish crossover fired counter-trend (price above EMA%d)",
                  symbol, timeframe_minutes, trend_period)

    return Signal(
        symbol, direction, cross_candle["timestamp"], cross_candle["close"],
        cross_candle["ema_fast"], cross_candle["ema_slow"], cross_candle["rsi"], timeframe_minutes,
        cross_candle["volume"], prev_candle["volume"], trend_label,
    )
