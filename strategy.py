
"""
EMA9/EMA20 line crossover with trend confirmation: a signal fires for
a given closed candle only when ALL of these hold on that candle —
  1. EMA9 crosses EMA20 (matching the visual cross point on the chart)
     — a plain crossover, nothing more. There is no longer a strong-
     candle filter or a minimum EMA-gap filter; any qualifying cross
     is eligible as soon as it happens.
  2. The cross direction agrees with the broader trend (EMA50) — a
     bullish cross only fires if the stock is in an uptrend, and a
     bearish cross only fires if the stock is in a downtrend.
     This condition is MANDATORY for stocks and commodities. It does
     NOT apply to indices at all — indices don't use check_signals()
     on 75-min data anymore; see below.

INDICES (NIFTY 50, NIFTY BANK, SENSEX) are NOT evaluated by
check_signals() on 75-min data. main.py instead calls check_signals()
directly on 5-min data (df5) with require_trend_confirmation=False, so
an index alert is a PURE EMA9/EMA20 crossover on the 5-min chart — no
EMA50 trend requirement, no 75-min involvement at all. RSI/volume/
VWAP/MACD/pivot context fields are still computed the same way (since
_evaluate_candle is timeframe-agnostic — see the NOTE below), just on
5-min bars for indices instead of 75-min bars.
Fires for stocks, indices, and commodities alike via the same
check_signals()/_evaluate_candle() code path — only the timeframe
passed in and the require_trend_confirmation flag differ per
instrument type (set by main.py).

RSI, candle patterns, VWAP position, and Camarilla R3/S3 pivot
proximity are attached to the signal as informational fields only.
They are shown in the alert message but never block it from firing.
Volume (vol_change_pct) is ALSO now informational only (no longer a
gating condition) — the crossing candle's volume vs the previous
candle's is shown in the message, but a signal fires whether volume
rose or not. If the previous candle's volume is 0/unavailable,
vol_change_pct is simply None on the signal/message.

IMPORTANT — catch-up window:
check_signals() scans the last config.CROSS_LOOKBACK_CANDLES closed
candles (not just the single latest one) and returns a signal for
EVERY qualifying candle in that window. This means: if a scheduled run
is skipped or delayed (so more than one 3-min candle closed since the
last run), any cross that happened on an "in-between" candle is still
caught and alerted on the next run, instead of silently disappearing
because it's no longer the "latest" candle. main.py's dedup is keyed
on (symbol, direction, candle_time), so a candle already alerted is
never re-sent even though it's re-checked on every later run.

NOTE (timeframe — FLIPPED): check_signals() itself is timeframe-agnostic
— it just evaluates whatever OHLCV df it's handed. main.py now passes
in df75 (1-min data, historical + today, resampled to 75-min candles)
for STOCKS/COMMODITIES — 75-min is their PRIMARY/ALERTING timeframe.
Every gating condition above (EMA9/20 cross, EMA50 trend agreement) is
evaluated on 75-min bars for them; RSI/volume/VWAP/MACD/pivot context
is also computed on 75-min bars but is informational only. This means
a stock/commodity alert fires right when a 75-min candle closes with a
qualifying cross.
For INDICES, main.py instead passes df5 (5-min candles) into the exact
same check_signals(), with require_trend_confirmation=False — so an
index alert fires right when a 5-min candle closes with a qualifying
EMA9/20 cross, and all the RSI/volume/VWAP/MACD/pivot context on that
alert is computed on 5-min bars too (still informational only).

get_3min_trend_info() below is a separate, filter-free helper that
reports EMA9/20 bias (and how close price is to the next cross) on
3-min candles. It is only used as supporting context UNDERNEATH a
STOCK/COMMODITY 75-min alert (main.py attaches it as
signal["trend_3min"]); it is not used for indices, since an index
alert already IS the 3-min signal.
3-min candles. It is USED by main.py: every 75-min alert has a 3-min
context block attached via signal["trend_3min"], purely so you can see
at a glance what the shorter-term 3-min picture is doing right now. It
never gates or blocks the 75-min alert; it is informational only.

Sector index trend (added): get_sector_trend() below reports whether a
stock's sector index (e.g. NIFTY BANK for HDFCBANK, NIFTY IT for TCS —
see config.STOCK_SECTOR_MAP) is currently in an UPTREND or DOWNTREND
(close vs EMA50, same rule as a stock's own trend). main.py computes
this once per sector index per run and attaches it to every matching
stock's alert as signal["sector_index"] / signal["sector_trend"].
Purely informational — never blocks a stock's alert, and stocks with
no sector mapping simply don't get this line.

VWAP (added): computed cumulatively from the start of the current
session's df (Upstox intraday endpoint only returns the current
trading day, so no extra day-boundary handling is needed). Purely
informational — never blocks a signal. Attached inline inside
_evaluate_candle since it needs the same df/idx already in scope, no
separate API call or extra fetch required.

MACD Divergence (added): standard MACD(12,26,9) is computed on the
same 3-min df already in scope (indicators.add_macd) — no extra fetch.
_detect_macd_divergence() checks the trailing
config.MACD_DIVERGENCE_LOOKBACK_CANDLES window for a bullish or
bearish divergence between price and the MACD line. Purely
informational — raw MACD values and any divergence note are attached
to the signal but never block it from firing.
"""

import datetime as _dt

import pandas as pd

import config
from indicators import (
    add_emas, add_rsi, add_volume_avg, add_ema50, add_macd,
    detect_candle_pattern, add_atr, add_ema,
)

# How close price needs to be to R3/S3 (as a % of price) to be flagged
# as "near" that level, rather than "mid-range".
PIVOT_PROXIMITY_PCT = 0.3


def _min_required_len(lookback, ema_slow=None):
    ema_slow = ema_slow if ema_slow is not None else config.EMA_SLOW
    return max(ema_slow, config.RSI_PERIOD, config.VOLUME_AVG_PERIOD, config.MACD_SLOW, 50) + lookback + 1


def _detect_macd_cross_recent(df, idx, lookback):
    """
    "MACD cross must be within [the lookback]" (added, per request) —
    a genuine MACD LINE crossing its SIGNAL line (sign of
    macd_line - macd_signal flipping) somewhere in the trailing
    `lookback` candles ending at idx. This is DIFFERENT from
    _detect_macd_divergence above (that's price vs MACD divergence,
    unrelated) and different from a plain "macd_line > macd_signal"
    level check (that's just today's current bias regardless of how
    long ago it crossed, or whether it even crossed at all this
    session). Reuses config.MACD_DIVERGENCE_LOOKBACK_CANDLES as the
    window (same "how far back is still recent enough" number already
    used for MACD elsewhere), rather than introducing a second lookback
    constant for a very similar idea.
    Returns "BULLISH" if the LATEST sign-flip in the window was
    macd_line crossing UP through macd_signal, "BEARISH" if the latest
    flip was crossing DOWN, or None if there was no flip in the window
    (or not enough data) -- caller compares this against the signal's
    direction, same pattern as opening_range_breakout.
    """
    window_start = max(0, idx - lookback + 1)
    window = df.iloc[window_start: idx + 1]
    if len(window) < 2 or "macd_line" not in window or "macd_signal" not in window:
        return None
    diff = window["macd_line"] - window["macd_signal"]
    if diff.isna().any():
        return None
    sign = diff.gt(0)
    flips = sign.ne(sign.shift(1))
    flips.iloc[0] = False  # first row has nothing to compare against
    flip_positions = flips[flips].index
    if len(flip_positions) == 0:
        return None
    latest_flip = flip_positions[-1]
    return "BULLISH" if sign.loc[latest_flip] else "BEARISH"


def _detect_macd_divergence(df, idx, lookback):
    """
    Simple half-window divergence check: splits the trailing `lookback`
    candles (ending at idx) into two halves and compares the extreme
    price point in each half against the MACD line at that same index.
      Bullish divergence: price's lowest low in the recent half is LOWER
      than its lowest low in the earlier half, but the MACD line at that
      recent low is HIGHER than the MACD line at the earlier low (price
      falling faster than downside momentum — weakening downtrend).
      Bearish divergence: mirror, using highs and MACD peaks (price
      rising to a new high while MACD makes a lower high — weakening
      uptrend).
    Returns {"bullish": bool, "bearish": bool} — both False if the
    window is too short or no divergence is present.
    """
    window_start = max(0, idx - lookback + 1)
    window = df.iloc[window_start: idx + 1]
    if len(window) < 4:
        return {"bullish": False, "bearish": False}

    mid = len(window) // 2
    first_half = window.iloc[:mid]
    second_half = window.iloc[mid:]

    first_low_pos = first_half["low"].idxmin()
    second_low_pos = second_half["low"].idxmin()
    bullish = (
        df.loc[second_low_pos, "low"] < df.loc[first_low_pos, "low"]
        and df.loc[second_low_pos, "macd_line"] > df.loc[first_low_pos, "macd_line"]
    )

    first_high_pos = first_half["high"].idxmax()
    second_high_pos = second_half["high"].idxmax()
    bearish = (
        df.loc[second_high_pos, "high"] > df.loc[first_high_pos, "high"]
        and df.loc[second_high_pos, "macd_line"] < df.loc[first_high_pos, "macd_line"]
    )

    return {"bullish": bool(bullish), "bearish": bool(bearish)}


def _compute_opening_range_breakout(df, idx):
    """
    Checklist item "1st 15-min High/Low breakout + candle close beyond
    it" (added, per request). Finds TODAY's first candle in df (same
    "first row of today" logic as get_opening_candle_bias, but usable
    at any idx, not just the latest row -- needed here since this runs
    inside _evaluate_candle for whichever candle is being scored) and
    checks whether candle `idx` CLOSED beyond that opening candle's
    high/low:
      - close > opening_high -> "BULLISH" (breakout above the opening
        range, closing outside it -- not just wicking through)
      - close < opening_low  -> "BEARISH" (breakdown below the opening
        range)
      - neither, OR idx IS today's first candle itself (nothing to
        break out of yet) -> None
    df is expected to be 15-min candles (df15) -- same timeframe the
    checklist itself is defined on. Never raises; returns None on any
    missing/insufficient data rather than blocking the caller.
    """
    curr = df.iloc[idx]
    ts = df["timestamp"]
    today = curr["timestamp"].date() if hasattr(curr["timestamp"], "date") else None
    if today is None:
        return None
    today_mask = ts.dt.date == today
    today_positions = [i for i, v in enumerate(today_mask) if v]
    if not today_positions or today_positions[0] == idx:
        return None
    opening_idx = today_positions[0]
    opening_high = float(df.iloc[opening_idx]["high"])
    opening_low = float(df.iloc[opening_idx]["low"])
    close_price = float(curr["close"])
    if close_price > opening_high:
        return "BULLISH"
    if close_price < opening_low:
        return "BEARISH"
    return None


def _compute_vwap_at(df, idx):
    """
    Cumulative session VWAP up to and including candle `idx`:
        VWAP = sum(typical_price * volume) / sum(volume)
    where typical_price = (high + low + close) / 3 for each candle.
    df is assumed to contain only the current trading session's candles
    (true for data from Upstox's intraday endpoint), so no explicit
    "since market open" filtering is needed — row 0 already is the
    session's first candle.
    """
    window = df.iloc[: idx + 1]
    typical_price = (window["high"] + window["low"] + window["close"]) / 3
    cum_vol = window["volume"].sum()
    if cum_vol <= 0:
        return None
    return float((typical_price * window["volume"]).sum() / cum_vol)


def _find_swing_points(df, end_idx, lookback, strength):
    """
    Fractal swing-point finder used by detect_trendline_break().
    Scans df in [end_idx-lookback, end_idx-1] (never includes end_idx
    itself — that's the candle being tested for a break) and returns
    two lists of (idx, price) tuples, oldest first:
      swing_highs: candles whose high is the max of the `strength`
                   candles on BOTH sides of it
      swing_lows:  candles whose low is the min of the `strength`
                   candles on BOTH sides of it
    A candle needs `strength` candles free on both sides to even be
    checked, so the most recent possible swing point is at
    end_idx-1-strength, not end_idx-1.
    """
    start = max(strength, end_idx - lookback)
    stop = end_idx - strength  # exclusive
    swing_highs, swing_lows = [], []
    for p in range(start, stop):
        window_hi = df["high"].iloc[p - strength: p + strength + 1]
        window_lo = df["low"].iloc[p - strength: p + strength + 1]
        if df["high"].iloc[p] == window_hi.max():
            swing_highs.append((p, float(df["high"].iloc[p])))
        if df["low"].iloc[p] == window_lo.min():
            swing_lows.append((p, float(df["low"].iloc[p])))
    return swing_highs, swing_lows


def _trendline_value_at(p1, p2, x):
    """Linear interpolation/extrapolation of the line through p1,p2 at x."""
    (x1, y1), (x2, y2) = p1, p2
    slope = (y2 - y1) / (x2 - x1)
    return y2 + slope * (x - x2)


def detect_trendline_break(df, idx, lookback=None, strength=None):
    """
    Diagonal trendline break (added, per request) — classic "connect
    the last two swing highs/lows and watch for price to close through
    that diagonal line" break, independent of EMA/RSI/VWAP/etc.

    Resistance line: drawn through the last 2 confirmed swing HIGHS,
    only valid if they're descending (2nd high < 1st high — a real
    downtrend line, not a flat/rising one). A BULLISH break fires when
    today's close crosses above that line while the previous candle's
    close was still at/below it (catches the exact breaking candle,
    not every candle after an old break).

    Support line: mirror image — last 2 confirmed swing LOWS, only
    valid if ascending (2nd low > 1st low). A BEARISH break fires when
    close crosses below it, previous candle still at/above it.

    Both lines are checked independently — in principle both a
    resistance and a support line could exist at once (e.g. inside a
    triangle), but only one can actually be broken by a single candle's
    close, so at most one break is ever returned.

    Returns None if no swing pair / no break, else:
      {"direction": "BULLISH" | "BEARISH",
       "line_type": "RESISTANCE" | "SUPPORT",
       "line_value": float (line's value at the break candle),
       "point1": {"idx", "time", "price"},   # older swing point
       "point2": {"idx", "time", "price"},   # newer swing point
       "candles_in_trend": int}              # point2.idx - point1.idx
    """
    lookback = lookback or config.TRENDLINE_LOOKBACK_CANDLES
    strength = strength or config.TRENDLINE_SWING_STRENGTH

    if idx < 1 or idx - lookback < strength + 1:
        return None

    swing_highs, swing_lows = _find_swing_points(df, idx, lookback, strength)

    close_now = float(df["close"].iloc[idx])
    close_prev = float(df["close"].iloc[idx - 1])

    # ---- resistance line (descending swing highs) -> bullish break ----
    if len(swing_highs) >= 2:
        p1, p2 = swing_highs[-2], swing_highs[-1]
        if p2[1] < p1[1]:  # descending
            line_now = _trendline_value_at(p1, p2, idx)
            line_prev = _trendline_value_at(p1, p2, idx - 1)
            if close_now > line_now and close_prev <= line_prev:
                return {
                    "direction": "BULLISH",
                    "line_type": "RESISTANCE",
                    "line_value": round(line_now, 2),
                    "point1": {"idx": p1[0], "time": df["timestamp"].iloc[p1[0]], "price": round(p1[1], 2)},
                    "point2": {"idx": p2[0], "time": df["timestamp"].iloc[p2[0]], "price": round(p2[1], 2)},
                    "candles_in_trend": p2[0] - p1[0],
                }

    # ---- support line (ascending swing lows) -> bearish break ----
    if len(swing_lows) >= 2:
        p1, p2 = swing_lows[-2], swing_lows[-1]
        if p2[1] > p1[1]:  # ascending
            line_now = _trendline_value_at(p1, p2, idx)
            line_prev = _trendline_value_at(p1, p2, idx - 1)
            if close_now < line_now and close_prev >= line_prev:
                return {
                    "direction": "BEARISH",
                    "line_type": "SUPPORT",
                    "line_value": round(line_now, 2),
                    "point1": {"idx": p1[0], "time": df["timestamp"].iloc[p1[0]], "price": round(p1[1], 2)},
                    "point2": {"idx": p2[0], "time": df["timestamp"].iloc[p2[0]], "price": round(p2[1], 2)},
                    "candles_in_trend": p2[0] - p1[0],
                }

    return None


def check_trendline_scan(df, symbol, lookback=None, strength=None):
    """
    Standalone Trendline Break scan entry point (added, per request) —
    separate from check_signals()/EMA cross entirely; a break here
    does NOT require an EMA cross on the same candle. Checks ONLY the
    latest closed candle (unlike check_signals' multi-candle
    CROSS_LOOKBACK_CANDLES catch-up window) since main.py calls this
    every scan cycle on the freshly fetched df, so there's no gap to
    catch up on, and re-scanning old candles every run would just
    re-detect the same already-alerted break.

    Returns a signal dict ready for telegram_notifier.send_trendline_alert,
    or None if nothing broke on the latest candle.
    """
    if len(df) < 5:
        return None
    idx = len(df) - 1
    brk = detect_trendline_break(df, idx, lookback=lookback, strength=strength)
    if brk is None:
        return None

    candle_time = df["timestamp"].iloc[idx]
    return {
        "symbol": symbol,
        "direction": brk["direction"],
        "line_type": brk["line_type"],
        "line_value": brk["line_value"],
        "close": round(float(df["close"].iloc[idx]), 2),
        "candle_time": candle_time,
        "point1": brk["point1"],
        "point2": brk["point2"],
        "candles_in_trend": brk["candles_in_trend"],
    }


def compute_daily_score(curr, prev, vwap, close_price):
    """
    "Daily Score" (added, per request) — a fixed 8-point bullish-quality
    checklist (was 7; MACD added per follow-up request), independent of
    whatever direction the EMA cross itself fired in and independent of
    the scan's own EMA_FAST/EMA_SLOW pair. Unlike a variable-denominator
    score (which would scale /N over only the dimensions that had data),
    every one of these 8 checks always has data by the time
    _evaluate_candle calls this, so it's a plain count out of a fixed
    8 — no scaling.

    The 8 checks:
      1. Close > VWAP
      2. EMA9 > EMA21           (fixed ds_ema9 / ds_ema21 columns)
      3. EMA21 > EMA50          (ds_ema21 vs ema_trend, also fixed)
      4. RSI(14) > 50
      5. RSI(14) < 70
      6. Volume > SMA(Volume, 20) x 1.5   (curr.vol_avg is already
         SMA(Volume, config.VOLUME_AVG_PERIOD=20))
      7. Close > 1 candle ago High (prev.high)
      8. MACD line > MACD signal (curr.macd_line/macd_signal — already
         computed every run via add_macd, same 3-min df, no extra
         fetch). Bullish-crossover framing, same as every other check
         here — not compared against the alert's own direction (that's
         what the intraday checklist's / recent-cross MACD checks are
         for elsewhere).

    Purely informational — never blocks or filters an alert. Any check
    whose input is missing (e.g. VWAP is None on the very first candle
    of a session, or MACD hasn't warmed up yet) simply counts as
    not-met rather than being excluded, so the score is always out of
    a flat 8 and directly comparable across every alert.

    Returns {"score": int (0-8), "total": 8, "checks": dict, "label": str}.
    """
    rsi_val = curr["rsi"] if not pd_isna(curr["rsi"]) else None
    vol_avg = curr["vol_avg"] if not pd_isna(curr["vol_avg"]) else None
    macd_line = curr["macd_line"] if not pd_isna(curr["macd_line"]) else None
    macd_signal = curr["macd_signal"] if not pd_isna(curr["macd_signal"]) else None

    checks = {
        "close_above_vwap":   vwap is not None and close_price > vwap,
        "ema9_above_ema21":   float(curr["ds_ema9"]) > float(curr["ds_ema21"]),
        "ema21_above_ema50":  float(curr["ds_ema21"]) > float(curr["ema_trend"]),
        "rsi_above_50":       rsi_val is not None and rsi_val > 50,
        "rsi_below_70":       rsi_val is not None and rsi_val < 70,
        "volume_spike_1_5x":  vol_avg is not None and vol_avg > 0 and curr["volume"] > (1.5 * vol_avg),
        "close_above_prev_high": close_price > float(prev["high"]),
        "macd_bullish":       macd_line is not None and macd_signal is not None and macd_line > macd_signal,
    }
    score = sum(1 for v in checks.values() if v)
    total = len(checks)

    return {
        "score": score,
        "total": total,
        "checks": checks,
        "label": f"{score}/{total}",
    }


def compute_daily_score_scan(df, symbol):
    """
    Standalone Daily Score check (added, per request — "ami alada ekta
    alert chai sob FNO STOCKER DAILY SCORE... ekta combined report/list
    — sob F&O stock er Daily Score ekshathe ekta message-e, 8/8 hole")
    — computes Daily Score for the LATEST closed candle only,
    completely independent of whether an EMA cross happened on it.
    Used by main.py to build the combined "Perfect Daily Score" F&O
    report (all qualifying stocks listed together in ONE message,
    rather than send_alert's per-symbol EMA-cross alert).

    Reuses the SAME primary_df each symbol's EMA-cross check already
    has in memory this run — no extra fetch. Adds the ds_ema9/ds_ema21/
    ema_trend/rsi/vol_avg columns needed by compute_daily_score if
    they aren't already on df (they usually already are, since
    check_signals/_evaluate_candle computed them moments earlier on
    this same df — the `in df.columns` guards make this a no-op then).

    Returns None if df doesn't have enough history for EMA50 to be
    meaningful yet. Otherwise:
      {"symbol", "score", "total", "label", "close", "candle_time"}
    — same daily_score fields, just flattened with symbol/close/time
    for easy sorting/listing in the report.
    """
    if df is None or len(df) < 55:
        return None

    if "ds_ema9" not in df.columns:
        df = add_ema(df, 9, "ds_ema9")
    if "ds_ema21" not in df.columns:
        df = add_ema(df, 21, "ds_ema21")
    if "ema_trend" not in df.columns:
        df = add_ema50(df)
    if "rsi" not in df.columns:
        df = add_rsi(df, config.RSI_PERIOD)
    if "vol_avg" not in df.columns:
        df = add_volume_avg(df, config.VOLUME_AVG_PERIOD)
    if "macd_line" not in df.columns or "macd_signal" not in df.columns:
        df = add_macd(df, config.MACD_FAST, config.MACD_SLOW, config.MACD_SIGNAL)

    idx = len(df) - 1
    curr = df.iloc[idx]
    prev = df.iloc[idx - 1]
    vwap = _compute_vwap_at(df, idx)
    close_price = float(curr["close"])

    daily_score = compute_daily_score(curr, prev, vwap, close_price)

    return {
        "symbol": symbol,
        "score": daily_score["score"],
        "total": daily_score["total"],
        "label": daily_score["label"],
        "close": close_price,
        "candle_time": curr["timestamp"],
    }


def _evaluate_candle(df, idx, symbol, r3, s3, require_trend_confirmation=True, prev_close=None,
                      ema_fast_period=None, ema_slow_period=None, require_volume_increase=False,
                      require_macd_cross=False, require_rsi_confirmation=False):
    ema_fast_period = ema_fast_period if ema_fast_period is not None else config.EMA_FAST
    ema_slow_period = ema_slow_period if ema_slow_period is not None else config.EMA_SLOW

    curr = df.iloc[idx]
    prev = df.iloc[idx - 1]

    # Condition 1: true EMA9/EMA20 line crossover on this candle.
    bullish_cross = prev["ema_fast"] <= prev["ema_slow"] and curr["ema_fast"] > curr["ema_slow"]
    bearish_cross = prev["ema_fast"] >= prev["ema_slow"] and curr["ema_fast"] < curr["ema_slow"]

    if not (bullish_cross or bearish_cross):
        return None

    direction = "BULLISH" if bullish_cross else "BEARISH"
    bullish = bullish_cross
    close_price = float(curr["close"])

    # Condition 2: cross direction must agree with the broader trend
    # (EMA50) — a bullish cross only fires in an uptrend, a bearish
    # cross only fires in a downtrend. This is MANDATORY for stocks and
    # commodities, but SKIPPED for indices (require_trend_confirmation
    # =False) — for indices we want every qualifying crossover alerted
    # regardless of the EMA50 trend. stock_trend is still computed and
    # shown in the message either way, purely informational when the
    # condition isn't enforced.
    stock_trend = "BULLISH" if curr["close"] > curr["ema_trend"] else "BEARISH"
    if require_trend_confirmation and direction != stock_trend:
        return None

    # Volume — MANDATORY when require_volume_increase=True (currently
    # only the 75-min stock/F&O/cash/commodity alerts — see
    # check_signals/main.py): the crossing candle's volume must be
    # strictly higher than the previous candle's, or the signal is
    # rejected outright. If prev_vol is 0/unavailable, the comparison
    # can't be made — the alert still fires rather than being blocked
    # on missing data, same as before. When require_volume_increase is
    # False (indices, and everywhere else by default), volume stays
    # informational-only, exactly as before.
    curr_vol = curr["volume"]
    prev_vol = prev["volume"]
    vol_change_pct = round(((curr_vol - prev_vol) / prev_vol) * 100, 1) if prev_vol else None
    if require_volume_increase and prev_vol and curr_vol <= prev_vol:
        return None
    rsi_val = curr["rsi"] if not pd_isna(curr["rsi"]) else None

    # RSI condition — MANDATORY when require_rsi_confirmation=True
    # (added, per request): RSI(14) > 50 for a BULLISH cross, < 50 for
    # a BEARISH cross, or the signal is rejected outright. If rsi_val
    # is unavailable (not enough history yet), the alert still fires
    # rather than being blocked on missing data — same
    # missing-data-never-blocks philosophy as require_volume_increase
    # above.
    if require_rsi_confirmation and rsi_val is not None:
        if bullish and rsi_val <= 50:
            return None
        if not bullish and rsi_val >= 50:
            return None

    cross_pattern = detect_candle_pattern(curr)
    prev_pattern = detect_candle_pattern(prev)

    # Informational trade-plan fields — never affect whether a signal
    # fires. Stop Loss = EMA20 on the cross candle. Target = the
    # trailing 15-min (config.TARGET_LOOKBACK_CANDLES candles) high for
    # a bullish entry, or the trailing 15-min low for a bearish entry.
    window_start = max(0, idx - (config.TARGET_LOOKBACK_CANDLES - 1))
    window = df.iloc[window_start:idx + 1]
    if bullish:
        target = float(window["high"].max())
    else:
        target = float(window["low"].min())
    stop_loss = float(curr["ema_slow"])

    # Risk:Reward, expressed as reward per 1 unit of risk (e.g. 2.35
    # means "1 : 2.35"). None if risk is 0 (entry == stop loss), which
    # can happen right at the cross when EMA20 == close.
    risk = abs(close_price - stop_loss)
    reward = abs(target - close_price)
    risk_reward = round(reward / risk, 2) if risk > 0 else None

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

    # VWAP — purely informational. Flags whether price is trading above
    # or below the session VWAP at the moment of the cross, and by what
    # %. None if volume data is unusable (cum_vol == 0), in which case
    # the alert simply omits the VWAP line.
    vwap = _compute_vwap_at(df, idx)
    vwap_note = None
    if vwap is not None and vwap > 0:
        vwap_diff_pct = round((close_price - vwap) / vwap * 100, 2)
        position = "Above VWAP" if close_price >= vwap else "Below VWAP"
        vwap_note = f"{position} ({vwap_diff_pct:+.2f}%)"

    # MACD — informational only. macd_line/signal/hist are shown as raw
    # values; macd_divergence flags a bullish/bearish divergence within
    # the trailing config.MACD_DIVERGENCE_LOOKBACK_CANDLES window, if
    # any (see _detect_macd_divergence). Never blocks a signal.
    macd_div = _detect_macd_divergence(df, idx, config.MACD_DIVERGENCE_LOOKBACK_CANDLES)
    macd_divergence_note = None
    if macd_div["bullish"]:
        macd_divergence_note = "Bullish Divergence (price lower low, MACD higher low)"
    elif macd_div["bearish"]:
        macd_divergence_note = "Bearish Divergence (price higher high, MACD lower high)"

    # MACD cross recency (added, per request) — used to REQUIRE the
    # MACD condition (Trade Score dimension #11 and the checklist's
    # "MACD bullish/bearish" item) to be a genuinely recent crossover,
    # not just today's current level. See _detect_macd_cross_recent.
    macd_cross_recent = _detect_macd_cross_recent(df, idx, config.MACD_DIVERGENCE_LOOKBACK_CANDLES)

    # MACD condition — MANDATORY when require_macd_cross=True (added,
    # per request): the LATEST MACD line/signal crossover within the
    # lookback window must match this candle's direction (macd_cross_
    # recent == direction), or the signal is rejected outright. UNLIKE
    # require_rsi_confirmation/require_volume_increase above, a None
    # here (no recent MACD crossover at all in the window) DOES block
    # when this is required -- "no recent cross" genuinely fails a
    # "must have a recent cross" requirement, it isn't missing/
    # unusable data the way an unavailable RSI reading is.
    if require_macd_cross and macd_cross_recent != direction:
        return None


    # Day change % — purely informational, vs previous trading day's
    # close (same prev-day close already fetched for R3/S3). None if
    # prev_close wasn't available (e.g. pivot fetch failed for this
    # symbol that day) — the alert simply omits this line then.
    day_change_pct = None
    if prev_close:
        day_change_pct = round((close_price - prev_close) / prev_close * 100, 2)

    # Opening range breakout (added, per request) — checklist item
    # "1st 15-min High/Low breakout + close beyond it". Purely
    # informational here, like trendline_break below; never blocks the
    # EMA-cross signal itself.
    opening_range_breakout = _compute_opening_range_breakout(df, idx)

    daily_score = compute_daily_score(curr, prev, vwap, close_price)

    # Trendline break (added, per request) — informational only here;
    # this is the SAME detect_trendline_break used by the standalone
    # check_trendline_scan, just also surfaced as an extra line when it
    # happens to coincide with this EMA-cross candle. Does not require
    # a break to have happened for the EMA-cross alert itself to fire.
    trendline_break = detect_trendline_break(df, idx)

    return {
        "symbol": symbol,
        "direction": direction,
        "close": round(close_price, 2),
        "entry": round(close_price, 2),
        "stop_loss": round(stop_loss, 2),
        "target": round(target, 2),
        "risk_reward": risk_reward,
        "rsi": round(float(rsi_val), 1) if rsi_val is not None else None,
        "volume": int(curr["volume"]),
        "vol_avg": round(float(curr["vol_avg"]), 0) if not pd_isna(curr["vol_avg"]) else None,
        "vol_change_pct": vol_change_pct,
        "ema_fast": round(float(curr["ema_fast"]), 2),
        "ema_slow": round(float(curr["ema_slow"]), 2),
        "ema_fast_period": ema_fast_period,
        "ema_slow_period": ema_slow_period,
        "ema_trend": round(float(curr["ema_trend"]), 2),
        "stock_trend": stock_trend,
        "candle_time": str(curr.get("timestamp", "")),
        "cross_candle_pattern": cross_pattern,
        "prev_candle_pattern": prev_pattern,
        "r3": r3,
        "s3": s3,
        "pivot_note": pivot_note,
        "prev_close": round(float(prev_close), 2) if prev_close else None,
        "day_change_pct": day_change_pct,
        "vwap": round(vwap, 2) if vwap is not None else None,
        "vwap_note": vwap_note,
        "macd_line": round(float(curr["macd_line"]), 2),
        "macd_signal": round(float(curr["macd_signal"]), 2),
        "macd_hist": round(float(curr["macd_hist"]), 2),
        "macd_divergence": macd_divergence_note,
        "macd_cross_recent": macd_cross_recent,
        "daily_score": daily_score,
        "trendline_break": trendline_break,
        "opening_range_breakout": opening_range_breakout,
    }


def check_signals(df, symbol, r3=None, s3=None, lookback=None, require_trend_confirmation=True, prev_close=None,
                   ema_fast=None, ema_slow=None, require_volume_increase=False, require_strong_candle=False,
                   require_macd_cross=False, require_rsi_confirmation=False):
    """
    Scans the last `lookback` closed candles (default:
    config.CROSS_LOOKBACK_CANDLES) for EMA crossovers — not just the
    single latest candle — so a run that was skipped/delayed still
    catches up on any cross it would otherwise have missed.

    ema_fast/ema_slow: the EMA periods to use for the crossover +
    labeling (defaults to config.EMA_FAST/EMA_SLOW, i.e. 9/20 — the F&O
    scan's periods). Pass config.NIFTY500_EMA_FAST/EMA_SLOW (9/21) for
    the Nifty 500 cash-stock scan. EMA50 trend-agreement, RSI, MACD etc.
    are unaffected — only the crossover pair itself changes.

    require_trend_confirmation=False disables condition 4 (EMA50 trend
    agreement) — used for indices, where every qualifying crossover
    should alert regardless of the broader trend. Leave True (default)
    for stocks/commodities.

    require_volume_increase=True makes the crossing candle's volume >
    previous candle's volume a MANDATORY condition (signal rejected
    otherwise) — used for the 75-min stock/F&O/cash/commodity alerts
    (see main.py). Leave False (default) for indices, where volume
    stays informational-only.

    require_strong_candle: accepted for call-site compatibility with
    main.py (config.REQUIRE_STRONG_CANDLE), but currently a no-op —
    the strong-candle-body filter was removed from _evaluate_candle
    (see this module's docstring: "There is no longer a strong-candle
    filter"). Kept as a parameter rather than removed so main.py can
    keep passing it without a signature error; has no effect either
    way right now.

    require_macd_cross=True (added, per request) makes a RECENT MACD
    line/signal crossover (within config.MACD_DIVERGENCE_LOOKBACK_
    CANDLES, matching this candle's direction) MANDATORY — see
    _detect_macd_cross_recent. Unlike a plain "MACD > Signal" level
    check, this requires an actual crossover to have happened
    recently, not just the current bias.

    require_rsi_confirmation=True (added, per request) makes RSI(14)
    > 50 (bullish) / < 50 (bearish) MANDATORY.

    Returns a list of signal dicts, oldest candle first. Empty list if
    nothing qualifies. Caller is responsible for de-duping against
    already-alerted (symbol, direction, candle_time) combos (see
    state.py) before sending each one.
    """
    lookback = lookback or config.CROSS_LOOKBACK_CANDLES
    ema_fast = ema_fast if ema_fast is not None else config.EMA_FAST
    ema_slow = ema_slow if ema_slow is not None else config.EMA_SLOW

    if len(df) < _min_required_len(1, ema_slow):
        return []

    # Shrink the lookback window if we don't have enough history yet
    # (e.g. right after market open) rather than returning nothing.
    max_possible_lookback = len(df) - _min_required_len(0, ema_slow)
    lookback = max(1, min(lookback, max_possible_lookback))

    df = add_emas(df, ema_fast, ema_slow)
    df = add_rsi(df, config.RSI_PERIOD)
    df = add_volume_avg(df, config.VOLUME_AVG_PERIOD)
    df = add_ema50(df)
    df = add_macd(df, config.MACD_FAST, config.MACD_SLOW, config.MACD_SIGNAL)
    # Fixed EMA9/EMA21 for the Daily Score (see compute_daily_score) —
    # kept separate from ema_fast/ema_slow above because those vary by
    # scan (9/20 on F&O, 9/50 on Nifty 500), while the Daily Score's
    # own EMA9 > EMA21 > EMA50 stack must always stay 9/21/50.
    df = add_ema(df, 9, "ds_ema9")
    df = add_ema(df, 21, "ds_ema21")

    signals = []
    n = len(df)
    start = max(1, n - lookback)
    for idx in range(start, n):
        sig = _evaluate_candle(
            df, idx, symbol, r3, s3,
            require_trend_confirmation=require_trend_confirmation,
            prev_close=prev_close,
            ema_fast_period=ema_fast,
            ema_slow_period=ema_slow,
            require_volume_increase=require_volume_increase,
            require_macd_cross=require_macd_cross,
            require_rsi_confirmation=require_rsi_confirmation,
        )
        if sig is not None:
            signals.append(sig)
    return signals


def check_signal(df, symbol, r3=None, s3=None):
    """
    Backward-compatible single-signal wrapper: returns only the most
    recent qualifying signal (or None), same as the original behavior.
    Prefer check_signals() in new code so missed/delayed runs still
    catch up on earlier crosses in the lookback window.
    """
    signals = check_signals(df, symbol, r3=r3, s3=s3, lookback=1)
    return signals[-1] if signals else None


def debug_ema_gap(df, symbol, ema_fast=None, ema_slow=None):
    """
    Debug helper only — NOT used in the firing decision. Returns how far
    apart EMA9/EMA20 are on the latest closed candle, as a % of price, so
    you can see in the Action logs which instruments are "close" to a
    cross even when none has fired yet.
    """
    ema_fast_period = ema_fast if ema_fast is not None else config.EMA_FAST
    ema_slow_period = ema_slow if ema_slow is not None else config.EMA_SLOW

    if len(df) < ema_slow_period + 2:
        return None

    df = add_emas(df, ema_fast_period, ema_slow_period)
    curr = df.iloc[-1]

    ema_fast_val = float(curr["ema_fast"])
    ema_slow_val = float(curr["ema_slow"])
    close_price = float(curr["close"])
    gap_pct = abs(ema_fast_val - ema_slow_val) / close_price * 100

    return {
        "symbol": symbol,
        "ema_fast": round(ema_fast_val, 2),
        "ema_slow": round(ema_slow_val, 2),
        "gap_pct": round(gap_pct, 3),
        "leaning": f"BULLISH (EMA{ema_fast_period} above)" if ema_fast_val > ema_slow_val else f"BEARISH (EMA{ema_fast_period} below)",
    }


def get_3min_trend_info(df_3min, symbol, lookback_candles=None, ema_fast=None, ema_slow=None):
    """
    Informative-only check on 3-min candles — no filters (no strong
    candle, no volume, no trend-agreement, no gap threshold). Reports:
      - bias: which side EMA9 is currently on relative to EMA20
      - candles_since_cross: how many 3-min candles ago EMA9/20 last
        crossed, within lookback_candles (None = no cross in that
        window — i.e. the 3-min chart has NOT crossed recently)
      - gap_pct: how close EMA9/EMA20 currently are to crossing, as a
        % of close price (0 = touching/about to cross; bigger = further
        from a cross)
    Attached as context to a 75-min alert (the PRIMARY/ALERTING
    timeframe — see check_signals above). Never blocks or fires its own
    alert.

    Returns None if there isn't enough 3-min history yet.
    """
    ema_fast_period = ema_fast if ema_fast is not None else config.EMA_FAST
    ema_slow_period = ema_slow if ema_slow is not None else config.EMA_SLOW

    if len(df_3min) < ema_slow_period + 2:
        return None

    lookback_candles = lookback_candles or config.INFO_3MIN_LOOKBACK_CANDLES

    df = add_emas(df_3min, ema_fast_period, ema_slow_period)
    n = len(df)

    curr = df.iloc[-1]
    bias = "BULLISH" if curr["ema_fast"] > curr["ema_slow"] else "BEARISH"
    close_price = float(curr["close"])
    gap_pct = round(abs(float(curr["ema_fast"]) - float(curr["ema_slow"])) / close_price * 100, 3) if close_price else None

    candles_since_cross = None
    cross_time = None
    start = max(1, n - lookback_candles)
    for idx in range(n - 1, start - 1, -1):
        prev = df.iloc[idx - 1]
        cur = df.iloc[idx]
        bull_cross = prev["ema_fast"] <= prev["ema_slow"] and cur["ema_fast"] > cur["ema_slow"]
        bear_cross = prev["ema_fast"] >= prev["ema_slow"] and cur["ema_fast"] < cur["ema_slow"]
        if bull_cross or bear_cross:
            candles_since_cross = (n - 1) - idx
            # FIXED: this used to read df.index[idx], but after
            # resample_*min() the dataframe index is just a plain
            # 0..n row counter (reset_index()) -- the real timestamp
            # lives in the "timestamp" COLUMN. df.index[idx] was
            # silently returning an integer row number instead of a
            # real date/time, which then crashed inside
            # telegram_notifier.py's date-formatting (str(3).split(" ")
            # has no [1] element -> IndexError) -- silently swallowed
            # by the try/except around send_alert in main.py, so the
            # WHOLE alert would just quietly fail to send whenever this
            # 15-min context block had a recent cross. Reading
            # df.iloc[idx]["timestamp"] instead fixes both the display
            # and this silent-drop bug.
            cross_time = cur["timestamp"]
            break

    return {
        "symbol": symbol,
        "bias": bias,
        "ema_fast": round(float(curr["ema_fast"]), 2),
        "ema_slow": round(float(curr["ema_slow"]), 2),
        "ema_fast_period": ema_fast_period,
        "ema_slow_period": ema_slow_period,
        "gap_pct": gap_pct,  # how close to a cross right now, on 75-min
        "candles_since_cross": candles_since_cross,  # None = no cross in lookback window
        "cross_time": cross_time,  # exact 75-min candle timestamp of the cross, or None
    }


def get_opening_candle_bias(df, symbol):
    """
    Opening 15-min candle bias (added, per request). Looks at the
    FIRST 15-min candle of TODAY's session (the 09:15 candle) in df
    (expects df15 — the 15-min resampled data). Compares that candle's
    open/low/high with a small tolerance (config.OPENING_CANDLE_EPSILON,
    default 0.01) instead of exact float equality, since real market
    data can carry tiny floating-point noise even on a genuinely equal
    tick:
      - Open == Low  -> "BULLISH" (price never traded below the open
        all through the first 15 minutes — no selling below open)
      - Open == High -> "BEARISH" (price never traded above the open
        — no buying above open)
      - Neither (the normal case — price moved both above and below
        open in the first 15 min) -> None, caller omits the line
    Returns None if config.OPENING_CANDLE_BIAS_ENABLED is False, if df
    is empty, or if today's first 15-min candle isn't in df yet (e.g.
    called before/right at market open before that first candle has
    actually closed).
    """
    if not config.OPENING_CANDLE_BIAS_ENABLED:
        return None
    if df is None or df.empty:
        return None
    ts = df["timestamp"]
    today = ts.iloc[-1].date()
    today_rows = df[ts.dt.date == today]
    if today_rows.empty:
        return None
    first = today_rows.iloc[0]
    o = float(first["open"])
    h = float(first["high"])
    l = float(first["low"])
    eps = config.OPENING_CANDLE_EPSILON
    open_eq_low = abs(o - l) <= eps
    open_eq_high = abs(o - h) <= eps
    if open_eq_low and not open_eq_high:
        return "BULLISH"
    if open_eq_high and not open_eq_low:
        return "BEARISH"
    return None


def get_opening_candle_buy_sell_estimate(df, symbol):
    """
    1st 15-min candle Buy/Sell volume ESTIMATE (added, per request).
    NOT real order-flow/tick data (Upstox's historical candle API
    doesn't expose who initiated each trade) -- this is the standard
    Chaikin-style approximation from the candle's own OHLC + volume:

        buy_volume  = volume * (close - low) / (high - low)
        sell_volume = volume - buy_volume

    i.e. a close near the candle's HIGH is read as mostly buy-side
    pressure, a close near the LOW as mostly sell-side. Same
    "today's first 15-min candle" lookup as get_opening_candle_bias
    above (expects df15). Returns None if df is empty, today's first
    candle isn't in df yet, or the candle has zero range (high==low,
    e.g. a single-tick/illiquid candle) -- division by zero would
    otherwise make the split meaningless. On success returns a dict:
    {"buy_volume": int, "sell_volume": int, "buy_pct": float} (buy_pct
    rounded to 1 decimal, 0-100).
    """
    if df is None or df.empty:
        return None
    ts = df["timestamp"]
    today = ts.iloc[-1].date()
    today_rows = df[ts.dt.date == today]
    if today_rows.empty:
        return None
    first = today_rows.iloc[0]
    o, h, l, c = float(first["open"]), float(first["high"]), float(first["low"]), float(first["close"])
    volume = float(first["volume"])
    candle_range = h - l
    if candle_range <= 0 or volume <= 0:
        return None
    buy_volume = volume * (c - l) / candle_range
    sell_volume = volume - buy_volume
    return {
        "buy_volume": int(round(buy_volume)),
        "sell_volume": int(round(sell_volume)),
        "buy_pct": round((buy_volume / volume) * 100, 1),
    }


def get_sector_trend(df):
    """
    Sector-index trend reading (added) — same rule the stock-level
    trend uses (stock_trend in _evaluate_candle): close above EMA50 =
    UPTREND, close below = DOWNTREND. No cross logic, no filters, just
    the current trend on the sector index's own 3-min data. Meant to be
    called on a sector index's df3 (today's 1-min resampled to 3-min,
    same as any stock) and attached to a related stock's alert in
    main.py via config.STOCK_SECTOR_MAP.

    Returns None if there isn't enough 3-min history yet today to warm
    up EMA50 (same ~50-candle / ~150-minute-into-session constraint the
    main per-stock signal already has) — the caller treats None as
    "not available yet", never an error.
    """
    if len(df) < 51:
        return None
    df = add_ema50(df)
    curr = df.iloc[-1]
    return "UPTREND" if curr["close"] > curr["ema_trend"] else "DOWNTREND"


def check_signals_15min(df_15min, symbol, lookback=None):
    """
    STANDALONE 15-min alert — pure EMA9/EMA20 crossover on the 15-min
    chart. Same design as the retired standalone 75-min alert used to
    be: no strong-candle filter, no EMA50 trend-agreement requirement,
    no min-gap filter. Intended for config.COMMODITIES only (see
    main.py — the caller filters by symbol), so it is not restricted
    here. Fires independently of check_signals(). Deduped separately
    in main.py with state key namespaced "::15m".

    Uses the same catch-up-window approach: scans the last `lookback`
    closed 15-min candles (default: config.CROSS_LOOKBACK_CANDLES), not
    just the latest one.

    Returns a list of signal dicts (oldest first), each tagged
    timeframe="15-min". Empty list if nothing qualifies or there isn't
    enough 15-min history yet.
    """
    lookback = lookback or config.CROSS_LOOKBACK_CANDLES

    if len(df_15min) < config.EMA_SLOW + 2:
        return []

    max_possible_lookback = len(df_15min) - (config.EMA_SLOW + 1)
    lookback = max(1, min(lookback, max_possible_lookback))

    df = add_emas(df_15min, config.EMA_FAST, config.EMA_SLOW)
    df = add_rsi(df, config.RSI_PERIOD)
    df = add_volume_avg(df, config.VOLUME_AVG_PERIOD)

    signals = []
    n = len(df)
    start = max(1, n - lookback)
    for idx in range(start, n):
        curr = df.iloc[idx]
        prev = df.iloc[idx - 1]

        bullish_cross = prev["ema_fast"] <= prev["ema_slow"] and curr["ema_fast"] > curr["ema_slow"]
        bearish_cross = prev["ema_fast"] >= prev["ema_slow"] and curr["ema_fast"] < curr["ema_slow"]

        if not (bullish_cross or bearish_cross):
            continue

        direction = "BULLISH" if bullish_cross else "BEARISH"

        curr_vol = curr["volume"]
        prev_vol = prev["volume"]
        vol_change_pct = round(((curr_vol - prev_vol) / prev_vol) * 100, 1) if prev_vol else None
        rsi_val = curr["rsi"] if not pd_isna(curr["rsi"]) else None

        cross_pattern = detect_candle_pattern(curr)
        prev_pattern = detect_candle_pattern(prev)

        signals.append({
            "symbol": symbol,
            "timeframe": "15-min",
            "direction": direction,
            "close": round(float(curr["close"]), 2),
            "rsi": round(float(rsi_val), 1) if rsi_val is not None else None,
            "volume": int(curr["volume"]),
            "vol_avg": round(float(curr["vol_avg"]), 0) if not pd_isna(curr["vol_avg"]) else None,
            "vol_change_pct": vol_change_pct,
            "ema_fast": round(float(curr["ema_fast"]), 2),
            "ema_slow": round(float(curr["ema_slow"]), 2),
            "candle_time": str(curr.get("timestamp", "")),
            "cross_candle_pattern": cross_pattern,
            "prev_candle_pattern": prev_pattern,
        })

    return signals


def passes_confluence_filter(signal):
    """
    Secondary quality filter layered ONLY on top of an already-
    qualifying 75-min EMA cross signal (stocks/commodities/Nifty 500 —
    never applied to index or VWAP-momentum alerts, which have their
    own separate rules). Combines fields the signal ALREADY carries
    (entry/stop_loss/target/risk_reward from _evaluate_candle, plus
    sector_trend/oi_buildup attached later in main.py) into a single
    "is this a genuinely good risk:reward setup" gate — no new
    indicator, no extra API call:

      1. risk_reward (entry vs the EMA-based stop vs the trailing
         config.TARGET_LOOKBACK_CANDLES high/low target) must be at
         least config.CONFLUENCE_MIN_RISK_REWARD — this is what keeps
         the stop small relative to the target.
      2. RSI must sit in a "trending but not yet exhausted" band —
         BULLISH: config.CONFLUENCE_RSI_BULLISH_MIN..MAX (skips a
         cross that's already overbought and due a pullback); BEARISH:
         config.CONFLUENCE_RSI_BEARISH_MIN..MAX (mirror — skips
         shorting into an already-oversold bounce).
      3. sector_trend (if the stock has a sector mapping — see
         config.STOCK_SECTOR_MAP) must agree with the signal
         direction. Stocks/instruments with no sector mapping, or no
         sector reading yet, simply skip this check (neither pass nor
         fail on it).
      4. oi_buildup bias (F&O stocks only, fetched on-demand in
         main.py — see get_stock_oi_buildup) must not OPPOSE the
         direction — a BEARISH (call-writing-dominant) buildup blocks
         a BULLISH signal and vice versa. NEUTRAL, or no buildup
         reading yet, both pass.

    Returns True/False. Only ever called on a signal that has ALREADY
    passed check_signals()'s own mandatory conditions (cross + EMA50
    trend agreement + volume increase) — this is an additional,
    optional layer on top, controlled by config.CONFLUENCE_FILTER_ENABLED.
    """
    risk_reward = signal.get("risk_reward")
    if config.CONFLUENCE_CHECK_RISK_REWARD:
        if risk_reward is None or risk_reward < config.CONFLUENCE_MIN_RISK_REWARD:
            return False

    rsi_val = signal.get("rsi")
    if config.CONFLUENCE_CHECK_RSI_BAND:
        if rsi_val is None:
            return False
        if signal["direction"] == "BULLISH":
            if not (config.CONFLUENCE_RSI_BULLISH_MIN <= rsi_val <= config.CONFLUENCE_RSI_BULLISH_MAX):
                return False
        else:
            if not (config.CONFLUENCE_RSI_BEARISH_MIN <= rsi_val <= config.CONFLUENCE_RSI_BEARISH_MAX):
                return False

    sector_trend = signal.get("sector_trend")
    if config.CONFLUENCE_CHECK_SECTOR_TREND and sector_trend is not None:
        expected = "UPTREND" if signal["direction"] == "BULLISH" else "DOWNTREND"
        if sector_trend != expected:
            return False

    oi_buildup = signal.get("oi_buildup")
    if config.CONFLUENCE_CHECK_OI_BUILDUP and oi_buildup is not None:
        bias = oi_buildup.get("bias")
        opposing = "BEARISH" if signal["direction"] == "BULLISH" else "BULLISH"
        if bias == opposing:
            return False

    return True


def compute_smart_money_signal(signal):
    """
    "Smart Money Entry" 🐋 — PURELY INFORMATIONAL, added per request
    (and made non-blocking from the start, matching the rest of the
    codebase's now-informational-everywhere style — OI buildup itself
    was also switched from blocking to informational, see
    CONFLUENCE_CHECK_OI_BUILDUP). Never stops an alert from being
    sent; only adds a tag + reasons line when several independent
    signs line up. Built entirely from fields the signal ALREADY
    carries by the time this is called (see main.py — delivery%,
    Bulk/Block deal, sector/OI, Momentum, Volume Spike, and EMA50/200
    are all attached before this runs) — no new fetch, no extra API
    call.

    Up to 9 points, 1 each:
      1. OI buildup bias agrees with the signal direction (F&O stocks
         only — cash-only Nifty 500 scan never sets this, so it's
         simply skipped there, same as every other missing field below)
      2. Crossing candle's volume beat the previous candle's
         (vol_change_pct > 0)
      3. delivery_pct >= config.SMART_MONEY_DELIVERY_THRESHOLD (prev
         trading day's NSE delivery %, not a per-stock trailing
         average)
      4. VWAP cushion: BULLISH needs Close >= VWAP by at least
         config.SMART_MONEY_VWAP_MIN_PCT; BEARISH the mirror
      5. A same-direction Bulk/Block deal (BUY for BULLISH, SELL for
         BEARISH) dated within config.SMART_MONEY_DEAL_LOOKBACK_DAYS
         calendar days of the signal's candle_time
      6. cross_candle_pattern is a Marubozu matching the direction
      7. Momentum: signal["momentum"] is True (today's close above the
         trailing 4-week high — see main.py's build_momentum_volume_data)
      8. Volume Spike: signal["volume_spike"] is True (yesterday's
         volume beat the volume from 5 trading days before that)
      9. EMA50/200 (daily) bias agrees with the signal direction —
         signal["ema_cross"]["bias"]

    Returns None if fewer than config.SMART_MONEY_MIN_SCORE points are
    scored (out of however many of the 9 dimensions actually had data
    available this run — missing fields are skipped, not counted
    against it); otherwise {"score": int, "possible": int,
    "reasons": [str, ...]}.
    """
    direction = signal["direction"]
    score = 0
    possible = 0
    reasons = []

    # ---- 1. OI buildup direction ----
    oi_buildup = signal.get("oi_buildup")
    if oi_buildup:
        possible += 1
        if oi_buildup.get("bias") == direction:
            score += 1
            reasons.append(f"OI buildup: {direction.title()}")

    # ---- 2. volume beat previous candle ----
    vol_change_pct = signal.get("vol_change_pct")
    if vol_change_pct is not None:
        possible += 1
        if vol_change_pct > 0:
            score += 1
            reasons.append(f"Volume up {vol_change_pct:+.1f}% vs previous candle")

    # ---- 3. delivery % ----
    deliv = signal.get("delivery_pct")
    if deliv is not None:
        possible += 1
        if deliv >= config.SMART_MONEY_DELIVERY_THRESHOLD:
            score += 1
            reasons.append(f"Delivery {deliv:.1f}% (>={config.SMART_MONEY_DELIVERY_THRESHOLD:.0f}%)")

    # ---- 4. VWAP cushion ----
    vwap = signal.get("vwap")
    close = signal.get("close")
    if vwap and close:
        possible += 1
        gap_pct = (close - vwap) / vwap * 100
        if direction == "BULLISH" and gap_pct >= config.SMART_MONEY_VWAP_MIN_PCT:
            score += 1
            reasons.append(f"Above VWAP by {gap_pct:+.2f}%")
        elif direction == "BEARISH" and gap_pct <= -config.SMART_MONEY_VWAP_MIN_PCT:
            score += 1
            reasons.append(f"Below VWAP by {gap_pct:+.2f}%")

    # ---- 5. same-direction Bulk/Block deal, recent ----
    deal = signal.get("last_bulk_block_deal")
    if deal and deal.get("date") and signal.get("candle_time"):
        possible += 1
        expected_side = "BUY" if direction == "BULLISH" else "SELL"
        if (deal.get("buy_sell") or "").upper() == expected_side:
            deal_dt = _parse_deal_date(deal["date"])
            candle_dt = _parse_candle_time(signal["candle_time"])
            if deal_dt is not None and candle_dt is not None:
                if 0 <= (candle_dt.date() - deal_dt.date()).days <= config.SMART_MONEY_DEAL_LOOKBACK_DAYS:
                    score += 1
                    reasons.append(f"{deal['type']} deal ({expected_side}) on {deal['date']}")

    # ---- 6. strong matching Marubozu ----
    pattern = signal.get("cross_candle_pattern")
    if pattern:
        possible += 1
        expected_pattern = "Bullish Marubozu" if direction == "BULLISH" else "Bearish Marubozu"
        if pattern == expected_pattern:
            score += 1
            reasons.append(f"Crossing candle: {pattern}")

    # ---- 7. Momentum (4-week high) ----
    momentum = signal.get("momentum")
    if momentum is not None:
        possible += 1
        if momentum:
            score += 1
            reasons.append(f"Momentum: above 4-week high ({signal.get('four_week_high_close')})")

    # ---- 8. Volume Spike (vs 5 trading days ago) ----
    volume_spike = signal.get("volume_spike")
    if volume_spike is not None:
        possible += 1
        if volume_spike:
            score += 1
            reasons.append("Volume spike: prev day vol > 5-day-ago vol")

    # ---- 9. EMA50/200 (daily) bias ----
    ema_cross = signal.get("ema_cross")
    if ema_cross:
        possible += 1
        if ema_cross.get("bias") == direction:
            score += 1
            reasons.append(f"EMA50/200 (daily): {ema_cross['bias'].title()}")

    if score < config.SMART_MONEY_MIN_SCORE:
        return None

    return {"score": score, "possible": possible, "reasons": reasons}


def _parse_deal_date(date_str):
    for fmt in ("%d-%b-%Y", "%d-%B-%Y"):
        try:
            return _dt.datetime.strptime(date_str.strip(), fmt)
        except Exception:
            continue
    return None


def _parse_candle_time(candle_time_str):
    # candle_time is str(pandas_timestamp), e.g. "2026-08-15 13:00:00"
    # or "2026-08-15 13:00:00+05:30" -- take just the date portion,
    # first 10 chars, which is always "YYYY-MM-DD" regardless of
    # whatever comes after.
    try:
        return _dt.datetime.strptime(candle_time_str[:10], "%Y-%m-%d")
    except Exception:
        return None


def check_vwap_momentum(df5, symbol, lookback=None):
    """
    Standalone BUY-side momentum scan — replicates a Chartink "VWAP EMA
    9,20 RSI" screener (uploaded screenshot, 2026-08-11): on the 5-min
    chart of an F&O futures-segment stock, fires once when ALL of these
    newly become true on a closed 5-min candle:
      1. 5-min RSI(14) > config.VWAP_MOMENTUM_RSI_MIN
      2. Day's Close / Day's Open > config.VWAP_MOMENTUM_DAY_CHANGE_MIN
         (today's cumulative move up from the day's opening price)
      3. Day's VWAP < the 5-min candle's Close (price trading above the
         day's volume-weighted average price)
      4. Day's cumulative Volume >= config.VWAP_MOMENTUM_MIN_VOLUME
      5. Day's Close > config.VWAP_MOMENTUM_MIN_PRICE

    "Day's Open/Close/Volume/VWAP" are all computed cumulatively from
    df5 (Upstox intraday data = current session only, so row 0 already
    is the session's first 5-min bar — same assumption
    _compute_vwap_at already relies on elsewhere in this file).

    Transition-only firing: a candle only qualifies if ALL conditions
    are true on it AND at least one condition was false on the
    immediately preceding candle (or there's no prior candle in the
    lookback window to compare) — otherwise the scan would re-alert on
    every single run for as long as the stock stays in the qualifying
    zone. main.py's candle_time-keyed state dedup is a second safety
    net on top of this.

    Returns a list of signal dicts (oldest first), each tagged
    timeframe="5-min", direction="BULLISH" (this is a buy-only screen,
    matching the Chartink scan it replicates). Empty list if nothing
    qualifies or there isn't enough 5-min history yet today.
    """
    if df5 is None or len(df5) < config.VWAP_MOMENTUM_RSI_PERIOD + 2:
        return []

    lookback = lookback or config.INDEX_ALERT_LOOKBACK_CANDLES
    df = df5.copy()
    df = add_rsi(df, config.VWAP_MOMENTUM_RSI_PERIOD)

    day_open = float(df.iloc[0]["open"])
    if day_open <= 0:
        return []

    n = len(df)
    start = max(1, n - lookback)

    def _qualifies(idx):
        row = df.iloc[idx]
        rsi_val = row["rsi"]
        if pd_isna(rsi_val):
            return False
        day_close = float(row["close"])
        day_volume = float(df.iloc[: idx + 1]["volume"].sum())
        day_vwap = _compute_vwap_at(df, idx)
        if day_vwap is None:
            return False
        return (
            float(rsi_val) > config.VWAP_MOMENTUM_RSI_MIN
            and (day_close / day_open) > config.VWAP_MOMENTUM_DAY_CHANGE_MIN
            and day_vwap < day_close
            and day_volume >= config.VWAP_MOMENTUM_MIN_VOLUME
            and day_close > config.VWAP_MOMENTUM_MIN_PRICE
        )

    signals = []
    for idx in range(start, n):
        if not _qualifies(idx):
            continue
        if idx > 0 and _qualifies(idx - 1):
            continue  # already qualifying last candle too — not a fresh trigger

        curr = df.iloc[idx]
        rsi_val = float(curr["rsi"])
        day_close = float(curr["close"])
        day_volume = float(df.iloc[: idx + 1]["volume"].sum())
        day_vwap = _compute_vwap_at(df, idx)

        signals.append({
            "symbol": symbol,
            "timeframe": "5-min",
            "direction": "BULLISH",
            "scan_type": "vwap_momentum",
            "close": round(day_close, 2),
            "day_open": round(day_open, 2),
            "day_change_pct": round((day_close / day_open - 1) * 100, 2),
            "vwap": round(day_vwap, 2),
            "rsi": round(rsi_val, 1),
            "volume": int(day_volume),
            "candle_time": str(curr.get("timestamp", "")),
        })

    return signals


def pd_isna(val):
    import pandas as pd
    return pd.isna(val)


def compute_session_vwap(df):
    """
    Public wrapper around _compute_vwap_at for the LATEST row of `df`
    — the cumulative session VWAP as of the most recent candle. Used
    by run_breakout_scan (main.py) on today's full-day 5-min candles
    to get today's session VWAP for Row 13 ("Bulls in charge": Close >
    VWAP). Returns None if df is empty or has zero cumulative volume.
    """
    if df is None or len(df) == 0:
        return None
    return _compute_vwap_at(df, len(df) - 1)


def check_breakout_scan(history, today_bar, symbol):
    """
    12-condition daily breakout screener (Chartink-style scan supplied
    2026-08-18), Row 2 (Market Cap) omitted — no data source in this
    bot (see chat). ALL 12 must pass for a signal to fire:

      1.  Close > config.BREAKOUT_MIN_PRICE
      3.  Close * Volume > config.BREAKOUT_MIN_TURNOVER
      4.  Volume > MULTIPLIER * 20-day-avg-Volume AS OF YESTERDAY
      5.  Close > 50-day SMA (including today)
      6.  Close > 200-day SMA (including today)
      7.  RSI(14) > BREAKOUT_RSI_MIN
      8.  RSI(14) < BREAKOUT_RSI_MAX
      9.  Close >= PCT * 250-day-High AS OF YESTERDAY
      10. Close > 20-day-High AS OF YESTERDAY
      11. (10-day High - 10-day Low) < PCT * Close (including today)
      12. ATR(14) > PCT * Close (including today)
      13. Close > today's session VWAP

    "AS OF YESTERDAY" = computed over `history` alone (which already
    excludes today — see main.fetch_daily_history), matching the
    Chartink build's "offset: 1 day ago" gear-icon setting on those
    three terms. Every other rolling stat is computed over
    `history + [today_bar]` (i.e. including today), matching the rows
    that had NO offset specified.

    history: list of {"date","open","high","low","close","volume"},
    oldest -> newest, strictly BEFORE today (main.fetch_daily_history's
    output).
    today_bar: {"date","open","high","low","close","volume","vwap"} for
    today, built from today's own intraday session (see
    main.build_todays_daily_bar). vwap may be None if today's intraday
    fetch failed/is empty — Row 13 then just fails gracefully (no
    signal), same as any other missing-data case in this bot.

    Returns a signal dict (all 12 conditions' raw numbers included, for
    the alert message) if every condition passes, else None. Returns
    None immediately if there isn't enough daily history yet for the
    250-day lookback to be meaningful.
    """
    needed = max(config.BREAKOUT_NEAR_HIGH_LOOKBACK_DAYS, config.BREAKOUT_SMA_LONG_PERIOD)
    if len(history) < needed or today_bar is None:
        return None

    hist_df = pd.DataFrame(history)

    # ---- "AS OF YESTERDAY" stats (history only, today excluded) ----
    vol_sma20_yday = float(hist_df["volume"].iloc[-config.BREAKOUT_VOLUME_SMA_PERIOD:].mean())
    high_250_yday = float(hist_df["high"].iloc[-config.BREAKOUT_NEAR_HIGH_LOOKBACK_DAYS:].max())
    high_20_yday = float(hist_df["high"].iloc[-config.BREAKOUT_NEW_HIGH_LOOKBACK_DAYS:].max())

    # ---- "INCLUDING TODAY" stats (history + today_bar) ----
    combined = history + [today_bar]
    cdf = pd.DataFrame(combined)
    if len(cdf) < config.BREAKOUT_SMA_LONG_PERIOD:
        return None

    sma50 = float(cdf["close"].iloc[-config.BREAKOUT_SMA_SHORT_PERIOD:].mean())
    sma200 = float(cdf["close"].iloc[-config.BREAKOUT_SMA_LONG_PERIOD:].mean())

    cdf = add_rsi(cdf, config.BREAKOUT_RSI_PERIOD)
    rsi_today = cdf["rsi"].iloc[-1]

    cdf = add_atr(cdf, config.BREAKOUT_ATR_PERIOD)
    atr_today = cdf["atr"].iloc[-1]

    tight_window = cdf.iloc[-config.BREAKOUT_TIGHT_BASE_LOOKBACK_DAYS:]
    tight_base_range = float(tight_window["high"].max() - tight_window["low"].min())

    close_today = float(today_bar["close"])
    volume_today = float(today_bar["volume"])
    turnover_today = close_today * volume_today
    vwap_today = today_bar.get("vwap")

    if pd.isna(rsi_today) or pd.isna(atr_today):
        return None
    rsi_today = float(rsi_today)
    atr_today = float(atr_today)

    checks = {
        "price_floor":       close_today > config.BREAKOUT_MIN_PRICE,
        "turnover":          turnover_today > config.BREAKOUT_MIN_TURNOVER,
        "volume_spike":      vol_sma20_yday > 0 and volume_today > (config.BREAKOUT_VOLUME_SPIKE_MULTIPLIER * vol_sma20_yday),
        "above_sma50":       close_today > sma50,
        "above_sma200":      close_today > sma200,
        "rsi_floor":         rsi_today > config.BREAKOUT_RSI_MIN,
        "rsi_ceiling":       rsi_today < config.BREAKOUT_RSI_MAX,
        "near_52w_high":     high_250_yday > 0 and close_today >= (config.BREAKOUT_NEAR_HIGH_PCT * high_250_yday),
        "new_breakout_high": high_20_yday > 0 and close_today > high_20_yday,
        "tight_base":        tight_base_range < (config.BREAKOUT_TIGHT_BASE_MAX_RANGE_PCT * close_today),
        "enough_volatility": atr_today > (config.BREAKOUT_ATR_MIN_PCT * close_today),
        "above_vwap":        vwap_today is not None and close_today > vwap_today,
    }

    if not all(checks.values()):
        return None

    return {
        "symbol": symbol,
        "date": today_bar["date"],
        "close": round(close_today, 2),
        "volume": int(volume_today),
        "turnover_cr": round(turnover_today / 1e7, 1),
        "sma50": round(sma50, 2),
        "sma200": round(sma200, 2),
        "rsi": round(rsi_today, 1),
        "high_250d": round(high_250_yday, 2),
        "pct_of_52w_high": round((close_today / high_250_yday) * 100, 1),
        "high_20d": round(high_20_yday, 2),
        "tight_base_range_pct": round((tight_base_range / close_today) * 100, 2),
        "atr": round(atr_today, 2),
        "atr_pct": round((atr_today / close_today) * 100, 2),
        "vwap": round(vwap_today, 2),
        "checks": checks,
    }


def compute_intraday_checklist(signal):
    """
    "15-Minute Intraday Trade Checklist" (added, per request) — a
    fixed 0-10 point checklist, purpose-built for entry timing rather
    than overall alert quality (Trade Score, the earlier variable-
    denominator /10 rollup this once sat alongside, was removed per a
    later request — this checklist is the only score left on the
    alert).
    Mirrors automatically for BULLISH (BUY setup) vs BEARISH (SELL
    setup) signals -- same 8 checks, opposite direction each time:

      BUY SETUP (BULLISH)                    SELL SETUP (BEARISH)
      EMA9 > EMA20            +1             EMA9 < EMA20            +1
      Price > VWAP            +1             Price < VWAP            +1
      Crossing vol > prev     +1             Crossing vol > prev     +1
      Volume > recent avg     +1             Volume > recent avg     +1
      MACD bullish            +1             MACD bearish            +1
      RSI > 50                +1             RSI < 50                +1
      1st 15-min Open=Low     +2             1st 15-min Open=High    +2
      1st 15-min High breakout+2             1st 15-min Low breakdown+2

    This is NOT rescaled by how many
    dimensions had data -- every one of the 8 checks always counts
    (missing/unavailable data just means that check is simply not
    satisfied, i.e. 0 points for it, same as a real "no"), so the
    denominator is always a fixed /10 and directly comparable across
    every alert. This matches the checklist as specified: "how many
    points are on offer and how many were actually hit", with a hard
    10-point ceiling.

    Reads only fields the signal already carries (from _evaluate_candle
    and, for opening_candle_bias, from main.py) -- no extra fetch.
    Returns {"score": int (0-10), "label": "X/10", "items": [(name,
    checked_bool, points), ...]} -- "items" is the tickable checklist
    itself, in the same top-to-bottom order as specified, for
    telegram_notifier to render as [ ]/[x] lines if desired.
    """
    direction = signal["direction"]
    bullish = direction == "BULLISH"

    ema_fast, ema_slow = signal.get("ema_fast"), signal.get("ema_slow")
    ema_ok = ema_fast is not None and ema_slow is not None and (
        ema_fast > ema_slow if bullish else ema_fast < ema_slow
    )

    close, vwap = signal.get("close"), signal.get("vwap")
    vwap_ok = close is not None and vwap is not None and (
        close > vwap if bullish else close < vwap
    )

    vol_change_pct = signal.get("vol_change_pct")
    vol_vs_prev_ok = vol_change_pct is not None and vol_change_pct > 0

    volume, vol_avg = signal.get("volume"), signal.get("vol_avg")
    vol_vs_avg_ok = volume is not None and vol_avg is not None and volume > vol_avg

    # CHANGED (per request): must be a genuinely RECENT MACD line/
    # signal crossover within config.MACD_DIVERGENCE_LOOKBACK_CANDLES,
    # not just today's current MACD level — see
    # strategy._detect_macd_cross_recent.
    macd_cross_recent = signal.get("macd_cross_recent")
    macd_ok = macd_cross_recent == ("BULLISH" if bullish else "BEARISH")

    rsi = signal.get("rsi")
    rsi_ok = rsi is not None and (rsi > 50 if bullish else rsi < 50)

    opening_bias = signal.get("opening_candle_bias")
    opening_bias_ok = opening_bias == ("BULLISH" if bullish else "BEARISH")

    opening_breakout = signal.get("opening_range_breakout")
    opening_breakout_ok = opening_breakout == ("BULLISH" if bullish else "BEARISH")

    items = [
        ("EMA9 > EMA20" if bullish else "EMA9 < EMA20", ema_ok, 1),
        ("Price > VWAP" if bullish else "Price < VWAP", vwap_ok, 1),
        ("Crossing candle volume > previous candle", vol_vs_prev_ok, 1),
        ("Volume clearly above recent average", vol_vs_avg_ok, 1),
        ("MACD bullish cross (recent)" if bullish else "MACD bearish cross (recent)", macd_ok, 1),
        ("RSI > 50" if bullish else "RSI < 50", rsi_ok, 1),
        ("1st 15-min candle Open = Low" if bullish else "1st 15-min candle Open = High", opening_bias_ok, 2),
        ("1st 15-min High breakout, close above High" if bullish
         else "1st 15-min Low breakdown, close below Low", opening_breakout_ok, 2),
    ]

    score = sum(points for _, checked, points in items if checked)

    return {
        "score": score,
        "label": f"{score}/10",
        "items": items,
    }


def compute_near_high_score(signal):
    """
    "Near N-month High" (added, per request — "1 to 6 month high show
    korabe, price jodi high er kache thake to trading score point jog
    hobe") — checks whether the current close is within
    config.NEAR_HIGH_THRESHOLD_PCT (5%) of ANY of the 1-6 month highs
    already computed in main.py's build_momentum_volume_data
    (signal["multi_month_highs"] = {1: high, 2: high, ..., 6: high},
    highest daily HIGH over each trailing N-month window). Whichever
    of the 6 months is numerically closest to today's close is the one
    reported — a stock can be "near" more than one month's high at
    once (e.g. if it's been flat for months), only the closest matters.

    Unlike Momentum (signal["momentum"], which only fires when close
    BREAKS ABOVE the 4-week high), this fires on APPROACH too — being
    2% below a 6-month high is exactly the kind of "coiling near
    resistance" setup this is meant to flag, not just actual breakouts.

    Checked regardless of alert direction (BULLISH or BEARISH) — same
    treatment as Daily Score's checks, which are always bullish-quality
    framed regardless of the signal's own direction.

    Returns None if signal["multi_month_highs"] isn't present/empty
    (not enough daily history yet for this symbol — same graceful
    degradation as every other optional field). Otherwise:
      {"score": 1 or 0, "possible": 1, "nearest_month": int (1-6),
       "nearest_high": float, "gap_pct": float}
    gap_pct is signed: negative means close is below that month's
    high, positive means close is already above it.
    """
    multi_month_highs = signal.get("multi_month_highs")
    close = signal.get("close")
    if not multi_month_highs or close is None:
        return None

    nearest_month, nearest_high, nearest_gap_pct = None, None, None
    for months, high in multi_month_highs.items():
        if high <= 0:
            continue
        gap_pct = (close - high) / high * 100
        if nearest_gap_pct is None or abs(gap_pct) < abs(nearest_gap_pct):
            nearest_month, nearest_high, nearest_gap_pct = months, high, gap_pct

    if nearest_month is None:
        return None

    is_near = abs(nearest_gap_pct) <= config.NEAR_HIGH_THRESHOLD_PCT
    return {
        "score": 1 if is_near else 0,
        "possible": 1,
        "nearest_month": nearest_month,
        "nearest_high": nearest_high,
        "gap_pct": round(nearest_gap_pct, 2),
    }


def compute_trading_score(signal):
    """
    "Trading Score" (added, per request — "sob miliye ekta trading
    score generate koro") — ONE combined /10 score that rolls up the
    four separate scores/checks already on the alert, so there's a
    single number to glance at before deciding whether to take the
    trade:

      - Buy/Sell Score (intraday_checklist — entry timing, fixed /10)
      - Daily Score (bullish-quality checklist, fixed /8)
      - Smart Money (institutional confirmation, variable /possible —
        stocks only, not present on index alerts, and only present at
        all when it scored >= config.SMART_MONEY_MIN_SCORE)
      - Near N-month High (added, per request — 1 point if close is
        within config.NEAR_HIGH_THRESHOLD_PCT of any 1-6 month high,
        see compute_near_high_score)

    Each present component is normalized to a common /10 scale, then
    averaged with EQUAL weight across however many components are
    actually available on this particular signal. A missing component
    (e.g. Smart Money didn't qualify this run, or multi_month_highs
    wasn't available yet) is simply left out of the average — not
    counted as 0 — so alerts with fewer available components are still
    scored fairly on whatever they do have.

    Returns None only if neither the checklist nor daily_score is
    attached yet (shouldn't happen in practice — both are always set
    on a signal before this is called). Otherwise:
      {"score": float (0-10, 1 decimal), "label": str}
    label bands: 8-10 STRONG, 6-7.9 GOOD, 4-5.9 MODERATE, <4 WEAK.
    """
    checklist = signal.get("intraday_checklist")
    daily_score = signal.get("daily_score")
    smart_money = signal.get("smart_money")
    near_high = signal.get("near_high")

    parts = []
    if checklist is not None:
        parts.append(checklist["score"] / 10 * 10)
    if daily_score is not None and daily_score.get("total"):
        parts.append(daily_score["score"] / daily_score["total"] * 10)
    if smart_money is not None and smart_money.get("possible"):
        parts.append(smart_money["score"] / smart_money["possible"] * 10)
    if near_high is not None and near_high.get("possible"):
        parts.append(near_high["score"] / near_high["possible"] * 10)

    if not parts:
        return None

    score = round(sum(parts) / len(parts), 1)

    if score >= 8:
        label = "STRONG"
    elif score >= 6:
        label = "GOOD"
    elif score >= 4:
        label = "MODERATE"
    else:
        label = "WEAK"

    return {"score": score, "label": label}
