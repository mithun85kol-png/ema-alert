
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
    detect_candle_pattern, add_atr,
)

# How close price needs to be to R3/S3 (as a % of price) to be flagged
# as "near" that level, rather than "mid-range".
PIVOT_PROXIMITY_PCT = 0.3


def _min_required_len(lookback, ema_slow=None):
    ema_slow = ema_slow if ema_slow is not None else config.EMA_SLOW
    return max(ema_slow, config.RSI_PERIOD, config.VOLUME_AVG_PERIOD, config.MACD_SLOW, 50) + lookback + 1


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


def _evaluate_candle(df, idx, symbol, r3, s3, require_trend_confirmation=True, prev_close=None,
                      ema_fast_period=None, ema_slow_period=None, require_volume_increase=False):
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

    # Day change % — purely informational, vs previous trading day's
    # close (same prev-day close already fetched for R3/S3). None if
    # prev_close wasn't available (e.g. pivot fetch failed for this
    # symbol that day) — the alert simply omits this line then.
    day_change_pct = None
    if prev_close:
        day_change_pct = round((close_price - prev_close) / prev_close * 100, 2)

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
    }


def check_signals(df, symbol, r3=None, s3=None, lookback=None, require_trend_confirmation=True, prev_close=None,
                   ema_fast=None, ema_slow=None, require_volume_increase=False, require_strong_candle=False):
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


def compute_trade_score(signal):
    """
    "Trade Signal Score" (added, per request) — a single 0-10 number
    that rolls up every already-computed quality signal on this alert
    into one headline number, so the reader doesn't have to mentally
    combine Confluence/Smart Money/Sector/EMA50-200/OI/VWAP/Momentum/
    Volume-Spike/Bulk-Block/other-timeframe-agreement themselves.
    PURELY INFORMATIONAL — like everything else this touches, it never
    blocks or filters an alert; it's computed AFTER all filtering
    (confluence, volume-spike gate, etc.) has already decided the
    alert is going out.

    10 independent 1-point dimensions, built entirely from fields the
    signal already carries by the time this is called (see main.py —
    no new fetch, no extra API call):
      1.  Confluence "High R:R" filter passed (signal["confluence_passed"])
      2.  Volume Spike (signal["volume_spike"] is True)
      3.  Momentum — close above the trailing 4-week high
      4.  EMA50/200 (daily) bias agrees with the signal direction
      5.  Sector index trend agrees with the signal direction
      6.  OI buildup bias agrees with the signal direction (F&O only)
      7.  VWAP cushion in the signal's favor (>= config.SMART_MONEY_VWAP_MIN_PCT)
      8.  Same-direction Bulk/Block deal within the recent lookback window
      9.  Crossing candle's volume beat the previous candle (vol_change_pct > 0)
      10. The OTHER (informational) timeframe's own EMA bias agrees
          with the signal direction (signal["trend_3min"]["bias"])

    Each dimension only counts toward "possible" if its underlying
    data was actually available this run (same graceful-skip pattern
    as compute_smart_money_signal) — a stock with, say, no sector
    mapping or no F&O option chain isn't unfairly penalized for a
    dimension that was never computable for it. The final score is
    scaled from whatever "possible" was up to a common /10 scale, so
    a stock scored on 7 available dimensions and one scored on 10 are
    still comparable.

    Returns {"score": int (0-10), "possible": int, "raw_score": int,
    "label": str} — never None; if literally nothing was available
    (shouldn't happen in practice, since direction/close always are),
    returns a 0/10.
    """
    direction = signal["direction"]
    raw_score = 0
    possible = 0

    # ---- 1. Confluence "High R:R" filter ----
    if signal.get("confluence_passed") is not None:
        possible += 1
        if signal.get("confluence_passed"):
            raw_score += 1

    # ---- 2. Volume Spike ----
    volume_spike = signal.get("volume_spike")
    if volume_spike is not None:
        possible += 1
        if volume_spike:
            raw_score += 1

    # ---- 3. Momentum (4-week high) ----
    momentum = signal.get("momentum")
    if momentum is not None:
        possible += 1
        if momentum:
            raw_score += 1

    # ---- 4. EMA50/200 (daily) bias ----
    ema_cross = signal.get("ema_cross")
    if ema_cross:
        possible += 1
        if ema_cross.get("bias") == direction:
            raw_score += 1

    # ---- 5. Sector trend agreement ----
    sector_trend = signal.get("sector_trend")
    if sector_trend:
        possible += 1
        stock_trend_as_sector = "UPTREND" if direction == "BULLISH" else "DOWNTREND"
        if sector_trend == stock_trend_as_sector:
            raw_score += 1

    # ---- 6. OI buildup direction ----
    oi_buildup = signal.get("oi_buildup")
    if oi_buildup:
        possible += 1
        if oi_buildup.get("bias") == direction:
            raw_score += 1

    # ---- 7. VWAP cushion ----
    vwap = signal.get("vwap")
    close = signal.get("close")
    if vwap and close:
        possible += 1
        gap_pct = (close - vwap) / vwap * 100
        if direction == "BULLISH" and gap_pct >= config.SMART_MONEY_VWAP_MIN_PCT:
            raw_score += 1
        elif direction == "BEARISH" and gap_pct <= -config.SMART_MONEY_VWAP_MIN_PCT:
            raw_score += 1

    # ---- 8. same-direction Bulk/Block deal, recent ----
    deal = signal.get("last_bulk_block_deal")
    if deal and deal.get("date") and signal.get("candle_time"):
        possible += 1
        expected_side = "BUY" if direction == "BULLISH" else "SELL"
        if (deal.get("buy_sell") or "").upper() == expected_side:
            deal_dt = _parse_deal_date(deal["date"])
            candle_dt = _parse_candle_time(signal["candle_time"])
            if deal_dt is not None and candle_dt is not None:
                if 0 <= (candle_dt.date() - deal_dt.date()).days <= config.SMART_MONEY_DEAL_LOOKBACK_DAYS:
                    raw_score += 1

    # ---- 9. volume beat previous candle ----
    vol_change_pct = signal.get("vol_change_pct")
    if vol_change_pct is not None:
        possible += 1
        if vol_change_pct > 0:
            raw_score += 1

    # ---- 10. other-timeframe EMA bias agreement ----
    trend3 = signal.get("trend_3min")
    if trend3 and trend3.get("bias"):
        possible += 1
        if trend3["bias"] == direction:
            raw_score += 1

    if possible == 0:
        return {"score": 0, "possible": 0, "raw_score": 0, "label": "0/10"}

    # Scale raw_score (out of "possible" dimensions that had data) up
    # to a common /10 scale, rounded to the nearest whole point.
    scaled = round((raw_score / possible) * 10)

    return {
        "score": scaled,
        "possible": possible,
        "raw_score": raw_score,
        "label": f"{scaled}/10",
    }
