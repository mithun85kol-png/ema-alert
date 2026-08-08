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
directly on 3-min data (df3) with require_trend_confirmation=False, so
an index alert is a PURE EMA9/EMA20 crossover on the 3-min chart — no
EMA50 trend requirement, no 75-min involvement at all. RSI/volume/
VWAP/MACD/pivot context fields are still computed the same way (since
_evaluate_candle is timeframe-agnostic — see the NOTE below), just on
3-min bars for indices instead of 75-min bars.
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
For INDICES, main.py instead passes df3 (3-min candles) into the exact
same check_signals(), with require_trend_confirmation=False — so an
index alert fires right when a 3-min candle closes with a qualifying
EMA9/20 cross, and all the RSI/volume/VWAP/MACD/pivot context on that
alert is computed on 3-min bars too (still informational only).

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

import config
from indicators import (
    add_emas, add_rsi, add_volume_avg, add_ema50, add_macd,
    detect_candle_pattern,
)

# How close price needs to be to R3/S3 (as a % of price) to be flagged
# as "near" that level, rather than "mid-range".
PIVOT_PROXIMITY_PCT = 0.3


def _min_required_len(lookback):
    return max(config.EMA_SLOW, config.RSI_PERIOD, config.VOLUME_AVG_PERIOD, config.MACD_SLOW, 50) + lookback + 1


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


def _evaluate_candle(df, idx, symbol, r3, s3, require_trend_confirmation=True, prev_close=None):
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

    # Volume — INFORMATIONAL ONLY (no longer a gating condition). The
    # crossing candle's volume vs the previous candle's is still
    # computed and shown in the alert (vol_change_pct), but a signal no
    # longer requires curr volume > prev volume to fire. If prev_vol is
    # 0/unavailable, vol_change_pct is simply None — the alert still
    # fires, just without that line's %.
    curr_vol = curr["volume"]
    prev_vol = prev["volume"]
    vol_change_pct = round(((curr_vol - prev_vol) / prev_vol) * 100, 1) if prev_vol else None
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


def check_signals(df, symbol, r3=None, s3=None, lookback=None, require_trend_confirmation=True, prev_close=None):
    """
    Scans the last `lookback` closed candles (default:
    config.CROSS_LOOKBACK_CANDLES) for EMA9/20 crossovers — not just the
    single latest candle — so a run that was skipped/delayed still
    catches up on any cross it would otherwise have missed.

    require_trend_confirmation=False disables condition 4 (EMA50 trend
    agreement) — used for indices, where every qualifying crossover
    should alert regardless of the broader trend. Leave True (default)
    for stocks/commodities.

    Returns a list of signal dicts, oldest candle first. Empty list if
    nothing qualifies. Caller is responsible for de-duping against
    already-alerted (symbol, direction, candle_time) combos (see
    state.py) before sending each one.
    """
    lookback = lookback or config.CROSS_LOOKBACK_CANDLES

    if len(df) < _min_required_len(1):
        return []

    # Shrink the lookback window if we don't have enough history yet
    # (e.g. right after market open) rather than returning nothing.
    max_possible_lookback = len(df) - _min_required_len(0)
    lookback = max(1, min(lookback, max_possible_lookback))

    df = add_emas(df, config.EMA_FAST, config.EMA_SLOW)
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


def get_3min_trend_info(df_3min, symbol, lookback_candles=None):
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
    if len(df_3min) < config.EMA_SLOW + 2:
        return None

    lookback_candles = lookback_candles or config.INFO_3MIN_LOOKBACK_CANDLES

    df = add_emas(df_3min, config.EMA_FAST, config.EMA_SLOW)
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
            # Timestamp (candle open time, from df's datetime index) of the
            # 75-min candle on which the cross actually happened — lets the
            # Telegram message show an exact date/time instead of just
            # "crossed N candle(s) ago".
            cross_time = df.index[idx]
            break

    return {
        "symbol": symbol,
        "bias": bias,
        "ema_fast": round(float(curr["ema_fast"]), 2),
        "ema_slow": round(float(curr["ema_slow"]), 2),
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


def pd_isna(val):
    import pandas as pd
    return pd.isna(val)
