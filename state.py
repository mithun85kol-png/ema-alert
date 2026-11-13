import json
import os
import datetime as dt
import config

# How many past candle_times to remember per (symbol, direction). Bounds
# the state file's size — only needs to comfortably exceed
# config.CROSS_LOOKBACK_CANDLES so nothing in the catch-up window can
# ever be re-sent, while old entries eventually age out.
MAX_REMEMBERED_PER_KEY = 50


def load_state(state_file=None):
    """
    state_file defaults to config.STATE_FILE.
    """
    state_file = state_file or config.STATE_FILE
    if not os.path.exists(state_file):
        return {}
    with open(state_file, "r") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return {}


def save_state(state, state_file=None):
    state_file = state_file or config.STATE_FILE
    with open(state_file, "w") as f:
        json.dump(state, f, indent=2, default=str)


def _get_times(state, key):
    """
    Returns the list of already-alerted candle_time strings for `key`.
    Transparently upgrades the old format (a single {"candle_time":...,
    "alerted_at":...} dict, from before the catch-up window was added)
    so existing state files don't need to be deleted/reset.
    """
    val = state.get(key)
    if val is None:
        return []
    if isinstance(val, dict):
        ct = val.get("candle_time")
        return [ct] if ct else []
    if isinstance(val, list):
        return val
    return []


def _sorted_recent(times, limit):
    """
    Keeps the LIMIT most recent entries by actual chronological
    candle_time, not append order. STRICTNESS FIX (per request, kept
    in sync with merge_state.py's identical fix): prevents a rare edge
    case where a genuinely recent candle_time gets trimmed out just
    because of insertion order, which would make a later run see that
    candle as "never alerted" and re-send it.
    """
    def _key(ct):
        parsed = _parse_datetime(ct)
        return parsed if parsed is not None else dt.datetime.min

    unique = list(dict.fromkeys(times))
    unique.sort(key=_key)
    return unique[-limit:]


def already_alerted(state, symbol, direction, candle_time):
    key = f"{symbol}:{direction}"
    return str(candle_time) in _get_times(state, key)


def mark_alerted(state, symbol, direction, candle_time):
    key = f"{symbol}:{direction}"
    times = _get_times(state, key)
    ct = str(candle_time)
    if ct not in times:
        times.append(ct)
    # Keep only the most recent entries (by real time, not insertion
    # order — see _sorted_recent) so the state file doesn't grow
    # forever; MAX_REMEMBERED_PER_KEY is comfortably larger than the
    # catch-up lookback window so nothing gets re-sent by accident.
    state[key] = _sorted_recent(times, MAX_REMEMBERED_PER_KEY)


def _parse_datetime(candle_time_str):
    """
    Parses a candle_time string (str(pandas_timestamp), e.g.
    "2026-08-19 11:45:00" or "2026-08-19 11:45:00+05:30") into a naive
    datetime for cooldown-window math. Strips any timezone offset
    suffix first, since every candle_time in a single run already
    shares the same timezone (IST) -- only the elapsed minutes between
    two of them matters here, not absolute UTC alignment.
    """
    if not candle_time_str:
        return None
    s = str(candle_time_str).strip()
    # Drop a trailing "+HH:MM" / "-HH:MM" offset if present.
    if len(s) > 6 and s[-6] in ("+", "-") and s[-3] == ":":
        s = s[:-6]
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return dt.datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def in_cooldown(state, symbol, direction, candle_time, cooldown_minutes):
    """
    Same-direction alert cooldown (added, per request) — True if
    (symbol, direction) already alerted within `cooldown_minutes` of
    `candle_time`, even if this is a genuinely new/different candle
    (i.e. a real new crossover, not the exact-duplicate case
    already_alerted() above handles). This is what stops a whipsaw-y
    stock from re-firing the same direction again too soon. A
    cooldown_minutes of 0 (or a candle_time / stored time that fails
    to parse) never blocks -- fails open, same principle as every
    other gate in this bot: a data hiccup never silently eats a real
    signal.
    """
    if not cooldown_minutes:
        return False
    key = f"{symbol}:{direction}"
    times = _get_times(state, key)
    if not times:
        return False
    current_dt = _parse_datetime(candle_time)
    if current_dt is None:
        return False
    for t in times:
        prev_dt = _parse_datetime(t)
        if prev_dt is None:
            continue
        gap_minutes = (current_dt - prev_dt).total_seconds() / 60.0
        if 0 <= gap_minutes < cooldown_minutes:
            return True
    return False


def count_today(state, symbol, direction, candle_time):
    """How many alerts (this one included, call AFTER mark_alerted) this symbol has had on
    the same day as candle_time: (same_direction_count, opposite_direction_count)."""
    day = str(candle_time)[:10]
    opp = "BEARISH" if direction == "BULLISH" else "BULLISH"
    same = sum(1 for t in _get_times(state, f"{symbol}:{direction}") if str(t)[:10] == day)
    other = sum(1 for t in _get_times(state, f"{symbol}:{opp}") if str(t)[:10] == day)
    return same, other


def alert_day_streak(state, symbol, direction, candle_time):
    """Consecutive trading days (ending on candle_time's day) on which this symbol had an
    alert in this direction. Call AFTER mark_alerted. 1 = first day, 2 = alerted yesterday
    too, 3 = day-before-yesterday too ... Weekends are skipped; a gap (e.g. a market
    holiday or a day with no alert) restarts the count at 1."""
    days = {str(t)[:10] for t in _get_times(state, f"{symbol}:{direction}")}
    try:
        d = dt.date.fromisoformat(str(candle_time)[:10])
    except ValueError:
        return 1
    days.add(d.isoformat())
    streak = 1
    while True:
        d -= dt.timedelta(days=1)
        while d.weekday() >= 5:          # skip Sat/Sun
            d -= dt.timedelta(days=1)
        if d.isoformat() in days:
            streak += 1
        else:
            break
        if streak > 60:
            break
    return streak


def alerted_today(state, symbol, direction, candle_time, any_direction=False):
    """True if this symbol already has an alert on the same day as candle_time
    (same direction, or either direction when any_direction=True)."""
    day = str(candle_time)[:10]
    dirs = ("BULLISH", "BEARISH") if any_direction else (direction,)
    for d in dirs:
        if any(str(t)[:10] == day for t in _get_times(state, f"{symbol}:{d}")):
            return True
    return False


def log_alert(state, entry):
    """Remember one sent alert (price, time ...) for the end-of-day summary report.
    Stored inside alert_state.json under '_alerts_log' -> {YYYY-MM-DD: [entries]}."""
    log = state.setdefault("_alerts_log", {})
    if not isinstance(log, dict):
        log = state["_alerts_log"] = {}
    day = str(entry["time"])[:10]
    lst = log.setdefault(day, [])
    sig = (entry["symbol"], entry["direction"], str(entry["time"]))
    if not any((e.get("symbol"), e.get("direction"), str(e.get("time"))) == sig for e in lst):
        lst.append(entry)
    for old in sorted(log.keys())[:-6]:      # keep the last ~6 days only
        log.pop(old, None)


def get_alert_log(state, day):
    log = state.get("_alerts_log")
    if not isinstance(log, dict):
        return []
    return list(log.get(day) or [])


def last_alert_score(state, symbol, direction, day):
    """'passed' points of the most recent alert today for this symbol+direction
    (from the alert log), or None if unknown."""
    best_t, best_p = None, None
    for e in get_alert_log(state, day):
        if e.get("symbol") == symbol and e.get("direction") == direction:
            try:
                p = int(str(e.get("score", "")).split("/")[0])
            except ValueError:
                continue
            if best_t is None or str(e.get("time")) >= best_t:
                best_t, best_p = str(e.get("time")), p
    return best_p
