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


def already_alerted(state, symbol, direction, candle_time):
    key = f"{symbol}:{direction}"
    return str(candle_time) in _get_times(state, key)


def mark_alerted(state, symbol, direction, candle_time):
    key = f"{symbol}:{direction}"
    times = _get_times(state, key)
    ct = str(candle_time)
    if ct not in times:
        times.append(ct)
    # Keep only the most recent entries so the state file doesn't grow
    # forever; MAX_REMEMBERED_PER_KEY is comfortably larger than the
    # catch-up lookback window so nothing gets re-sent by accident.
    state[key] = times[-MAX_REMEMBERED_PER_KEY:]
