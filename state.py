import json
import os
import datetime as dt
import config


def load_state():
    if not os.path.exists(config.STATE_FILE):
        return {}
    with open(config.STATE_FILE, "r") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return {}


def save_state(state):
    with open(config.STATE_FILE, "w") as f:
        json.dump(state, f, indent=2, default=str)


def already_alerted(state, symbol, direction, candle_time):
    key = f"{symbol}:{direction}"
    last = state.get(key)
    if not last:
        return False
    if last.get("candle_time") == str(candle_time):
        return True
    return False


def mark_alerted(state, symbol, direction, candle_time):
    key = f"{symbol}:{direction}"
    state[key] = {
        "candle_time": str(candle_time),
        "alerted_at": dt.datetime.utcnow().isoformat(),
    }
