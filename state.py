"""
Persists the timestamp of the last alert sent per symbol so the bot doesn't
re-send the same alert if the same candle is checked more than once, or
if it restarts mid-session.
"""
import json
import os
import config

log = config.get_logger(__name__)


def load_state() -> dict:
    if not os.path.exists(config.STATE_FILE):
        return {}
    try:
        with open(config.STATE_FILE, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        log.warning("Could not read state file, starting fresh")
        return {}


def save_state(state: dict) -> None:
    try:
        with open(config.STATE_FILE, "w") as f:
            json.dump(state, f, indent=2, default=str)
    except OSError as e:
        log.error("Could not write state file: %s", e)


def already_alerted(state: dict, symbol: str, candle_time) -> bool:
    return state.get(symbol) == str(candle_time)


def mark_alerted(state: dict, symbol: str, candle_time) -> None:
    state[symbol] = str(candle_time)
