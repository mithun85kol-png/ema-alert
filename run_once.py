"""
EMA Alert Bot - single-run entrypoint, meant to be invoked by GitHub Actions
on a schedule (e.g. every 5 minutes during market hours).
"""
from datetime import datetime
import pytz

import config
import state as state_store
from upstox_client import UpstoxClient
from strategy import evaluate
from telegram_notifier import send_message, format_signal_message

log = config.get_logger("ema_alert_bot")


def within_market_hours(now: datetime) -> bool:
    open_h, open_m = map(int, config.MARKET_OPEN.split(":"))
    close_h, close_m = map(int, config.MARKET_CLOSE.split(":"))
    open_t = now.replace(hour=open_h, minute=open_m, second=0, microsecond=0)
    close_t = now.replace(hour=close_h, minute=close_m, second=0, microsecond=0)
    return open_t <= now <= close_t and now.weekday() < 5


def run_cycle(client: UpstoxClient, state: dict) -> bool:
    changed = False
    for item in config.WATCHLIST:
        symbol = item["symbol"]
        instrument_key = item["instrument_key"]
        try:
            raw = client.get_recent_candles(
                instrument_key,
                interval=config.BASE_CANDLE_INTERVAL,
                lookback_days=config.CANDLE_LOOKBACK_DAYS,
            )
            if raw.empty:
                log.warning("%s: no candle data returned", symbol)
                continue

            signal = evaluate(symbol, raw)
            if signal is None:
                continue

            if state_store.already_alerted(state, symbol, signal.candle_time):
                continue

            message = format_signal_message(signal)
            if send_message(message):
                log.info("Alert sent: %s %s @ %s", symbol, signal.direction, signal.candle_time)
                state_store.mark_alerted(state, symbol, signal.candle_time)
                changed = True

        except Exception as e:
            log.exception("Error processing %s: %s", symbol, e)

    return changed


def main():
    tz = pytz.timezone(config.TIMEZONE)
    now = datetime.now(tz)

    if not within_market_hours(now):
        log.info("Outside market hours (%s IST) - skipping this run", now.strftime("%H:%M"))
        return

    log.info("Running EMA Alert check (EMA %d/%d, RSI %d, %d-min timeframe) at %s IST",
              config.EMA_FAST, config.EMA_SLOW, config.RSI_PERIOD,
              config.TIMEFRAME_MINUTES, now.strftime("%H:%M"))

    client = UpstoxClient()
    state = state_store.load_state()

    changed = run_cycle(client, state)

    if changed:
        state_store.save_state(state)
        log.info("State updated and saved.")
    else:
        log.info("No new signals this run.")


if __name__ == "__main__":
    main()
