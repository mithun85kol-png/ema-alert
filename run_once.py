"""
EMA Alert Bot - single-run entrypoint. Checks each symbol for both an EMA
crossover signal and a Bollinger Band re-entry signal, independently.
GOLD and SILVER (MCX front-month futures) are resolved dynamically every
run so their contract keys always stay current.
"""
from datetime import datetime
import pytz

import config
import state as state_store
from upstox_client import UpstoxClient
from strategy import evaluate as evaluate_ema
import strategy_bb
import commodities
from telegram_notifier import send_message, format_signal_message, format_bb_signal_message

log = config.get_logger("ema_alert_bot")


def within_market_hours(now: datetime) -> bool:
    open_h, open_m = map(int, config.MARKET_OPEN.split(":"))
    close_h, close_m = map(int, config.MARKET_CLOSE.split(":"))
    open_t = now.replace(hour=open_h, minute=open_m, second=0, microsecond=0)
    close_t = now.replace(hour=close_h, minute=close_m, second=0, microsecond=0)
    return open_t <= now <= close_t and now.weekday() < 5


def run_cycle(client: UpstoxClient, state: dict, watchlist: list) -> bool:
    changed = False
    for item in watchlist:
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

            ema_signal = evaluate_ema(symbol, raw)
            if ema_signal is not None and not state_store.already_alerted(
                state, symbol, ema_signal.candle_time, tag="EMA"
            ):
                if send_message(format_signal_message(ema_signal)):
                    log.info("EMA alert sent: %s %s @ %s", symbol, ema_signal.direction, ema_signal.candle_time)
                    state_store.mark_alerted(state, symbol, ema_signal.candle_time, tag="EMA")
                    changed = True

            bb_signal = strategy_bb.evaluate(symbol, raw)
            if bb_signal is not None and not state_store.already_alerted(
                state, symbol, bb_signal.candle_time, tag="BB"
            ):
                if send_message(format_bb_signal_message(bb_signal)):
                    log.info("BB alert sent: %s %s @ %s", symbol, bb_signal.direction, bb_signal.candle_time)
                    state_store.mark_alerted(state, symbol, bb_signal.candle_time, tag="BB")
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

    log.info("Running alert check (EMA %d/%d, BB %d/%.1f, %d-min timeframe) at %s IST",
              config.EMA_FAST, config.EMA_SLOW, config.BB_LENGTH, config.BB_MULT,
              config.TIMEFRAME_MINUTES, now.strftime("%H:%M"))

    client = UpstoxClient()
    state = state_store.load_state()

    watchlist = list(config.WATCHLIST) + commodities.build_commodity_watchlist(client)

    changed = run_cycle(client, state, watchlist)

    if changed:
        state_store.save_state(state)
        log.info("State updated and saved.")
    else:
        log.info("No new signals this run.")


if __name__ == "__main__":
    main()
