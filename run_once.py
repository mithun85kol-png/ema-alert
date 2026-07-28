"""
EMA Alert Bot - single-run entrypoint for the INDEX (3-min) and COMMODITY
(3-min) watchlist groups. Runs frequently via ema-alert.yml. The STOCK
group (75-min, all Nifty50 stocks) runs separately via run_stocks.py /
stock-alert.yml so it doesn't add cost to this frequent schedule.
"""
from datetime import datetime
import pytz

import config
import state as state_store
from upstox_client import UpstoxClient
from strategy import evaluate as evaluate_ema
import commodities
from telegram_notifier import send_message, format_signal_message

log = config.get_logger("ema_alert_bot")


def within_hours(now: datetime, close_time_str: str) -> bool:
    open_h, open_m = map(int, config.MARKET_OPEN.split(":"))
    close_h, close_m = map(int, close_time_str.split(":"))
    open_t = now.replace(hour=open_h, minute=open_m, second=0, microsecond=0)
    close_t = now.replace(hour=close_h, minute=close_m, second=0, microsecond=0)
    return open_t <= now <= close_t and now.weekday() < 5


def run_group(client: UpstoxClient, state: dict, items: list, timeframe: int) -> bool:
    changed = False
    for item in items:
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

            ema_signal = evaluate_ema(symbol, raw, timeframe)
            if ema_signal is not None and not state_store.already_alerted(
                state, symbol, ema_signal.candle_time, tag="EMA"
            ):
                if send_message(format_signal_message(ema_signal)):
                    log.info("EMA alert sent: %s %s @ %s", symbol, ema_signal.direction, ema_signal.candle_time)
                    state_store.mark_alerted(state, symbol, ema_signal.candle_time, tag="EMA")
                    changed = True

        except Exception as e:
            log.exception("Error processing %s: %s", symbol, e)

    return changed


def main():
    tz = pytz.timezone(config.TIMEZONE)
    now = datetime.now(tz)

    equity_open = within_hours(now, config.MARKET_CLOSE)
    commodity_open = within_hours(now, config.COMMODITY_MARKET_CLOSE)

    if not equity_open and not commodity_open:
        log.info("Outside all market hours (%s IST) - skipping this run", now.strftime("%H:%M"))
        return

    log.info("Running index/commodity alert check at %s IST [equity_open=%s, commodity_open=%s]",
              now.strftime("%H:%M"), equity_open, commodity_open)

    client = UpstoxClient()
    state = state_store.load_state()
    changed = False

    if equity_open:
        changed |= run_group(client, state, config.INDEX_WATCHLIST, config.INDEX_TIMEFRAME)

    if commodity_open:
        commodity_watchlist = commodities.build_commodity_watchlist(client)
        changed |= run_group(client, state, commodity_watchlist, config.COMMODITY_TIMEFRAME)

    if changed:
        state_store.save_state(state)
        log.info("State updated and saved.")
    else:
        log.info("No new signals this run.")


if __name__ == "__main__":
    main()
