"""
EMA Alert Bot - single-run entrypoint.

Groups:
  - Indices (Nifty50, BankNifty, Sensex): EMA9/20 cross + Bollinger Band,
    checked on config.INDEX_TIMEFRAME_MINUTES (3-min).
  - Custom equities + commodities (GOLD/SILVER/CRUDEOIL, MCX front-month)
    + all Nifty 50 stocks: EMA9/20 cross + Bollinger Band, checked on
    config.TIMEFRAME_MINUTES (5-min).
  - All Nifty 50 stocks (extra, independent): EMA 50/200 crossover on a
    75-min timeframe.

Equity/index instruments are only checked during NSE hours; commodities
are checked until MCX close.
"""
from datetime import datetime
import pytz

import config
import state as state_store
from upstox_client import UpstoxClient
from strategy import evaluate as evaluate_ema
import strategy_bb
import strategy_ema50_200
import commodities
import nifty50_watchlist
from telegram_notifier import (
    send_message,
    format_signal_message,
    format_bb_signal_message,
    format_ema50200_message,
)

log = config.get_logger("ema_alert_bot")


def within_hours(now: datetime, close_time_str: str) -> bool:
    open_h, open_m = map(int, config.MARKET_OPEN.split(":"))
    close_h, close_m = map(int, close_time_str.split(":"))
    open_t = now.replace(hour=open_h, minute=open_m, second=0, microsecond=0)
    close_t = now.replace(hour=close_h, minute=close_m, second=0, microsecond=0)
    return open_t <= now <= close_t and now.weekday() < 5


def merge_watchlists(*lists) -> list:
    """Combines watchlists, de-duplicating by symbol (keeps first occurrence)
    so we don't fetch/alert the same instrument twice in one run."""
    merged = []
    seen = set()
    for wl in lists:
        for item in wl:
            if item["symbol"] not in seen:
                seen.add(item["symbol"])
                merged.append(item)
    return merged


def run_cycle(client: UpstoxClient, state: dict, watchlist: list, timeframe_minutes: int) -> bool:
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

            ema_signal = evaluate_ema(symbol, raw, timeframe_minutes=timeframe_minutes)
            if ema_signal is not None and not state_store.already_alerted(
                state, symbol, ema_signal.candle_time, tag="EMA"
            ):
                if send_message(format_signal_message(ema_signal)):
                    log.info("EMA alert sent: %s %s @ %s", symbol, ema_signal.direction, ema_signal.candle_time)
                    state_store.mark_alerted(state, symbol, ema_signal.candle_time, tag="EMA")
                    changed = True

            bb_signal = strategy_bb.evaluate(symbol, raw, timeframe_minutes=timeframe_minutes)
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


def run_ema50200_cycle(client: UpstoxClient, state: dict, watchlist: list) -> bool:
    """Extra, independent check: EMA 50/200 crossover on 75-min candles for
    Nifty 50 stocks."""
    changed = False
    for item in watchlist:
        symbol = item["symbol"]
        instrument_key = item["instrument_key"]
        try:
            raw = client.get_recent_candles(
                instrument_key,
                interval=config.BASE_CANDLE_INTERVAL,
                lookback_days=config.CANDLE_LOOKBACK_DAYS_EMA50200,
            )
            if raw.empty:
                log.warning("%s: no candle data returned (EMA50/200)", symbol)
                continue

            signal = strategy_ema50_200.evaluate(symbol, raw)
            if signal is not None and not state_store.already_alerted(
                state, symbol, signal.candle_time, tag="EMA50200"
            ):
                if send_message(format_ema50200_message(signal)):
                    log.info("EMA50/200 alert sent: %s %s @ %s", symbol, signal.direction, signal.candle_time)
                    state_store.mark_alerted(state, symbol, signal.candle_time, tag="EMA50200")
                    changed = True

        except Exception as e:
            log.exception("Error processing %s (EMA50/200): %s", symbol, e)

    return changed


def main():
    tz = pytz.timezone(config.TIMEZONE)
    now = datetime.now(tz)

    equity_open = within_hours(now, config.MARKET_CLOSE)
    commodity_open = within_hours(now, config.COMMODITY_MARKET_CLOSE)

    if not equity_open and not commodity_open:
        log.info("Outside all market hours (%s IST) - skipping this run", now.strftime("%H:%M"))
        return

    log.info(
        "Running alert check (EMA %d/%d, BB %d/%.1f) at %s IST "
        "[equity_open=%s, commodity_open=%s]",
        config.EMA_FAST, config.EMA_SLOW, config.BB_LENGTH, config.BB_MULT,
        now.strftime("%H:%M"), equity_open, commodity_open,
    )

    client = UpstoxClient()
    state = state_store.load_state()

    changed = False
    nifty50_list = []

    # ---- Indices: 3-min EMA9/20 + Bollinger ----
    if equity_open:
        changed = run_cycle(client, state, config.INDEX_WATCHLIST, config.INDEX_TIMEFRAME_MINUTES) or changed

    # ---- Custom equities + all Nifty50 stocks + commodities: 5-min ----
    five_min_watchlist = []
    if equity_open:
        nifty50_list = nifty50_watchlist.build_nifty50_watchlist()
        five_min_watchlist = merge_watchlists(config.WATCHLIST, nifty50_list)
    if commodity_open:
        five_min_watchlist += commodities.build_commodity_watchlist(client)

    if five_min_watchlist:
        changed = run_cycle(client, state, five_min_watchlist, config.TIMEFRAME_MINUTES) or changed

    # ---- Extra: EMA 50/200 (75-min) for Nifty 50 stocks ----
    if equity_open and nifty50_list:
        log.info("Running extra EMA 50/200 (75-min) check for Nifty 50 stocks")
        changed = run_ema50200_cycle(client, state, nifty50_list) or changed

    if changed:
        state_store.save_state(state)
        log.info("State updated and saved.")
    else:
        log.info("No new signals this run.")


if __name__ == "__main__":
    main()
