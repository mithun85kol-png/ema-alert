"""
EMA Alert Bot - single-run entrypoint for the STOCK watchlist group
(all Nifty50 stocks + BDL). Checked on every timeframe in
config.STOCK_TIMEFRAMES (5-min and 75-min), each firing its own
independent alert. Runs on its own less-frequent schedule
(stock-alert.yml), separate from the index/commodity checks, to keep
GitHub Actions minute usage low.
"""
from datetime import datetime
import pytz

import config
import state as state_store
from upstox_client import UpstoxClient
import nse_equity_lookup
from run_once import within_hours, run_group

log = config.get_logger("ema_alert_bot_stocks")


def main():
    tz = pytz.timezone(config.TIMEZONE)
    now = datetime.now(tz)

    if not within_hours(now, config.MARKET_CLOSE):
        log.info("Outside NSE equity hours (%s IST) - skipping this run", now.strftime("%H:%M"))
        return

    log.info("Running stock alert check (%d symbols, timeframes=%s) at %s IST",
              len(config.STOCK_SYMBOLS), config.STOCK_TIMEFRAMES, now.strftime("%H:%M"))

    client = UpstoxClient()
    state = state_store.load_state()

    stock_watchlist = nse_equity_lookup.build_stock_watchlist(client)

    changed = False
    for tf in config.STOCK_TIMEFRAMES:
        changed |= run_group(client, state, stock_watchlist, tf)

    if changed:
        state_store.save_state(state)
        log.info("State updated and saved.")
    else:
        log.info("No new signals this run.")


if __name__ == "__main__":
    main()
