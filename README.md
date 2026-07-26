# ema-alert

EMA 9/20 crossover alert bot for NSE, filtered by RSI, on 5-minute candles.
Sends Telegram alerts only. Uses an Upstox Analytics Token for market data
and runs on a schedule via GitHub Actions.

See individual files for setup details. Configure secrets under
Settings > Secrets and variables > Actions:
UPSTOX_ANALYTICS_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

Edit WATCHLIST in config.py to set which symbols to track.
