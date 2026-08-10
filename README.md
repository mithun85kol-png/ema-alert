# ema-alert

EMA crossover alert bot for NSE, with RSI/volume/VWAP/MACD/pivot context.
Sends Telegram alerts only. Uses an Upstox Access Token for market data and
runs on a schedule via GitHub Actions.

Runs **two independent scans** every run (see `main.py`):

1. **F&O / Index / Commodity scan** (`run_fo_scan`) — the ~50-name F&O
   underlyings + indices (NIFTY 50, NIFTY BANK, SENSEX) + MCX commodities
   (GOLD, SILVER, CRUDEOIL). Stocks/commodities alert on a **75-min
   EMA9/EMA20 cross + mandatory EMA50 trend agreement**; indices alert on a
   pure **3-min EMA9/EMA20 cross** (no trend gate).
2. **Nifty 500 cash-stock scan** (`run_nifty500_scan`) — the full Nifty 500
   constituent list (cash/EQ segment), fetched live from NSE's archives so
   it stays correct across NSE's periodic index rebalances (see
   `instruments.resolve_nifty500_stocks` — cached once per calendar day).
   Same conditions as the F&O 75-min flow (EMA cross + mandatory EMA50
   trend agreement, RSI/volume/VWAP/MACD/pivot/sector/delivery-%
   informational context) but using **EMA9/EMA21**
   (`config.NIFTY500_EMA_FAST` / `NIFTY500_EMA_SLOW`) instead of EMA9/20,
   and no PCR/option-chain fields (cash-focused; most Nifty 500 names have
   no option chain). A stock that's in both lists (F&O-eligible *and* in
   the Nifty 500) is scanned — and can alert — independently under each
   EMA pair; the alert's "F&O: Yes/No" line tells you which kind of stock
   it is.

Configure secrets under Settings > Secrets and variables > Actions:
`UPSTOX_ACCESS_TOKEN`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`

Edit `FO_STOCK_WATCHLIST` / `USE_FULL_FO_LIST` in `config.py` to change the
F&O scan's symbols. The Nifty 500 scan's symbol list is not edited in
config — it's always the live NSE list. EMA periods for both scans
(`EMA_FAST`/`EMA_SLOW` and `NIFTY500_EMA_FAST`/`NIFTY500_EMA_SLOW`) are in
`config.py`.

See individual files for further setup details.
