import requests
import config


def send_alert(signal):
    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        print("Telegram not configured, skipping send:", signal)
        return

    arrow = "🟢⬆️" if signal["direction"] == "BULLISH" else "🔴⬇️"
    text = (
        f"{arrow} *{signal['symbol']}* — EMA9/EMA20 {signal['direction']} cross\n"
        f"Price: {signal['close']}\n"
        f"EMA9: {signal['ema_fast']}  |  EMA20: {signal['ema_slow']}\n"
        f"RSI(14): {signal['rsi']}\n"
        f"Volume: {signal['volume']} (avg {signal['vol_avg']})\n"
        f"Candle: {signal['candle_time']}"
    )

    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={
            "chat_id": config.TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "Markdown",
        }, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print("Telegram send failed:", e)
