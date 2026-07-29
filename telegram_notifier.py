import requests
import config


def _fmt(val, suffix=""):
    return "N/A" if val is None else f"{val}{suffix}"


def send_alert(signal):
    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        print("Telegram not configured, skipping send:", signal)
        return

    arrow = "🟢⬆️" if signal["direction"] == "BULLISH" else "🔴⬇️"
    trend_icon = "📈" if signal.get("stock_trend") == "BULLISH" else "📉"
    sector_icon = "📈" if signal.get("sector_trend") == "BULLISH" else ("📉" if signal.get("sector_trend") == "BEARISH" else "❔")

    vol_change = signal.get("vol_change_pct")
    vol_change_str = _fmt(vol_change, "%") if vol_change is None else f"{'+' if vol_change >= 0 else ''}{vol_change}%"

    text = (
        f"{arrow} *{signal['symbol']}* — EMA9/EMA20 {signal['direction']} cross\n"
        f"Price: {signal['close']}\n"
        f"EMA9: {signal['ema_fast']}  |  EMA20: {signal['ema_slow']}\n"
        f"RSI(14): {_fmt(signal.get('rsi'))}\n"
        f"Stock trend (EMA50): {trend_icon} {signal.get('stock_trend', 'UNKNOWN')}\n"
        f"Sector: {signal.get('sector', 'UNKNOWN')} — {sector_icon} {signal.get('sector_trend', 'UNKNOWN')}\n"
        f"Volume: {signal['volume']} (avg {_fmt(signal.get('vol_avg'))})\n"
        f"Volume vs prev candle: {vol_change_str}\n"
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
