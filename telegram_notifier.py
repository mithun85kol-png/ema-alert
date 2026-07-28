"""
Sends alert messages to a Telegram chat via a bot.
"""
import requests
import config

log = config.get_logger(__name__)


def send_message(text: str) -> bool:
    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        log.error("Telegram bot token / chat id not configured. Check your .env file.")
        return False

    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": config.TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code != 200:
            log.error("Telegram send failed: %s", resp.text[:300])
            return False
        return True
    except requests.RequestException as e:
        log.error("Telegram send exception: %s", e)
        return False


def _volume_line(volume: float, prev_volume: float) -> str:
    if prev_volume and volume > prev_volume:
        note = "higher than previous ⬆️"
    elif prev_volume and volume < prev_volume:
        note = "lower than previous ⬇️"
    else:
        note = "same as previous"
    return f"Volume: {volume:,.0f} ({note})"


def format_signal_message(signal) -> str:
    arrow = "🟢⬆️" if signal.direction == "BULLISH" else "🔴⬇️"
    return (
        f"{arrow} <b>{signal.symbol}</b> — EMA {signal.direction} crossover\n"
        f"Timeframe: {signal.timeframe}-min | {signal.candle_time.strftime('%Y-%m-%d %H:%M')}\n"
        f"Close: {signal.close:.2f}\n"
        f"EMA9: {signal.ema_fast:.2f}  EMA20: {signal.ema_slow:.2f}\n"
        f"RSI(14): {signal.rsi:.1f}\n"
        f"{_volume_line(signal.volume, signal.prev_volume)}"
    )


def format_bb_signal_message(signal) -> str:
    arrow = "🔴⬇️" if signal.direction == "SELL" else "🟢⬆️"
    band_label = "Upper Band" if signal.direction == "SELL" else "Lower Band"
    return (
        f"{arrow} <b>{signal.symbol}</b> — Bollinger Band {signal.direction} (strong candle)\n"
        f"Timeframe: {signal.timeframe}-min | {signal.candle_time.strftime('%Y-%m-%d %H:%M')}\n"
        f"Close: {signal.close:.2f}  Open: {signal.open:.2f}\n"
        f"{band_label}: {signal.band_level:.2f}\n"
        f"{_volume_line(signal.volume, signal.prev_volume)}"
    )
