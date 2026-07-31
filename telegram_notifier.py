import requests
import config


def send_alert(signal):
    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        print("Telegram not configured, skipping send:", signal)
        return

    direction = signal["direction"]  # "BULLISH" or "BEARISH"
    arrow = "🟢⬆️" if direction == "BULLISH" else "🔴⬇️"

    candle_time = signal["candle_time"]
    # candle_time expected like "2026-07-29 12:15:00+05:30" -> split date/time
    date_part, time_part = str(candle_time).split(" ")[0], str(candle_time).split(" ")[1][:5]

    stock_trend = signal.get("stock_trend", "UNKNOWN")
    trend_icon = "📈" if stock_trend == "BULLISH" else "📉"
    trend_label = "UPTREND" if stock_trend == "BULLISH" else "DOWNTREND"

    volume = signal["volume"]
    volume_str = f"{volume:,}"

    vol_change = signal.get("vol_change_pct")
    if vol_change is None:
        vol_note = ""
    elif vol_change >= 0:
        vol_note = f"({vol_change:.1f}% higher than previous ⬆️)"
    else:
        vol_note = f"({abs(vol_change):.1f}% lower than previous ⬇️)"

    pivot_note = signal.get("pivot_note")
    pivot_line = f"Pivot: {pivot_note}\n" if pivot_note else ""

    text = (
        f"{arrow} {signal['symbol']} — EMA {direction} crossover\n"
        f"Timeframe: 5-min | {date_part} {time_part}\n"
        f"Close: {signal['close']}\n"
        f"EMA9: {signal['ema_fast']}  EMA20: {signal['ema_slow']}\n"
        f"RSI(14): {signal.get('rsi', 'N/A')}\n"
        f"Trend: {trend_label} {trend_icon}\n"
        f"Volume: {volume_str} {vol_note}\n"
        f"{pivot_line}"
        f"Crossing Candle: {signal.get('cross_candle_pattern', 'N/A')}\n"
        f"Previous Candle: {signal.get('prev_candle_pattern', 'N/A')}"
    )

    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={
            "chat_id": config.TELEGRAM_CHAT_ID,
            "text": text,
        }, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print("Telegram send failed:", e)
