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
    elif vol_change > 0:
        vol_note = " (Higher than previous ⬆️)"
    elif vol_change < 0:
        vol_note = " (Lower than previous ⬇️)"
    else:
        vol_note = " (Same as previous)"

    # Informational trade-plan block: entry = cross candle close, stop
    # loss = EMA20, target = trailing 15-min high (bullish) / low
    # (bearish). Purely informational — not a recommendation, just the
    # rule you asked to see spelled out on every alert.
    entry = signal.get("entry")
    stop_loss = signal.get("stop_loss")
    target = signal.get("target")
    risk_reward = signal.get("risk_reward")
    target_label = "15-min High" if direction == "BULLISH" else "15-min Low"
    trade_plan_block = ""
    if entry is not None and stop_loss is not None and target is not None:
        trade_plan_block = (
            f"Entry: {entry}\n"
            f"Stop Loss (EMA20): {stop_loss}\n"
            f"Target ({target_label}): {target}\n"
        )
        if risk_reward is not None:
            trade_plan_block += f"Risk:Reward = 1:{risk_reward}\n"

    # PCR (Put-Call Ratio) — informational only, present only on index
    # signals (see main.py). Not shown for stocks/commodities.
    pcr = signal.get("pcr")
    pcr_line = f"PCR: {pcr}\n" if pcr is not None else ""

    # F&O / Cash flag — shown at the TOP of the message, right under the
    # header line. Only present for actual stock signals (both the F&O
    # scan's stocks and the Nifty 500 scan's stocks) — indices and
    # commodities never set this key, so no F&O line is shown for them.
    is_fno = signal.get("is_fno")
    if is_fno is True:
        fno_line = "F&O: Yes ✅\n"
    elif is_fno is False:
        fno_line = "F&O: No (Cash only) 💵\n"
    else:
        fno_line = ""

    r3 = signal.get("r3")
    s3 = signal.get("s3")
    pivot_note = signal.get("pivot_note")

    if r3 is not None and s3 is not None:
        pivot_block = f"R3: {r3}  S3: {s3}\n"
        if pivot_note:
            pivot_block += f"Pivot: {pivot_note}\n"
    else:
        pivot_block = ""

    # 75-min informative trend — purely contextual, never blocks the
    # 3-min signal from firing. Only shown when trend_75min was
    # successfully computed and attached to the signal (see main.py).
    # Flags agreement/disagreement with the 3-min cross direction so
    # it's easy to spot at a glance.
    trend_75 = signal.get("trend_75min")
    trend_75_block = ""
    if trend_75:
        bias_75 = trend_75.get("bias")
        candles_since = trend_75.get("candles_since_cross")
        agree_icon = "✅" if bias_75 == direction else "⚠️"
        if candles_since is not None:
            cross_note = f"crossed {candles_since} candle(s) ago" if candles_since > 0 else "crossed this candle"
        else:
            cross_note = "no recent cross"
        trend_75_block = (
            f"────────────────\n"
            f"75-min trend (info): {bias_75} {agree_icon} ({cross_note})\n"
            f"────────────────\n"
        )

    text = (
        f"{arrow} {signal['symbol']} — EMA {direction} crossover\n"
        f"{fno_line}"
        f"Timeframe: 3-min | {date_part} {time_part}\n"
        f"Close: {signal['close']}\n"
        f"EMA9: {signal['ema_fast']}  EMA20: {signal['ema_slow']}\n"
        f"{trade_plan_block}"
        f"RSI(14): {signal.get('rsi', 'N/A')}\n"
        f"Trend: {trend_label} {trend_icon}\n"
        f"Volume: {volume_str}{vol_note}\n"
        f"{pcr_line}"
        f"{pivot_block}"
        f"{trend_75_block}"
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
