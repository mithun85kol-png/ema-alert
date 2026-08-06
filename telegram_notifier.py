import requests
import config


def send_alert(signal):
    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        print("Telegram not configured, skipping send:", signal)
        return

    # 15-min standalone commodity alerts (strategy.check_signals_15min)
    # use a minimal message style — pure EMA9/EMA20 cross, no
    # trend/trade-plan/PCR/pivot/VWAP blocks.
    if signal.get("timeframe") == "15-min":
        _send_15min_alert(signal)
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

    # Trade-plan fields (entry/stop-loss/target/risk-reward) are still
    # computed in strategy.py, just no longer displayed here — kept out
    # per request. Set trade_plan_block back to a non-empty string if
    # you want it shown again.
    trade_plan_block = ""

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

    # VWAP — informational only, one line. Omitted entirely if it
    # couldn't be computed for this candle (see strategy.py).
    vwap = signal.get("vwap")
    vwap_note = signal.get("vwap_note")
    vwap_line = f"VWAP: {vwap} ({vwap_note})\n" if vwap is not None and vwap_note else ""

    # MACD — informational only. Shows raw MACD/Signal values and bias,
    # plus a divergence note if one was detected (see strategy.py).
    macd_line = signal.get("macd_line")
    macd_signal_val = signal.get("macd_signal")
    macd_divergence = signal.get("macd_divergence")
    macd_block = ""
    if macd_line is not None and macd_signal_val is not None:
        macd_bias = "Bullish" if macd_line > macd_signal_val else "Bearish"
        macd_block = f"MACD: {macd_line} / Signal: {macd_signal_val} ({macd_bias})\n"
        if macd_divergence:
            macd_block += f"MACD {macd_divergence}\n"

    # 75-min context — informational only (see strategy.get_75min_trend_info,
    # attached in main.py as signal["trend_75min"]). Shows the bigger-
    # timeframe EMA9/20 bias, whether/when it last crossed, and how
    # close it currently is to crossing. Omitted entirely if there
    # wasn't enough 75-min history warmed up yet for this symbol.
    trend75 = signal.get("trend_75min")
    trend75_block = ""
    if trend75:
        bias_icon = "📈" if trend75["bias"] == "BULLISH" else "📉"
        since = trend75.get("candles_since_cross")
        if since is None:
            cross_note = f"no cross in last {config.TREND_75MIN_LOOKBACK_CANDLES} candles"
        elif since == 0:
            cross_note = "crossed on the latest 75-min candle"
        else:
            cross_note = f"crossed {since} candle(s) ago"
        gap = trend75.get("gap_pct")
        gap_note = f", currently {gap}% apart" if gap is not None else ""
        trend75_block = (
            f"75-min: {trend75['bias']} {bias_icon} "
            f"(EMA9 {trend75['ema_fast']} / EMA20 {trend75['ema_slow']}) — "
            f"{cross_note}{gap_note}\n"
        )

    # Sector index trend (added) — informational only, present only for
    # stocks whose symbol is in config.STOCK_SECTOR_MAP (see main.py).
    # Notes whether the sector is UPTREND/DOWNTREND and whether that
    # agrees with the stock's own EMA50 trend (stock_trend above).
    sector_index = signal.get("sector_index")
    sector_trend = signal.get("sector_trend")
    sector_line = ""
    if sector_index:
        if sector_trend:
            sector_icon = "📈" if sector_trend == "UPTREND" else "📉"
            stock_trend_as_sector = "UPTREND" if stock_trend == "BULLISH" else "DOWNTREND"
            agree_note = (
                " (agrees with stock trend ✅)" if sector_trend == stock_trend_as_sector
                else " (diverges from stock trend ⚠️)"
            )
            sector_line = f"Sector ({sector_index}): {sector_trend} {sector_icon}{agree_note}\n"
        else:
            sector_line = f"Sector ({sector_index}): not enough data yet\n"

    text = (
        f"{arrow} {signal['symbol']} — EMA {direction} crossover\n"
        f"{fno_line}"
        f"Timeframe: 3-min | {date_part} {time_part}\n"
        f"Close: {signal['close']}\n"
        f"EMA9: {signal['ema_fast']}  EMA20: {signal['ema_slow']}\n"
        f"{trend75_block}"
        f"{trade_plan_block}"
        f"RSI(14): {signal.get('rsi', 'N/A')}\n"
        f"{macd_block}"
        f"Trend: {trend_label} {trend_icon}\n"
        f"{sector_line}"
        f"Volume: {volume_str}{vol_note}\n"
        f"{vwap_line}"
        f"{pcr_line}"
        f"{pivot_block}"
        f"Crossing Candle: {signal.get('cross_candle_pattern', 'N/A')}\n"
        f"Previous Candle: {signal.get('prev_candle_pattern', 'N/A')}\n"
    ).rstrip()

    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={
            "chat_id": config.TELEGRAM_CHAT_ID,
            "text": text,
        }, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print("Telegram send failed:", e)


def _send_15min_alert(signal):
    """
    Standalone 15-min COMMODITY crossover alert (GOLD/SILVER/CRUDEOIL
    etc. — see main.py, which only calls check_signals_15min for
    config.COMMODITIES symbols). Same minimal style as the 75-min alert
    — pure EMA9/EMA20 cross, no trend/trade-plan/PCR/pivot/VWAP blocks.
    """
    direction = signal["direction"]
    arrow = "🟢⬆️" if direction == "BULLISH" else "🔴⬇️"

    candle_time = signal["candle_time"]
    date_part, time_part = str(candle_time).split(" ")[0], str(candle_time).split(" ")[1][:5]

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

    text = (
        f"{arrow} {signal['symbol']} — 15-MIN EMA {direction} crossover\n"
        f"Timeframe: 15-min | {date_part} {time_part}\n"
        f"Close: {signal['close']}\n"
        f"EMA9: {signal['ema_fast']}  EMA20: {signal['ema_slow']}\n"
        f"RSI(14): {signal.get('rsi', 'N/A')}\n"
        f"Volume: {volume_str}{vol_note}\n"
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
        print("Telegram send failed (15-min):", e)
