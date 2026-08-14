import requests
import config


def send_alert(signal):
    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        print("Telegram not configured, skipping send:", signal)
        return

    # NOTE (fixed 2026-08-14): there used to be a special case here that
    # routed any signal with timeframe == "15-min" into a minimal,
    # stripped-down message (_send_15min_alert below) — that was built
    # for the OLD standalone commodity 15-min alert
    # (strategy.check_signals_15min), which main.py no longer calls at
    # all. Once 15-min became the PRIMARY/ALERTING timeframe for
    # stocks/commodities/cash (2026-08-13), every real alert started
    # carrying timeframe="15-min" too, so it was silently being
    # downgraded to the minimal message instead of the full one below.
    # Removed — every signal now always gets the full message
    # regardless of its timeframe label. _send_15min_alert is kept
    # (unused) only in case check_signals_15min is ever reintroduced.

    direction = signal["direction"]  # "BULLISH" or "BEARISH"
    arrow = "🟢⬆️" if direction == "BULLISH" else "🔴⬇️"

    # Timeframe label — "15-min" for stocks/commodities/cash (the
    # PRIMARY/ALERTING timeframe, flipped 2026-08-13), "5-min" for
    # indices. Defaults to "15-min" for backward compatibility if an
    # older caller didn't set this.
    timeframe_label = signal.get("timeframe", "15-min")

    candle_time = signal["candle_time"]
    # candle_time expected like "2026-07-29 12:15:00+05:30" -> split date/time
    date_part, time_part = str(candle_time).split(" ")[0], str(candle_time).split(" ")[1][:5]

    # stock_trend (EMA50 trend direction) is still computed/kept on the
    # signal for other logic (e.g. the sector-agreement note below), but
    # per request (2026-08-12) the "Trend: UPTREND/DOWNTREND" line itself
    # is no longer shown in the Telegram message.
    stock_trend = signal.get("stock_trend", "UNKNOWN")

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
    # computed in strategy.py for every signal, but per request
    # (2026-08-12) are no longer shown in the Telegram message. The
    # "🎯 High R:R" header tag is kept — it still reflects whether the
    # signal passed strategy.passes_confluence_filter (see main.py —
    # set as signal["confluence_passed"]) even though the underlying
    # numbers are no longer printed.
    header_tag = " 🎯 High R:R" if signal.get("confluence_passed") else ""
    trade_plan_block = ""

    # PCR (Put-Call Ratio) — informational only, present on index
    # signals (every run) and F&O stock signals (on-demand, see
    # main.py). Not shown for cash-only stocks/commodities (no option
    # chain).
    pcr = signal.get("pcr")
    pcr_line = f"PCR: {pcr}\n" if pcr is not None else ""

    # Call/Put writing buildup (added) — informational only. Indices:
    # computed every run (see main.py's update_oi_buildup), so this is
    # always a tight ~3-min comparison. F&O stocks: computed ON-DEMAND
    # only when this alert fires (see get_stock_oi_buildup), so the
    # comparison window can be minutes to days — oi_buildup_since_hours
    # (stocks only) is shown so that's clear rather than implying a
    # tight window. Omitted entirely if there's no previous snapshot to
    # compare against yet, or nothing crossed the noise threshold.
    oi_buildup = signal.get("oi_buildup")
    oi_buildup_line = ""
    if oi_buildup:
        bias = oi_buildup["bias"]
        bias_icon = "📉" if bias == "BEARISH" else ("📈" if bias == "BULLISH" else "➖")
        since_hours = signal.get("oi_buildup_since_hours")
        since_note = f" (vs snapshot {since_hours}h ago)" if since_hours is not None else ""
        oi_buildup_line = (
            f"OI Buildup: {bias} {bias_icon} ({oi_buildup['note']}){since_note}\n"
            f"  Call writing: {oi_buildup['call_writing_oi']:,}  "
            f"Put writing: {oi_buildup['put_writing_oi']:,}\n"
        )

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

    # Day change % — informational only, vs previous trading day's
    # close (see strategy.py / main.py). Omitted entirely if
    # prev_close wasn't available for this symbol today.
    day_change_pct = signal.get("day_change_pct")
    if day_change_pct is not None:
        day_change_icon = "🟢" if day_change_pct >= 0 else "🔴"
        day_change_line = f"Day Change: {day_change_pct:+.2f}% {day_change_icon}\n"
    else:
        day_change_line = ""

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

    # 75-min context (RELABELED 2026-08-14 — was "15-min:", showing
    # 15-min data under a 75-min alert; now the timeframes flipped, so
    # this block shows 75-min data under a 15-min alert instead — see
    # strategy.get_3min_trend_info, called on df75 in main.py as of
    # 2026-08-13, attached as signal["trend_3min"]). Shows the longer-
    # timeframe EMA9/20 bias, whether/when it last crossed (with an
    # exact timestamp), and how close it currently is to crossing.
    # Omitted entirely if there wasn't enough 75-min history warmed up
    # yet for this symbol.
    trend3 = signal.get("trend_3min")
    trend3_block = ""
    if trend3:
        bias_icon = "📈" if trend3["bias"] == "BULLISH" else "📉"
        since = trend3.get("candles_since_cross")
        cross_time = trend3.get("cross_time")
        # Format the exact crossover timestamp (if we have one) as
        # "YYYY-MM-DD HH:MM" so the message doesn't just say "N candle(s)
        # ago" and force the reader to do the math themselves.
        if cross_time is not None:
            cross_time_str = str(cross_time)
            cross_date_part = cross_time_str.split(" ")[0]
            cross_time_part = cross_time_str.split(" ")[1][:5]
            cross_time_note = f" ({cross_date_part} {cross_time_part})"
        else:
            cross_time_note = ""

        if since is None:
            cross_note = f"no cross in last {config.INFO_3MIN_LOOKBACK_CANDLES} candles"
        elif since == 0:
            cross_note = f"crossed on the latest 75-min candle{cross_time_note}"
        else:
            cross_note = f"crossed {since} candle(s) ago{cross_time_note}"
        gap = trend3.get("gap_pct")
        gap_note = f", currently {gap}% apart" if gap is not None else ""
        trend3_fast_p = trend3.get("ema_fast_period", 9)
        trend3_slow_p = trend3.get("ema_slow_period", 20)
        trend3_block = (
            f"75-min: {trend3['bias']} {bias_icon} "
            f"(EMA{trend3_fast_p} {trend3['ema_fast']} / EMA{trend3_slow_p} {trend3['ema_slow']}) — "
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

    # Delivery % — informational only, previous trading day's NSE
    # delivery percentage (see main.py / delivery_data.py). Stocks
    # only — indices/commodities never set this key, so it's simply
    # omitted for them. None if the bhavcopy fetch failed/skipped.
    delivery_pct = signal.get("delivery_pct")
    delivery_line = f"Delivery % (prev day): {delivery_pct}%\n" if delivery_pct is not None else ""

    # Bulk/Block deals (added) — informational only, stocks only (see
    # main.py / bulk_block_data.py). Shows each large-trade entry NSE
    # published for this symbol on the most recent trading day, if
    # any — client name, Buy/Sell, quantity, price, and whether it was
    # a Bulk or Block deal. Omitted entirely if none were found (most
    # symbols, most days) or the fetch failed.
    # Last Bulk/Block deal (added) — informational only, stocks only
    # (see main.py / bulk_block_data.py). The single most recent
    # Bulk/Block deal NSE has published for this symbol, whenever it
    # was (searched over bulk_block_data.LOOKBACK_DAYS) — not
    # restricted to today. Omitted entirely if none was found in that
    # window, or the fetch failed/got blocked.
    last_deal = signal.get("last_bulk_block_deal")
    bulk_block_block = ""
    if last_deal:
        side_icon = "🟢" if last_deal["buy_sell"] == "BUY" else ("🔴" if last_deal["buy_sell"] == "SELL" else "➖")
        bulk_block_block = (
            f"Last Bulk/Block Deal: {last_deal['date']} [{last_deal['type']}]\n"
            f"  {last_deal['client']} {last_deal['buy_sell']} {side_icon} "
            f"{last_deal['quantity']} @ {last_deal['price']}\n"
        )

    # EMA period labels — default to 9/20 (F&O scan) for backward
    # compatibility if an older caller didn't set these; the Nifty 500
    # cash scan sets them to 9/21 (see strategy.py / main.py).
    ema_fast_p = signal.get("ema_fast_period", 9)
    ema_slow_p = signal.get("ema_slow_period", 20)

    text = (
        f"{arrow} <b>{signal['symbol']}</b> — EMA {direction} crossover ({timeframe_label}){header_tag}\n"
        f"{fno_line}"
        f"Timeframe: {timeframe_label} | {date_part} {time_part}\n"
        f"Close: {signal['close']}\n"
        f"{day_change_line}"
        f"EMA{ema_fast_p}: {signal['ema_fast']}  EMA{ema_slow_p}: {signal['ema_slow']}\n"
        f"{vwap_line}"
        f"Volume: {volume_str}{vol_note}\n"
        f"{delivery_line}"
        f"{bulk_block_block}"
        f"{sector_line}"
        f"{trade_plan_block}"
        f"RSI(14): {signal.get('rsi', 'N/A')}\n"
        f"{pcr_line}"
        f"{oi_buildup_line}"
        f"Crossing Candle: {signal.get('cross_candle_pattern', 'N/A')}\n"
        f"Previous Candle: {signal.get('prev_candle_pattern', 'N/A')}\n"
        f"{trend3_block}"
    ).rstrip()

    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={
            "chat_id": config.TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
        }, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print("Telegram send failed:", e)


def _send_15min_alert(signal):
    """
    UNUSED as of 2026-08-14 (no longer called by send_alert — see the
    NOTE at the top of send_alert). Kept only in case
    strategy.check_signals_15min is ever reintroduced for a genuine
    standalone-minimal alert. Was the standalone 15-min COMMODITY
    crossover alert style (GOLD/SILVER/CRUDEOIL etc.) — pure EMA9/EMA20
    cross, no trend/trade-plan/PCR/pivot/VWAP blocks.
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
