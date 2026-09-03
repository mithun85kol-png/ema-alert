import requests
import config


def send_alert(signal):
    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        print("Telegram not configured, skipping send:", signal)
        return

    # NOTE: every signal always gets this full message regardless of
    # its timeframe label — there is no separate stripped-down message
    # path anymore. _send_15min_alert below is kept unused, only in
    # case a standalone minimal alert is ever wanted again.

    direction = signal["direction"]  # "BULLISH" or "BEARISH"
    arrow = "🟢⬆️" if direction == "BULLISH" else "🔴⬇️"

    # Timeframe label — set by main.py per signal: "15-min" or
    # "75-min" for stocks/commodities/cash (whichever
    # config.PRIMARY_TIMEFRAME currently selects), "15-min" for
    # indices too (CHANGED from 5-min, per request). Defaults to
    # "75-min" for backward compatibility if an
    # older caller didn't set this.
    timeframe_label = signal.get("timeframe", "75-min")

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
        vol_note = f" (+{vol_change}% vs previous ⬆️)"
    elif vol_change < 0:
        vol_note = f" ({vol_change}% vs previous ⬇️)"
    else:
        vol_note = " (Same as previous)"

    # Day Volume vs 1-month average (ADDED, per request, 2026-09-01 —
    # "volume alert tao kore dao same [as delivery]") — same "today's
    # own value vs this stock's own trailing 1-month average" style as
    # Delivery %, but for the DAY's total volume (previous completed
    # trading day), not this candle's own (much smaller) volume above.
    # Omitted entirely if the 1-month average isn't available.
    prev_day_volume = signal.get("prev_day_volume")
    volume_avg_1m = signal.get("volume_avg_1m")
    if prev_day_volume is not None and volume_avg_1m:
        day_vol_vs_avg_pct = (prev_day_volume - volume_avg_1m) / volume_avg_1m * 100.0
        sign = "+" if day_vol_vs_avg_pct >= 0 else ""
        day_volume_line = (
            f"Day Volume (prev day): {prev_day_volume:,} "
            f"(1M avg: {volume_avg_1m:,.0f}, {sign}{day_vol_vs_avg_pct:.1f}% vs avg)\n"
        )
    else:
        day_volume_line = ""

    # Trade-plan fields (entry/stop-loss/target/risk-reward) are still
    # computed in strategy.py for every signal, but per request
    # (2026-08-12) are no longer shown in the Telegram message. The
    # "🎯 High R:R" header tag is kept — it still reflects whether the
    # signal passed strategy.passes_confluence_filter (see main.py —
    # set as signal["confluence_passed"]) even though the underlying
    # numbers are no longer printed.
    # REMOVED (per request) — "🎯 High R:R" was tied to
    # signal["confluence_passed"], but config.CONFLUENCE_FILTER_ENABLED
    # is now False so it was showing on every single alert (vestigial).
    header_tag = ""
    smart_money = signal.get("smart_money")
    if smart_money:
        header_tag += " 🐋 Smart Money"
    # Volume Spike (SHORTENED, per request): now a single icon in the
    # header instead of its own line — it's a required/blocking
    # condition now (see config.REQUIRE_VOLUME_SPIKE), so on any alert
    # that reaches this point it's already Yes (for stocks/
    # commodities; indices aren't gated on it, so it can still be
    # False/None there, in which case the icon is simply omitted).
    if signal.get("volume_spike") is True:
        header_tag += " 📊"
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
        # SHORTENED (per request): merged to one line, dropped the raw
        # Call/Put writing OI numbers (the bias + note already say
        # what matters).
        oi_buildup_line = f"OI Buildup: {bias} {bias_icon} ({oi_buildup['note']}){since_note}\n"

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

    # 15-min context (RELABELED back 2026-08-14 — reverted to "15-min:",
    # showing 15-min data under a 75-min alert, via
    # strategy.get_3min_trend_info, called on df15 in main.py, attached
    # as signal["trend_3min"]). Shows the 15-min EMA9/20 bias,
    # whether/when it last crossed (with an exact timestamp), and how
    # close it currently is to crossing. Omitted entirely if there
    # wasn't enough 15-min history warmed up yet for this symbol.
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

        info_label = signal.get("info_timeframe_label", "15-min")
        if since is None:
            cross_note = f"no cross in last {config.INFO_3MIN_LOOKBACK_CANDLES} candles"
        elif since == 0:
            cross_note = f"crossed on the latest {info_label} candle{cross_time_note}"
        else:
            cross_note = f"crossed {since} candle(s) ago{cross_time_note}"
        gap = trend3.get("gap_pct")
        gap_note = f", currently {gap}% apart" if gap is not None else ""
        trend3_fast_p = trend3.get("ema_fast_period", 9)
        trend3_slow_p = trend3.get("ema_slow_period", 20)
        # info_timeframe_label reflects whichever timeframe is CURRENTLY
        # informational (the opposite of config.PRIMARY_TIMEFRAME) —
        # set by main.py. Falls back to "15-min" for backward
        # compatibility if an older caller didn't set it (already
        # resolved above, next to cross_note).
        trend3_block = (
            f"{info_label}: {trend3['bias']} {bias_icon} "
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
    # SHORTENED (per request): dropped the "not enough data yet" line
    # (shown only when there's something to say) and the "agrees/
    # diverges" phrase, kept as a short ✅/⚠️ icon instead.
    if sector_index and sector_trend:
        sector_icon = "📈" if sector_trend == "UPTREND" else "📉"
        stock_trend_as_sector = "UPTREND" if stock_trend == "BULLISH" else "DOWNTREND"
        agree_icon = "✅" if sector_trend == stock_trend_as_sector else "⚠️"
        sector_line = f"Sector: {sector_trend} {sector_icon} {agree_icon}\n"

    # Delivery % — informational only, previous trading day's NSE
    # delivery percentage (see main.py / delivery_data.py). Stocks
    # only — indices/commodities never set this key, so it's simply
    # omitted for them. None if the bhavcopy fetch failed/skipped.
    delivery_pct = signal.get("delivery_pct")
    # 1-month average comparison (ADDED, per request, 2026-09-01 —
    # "delivery ota last one month average theke koto besi kore dite
    # parbe?") — signal["delivery_avg_1m"] is that SAME symbol's own
    # trailing ~1-month average delivery % (see delivery_data.py's
    # get_delivery_avg_1m), so this shows how today's delivery %
    # compares to that stock's own recent norm, not a market-wide
    # figure. Omitted (falls back to just the plain % line, same as
    # before) if the average isn't available for this symbol.
    delivery_avg_1m = signal.get("delivery_avg_1m")
    if delivery_pct is not None and delivery_avg_1m:
        vs_avg_pct = (delivery_pct - delivery_avg_1m) / delivery_avg_1m * 100.0
        sign = "+" if vs_avg_pct >= 0 else ""
        delivery_line = (
            f"Delivery % (prev day): {delivery_pct}% "
            f"(1M avg: {delivery_avg_1m:.1f}%, {sign}{vs_avg_pct:.1f}% vs avg)\n"
        )
    elif delivery_pct is not None:
        delivery_line = f"Delivery % (prev day): {delivery_pct}%\n"
    else:
        delivery_line = ""

    # Momentum (added) — informational only. True when the alert's
    # close price is above the highest DAILY CLOSE over the last
    # ~4 weeks (20 completed trading days) — see
    # main.py's build_momentum_volume_data. Omitted entirely if there
    # wasn't enough daily history to compute it for this symbol today.
    # SHORTENED (per request): one compact line instead of a full
    # sentence, and skipped entirely when False (a "No" line rarely
    # changes the reader's decision — only shown when it's actually
    # above the 4-week high, worth flagging).
    momentum = signal.get("momentum")
    four_week_high = signal.get("four_week_high_close")
    if momentum is True:
        momentum_line = f"Momentum: 🚀 Above 4-wk high ({four_week_high})\n"
    else:
        momentum_line = ""

    # "Last High" 1-6 month (added, per request) — highest daily HIGH
    # over the trailing N months, see main.py's build_momentum_volume_data
    # (MONTHLY_HIGH_LOOKBACKS_TRADING_DAYS). One compact line, months
    # separated by " | " — a month is simply skipped if there wasn't
    # enough daily history yet to compute it for this symbol.
    multi_month_highs = signal.get("multi_month_highs")
    multi_month_high_line = ""
    if multi_month_highs:
        parts = [f"{m}M {multi_month_highs[m]}" for m in sorted(multi_month_highs)]
        multi_month_high_line = "Last High: " + " | ".join(parts) + "\n"

    # Near N-month High (added, per request) — see
    # strategy.compute_near_high_score; only shown when close is
    # actually within config.NEAR_HIGH_THRESHOLD_PCT (5%) of the
    # nearest month's high (score==1) — omitted otherwise, same
    # "only show when it changes the reader's decision" rule as
    # Momentum above.
    near_high = signal.get("near_high")
    near_high_line = ""
    if near_high and near_high["score"] == 1:
        gap = near_high["gap_pct"]
        gap_note = f"{abs(gap):.1f}% below" if gap < 0 else f"{gap:.1f}% above"
        near_high_line = f"🎯 Near {near_high['nearest_month']}M high ({gap_note} {near_high['nearest_high']})\n"

    # Volume Spike (SHORTENED, per request) — moved to a header icon
    # (📊, see header_tag above) instead of its own line.

    # EMA50/200 daily cross (added) — informational only, the classic
    # Golden Cross (EMA50 above EMA200 = long-term bullish) / Death
    # Cross (below = long-term bearish), on DAILY closes — see main.py's
    # build_momentum_volume_data / _compute_ema50_200_cross. Omitted
    # entirely if there wasn't enough daily history (200+ days) yet for
    # this symbol.
    ema_cross = signal.get("ema_cross")
    if ema_cross:
        ema_cross_icon = "📈" if ema_cross["bias"] == "BULLISH" else "📉"
        if ema_cross.get("cross_date"):
            ema_cross_note = f"last crossed {ema_cross['cross_date']}"
        else:
            ema_cross_note = "no cross in available history"
        ema_cross_line = (
            f"EMA50/200 (daily): {ema_cross['bias']} {ema_cross_icon} — {ema_cross_note}\n"
        )
    else:
        ema_cross_line = ""

    # "Smart Money Entry" 🐋 (added) — informational only, see
    # strategy.compute_smart_money_signal / config.SMART_MONEY_*.
    # SHORTENED (per request, 2026-08-25 — "smart money te oto detail
    # dorkar nei just score set kore dao, jate alert choto hoy"):
    # score only now, the itemized reasons list is dropped from the
    # message (strategy.compute_smart_money_signal still computes and
    # attaches "reasons" on the signal dict, just not rendered here).
    smart_money_block = ""
    if smart_money:
        smart_money_block = f"🐋 Smart Money: {smart_money['score']}/{smart_money['possible']}\n"

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
        # SHORTENED (per request): merged to one line.
        bulk_block_block = (
            f"Bulk/Block ({last_deal['date']}): {last_deal['client']} "
            f"{last_deal['buy_sell']} {side_icon} {last_deal['quantity']} @ {last_deal['price']}\n"
        )

    # EMA period labels — default to 9/20 (F&O scan) for backward
    # compatibility if an older caller didn't set these; the Nifty 500
    # cash scan sets them to 9/21 (see strategy.py / main.py).
    ema_fast_p = signal.get("ema_fast_period", 9)
    ema_slow_p = signal.get("ema_slow_period", 20)

    # Chart link (added) — TradingView deep link, set by main.py's
    # build_chart_link() for every signal (indices, stocks,
    # commodities alike). Omitted if an older caller didn't set it.
    chart_link = signal.get("chart_link")
    chart_line = f"📈 <a href=\"{chart_link}\">Open Chart (TradingView)</a>\n" if chart_link else ""

    # Angel One link removed per request (2026-08-18).

    # SHORTENED (per request): merged the "Timeframe: X | date time"
    # line with "Close: Y" into one line -- the header already states
    # the timeframe, so repeating it as its own line was redundant.
    # Trade Signal Score (added, per request) — rolls up every
    # already-shown quality signal (Confluence, Volume Spike, Momentum,
    # EMA50/200, Sector, OI Buildup, VWAP cushion, Bulk/Block, candle
    # volume, other-timeframe agreement) into one headline /10 number —
    # see strategy.compute_trade_score. Shown right under the header so
    # it's the first thing read, before any of the individual detail
    # lines below. Icon reflects the score band: 7-10 strong, 5-6
    # decent, 0-4 weak.
    # Trade Score REPLACED by the simple BUY/SELL Setup line below
    # (per request — "simple rakho, trade score replace koro buy sell
    # diye"). trade_score is still computed/attached on the signal
    # (main.py/strategy.py unchanged) — just not rendered here anymore.
    trade_score = signal.get("trade_score")
    trade_score_line = ""

    # Trading Score (added, per request, 2026-08-25 — "sob miliye
    # ekta trading score generate koro") — ONE combined /10 number
    # rolling up Buy/Sell Score + Daily Score + Smart Money (when
    # present). See strategy.compute_trading_score for how it's
    # derived. Shown at the very top of the scores group, above the
    # individual Buy/Sell Score, Daily Score and Smart Money lines,
    # so it's the single number to check first before deciding
    # whether to take the trade.
    trading_score = signal.get("trading_score")
    trading_score_line = ""
    if trading_score:
        ts_val = trading_score["score"]
        if ts_val >= 8:
            ts_icon = "🔥"
        elif ts_val >= 6:
            ts_icon = "✅"
        elif ts_val >= 4:
            ts_icon = "⚠️"
        else:
            ts_icon = "🚫"
        trading_score_line = (
            f"{ts_icon} <b>Trading Score: {trading_score['score']}/10 "
            f"({trading_score['label']})</b>\n"
        )

    # Alert Gate trigger reasons (SIMPLIFIED, per request, 2026-08-28
    # — see strategy.passes_alert_gate) — shown as a short "Trigger:"
    # line right under Trading Score. Now always just the single
    # Trading Score GOOD/STRONG reason (the gate was simplified from
    # its earlier 7-case OR-gate down to one condition), kept as a
    # list/join for forward compatibility if the gate logic changes
    # again. Empty when the gate is disabled
    # (config.QUALITY_GATE_ENABLED = False) since gate_reasons is
    # never set in that case.
    gate_reasons = signal.get("alert_gate_reasons")
    gate_reasons_line = ""
    if gate_reasons:
        gate_reasons_line = f"🎯 Trigger: {', '.join(gate_reasons)}\n"

    # 15-Minute Intraday Trade Checklist (added, per request) — now
    # rendered as ONE simple line (setup direction + score), replacing
    # Trade Score above, instead of the full itemized checkbox block
    # from the earlier version. See strategy.compute_intraday_checklist
    # for how the score itself is derived.
    # RENAMED (per request, 2026-08-25 — "buy setup change kore buy
    # score koro"): "BUY SETUP"/"SELL SETUP" -> "Buy Score"/"Sell
    # Score". Same underlying strategy.compute_intraday_checklist
    # /10 value, just relabeled to read as a score alongside Daily
    # Score / Smart Money / Trading Score, which are all grouped
    # together near the top of the message now (see `text = (...)`
    # below).
    checklist = signal.get("intraday_checklist")
    checklist_block = ""
    if checklist:
        is_bullish = signal["direction"] == "BULLISH"
        setup_icon = "🟢" if is_bullish else "🔴"
        setup_label = "Buy Score" if is_bullish else "Sell Score"
        checklist_block = f"{setup_icon} {setup_label}: <b>{checklist['label']}</b>\n"

    # Daily Score (added, per request) — fixed 7-point bullish-quality
    # checklist (Close>VWAP, EMA9>21>50 stack, RSI 50-70 band, Volume
    # >1.5x its 20-SMA, Close > previous candle's High) — see
    # strategy.compute_daily_score. Always out of a flat 7 (unlike
    # Trade Score's variable "possible"), shown right under it.
    daily_score = signal.get("daily_score")
    daily_score_line = ""
    if daily_score:
        if daily_score["score"] >= 7:
            ds_icon = "🔥"
        elif daily_score["score"] >= 5:
            ds_icon = "✅"
        else:
            ds_icon = "⚠️"
        daily_score_line = f"{ds_icon} Daily Score: <b>{daily_score['label']}</b>\n"

    # Trendline break line removed from the EMA-cross alert (per
    # request) — strategy.detect_trendline_break still runs and is
    # still attached as signal["trendline_break"] (other code may read
    # it), it's just no longer rendered here. The standalone Trendline
    # Break alert below (send_trendline_alert) is separately gated off
    # by config.ENABLE_TRENDLINE_ALERTS in main.py.
    trendline_line = ""

    # Opening 15-min candle bias — see strategy.get_opening_candle_bias
    # / config.OPENING_CANDLE_BIAS_ENABLED. CHANGED (per request):
    # always shown now, even on the normal/None case (price moved both
    # above and below the day's open in the first 15 minutes) — shown
    # as "Neutral" instead of omitting the line. Placed right under
    # Daily Score in the message body (see `text = (...)` below).
    # CHANGED (per request): this line is now shown on EVERY alert,
    # not just when a bias was actually detected — a Neutral/N/A
    # reading is shown explicitly instead of omitting the line.
    opening_bias = signal.get("opening_candle_bias")
    if opening_bias == "BULLISH":
        opening_bias_line = "🟢 Opening 15-min: BULLISH (Open = Low)\n"
    elif opening_bias == "BEARISH":
        opening_bias_line = "🔴 Opening 15-min: BEARISH (Open = High)\n"
    else:
        # None covers both a genuinely neutral first 15-min candle
        # (price moved both above and below open) and the rare case
        # today's first 15-min candle isn't in df15 yet.
        opening_bias_line = "⚪ Opening 15-min: Neutral\n"

    # 1st 15-min Buy/Sell volume ESTIMATE (added, per request) — NOT
    # real order-flow data, see strategy.get_opening_candle_buy_sell_estimate
    # docstring for the (Chaikin-style) approximation used. Omitted
    # entirely if unavailable (zero-range candle, no data yet, or an
    # older signal dict without this field).
    opening_buy_sell = signal.get("opening_buy_sell")
    opening_buy_sell_line = ""
    if opening_buy_sell:
        opening_buy_sell_line = (
            f"📐 1st 15-min Buy/Sell (est.): "
            f"Buy {opening_buy_sell['buy_volume']:,} / "
            f"Sell {opening_buy_sell['sell_volume']:,} "
            f"({opening_buy_sell['buy_pct']}% buy)\n"
        )

    # Message layout (reordered per request, 2026-08-25):
    #   1. Header line
    #   2. F&O/Cash flag — moved to the very top, right under the
    #      header, instead of down near the chart link.
    #   3. Scores, all grouped together: Trading Score (combined)
    #      first, then Buy/Sell Score, Daily Score, and Smart Money
    #      (moved up from its old spot near trend3/bulk-block/sector
    #      further down) — one place to read every score at a glance.
    #   4. Everything else, unchanged order.
    # Message layout — SHORTENED (per request, 2026-08-30 — "message
    # gulo lomba asche, aro length choto koro"). Visible-by-default part
    # trimmed down to just the "read this in 2 seconds" essentials
    # (header, F&O flag, Trading Score, chart link, close/date, day
    # change) — everything else moved inside Telegram "expandable
    # blockquote" tags (<blockquote expandable>, HTML parse mode).
    #
    # SPLIT INTO TWO SEPARATE TAPS (per request, 2026-09-03 — "volume
    # ar delivery ta alada tap banao") — Volume/Day Volume/Delivery %
    # (the three "how much is actually trading/holding" numbers,
    # including the two 1-month-average comparisons) now sit in their
    # OWN expandable block, separate from the rest of the scoring/
    # trend detail block. Two independent taps, so opening one doesn't
    # force the other one open too.
    volume_delivery_block = (
        f"Volume: {volume_str}{vol_note}\n"
        f"{day_volume_line}"
        f"{delivery_line}"
    ).rstrip()

    details_block = (
        f"{gate_reasons_line}"
        f"{smart_money_block}"
        f"{opening_bias_line}"
        f"{opening_buy_sell_line}"
        f"{trendline_line}"
        f"{vwap_line}"
        f"{momentum_line}"
        f"{multi_month_high_line}"
        f"{near_high_line}"
        f"{ema_cross_line}"
        f"{trend3_block}"
        f"{bulk_block_block}"
        f"{sector_line}"
        f"{trade_plan_block}"
        f"RSI(14): {signal.get('rsi', 'N/A')}\n"
        f"{pcr_line}"
        f"{oi_buildup_line}"
    ).rstrip()

    volume_delivery_tap = (
        f"<blockquote expandable>{volume_delivery_block}</blockquote>\n"
        if volume_delivery_block else ""
    )

    text = (
        f"{arrow} <b>{signal['symbol']}</b> — EMA {direction} crossover ({timeframe_label}){header_tag}\n"
        f"{fno_line}"
        f"{trade_score_line}"
        f"{trading_score_line}"
        f"{checklist_block}"
        f"{daily_score_line}"
        f"{chart_line}"
        f"{date_part} {time_part} | Close: {signal['close']}\n"
        f"{day_change_line}"
        f"{volume_delivery_tap}"
        f"<blockquote expandable>{details_block}</blockquote>"
    ).rstrip()

    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={
            "chat_id": config.TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print("Telegram send failed:", e)


def send_opening_bias_report(bullish, bearish, no_data, now_ist, bullish_links=None, bearish_links=None, pct_changes=None):
    """
    NEW (per request) — SCAN_MODE=opening_bias_report. ONE consolidated
    message listing every F&O stock whose today's first 15-min candle
    was a clean Open==Low (bullish) or Open==High (bearish). Same
    always-send principle as send_ema_cross_report: sends something
    even when both lists are empty, so a scheduled run never looks
    like it might have silently failed.

    Chart links (added, per request) — bullish_links/bearish_links are
    optional {symbol: url} maps (see main.py's build_chart_link); each
    symbol becomes a tappable TradingView link instead of plain text.
    Defaults to None so an older caller without the links still works
    (falls back to plain symbol names, comma-joined, same as before).

    pct_changes (ADDED, per request, 2026-09-01 — "stocke er pase
    change ana jabe?") — optional {symbol: pct} of that first 15-min
    candle's close vs previous day's close (see main.py's
    build_todays_opening_bias_list). When present, each line shows
    "SYMBOL +x.xx%" / "SYMBOL -x.xx%" instead of just the symbol name;
    a symbol missing from this dict (pivot data unavailable) just
    shows without a % suffix, same graceful-omission pattern as the
    chart links.
    """
    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        print("Telegram not configured, skipping send:", bullish, bearish)
        return

    bullish_links = bullish_links or {}
    bearish_links = bearish_links or {}
    pct_changes = pct_changes or {}

    def _fmt(symbols, links):
        if not links and not pct_changes:
            return ", ".join(symbols)

        def _one(sym):
            label = f"<a href=\"{links[sym]}\">{sym}</a>" if sym in links else sym
            if sym in pct_changes:
                pct = pct_changes[sym]
                sign = "+" if pct >= 0 else ""
                label += f" {sign}{pct:.2f}%"
            return f"• {label}"

        return "\n".join(_one(sym) for sym in symbols)

    date_str = now_ist.strftime("%Y-%m-%d")
    time_str = now_ist.strftime("%H:%M")

    lines = [f"📐 <b>F&O Opening 15-min Bias</b> — {date_str} {time_str}\n"]

    if bullish:
        lines.append(f"🟢 <b>Open = Low</b> ({len(bullish)}):")
        lines.append(_fmt(bullish, bullish_links))
        lines.append("")

    if bearish:
        lines.append(f"🔴 <b>Open = High</b> ({len(bearish)}):")
        lines.append(_fmt(bearish, bearish_links))
        lines.append("")

    if not bullish and not bearish:
        lines.append("No clean Open=Low / Open=High candles today.")
        lines.append("")

    if no_data:
        lines.append(f"⚪ No data ({len(no_data)}): {', '.join(no_data)}")

    text = "\n".join(lines).rstrip()

    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={
            "chat_id": config.TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print("Telegram send failed (opening_bias_report):", e)


def send_ema_cross_report(crosses, now_ist, period_label="Fresh Cross"):
    """
    RE-ADDED (was missing from this checkout — see chat) — the
    standalone "EMA50/200 (Golden/Death Cross) + Delivery%" report.
    ONE message covering every symbol with a fresh cross (see main.py's
    build_todays_ema_cross_list / build_todays_ema_cross_list_evening
    for exactly what "fresh" means for each). Always sends something,
    even when the list is empty, so a scheduled run never looks like it
    might have silently failed.

    period_label (added, per request — "sokale gotokal, bikale aajker
    cross chai") distinguishes the morning run (reports crosses on the
    most recent COMPLETED daily candle — i.e. yesterday's close, since
    today's candle doesn't exist yet at market open) from the evening
    run (reports crosses specifically on TODAY's own daily candle, only
    if Upstox has already finalized it by send time — see
    build_todays_ema_cross_list_evening's docstring for why that's not
    always guaranteed). Defaults to the generic "Fresh Cross" for
    backward compatibility with any other caller.
    """
    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        print("Telegram not configured, skipping send:", crosses)
        return

    date_str = now_ist.strftime("%Y-%m-%d")
    time_str = now_ist.strftime("%H:%M")

    if not crosses:
        text = (
            f"📊 <b>EMA50/200 Cross Report — {period_label}</b> — {date_str} {time_str}\n"
            f"No fresh Golden/Death Cross ({period_label.lower()})."
        )
    else:
        lines = [f"📊 <b>EMA50/200 Cross Report — {period_label}</b> — {date_str} {time_str}\n"]
        golden = [c for c in crosses if c["bias"] == "BULLISH"]
        death = [c for c in crosses if c["bias"] == "BEARISH"]

        if golden:
            lines.append("🟢 <b>Golden Cross</b> (EMA50 crossed above EMA200):")
            for c in golden:
                deliv = f"{c['delivery_pct']:.1f}%" if c["delivery_pct"] is not None else "N/A"
                lines.append(
                    f"  • <b>{c['symbol']}</b> — EMA50 {c['ema50']} / EMA200 {c['ema200']} "
                    f"| Delivery: {deliv} | {c['cross_date']}"
                )
            lines.append("")

        if death:
            lines.append("🔴 <b>Death Cross</b> (EMA50 crossed below EMA200):")
            for c in death:
                deliv = f"{c['delivery_pct']:.1f}%" if c["delivery_pct"] is not None else "N/A"
                lines.append(
                    f"  • <b>{c['symbol']}</b> — EMA50 {c['ema50']} / EMA200 {c['ema200']} "
                    f"| Delivery: {deliv} | {c['cross_date']}"
                )

        text = "\n".join(lines).rstrip()

    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={
            "chat_id": config.TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
        }, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print("Telegram send failed (ema_cross_report):", e)


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


def send_breakout_alert(signal):
    """
    Breakout scan alert (added 2026-08-18) — sent by
    main.run_breakout_scan when a stock passes all 12 conditions of
    strategy.check_breakout_scan (Row 2/Market Cap omitted, see chat).
    Chart link always opens on the 15-min interval, same convention as
    send_alert (see build_chart_link in main.py). Link preview
    disabled, same as send_alert.
    """
    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        print("Telegram not configured, skipping send:", signal)
        return

    symbol = signal["symbol"]
    chart_link = signal.get("chart_link")
    chart_line = f"📈 <a href=\"{chart_link}\">Open Chart (TradingView)</a>\n" if chart_link else ""

    text = (
        f"🚀 <b>{symbol}</b> — Breakout Scan match\n"
        f"{chart_line}"
        f"Date: {signal['date']}\n"
        f"Close: {signal['close']}\n"
        f"Volume: {signal['volume']:,}\n"
        f"Turnover: ₹{signal['turnover_cr']:,.1f} Cr\n"
        f"SMA50: {signal['sma50']}  |  SMA200: {signal['sma200']}\n"
        f"RSI(14): {signal['rsi']}\n"
        f"52-week High: {signal['high_250d']} (Close is {signal['pct_of_52w_high']}% of it)\n"
        f"20-day High: {signal['high_20d']} (today's close broke above it)\n"
        f"10-day range: {signal['tight_base_range_pct']}% of close (tight base)\n"
        f"ATR(14): {signal['atr']} ({signal['atr_pct']}% of close)\n"
        f"VWAP: {signal['vwap']} (Close above)\n"
    )

    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={
            "chat_id": config.TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print("Telegram send failed (breakout scan):", e)


def send_consolidation_breakout_alert(signal):
    """
    SUPERSEDED (per request — "je single alert ache ota summary kore
    dao") by send_consolidation_breakout_summary below, which batches
    every stock from one scan run into ONE message instead of one
    message per stock. Kept here unused (rather than deleted) in case
    anything else still wants a single-signal send.
    """
    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        print("Telegram not configured, skipping send:", signal)
        return

    symbol = signal["symbol"]
    direction = signal["direction"]
    arrow = "🟢⬆️" if direction == "BULLISH" else "🔴⬇️"
    break_label = "above" if direction == "BULLISH" else "below"

    chart_link = signal.get("chart_link")
    chart_line = f"📈 <a href=\"{chart_link}\">Open Chart (TradingView)</a>\n" if chart_link else ""

    text = (
        f"{arrow} <b>{symbol}</b> — Consolidation Breakout ({direction})\n"
        f"{chart_line}"
        f"Date: {signal['date']}\n"
        f"Close: {signal['close']} — broke {break_label} the {signal['lookback_days']}-day range\n"
        f"Range: {signal['range_low']} – {signal['range_high']} ({signal['range_pct']}% of close)\n"
        f"Volume: {signal['volume']:,} ({signal['volume_multiple']}x the {signal['lookback_days']}-day avg of {signal['avg_volume']:,})\n"
        f"Turnover: ₹{signal['turnover_cr']:,.1f} Cr\n"
    ).rstrip()

    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={
            "chat_id": config.TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print("Telegram send failed (consolidation breakout):", e)


def send_consolidation_breakout_summary(signals, now_ist):
    """
    Consolidation Breakout — SUMMARY (added, per request — "je single
    alert ache ota summary kore dao jate eksonge onek stock ase").
    Batches every stock that broke out this run into ONE message,
    grouped by direction (Bullish first, then Bearish, numbered
    within each group), same overall shape as send_daily_score_report
    above — instead of send_consolidation_breakout_alert's one-message
    -per-stock.

    Used by BOTH the once/day standalone scan
    (main.run_consolidation_breakout_scan) and the live intraday
    variant inside run_fo_scan / run_nifty500_scan — `signals` is
    whatever that caller collected during its own run (already in the
    exact dict shape strategy.check_consolidation_breakout_scan /
    check_consolidation_breakout_live return, each with "chart_link"
    added by the caller).

    Sends nothing if `signals` is empty (unlike send_opening_bias_report
    /send_ema_cross_report's "always send something" — a scan run with
    zero breakouts is the overwhelmingly common case for this alert,
    so a message every single cycle would be pure noise).
    """
    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        print("Telegram not configured, skipping send:", signals)
        return
    if not signals:
        return

    date_str = now_ist.strftime("%Y-%m-%d")
    time_str = now_ist.strftime("%H:%M")

    bullish = [s for s in signals if s["direction"] == "BULLISH"]
    bearish = [s for s in signals if s["direction"] == "BEARISH"]

    lines = [f"🎯 <b>Consolidation Breakout</b> — {date_str} {time_str}\n"]

    def _format_group(group, icon, break_label):
        for i, s in enumerate(group, start=1):
            chart_link = s.get("chart_link")
            symbol_part = f"<a href=\"{chart_link}\">{s['symbol']}</a>" if chart_link else s["symbol"]
            lines.append(
                f"{i}. <b>{symbol_part}</b> — Close {s['close']}, broke {break_label} "
                f"{s['range_low']}–{s['range_high']} ({s['range_pct']}% range), "
                f"Vol {s['volume_multiple']}x avg"
            )

    if bullish:
        lines.append(f"🟢⬆️ <b>Bullish</b> ({len(bullish)}):")
        _format_group(bullish, "🟢⬆️", "above")
        lines.append("")

    if bearish:
        lines.append(f"🔴⬇️ <b>Bearish</b> ({len(bearish)}):")
        _format_group(bearish, "🔴⬇️", "below")
        lines.append("")

    text = "\n".join(lines).rstrip()

    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={
            "chat_id": config.TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print("Telegram send failed (consolidation breakout summary):", e)


def send_trendline_alert(signal):
    """
    Standalone Trendline Break alert (added, per request) — sent by
    main.run_trendline_scan whenever strategy.check_trendline_scan
    finds a break on the LATEST closed candle, completely independent
    of the EMA cross alert (send_alert above) — no EMA cross needs to
    have happened. Runs in the same scan cycle as the EMA cross scan
    (same fetch, same cadence), just checked and sent separately —
    see run_trendline_scan for exactly when this fires.
    """
    if not getattr(config, "ENABLE_TRENDLINE_ALERTS", True):
        # Master switch off — trendline break alerts disabled (too
        # noisy). See config.ENABLE_TRENDLINE_ALERTS.
        return

    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        print("Telegram not configured, skipping send:", signal)
        return

    direction = signal["direction"]
    arrow = "🟢⬆️" if direction == "BULLISH" else "🔴⬇️"
    line_type_label = "Resistance" if signal["line_type"] == "RESISTANCE" else "Support"

    candle_time = signal["candle_time"]
    date_part, time_part = str(candle_time).split(" ")[0], str(candle_time).split(" ")[1][:5]

    chart_link = signal.get("chart_link")
    chart_line = f"📈 <a href=\"{chart_link}\">Open Chart (TradingView)</a>\n" if chart_link else ""

    p1, p2 = signal["point1"], signal["point2"]
    p1_time = str(p1["time"]).split(" ")[0] + " " + str(p1["time"]).split(" ")[1][:5]
    p2_time = str(p2["time"]).split(" ")[0] + " " + str(p2["time"]).split(" ")[1][:5]

    text = (
        f"{arrow} <b>{signal['symbol']}</b> — Trendline Break ({line_type_label}, {direction})\n"
        f"{chart_line}"
        f"{date_part} {time_part} | Close: {signal['close']}\n"
        f"Line value at break: {signal['line_value']}\n"
        f"Trendline: {p1['price']} ({p1_time}) → {p2['price']} ({p2_time}) "
        f"— {signal['candles_in_trend']} candles\n"
    ).rstrip()

    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={
            "chat_id": config.TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print("Telegram send failed (trendline break):", e)


def send_daily_score_report(hits, now_ist):
    """
    "Perfect Daily Score" report (added, per request — "sob F&O stock
    er Daily Score ekshathe ekta message-e, 8/8 hole") — ONE combined
    message listing every stock whose Daily Score is currently at/above
    config.DAILY_SCORE_REPORT_MIN_SCORE, instead of a separate message
    per stock.

    EXPANDED (per request, 2026-08-28) — now covers CASH (pure Nifty
    500, non-F&O) stocks too, not just F&O. See main.py's run() for
    exactly when this gets called — run_fo_scan collects the F&O-
    tagged half, run_nifty500_scan collects the CASH-tagged half
    (that scan already skips anything F&O-eligible, so there's no
    overlap), the two lists are combined and sent together only when
    the qualifying SET has changed since last sent, so a stock sitting
    at 8/8 for hours doesn't repeat this every scan cycle.

    `hits` is a list of dicts from strategy.compute_daily_score_scan,
    each with an added "is_fno" bool (added, per request — True for
    F&O-eligible, False for cash-only) and "chart_link", already
    sorted by main.py (highest score first, then FNO before CASH,
    then alphabetically).
    """
    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        print("Telegram not configured, skipping send:", hits)
        return
    if not hits:
        return

    date_str = now_ist.strftime("%Y-%m-%d")
    time_str = now_ist.strftime("%H:%M")

    lines = [f"🏆 <b>Perfect Daily Score — F&O + CASH</b> ({date_str} {time_str})\n"]
    for i, h in enumerate(hits, start=1):
        # Chart link (added, per request) — same TradingView 15-min
        # deep link every other alert type already shows, appended
        # after the symbol name as a tappable "Chart" link. Omitted
        # gracefully if an older hits dict doesn't have it set.
        chart_link = h.get("chart_link")
        chart_part = f" — <a href=\"{chart_link}\">Chart</a>" if chart_link else ""
        # FNO/CASH tag (added, per request, 2026-08-28) — defaults to
        # FNO for any older hits dict that doesn't have is_fno set yet.
        tag = "FNO" if h.get("is_fno", True) else "CASH"
        lines.append(f"{i}. <b>{h['symbol']}</b> [{tag}]{chart_part} — {h['label']} — Close: {h['close']}")

    text = "\n".join(lines)

    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={
            "chat_id": config.TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print("Telegram send failed (daily score report):", e)


def _bulk_deal_chart_link(symbol):
    """
    TradingView deep link for a bulk/block-deal symbol — same
    resolution rule as main.py's build_chart_link (checks
    config.TRADINGVIEW_SYMBOL_OVERRIDES first, else "NSE:{symbol}").
    Kept as a small local copy here (instead of importing from main.py)
    to avoid a circular import — telegram_notifier is imported BY
    main.py, so main.py can't be imported back here.
    """
    tv_symbol = config.TRADINGVIEW_SYMBOL_OVERRIDES.get(symbol, f"NSE:{symbol}")
    return f"https://www.tradingview.com/chart/?symbol={tv_symbol}&interval=15"


def send_mode_failure_notice(mode, error, now_ist):
    """
    ADDED (per request, 2026-08-30 — "ema cross report run korlam,
    kichu ashe ni") — a short Telegram warning sent when a standalone
    report SCAN_MODE (ema_cross_report, ema_cross_report_evening,
    opening_bias_report, breakout_scan, consolidation_breakout_scan)
    crashes with an unhandled exception. Before this, main.py's run()
    called these with no try/except at all, so a crash (e.g. NSE fetch
    blocked, Upstox token issue, a bad symbol) meant the process just
    died — no Telegram message, nothing — and the run looked like it
    silently did nothing rather than actually failing. This makes a
    silent crash visible instead: you at least get told WHICH mode
    failed and WHY, instead of just... nothing showing up.
    """
    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        print(f"Telegram not configured, skipping failure notice for {mode}:", error)
        return

    date_str = now_ist.strftime("%Y-%m-%d")
    time_str = now_ist.strftime("%H:%M")
    text = (
        f"⚠️ <b>Scan failed</b> — {date_str} {time_str}\n"
        f"Mode: <code>{mode}</code>\n"
        f"Error: {error}"
    )

    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={
            "chat_id": config.TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print(f"Telegram send failed (mode failure notice for {mode}):", e)


def send_top_movers_report(gainers, losers, now_ist, gainer_links=None, loser_links=None):
    """
    ADDED (per request, 2026-08-31 — "arek ta alert dao top gainer
    looser in 1st 1 minit") — SCAN_MODE=top_movers_1min entry point
    (see main.py's run_top_movers_1min / build_todays_top_movers_1min).

    gainers/losers: lists of {symbol, close, prev_close, pct} — already
    sorted and trimmed to config.TOP_MOVERS_COUNT by the caller.
    gainer_links/loser_links: optional {symbol: chart_url} for the
    TradingView chart link on each row.

    Always sends something (even a "no data" message) — same
    always-report principle as send_ema_cross_report /
    send_opening_bias_report, so a quiet run is visibly "checked,
    nothing to show" rather than indistinguishable from a crash.
    """
    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        print("Telegram not configured, skipping send: top movers report")
        return

    gainer_links = gainer_links or {}
    loser_links = loser_links or {}
    date_str = now_ist.strftime("%Y-%m-%d")
    time_str = now_ist.strftime("%H:%M")

    def _format_row(i, m, links):
        chart_link = links.get(m["symbol"])
        name = f"<a href=\"{chart_link}\">{m['symbol']}</a>" if chart_link else f"<b>{m['symbol']}</b>"
        sign = "+" if m["pct"] >= 0 else ""
        return f"{i}. {name} — {sign}{m['pct']:.2f}% (Close: {m['close']} vs Prev: {m['prev_close']})"

    lines = [f"🚀🐌 <b>Top Gainers/Losers — 1st 1-min</b> — {date_str} {time_str}\n"]

    if gainers:
        lines.append("📈 <b>Top Gainers:</b>")
        lines.extend(_format_row(i, m, gainer_links) for i, m in enumerate(gainers, start=1))
    else:
        lines.append("📈 Top Gainers: none")

    lines.append("")

    if losers:
        lines.append("📉 <b>Top Losers:</b>")
        lines.extend(_format_row(i, m, loser_links) for i, m in enumerate(losers, start=1))
    else:
        lines.append("📉 Top Losers: none")

    text = "\n".join(lines)

    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={
            "chat_id": config.TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print("Telegram send failed (top movers report):", e)


def send_bulk_deal_summary(deals, now_ist):
    """
    Standalone Bulk/Block Deal alert — SUMMARY (CHANGED, per request,
    2026-08-30 — "erokom na, ekta message e ei sob stock cover koro"),
    then GROUPED BY SYMBOL (CHANGED again, per request — "same stock ek
    bar thakbe, tap korle list berobe, chart link thakbe").

    Was previously one line PER deal, so the same symbol (e.g. AASTHA)
    repeated once per deal it had. Now every symbol appears exactly
    ONCE as a single collapsed header line (icon + name + chart link +
    deal count); tapping that line expands a Telegram "expandable
    blockquote" (<blockquote expandable>, HTML parse mode) showing that
    symbol's individual deals underneath, collapsed by default so a
    237-deal run doesn't turn into a huge wall of text.

    `deals` is a list of dicts from bulk_block_data.get_all_recent_
    deals: {type, date, symbol, security_name, client, buy_sell,
    quantity, price} — already sorted oldest-first by the caller.

    Symbol groups are ordered BUY-only first, then SELL-only, then
    mixed/other — alphabetically by symbol within each group (same
    "buy before sell" precedent as the old per-deal layout, just
    applied to symbol groups instead of individual deals now).

    Telegram caps a single message at 4096 characters. Each symbol's
    full block (header + its expandable blockquote) is kept atomic —
    never split across two messages, since a blockquote tag split mid-
    way would break the HTML — so packing is done per-block, not
    per-line. Sends nothing if `deals` is empty.
    """
    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        print("Telegram not configured, skipping send:", deals)
        return
    if not deals:
        return

    date_str = now_ist.strftime("%Y-%m-%d")
    time_str = now_ist.strftime("%H:%M")

    # ---- Group deals by symbol (preserve each group's own oldest-
    # first deal order, same as the caller's overall sort) ----
    groups = {}
    for d in deals:
        groups.setdefault(d["symbol"], []).append(d)

    def _group_direction(group_deals):
        sides = {d.get("buy_sell") for d in group_deals}
        if sides == {"BUY"}:
            return "BUY"
        if sides == {"SELL"}:
            return "SELL"
        return "MIXED"

    def _format_deal_line(i, deal):
        buy_sell = deal.get("buy_sell", "")
        arrow = "🟢⬆️" if buy_sell == "BUY" else "🔴⬇️" if buy_sell == "SELL" else "⚪"
        deal_type = deal.get("type", "Bulk")
        type_icon = "📦" if deal_type == "Bulk" else "🧱"
        quantity = deal.get("quantity") or "N/A"
        price = deal.get("price") or "N/A"
        client = deal.get("client") or "N/A"
        return (
            f"{i}. {arrow} {type_icon} {deal_type} ({buy_sell or 'N/A'})\n"
            f"   {deal.get('date', 'N/A')} | {client} | Qty {quantity} | ₹{price}"
        )

    def _format_symbol_block(symbol, group_deals):
        direction = _group_direction(group_deals)
        header_icon = {"BUY": "🟢⬆️", "SELL": "🔴⬇️", "MIXED": "🟡🔁"}[direction]
        chart_link = _bulk_deal_chart_link(symbol)
        n = len(group_deals)
        deal_word = "deal" if n == 1 else "deals"
        header_line = (
            f"{header_icon} <b>{symbol}</b> — "
            f"<a href=\"{chart_link}\">Chart</a> ({n} {deal_word})"
        )
        detail_lines = "\n".join(
            _format_deal_line(i, d) for i, d in enumerate(group_deals, start=1)
        )
        return f"{header_line}\n<blockquote expandable>{detail_lines}</blockquote>"

    # Order: BUY-only groups, then SELL-only, then mixed — alphabetical
    # by symbol within each bucket.
    symbols_sorted = sorted(
        groups.keys(),
        key=lambda s: (
            {"BUY": 0, "SELL": 1, "MIXED": 2}[_group_direction(groups[s])],
            s,
        ),
    )

    blocks = [_format_symbol_block(sym, groups[sym]) for sym in symbols_sorted]

    header = (
        f"📦🧱 <b>Bulk/Block Deals</b> — {date_str} {time_str} "
        f"({len(deals)} new, {len(groups)} stocks)\n"
    )

    # Chunk into <=4096-char messages, packing whole symbol BLOCKS
    # (never splitting a block's own header+blockquote across two
    # messages).
    MAX_LEN = 4000  # a little headroom under Telegram's 4096 cap
    chunks = []
    current = header
    for block in blocks:
        candidate = current + block + "\n\n"
        if len(candidate) > MAX_LEN and current != header:
            chunks.append(current.rstrip())
            current = header + block + "\n\n"
        else:
            current = candidate
    if current.strip():
        chunks.append(current.rstrip())

    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    for text in chunks:
        try:
            r = requests.post(url, data={
                "chat_id": config.TELEGRAM_CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }, timeout=15)
            r.raise_for_status()
        except Exception as e:
            print("Telegram send failed (bulk/block deal summary):", e)


def send_bulk_deal_alert(deal):
    """
    Standalone Bulk/Block Deal alert (added, per request — NSE Bulk
    Deals page screenshot, "aro ekta alert chai"). Sent by
    bulk_block_data.check_and_alert whenever a NEW deal shows up in
    NSE's bulk.csv/block.csv archive snapshot — market-wide, every
    symbol, same as the NSE page itself, NOT limited to the F&O/Nifty
    500 watchlist the rest of this bot scans. `deal` is one dict from
    bulk_block_data.get_all_recent_deals: {type, date, symbol,
    security_name, client, buy_sell, quantity, price}.
    """
    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        print("Telegram not configured, skipping send:", deal)
        return

    buy_sell = deal.get("buy_sell", "")
    if buy_sell == "BUY":
        arrow = "🟢⬆️"
    elif buy_sell == "SELL":
        arrow = "🔴⬇️"
    else:
        arrow = "⚪"

    deal_type = deal.get("type", "Bulk")
    type_icon = "📦" if deal_type == "Bulk" else "🧱"

    security_name = deal.get("security_name")
    name_line = f"{security_name}\n" if security_name else ""

    quantity = deal.get("quantity") or "N/A"
    price = deal.get("price") or "N/A"
    client = deal.get("client") or "N/A"

    text = (
        f"{arrow} <b>{deal['symbol']}</b> — {type_icon} {deal_type} Deal ({buy_sell or 'N/A'})\n"
        f"{name_line}"
        f"Date: {deal.get('date', 'N/A')}\n"
        f"Client: {client}\n"
        f"Quantity: {quantity}\n"
        f"Price: {price}"
    )

    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={
            "chat_id": config.TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print("Telegram send failed (bulk/block deal):", e)


def send_trading_score_summary(hits, now_ist):
    """
    Trading Score Summary (added, per request — "trading score
    good/strong hole eksathe stock summary dao with chart") — ONE
    combined message listing every stock whose individual EMA-cross
    alert went out THIS run (already passed both the EMA-cross gate
    and the Trading Score GOOD/STRONG gate — see
    strategy.passes_alert_gate). Sent IN ADDITION to each symbol's own
    detailed alert, as a quick-glance roll-up — not a replacement.

    `hits` is a list of dicts from main.py's run(): {symbol, direction,
    trading_score, chart_link, is_fno}, already sorted (highest score
    first, then FNO before CASH, then alphabetically).
    """
    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        print("Telegram not configured, skipping send:", hits)
        return
    if not hits:
        return

    date_str = now_ist.strftime("%Y-%m-%d")
    time_str = now_ist.strftime("%H:%M")

    lines = [f"🏆 <b>Trading Score Summary — GOOD/STRONG</b> ({date_str} {time_str})\n"]
    for i, h in enumerate(hits, start=1):
        direction = h.get("direction", "")
        arrow = "🟢⬆️" if direction == "BULLISH" else "🔴⬇️" if direction == "BEARISH" else "⚪"
        tag = "FNO" if h.get("is_fno", True) else "CASH"
        ts = h.get("trading_score") or {}
        score_part = f"{ts.get('label', 'N/A')} ({ts.get('score', 'N/A')}/10)"
        chart_link = h.get("chart_link")
        chart_part = f" — <a href=\"{chart_link}\">Chart</a>" if chart_link else ""
        lines.append(f"{i}. {arrow} <b>{h['symbol']}</b> [{tag}]{chart_part} — {score_part}")

    text = "\n".join(lines)

    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={
            "chat_id": config.TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print("Telegram send failed (trading score summary):", e)
