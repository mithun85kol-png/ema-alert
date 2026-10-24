"""
BACKTEST of the 15-min MACD alert (2026-10-24).

Run it where Upstox is reachable (your PC, or the GitHub Actions workflow backtest.yml):

    UPSTOX_ACCESS_TOKEN=xxxx python backtest.py               # F&O stocks, last 60 days
    python backtest.py --days 90 --limit 40                   # fewer stocks, longer
    python backtest.py --csv some_folder_of_csvs/             # offline: <SYMBOL>.csv with
                                                              # timestamp,open,high,low,close,volume (1-min or 15-min)

What it does
  * builds 15-min candles (1-min from Upstox, resampled), IST market hours only
  * signal = MACD(12/26/9) line crosses its signal line on a closed 15-min candle
    (same trigger as the live alert), BULLISH or BEARISH
  * "strength" = the same direction-aware checks as the live alert, using the data a
    backtest can rebuild: last-day volume > 1M average (mandatory), MACD side of zero
    (mandatory), RSI >=55 / <=45, 75-min EMA50 side, VWAP side, 1st 15-min high/low,
    candle volume up, EMA 9/20 bias.  (Delivery % is NOT available, so the bar is scaled:
    7 of 9 -> 7 of 8.)  The live alert's DAILY line is informational and is not tested.
  * entry = OPEN of the next 15-min candle (what you could actually trade), skip if it is
    the last candle of the day
  * exits measured three ways: after 1 hour, after 2 hours, and at the day's close
    (intraday).  Win = the move is in the signal's direction.  Costs are NOT deducted;
    --cost-pct subtracts a round-trip cost from every trade if you want.
  * also tests a simple stop/target (1.0 x ATR stop, 1.5 x ATR target, whichever hits first,
    else exit at day close) so you see a risk-reward view, not only win rate.

It reports: all crosses vs. crosses that pass the live alert's filter, bullish vs bearish,
and win rate by strength score.  Past results do not promise future results.
"""
import argparse
import glob
import math
import os
import sys
import datetime as dt

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from indicators import add_macd, add_rsi, add_ema  # noqa: E402

MARKET_OPEN, MARKET_CLOSE = dt.time(9, 15), dt.time(15, 30)
MIN_CHECKS, FULL_CHECKS = 7, 8          # live: 7 of 9; delivery missing here -> 7 of 8


# ----------------------------------------------------------------- data -----
def to_15min(df):
    """df: timestamp(open/high/low/close/volume) -> 15-min candles, NSE hours, tz-naive IST."""
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    if df["timestamp"].dt.tz is not None:
        df["timestamp"] = df["timestamp"].dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    df = df.sort_values("timestamp").drop_duplicates("timestamp").set_index("timestamp")
    df = df.between_time(MARKET_OPEN, dt.time(15, 29))
    o = df["open"].resample("15min", origin="start_day", offset="9h15min").first()
    h = df["high"].resample("15min", origin="start_day", offset="9h15min").max()
    l = df["low"].resample("15min", origin="start_day", offset="9h15min").min()
    c = df["close"].resample("15min", origin="start_day", offset="9h15min").last()
    v = df["volume"].resample("15min", origin="start_day", offset="9h15min").sum()
    out = pd.DataFrame({"open": o, "high": h, "low": l, "close": c, "volume": v}).dropna(subset=["close"])
    return out.reset_index()


def fetch_upstox(symbol, key, days, token):
    """1-minute candles from Upstox v3 historical API, in 28-day chunks (its 1-month limit)."""
    import time
    import requests
    hdr = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    frames, end = [], dt.date.today()
    start_all = end - dt.timedelta(days=days)
    cur_end = end
    while cur_end > start_all:
        cur_start = max(start_all, cur_end - dt.timedelta(days=27))
        url = (f"https://api.upstox.com/v3/historical-candle/{key}/minutes/1/"
               f"{cur_end.isoformat()}/{cur_start.isoformat()}")
        for attempt in range(3):
            try:
                r = requests.get(url, headers=hdr, timeout=30)
                if r.status_code == 429:
                    time.sleep(2 + attempt * 2); continue
                r.raise_for_status()
                c = r.json().get("data", {}).get("candles", [])
                if c:
                    d = pd.DataFrame(c, columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])
                    frames.append(d[["timestamp", "open", "high", "low", "close", "volume"]])
                break
            except Exception as e:
                if attempt == 2:
                    print(f"  {symbol}: chunk {cur_start}..{cur_end} failed: {e}")
                time.sleep(1)
        time.sleep(0.4)          # stay well under Upstox rate limits
        cur_end = cur_start - dt.timedelta(days=1)
    if not frames:
        return None
    return pd.concat(frames, ignore_index=True)


def daily_from_15(df15):
    d = df15.assign(day=df15["timestamp"].dt.date).groupby("day").agg(
        open=("open", "first"), high=("high", "max"), low=("low", "min"),
        close=("close", "last"), volume=("volume", "sum"))
    return d


# -------------------------------------------------------------- backtest ----
def backtest_symbol(symbol, df15, atr_period=14, cost_pct=0.0):
    if len(df15) < 400:
        return []
    df = add_macd(df15.copy(), 12, 26, 9)
    df = add_rsi(df, 14)
    df = add_ema(df, 9, "ema9")
    df = add_ema(df, 20, "ema20")
    df["day"] = df["timestamp"].dt.date
    # session VWAP
    tp = (df["high"] + df["low"] + df["close"]) / 3
    df["vwap"] = (tp * df["volume"]).groupby(df["day"]).cumsum() / df["volume"].groupby(df["day"]).cumsum().replace(0, np.nan)
    # ATR on 15-min
    pc = df["close"].shift(1)
    tr = pd.concat([df["high"] - df["low"], (df["high"] - pc).abs(), (df["low"] - pc).abs()], axis=1).max(axis=1)
    df["atr"] = tr.ewm(alpha=1 / atr_period, adjust=False).mean()
    # 75-min EMA50 (built from 15-min, as-of the last CLOSED 75-min bar)
    d75 = df.set_index("timestamp")["close"].resample("75min", origin="start_day", offset="9h15min").last().dropna()
    e75 = d75.ewm(span=50, adjust=False).mean()
    # a 75-min bar closes 75 min after its start -> usable from then on
    e75_df = pd.DataFrame({"t": e75.index + pd.Timedelta(minutes=75), "e75": e75.values, "c75": d75.values})
    df = pd.merge_asof(df.sort_values("timestamp"), e75_df.sort_values("t"), left_on="timestamp", right_on="t", direction="backward")
    # 1st 15-min candle of each day
    first = df.groupby("day").first()[["high", "low"]].rename(columns={"high": "f_high", "low": "f_low"})
    df = df.join(first, on="day")
    # last-day volume vs 1M (20 sessions) average -- both use PREVIOUS completed sessions
    dd = daily_from_15(df15)
    dd["prev_vol"] = dd["volume"].shift(1)
    dd["avg_1m"] = dd["volume"].shift(1).rolling(20, min_periods=10).mean()
    df = df.join(dd[["prev_vol", "avg_1m"]], on="day")

    diff = df["macd_line"] - df["macd_signal"]
    cross_up = (diff.shift(1) <= 0) & (diff > 0)
    cross_dn = (diff.shift(1) >= 0) & (diff < 0)
    trades = []
    idx_by_day = df.groupby("day").indices
    ts = df["timestamp"]
    for i in np.where(cross_up | cross_dn)[0]:
        if i < 60 or pd.isna(df.at[i, "rsi"]):
            continue
        day = df.at[i, "day"]
        pos_in_day = list(idx_by_day[day]).index(i)
        day_idx = list(idx_by_day[day])
        if pos_in_day + 1 >= len(day_idx):          # no next candle to enter on
            continue
        bull = bool(cross_up[i])
        row = df.iloc[i]
        # ---- strength checks (same direction-aware logic as the live alert) ----
        checks = {}
        if pd.notna(row["prev_vol"]) and pd.notna(row["avg_1m"]) and row["avg_1m"] > 0:
            checks["volume"] = row["prev_vol"] > row["avg_1m"]
        checks["macd_zero"] = (row["macd_line"] > 0) if bull else (row["macd_line"] < 0)
        checks["rsi"] = (row["rsi"] >= 55) if bull else (row["rsi"] <= 45)
        if pd.notna(row["c75"]) and pd.notna(row["e75"]):
            checks["ema75"] = (row["close"] > row["e75"]) == bull
        if pd.notna(row["vwap"]):
            checks["vwap"] = (row["close"] > row["vwap"]) == bull
        if pos_in_day >= 1:
            checks["first15"] = (row["close"] > row["f_high"]) if bull else (row["close"] < row["f_low"])
        if i > 0 and df.at[i - 1, "volume"] > 0:
            checks["candle_volume"] = row["volume"] > df.at[i - 1, "volume"]
        checks["ema920"] = (row["ema9"] > row["ema20"]) == bull
        total = len(checks)
        passed = int(sum(bool(v) for v in checks.values()))
        need = math.ceil(MIN_CHECKS * total / FULL_CHECKS - 1e-9)
        mandatory_ok = all(checks.get(k, True) for k in ("volume", "macd_zero"))
        passes_filter = mandatory_ok and passed >= need

        # ---- trade: enter next candle open ----
        j = day_idx[pos_in_day + 1]
        entry = float(df.at[j, "open"])
        sgn = 1 if bull else -1
        res = {"symbol": symbol, "time": row["timestamp"], "dir": "BULL" if bull else "BEAR",
               "score": passed, "total": total, "passes": passes_filter}
        for name, n in (("1h", 4), ("2h", 8)):
            k = min(j + n - 1, day_idx[-1])
            res["ret_" + name] = sgn * (float(df.at[k, "close"]) / entry - 1) * 100 - cost_pct
        res["ret_eod"] = sgn * (float(df.at[day_idx[-1], "close"]) / entry - 1) * 100 - cost_pct
        # stop / target in ATR
        atr = float(row["atr"]) if pd.notna(row["atr"]) else None
        if atr and atr > 0:
            stop = entry - sgn * 1.0 * atr
            tgt = entry + sgn * 1.5 * atr
            out = None
            for k in range(j, day_idx[-1] + 1):
                hi, lo = float(df.at[k, "high"]), float(df.at[k, "low"])
                hit_stop = lo <= stop if bull else hi >= stop
                hit_tgt = hi >= tgt if bull else lo <= tgt
                if hit_stop and hit_tgt:          # both inside one candle: assume the stop came first
                    out = -1.0; break
                if hit_stop:
                    out = -1.0; break
                if hit_tgt:
                    out = 1.5; break
            if out is None:
                out = sgn * (float(df.at[day_idx[-1], "close"]) - entry) / atr
            res["r_mult"] = out
        trades.append(res)
    return trades


# ---------------------------------------------------------------- report ----
def summarize(t, label):
    if t.empty:
        print(f"{label}: no trades"); return
    line = f"{label:<34} n={len(t):<5}"
    for c, nm in (("ret_1h", "1h"), ("ret_2h", "2h"), ("ret_eod", "EOD")):
        line += f" | {nm} win {100 * (t[c] > 0).mean():5.1f}% avg {t[c].mean():+.3f}%"
    if "r_mult" in t:
        r = t["r_mult"].dropna()
        if len(r):
            line += f" | ATR-trade win {100 * (r > 0).mean():5.1f}% avg {r.mean():+.2f}R"
    print(line)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--limit", type=int, default=0, help="only the first N F&O stocks (0 = all)")
    ap.add_argument("--csv", default="", help="folder of <SYMBOL>.csv instead of Upstox")
    ap.add_argument("--cost-pct", type=float, default=0.0, help="round-trip cost %% per trade")
    ap.add_argument("--out", default="backtest_trades.csv")
    a = ap.parse_args()

    data = {}
    if a.csv:
        for f in sorted(glob.glob(os.path.join(a.csv, "*.csv"))):
            sym = os.path.splitext(os.path.basename(f))[0]
            raw = pd.read_csv(f)
            if "timestamp" not in raw.columns:      # e.g. a saved results file
                continue
            data[sym] = to_15min(raw)
    else:
        import config, instruments
        token = os.environ.get("UPSTOX_ACCESS_TOKEN") or config.UPSTOX_ACCESS_TOKEN
        if not token:
            sys.exit("UPSTOX_ACCESS_TOKEN is not set.")
        wl = instruments.resolve_fo_stock_list(None)
        items = list(wl.items())[: a.limit or None]
        for n, (sym, key) in enumerate(items, 1):
            print(f"[{n}/{len(items)}] {sym}", flush=True)
            raw = fetch_upstox(sym, key, a.days, token)
            if raw is not None and len(raw):
                data[sym] = to_15min(raw)

    rows = []
    for sym, df15 in data.items():
        try:
            rows += backtest_symbol(sym, df15, cost_pct=a.cost_pct)
        except Exception as e:
            print(f"{sym}: failed {e}")
    if not rows:
        print("No trades found (not enough data?)"); return
    t = pd.DataFrame(rows)
    t.to_csv(a.out, index=False)
    print(f"\n{len(data)} symbols, {t['time'].min():%d %b %Y} -> {t['time'].max():%d %b %Y}, "
          f"{len(t)} MACD crosses, cost {a.cost_pct}% per trade\n")
    summarize(t, "ALL crosses (no filter)")
    summarize(t[t["passes"]], "Passes live-alert filter")
    summarize(t[~t["passes"]], "Rejected by the filter")
    print()
    for d in ("BULL", "BEAR"):
        summarize(t[(t["dir"] == d) & t["passes"]], f"Filtered {d}")
    print("\nBy number of checks passed (all crosses):")
    for s in sorted(t["score"].unique()):
        summarize(t[t["score"] == s], f"  score {s}")
    print("\nTrades saved to", a.out)


if __name__ == "__main__":
    main()
