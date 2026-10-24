"""
SIMPLE BACKTEST (2026-10-24): price vs EMA50  +  MACD cross  +  RSI.

    UPSTOX_ACCESS_TOKEN=xxxx python backtest_simple.py --days 60 --limit 20 --tf 15
    python backtest_simple.py --csv folder_of_csvs/ --tf 15

Rules (BULLISH shown, BEARISH is the mirror image)
  trigger : MACD(12/26/9) line crosses ABOVE its signal line on a closed candle
  filter 1: close ABOVE EMA50      (BEARISH: below)
  filter 2: RSI(14) >= --rsi-bull  (BEARISH: RSI <= --rsi-bear)       default 55 / 45
  filter 3: LAST DAY volume vs the 20-day average - tested BOTH ways (below / above), rows 5 and 6.
            (needs ~20 earlier sessions, so use --days 90 or more for enough trades)
  entry   : OPEN of the next candle ; exits: after 1h, after 2h, at the day's close,
            plus a 1 x ATR stop / 1.5 x ATR target version.  --cost-pct is deducted per trade.

The report compares: every MACD cross, + EMA50 only, + RSI only, and all three together,
so you can see what each filter adds.  --tf = candle size in minutes (15, 30, 60, 75 ...).
"""
import argparse
import glob
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from indicators import add_macd, add_rsi, add_ema  # noqa: E402
from backtest import to_15min, fetch_upstox         # noqa: E402


def run_symbol(symbol, df, tf, rsi_bull, rsi_bear, cost_pct):
    if len(df) < 200:
        return []
    df = add_macd(df.copy(), 12, 26, 9)
    df = add_rsi(df, 14)
    df = add_ema(df, 50, "ema50")
    df["day"] = df["timestamp"].dt.date
    pc = df["close"].shift(1)
    tr = pd.concat([df["high"] - df["low"], (df["high"] - pc).abs(), (df["low"] - pc).abs()], axis=1).max(axis=1)
    df["atr"] = tr.ewm(alpha=1 / 14, adjust=False).mean()
    # last completed day's volume vs the average of the last 20 completed days (same idea as the live alert)
    dv = df.groupby("day")["volume"].sum()
    vol_tab = pd.DataFrame({"prev_vol": dv.shift(1), "avg20": dv.shift(1).rolling(20, min_periods=15).mean()})
    df = df.join(vol_tab, on="day")
    diff = df["macd_line"] - df["macd_signal"]
    up = (diff.shift(1) <= 0) & (diff > 0)
    dn = (diff.shift(1) >= 0) & (diff < 0)
    day_idx = df.groupby("day").indices
    per_h = max(1, round(60 / tf))
    out = []
    for i in np.where(up | dn)[0]:
        if i < 60 or pd.isna(df.at[i, "rsi"]):
            continue
        idxs = list(day_idx[df.at[i, "day"]])
        pos = idxs.index(i)
        if pos + 1 >= len(idxs):
            continue
        bull = bool(up[i])
        sgn = 1 if bull else -1
        row = df.iloc[i]
        ema_ok = (row["close"] > row["ema50"]) == bull
        rsi_ok = (row["rsi"] >= rsi_bull) if bull else (row["rsi"] <= rsi_bear)
        j = idxs[pos + 1]
        entry = float(df.at[j, "open"])
        last = idxs[-1]
        res = {"symbol": symbol, "time": row["timestamp"], "dir": "BULL" if bull else "BEAR",
               "ema_ok": bool(ema_ok), "rsi_ok": bool(rsi_ok), "rsi": round(float(row["rsi"]), 1),
               "vol_low": (bool(row["prev_vol"] < row["avg20"]) if pd.notna(row["avg20"]) else None),
               "vol_high": (bool(row["prev_vol"] > row["avg20"]) if pd.notna(row["avg20"]) else None)}
        for nm, n in (("1h", per_h), ("2h", 2 * per_h)):
            k = min(j + n - 1, last)
            res["ret_" + nm] = sgn * (float(df.at[k, "close"]) / entry - 1) * 100 - cost_pct
        res["ret_eod"] = sgn * (float(df.at[last, "close"]) / entry - 1) * 100 - cost_pct
        atr = float(row["atr"])
        if atr > 0:
            stop, tgt = entry - sgn * atr, entry + sgn * 1.5 * atr
            r = None
            for k in range(j, last + 1):
                hi, lo = float(df.at[k, "high"]), float(df.at[k, "low"])
                hs = lo <= stop if bull else hi >= stop
                ht = hi >= tgt if bull else lo <= tgt
                if hs:                      # stop first when both hit in one candle (pessimistic)
                    r = -1.0; break
                if ht:
                    r = 1.5; break
            res["r_mult"] = r if r is not None else sgn * (float(df.at[last, "close"]) - entry) / atr
        out.append(res)
    return out


def show(t, label):
    if t.empty:
        print(f"{label:<34} n=0"); return
    s = f"{label:<34} n={len(t):<5}"
    for c, nm in (("ret_1h", "1h"), ("ret_2h", "2h"), ("ret_eod", "EOD")):
        s += f" | {nm} win {100 * (t[c] > 0).mean():5.1f}% avg {t[c].mean():+.3f}%"
    r = t["r_mult"].dropna() if "r_mult" in t else []
    if len(r):
        s += f" | ATR-trade win {100 * (r > 0).mean():5.1f}% avg {r.mean():+.2f}R"
    print(s)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--tf", type=int, default=15, help="candle minutes")
    ap.add_argument("--rsi-bull", type=float, default=55)
    ap.add_argument("--rsi-bear", type=float, default=45)
    ap.add_argument("--cost-pct", type=float, default=0.0)
    ap.add_argument("--csv", default="")
    ap.add_argument("--out", default="backtest_simple_trades.csv")
    a = ap.parse_args()

    data = {}
    if a.csv:
        for f in sorted(glob.glob(os.path.join(a.csv, "*.csv"))):
            raw = pd.read_csv(f)
            if "timestamp" in raw.columns:
                data[os.path.splitext(os.path.basename(f))[0]] = to_15min(raw, a.tf)
    else:
        import config, instruments
        token = os.environ.get("UPSTOX_ACCESS_TOKEN") or config.UPSTOX_ACCESS_TOKEN
        if not token:
            sys.exit("UPSTOX_ACCESS_TOKEN is not set.")
        items = list(instruments.resolve_fo_stock_list(None).items())[: a.limit or None]
        for n, (sym, key) in enumerate(items, 1):
            print(f"[{n}/{len(items)}] {sym}", flush=True)
            raw = fetch_upstox(sym, key, a.days, token)
            if raw is not None and len(raw):
                data[sym] = to_15min(raw, a.tf)

    rows = []
    for sym, df in data.items():
        try:
            rows += run_symbol(sym, df, a.tf, a.rsi_bull, a.rsi_bear, a.cost_pct)
        except Exception as e:
            print(f"{sym}: failed {e}")
    if not rows:
        print("No trades found (not enough data?)"); return
    t = pd.DataFrame(rows)
    t.to_csv(a.out, index=False)
    print(f"\n{len(data)} symbols, {t['time'].min():%d %b %Y} -> {t['time'].max():%d %b %Y}, "
          f"{a.tf}-min candles, RSI {a.rsi_bull:g}/{a.rsi_bear:g}, cost {a.cost_pct}% per trade\n")
    show(t, "1) Every MACD cross")
    show(t[t["ema_ok"]], "2) + price vs EMA50")
    show(t[t["rsi_ok"]], "3) + RSI only")
    both = t[t["ema_ok"] & t["rsi_ok"]]
    show(both, "4) MACD + EMA50 + RSI (all 3)")
    known = both[both["vol_low"].notna()]
    show(known[known["vol_low"] == True], "5) all 3 + last-day vol < 20D avg")   # noqa: E712
    show(known[known["vol_high"] == True], "6) all 3 + last-day vol > 20D avg")  # noqa: E712
    show(known, "   (all 3, where 20D avg exists)")
    print()
    for d in ("BULL", "BEAR"):
        show(both[both["dir"] == d], f"   all 3, {d} only")
    print("\nTrades saved to", a.out)


if __name__ == "__main__":
    main()
