"""
MACD + RSI + VWAP BACKTEST (2026-10-25): MACD cross with RSI, price above/below VWAP. No volume filter.

    UPSTOX_ACCESS_TOKEN=xxxx python backtest_macd_vwap.py --days 90 --limit 20 --tf 15
    python backtest_macd_vwap.py --csv folder_of_csvs/ --tf 15

Rules (BULLISH shown, BEARISH is the mirror image)
  trigger : MACD(12/26/9) line crosses ABOVE its signal line on a closed candle
  filter 1: price (close) ABOVE the day's VWAP        (BEARISH: below)   - VWAP restarts every session
  filter 2: RSI(14) >= --rsi-bull                      (BEARISH: <= --rsi-bear)   default 55 / 45
  entry   : OPEN of the next candle ; exits: after 1h, after 2h, day close, plus 1 x ATR stop / 1.5 x ATR target.
The table compares every MACD cross, + VWAP, + RSI, and both together.  --cost-pct is deducted per trade.
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
    df["day"] = df["timestamp"].dt.date
    tp = (df["high"] + df["low"] + df["close"]) / 3
    cv = df["volume"].groupby(df["day"]).cumsum().replace(0, np.nan)
    df["vwap"] = (tp * df["volume"]).groupby(df["day"]).cumsum() / cv
    pc = df["close"].shift(1)
    tr = pd.concat([df["high"] - df["low"], (df["high"] - pc).abs(), (df["low"] - pc).abs()], axis=1).max(axis=1)
    df["atr"] = tr.ewm(alpha=1 / 14, adjust=False).mean()
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
        ema_ok = bool(pd.notna(row["vwap"]) and (row["close"] > row["vwap"]) == bull)
        rsi_ok = (row["rsi"] >= rsi_bull) if bull else (row["rsi"] <= rsi_bear)
        j = idxs[pos + 1]
        entry = float(df.at[j, "open"])
        last = idxs[-1]
        res = {"symbol": symbol, "time": row["timestamp"], "dir": "BULL" if bull else "BEAR",
               "ema_ok": bool(ema_ok), "rsi_ok": bool(rsi_ok), "rsi": round(float(row["rsi"]), 1)}
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
        print(f"{label}\n   n=0"); return
    w = lambda c: 100 * (t[c] > 0).mean()
    rm = t["r_mult"].dropna() if "r_mult" in t else []
    atr = f" · ATR {100 * (rm > 0).mean():.0f}% ({rm.mean():+.2f}R)" if len(rm) else ""
    print(f"{label}  [n={len(t)}]\n   1h {w('ret_1h'):.0f}% · 2h {w('ret_2h'):.0f}% · EOD {w('ret_eod'):.0f}% (avg {t['ret_eod'].mean():+.2f}%){atr}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--tf", type=int, default=15, help="candle minutes")
    ap.add_argument("--rsi-bull", type=float, default=55)
    ap.add_argument("--rsi-bear", type=float, default=45)
    ap.add_argument("--cost-pct", type=float, default=0.0)
    ap.add_argument("--csv", default="")
    ap.add_argument("--out", default="backtest_macd_vwap_trades.csv")
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
    show(t[t["ema_ok"]], "2) + price above/below VWAP")
    show(t[t["rsi_ok"]], "3) + RSI only")
    both = t[t["ema_ok"] & t["rsi_ok"]]
    show(both, "4) MACD + RSI + VWAP (all 3)")
    print()
    for d in ("BULL", "BEAR"):
        show(both[both["dir"] == d], f"   all 3, {d} only")
    print("\nTrades saved to", a.out)


if __name__ == "__main__":
    main()
