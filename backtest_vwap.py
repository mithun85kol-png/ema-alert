"""
VWAP BACKTEST (2026-10-24): price crosses VWAP  +  MACD  +  EMA50  +  volume.

    UPSTOX_ACCESS_TOKEN=xxxx python backtest_vwap.py --days 90 --limit 20 --tf 15
    python backtest_vwap.py --csv folder_of_csvs/ --tf 15

Trigger : the candle CLOSES above the day's VWAP after closing at/below it on the previous candle
          (BEARISH: closes below after being at/above).  VWAP restarts every session.
Filters, each tested alone and all together:
  EMA50  : close above EMA50 (BEARISH: below)
  MACD   : MACD line above its signal line (BEARISH: below)   -- the state on the trigger candle
  MACD-X : the MACD line crossed its signal line within the last --macd-window candles (default 4)
  VOLUME : trigger-candle volume > its 20-candle average
  LASTDAY: last completed day's volume > the 20-day average   (and the < version, shown separately)
Entry = OPEN of the next candle; exits after 1h, 2h, day close, plus a 1 x ATR stop / 1.5 x ATR target.
--cost-pct is deducted from every trade.  Use --days 90+ so the 20-day average has data.
"""
import argparse, glob, os, sys
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from indicators import add_macd, add_rsi, add_ema  # noqa: E402,F401
from backtest import to_15min, fetch_upstox         # noqa: E402


def run_symbol(symbol, df, tf, macd_window, cost_pct):
    if len(df) < 200:
        return []
    df = add_macd(df.copy(), 12, 26, 9)
    df = add_ema(df, 50, "ema50")
    df["day"] = df["timestamp"].dt.date
    tp = (df["high"] + df["low"] + df["close"]) / 3
    cv = df["volume"].groupby(df["day"]).cumsum().replace(0, np.nan)
    df["vwap"] = (tp * df["volume"]).groupby(df["day"]).cumsum() / cv
    df["vol_avg"] = df["volume"].shift(1).rolling(20, min_periods=15).mean()
    pc = df["close"].shift(1)
    tr = pd.concat([df["high"] - df["low"], (df["high"] - pc).abs(), (df["low"] - pc).abs()], axis=1).max(axis=1)
    df["atr"] = tr.ewm(alpha=1 / 14, adjust=False).mean()
    dv = df.groupby("day")["volume"].sum()
    df = df.join(pd.DataFrame({"prev_vol": dv.shift(1), "avg20": dv.shift(1).rolling(20, min_periods=15).mean()}), on="day")
    diff = df["macd_line"] - df["macd_signal"]
    x_up = ((diff.shift(1) <= 0) & (diff > 0)).astype(int).rolling(macd_window, min_periods=1).max() > 0
    x_dn = ((diff.shift(1) >= 0) & (diff < 0)).astype(int).rolling(macd_window, min_periods=1).max() > 0
    v_up = (df["close"].shift(1) <= df["vwap"].shift(1)) & (df["close"] > df["vwap"])
    v_dn = (df["close"].shift(1) >= df["vwap"].shift(1)) & (df["close"] < df["vwap"])
    day_idx = df.groupby("day").indices
    per_h = max(1, round(60 / tf))
    out = []
    for i in np.where(v_up | v_dn)[0]:
        if i < 60 or pd.isna(df.at[i, "vwap"]):
            continue
        idxs = list(day_idx[df.at[i, "day"]])
        pos = idxs.index(i)
        if pos < 1 or pos + 1 >= len(idxs):
            continue
        bull = bool(v_up[i]); sgn = 1 if bull else -1
        row = df.iloc[i]
        j, last = idxs[pos + 1], idxs[-1]
        entry = float(df.at[j, "open"])
        r = {"symbol": symbol, "time": row["timestamp"], "dir": "BULL" if bull else "BEAR",
             "ema": bool((row["close"] > row["ema50"]) == bull),
             "macd": bool((diff.iat[i] > 0) == bull),
             "macdx": bool(x_up.iat[i] if bull else x_dn.iat[i]),
             "vol": bool(row["volume"] > row["vol_avg"]) if pd.notna(row["vol_avg"]) else None,
             "last_hi": bool(row["prev_vol"] > row["avg20"]) if pd.notna(row["avg20"]) else None,
             "last_lo": bool(row["prev_vol"] < row["avg20"]) if pd.notna(row["avg20"]) else None}
        for nm, n in (("1h", per_h), ("2h", 2 * per_h)):
            k = min(j + n - 1, last)
            r["ret_" + nm] = sgn * (float(df.at[k, "close"]) / entry - 1) * 100 - cost_pct
        r["ret_eod"] = sgn * (float(df.at[last, "close"]) / entry - 1) * 100 - cost_pct
        atr = float(row["atr"])
        if atr > 0:
            stop, tgt = entry - sgn * atr, entry + sgn * 1.5 * atr
            m = None
            for k in range(j, last + 1):
                hi, lo = float(df.at[k, "high"]), float(df.at[k, "low"])
                if (lo <= stop) if bull else (hi >= stop):
                    m = -1.0; break
                if (hi >= tgt) if bull else (lo <= tgt):
                    m = 1.5; break
            r["r_mult"] = m if m is not None else sgn * (float(df.at[last, "close"]) - entry) / atr
        out.append(r)
    return out


def show(t, label):
    if t.empty:
        print(f"{label:<38} n=0"); return
    s = f"{label:<38} n={len(t):<5}"
    for c, nm in (("ret_1h", "1h"), ("ret_2h", "2h"), ("ret_eod", "EOD")):
        s += f" | {nm} win {100 * (t[c] > 0).mean():5.1f}% avg {t[c].mean():+.3f}%"
    rm = t["r_mult"].dropna() if "r_mult" in t else []
    if len(rm):
        s += f" | ATR-trade win {100 * (rm > 0).mean():5.1f}% avg {rm.mean():+.2f}R"
    print(s)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=90)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--tf", type=int, default=15)
    ap.add_argument("--macd-window", type=int, default=4)
    ap.add_argument("--cost-pct", type=float, default=0.0)
    ap.add_argument("--csv", default="")
    ap.add_argument("--out", default="backtest_vwap_trades.csv")
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
            rows += run_symbol(sym, df, a.tf, a.macd_window, a.cost_pct)
        except Exception as e:
            print(f"{sym}: failed {e}")
    if not rows:
        print("No trades found (not enough data?)"); return
    t = pd.DataFrame(rows); t.to_csv(a.out, index=False)
    print(f"\n{len(data)} symbols, {t['time'].min():%d %b %Y} -> {t['time'].max():%d %b %Y}, {a.tf}-min candles, "
          f"cost {a.cost_pct}% per trade\n")
    show(t, "1) Every VWAP cross")
    show(t[t.ema], "2) + price vs EMA50")
    show(t[t.macd], "3) + MACD side (above/below signal)")
    show(t[t.macdx], f"4) + MACD crossed in last {a.macd_window} candles")
    show(t[t.vol == True], "5) + candle volume > 20-candle avg")      # noqa: E712
    allf = t[t.ema & t.macd & (t.vol == True)]                          # noqa: E712
    show(allf, "6) VWAP + EMA50 + MACD side + volume")
    allx = t[t.ema & t.macdx & (t.vol == True)]                         # noqa: E712
    show(allx, "7) VWAP + EMA50 + MACD cross + volume")
    print("\n   adding LAST-DAY volume vs 20D avg to row 7:")
    show(allx[allx.last_hi == True], "   7 + last-day vol > 20D avg")  # noqa: E712
    show(allx[allx.last_lo == True], "   7 + last-day vol < 20D avg")  # noqa: E712
    print()
    for d in ("BULL", "BEAR"):
        show(allx[allx["dir"] == d], f"   row 7, {d} only")
    print("\nTrades saved to", a.out)


if __name__ == "__main__":
    main()
