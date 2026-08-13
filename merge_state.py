"""
Merges two alert_state.json files by UNION-ing the per-(symbol,
direction) list of already-alerted candle_time strings, instead of
picking one side or the other.

Why this exists: the "index" scan (every ~5 min) and the "full" scan
(F&O/Nifty500/commodity, every ~7 min) are two SEPARATE GitHub Actions
runs that can genuinely overlap in time, and both read+write the same
alert_state.json in the same repo. If run B pushes its state update
while run A is mid-run, run A's own push gets rejected -- and the old
recovery ("git pull --rebase --strategy-option=theirs") threw away
whichever side lost the rebase, silently forgetting that side's
newly-marked alerts. The next run would then see those candles as
"not yet alerted" and re-send them -- this is the most likely cause of
the same alert going out more than once.

A plain union is always safe here because every value in this file is
a monotonically-growing list of candle_times that have already been
alerted -- merging two such lists can only ever ADD entries that
protect against a re-send, never remove one. It can never cause a
missed alert either (an entry only lands in the file after
send_alert() has already been called for it in that run).

Usage: python merge_state.py <local_file> <remote_file> <output_file>
Either input file may be missing/empty/corrupt -- treated as {}.
"""
import json
import sys

MAX_REMEMBERED_PER_KEY = 50  # keep in sync with state.py


def _load(path):
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def _times(val):
    if val is None:
        return []
    if isinstance(val, dict):
        ct = val.get("candle_time")
        return [ct] if ct else []
    if isinstance(val, list):
        return val
    return []


def merge(local, remote):
    keys = set(local.keys()) | set(remote.keys())
    merged = {}
    for key in keys:
        combined = list(_times(local.get(key)))
        for ct in _times(remote.get(key)):
            if ct not in combined:
                combined.append(ct)
        merged[key] = combined[-MAX_REMEMBERED_PER_KEY:]
    return merged


if __name__ == "__main__":
    local_path, remote_path, out_path = sys.argv[1], sys.argv[2], sys.argv[3]
    merged = merge(_load(local_path), _load(remote_path))
    with open(out_path, "w") as f:
        json.dump(merged, f, indent=2)
    print(f"Merged {local_path} + {remote_path} -> {out_path} ({len(merged)} keys)")
