"""
Synthetic market-data feed (Bloomberg/Kafka-style ticks) written as JSON files.

WHAT IT DOES
  Each run drops one "micro-batch" of ticks into /Volumes/<catalog>/bronze/landing/ticks/batch_<NNNN>/
  for equities (AAPL, MSFT, HSBA.L, VOD.L) and FX pairs (GBPUSD, EURUSD, EURGBP).

DELIBERATE PROBLEMS (so the pipeline's guarantees can be tested)
  * duplicates   ~2% of ticks re-sent with the same tick_id (at-least-once delivery)
  * late ticks   from batch 1, a few ticks 3 hours old (beyond the 1-hour watermark -> dropped)
  * malformed    a few ticks with a NULL symbol or a negative price (-> quarantine)
"""
import argparse
import datetime as dt
import json
import math
import os
import random
import sys

INSTRUMENTS = {"AAPL": ("EQUITY", 228.0), "MSFT": ("EQUITY", 432.0), "HSBA.L": ("EQUITY", 7.1),
               "VOD.L": ("EQUITY", 0.72), "GBPUSD": ("FX", 1.27), "EURUSD": ("FX", 1.09), "EURGBP": ("FX", 0.855)}


def generate(root: str, batch: int, ticks_per_instrument: int = 300, seed: int = 21):
    # STEP 1 - each batch covers one trading day starting 2026-10-19
    r = random.Random(seed + batch)
    day = dt.datetime(2026, 10, 19, 8, 0, 0) + dt.timedelta(days=batch)
    rows = []
    for sym, (asset_class, price) in INSTRUMENTS.items():
        p = price * math.exp(r.gauss(0, 0.01) * batch)
        for i in range(ticks_per_instrument):
            # STEP 2 - random walk price, bid/ask spread, event time spread across the day
            p *= math.exp(r.gauss(0, 0.0008))
            spread = p * (0.0002 if asset_class == "FX" else 0.0005)
            ts = day + dt.timedelta(seconds=i * 100 + r.randint(0, 50))
            rows.append(dict(tick_id=f"{sym}-{batch:04d}-{i:06d}", symbol=sym, asset_class=asset_class,
                             price=round(p, 6), bid=round(p - spread, 6), ask=round(p + spread, 6),
                             size=r.randint(1, 500) * (1000 if asset_class == "FX" else 10),
                             event_ts=ts.isoformat() + "Z", source_seq=batch * 1_000_000 + i))
    # STEP 3 - inject the problems the pipeline must handle
    rows += [dict(x) for x in r.sample(rows, len(rows) // 50)]                      # duplicates
    if batch >= 1:
        for x in r.sample(rows, 5):                                                    # late beyond watermark
            late = dict(x, tick_id=x["tick_id"] + "-LATE")
            late["event_ts"] = (dt.datetime.fromisoformat(x["event_ts"][:-1]) - dt.timedelta(hours=27)).isoformat() + "Z"
            rows.append(late)
    rows += [dict(rows[0], tick_id=f"BAD-{batch}-1", symbol=None),
             dict(rows[1], tick_id=f"BAD-{batch}-2", price=-1.0)]
    r.shuffle(rows)                                                                     # out-of-order arrival
    # STEP 4 - write the batch as JSON lines
    path = os.path.join(root, f"batch_{batch:04d}")
    os.makedirs(path, exist_ok=True)
    with open(os.path.join(path, "ticks.json"), "w") as f:
        for x in rows:
            f.write(json.dumps(x) + "\n")
    print(f"wrote {len(rows)} ticks to {path}")


def main(argv=None):
    p = argparse.ArgumentParser(description="Synthetic market tick generator")
    p.add_argument("--catalog", required=True, help="e.g. finance_dev")
    p.add_argument("--batch-id", type=int, default=0)
    p.add_argument("--output-root", default=None)
    a = p.parse_args(argv)
    generate(a.output_root or f"/Volumes/{a.catalog}/bronze/landing/ticks", a.batch_id)


if __name__ == "__main__":
    main(sys.argv[1:])
