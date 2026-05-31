"""Event producer for the landing zone. --mode stream emits synthetic sales events
continuously (one JSON file per tick); --mode batch replays an existing CSV once.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import random
import time
import uuid

from config import LANDING_DIR, RAW_CSV, ensure_dirs, now_ist_str

# ----- realistic data-quality noise -----------------------------------------
# Each kind maps to a specific DQ rule, with weights that mimic the real world:
# missing/typo data is common; broken keys are rare. ~4% of lines get one issue.
DIRTY_RATE = 0.04
_BAD_COUNTRIES = ["Wakanda", "Atlantis", "Narnia", "Westeros", "Unknown"]
_DIRTY_KINDS = [
    ("null_customer", 30),       # -> not_null::customer_id   (completeness)
    ("bad_country", 22),         # -> in_set::country         (validity)
    ("zero_or_neg_price", 16),   # -> min_value::unit_price   (accuracy, critical)
    ("huge_quantity", 12),       # -> between::quantity       (accuracy)
    ("bad_date", 10),            # -> not_null::invoice_ts    (completeness)
    ("null_stock", 6),           # -> not_null::stock_code    (completeness, critical)
    ("null_invoice", 4),         # -> not_null::invoice_no    (completeness, critical)
]


def _corrupt(r: dict, rng: random.Random) -> None:
    kind = rng.choices([k for k, _ in _DIRTY_KINDS],
                       weights=[w for _, w in _DIRTY_KINDS], k=1)[0]
    if kind == "null_customer":
        r["CustomerID"] = ""
    elif kind == "bad_country":
        r["Country"] = rng.choice(_BAD_COUNTRIES)
    elif kind == "zero_or_neg_price":
        r["UnitPrice"] = rng.choice([0.0, -round(r["UnitPrice"], 2)])
    elif kind == "huge_quantity":
        r["Quantity"] = rng.randint(10001, 99999)
    elif kind == "bad_date":
        r["InvoiceDate"] = "not-a-date"
    elif kind == "null_stock":
        r["StockCode"] = None
    elif kind == "null_invoice":
        r["InvoiceNo"] = None


def _write_file(rows) -> str:
    name = f"events-{uuid.uuid4().hex}.json"
    tmp = LANDING_DIR / (name + ".tmp")
    final = LANDING_DIR / name
    with open(tmp, "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    os.replace(tmp, final)  # atomic on the same filesystem; reader only sees complete files
    return str(final)


def run_batch(batch_size: int) -> None:
    ensure_dirs()
    with open(RAW_CSV, newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        raise SystemExit("No raw rows. Run generate_data.py first.")
    total = 0
    for i in range(0, len(rows), batch_size):
        chunk = rows[i:i + batch_size]
        _write_file(chunk)
        total += len(chunk)
    print(f"batch: landed {total:,} events")


def run_stream(interval: float, per_tick: int, seed: int | None) -> None:
    """Generate events endlessly so the pipeline always has fresh data."""
    ensure_dirs()
    from generate_data import COUNTRIES, PRODUCTS, name_for
    rng = random.Random(seed)
    invoice = 600000
    tick = 0
    print(f"stream: emitting ~{per_tick} line-events every {interval}s into {LANDING_DIR}")
    while True:
        rows = []
        # Build whole invoices: one customer, one timestamp, several line items.
        while len(rows) < per_tick:
            invoice += 1
            invoice_no = str(invoice)
            cust = rng.randint(12346, 13000)
            name = name_for(cust)
            country = rng.choice(COUNTRIES)
            ts = now_ist_str()
            for _ in range(rng.randint(1, 4)):     # line items in this invoice
                code, desc, price = rng.choice(PRODUCTS)
                r = {
                    "InvoiceNo": invoice_no,
                    "StockCode": code,
                    "Description": desc,
                    "Quantity": rng.randint(1, 24),
                    "InvoiceDate": ts,                 # IST timestamp
                    "UnitPrice": round(price * rng.uniform(0.9, 1.1), 2),
                    "CustomerID": cust,
                    "CustomerName": name,
                    "Country": country,
                }
                if rng.random() < DIRTY_RATE:          # realistic, weighted data issues
                    _corrupt(r, rng)
                rows.append(r)
        path = _write_file(rows)
        tick += 1
        print(f"tick {tick}: landed {len(rows)} line-events -> {path}")
        time.sleep(interval)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["batch", "stream"], default="stream")
    ap.add_argument("--batch-size", type=int, default=1000)
    ap.add_argument("--interval", type=float, default=2.0)
    ap.add_argument("--per-tick", type=int, default=80)
    ap.add_argument("--seed", type=int, default=None)
    a = ap.parse_args()
    if a.mode == "batch":
        run_batch(a.batch_size)
    else:
        run_stream(a.interval, a.per_tick, a.seed)
