"""Generate a synthetic Online-Retail-style dataset (schema matches UCI Online Retail
II; see scripts/download_real_data.py to swap in the real file). Injects some messy
rows so the data-quality checks have violations to catch.
"""
from __future__ import annotations

import argparse
import csv
import random
from datetime import datetime, timedelta

from config import RAW_CSV, ensure_dirs

NAMES = [
    "Aarav Sharma", "Diya Patel", "Vivaan Reddy", "Ananya Iyer", "Aditya Nair",
    "Isha Gupta", "Kabir Singh", "Saanvi Rao", "Arjun Mehta", "Myra Joshi",
    "Reyansh Das", "Aadhya Kulkarni", "Krishna Verma", "Anika Bose", "Ishaan Khan",
    "Navya Pillai", "Shaurya Menon", "Kiara Chopra", "Atharv Ghosh", "Pari Saxena",
]


def name_for(customer_id: int) -> str:
    """Deterministic name for a customer id (stable across runs)."""
    return NAMES[int(customer_id) % len(NAMES)]


COUNTRIES = ["United Kingdom", "Germany", "France", "EIRE", "Spain", "Netherlands", "Australia"]
PRODUCTS = [
    ("85123A", "WHITE HANGING HEART T-LIGHT HOLDER", 2.55),
    ("71053", "WHITE METAL LANTERN", 3.39),
    ("84406B", "CREAM CUPID HEARTS COAT HANGER", 2.75),
    ("84029G", "KNITTED UNION FLAG HOT WATER BOTTLE", 3.39),
    ("22633", "HAND WARMER UNION JACK", 1.85),
    ("22745", "POPPY'S PLAYHOUSE BEDROOM", 2.10),
    ("21730", "GLASS STAR FROSTED T-LIGHT HOLDER", 4.25),
    ("22197", "POPCORN HOLDER", 0.85),
    ("23166", "MEDIUM CERAMIC TOP STORAGE JAR", 1.25),
    ("47566", "PARTY BUNTING", 4.95),
]


def _row(invoice, dt, cust, country, rng):
    code, desc, price = rng.choice(PRODUCTS)
    qty = rng.randint(1, 24)
    return {
        "InvoiceNo": invoice,
        "StockCode": code,
        "Description": desc,
        "Quantity": qty,
        "InvoiceDate": dt.strftime("%Y-%m-%d %H:%M:%S"),
        "UnitPrice": round(price * rng.uniform(0.9, 1.1), 2),
        "CustomerID": cust,
        "CustomerName": name_for(cust),
        "Country": country,
    }


def generate(n_invoices: int, seed: int = 42) -> int:
    ensure_dirs()
    rng = random.Random(seed)
    start = datetime(2011, 1, 1, 8, 0, 0)
    rows = []
    invoice_seq = 536365

    for _ in range(n_invoices):
        invoice = str(invoice_seq)
        invoice_seq += 1
        dt = start + timedelta(minutes=rng.randint(0, 525_600))  # within a year
        cust = rng.randint(12346, 12500)
        country = rng.choice(COUNTRIES)
        for _ in range(rng.randint(1, 6)):  # line items
            rows.append(_row(invoice, dt, cust, country, rng))

    # ---- inject messy data so DQ has work to do (~3% of rows) ----
    dirty = max(1, len(rows) // 33)
    for _ in range(dirty):
        r = dict(rng.choice(rows))
        kind = rng.choice(["null_cust", "neg_qty", "zero_price", "dup"])
        if kind == "null_cust":
            r["CustomerID"] = ""
        elif kind == "neg_qty":
            r["Quantity"] = -abs(r["Quantity"])  # cancellations / returns
        elif kind == "zero_price":
            r["UnitPrice"] = 0.0
        # "dup" => append an exact duplicate of an existing row
        rows.append(r)

    rng.shuffle(rows)

    with open(RAW_CSV, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    print(f"Wrote {len(rows):,} rows ({dirty} intentionally dirty) -> {RAW_CSV}")
    return len(rows)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--invoices", type=int, default=2000,
                    help="number of invoices (each has several line items)")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    generate(args.invoices, args.seed)
