"""Download the real UCI 'Online Retail II' dataset and adapt it to the
pipeline's expected CSV schema, so you can run on real data instead of the
synthetic generator.

    python scripts/download_real_data.py

Then run the pipeline as usual (skip generate_data; the raw CSV is in place).
Requires `openpyxl` to read the source .xlsx:  pip install openpyxl
"""
from __future__ import annotations

import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from config import RAW_CSV, RAW_DIR, ensure_dirs  # noqa: E402

URL = "https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip"


def main() -> None:
    ensure_dirs()
    import pandas as pd

    zip_path = RAW_DIR / "online_retail_ii.zip"
    print(f"Downloading {URL} ...")
    urllib.request.urlretrieve(URL, zip_path)

    # The archive contains an .xlsx with two sheets (2009-10 and 2010-11).
    print("Reading workbook (needs openpyxl)...")
    sheets = pd.read_excel(zip_path, sheet_name=None, engine="openpyxl")
    df = pd.concat(sheets.values(), ignore_index=True)

    # Map UCI columns -> our pipeline schema.
    df = df.rename(columns={
        "Invoice": "InvoiceNo", "Price": "UnitPrice", "Customer ID": "CustomerID",
    })
    cols = ["InvoiceNo", "StockCode", "Description", "Quantity",
            "InvoiceDate", "UnitPrice", "CustomerID", "Country"]
    df = df[cols]
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    df.to_csv(RAW_CSV, index=False)
    print(f"Wrote {len(df):,} rows -> {RAW_CSV}")


if __name__ == "__main__":
    main()
