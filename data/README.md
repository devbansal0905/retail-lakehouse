# Data

No raw data is committed to this repo (see `.gitignore`). You have two options:

### Option A - synthetic (default, offline)
The pipeline generates a synthetic Online-Retail-style dataset automatically:

```bash
python src/generate_data.py --invoices 2000
```

It deliberately injects ~3% messy rows (nulls, negative quantities, zero
prices, duplicates) so the data-quality layer has real violations to catch.

### Option B - the real UCI "Online Retail II" dataset
~1M real e-commerce transactions.

```bash
pip install openpyxl
python scripts/download_real_data.py
```

Source: UCI Machine Learning Repository, "Online Retail II"
<https://archive.ics.uci.edu/dataset/502/online+retail+ii>

Both options produce `lakehouse/raw/online_retail.csv` with the same schema, so
the rest of the pipeline is identical.
