"""Paths and timezone configuration. All paths derive from RETAIL_LAKEHOUSE_HOME
(default ./lakehouse); timestamps are stored in UTC and shown in DISPLAY_TZ.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

# India Standard Time (no DST -> fixed UTC+05:30; no tzdata dependency needed).
IST = timezone(timedelta(hours=5, minutes=30))


def now_ist_str(fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    return datetime.now(IST).strftime(fmt)


# Display timezone (configurable). Data is stored in UTC; timestamps are shown
# in this tz. Default 330 minutes = IST (UTC+05:30).
DISPLAY_TZ = timezone(timedelta(minutes=int(os.environ.get("DISPLAY_TZ_OFFSET_MINUTES", "330"))))


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def now_display_str(fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    return datetime.now(DISPLAY_TZ).strftime(fmt)


def to_display(utc_iso: str, fmt: str = "%H:%M:%S") -> str:
    """Convert a stored UTC ISO timestamp to the configured display tz."""
    try:
        dt = datetime.fromisoformat(utc_iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(DISPLAY_TZ).strftime(fmt)
    except Exception:
        return utc_iso

# Root for all generated data + Delta tables (NOT the code repo).
HOME = Path(os.environ.get("RETAIL_LAKEHOUSE_HOME", Path.cwd() / "lakehouse")).resolve()

# Raw source CSV (synthetic generator output, or the real UCI dataset).
RAW_DIR = HOME / "raw"
RAW_CSV = RAW_DIR / "online_retail.csv"

# Landing zone the producer writes JSON event files into.
LANDING_DIR = HOME / "landing"

# Medallion Delta layers.
BRONZE_DIR = HOME / "bronze" / "sales_events"
SILVER_DIR = HOME / "silver" / "sales"
GOLD_DIR = HOME / "gold"

# Gold state + control tables (all Delta). The streaming pipeline is the only
# writer; the dashboard reads these tables directly (delta-rs + DuckDB).
GOLD_INVOICE_DIR = GOLD_DIR / "gold_invoice"        # one row per invoice
GOLD_PRODUCT_DIR = GOLD_DIR / "gold_product"        # one row per product
DQ_CONTROL_DIR = GOLD_DIR / "dq_control"            # cumulative per-rule (MERGE-upserted)
DQ_RUNS_DIR = GOLD_DIR / "dq_runs"                  # append-only per-batch DQ log
SERVING_SNAPSHOT_DIR = GOLD_DIR / "serving_snapshot"  # single-row dashboard payload (read by the web layer)

# Streaming checkpoints.
CHECKPOINT_DIR = HOME / "_checkpoints"

# Serving layer (legacy batch CSV export; the realtime app reads Delta directly).
SERVE_DIR = HOME / "serve"

# Data-quality reports.
DQ_DIR = HOME / "dq_reports"


# --- scaling / maintenance knobs -------------------------------------------
# Run OPTIMIZE (+ Z-ORDER) and VACUUM every N non-empty batches (0 = never).
OPTIMIZE_EVERY_N_BATCHES = int(os.environ.get("OPTIMIZE_EVERY_N_BATCHES", "50"))
# VACUUM retention; Delta requires >= 168h unless the safety check is disabled.
VACUUM_RETAIN_HOURS = int(os.environ.get("VACUUM_RETAIN_HOURS", "168"))
# Minimum seconds between SSE pushes (coalesces bursts of frequent commits).
SSE_MIN_INTERVAL_SECONDS = float(os.environ.get("SSE_MIN_INTERVAL_SECONDS", "1.0"))


def ensure_dirs() -> None:
    for p in (RAW_DIR, LANDING_DIR, BRONZE_DIR.parent, SILVER_DIR.parent,
              GOLD_DIR, CHECKPOINT_DIR, SERVE_DIR, DQ_DIR):
        p.mkdir(parents=True, exist_ok=True)


def p(path) -> str:
    """Delta/Spark want string paths."""
    return str(path)
