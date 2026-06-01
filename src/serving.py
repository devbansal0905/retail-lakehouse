"""Read path for the dashboard and NL-to-SQL agent.

The dashboard reads a single-row serving snapshot table (materialized by the
pipeline, see serving_state.py), so a refresh is an O(1) read of one row rather
than a scan of the gold tables. The snapshot's Delta version is the SSE change
signal. The NL-to-SQL agent still queries the gold tables directly via DuckDB,
but only when a question is asked, not on every refresh.
"""
from __future__ import annotations

import json

from config import GOLD_INVOICE_DIR, GOLD_PRODUCT_DIR, SERVING_SNAPSHOT_DIR, p


def _table(path):
    from deltalake import DeltaTable
    try:
        return DeltaTable(p(path))
    except Exception:  # noqa: BLE001 - table not created until the first batch
        return None


def latest_version() -> str | None:
    """Delta version of the serving snapshot; the SSE loop pushes when it changes."""
    t = _table(SERVING_SNAPSHOT_DIR)
    return str(t.version()) if t is not None else None


def build_payload() -> dict | None:
    """Return the precomputed dashboard payload (one tiny row), or None if the
    pipeline has not written a snapshot yet."""
    t = _table(SERVING_SNAPSHOT_DIR)
    if t is None:
        return None
    rows = t.to_pyarrow_table().column("payload").to_pylist()
    if not rows:
        return None
    return json.loads(rows[0])


# ----------------------- NL-to-SQL query surface -----------------------------
# Queried only when a user asks a question, so a direct gold read is fine here.

def _build_kpi_views(con) -> None:
    con.execute("""
        CREATE TABLE kpi_overview AS
        SELECT
          round(coalesce((SELECT sum(amount) FROM gold_invoice WHERE NOT is_cancellation), 0), 2) AS total_revenue,
          (SELECT count(*) FROM gold_invoice WHERE NOT is_cancellation) AS total_orders,
          round(coalesce((SELECT avg(amount) FROM gold_invoice WHERE NOT is_cancellation), 0), 2) AS avg_order_value,
          (SELECT count(DISTINCT customer_id) FROM gold_invoice WHERE customer_id IS NOT NULL) AS unique_customers,
          round(coalesce((SELECT sum(amount) FROM gold_invoice WHERE NOT is_cancellation), 0)
                / nullif((SELECT count(DISTINCT customer_id) FROM gold_invoice WHERE customer_id IS NOT NULL), 0), 2) AS revenue_per_customer
    """)
    con.execute("""
        CREATE TABLE kpi_country AS
        SELECT country, round(sum(amount), 2) AS revenue, count(*) AS orders
        FROM gold_invoice GROUP BY country ORDER BY revenue DESC
    """)
    con.execute("""
        CREATE TABLE kpi_top_products AS
        SELECT stock_code, description, round(revenue, 2) AS revenue, units
        FROM gold_product ORDER BY revenue DESC
    """)
    con.execute("""
        CREATE TABLE kpi_customers AS
        SELECT customer_id, max(customer_name) AS customer_name,
               round(sum(amount), 2) AS lifetime_value, count(*) AS orders,
               CAST(max(last_order_date) AS VARCHAR) AS last_order_date
        FROM gold_invoice WHERE customer_id IS NOT NULL
        GROUP BY customer_id ORDER BY lifetime_value DESC
    """)


def duckdb_for_nl():
    """DuckDB connection with the KPI tables registered, or None if gold is empty."""
    import duckdb
    gi, gp = _table(GOLD_INVOICE_DIR), _table(GOLD_PRODUCT_DIR)
    if gi is None or gp is None:
        return None
    con = duckdb.connect()
    con.register("gold_invoice", gi.to_pyarrow_table())
    con.register("gold_product", gp.to_pyarrow_table())
    _build_kpi_views(con)
    return con
