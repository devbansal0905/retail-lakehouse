"""Serving layer -- reads the gold + data-quality Delta tables directly.

The dashboard and the NL-to-SQL agent both read straight from the Delta tables
(gold_invoice, gold_product, dq_runs, dq_control) using delta-rs + DuckDB SQL.
There is no Spark/JVM in the web process and no intermediate snapshot file: the
streaming pipeline is the only writer, this module is read-only.

The current Delta version of the gold/control tables is the change signal the
SSE endpoint watches -- when any of them commits a new version, the dashboard
re-reads and pushes.
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from config import DISPLAY_TZ, DQ_CONTROL_DIR, DQ_RUNS_DIR, GOLD_INVOICE_DIR, GOLD_PRODUCT_DIR, p


def _table(path):
    """Open a Delta table read-only, or None if it hasn't been created yet."""
    from deltalake import DeltaTable
    try:
        return DeltaTable(p(path))
    except Exception:  # noqa: BLE001 - table not created until the first batch
        return None


def _version(path) -> int | None:
    t = _table(path)
    return t.version() if t is not None else None


def latest_version() -> str | None:
    """A change token combining the gold + control table versions; None until
    the gold state exists. The SSE loop pushes whenever this token changes."""
    gi = _version(GOLD_INVOICE_DIR)
    if gi is None:
        return None
    gp = _version(GOLD_PRODUCT_DIR)
    dc = _version(DQ_CONTROL_DIR)
    return f"{gi}.{gp}.{dc}"


def _jsonable(v):
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    return v


def _rows(cur) -> list[dict]:
    cols = [c[0] for c in cur.description]
    return [{c: _jsonable(v) for c, v in zip(cols, r, strict=False)} for r in cur.fetchall()]


def _connect_gold():
    """DuckDB connection with gold_invoice / gold_product registered, or None."""
    import duckdb
    gi, gp = _table(GOLD_INVOICE_DIR), _table(GOLD_PRODUCT_DIR)
    if gi is None or gp is None:
        return None
    con = duckdb.connect()
    con.register("gold_invoice", gi.to_pyarrow_table())
    con.register("gold_product", gp.to_pyarrow_table())
    return con


def _build_kpi_views(con) -> None:
    """Create the four KPI tables the catalog/NL-to-SQL agent expects, derived
    from the gold state with SQL."""
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
    """Return a DuckDB connection with the KPI tables registered (for NL-to-SQL),
    or None if the gold state has not been created yet."""
    con = _connect_gold()
    if con is None:
        return None
    _build_kpi_views(con)
    return con


def _read_kpis() -> dict | None:
    con = _connect_gold()
    if con is None:
        return None
    _build_kpi_views(con)
    overview = _rows(con.execute("SELECT * FROM kpi_overview"))[0]
    country = _rows(con.execute("SELECT * FROM kpi_country LIMIT 12"))
    top_products = _rows(con.execute("SELECT * FROM kpi_top_products LIMIT 10"))
    customers = _rows(con.execute("SELECT * FROM kpi_customers LIMIT 10"))
    con.close()
    return {"overview": overview, "country": country,
            "top_products": top_products, "customers": customers}


def _dq_view(con, table: str, where: str = "") -> dict:
    """Summarise a DQ table (dq_runs slice or dq_control) into the shape the
    quality dashboard renders."""
    rules = _rows(con.execute(
        f"SELECT check_name AS check, column_name AS column, dimension, critical, "
        f"total, rows_passed, rows_failed, passed_percent FROM {table} {where} "
        f"ORDER BY dimension, rule"))
    by_dim: dict[str, dict] = {}
    crit: list[str] = []
    for r in rules:
        d = by_dim.setdefault(r["dimension"], {"rows_failed": 0, "rules": 0})
        d["rows_failed"] += r["rows_failed"]
        d["rules"] += 1
    crit = _rows(con.execute(
        f"SELECT rule FROM {table} {where} {'AND' if where else 'WHERE'} "
        f"critical AND rows_failed > 0"))
    return {"rules": rules, "by_dimension": by_dim,
            "critical_failures": [c["rule"] for c in crit]}


def _read_dq() -> dict | None:
    import duckdb
    runs, control = _table(DQ_RUNS_DIR), _table(DQ_CONTROL_DIR)
    if runs is None or control is None:
        return None
    con = duckdb.connect()
    con.register("dq_runs", runs.to_pyarrow_table())
    con.register("dq_control", control.to_pyarrow_table())

    latest = con.execute("SELECT max(batch_id) FROM dq_runs").fetchone()[0]
    cur = _dq_view(con, "dq_runs", where=f"WHERE batch_id = {int(latest)}")
    cur["row_count"] = con.execute(
        f"SELECT max(total) FROM dq_runs WHERE batch_id = {int(latest)}").fetchone()[0]
    gen = con.execute(
        f"SELECT max(generated_ts) FROM dq_runs WHERE batch_id = {int(latest)}").fetchone()[0]

    overall = _dq_view(con, "dq_control")
    agg = con.execute(
        "SELECT max(total), max(batches), "
        "coalesce(sum(CASE WHEN critical THEN rows_failed ELSE 0 END), 0) FROM dq_control").fetchone()
    overall["rows_checked"], overall["batches"], overall["critical_violations"] = (
        agg[0], agg[1], agg[2])
    con.close()
    return {"batch_id": int(latest), "generated_at": gen,
            "current": cur, "overall": overall}


def build_payload() -> dict | None:
    """Assemble the full dashboard payload (KPIs + data quality) by reading the
    Delta tables directly. Returns None until the pipeline has written gold."""
    kpis = _read_kpis()
    if kpis is None:
        return None
    payload = {
        "version": latest_version(),
        "generated_at": datetime.now(DISPLAY_TZ).isoformat(timespec="seconds"),
        **kpis,
    }
    dq = _read_dq()
    if dq is not None:
        payload["data_quality"] = dq
    return payload
