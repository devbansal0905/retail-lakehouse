"""Materialize the dashboard payload into a single-row Delta snapshot each batch.

The web layer then reads one tiny row instead of scanning the gold tables and the
data-quality log on every request. KPI aggregation runs once per batch in Spark
(distributed) rather than per dashboard refresh per client, and the read path
becomes O(1). Also holds the periodic OPTIMIZE/VACUUM maintenance.
"""
from __future__ import annotations

import json
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql import types as T

import config
from config import (
    DISPLAY_TZ,
    DQ_CONTROL_DIR,
    GOLD_INVOICE_DIR,
    GOLD_PRODUCT_DIR,
    SERVING_SNAPSHOT_DIR,
    SILVER_DIR,
    p,
)

_SNAPSHOT_SCHEMA = T.StructType([
    T.StructField("snapshot_version", T.LongType()),
    T.StructField("generated_at", T.StringType()),
    T.StructField("payload", T.StringType()),
])


def _overview_and_rollups(spark) -> dict:
    """Compute the KPI rollups from the compact gold state tables (Spark)."""
    gi = spark.read.format("delta").load(p(GOLD_INVOICE_DIR))
    gp = spark.read.format("delta").load(p(GOLD_PRODUCT_DIR))

    sales = gi.filter(~F.col("is_cancellation"))
    o = sales.agg(
        F.coalesce(F.round(F.sum("amount"), 2), F.lit(0.0)).alias("rev"),
        F.count(F.lit(1)).alias("orders"),
        F.coalesce(F.round(F.avg("amount"), 2), F.lit(0.0)).alias("aov"),
    ).first()
    cust = gi.filter(F.col("customer_id").isNotNull())
    total_cust = cust.select("customer_id").distinct().count()

    overview = {
        "total_revenue": round(o["rev"] or 0.0, 2),
        "total_orders": int(o["orders"] or 0),
        "avg_order_value": round(o["aov"] or 0.0, 2),
        "unique_customers": int(total_cust),
        "revenue_per_customer": round((o["rev"] or 0.0) / total_cust, 2) if total_cust else 0.0,
    }
    country = [r.asDict() for r in
               gi.groupBy("country").agg(F.round(F.sum("amount"), 2).alias("revenue"),
                                         F.count(F.lit(1)).alias("orders"))
                 .orderBy(F.desc("revenue")).limit(12).collect()]
    customers = [r.asDict() for r in
                 cust.groupBy("customer_id").agg(
                     F.max("customer_name").alias("customer_name"),
                     F.round(F.sum("amount"), 2).alias("lifetime_value"),
                     F.count(F.lit(1)).alias("orders"),
                     F.max("last_order_date").alias("last_order_date"))
                 .orderBy(F.desc("lifetime_value")).limit(10).collect()]
    top_products = [r.asDict() for r in gp.orderBy(F.desc("revenue")).limit(10).collect()]
    return {"overview": overview, "country": country,
            "top_products": top_products, "customers": customers}


def _current_dq(batch_id: int, rep: dict) -> dict:
    return {
        "row_count": rep["row_count"],
        "rules": rep["rules"],
        "by_dimension": rep["by_dimension"],
        "critical_failures": rep["critical_failures"],
    }


def _overall_dq(spark) -> dict:
    from delta.tables import DeltaTable
    if not DeltaTable.isDeltaTable(spark, p(DQ_CONTROL_DIR)):
        return {"rows_checked": 0, "batches": 0, "critical_violations": 0,
                "rules": [], "by_dimension": {}, "critical_failures": []}
    df = spark.read.format("delta").load(p(DQ_CONTROL_DIR))
    rules, by_dim, crit = [], {}, []
    for row in df.collect():
        r = row.asDict()
        rules.append({"check": r["check_name"], "column": r["column_name"],
                      "dimension": r["dimension"], "critical": r["critical"],
                      "total": r["total"], "rows_passed": r["rows_passed"],
                      "rows_failed": r["rows_failed"], "passed_percent": r["passed_percent"]})
        d = by_dim.setdefault(r["dimension"], {"rows_failed": 0, "rules": 0})
        d["rows_failed"] += r["rows_failed"]
        d["rules"] += 1
        if r["critical"] and r["rows_failed"] > 0:
            crit.append(r["rule"])
    agg = df.agg(F.coalesce(F.max("total"), F.lit(0)).alias("rc"),
                 F.coalesce(F.max("batches"), F.lit(0)).alias("b"),
                 F.coalesce(F.sum(F.when(F.col("critical"), F.col("rows_failed")).otherwise(0)),
                            F.lit(0)).alias("cv")).first()
    return {"rows_checked": int(agg["rc"]), "batches": int(agg["b"]),
            "critical_violations": int(agg["cv"]),
            "rules": rules, "by_dimension": by_dim, "critical_failures": crit}


def build_snapshot(spark, batch_id: int, rep: dict) -> None:
    """Compute the dashboard payload from gold + DQ control and overwrite the
    single-row serving snapshot table."""
    generated_at = datetime.now(DISPLAY_TZ).isoformat(timespec="seconds")
    payload = {
        "version": int(batch_id),
        "generated_at": generated_at,
        **_overview_and_rollups(spark),
        "data_quality": {
            "batch_id": int(batch_id),
            "generated_at": config.now_display_str(),
            "current": _current_dq(batch_id, rep),
            "overall": _overall_dq(spark),
        },
    }
    row = [(int(batch_id), generated_at, json.dumps(payload, default=str))]
    (spark.createDataFrame(row, _SNAPSHOT_SCHEMA)
     .write.format("delta").mode("overwrite").save(p(SERVING_SNAPSHOT_DIR)))


def maybe_maintain(spark, batch_id: int) -> None:
    """Every OPTIMIZE_EVERY_N_BATCHES, compact + Z-ORDER on the MERGE keys and
    VACUUM. Z-ordering on the merge key keeps MERGE file-pruning effective as the
    tables grow. Best-effort: never let maintenance break the stream."""
    n = config.OPTIMIZE_EVERY_N_BATCHES
    if n <= 0 or batch_id == 0 or batch_id % n != 0:
        return
    targets = [(SILVER_DIR, "line_key"), (GOLD_INVOICE_DIR, "invoice_no"),
               (GOLD_PRODUCT_DIR, "stock_code")]
    for path, key in targets:
        try:
            spark.sql(f"OPTIMIZE delta.`{p(path)}` ZORDER BY ({key})")
            spark.sql(f"VACUUM delta.`{p(path)}` RETAIN {config.VACUUM_RETAIN_HOURS} HOURS")
        except Exception as exc:  # noqa: BLE001 - maintenance must not stop ingestion
            print(f"[maintenance] skipped {path}: {exc}")
