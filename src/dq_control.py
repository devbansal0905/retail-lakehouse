"""Data-quality control tables (Delta), updated per micro-batch.

Two Delta tables hold the data-quality state -- nothing is kept in memory:

  dq_runs     append-only log, one row per (batch, rule). The "current batch"
              view on the dashboard is just the newest batch_id in this table.

  dq_control  the cumulative control table, one row per rule, MERGE-upserted
              each batch: matched rows ACCUMULATE (t.rows_passed + s.rows_passed,
              t.total + s.total, ...) and the pass rate is recomputed from the
              running totals; unseen rules are inserted. New batches therefore
              fold into the running result with an additive Delta MERGE, the same
              control-table pattern used in production.

The dashboard reads both tables directly (delta-rs + DuckDB) -- see serving.py.
"""
from __future__ import annotations

import time

from pyspark.sql import functions as F
from pyspark.sql import types as T

import config
from config import DQ_CONTROL_DIR as DQ_CONTROL
from config import DQ_RUNS_DIR as DQ_RUNS
from config import p

# Schema of the per-batch run log (and the source rows fed into the MERGE).
_RUNS_SCHEMA = T.StructType([
    T.StructField("batch_id", T.LongType()),
    T.StructField("generated_ts", T.StringType()),
    T.StructField("rule", T.StringType()),
    T.StructField("check_name", T.StringType()),
    T.StructField("column_name", T.StringType()),
    T.StructField("dimension", T.StringType()),
    T.StructField("critical", T.BooleanType()),
    T.StructField("total", T.LongType()),
    T.StructField("rows_passed", T.LongType()),
    T.StructField("rows_failed", T.LongType()),
    T.StructField("passed_percent", T.DoubleType()),
])

# Additive accumulation: matched rules sum their counts and recompute the rate.
_CONTROL_SET = {
    "total": "t.total + s.total",
    "rows_passed": "t.rows_passed + s.rows_passed",
    "rows_failed": "t.rows_failed + s.rows_failed",
    "passed_percent": "round(100.0 * (t.rows_passed + s.rows_passed) / (t.total + s.total), 2)",
    "batches": "t.batches + 1",
    "last_batch_id": "s.last_batch_id",
    "updated_ts": "s.updated_ts",
}


def _runs_rows(batch_id: int, rep: dict) -> list[tuple]:
    ts = config.now_display_str()
    out = []
    for r in rep["rules"]:
        total = int(r["total"])
        passed = int(r["rows_passed"])
        pct = round(100.0 * passed / total, 2) if total else 100.0
        out.append((int(batch_id), ts, r["rule"], r["check"], r["column"],
                    r["dimension"], bool(r["critical"]), total, passed,
                    int(r["rows_failed"]), pct))
    return out


def _merge_control(spark, src) -> None:
    """Additive upsert into dq_control, retried on concurrent writers."""
    from delta.exceptions import DeltaConcurrentModificationException
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, p(DQ_CONTROL)):
        src.write.format("delta").mode("overwrite").save(p(DQ_CONTROL))
        return
    tgt = DeltaTable.forPath(spark, p(DQ_CONTROL))
    while True:
        try:
            (tgt.alias("t").merge(src.alias("s"), "t.rule = s.rule")
                .whenMatchedUpdate(set=_CONTROL_SET)
                .whenNotMatchedInsertAll()
                .execute())
            return
        except DeltaConcurrentModificationException:
            time.sleep(0.5)


def record_batch(spark, batch_id: int, rep: dict) -> None:
    """Persist one batch's DQ report: append the run log, then MERGE the
    cumulative control table. No state is held between batches in memory."""
    runs = spark.createDataFrame(_runs_rows(batch_id, rep), _RUNS_SCHEMA)
    runs.write.format("delta").mode("append").save(p(DQ_RUNS))

    control_src = runs.select(
        "rule", "check_name", "column_name", "dimension", "critical",
        "total", "rows_passed", "rows_failed", "passed_percent",
        F.lit(1).cast("long").alias("batches"),
        F.col("batch_id").alias("last_batch_id"),
        F.col("generated_ts").alias("updated_ts"))
    _merge_control(spark, control_src)
