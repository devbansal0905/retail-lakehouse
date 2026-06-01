"""Spark Structured Streaming pipeline. Per non-empty micro-batch: append to bronze,
CDC-MERGE into silver (Change Data Feed enabled), fold the silver CDF into the gold
state tables, then run data quality and MERGE the result into the Delta control tables.
"""
from __future__ import annotations

import os

from pyspark.sql import functions as F

import dq_checks as DQ
import dq_control as DQC
import gold_incremental as GI
import serving_state as SS
import silver_transform as ST
from bronze_ingest import LANDING_SCHEMA
from config import BRONZE_DIR, CHECKPOINT_DIR, LANDING_DIR, SILVER_DIR, ensure_dirs, p
from dq_rules import SILVER_RULES
from spark_session import get_spark


def _silver_version(spark) -> int:
    from delta.tables import DeltaTable
    return DeltaTable.forPath(spark, p(SILVER_DIR)).history(1).select("version").first()[0]


def process_batch(batch_df, batch_id: int) -> None:
    if batch_df.isEmpty():
        return  # change-driven: nothing new, don't touch the tables
    spark = batch_df.sparkSession

    # 1) BRONZE: append raw events
    batch_df.write.format("delta").mode("append").option("mergeSchema", "true").save(p(BRONZE_DIR))

    # 2) SILVER: clean -> CDC MERGE; capture the version range it writes
    from delta.tables import DeltaTable
    silver_new = ST.transform(batch_df)
    if DeltaTable.isDeltaTable(spark, p(SILVER_DIR)):
        start_version = _silver_version(spark) + 1
        ST.merge_with_retry(DeltaTable.forPath(spark, p(SILVER_DIR)),
                            silver_new, "t.line_key = s.line_key")
    else:
        (silver_new.write.format("delta")
         .option("delta.enableChangeDataFeed", "true")
         .mode("overwrite").save(p(SILVER_DIR)))
        start_version = 0

    # 3) CDF: read only the rows that changed, fold into gold state
    changes = (spark.read.format("delta")
               .option("readChangeFeed", "true")
               .option("startingVersion", start_version)
               .load(p(SILVER_DIR)))
    GI.apply_cdf_to_gold(spark, changes)

    # 4) DATA QUALITY: score this batch (pre-filter, so injected bad rows are
    #    visible) and MERGE the result into the Delta control tables.
    src = ST.dedupe(ST.clean_and_type(batch_df))
    rep = DQ.run_expectations(src, SILVER_RULES)
    DQC.record_batch(spark, batch_id, rep)

    # 5) SERVING: materialise the dashboard payload into a one-row snapshot the
    #    web layer reads directly, then run periodic table maintenance.
    SS.build_snapshot(spark, batch_id, rep)
    SS.maybe_maintain(spark, batch_id)

    print(f"[batch {batch_id}] CDF (v>={start_version}) folded into gold; "
          f"DQ control + serving snapshot updated ({rep['row_count']} rows checked)")


def run(trigger_seconds: float = 5.0) -> None:
    ensure_dirs()
    spark = get_spark("stream-pipeline")
    stream = (spark.readStream
              .schema(LANDING_SCHEMA)
              .json(p(LANDING_DIR))
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_source_file", F.input_file_name()))
    query = (stream.writeStream
             .foreachBatch(process_batch)
             .option("checkpointLocation", p(CHECKPOINT_DIR / "stream"))
             .trigger(processingTime=f"{trigger_seconds} seconds")
             .start())
    print("streaming pipeline started; waiting for events...")
    query.awaitTermination()


if __name__ == "__main__":
    run(float(os.environ.get("STREAM_TRIGGER", "5")))
