"""BRONZE: structured-streaming ingest of landed JSON events into Delta.

- Reads the landing zone as a stream with an explicit schema (never infer
  schema on a stream).
- Uses trigger=availableNow so it drains all currently-available files then
  stops, which makes it equally usable as a batch job and as an incremental
  micro-batch job (run it again and it only picks up new files).
- Appends raw, untyped-ish events plus ingestion metadata. Bronze is a
  faithful, append-only record of what arrived.

On Databricks you'd swap `.format("json")` for Auto Loader
(`.format("cloudFiles")`); everything else is identical.
"""
from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from config import BRONZE_DIR, CHECKPOINT_DIR, LANDING_DIR, ensure_dirs, p
from spark_session import get_spark

# Raw landing schema: read everything as string, cast later in silver.
LANDING_SCHEMA = StructType([
    StructField("InvoiceNo", StringType()),
    StructField("StockCode", StringType()),
    StructField("Description", StringType()),
    StructField("Quantity", StringType()),
    StructField("InvoiceDate", StringType()),
    StructField("UnitPrice", StringType()),
    StructField("CustomerID", StringType()),
    StructField("CustomerName", StringType()),
    StructField("Country", StringType()),
])


def run() -> int:
    ensure_dirs()
    spark = get_spark("bronze-ingest")

    stream = (
        spark.readStream
        .schema(LANDING_SCHEMA)
        .json(p(LANDING_DIR))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )

    query = (
        stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", p(CHECKPOINT_DIR / "bronze"))
        .trigger(availableNow=True)
        .start(p(BRONZE_DIR))
    )
    query.awaitTermination()

    count = spark.read.format("delta").load(p(BRONZE_DIR)).count()
    print(f"BRONZE rows: {count:,}")
    spark.stop()
    return count


if __name__ == "__main__":
    run()
