"""SILVER: clean, type, dedupe, and CDC-merge bronze events into Delta."""
from __future__ import annotations

import time

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import BRONZE_DIR, SILVER_DIR, ensure_dirs, p
from spark_session import get_spark


def clean_and_type(df: DataFrame) -> DataFrame:
    # customer_name is optional (real UCI data / older inputs lack it)
    name_col = (F.trim(F.col("CustomerName")) if "CustomerName" in df.columns
                else F.lit(None).cast("string"))
    return (
        df.withColumn("customer_name", name_col)
          .withColumn("invoice_no", F.trim("InvoiceNo"))
          .withColumn("stock_code", F.trim("StockCode"))
          .withColumn("description", F.trim("Description"))
          .withColumn("quantity", F.col("Quantity").cast("int"))
          .withColumn("unit_price", F.col("UnitPrice").cast("double"))
          .withColumn("invoice_ts", F.to_timestamp("InvoiceDate", "yyyy-MM-dd HH:mm:ss"))
          .withColumn("customer_id",
                      F.when(F.trim("CustomerID") == "", None)
                       .otherwise(F.col("CustomerID")).cast("int"))
          .withColumn("country", F.trim("Country"))
          .withColumn("line_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
          .withColumn("is_cancellation",
                      F.col("invoice_no").startswith("C") | (F.col("quantity") < 0))
          .select("invoice_no", "stock_code", "description", "quantity",
                  "unit_price", "line_amount", "invoice_ts", "customer_id",
                  "customer_name", "country", "is_cancellation")
    )


def filter_valid(df: DataFrame) -> DataFrame:
    return df.filter(
        F.col("invoice_no").isNotNull()
        & F.col("stock_code").isNotNull()
        & F.col("invoice_ts").isNotNull()
        & F.col("unit_price").isNotNull() & (F.col("unit_price") > 0)
        & F.col("quantity").isNotNull() & (F.col("quantity") != 0)
    )


def dedupe(df: DataFrame) -> DataFrame:
    deduped = df.dropDuplicates(
        ["invoice_no", "stock_code", "quantity", "unit_price", "invoice_ts"]
    )
    return deduped.withColumn(
        "line_key",
        F.sha2(F.concat_ws("||",
                           F.col("invoice_no"), F.col("stock_code"),
                           F.col("quantity").cast("string"),
                           F.col("unit_price").cast("string"),
                           F.coalesce(F.col("invoice_ts").cast("string"), F.lit(""))), 256),
    )


def transform(df: DataFrame) -> DataFrame:
    return dedupe(filter_valid(clean_and_type(df)))


def merge_with_retry(tgt, source, condition: str, max_retries: int = 5) -> None:
    from delta.exceptions import DeltaConcurrentModificationException
    attempt = 0
    while True:
        try:
            (tgt.alias("t")
                .merge(source.alias("s"), condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute())
            return
        except DeltaConcurrentModificationException:
            attempt += 1
            if attempt > max_retries:
                raise
            time.sleep(min(2 ** attempt, 10))


def run() -> int:
    ensure_dirs()
    spark = get_spark("silver-transform")
    from delta.tables import DeltaTable

    bronze = spark.read.format("delta").load(p(BRONZE_DIR))
    silver = transform(bronze)

    if DeltaTable.isDeltaTable(spark, p(SILVER_DIR)):
        tgt = DeltaTable.forPath(spark, p(SILVER_DIR))
        merge_with_retry(tgt, silver, "t.line_key = s.line_key")
    else:
        silver.write.format("delta").mode("overwrite").save(p(SILVER_DIR))

    count = spark.read.format("delta").load(p(SILVER_DIR)).count()
    print(f"SILVER rows: {count:,}")
    spark.stop()
    return count


if __name__ == "__main__":
    run()
