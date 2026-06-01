"""Builds a Delta-enabled local SparkSession for local and CI runs."""
from __future__ import annotations

from pyspark.sql import SparkSession

try:
    from delta import configure_spark_with_delta_pip
    _HAS_DELTA = True
except Exception:  # pragma: no cover - delta always present via requirements
    _HAS_DELTA = False


def get_spark(app_name: str = "retail-lakehouse", shuffle_partitions: int = 4) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", "Asia/Kolkata")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .config("spark.driver.memory", "2g")
        # Quieter local runs.
        .config("spark.ui.showConsoleProgress", "false")
    )
    if _HAS_DELTA:
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    else:  # pragma: no cover
        spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
