"""Incremental gold maintenance from the silver Change Data Feed. Each change is signed
(+1 for insert/update_postimage, -1 for delete/update_preimage) and folded into the
additive state tables gold_invoice and gold_product via an additive Delta MERGE, so
per-batch cost scales with change volume rather than table size.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import GOLD_INVOICE_DIR as GOLD_INVOICE
from config import GOLD_PRODUCT_DIR as GOLD_PRODUCT
from config import p

_INSERT_LIKE = ["insert", "update_postimage"]
_DELETE_LIKE = ["delete", "update_preimage"]


def signed_changes(cdf: DataFrame) -> DataFrame:
    """Attach +1/-1 sign per CDF row and signed amount/quantity columns."""
    sgn = (F.when(F.col("_change_type").isin(_INSERT_LIKE), F.lit(1))
            .when(F.col("_change_type").isin(_DELETE_LIKE), F.lit(-1))
            .otherwise(F.lit(0)))
    return (cdf.withColumn("_sgn", sgn).filter(F.col("_sgn") != 0)
            .withColumn("_is_c", F.col("invoice_no").startswith("C"))
            .withColumn("amt", F.col("line_amount") * F.col("_sgn"))
            .withColumn("qty", F.col("quantity") * F.col("_sgn")))


def invoice_deltas(signed: DataFrame) -> DataFrame:
    """Net per-invoice change: additive amount + stable invoice attributes."""
    return (signed.groupBy("invoice_no").agg(
        F.first("country", ignorenulls=True).alias("country"),
        F.first("customer_id", ignorenulls=True).alias("customer_id"),
        F.first("customer_name", ignorenulls=True).alias("customer_name"),
        F.first("_is_c").alias("is_cancellation"),
        F.max(F.to_date("invoice_ts")).alias("last_order_date"),
        F.round(F.sum("amt"), 2).alias("amount")))


def product_deltas(signed: DataFrame) -> DataFrame:
    """Net per-product change: additive revenue + units."""
    return (signed.groupBy("stock_code").agg(
        F.first("description", ignorenulls=True).alias("description"),
        F.round(F.sum("amt"), 2).alias("revenue"),
        F.sum("qty").alias("units")))


def _merge_add(spark, path, delta_df, key, add_cols, set_cols, or_cols) -> None:
    """Additive upsert: matched rows accumulate (t.c + s.c); new keys insert."""
    from delta.tables import DeltaTable
    if not DeltaTable.isDeltaTable(spark, p(path)):
        delta_df.write.format("delta").mode("overwrite").save(p(path))
        return
    tgt = DeltaTable.forPath(spark, p(path))
    setmap = {c: f"t.{c} + s.{c}" for c in add_cols}
    setmap.update({c: f"s.{c}" for c in set_cols})
    setmap.update({c: f"t.{c} OR s.{c}" for c in or_cols})
    (tgt.alias("t").merge(delta_df.alias("s"), f"t.{key} = s.{key}")
        .whenMatchedUpdate(set=setmap)
        .whenNotMatchedInsertAll()
        .execute())


def apply_cdf_to_gold(spark, cdf: DataFrame) -> None:
    """Fold one batch of silver changes into the compact gold state tables."""
    signed = signed_changes(cdf)
    _merge_add(spark, GOLD_INVOICE, invoice_deltas(signed), key="invoice_no",
               add_cols=["amount"],
               set_cols=["country", "customer_id", "customer_name", "last_order_date"],
               or_cols=["is_cancellation"])
    _merge_add(spark, GOLD_PRODUCT, product_deltas(signed), key="stock_code",
               add_cols=["revenue", "units"], set_cols=["description"], or_cols=[])
