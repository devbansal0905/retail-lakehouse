"""GOLD: build a star schema + KPI tables from silver, export to the serve layer.

Star schema:
  dim_customer, dim_product, dim_date, fact_sales
KPIs:
  kpi_cltv          - customer lifetime value (net revenue, orders, recency)
  kpi_country       - revenue + orders by country
  kpi_top_products  - best sellers by net revenue
  kpi_overview      - single-row headline metrics (AOV, repeat-rate, etc.)

Gold tables are written as Delta (queryable by Spark/Genie) AND exported as CSV
to the serve layer so the Streamlit dashboard needs no Spark runtime.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import GOLD_DIR, SERVE_DIR, SILVER_DIR, ensure_dirs, p
from spark_session import get_spark

# ---------------------- dimension / fact builders (pure) ----------------------

def build_dim_customer(silver: DataFrame) -> DataFrame:
    return (silver.filter(F.col("customer_id").isNotNull())
            .groupBy("customer_id")
            .agg(F.first("customer_name", ignorenulls=True).alias("customer_name"),
                 F.first("country", ignorenulls=True).alias("country"),
                 F.min("invoice_ts").alias("first_seen"),
                 F.max("invoice_ts").alias("last_seen")))


def build_dim_product(silver: DataFrame) -> DataFrame:
    return (silver.groupBy("stock_code")
            .agg(F.first("description", ignorenulls=True).alias("description")))


def build_dim_date(silver: DataFrame) -> DataFrame:
    d = silver.select(F.to_date("invoice_ts").alias("date")).distinct()
    return (d.withColumn("year", F.year("date"))
             .withColumn("month", F.month("date"))
             .withColumn("day", F.dayofmonth("date"))
             .withColumn("day_of_week", F.date_format("date", "EEEE")))


def build_fact_sales(silver: DataFrame) -> DataFrame:
    return silver.select(
        "line_key", "invoice_no", "stock_code", "customer_id", "customer_name",
        F.to_date("invoice_ts").alias("date"),
        "quantity", "unit_price", "line_amount", "is_cancellation", "country",
    )


# ------------------------------- KPI builders --------------------------------

def build_kpi_cltv(fact: DataFrame) -> DataFrame:
    return (fact.filter(F.col("customer_id").isNotNull())
            .groupBy("customer_id")
            .agg(F.first("customer_name", ignorenulls=True).alias("customer_name"),
                 F.round(F.sum("line_amount"), 2).alias("lifetime_value"),
                 F.countDistinct("invoice_no").alias("orders"),
                 F.max("date").alias("last_order_date"))
            .orderBy(F.desc("lifetime_value")))


def build_kpi_country(fact: DataFrame) -> DataFrame:
    return (fact.groupBy("country")
            .agg(F.round(F.sum("line_amount"), 2).alias("revenue"),
                 F.countDistinct("invoice_no").alias("orders"))
            .orderBy(F.desc("revenue")))


def build_kpi_top_products(fact: DataFrame, dim_product: DataFrame) -> DataFrame:
    agg = (fact.groupBy("stock_code")
           .agg(F.round(F.sum("line_amount"), 2).alias("revenue"),
                F.sum("quantity").alias("units")))
    return (agg.join(dim_product, "stock_code", "left")
               .orderBy(F.desc("revenue")))


def build_kpi_overview(fact: DataFrame) -> DataFrame:
    # Pass 1: per-order rollup -> revenue, order count, AOV in a single action.
    orders = (fact.filter(~F.col("is_cancellation"))
              .groupBy("invoice_no").agg(F.sum("line_amount").alias("order_value")))
    o = orders.agg(F.coalesce(F.sum("order_value"), F.lit(0.0)).alias("revenue"),
                   F.count(F.lit(1)).alias("orders"),
                   F.coalesce(F.avg("order_value"), F.lit(0.0)).alias("aov")).first()
    # Pass 2: per-customer order counts -> unique + repeat rate in a single action.
    total_cust = (fact.filter(F.col("customer_id").isNotNull())
                  .select("customer_id").distinct().count())
    row = {
        "total_revenue": round(o["revenue"] or 0.0, 2),
        "total_orders": int(o["orders"] or 0),
        "avg_order_value": round(o["aov"] or 0.0, 2),
        "unique_customers": int(total_cust),
        "revenue_per_customer": round((o["revenue"] or 0.0) / total_cust, 2) if total_cust else 0.0,
    }
    return fact.sparkSession.createDataFrame([row])


# --------------------------------- runner ------------------------------------

_GOLD_TABLES = {}  # name -> builder closure, filled in run()


def run() -> dict:
    ensure_dirs()
    spark = get_spark("gold-model")
    silver = spark.read.format("delta").load(p(SILVER_DIR))

    dim_customer = build_dim_customer(silver)
    dim_product = build_dim_product(silver)
    dim_date = build_dim_date(silver)
    fact_sales = build_fact_sales(silver)

    tables = {
        "dim_customer": dim_customer,
        "dim_product": dim_product,
        "dim_date": dim_date,
        "fact_sales": fact_sales,
        "kpi_cltv": build_kpi_cltv(fact_sales),
        "kpi_country": build_kpi_country(fact_sales),
        "kpi_top_products": build_kpi_top_products(fact_sales, dim_product),
        "kpi_overview": build_kpi_overview(fact_sales),
    }

    counts = {}
    for name, df in tables.items():
        df = df.cache()
        (df.write.format("delta").mode("overwrite")
           .option("overwriteSchema", "true").save(p(GOLD_DIR / name)))
        # serve layer: single-file CSV for the dashboard
        (df.coalesce(1).write.option("header", True)
           .mode("overwrite").csv(p(SERVE_DIR / name)))
        counts[name] = df.count()

    print("GOLD tables:", counts)
    spark.stop()
    return counts


if __name__ == "__main__":
    run()
