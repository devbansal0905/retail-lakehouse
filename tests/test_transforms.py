"""Unit tests for the silver transforms, DQ rules, and gold KPIs.

Run with:  pytest -q
A local SparkSession is spun up once per session. These tests use tiny
in-memory data so they're fast and deterministic -- no Delta tables needed.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import dq_checks as DQ  # noqa: E402
import gold_model as GM  # noqa: E402
import nl_to_sql as NL  # noqa: E402
import silver_transform as ST  # noqa: E402
from spark_session import get_spark  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    s = get_spark("tests")
    yield s
    s.stop()


def _bronze(spark, rows):
    cols = ["InvoiceNo", "StockCode", "Description", "Quantity",
            "InvoiceDate", "UnitPrice", "CustomerID", "Country"]
    return spark.createDataFrame(rows, cols)


def test_clean_types_and_cancellation_flag(spark):
    df = _bronze(spark, [
        ("536365", "85123A", "WIDGET", "6", "2011-01-01 08:26:00", "2.55", "17850", "United Kingdom"),
        ("C536366", "85123A", "WIDGET", "-2", "2011-01-02 09:00:00", "2.55", "17850", "United Kingdom"),
    ])
    out = {r["invoice_no"]: r for r in ST.clean_and_type(df).collect()}
    assert out["536365"]["quantity"] == 6
    assert abs(out["536365"]["line_amount"] - 15.30) < 1e-6
    assert out["536365"]["is_cancellation"] is False
    assert out["C536366"]["is_cancellation"] is True


def test_filter_drops_invalid_rows(spark):
    df = _bronze(spark, [
        ("536365", "85123A", "OK", "6", "2011-01-01 08:26:00", "2.55", "17850", "UK"),
        ("536367", "85123A", "ZEROQTY", "0", "2011-01-01 08:26:00", "2.55", "17850", "UK"),
        ("536368", "85123A", "ZEROPRICE", "3", "2011-01-01 08:26:00", "0", "17850", "UK"),
        ("536369", None, "NOSTOCK", "3", "2011-01-01 08:26:00", "2.55", "17850", "UK"),
    ])
    valid = ST.filter_valid(ST.clean_and_type(df))
    invoices = {r["invoice_no"] for r in valid.collect()}
    assert invoices == {"536365"}


def test_dedupe_removes_exact_duplicates_and_adds_key(spark):
    dup = ("536365", "85123A", "WIDGET", "6", "2011-01-01 08:26:00", "2.55", "17850", "UK")
    df = _bronze(spark, [dup, dup, dup])
    out = ST.dedupe(ST.filter_valid(ST.clean_and_type(df)))
    rows = out.collect()
    assert len(rows) == 1
    assert len(rows[0]["line_key"]) == 64


def test_dq_catches_injected_violations(spark):
    from dq_rules import SILVER_RULES
    df = _bronze(spark, [
        ("536365", "85123A", "OK", "6", "2011-01-01 08:26:00", "2.55", "17850", "United Kingdom"),
        ("536370", "85123A", "BADPRICE", "6", "2011-01-01 08:26:00", "-1", "17850", "United Kingdom"),
    ])
    silver = ST.clean_and_type(df)  # NOT filtered, so DQ sees the bad row
    silver = ST.dedupe(silver)
    report = DQ.run_expectations(silver, SILVER_RULES)
    by_rule = {r["rule"]: r for r in report["rules"]}
    pk = "min_value::unit_price::accuracy"
    assert by_rule[pk]["rows_failed"] == 1
    assert by_rule[pk]["dimension"] == "accuracy"
    assert pk in report["critical_failures"]
    assert "accuracy" in report["by_dimension"]


def test_dq_registry_covers_all_non_unique_rules():
    from dq_rules import SILVER_RULES
    for r in SILVER_RULES:
        if r["check"] != "unique":
            assert r["check"] in DQ.REGISTRY, f"missing check: {r['check']}"


def test_gold_kpis_are_correct(spark):
    df = _bronze(spark, [
        ("1001", "A", "Apple", "2", "2011-01-01 10:00:00", "10.0", "1", "UK"),
        ("1002", "B", "Banana", "1", "2011-01-02 10:00:00", "5.0", "1", "UK"),
        ("1003", "A", "Apple", "3", "2011-01-03 10:00:00", "10.0", "2", "France"),
    ])
    silver = ST.transform(df)
    fact = GM.build_fact_sales(silver)

    cltv = {r["customer_id"]: r for r in GM.build_kpi_cltv(fact).collect()}
    assert abs(cltv[1]["lifetime_value"] - 25.0) < 1e-6
    assert cltv[1]["orders"] == 2
    assert abs(cltv[2]["lifetime_value"] - 30.0) < 1e-6

    overview = GM.build_kpi_overview(fact).first().asDict()
    assert overview["total_orders"] == 3
    assert overview["unique_customers"] == 2
    assert abs(overview["revenue_per_customer"] - 27.5) < 1e-6  # 55 / 2
    assert abs(overview["total_revenue"] - 55.0) < 1e-6


def test_nl_to_sql_is_select_only(monkeypatch):
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)  # force rule-based backend
    assert NL.question_to_sql("top products").lower().startswith("select")
    assert NL.question_to_sql("revenue by country").lower().startswith("select")
    assert NL.question_to_sql("best customers").lower().startswith("select")


def test_nl_to_sql_guard_rejects_non_select(monkeypatch):
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setattr(NL, "_rule_based", lambda q: "DROP TABLE fact_sales")
    with pytest.raises(ValueError):
        NL.question_to_sql("delete everything")


# --- incremental CDF gold (gold_incremental) -------------------------------

def _cdf(spark, rows, change="insert"):
    import gold_incremental  # noqa: F401  (ensure importable)
    cols = ["invoice_no", "stock_code", "description", "customer_id",
            "customer_name", "country", "line_amount", "quantity"]
    from pyspark.sql import functions as F
    return (spark.createDataFrame(rows, cols)
            .withColumn("invoice_ts", F.to_timestamp(F.lit("2011-01-01 10:00:00")))
            .withColumn("_change_type", F.lit(change)))


def test_cdf_signs(spark):
    import gold_incremental as GI
    pre = GI.signed_changes(_cdf(spark, [("A", "P1", "W", 1, "X", "IN", 10.0, 1)], "update_preimage")).first()
    post = GI.signed_changes(_cdf(spark, [("A", "P1", "W", 1, "X", "IN", 10.0, 1)], "insert")).first()
    assert pre["_sgn"] == -1 and pre["amt"] == -10.0
    assert post["_sgn"] == 1 and post["amt"] == 10.0


def test_incremental_invoice_deltas_equal_full_recompute(spark):
    from pyspark.sql import functions as F

    import gold_incremental as GI
    rows = [("A", "P1", "Widget", 1, "Aarav", "India", 10.0, 1),
            ("A", "P2", "Gadget", 1, "Aarav", "India", 5.0, 1),
            ("B", "P1", "Widget", 2, "Diya", "France", 20.0, 2),
            ("C", "P3", "Gizmo", 3, "Vivaan", "India", 7.5, 1)]
    full = {r["invoice_no"]: r["amount"]
            for r in GI.invoice_deltas(GI.signed_changes(_cdf(spark, rows))).collect()}
    # apply as two separate CDF batches, then additively combine (== Delta MERGE add)
    d1 = GI.invoice_deltas(GI.signed_changes(_cdf(spark, rows[:2])))
    d2 = GI.invoice_deltas(GI.signed_changes(_cdf(spark, rows[2:])))
    inc = {r["invoice_no"]: r["amount"]
           for r in d1.unionByName(d2).groupBy("invoice_no")
           .agg(F.round(F.sum("amount"), 2).alias("amount")).collect()}
    assert inc == full
    assert full["A"] == 15.0 and full["B"] == 40.0  # B: 20*2


# --- NL-to-SQL grounding + validation (metadata + nl_to_sql) ----------------

def test_catalog_known_tables():
    import metadata
    assert metadata.known_tables() == {"kpi_overview", "kpi_country",
                                       "kpi_top_products", "kpi_customers"}
    assert "revenue" in metadata.columns_of("kpi_country")
    assert "kpi_country" in metadata.schema_prompt()


def test_validate_rejects_unknown_table(monkeypatch):
    monkeypatch.delenv("NEO4J_URI", raising=False)
    import metadata
    import nl_to_sql as NL
    cat = metadata.CATALOG
    import pytest as _pytest
    with _pytest.raises(ValueError):
        NL.validate_sql("SELECT * FROM kpi_sales_fact", cat)          # hallucinated
    with _pytest.raises(ValueError):
        NL.validate_sql("DROP TABLE kpi_country", cat)                # not a SELECT
    # valid query passes and tables are extracted
    assert NL.validate_sql("SELECT country FROM kpi_country", cat)
    assert NL.referenced_tables("select x from kpi_country c join kpi_customers k on 1=1") == {
        "kpi_country", "kpi_customers"}


def test_rule_based_queries_are_all_valid(monkeypatch):
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("NEO4J_URI", raising=False)
    import nl_to_sql as NL
    for q in ["revenue by country", "top products", "best customers",
              "repeat rate", "average order value", "total revenue"]:
        sql = NL.question_to_sql(q)            # validates internally
        assert sql.lower().startswith("select")


# --- data-quality control tables: cumulative additive MERGE (dq_control) -----

def _dq_report(row_count, price_failed, country_failed):
    """A minimal DQ report in the shape dq_checks.run_expectations returns."""
    return {"row_count": row_count, "by_dimension": {}, "critical_failures": [],
            "rules": [
                {"rule": "min_value::unit_price", "check": "min_value",
                 "column": "unit_price", "dimension": "accuracy", "critical": True,
                 "total": row_count, "rows_passed": row_count - price_failed,
                 "rows_failed": price_failed},
                {"rule": "in_set::country", "check": "in_set", "column": "country",
                 "dimension": "validity", "critical": False, "total": row_count,
                 "rows_passed": row_count - country_failed, "rows_failed": country_failed}]}


def test_dq_control_merge_accumulates_across_batches(tmp_path, monkeypatch):
    """Two batches MERGE into the control table; matched rules accumulate and the
    pass rate is recomputed from the running totals (not averaged per batch)."""
    import importlib

    monkeypatch.setenv("RETAIL_LAKEHOUSE_HOME", str(tmp_path))
    import config
    importlib.reload(config)
    import dq_control
    importlib.reload(dq_control)

    spark = get_spark("test-dq-control")
    dq_control.record_batch(spark, 1, _dq_report(100, 1, 5))
    dq_control.record_batch(spark, 2, _dq_report(100, 1, 5))

    from delta.tables import DeltaTable
    control = {r["rule"]: r.asDict() for r in
               DeltaTable.forPath(spark, config.p(config.DQ_CONTROL_DIR)).toDF().collect()}
    price = control["min_value::unit_price"]
    assert price["batches"] == 2 and price["total"] == 200
    assert price["rows_failed"] == 2 and price["passed_percent"] == 99.0   # 198/200
    country = control["in_set::country"]
    assert country["rows_failed"] == 10 and country["passed_percent"] == 95.0  # 190/200

    runs = DeltaTable.forPath(spark, config.p(config.DQ_RUNS_DIR)).toDF()
    assert runs.count() == 4                       # 2 rules x 2 batches (append log)
    assert runs.select("batch_id").distinct().count() == 2
