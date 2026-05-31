"""Rules-driven, distributed data-quality engine.

Design mirrors a production control-table DQ framework:
  - rules are declarative data (see dq_rules.py)
  - a REGISTRY maps each check name to a predicate builder that returns a
    boolean Column which is TRUE when a row PASSES
  - row-level rules are evaluated in a SINGLE aggregation pass (no per-rule
    full scans); uniqueness is group-level so it gets its own pass
  - every result carries a quality DIMENSION and a pass %, keyed as
    "check::column::dimension" (same shape as the production rule keys)
  - critical-rule violations can gate the pipeline

Checks run natively on Spark in a single pass; no external DQ dependency.
"""
from __future__ import annotations

import json
import time
from collections.abc import Callable
from datetime import datetime, timezone

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from config import DQ_DIR, SILVER_DIR, ensure_dirs, p
from dq_rules import SILVER_RULES
from spark_session import get_spark

# ----------------------- predicate builders (registry) -----------------------
# Each returns a boolean Column: TRUE = row passes the rule.

def _not_null(colname: str, params: dict) -> Column:
    return F.col(colname).isNotNull()

def _min_value(colname: str, params: dict) -> Column:
    return F.col(colname) >= params["min"]

def _max_value(colname: str, params: dict) -> Column:
    return F.col(colname) <= params["max"]

def _between(colname: str, params: dict) -> Column:
    return F.col(colname).between(params["min"], params["max"])

def _in_set(colname: str, params: dict) -> Column:
    return F.col(colname).isin(params["values"])

def _regex(colname: str, params: dict) -> Column:
    return F.col(colname).rlike(params["pattern"])

def _length(colname: str, params: dict) -> Column:
    return F.length(F.col(colname)) == params["length"]


REGISTRY: dict[str, Callable[[str, dict], Column]] = {
    "not_null": _not_null,
    "min_value": _min_value,
    "max_value": _max_value,
    "between": _between,
    "in_set": _in_set,
    "regex": _regex,
    "length": _length,
    # "unique" is handled separately (group-level), see _eval_unique
}


def rule_key(rule: dict) -> str:
    return f"{rule['check']}::{rule['column']}::{rule['dimension']}"


def build_predicate(rule: dict) -> Column:
    fn = REGISTRY[rule["check"]]
    return fn(rule["column"], rule.get("params", {}))


# ------------------------------- evaluation ----------------------------------

def _result(rule, total, failed) -> dict:
    passed = total - failed
    return {
        "rule": rule_key(rule),
        "column": rule["column"],
        "check": rule["check"],
        "dimension": rule["dimension"],
        "critical": rule.get("critical", False),
        "total": total,
        "rows_passed": passed,
        "rows_failed": failed,
        "passed_percent": round(100.0 * passed / total, 2) if total else 100.0,
    }


def _eval_unique(df: DataFrame, rule: dict, total: int) -> dict:
    col = rule["column"]
    dup_rows = (df.groupBy(col).count().filter(F.col("count") > 1)
                  .agg(F.coalesce(F.sum("count"), F.lit(0)).alias("n")).first()["n"])
    return _result(rule, total, int(dup_rows or 0))


def run_expectations(df: DataFrame, rules: list[dict]) -> dict:
    total = df.count()
    row_rules = [r for r in rules if r["check"] != "unique"]
    unique_rules = [r for r in rules if r["check"] == "unique"]

    results: list[dict] = []
    if total and row_rules:
        # one aggregation pass: count failures per rule
        agg = df.agg(*[
            F.sum(F.when(build_predicate(r), F.lit(0)).otherwise(F.lit(1))).alias(rule_key(r))
            for r in row_rules
        ]).first().asDict()
        results += [_result(r, total, int(agg.get(rule_key(r)) or 0)) for r in row_rules]
    else:
        results += [_result(r, total, 0) for r in row_rules]

    results += [_eval_unique(df, r, total) for r in unique_rules]

    by_dim: dict[str, dict] = {}
    for r in results:
        d = by_dim.setdefault(r["dimension"], {"rows_failed": 0, "rules": 0})
        d["rows_failed"] += r["rows_failed"]
        d["rules"] += 1

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "row_count": total,
        "rules": results,
        "by_dimension": by_dim,
        "critical_failures": [r["rule"] for r in results if r["critical"] and r["rows_failed"] > 0],
    }


def persist_report(report: dict, name: str = "silver") -> None:
    ensure_dirs()
    out = DQ_DIR / f"{name}_dq_{int(time.time())}.json"
    out.write_text(json.dumps(report, indent=2))
    print(f"\nData-quality report ({report['row_count']:,} rows):")
    print(f"  {'check::column':38} {'dimension':13} {'pass%':>7} {'failed':>8} crit")
    for r in sorted(report["rules"], key=lambda x: x["dimension"]):
        flag = "  *" if r["critical"] else ""
        key = f"{r['check']}::{r['column']}"
        print(f"  {key:38} {r['dimension']:13} {r['passed_percent']:>7} {r['rows_failed']:>8}{flag}")
    if report["critical_failures"]:
        print(f"  CRITICAL FAILURES: {report['critical_failures']}")
    print(f"  report -> {out}")


def run(fail_on_critical: bool = False) -> dict:
    spark = get_spark("dq-checks")
    silver = spark.read.format("delta").load(p(SILVER_DIR))
    report = run_expectations(silver, SILVER_RULES)
    persist_report(report, "silver")
    spark.stop()
    if fail_on_critical and report["critical_failures"]:
        raise SystemExit(f"DQ gate failed: {report['critical_failures']}")
    return report


if __name__ == "__main__":
    import sys
    run(fail_on_critical="--strict" in sys.argv)
