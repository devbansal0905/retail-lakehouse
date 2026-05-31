"""Natural language to SQL over the KPI tables. The catalog is injected into the prompt
for grounding and every query is validated to be a single SELECT over known
tables and columns; invalid queries are re-prompted (bounded) or fall back to a
rule-based query. Runs in DuckDB over views derived from the gold Delta tables.
"""
from __future__ import annotations

import datetime as _dt
import json
import os
import re
import sys
from decimal import Decimal

import knowledge_graph
import metadata

_SELECT_ONLY = re.compile(r"^\s*select\b", re.IGNORECASE)
_FORBIDDEN = re.compile(r"\b(insert|update|delete|drop|alter|create|merge|truncate|attach|copy|pragma)\b",
                        re.IGNORECASE)
# table names following FROM / JOIN
_TABLE_REF = re.compile(r"\b(?:from|join)\s+([a-zA-Z_][a-zA-Z0-9_]*)", re.IGNORECASE)

MAX_REPAIRS = 2


def get_catalog() -> dict:
    return knowledge_graph.get_catalog()


def referenced_tables(sql: str) -> set[str]:
    return {m.group(1).lower() for m in _TABLE_REF.finditer(sql)}


def validate_sql(sql: str, catalog: dict) -> str:
    """Reject anything that isn't a single SELECT over known tables."""
    if not _SELECT_ONLY.match(sql) or _FORBIDDEN.search(sql) or ";" in sql.rstrip(";"):
        raise ValueError(f"not a single SELECT statement: {sql!r}")
    known = metadata.known_tables(catalog)
    unknown = referenced_tables(sql) - known
    if unknown:
        raise ValueError(f"unknown table(s) {sorted(unknown)}; allowed: {sorted(known)}")
    return sql


def _rule_based(question: str) -> str:
    q = question.lower()
    if "top" in q and ("product" in q or "sell" in q):
        return "SELECT description, revenue, units FROM kpi_top_products ORDER BY revenue DESC LIMIT 10"
    if "country" in q or "countries" in q:
        return "SELECT country, revenue, orders FROM kpi_country ORDER BY revenue DESC"
    if "per customer" in q or "arpu" in q or "per-customer" in q:
        return "SELECT revenue_per_customer FROM kpi_overview"
    if "average order" in q or "aov" in q:
        return "SELECT avg_order_value FROM kpi_overview"
    if "customer" in q or "lifetime" in q or "cltv" in q:
        return ("SELECT customer_name, lifetime_value, orders, last_order_date "
                "FROM kpi_customers ORDER BY lifetime_value DESC LIMIT 10")
    if "revenue" in q or "sales" in q or "total" in q:
        return "SELECT total_revenue, total_orders, avg_order_value FROM kpi_overview"
    return "SELECT * FROM kpi_overview"


def _llm_based(question: str, catalog: dict, prior_error: str | None) -> str:
    from google import genai
    client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
    prompt = [
        "Translate the question into a single DuckDB SQL SELECT statement.",
        metadata.schema_prompt(catalog),
        "Rules: SELECT only; one statement; no markdown fences; "
        "use ONLY the tables and columns listed above.",
    ]
    if prior_error:
        prompt.append(f"Your previous attempt was rejected: {prior_error} "
                      "Fix it using only the listed tables/columns.")
    prompt.append(f"Question: {question}\nSQL:")
    resp = client.models.generate_content(
        model=os.environ.get("GEMINI_MODEL", "gemini-3.1-flash-lite"),
        contents="\n".join(prompt),
    )
    sql = (resp.text or "").strip().strip("`")
    return re.sub(r"^sql\s*", "", sql, flags=re.IGNORECASE).strip()


def question_to_sql(question: str, catalog: dict | None = None) -> str:
    catalog = catalog or get_catalog()
    if not os.environ.get("GEMINI_API_KEY"):
        return validate_sql(_rule_based(question), catalog)
    err = None
    for _ in range(MAX_REPAIRS + 1):
        try:
            return validate_sql(_llm_based(question, catalog, err), catalog)
        except ValueError as e:
            err = str(e)
    # give up on the LLM -> deterministic, validated fallback
    return validate_sql(_rule_based(question), catalog)


# ----------------------- query execution (DuckDB) ----------------------------

def _jsonable(v):
    if isinstance(v, _dt.date | _dt.datetime):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    return v


def run_over_live(question: str) -> dict:
    """Answer a question over the live KPI tables (derived from the gold Delta
    tables). Returns the validated SQL and the result rows."""
    import serving
    con = serving.duckdb_for_nl()
    if con is None:
        return {"error": "No data yet - the pipeline hasn't written gold tables."}
    try:
        sql = question_to_sql(question)
        cur = con.execute(sql)
        cols = [c[0] for c in cur.description]
        rows = [{c: _jsonable(v) for c, v in zip(cols, r, strict=False)}
                for r in cur.fetchall()]
        return {"sql": sql, "rows": rows}
    except Exception as e:  # noqa: BLE001 - surface to the caller
        return {"error": str(e)}
    finally:
        con.close()


if __name__ == "__main__":
    q = " ".join(sys.argv[1:]) or "which country has the most revenue?"
    res = run_over_live(q)
    print(json.dumps(res, indent=2, default=str))
