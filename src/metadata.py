"""Metadata catalog: the tables and columns the NL-to-SQL agent is allowed to query.
Loaded into Neo4j, injected into the prompt for grounding, and used to validate
generated SQL.
"""
from __future__ import annotations

# table -> {description, grain, columns: {name: (type, description)}}
CATALOG: dict[str, dict] = {
    "kpi_overview": {
        "description": "Single-row headline KPIs for the whole business.",
        "grain": "one row",
        "columns": {
            "total_revenue": ("double", "net revenue across non-cancelled invoices"),
            "total_orders": ("bigint", "count of distinct non-cancelled invoices"),
            "avg_order_value": ("double", "total_revenue / total_orders"),
            "unique_customers": ("bigint", "count of distinct customers"),
            "revenue_per_customer": ("double", "average net revenue per customer"),
        },
    },
    "kpi_country": {
        "description": "Revenue and order counts aggregated by country.",
        "grain": "one row per country",
        "columns": {
            "country": ("string", "customer country"),
            "revenue": ("double", "net revenue for the country"),
            "orders": ("bigint", "distinct invoices for the country"),
        },
    },
    "kpi_top_products": {
        "description": "Best-selling products by revenue.",
        "grain": "one row per product",
        "columns": {
            "stock_code": ("string", "product code"),
            "description": ("string", "product description"),
            "revenue": ("double", "net revenue for the product"),
            "units": ("bigint", "total units sold"),
        },
    },
    "kpi_customers": {
        "description": "Top customers by lifetime value.",
        "grain": "one row per customer",
        "columns": {
            "customer_id": ("bigint", "customer identifier"),
            "customer_name": ("string", "customer full name"),
            "lifetime_value": ("double", "net revenue from the customer"),
            "orders": ("bigint", "distinct invoices for the customer"),
            "last_order_date": ("date", "most recent order date"),
        },
    },
}

# Conceptual relationships (modelled in the knowledge graph).
RELATIONSHIPS = [
    ("kpi_customers", "customer_id", "DESCRIBES", "customer"),
    ("kpi_country", "country", "DESCRIBES", "country"),
    ("kpi_top_products", "stock_code", "DESCRIBES", "product"),
]


def known_tables(catalog: dict | None = None) -> set[str]:
    return set((catalog or CATALOG).keys())


def columns_of(table: str, catalog: dict | None = None) -> set[str]:
    cat = catalog or CATALOG
    return set(cat.get(table, {}).get("columns", {}).keys())


def schema_prompt(catalog: dict | None = None) -> str:
    """Human/LLM-readable schema description used to ground the model."""
    cat = catalog or CATALOG
    lines = ["Tables (DuckDB SQL). Use ONLY these tables and columns:"]
    for t, meta in cat.items():
        cols = ", ".join(f"{c} {typ}" for c, (typ, _) in meta["columns"].items())
        lines.append(f"  {t}({cols})  -- {meta['description']}")
    return "\n".join(lines)
