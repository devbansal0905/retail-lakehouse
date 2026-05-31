"""Declarative data-quality rule config.

Rules are *data*, not code: each rule names a column, a check, optional params,
a quality dimension, and whether it's critical (gates the build). This is the
same metadata-driven pattern used in production frameworks where rules live in
a control table and are applied generically -- here they live in one list so
the repo stays self-contained, but the engine (dq_checks.py) treats them
identically to table-driven rules.

Quality dimensions follow standard DAMA categories:
  completeness | validity | uniqueness | accuracy | consistency
"""
from __future__ import annotations

# Allowed country set for the validity check (synthetic + UCI overlap).
KNOWN_COUNTRIES = [
    "United Kingdom", "Germany", "France", "EIRE", "Spain",
    "Netherlands", "Australia", "Belgium", "Switzerland", "Portugal",
]

# Each rule: column, check (must exist in dq_checks.REGISTRY), params,
# dimension, critical.
SILVER_RULES = [
    {"column": "invoice_no", "check": "not_null", "dimension": "completeness", "critical": True},
    {"column": "stock_code", "check": "not_null", "dimension": "completeness", "critical": True},
    {"column": "invoice_ts", "check": "not_null", "dimension": "completeness", "critical": False},
    {"column": "unit_price", "check": "min_value", "params": {"min": 0.01}, "dimension": "accuracy", "critical": True},
    {"column": "quantity",  "check": "between", "params": {"min": -10000, "max": 10000}, "dimension": "accuracy", "critical": False},
    {"column": "customer_id", "check": "not_null", "dimension": "completeness", "critical": False},
    {"column": "country", "check": "in_set", "params": {"values": KNOWN_COUNTRIES}, "dimension": "validity", "critical": False},
    {"column": "line_key", "check": "length", "params": {"length": 64}, "dimension": "validity", "critical": False},
    {"column": "line_key", "check": "unique", "dimension": "uniqueness", "critical": False},
]
