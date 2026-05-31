"""Neo4j metadata knowledge graph for the query catalog. load_catalog seeds it from
metadata.CATALOG and fetch_catalog reads it back; if Neo4j is unset or unreachable,
callers fall back to metadata.CATALOG.
"""
from __future__ import annotations

import os

import metadata


def available() -> bool:
    return bool(os.environ.get("NEO4J_URI"))


def _driver():
    from neo4j import GraphDatabase
    uri = os.environ["NEO4J_URI"]
    user = os.environ.get("NEO4J_USER", "neo4j")
    pwd = os.environ.get("NEO4J_PASSWORD", "password")
    return GraphDatabase.driver(uri, auth=(user, pwd))


def load_catalog(catalog: dict | None = None) -> None:
    """Idempotently (re)build the metadata graph from the catalog."""
    cat = catalog or metadata.CATALOG
    drv = _driver()
    try:
        with drv.session() as s:
            s.run("MATCH (n) WHERE n:Table OR n:Column OR n:Concept DETACH DELETE n")
            for t, meta in cat.items():
                s.run("MERGE (x:Table {name:$n}) SET x.description=$d, x.grain=$g",
                      n=t, d=meta["description"], g=meta["grain"])
                for col, (typ, desc) in meta["columns"].items():
                    s.run(
                        "MATCH (x:Table {name:$t}) "
                        "MERGE (c:Column {name:$c, table:$t}) SET c.type=$ty, c.description=$de "
                        "MERGE (x)-[:HAS_COLUMN]->(c)",
                        t=t, c=col, ty=typ, de=desc)
            for table, col, _rel, concept in metadata.RELATIONSHIPS:
                s.run(
                    "MERGE (k:Concept {name:$concept}) "
                    "WITH k MATCH (x:Table {name:$table}) "
                    "MERGE (x)-[r:DESCRIBES]->(k) SET r.via=$col",
                    concept=concept, table=table, col=col)
    finally:
        drv.close()


def fetch_catalog() -> dict:
    """Read the catalog back out of the graph (source of truth at query time)."""
    drv = _driver()
    cat: dict[str, dict] = {}
    try:
        with drv.session() as s:
            for rec in s.run("MATCH (t:Table) RETURN t.name AS n, t.description AS d, t.grain AS g"):
                cat[rec["n"]] = {"description": rec["d"], "grain": rec["g"], "columns": {}}
            for rec in s.run(
                "MATCH (t:Table)-[:HAS_COLUMN]->(c:Column) "
                "RETURN t.name AS t, c.name AS c, c.type AS ty, c.description AS de"):
                cat[rec["t"]]["columns"][rec["c"]] = (rec["ty"], rec["de"])
    finally:
        drv.close()
    return cat


def get_catalog() -> dict:
    """Catalog from the graph when available, else the in-repo definition."""
    if available():
        try:
            cat = fetch_catalog()
            if cat:
                return cat
        except Exception:
            pass  # graph down / not yet loaded -> fall back
    return metadata.CATALOG


if __name__ == "__main__":
    load_catalog()
    print("loaded metadata graph:", sorted(fetch_catalog().keys()))
