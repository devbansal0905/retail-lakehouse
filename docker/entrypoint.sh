#!/usr/bin/env bash
set -e
export RETAIL_LAKEHOUSE_HOME="${RETAIL_LAKEHOUSE_HOME:-/data}"
export PYSPARK_PYTHON=python3

echo "=================================================================="
echo " retail-lakehouse :: REAL-TIME stack"
echo "  producer (continuous) -> Spark streaming (MERGE+CDF) -> SSE web"
echo "  >>> open  http://localhost:8000  <<<"
echo "=================================================================="


# Seed the Neo4j metadata knowledge graph (best-effort; app degrades gracefully)
echo "loading metadata knowledge graph into Neo4j..."
python3 -c "import sys; sys.path.insert(0,'src'); import knowledge_graph as kg; kg.load_catalog(); print('metadata graph loaded')" || echo "neo4j load skipped (graph optional)"

# 1) continuous event producer
python3 src/producer.py --mode stream --interval "${PRODUCE_INTERVAL:-2}" \
        --per-tick "${PER_TICK:-120}" &

# 2) Spark Structured Streaming pipeline (bronze->silver MERGE/CDF->gold->publish)
python3 src/stream_pipeline.py &

# 3) FastAPI SSE dashboard (foreground)
exec uvicorn realtime_app:app --host 0.0.0.0 --port 8000 --app-dir src
