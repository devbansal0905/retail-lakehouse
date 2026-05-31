# Run the real-time stack with Docker

You only need **Docker Desktop** running. From inside `retail-lakehouse/`:

```bash
docker compose up --build
```

This starts three processes in the container:
1. a **continuous producer** dripping synthetic sales events into the landing zone,
2. a **Spark Structured Streaming** job that, per micro-batch, appends to bronze,
   **MERGEs** into silver (Change Data Feed enabled), recomputes gold KPIs, and
   publishes a live snapshot **only when the data changes**,
3. a **FastAPI SSE server** that pushes those snapshots to the browser.

Open the live dashboard:

**http://localhost:8000**

You'll see KPI cards, a revenue-by-country chart, and top products updating in
real time as events flow - plus an **Ask the data** box (NL-to-SQL).

Stop with `Ctrl+C`, then `docker compose down`.

## Knobs (env in docker-compose.yml or your shell / .env)
- `PRODUCE_INTERVAL` - seconds between event ticks (default 2)
- `PER_TICK` - events per tick (default 120)
- `GEMINI_API_KEY` - enables the Gemini NL-to-SQL backend (rule-based fallback otherwise)
- `GEMINI_MODEL` - default `gemini-3.1-flash-lite`
- `DISPLAY_TZ_OFFSET_MINUTES` - display timezone offset in minutes (default 330 = IST; data is stored in UTC)

## Endpoints
- `GET /` - live dashboard
- `GET /chat` - NL-to-SQL chatbot (per-session history)
- `GET /quality` - live data-quality dashboard (per-batch checks)
- `GET /stream` - Server-Sent Events stream (pushes on each new gold version)
- `GET /api/kpis` - current KPI snapshot (JSON)
- `GET /ask?q=...` - natural-language question -> SQL -> rows

## Tests
```bash
docker compose run --rm retail-lakehouse pytest -q
```

## Resetting the data (after schema changes)
The lakehouse persists in a Docker volume. If you change the event schema (e.g.
add a column) the old Delta tables can conflict. Start clean with:
```bash
docker compose down -v   # removes the lakehouse-data volume
docker compose up --build
```

## Metadata knowledge graph (Neo4j)
`docker compose up` also starts **Neo4j**; on boot the app seeds it with the
query catalog (tables/columns/relationships). The NL-to-SQL agent reads this
catalog to ground the model and to validate generated SQL, so it can't query
tables that don't exist. Browse the graph at http://localhost:7474
(user `neo4j`, password `password123`). If Neo4j is unavailable the app falls
back to the in-repo catalog (`src/metadata.py`).

## Login
The dashboard is gated by a username/password login. Users are stored in Neo4j
(salted PBKDF2 hashes; an in-memory store is used if Neo4j is unavailable). A
default user is seeded on startup:
- username `admin`, password `admin123` (override with `APP_USER` / `APP_PASSWORD`).

After signing in, the **Ask the data** panel keeps a per-session conversation
history (every question + generated SQL + result is shown for the current
session). Press **Enter** in the box to run a query. "Sign out" ends the session.
