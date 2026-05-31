# Publishing checklist

## 1. Replace the badge placeholder
In `README.md`, change `YOUR_GITHUB_USERNAME` to your GitHub username (2 places in the CI badge URL).

## 2. Add screenshots
Run it once (`docker compose up --build`), then save:
- the dashboard (http://localhost:8501) → `docs/screenshots/dashboard.png`
- the terminal output of the run → `docs/screenshots/pipeline_run.png`

The README already references both.

## 3. Initialise git with clean, logical commits
(Reads like real engineering instead of one dump.)

```bash
cd retail-lakehouse
git init -b main

git add src/config.py src/spark_session.py src/generate_data.py src/producer.py requirements.txt .gitignore
git commit -m "scaffold: config, spark session, data generator + event producer"

git add src/bronze_ingest.py src/silver_transform.py
git commit -m "medallion: streaming bronze ingest + silver clean/dedupe/CDC-merge"

git add src/gold_model.py
git commit -m "gold: star schema + KPI tables (CLTV, AOV, repeat-rate, by-country)"

git add src/dq_rules.py src/dq_checks.py
git commit -m "data quality: rules-driven engine with quality dimensions + critical gating"

git add src/nl_to_sql.py src/run_pipeline.py dashboards/ notebooks/ scripts/
git commit -m "serving: NL-to-SQL layer, orchestrator, Streamlit dashboard"

git add tests/ .github/ ruff.toml
git commit -m "tests + CI: pytest suite, ruff lint, GitHub Actions"

git add Dockerfile docker-compose.yml docker/ .dockerignore RUN_WITH_DOCKER.md
git commit -m "docker: one-command run + dashboard"

git add README.md architecture.png docs/ data/ LICENSE
git commit -m "docs: README, architecture diagram, license"
```

## 4. Create the repo and push
```bash
# create an empty repo named retail-lakehouse on github.com first, then:
git remote add origin https://github.com/YOUR_GITHUB_USERNAME/retail-lakehouse.git
git push -u origin main
```

## 5. On GitHub (repo settings / home page)
- **Description:** `Real-time retail lakehouse on Databricks-style patterns — streaming ingestion, Delta medallion, rules-driven data quality, star-schema KPIs, and NL-to-SQL. PySpark + Delta Lake.`
- **Topics:** `pyspark` `delta-lake` `data-engineering` `databricks` `spark` `etl` `lakehouse` `data-quality` `streaming`
- **Pin** the repo to your profile (Profile → Customize your pins).
- Confirm the green CI check appears after the first push.

## 0. Local-only files (don't commit)
`PUBLISH.md` and `docs/screenshots/README.md` are personal helpers. Delete them (or leave them out of your commits) before pushing the public repo.
