# Dagster OSS Template

A **local** Dagster OSS demo showcasing multi-code-location architecture, dbt + DuckDB integration, daily + monthly partitions, file-arrival + cross-partition sensors, auto-materialization, and cross-location ML — on a real ELT pipeline adapted from [sant3e/dbt_snowflake_dwh_project](https://github.com/sant3e/dbt_snowflake_dwh_project).

> **Local only.** No auth, no secrets manager, no Postgres, no Snowflake, no Dagster+. Do not deploy any part of this.

---

## What's inside

```
dagster_oss_template/
├── docker-compose.yml          4 services on one network
├── docker/                     Dockerfiles for webserver/daemon and code locations
├── dagster_home/               dagster.yaml (SQLite storage, QueuedRunCoordinator) + workspace.yaml
├── dbt_project/                dbt + DuckDB project, ported from sant3e
│   ├── models/
│   │   ├── landing/            raw_* thin pass-throughs (read Dagster-landed tables)
│   │   ├── staging/            stg_* cleaned tables (Snowflake → DuckDB ported SQL)
│   │   ├── mart/               dim_customers, dim_products, dim_products_history, fct_sales
│   │   ├── reporting/          rpt_sales_summary_by_customer, rpt_sales_performance_by_product
│   │   ├── ml_features/        customer_rfm (owned by ml_team)
│   │   └── groups.yml          elt_team, ml_team + owners
│   └── seeds/                  static reference CSVs (CUST_AZ12, PX_CAT_G1V2)
├── code_locations/
│   ├── elt_pipelines/          gRPC server on :4000, owns the elt layers
│   └── ml_pipelines/           gRPC server on :4000, owns the ml layer
├── data/landing/               where daily + monthly snapshot CSVs land (day 1 ships here)
├── future_landing_data/        pre-generated day-2 + next-month CSVs for demo purposes
├── warehouse/                  DuckDB file + joblib model artifacts (created on first run)
└── scripts/                    manifest regen, wipe, future-data generator
```

---

## Quickstart

Prereqs: Docker Desktop + `make`.

```bash
git clone https://github.com/sant3e/dagster-oss-dbt-duckdb-demo.git
cd dagster-oss-dbt-duckdb-demo
make build        # build webserver/daemon + both code-location images
make up           # start the stack (webserver + daemon + 2 gRPC code locations)
open http://localhost:3000
```

Confirm on the **Deployment** tab that both `elt_pipelines` and `ml_pipelines` are green.

> **Before you delete the folder or move on**, bring the stack down first:
>
> ```bash
> make down           # or: docker compose down
> ```
>
> Docker tracks the containers independently of the source tree, so a bare `rm -rf` on the folder leaves them running (and port 3000 occupied). If you want to reclaim disk too, remove the images with `docker rmi dagster_oss_template/dagster:latest dagster_oss_template/elt_pipelines:latest dagster_oss_template/ml_pipelines:latest` or run `docker system prune`.

---

## Running the demo

### Step 1 — Load static reference data
`dbt seed` loads two reference tables that don't need daily or monthly snapshots (ERP customer birthdays + product category taxonomy).

In the Dagster UI: **Jobs → `dbt_seed_job` → Materialize**. Or from the shell:

```bash
make shell-elt
dagster job execute -m elt_pipelines.definitions -j dbt_seed_job
```

This populates `seeds.CUST_AZ12` (18,484 rows) and `seeds.PX_CAT_G1V2` (37 rows) in DuckDB.

### Step 2 — Materialize day-1 landing (already shipped as CSVs in `data/landing/`)

Turn on **`landing_file_sensor`** in the UI (Automation → Sensors → toggle on). Within 30 seconds it detects the four CSVs already sitting in `data/landing/` and launches:

- `raw_sales_details` partition `2026-04-01` (60,398 rows)
- `raw_cust_info` partition `2026-04-01` (18,494 rows)
- `raw_loc_a101` partition `2026-04-01` (18,484 rows)
- `raw_prd_info_monthly` partition `2026-04-01` (397 rows)

These populate `raw.sales_details`, `raw.cust_info`, `raw.loc_a101`, `raw.prd_info` in DuckDB.

### Step 3 — Watch the bridge sensor fire the ELT

Turn on **`daily_monthly_bridge_sensor`**. It notices that all three daily upstreams are ready for `2026-04-01` AND the monthly upstream has been materialized for month-of(2026-04-01), so it fires the **`dbt_elt_job`** for that day. The dbt pipeline builds landing → staging → mart → reporting in DuckDB.

### Step 4 — Drop event-day files to see the daily cadence in action

The `future_landing_data/` folder is **empty by default** (it only ships with a README). You generate the event-style CSVs yourself using the Faker-based generator — this keeps the repo lean and lets you pick whatever dates you want for the demo.

First-time prereq (one-off):
```bash
pip install faker
# on macOS with PEP-668 protections:
pip install --break-system-packages faker
```

Generate a few days of event snapshots:
```bash
# From the project root — covers April 2–5:
python3 scripts/generate_future_landing_data.py --start 2026-04-02 --days 4
```

That produces 12 files in `future_landing_data/` — 4 daily customer snapshots, 4 daily location snapshots, 4 daily sales snapshots (realistic Faker names, positive sales values, monotonic IDs).

Then copy them into `data/landing/` so the sensor picks them up:
```bash
cp future_landing_data/*.csv data/landing/
```

You'll see (within 30 s):

1. `landing_file_sensor` fires new landing runs: one daily run per `YYYY_MM_DD` triple of files found, one monthly run per `YYYY_MM` file found.
2. `daily_monthly_bridge_sensor` fires `dbt_elt_job` for every day whose month has a materialized monthly partition. If you only drop daily files for May but no May monthly file, the bridge sensor **waits** — exactly the cadence-mismatch pattern it exists to demonstrate.

#### Other generator invocations

```bash
# Single day:
python3 scripts/generate_future_landing_data.py --start 2026-04-06

# Date range (inclusive both ends):
python3 scripts/generate_future_landing_data.py --start 2026-04-06 --end 2026-04-12

# N days from a start, plus a monthly file for each month touched:
python3 scripts/generate_future_landing_data.py --start 2026-06-01 --days 7 --monthly
```

Files land in `future_landing_data/`. The generator is idempotent across invocations — customer IDs, product IDs, and order numbers continue monotonically — so you can generate April, then May, then June without collisions. See `future_landing_data/README.md` for every option.

### Step 5 — Run the ML pipeline

Turn on **`customer_rfm_updated_sensor`** in the `ml_pipelines` code location. After the ELT has produced `reporting.rpt_sales_summary_by_customer`, the ml_features dbt model `customer_rfm` materializes, which triggers the ML training job producing `ml_features.customer_segments` (KMeans) and `ml_features.churn_predictions` (LogReg + serialized joblib model at `/warehouse/artifacts/churn_model.joblib`).

---

## Querying the warehouse

The DuckDB file lives at `./warehouse/oss_template.duckdb` on the host. You can query it in several ways.

### Option A — from inside a container

```bash
make shell-elt
python -c "
import duckdb
c = duckdb.connect('/warehouse/oss_template.duckdb')
print(c.execute('SHOW ALL TABLES').fetchdf())
"
```

### Option B — DuckDB CLI on the host

```bash
# Install once: brew install duckdb
duckdb ./warehouse/oss_template.duckdb
```

> DuckDB allows only one writer at a time. Close any containers that might be writing before opening an interactive CLI, or open the file in read-only mode: `duckdb -readonly ./warehouse/oss_template.duckdb`.

### Sample queries

**1. Top 10 customers by total sales (ELT reporting layer):**
```sql
SELECT first_name, last_name, country, total_orders, total_sales
FROM reporting.rpt_sales_summary_by_customer
ORDER BY total_sales DESC
LIMIT 10;
```

**2. Compare row counts between snapshots** (proves day-2 ingestion actually added new orders, not just refreshed existing ones):
```sql
SELECT snapshot_date, COUNT(*) AS rows, COUNT(DISTINCT sls_ord_num) AS unique_orders
FROM raw.sales_details
GROUP BY snapshot_date
ORDER BY snapshot_date;
```

**3. ML output — highest-churn-risk customers joined to their segment:**
```sql
SELECT
    cp.first_name,
    cp.last_name,
    cp.country,
    cs.segment_label,
    ROUND(cp.churn_probability, 3) AS churn_prob,
    cp.recency_days,
    cp.frequency,
    ROUND(cp.monetary, 0) AS monetary
FROM ml_features.churn_predictions cp
JOIN ml_features.customer_segments cs USING (customer_key)
WHERE cp.is_high_risk = TRUE
ORDER BY cp.churn_probability DESC
LIMIT 15;
```

---

## Feature tour (mapped to the UI)

| Feature | Where to find it |
|---|---|
| **Multi-code-location** | Deployment tab — two green locations. Asset graph stitches across them. |
| **dbt + DuckDB** | Assets tab — `landing → staging → mart → reporting → ml_features`. Each dbt asset has asset checks populated from the unique/not_null dbt tests. |
| **dbt groups & access** | `dbt_project/models/groups.yml` + `+group:`/`+access:` in `dbt_project.yml`. Staging is `access: private`, mart/reporting are `public`. |
| **Daily partitions** | `raw_sales_details`, `raw_cust_info`, `raw_loc_a101` — partition grid in the UI. |
| **Monthly partition** | `raw_prd_info_monthly` — a separate partition grid with month-start keys. |
| **Backfills** | Click **Backfill** on any partitioned asset. Runs execute serially thanks to the `duckdb_writer` tag concurrency limit. |
| **File-arrival sensor** | `landing_file_sensor` — one sensor routes both daily AND monthly files to the right job. |
| **Cross-partition sensor (imperative)** | `daily_monthly_bridge_sensor` — bridges daily downstreams to their monthly upstream. |
| **Auto-materialize (declarative)** | `AutomationCondition.eager()` on mart models — `code_locations/elt_pipelines/elt_pipelines/assets/dbt.py`. |
| **Manual dbt-seed job** | `dbt_seed_job` in Jobs. |
| **Cross-location asset sensor** | `customer_rfm_updated_sensor` (ml_pipelines) — listens to a materialization produced by the ml dbt layer whose upstreams are in elt_pipelines. |
| **ML fan-out** | `customer_segments` (KMeans) + `churn_predictions` (LogisticRegression) both consume `customer_rfm`. |

---

## Sensor vs AutomationCondition — why both?

They target *different* asset hops in the graph so they don't fight each other:

- **`daily_monthly_bridge_sensor`** drives the **ELT dbt job** only when all daily upstreams AND the month-of(D) monthly upstream are ready for the same day D. It's explicit, cursor-based, easy to debug, and expresses a constraint auto-materialize cannot.
- **`AutomationCondition.eager()`** on mart models drives the **staging → mart** hop automatically whenever staging updates. Declarative, less code, but harder to reason about for newcomers.

Use the contrast as a teaching moment: sensors win when you need to express "wait until X and Y are ready for different partition cadences"; AutomationCondition wins for the 80% of "refresh this when upstreams change" cases.

---

## Why these architectural choices

- **SQLite for Dagster metadata** — zero config, file-based, perfect for a local demo. Production would be Postgres.
- **One shared DuckDB file** mounted into both code-location containers. Lets `ml_pipelines` read the elt marts that `elt_pipelines` wrote. DuckDB permits only one writer at a time, so we serialize writes via `QueuedRunCoordinator` + `tag_concurrency_limits` on the `duckdb_writer` key.
- **One dbt project** at repo root. Each code location loads a different selector (elt takes everything except `ml_features`; ml takes only `ml_features`). Groups (`elt_team`, `ml_team`) + `+access:` settings enforce boundaries at the dbt layer.
- **`@dbt_assets` per code location** — not `load_assets_from_dbt_project`. The manifest is produced inside each container at startup with a per-container `--target-path` so the two containers don't race on the same `target/` folder.

---

## Adding a new code location

1. `cp -r code_locations/ml_pipelines code_locations/<new_name>` and rename the package + `[tool.dagster]` fields in its `pyproject.toml`.
2. Add a new `grpc_server` entry to `dagster_home/workspace.yaml`.
3. Add a new service to `docker-compose.yml` (copy the `ml_pipelines` service, change the `CODE_LOCATION` build arg + container name).
4. `make build && make down && make up`.

---

## Troubleshooting

- **"Code location failed to load"** — Check `docker compose logs <location>` for import errors. Most common cause: a stale `manifest.json`. Restart the offending container or rebuild (`make build`).
- **"database is locked"** — Another process holds a DuckDB writer. Check `make ps`; confirm only one writer asset runs at a time (the `duckdb_writer` concurrency key should prevent this). For interactive queries use `duckdb -readonly`.
- **Sensor not firing** — From the UI, confirm the sensor is toggled on (they are all OFF by default for local dev).
- **Monthly partition rejected** — `MonthlyPartitionsDefinition` uses `end_offset=1` so the current month is valid; if you change `start_date`, make sure your file's month is within the supported range.
- **Fresh slate** — `make wipe && make up` clears the DuckDB file + Dagster SQLite storage + dbt target directory (keeps landing files).

---

## Keeping the repo clean for the next person

Running the demo creates files the repo doesn't ship with — a DuckDB warehouse, a joblib ML artifact, Dagster's SQLite storage, any CSVs you dropped into `data/landing/` during the demo, and anything you generated into `future_landing_data/`.

The `.gitignore` is set up so **only the known-good baseline is tracked**:

- `data/landing/` — only the four day-1 files are tracked. Anything else dropped in is ignored.
- `future_landing_data/` — only `README.md` is tracked. Every CSV you generate there is ignored.
- Runtime state (`warehouse/*.duckdb`, `dagster_home/storage/`, `dbt_project/target/`, etc.) is fully ignored.

So **`git status` stays clean even after a full demo run**. Still, if you want to reset to a pristine "just cloned" state — e.g., before handing the repo off — there's a single target:

```bash
make reset-demo
```

This stops the stack, wipes the DuckDB warehouse + Dagster SQLite + dbt artifacts, removes any extra CSVs from `data/landing/`, and clears everything from `future_landing_data/` except its README. Docker images are preserved (rebuild with `make build` if you want those gone too). Afterwards the tree looks exactly like a fresh checkout.

---

## Things this template intentionally does NOT do

- No authentication on the UI (don't expose port 3000 publicly).
- No secrets manager — env vars in `.env.example` are plain-text.
- No Postgres, no Dagster+, no k8s, no branch deployments, no Snowflake.
- No production retry/alerting policies — add your own when adapting.
- No CI — this is a teaching artifact.
