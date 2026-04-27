# Dagster OSS Template

A **local** Dagster OSS demo showcasing multi-code-location architecture, dbt + DuckDB integration, **end-to-end daily partitioning** (every layer of the dbt graph), monthly-source-into-daily-pipeline via `latest-available` lookup, file-arrival + cadence-bridge + cross-location sensors, and a partitioned ML fan-out — on a real ELT pipeline adapted from [sant3e/dbt_snowflake_dwh_project](https://github.com/sant3e/dbt_snowflake_dwh_project).

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
│   │   ├── landing/            raw_* daily-partitioned (filter source by snapshot_dt)
│   │   ├── staging/            stg_* daily-partitioned, cleaned
│   │   ├── mart/               dim_customers, dim_products, dim_products_history, fct_sales — daily-partitioned
│   │   ├── reporting/          rpt_* daily-partitioned
│   │   ├── ml_features/        customer_rfm (owned by ml_team) — daily-partitioned
│   │   └── groups.yml          elt_team, ml_team + owners
│   └── seeds/                  static reference CSVs (CUST_AZ12, PX_CAT_G1V2 — NOT partitioned)
├── code_locations/
│   ├── elt_pipelines/          gRPC server on :4000, owns the elt layers
│   └── ml_pipelines/           gRPC server on :4000, owns the ml layer
├── data/landing/               where daily + monthly snapshot CSVs land (day 1 ships here)
├── future_landing_data/        scratch space for Faker-generated day-N CSVs (empty by default)
├── warehouse/                  DuckDB file + joblib model artifacts (created on first run)
└── scripts/                    reset_demo.sh, wipe.sh, Faker data generator
```

> **dbt packages:** `dbt_utils` is listed in `dbt_project/packages.yml` and installed automatically on container startup (runs `dbt deps` if `dbt_packages/dbt_utils/` is missing). Used for compound uniqueness tests like `(natural_key, snapshot_date)`.

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
- `raw_cust_info` partition `2026-04-01` (18,484 rows)
- `raw_loc_a101` partition `2026-04-01` (18,484 rows)
- `raw_prd_info_monthly` partition `2026-04-01` (397 rows)

These populate `raw.sales_details`, `raw.cust_info`, `raw.loc_a101`, `raw.prd_info` in DuckDB (all with a `snapshot_date` / `snapshot_month` column carrying the partition key).

### Step 3 — Watch the bridge sensor fire the ELT

Turn on **`daily_monthly_bridge_sensor`**. It notices that all three daily upstreams are ready for `2026-04-01` AND the monthly upstream has been materialized for month-of(2026-04-01), so it fires the **`dbt_elt_job`** for that day with `partition_key=2026-04-01`. The dbt pipeline builds landing → staging → mart → reporting in DuckDB — each model filtered to that single partition via `{{ var('snapshot_dt') }}`, and incrementally written with `delete+insert` on `snapshot_date`.

Freshness checks are evaluated by Dagster's built-in `default_automation_condition_sensor` (on by default in 1.12+). If it's off in your deployment, toggle it on from Automation → Sensors so the **Checks** tab of each asset gets freshness updates (PASS / WARN / FAIL) on every tick.

### Step 4 — Drop event-day files to see the daily cadence in action

The `future_landing_data/` folder is **empty by default** (it only ships with a README). You generate the event-style CSVs yourself using the Faker-based generator — this keeps the repo lean and lets you pick whatever dates you want for the demo.

First-time prereq (one-off):
```bash
# Recommended — works regardless of which pip your shell picks up:
python3 -m pip install --break-system-packages faker

# If that's rejected by PEP 668 without the flag, the --break-system-packages
# is required on modern Homebrew/macOS Python installs.
# If you use pyenv/conda/venv, drop the flag.
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

### Step 5 — Turn on the ELT→ML bridge and watch the chain complete itself

Turn on **`elt_to_ml_bridge_sensor`** in the `ml_pipelines` code location: **Automation → Sensors → toggle on**.

That's it. Within 30 s of each ELT partition finishing, the sensor detects a new materialization of `reporting.rpt_sales_summary_by_customer`, reads its `partition_key`, and fires `ml_training_job` for that same partition. You'll see `ml_features/customer_rfm` (dbt) → `ml_features/customer_segments` (KMeans) → `ml_features/churn_predictions` (LogReg) all materialize for that day.

With the sensor **off**, the ml assets stay idle no matter what elt does — that's the layered-sensor invariant working as designed. Turn it off, drop more day-N files into `data/landing/`, watch landing + elt run while ml waits. Turn it on, and ml catches up automatically for every new partition.

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

**2. Per-partition row counts across the pipeline** (proves every layer is daily-partitioned and `delete+insert` replaces only the current partition — not a cumulative append):
```sql
SELECT 'landing.raw_crm_sales_details' AS tbl, snapshot_date, COUNT(*) AS rows FROM landing.raw_crm_sales_details GROUP BY 1,2
UNION ALL
SELECT 'staging.stg_crm_sales_details', snapshot_date, COUNT(*) FROM staging.stg_crm_sales_details GROUP BY 1,2
UNION ALL
SELECT 'mart.fct_sales', snapshot_date, COUNT(*) FROM mart.fct_sales GROUP BY 1,2
UNION ALL
SELECT 'reporting.rpt_sales_summary_by_customer', snapshot_date, COUNT(*) FROM reporting.rpt_sales_summary_by_customer GROUP BY 1,2
ORDER BY tbl, snapshot_date;
```

**3. ML output — highest-churn-risk customers joined to their segment, for a specific partition:**
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
JOIN ml_features.customer_segments cs
  ON cp.customer_key = cs.customer_key
 AND cp.snapshot_date = cs.snapshot_date
WHERE cp.snapshot_date = '2026-04-01'
  AND cp.is_high_risk = TRUE
ORDER BY cp.churn_probability DESC
LIMIT 15;
```

---

## Feature tour (mapped to the UI)

| Feature | Where to find it |
|---|---|
| **Multi-code-location** | Deployment tab — two green locations. Asset graph stitches across them. |
| **dbt + DuckDB** | Assets tab — `landing → staging → mart → reporting → ml_features`. Every dbt model is **daily-partitioned on `snapshot_date`**, incremental (`delete+insert` on `snapshot_date`). Dbt tests (including `dbt_utils.unique_combination_of_columns` for `(natural_key, snapshot_date)`) surface as asset checks. |
| **dbt groups & access** | `dbt_project/models/groups.yml` + `+group:`/`+access:` in `dbt_project.yml`. Staging is `access: private`, mart/reporting are `public`. |
| **Daily partitions** | Every layer — `raw_*`, `landing/*`, `staging/*`, `mart/*`, `reporting/*`, `ml_features/*`. Partition grid on every asset. Dagster passes `--vars '{"snapshot_dt":"YYYY-MM-DD"}'` into dbt; each model filters itself on that var. |
| **Monthly source → daily join** | `raw_crm_prd_info` uses a **latest-available-on-or-before** pattern: for partition D, picks `MAX(snapshot_month) ≤ D`. Bridges a once-a-month source into a daily-running pipeline. |
| **Monthly partition** | `raw_prd_info_monthly` — a separate partition grid with month-start keys. |
| **Backfills** | Click **Backfill** on any partitioned asset. Runs execute serially thanks to the `duckdb_writer` tag concurrency limit. `BackfillPolicy.multi_run()` on the dbt assets means each partition becomes its own run (visible in the runs tab). |
| **File-arrival sensor** | `landing_file_sensor` — one sensor routes both daily AND monthly files to the right job. |
| **Cross-partition bridge sensor** | `daily_monthly_bridge_sensor` — fires `dbt_elt_job` with `partition_key=D` only when all three daily landings for D AND the monthly landing for month-of(D) are ready. |
| **Manual dbt-seed job** | `dbt_seed_job` in Jobs — non-partitioned (static reference data). |
| **Cross-location asset sensor** | `elt_to_ml_bridge_sensor` (ml_pipelines) — listens for new partitions of `reporting.rpt_sales_summary_by_customer` in the elt_pipelines code location and fires `ml_training_job` with the same `partition_key`. |
| **ML fan-out** | `customer_segments` (KMeans) + `churn_predictions` (LogisticRegression) both consume `customer_rfm` within a single partition. |
| **Freshness checks** | Every partitioned asset has a time-partition freshness check (deadline_cron driven). Open any asset → **Checks** tab → you'll see a `freshness_check` row with PASS / WARN / FAIL. Thresholds live in `freshness_checks.py` in each code location. |

---

## Sensor-driven orchestration — one layer at a time

The project is **entirely sensor-driven** now that every layer is partitioned. Three sensors form a cascade, each one bridging a different hop in the graph:

| Hop | Sensor | What it does |
|---|---|---|
| Filesystem → landing assets | `landing_file_sensor` | Detects new `<prefix>_YYYY_MM_DD.csv` / `prd_info_YYYY_MM.csv` files, emits one `RunRequest` per file with the right `partition_key` + job. |
| landing → ELT (elt_pipelines) | `daily_monthly_bridge_sensor` | Waits until all three daily landings for day D AND the monthly landing for month-of(D) are ready, then fires `dbt_elt_job` with `partition_key=D`. Bridges the daily/monthly cadence mismatch. |
| elt → ml (cross-code-location) | `elt_to_ml_bridge_sensor` (ml_pipelines) | Listens for new partitions of `reporting.rpt_sales_summary_by_customer`, fires `ml_training_job` with the same `partition_key`. |

Each sensor is independently togglable. Turn off the ml bridge to stop the ml chain from auto-firing while elt keeps running. Turn off the elt bridge to stop the elt chain from auto-firing while landing keeps ingesting. That's the "layered sensor" pattern: every boundary between responsibilities is an explicit, disablable gate.

### What about `AutomationCondition`?

Earlier versions of this template used `AutomationCondition.eager()` on mart models as a declarative alternative. With the full pipeline now daily-partitioned, `AutomationCondition` is less convenient — it needs per-partition wiring and doesn't naturally express "wait for BOTH a daily partition AND the enclosing monthly partition" before firing downstream. The imperative sensors do that in 10 lines. Teams that want declarative orchestration for partitioned pipelines typically pair `AutomationCondition` with `AutoMaterializePolicy`, which is a bigger topic this demo leaves for the reader.

### Rules of thumb

- **Use sensors** when you need cadence-bridging (daily vs monthly), cross-code-location triggers, or explicit layer boundaries.
- **Use `AutomationCondition`** for the simple case — unpartitioned or uniformly-partitioned dependency chains where "refresh on any upstream change" is the whole rule.

---

## Freshness — how stale is each asset?

Freshness checks surface "this asset hasn't been refreshed recently enough" as a first-class asset check. Every partitioned asset in this project has one attached.

**Where to look in the UI:** click any asset → **Checks** tab. You'll see a `freshness_check` row with one of:

- ✅ **PASS** — the expected partition has been materialized before its deadline.
- ⚠️ **WARN** — partition is approaching the deadline.
- ❌ **FAIL** — partition is overdue.

**What's checked:**

Every partitioned asset in both code locations uses `build_time_partition_freshness_checks` with a `deadline_cron`. The check asks: *"is the latest expected partition materialized before `deadline_cron`?"* That's the right model for a partitioned pipeline — "is today's partition in yet?" rather than "did anything at all run in the last N hours?".

| Layer | Deadline |
|---|---|
| `raw_sales_details`, `raw_cust_info`, `raw_loc_a101` (daily landing) | 9am every day |
| `raw_prd_info_monthly` (monthly landing) | 9am on the 2nd of each month |
| `landing/*`, `staging/*`, `mart/*`, `reporting/*` (dbt ELT) | 10am every day |
| `ml_features/*` (customer_rfm, customer_segments, churn_predictions) | 11am every day |

**Thresholds live in:**
- `code_locations/elt_pipelines/elt_pipelines/freshness_checks.py`
- `code_locations/ml_pipelines/ml_pipelines/freshness_checks.py`

Tune them up or down to match what "fresh enough" means for your pipeline. Freshness checks are evaluated by Dagster's `default_automation_condition_sensor`, so just make sure that sensor is on (see Step 3).

---

## Why these architectural choices

- **SQLite for Dagster metadata** — zero config, file-based, perfect for a local demo. Production would be Postgres.
- **One shared DuckDB file** mounted into both code-location containers. Lets `ml_pipelines` read the elt marts that `elt_pipelines` wrote. DuckDB permits only one writer at a time, so we serialize writes via `QueuedRunCoordinator` + `tag_concurrency_limits` on the `duckdb_writer` key.
- **One dbt project** at repo root. Each code location loads a different selector (elt takes everything except `ml_features`; ml takes only `ml_features`). Groups (`elt_team`, `ml_team`) + `+access:` settings enforce boundaries at the dbt layer.
- **`@dbt_assets` per code location** — not `load_assets_from_dbt_project`. The manifest is produced inside each container at startup with a per-container `--target-path` so the two containers don't race on the same `target/` folder.
- **Daily partitioning everywhere** — every dbt model is `materialized='incremental'` with `unique_key='snapshot_date'` and `incremental_strategy='delete+insert'`. Dagster reads `context.partition_time_window.start` and passes the partition date into dbt as `--vars '{"snapshot_dt":"YYYY-MM-DD"}'`. Each model then filters its own upstreams by that var. This is the pattern used in production Dagster/dbt projects (see the reference implementation at `imp_finance_mart`) — each partition holds the state of the world for that one day, and re-running a partition replaces only its own rows.
- **Split `@dbt_assets` blocks** — the partitioned block (landing dbt + staging + mart + reporting) uses `partitions_def=daily_partitions`; the seed chain (seeds + their `raw_erp_*` + `stg_erp_*` wrappers) uses a SEPARATE unpartitioned block so static reference data doesn't pretend to have a daily cadence.
- **Monthly source → daily pipeline via `latest-available-on-or-before`** — `raw_crm_prd_info` filters the monthly source with `WHERE snapshot_month = (SELECT MAX(snapshot_month) FROM source WHERE snapshot_month <= '{{ var("snapshot_dt") }}'::DATE)`. A daily run on 2026-04-15 uses the 2026-04-01 product snapshot; on 2026-05-15 it'd pick 2026-05-01 as soon as that monthly file lands.
- **dbt_utils for compound uniqueness tests** — since every natural key (customer_id, product_id, …) appears once per partition, `(natural_key, snapshot_date)` is the real uniqueness constraint. We use `dbt_utils.unique_combination_of_columns` for that instead of plain `unique`. Tests surface in the Dagster UI as asset checks.
- **ML assets partitioned too** — `customer_segments` and `churn_predictions` are daily assets with the same `delete+insert` semantics as the dbt side. The joblib artifact file is partition-stamped (`churn_model_YYYY-MM-DD.joblib`) so you can see a fresh artifact per partition without overwriting yesterday's.

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
- **"dbt found N package(s) specified in packages.yml, but only 0 package(s) installed in dbt_packages"** — happens right after `reset-demo` because that script wipes `dbt_packages/`. The code-location containers run `dbt deps` on startup if `dbt_packages/dbt_utils/` is missing; give it a few seconds and it self-heals. If it persists, `make down && make up` forces a fresh startup sequence.
- **"Binder Error: Cannot compare values of type VARCHAR and type DATE"** — means a Dagster-landed raw table has `snapshot_date` as VARCHAR but the dbt model is comparing it to a DATE. Every landing dbt model now does `snapshot_date::DATE` in both the SELECT and the WHERE; if you add a new source, follow that pattern.
- **Backfill produces a run per partition but they all queue** — expected: `duckdb_writer` concurrency limit is 1 so runs serialize. Backfills of many partitions take time linearly; switch to a real warehouse to parallelize.
- **Fresh slate** — `make reset-demo` stops the stack and returns the repo to a just-cloned state (see "Keeping the repo clean for the next person"). For a lighter wipe that keeps landing files, use `make wipe`.

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

## What this demo simulates (vs real-world)

The Dagster patterns in this repo (code locations, partitioned assets, sensors, `@dbt_assets` with `--vars` partition plumbing, incremental `delete+insert` models, asset checks + freshness) are the **real thing** — they'd work unchanged in production. But the **data source and the landing layer are stand-ins** for real infrastructure. If you're using this demo to learn Dagster, it's worth being explicit about what's real and what's simulated.

### Real-world flow

In a production setup, daily snapshots come from an **actual ingestion pipeline** that lands rows into a **real warehouse** (Snowflake, BigQuery, etc.). Dagster watches the warehouse, not a filesystem folder.

```
[source system]
     │ (extract via Airbyte / Fivetran / custom script)
     ▼
[landing area in a real warehouse — Snowflake/BigQuery/S3/GCS]
  e.g. LANDING.RAW_SALES_DETAILS with a snapshot_date column,
       appended daily by the ingestion pipeline
     │
     ▼
[Dagster sensor monitoring the landing area]
  - Snowflake partition sensor, GCS/S3 object sensor,
    or custom table-growth sensor
  - On detecting new data, fires a partitioned run for that day
     │
     ▼
[dbt models reading from LANDING via source(), filtering by
 snapshot_date = '{{ var("snapshot_dt") }}' passed in by Dagster]
     │
     ▼
[staging → mart → reporting — each incremental, delete+insert on the
 current partition, one row-set per snapshot_date]
     │
     ▼
[cross-location bridge sensor fires the ML training job for the same
 partition once reporting is ready]
```

Seeds, meanwhile, are genuinely static reference data — you drop a new CSV into `seeds/` and run `dbt seed` when the reference table needs refreshing. No sensor, no cadence.

### How this demo implements it

We replace three real components with cheaper stand-ins so the whole thing runs on your laptop:

```
[no source system — we fake it with a Faker-based generator]
     │
     ▼
[./data/landing/ folder on the host, bind-mounted into containers]
  = pretending to be the landing area
     │
     ▼
[landing_file_sensor watches the folder via os.listdir()]
  = pretending to be a warehouse partition sensor
     │
     ▼
[Dagster landing assets read the CSVs and write to DuckDB raw.* tables
 with a snapshot_date column carrying the partition key]
  = pretending to be the "ingestion finished, it's in LANDING now" state
     │
     ▼
[dbt reads from raw.* via source('dagster_raw', ...) and filters by
 snapshot_date = '{{ var("snapshot_dt") }}' (Dagster passes --vars)]
     │
     ▼
[staging → mart → reporting — same partition model as real world]
     │
     ▼
[elt_to_ml_bridge_sensor fires ml_training_job for the same partition]
```

### What's simulated vs real

| Real world | Demo stand-in |
|---|---|
| External source system | Faker-based generator (`scripts/generate_future_landing_data.py`) |
| Ingestion pipeline (Airbyte / Fivetran / custom) | A plain `cp` command copying CSVs into `data/landing/` |
| Landing warehouse table (Snowflake, BigQuery, …) | `./data/landing/*.csv` files on disk |
| Sensor polling warehouse for new partitions | `landing_file_sensor` polling the folder |
| Snowflake / BigQuery warehouse | Single DuckDB file at `./warehouse/oss_template.duckdb` |

**What stays identical to production:**
- Every Dagster pattern (code locations, partitioned assets, sensors, partitioned jobs, asset checks, freshness checks).
- Daily `delete+insert` incremental dbt models with `snapshot_date` as the partition watermark — exactly how a real Snowflake/BigQuery ELT is structured.
- The `--vars snapshot_dt` plumbing: Dagster reads `context.partition_time_window.start` and hands it to dbt; every model filters on `{{ var("snapshot_dt") }}`.
- The latest-available-on-or-before pattern for monthly-into-daily joins (`raw_crm_prd_info`).
- Cadence-bridging (`daily_monthly_bridge_sensor`) — exactly how you'd coordinate daily downstreams against a monthly upstream in any real setup.
- The dbt project structure and group-based access boundaries.

**What you'd swap for production:**
- Replace `landing_file_sensor` with whatever sensor suits your source (a Snowflake partition sensor, an S3 object sensor, etc.). Everything downstream of the sensor stays the same.
- Replace DuckDB with your production warehouse. All dbt SQL would need minimal adjustment for vendor-specific functions (this project's SQL was ported FROM Snowflake TO DuckDB, so the reverse is small).
- Replace the `cp` step with a real ingestion tool.

The point: the Dagster and dbt pieces are production-shaped; only the infrastructure underneath them is local-first.

---

## Things this template intentionally does NOT do

- No authentication on the UI (don't expose port 3000 publicly).
- No secrets manager — env vars in `.env.example` are plain-text.
- No Postgres, no Dagster+, no k8s, no branch deployments, no Snowflake.
- No production retry/alerting policies — add your own when adapting.
- No CI — this is a teaching artifact.
