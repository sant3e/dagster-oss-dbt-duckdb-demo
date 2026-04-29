# Dagster OSS Template

A **local** Dagster OSS demo showcasing multi-code-location architecture, dbt + DuckDB integration, **end-to-end daily partitioning** (every layer of the dbt graph), monthly-source-into-daily-pipeline via `latest-available` lookup, file-arrival + cadence-bridge + cross-location sensors, and a partitioned ML fan-out — on a real ELT pipeline adapted from [sant3e/dbt_snowflake_dwh_project](https://github.com/sant3e/dbt_snowflake_dwh_project).

> **Local only.** No auth, no secrets manager, no Postgres, no Snowflake, no Dagster+. Do not deploy any part of this.

---

## What's inside

```
dagster_oss_template/
├── Makefile                    one-liners for build / up / down / wipe / reset-demo
├── docker-compose.yml          webserver + daemon + 2 gRPC code locations on one network
├── docker/                     Dockerfiles for webserver/daemon and code locations
├── dagster_home/               dagster.yaml (SQLite storage, QueuedRunCoordinator) + workspace.yaml
├── dbt_project/                dbt + DuckDB project, ported from sant3e
│   ├── macros/                 shared SQL helpers
│   ├── models/
│   │   ├── sources.yml         dbt sources mapped onto Dagster raw/* AssetKeys
│   │   ├── groups.yml          elt_team, ml_team + owners
│   │   ├── staging/            stg_* daily-partitioned, reads sources or seeds
│   │   ├── mart/               dim_customers, dim_products, dim_products_history, fct_sales — daily-partitioned
│   │   ├── reporting/          rpt_* daily-partitioned
│   │   └── ml_features/        customer_rfm (owned by ml_team) — daily-partitioned
│   └── seeds/                  static reference CSVs (CUST_AZ12, PX_CAT_G1V2 — NOT partitioned)
├── code_locations/
│   ├── elt_pipelines/          gRPC server on :4000, owns the elt layers
│   └── ml_pipelines/           gRPC server on :4000, owns the ml layer
├── data/landing/               where daily + monthly snapshot CSVs land (day 1 ships here)
├── future_landing_data/        scratch space for Faker-generated day-N CSVs (empty by default)
├── warehouse/                  DuckDB file + joblib model artifacts (created on first run)
├── scripts/                    rebase_day1_csvs.sh, generate_future_landing_data.py, reset_demo.sh, wipe.sh
├── docs/                       slide deck, diagrams, and a why-Dagster explainer
└── pyproject.toml              top-level dev tooling config
```

---

## Quickstart

Prereqs: Docker Desktop + `make` (macOS: `xcode-select --install` · Linux: ships with `build-essential` · Windows: `choco install make` or use WSL).

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

### Step 1 — Rebase the day-1 landing CSVs to today − 3

Throughout this README, **day-1 / day-2 / day-3** refer to three consecutive demo partitions anchored at `today − 3` / `today − 2` / `today − 1`.

The four CSVs shipped in `data/landing/` are a **template**. The `rebase_day1_csvs.sh` script renames them and rewrites their `snapshot_date` / `snapshot_month` column so day-1 always equals `today − 3` — keeping all demo partitions inside the 7-day eager-lookback window.

```bash
./scripts/rebase_day1_csvs.sh
```

### Step 2 — Load static reference data

In the Dagster UI: **Jobs → `dbt_seed_job` → Materialize**.

This loads `seeds.CUST_AZ12` (18,484 rows) and `seeds.PX_CAT_G1V2` (37 rows) — ERP customer birthdays and product category taxonomy. Neither changes day-to-day, so they're unpartitioned and loaded once.

### Step 3 — Turn on the AutomationCondition + cross-partition sensors

In the Dagster UI: **Automation → Sensors**, toggle on:

- **`elt_automation_condition_sensor`** (in `elt_pipelines`)
- **`ml_automation_condition_sensor`** (in `ml_pipelines`)
- **`cross_partition_sensor`** (in `elt_pipelines`) — `cross_partition_sensor` fires `mart/dim_products_history` (tagged `latest_available`) for every new daily partition, joining the current-month monthly snapshot from `stg_crm_prd_info` (tagged `latest_available_source`) with the daily seed-derived `stg_erp_PX_CAT_G1V2`.

### Step 4 — Materialize day-1 landing

In the UI, toggle on **`landing_file_sensor`**. Within 30 seconds it detects the four CSVs in `data/landing/` and launches the day-1 partition:

- `raw_sales_details` (60,398 rows)
- `raw_cust_info` (18,484 rows)
- `raw_loc_a101` (18,484 rows)
- `raw_prd_info_monthly` (397 rows)

Once `raw/*` lands, `AutomationCondition` cascades day-1 downstream through staging → mart → reporting within ~90 seconds.

### Step 5 — Drop event-day files to see the daily cadence in action

The `future_landing_data/` folder is **empty by default**. Generate day-2 and day-3 event CSVs:

First-time prereq (one-off):
```bash
python3 -m pip install --break-system-packages faker
```

Generate:
```bash
python3 scripts/generate_future_landing_data.py --relative-to-today
```

Copy the generated files into `data/landing/`:
```bash
cp future_landing_data/*.csv data/landing/
```

`landing_file_sensor` picks them up within 30s and day-2 + day-3 cascade through staging → mart → reporting automatically.

#### Other generator invocations

```bash
# A single explicit day:
python3 scripts/generate_future_landing_data.py --start YYYY-MM-DD

# A date range (inclusive):
python3 scripts/generate_future_landing_data.py --start YYYY-MM-DD --end YYYY-MM-DD

# N days plus a monthly file for each month touched:
python3 scripts/generate_future_landing_data.py --start YYYY-MM-DD --days 7 --monthly
```

The generator is idempotent across invocations — customer IDs, product IDs, and order numbers continue monotonically so non-overlapping ranges never collide.

### Step 6 — Turn on the ELT→ML bridge

Toggle **`elt_to_ml_bridge_sensor`** on in the `ml_pipelines` code location.

Within 30s of each elt partition finishing, the sensor reads the new `reporting.rpt_sales_summary_by_customer` partition and fires `ml_training_job` for the same partition. `ml_features/customer_rfm` → `ml_features/customer_segments` → `ml_features/churn_predictions` all materialize.

Leave it on for the demo. Turn it **off** only when you want to prevent `elt_automation_condition_sensor` + `ml_automation_condition_sensor` from auto-materializing ml assets in the `ml_pipelines` code location on new reporting partitions — e.g. debugging an issue in the ml code, an upstream data quality problem you've spotted in elt that shouldn't propagate into ml, a planned ml-side release freeze, or if ml is expensive and you're iterating on elt without needing new predictions yet. With the bridge off, elt's own AC sensor keeps cascading landing → staging → mart → reporting as usual; only the ml chain stays paused. Flipping it back on resumes ml, which catches up on every reporting partition that accumulated while it was off.

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
SELECT 'raw.sales_details' AS tbl, snapshot_date, COUNT(*) AS rows FROM raw.sales_details GROUP BY 1,2
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
WHERE cp.snapshot_date = '<your-partition-date>'    -- pick any materialized partition, e.g. today - 3
  AND cp.is_high_risk = TRUE
ORDER BY cp.churn_probability DESC
LIMIT 15;
```

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

The Dagster and dbt pieces are **production-shaped**. Only the infrastructure underneath them is local-first. If you adapt this to production, three things change:

| What the demo does | What production would do |
|---|---|
| Faker generator drops CSVs into `data/landing/` | A real ingestion pipeline (Airbyte / Fivetran / custom) lands rows in a warehouse table |
| `landing_file_sensor` watches the folder via `os.listdir()` | A Snowflake partition sensor, S3/GCS object sensor, or custom warehouse sensor |
| DuckDB file at `./warehouse/oss_template.duckdb` | Snowflake / BigQuery |

Everything downstream of the sensor is unchanged: partitioned assets, `@dbt_assets` with `--vars snapshot_dt` plumbing, incremental `delete+insert` models keyed on `snapshot_date`, the latest-available-on-or-before pattern for monthly-into-daily joins, tag-driven `cross_partition_sensor`, freshness policies, asset checks, and the cross-location ML bridge. The SQL was ported FROM Snowflake TO DuckDB, so the reverse port is minimal.

---

## Orchestration at a glance

The graph is entirely sensor-driven. Four mechanisms cover the whole pipeline, each an independently-togglable gate on a specific hop:

| Hop | Mechanism | Why |
|---|---|---|
| Filesystem → landing assets | `landing_file_sensor` | Stand-in for a warehouse partition/object sensor — one sensor routes both daily `<prefix>_YYYY_MM_DD.csv` and monthly `prd_info_YYYY_MM.csv` files to the right job with the right `partition_key`. |
| landing → daily staging → mart → reporting | `AutomationCondition.eager()` | Handles the common case: a daily parent materializing fires its daily children per partition. Rule is `missing_in_window \| code_version_change \| eager_with_lookback` with a 7-day lookback, which bypasses the `since_last_handled()` cold-start trap. |
| Seed-only staging, monthly→daily bridge, mart bridge | `cross_partition_sensor` (Pass-0/1/2) | Covers the three cases `eager()` can't: seed-only stg (no partitioned upstream to fire on), `stg_crm_prd_info` (sparse monthly parent — `any_deps_missing()` stays TRUE, native eager never synthesizes the daily child), and `dim_products_history` (daily mart that must fire even when the monthly dim hasn't updated). Tag-driven: `latest_available_source` + `latest_available`. |
| reporting → ml (cross-code-location) | `elt_to_ml_bridge_sensor` | Lives in `ml_pipelines`, listens for new partitions of `reporting.rpt_sales_summary_by_customer` (in `elt_pipelines`), and fires `ml_training_job` with the same `partition_key`. Turn off to pause ml while elt keeps running. |

**Where to see it in the UI:** **Deployment** tab for the two code locations · **Automation → Sensors** for every sensor above · any asset's partition grid for per-day status · any asset's **Checks** tab for dbt tests (including `dbt_utils.unique_combination_of_columns`) and freshness · **Jobs → `dbt_seed_job`** to load the unpartitioned reference tables · **Backfill** on any partitioned asset (runs serialize via the `duckdb_writer` tag concurrency key; each partition becomes its own run via `BackfillPolicy.multi_run()`).

**ML fan-out:** `customer_rfm` feeds both `customer_segments` (KMeans) and `churn_predictions` (LogisticRegression), all within a single daily partition.

---

## Freshness

A `FreshnessPolicy.cron(...)` declares each asset's expected cadence as metadata on the asset itself — turning "this table should be updated by 10am every day" into a contract Dagster continuously evaluates, instead of an assumption you'd only discover was violated when a downstream consumer complains. It solves the silent-staleness problem: a materialization job succeeded, no alert fired, but the table is hours behind what consumers expect.

The policy is NOT an asset check and NOT a step in any materialization plan — runs stay green when the materialization succeeds, and freshness is evaluated separately by the automation infrastructure that `default_automation_condition_sensor` drives. Failures surface as PASS / WARN / FAIL on the asset's **Checks** tab and as asset-health state on the asset graph, so staleness becomes visible the moment it happens.

**Per-layer deadlines in this project:**

| Layer | Deadline |
|---|---|
| Daily Python landings (`raw_sales_details`, `raw_cust_info`, `raw_loc_a101`) | 9am every day |
| Monthly Python landing (`raw_prd_info_monthly`) | 9am on the 2nd of each month |
| `staging/*`, `mart/*`, `reporting/*` (dbt) | 10am every day |
| `ml_features/*` | 11am every day |

Policies live in `{elt,ml}_pipelines/constants.py` and the dbt translator's `get_freshness_policy()` in each `assets/dbt.py`. Tune them to match what "fresh enough" means for your pipeline.

---

## Why these architectural choices

- **SQLite for Dagster metadata** — zero config, file-based. Production would be Postgres.
- **One shared DuckDB file** mounted into both code-location containers so `ml_pipelines` can read elt marts. DuckDB allows one writer at a time, so writes serialize via `QueuedRunCoordinator` + the `duckdb_writer` tag concurrency limit.
- **One dbt project, two selectors** — elt takes everything except `ml_features`; ml takes only `ml_features`. Groups (`elt_team`, `ml_team`) + `+access:` enforce ownership boundaries at the dbt layer.
- **`@dbt_assets` per code location**, not `load_assets_from_dbt_project`. Each container builds its own manifest with a per-container `--target-path` so the two don't race on `target/`.
- **Daily partitioning everywhere** — every dbt model is `incremental` + `delete+insert` on `snapshot_date`. Dagster passes `--vars '{"snapshot_dt":"YYYY-MM-DD"}'`; each model filters itself on that var, so re-running a partition replaces only its rows.
- **Split `@dbt_assets` blocks** — one partitioned block for staging/mart/reporting, one seed-only block for `dbt_seed_job`. There's no separate "dbt landing" layer: the Python `raw/*` assets ARE the landing, and `models/sources.yml` declares `meta.dagster.asset_key` so each dbt source collapses onto its matching Python AssetKey.
- **Slow-cadence source → daily pipeline** — `stg_crm_prd_info` (tagged `latest_available_source`) carries the monthly cadence forward; `dim_products_history` (tagged `latest_available`) bridges it into the daily grain via `cross_partition_sensor`. Day 1…N of a month use the same monthly snapshot; a new monthly file flips subsequent partitions automatically.
- **`dbt_utils` for compound uniqueness** — `(natural_key, snapshot_date)` is the real constraint (natural keys repeat across partitions), so we use `dbt_utils.unique_combination_of_columns` instead of plain `unique`. Tests surface as asset checks.
- **Intentional WARN test** — `not_null_dim_customers_customer_id` is `severity: warn` because the upstream CRM has a few NULL `cst_id` rows. Shows the test-as-observability pattern: known data quality signals are visible, the pipeline keeps moving.
- **Partitioned ML assets** — `customer_segments` + `churn_predictions` use the same `delete+insert` semantics as dbt. The joblib artifact is partition-stamped (`churn_model_YYYY-MM-DD.joblib`) so yesterday's model isn't overwritten.

---

## Adding a new code location

1. `cp -r code_locations/ml_pipelines code_locations/<new_name>` and rename the package + `[tool.dagster]` fields in its `pyproject.toml`.
2. Add a new `grpc_server` entry to `dagster_home/workspace.yaml`.
3. Add a new service to `docker-compose.yml` (copy the `ml_pipelines` service, change the `CODE_LOCATION` build arg + container name).
4. `make build && make down && make up`.

---

## Things this template intentionally does NOT do

- No authentication on the UI (don't expose port 3000 publicly).
- No secrets manager — env vars in `.env.example` are plain-text.
- No Postgres, no Dagster+, no k8s, no branch deployments, no Snowflake.
- No production retry/alerting policies — add your own when adapting.
- No CI — this is a teaching artifact.

---

## Troubleshooting

- **"Code location failed to load"** — Check `docker compose logs <location>` for import errors. Most common cause: a stale `manifest.json`. Restart the offending container or rebuild (`make build`).
- **"database is locked"** — Another process holds a DuckDB writer. Check `make ps`; confirm only one writer asset runs at a time (the `duckdb_writer` concurrency key should prevent this). For interactive queries use `duckdb -readonly`.
- **Sensor not firing** — From the UI, confirm the sensor is toggled on (they are all OFF by default for local dev).
- **Freshness stuck / not updating** — Freshness is implemented via `FreshnessPolicy` attached to assets (not as a separate check step). Make sure `default_automation_condition_sensor` is on — that's what evaluates freshness and surfaces PASS/WARN/FAIL on the Checks tab. There is no separate freshness sensor to toggle.
- **Monthly partition rejected** — `MonthlyPartitionsDefinition` uses `end_offset=1` so the current month is valid; if you change `start_date`, make sure your file's month is within the supported range.
- **"dbt found N package(s) specified in packages.yml, but only 0 package(s) installed in dbt_packages"** — happens right after `reset-demo` because that script wipes `dbt_packages/`. The code-location containers run `dbt deps` on startup if `dbt_packages/dbt_utils/` is missing; give it a few seconds and it self-heals. If it persists, `make down && make up` forces a fresh startup sequence.
- **"Binder Error: Cannot compare values of type VARCHAR and type DATE"** — means a Dagster-landed raw table has `snapshot_date` as VARCHAR but the dbt model is comparing it to a DATE. Every staging dbt model does `snapshot_date::DATE AS snapshot_date` in the SELECT and `snapshot_date::DATE = '{{ var(...) }}'::DATE` in the WHERE; if you add a new source or staging model, follow that pattern.
- **Backfill produces a run per partition but they all queue** — expected: `duckdb_writer` concurrency limit is 1 so runs serialize. Backfills of many partitions take time linearly; switch to a real warehouse to parallelize.
- **Fresh slate** — `make reset-demo` stops the stack and returns the repo to a just-cloned state (see "Keeping the repo clean for the next person"). For a lighter wipe that keeps landing files, use `make wipe`.
- **`AutomationCondition.eager()` 7-day lookback window** — in `code_locations/elt_pipelines/elt_pipelines/assets/dbt.py`. The `rebase_day1_csvs.sh` script + the Faker generator's `--relative-to-today` mode keep all three demo partitions inside this window. If you generate partitions further back than 7 days, either rebase day-1 closer to those dates, or widen `timedelta(days=7)` in `get_automation_condition`.
