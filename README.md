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
│   │   ├── staging/            stg_* daily-partitioned, reads dbt sources or seeds, cleaned
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

## Feature tour (mapped to the UI)

| Feature | Where to find it |
|---|---|
| **Multi-code-location** | Deployment tab — two green locations. Asset graph stitches across them. |
| **dbt + DuckDB** | Assets tab — `raw_* (Python) → staging → mart → reporting → ml_features`. Every dbt model is **daily-partitioned on `snapshot_date`**, incremental (`delete+insert` on `snapshot_date`). Dbt tests (including `dbt_utils.unique_combination_of_columns` for `(natural_key, snapshot_date)`) surface as asset checks. |
| **dbt groups & access** | `dbt_project/models/groups.yml` + `+group:`/`+access:` in `dbt_project.yml`. Staging is `access: private`, mart/reporting are `public`. |
| **Daily partitions** | Every layer — `raw_*` (Python), `staging/*`, `mart/*`, `reporting/*`, `ml_features/*`. Partition grid on every asset. Dagster passes `--vars '{"snapshot_dt":"YYYY-MM-DD"}'` into dbt; each model filters itself on that var. |
| **Monthly source → daily pipeline** | The monthly product source lands in DuckDB via the Python asset `raw/raw_prd_info_monthly`. The dbt source `dagster_raw.prd_info` declares `meta.dagster.asset_key: ["raw", "raw_prd_info_monthly"]` so the source collapses onto that Python AssetKey — one node, not two. The staging model `stg_crm_prd_info` reads the source directly, picks the latest-available monthly snapshot on-or-before the current partition date (via an `INNER JOIN (SELECT MAX(snapshot_month))`), and carries the slow cadence forward (tagged `latest_available_source`). The first mart that bridges this slow cadence into the daily pipeline is `dim_products_history` (tagged `latest_available`). |
| **Cross-partition bridge sensor** | `cross_partition_sensor` — tag-driven, ported from `imp_v2-dagster-etl/bhi_imp/sensor/cross_partition_sensor.py` and extended with two extra passes to cover what AC can't. Runs three passes per tick: **Pass-0** fires seed-derived partitioned staging models (`stg_erp_CUST_AZ12`, `stg_erp_PX_CAT_G1V2`) for every daily partition where a sibling daily `raw/*` asset has materialized — AC can't drive these because their only upstream is an unpartitioned seed, so `any_deps_updated()` never fires. **Pass-1** fires `latest_available_source`-tagged staging models (e.g. `stg_crm_prd_info`) once per materialized monthly source partition — prevents the 26-partition fan-out that AC would cause on a daily-partitioned stg with a monthly upstream. **Pass-2** fires `latest_available`-tagged marts (e.g. `dim_products_history`) in expansion mode using the intersection of exact-match daily deps so the bridging mart never stalls waiting for a monthly source update. |
| **AutomationCondition.eager()** | Every partitioned dbt asset carries it EXCEPT models tagged `latest_available` OR `latest_available_source` — both are driven by `cross_partition_sensor`. AC handles the standard case: daily raw/* → daily staging → daily mart → daily reporting. The rule is `missing_in_window \| code_version_change \| eager_with_lookback` with a **7-day lookback window** (widen in `assets/dbt.py` for longer demo ranges). The `missing_in_window` branch fires any missing partition in the window regardless of sensor-enable order — this bypasses the `since_last_handled()` cold-start trap. Ported from `imp_v2-dagster-etl`. |
| **Monthly partition** | `raw_prd_info_monthly` — a separate partition grid with month-start keys. |
| **Backfills** | Click **Backfill** on any partitioned asset. Runs execute serially thanks to the `duckdb_writer` tag concurrency limit. `BackfillPolicy.multi_run()` on the dbt assets means each partition becomes its own run (visible in the runs tab). |
| **File-arrival sensor** | `landing_file_sensor` — one sensor routes both daily AND monthly files to the right job. |
| **Manual dbt-seed job** | `dbt_seed_job` in Jobs — seeds only (CUST_AZ12, PX_CAT_G1V2). Static reference data, unpartitioned. Downstream `stg_erp_CUST_AZ12` / `stg_erp_PX_CAT_G1V2` staging models are partitioned — they read the seed directly and stamp `snapshot_date` onto every row per partition, cascading via AutomationCondition. |
| **Cross-location asset sensor** | `elt_to_ml_bridge_sensor` (ml_pipelines) — listens for new partitions of `reporting.rpt_sales_summary_by_customer` in the elt_pipelines code location and fires `ml_training_job` with the same `partition_key`. |
| **ML fan-out** | `customer_segments` (KMeans) + `churn_predictions` (LogisticRegression) both consume `customer_rfm` within a single partition. |
| **Freshness** | Every partitioned asset carries a `FreshnessPolicy.cron(deadline_cron=..., lower_bound_delta=...)` — attached to `@asset(...)` for Python assets or via the dbt translator's `get_freshness_policy()` override. Evaluated automatically by Dagster's automation infrastructure; no separate sensor to toggle on. Open any asset → **Checks** tab → you'll see `freshness_check` with PASS / WARN / FAIL. Policies live in `elt_pipelines/constants.py` and `ml_pipelines/constants.py`, plus the translator methods in each `assets/dbt.py`. |

---

## Sensor-driven orchestration — one layer at a time

The project is **entirely sensor-driven** now that every layer is partitioned. Sensors form a cascade, each one bridging a different hop in the graph:

| Hop | Sensor | What it does |
|---|---|---|
| Filesystem → landing assets | `landing_file_sensor` | Detects new `<prefix>_YYYY_MM_DD.csv` / `prd_info_YYYY_MM.csv` files, emits one `RunRequest` per file with the right `partition_key` + job. |
| landing (Python daily) → staging (daily-fed stg) | `AutomationCondition.eager()` (via `elt_automation_condition_sensor`) | Staging models fed directly by daily `raw/*` (`stg_crm_cust_info`, `stg_crm_sales_details`, `stg_erp_LOC_A101`) auto-fire per partition when their upstream materializes. |
| landing (seeds) → staging (seed-derived stg) | `cross_partition_sensor` **Pass-0** | Staging models whose ONLY upstream is an unpartitioned seed (`stg_erp_CUST_AZ12`, `stg_erp_PX_CAT_G1V2`) can't be driven by AC — the seed has no time-partition for `any_deps_updated()` to fire on. Pass-0 fires them for every daily partition where a sibling daily `raw/*` is materialized. |
| landing (monthly Python) → staging (slow-cadence stg) | `cross_partition_sensor` **Pass-1** | `stg_crm_prd_info` is daily-partitioned but its only upstream (`raw/raw_prd_info_monthly`) is monthly. AC's default partition mapping would satisfy `any_deps_updated()` for every daily partition in the month (26-day fan-out). Pass-1 fires this model exactly once per materialized monthly source partition. |
| mart / cross-cadence bridge | `cross_partition_sensor` **Pass-2** (for `dim_products_history`) | The first mart that joins a daily dimension (`stg_erp_PX_CAT_G1V2`) with a slow-cadence dimension (`stg_crm_prd_info`). Tagged `latest_available`. The sensor fires it daily in expansion mode using the intersection of exact-match deps — the daily pipeline keeps producing rows using the latest-available monthly snapshot. The SQL inside uses a `MAX(snapshot_date) <= var.snapshot_dt` CTE to pick the in-effect monthly snapshot and stamps the current daily partition. |
| mart / reporting (downstream of the bridge) | `AutomationCondition.eager()` | `dim_products`, `fct_sales`, `rpt_*` all auto-cascade once `dim_products_history` materializes for a partition. |
| elt → ml (cross-code-location) | `elt_to_ml_bridge_sensor` (ml_pipelines) | Listens for new partitions of `reporting.rpt_sales_summary_by_customer`, fires `ml_training_job` with the same `partition_key`. Turn it **off** to stop the ml chain while elt keeps running. |

Each sensor is independently togglable. Turn off the ml bridge to stop the ml chain from auto-firing while elt keeps running. Turn off the AC sensor to stop the cascade while landing keeps ingesting. That's the "layered sensor" pattern: every boundary between responsibilities is an explicit, disablable gate.

---

## Freshness — how stale is each asset?

Every partitioned asset in this project carries a `FreshnessPolicy.cron(...)`. This is pure metadata on the asset — NOT an asset check, NOT a step in any job. Dagster's automation infrastructure evaluates the policy on its regular tick (driven by `default_automation_condition_sensor`) and surfaces the result on the asset's **Checks** tab.

Because `FreshnessPolicy` is not a check step, **no materialization job includes a freshness step in its execution plan**. Runs are always clean green when the materialization succeeds; freshness is evaluated and surfaced independently.

**How it's attached:**

- Python assets — `@asset(freshness_policy=FRESHNESS_LANDING_DAILY | FRESHNESS_LANDING_MONTHLY | FRESHNESS_ML_DAILY)`.
- dbt models — via `get_freshness_policy()` on the custom `DagsterDbtTranslator` (no per-model YAML config needed; the translator assigns per-layer policies centrally).

**Where to look in the UI:** with `default_automation_condition_sensor` on, open any partitioned asset → **Checks** tab → `freshness_check` row with one of:

- ✅ **PASS** — the expected partition has been materialized before its deadline.
- ⚠️ **WARN** — partition is approaching the deadline.
- ❌ **FAIL** — partition is overdue.

**Per-layer deadlines (cron-based):**

| Layer | Deadline |
|---|---|
| `raw_sales_details`, `raw_cust_info`, `raw_loc_a101` (daily Python landings) | 9am every day |
| `raw_prd_info_monthly` (monthly Python landing) | 9am on the 2nd of each month |
| `staging/*`, `mart/*`, `reporting/*` (dbt) | 10am every day |
| `ml_features/*` (dbt + Python) | 11am every day |

**Policies live in:**
- `code_locations/elt_pipelines/elt_pipelines/constants.py` — `FRESHNESS_LANDING_DAILY`, `FRESHNESS_LANDING_MONTHLY`
- `code_locations/elt_pipelines/elt_pipelines/assets/dbt.py` — `_FRESHNESS_LANDING`, `_FRESHNESS_ELT` + the translator's `get_freshness_policy`
- `code_locations/ml_pipelines/ml_pipelines/constants.py` — `FRESHNESS_ML_DAILY`
- `code_locations/ml_pipelines/ml_pipelines/assets/dbt.py` — translator's `get_freshness_policy`

Tune them up or down to match what "fresh enough" means for your pipeline. No sensor to toggle on — as long as `default_automation_condition_sensor` is running, freshness is evaluated.

---

## Why these architectural choices

- **SQLite for Dagster metadata** — zero config, file-based, perfect for a local demo. Production would be Postgres.
- **One shared DuckDB file** mounted into both code-location containers. Lets `ml_pipelines` read the elt marts that `elt_pipelines` wrote. DuckDB permits only one writer at a time, so we serialize writes via `QueuedRunCoordinator` + `tag_concurrency_limits` on the `duckdb_writer` key.
- **One dbt project** at repo root. Each code location loads a different selector (elt takes everything except `ml_features`; ml takes only `ml_features`). Groups (`elt_team`, `ml_team`) + `+access:` settings enforce boundaries at the dbt layer.
- **`@dbt_assets` per code location** — not `load_assets_from_dbt_project`. The manifest is produced inside each container at startup with a per-container `--target-path` so the two containers don't race on the same `target/` folder.
- **Daily partitioning everywhere** — every dbt model is `materialized='incremental'` with `unique_key='snapshot_date'` and `incremental_strategy='delete+insert'`. Dagster reads `context.partition_time_window.start` and passes the partition date into dbt as `--vars '{"snapshot_dt":"YYYY-MM-DD"}'`. Each model then filters its own upstreams by that var. This is the pattern used in production Dagster/dbt projects (see the reference implementation at `imp_finance_mart`) — each partition holds the state of the world for that one day, and re-running a partition replaces only its own rows.
- **Split `@dbt_assets` blocks** — the partitioned block contains all of staging + mart + reporting. The seed-only block contains just the two dbt seeds (CUST_AZ12, PX_CAT_G1V2), materialized once via `dbt_seed_job`. There is no separate "dbt landing" layer — the Python-owned `raw/*` assets ARE the landing, and dbt sources in `models/sources.yml` declare `meta.dagster.asset_key` so each source collapses onto its matching Python landing AssetKey. Staging reads the source directly.
- **Slow-cadence source → daily pipeline** — the monthly product source carries its cadence into dbt via `stg_crm_prd_info` (tagged `latest_available_source`). The first mart that bridges it into the daily grain is `dim_products_history` (tagged `latest_available`), which joins the slow-cadence stg with a daily seed-derived dim (`stg_erp_PX_CAT_G1V2`). `cross_partition_sensor` fires `dim_products_history` daily in expansion mode so the daily pipeline keeps producing new mart rows even while the monthly source hasn't updated. Concretely: day 1 through day N of a given month all use that month's single monthly product snapshot; when a new month's monthly file lands, subsequent daily partitions pick it up automatically.
- **dbt_utils for compound uniqueness tests** — since every natural key (customer_id, product_id, …) appears once per partition, `(natural_key, snapshot_date)` is the real uniqueness constraint. We use `dbt_utils.unique_combination_of_columns` for that instead of plain `unique`. Tests surface in the Dagster UI as asset checks.
- **Intentional WARN test — `not_null_dim_customers_customer_id`** — the upstream CRM data (from `sant3e/dbt_snowflake_dwh_project`) has a few rows with NULL `cst_id`. The test on `mart/dim_customers.customer_id` is configured with `severity: warn` so it surfaces the issue on the Checks tab without blocking the run or any downstream assets. Open `mart/dim_customers` → Checks tab → you'll see `not_null_dim_customers_customer_id` with a yellow WARN badge. This demonstrates the test-as-observability pattern: known data quality signals flow visibly, the pipeline keeps moving.
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
- **Freshness stuck / not updating** — Freshness is implemented via `FreshnessPolicy` attached to assets (not as a separate check step). Make sure `default_automation_condition_sensor` is on — that's what evaluates freshness and surfaces PASS/WARN/FAIL on the Checks tab. There is no separate freshness sensor to toggle.
- **Monthly partition rejected** — `MonthlyPartitionsDefinition` uses `end_offset=1` so the current month is valid; if you change `start_date`, make sure your file's month is within the supported range.
- **"dbt found N package(s) specified in packages.yml, but only 0 package(s) installed in dbt_packages"** — happens right after `reset-demo` because that script wipes `dbt_packages/`. The code-location containers run `dbt deps` on startup if `dbt_packages/dbt_utils/` is missing; give it a few seconds and it self-heals. If it persists, `make down && make up` forces a fresh startup sequence.
- **"Binder Error: Cannot compare values of type VARCHAR and type DATE"** — means a Dagster-landed raw table has `snapshot_date` as VARCHAR but the dbt model is comparing it to a DATE. Every staging dbt model does `snapshot_date::DATE AS snapshot_date` in the SELECT and `snapshot_date::DATE = '{{ var(...) }}'::DATE` in the WHERE; if you add a new source or staging model, follow that pattern.
- **Backfill produces a run per partition but they all queue** — expected: `duckdb_writer` concurrency limit is 1 so runs serialize. Backfills of many partitions take time linearly; switch to a real warehouse to parallelize.
- **Fresh slate** — `make reset-demo` stops the stack and returns the repo to a just-cloned state (see "Keeping the repo clean for the next person"). For a lighter wipe that keeps landing files, use `make wipe`.
- **`AutomationCondition.eager()` 7-day lookback window** — in `code_locations/elt_pipelines/elt_pipelines/assets/dbt.py`. The `rebase_day1_csvs.sh` script + the Faker generator's `--relative-to-today` mode keep all three demo partitions inside this window. If you generate partitions further back than 7 days, either rebase day-1 closer to those dates, or widen `timedelta(days=7)` in `get_automation_condition`.

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
- The latest-available-on-or-before pattern for monthly-into-daily joins (in `stg_crm_prd_info`'s SQL filter).
- Cadence-bridging via tag-driven `cross_partition_sensor`: `stg_crm_prd_info` tagged `latest_available_source`, `dim_products_history` tagged `latest_available`. Mirrors how `imp_finance_mart` coordinates daily marts against slow-cadence staging models.
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
