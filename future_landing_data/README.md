# Future landing data

Pre-generated **event-style** snapshot CSVs you can drop into `../data/landing/` to demo the sensors firing. Unlike the day-1 template files in `data/landing/` (which are full-history initialization snapshots), these files contain **only the events that happened on the day they represent** — new orders placed, new customers signed up, countries changed, etc. Dagster's landing assets append them to DuckDB partitioned per day.

## The default demo flow

Paired with `scripts/rebase_day1_csvs.sh` (which anchors **day-1 at today − 3**), the generator's `--relative-to-today` flag emits **day-2 (today − 2) and day-3 (today − 1)**:

```bash
python3 scripts/generate_future_landing_data.py --relative-to-today
```

That produces 6 files (3 prefixes × 2 days). Together with the rebased day-1 files, the demo now covers day-1, day-2, and day-3 — three consecutive daily partitions inside the 7-day eager-lookback window.

Rough per-day volumes (deterministic with `--seed 42`):
- 5–13 customer events (new customers + marital-status flips)
- 9–11 location events (new customer locations + country moves)
- 24–36 sales events (brand-new orders placed that day)
- 15–20 product events in each monthly file (new products + cost revisions)

## How to use them in the demo

Copy all files, or just one day at a time, into `../data/landing/`:

```bash
# From the project root:
cp future_landing_data/*.csv data/landing/
```

Then:
- `landing_file_sensor` picks up each new CSV within ~30 s and kicks the matching partitioned landing asset (daily or monthly, automatically).
- `AutomationCondition.eager()` on the dbt staging models (`stg_crm_*`, `stg_erp_*`), all of mart, all of reporting auto-fires them per partition as the upstream `raw/*` Python landing assets materialize.
- `cross_partition_sensor` fires `mart/dim_products_history` for each new daily partition in expansion mode, reusing the latest monthly product snapshot whenever a newer one isn't available. If you cross into a later month without dropping that month's monthly file, the sensor keeps using the previous month's snapshot until a newer one lands.

## Regenerating / generating more days

Prereq:
```bash
# Recommended — guarantees pip installs into the same interpreter
# that `python3` resolves to (avoids the classic Homebrew situation
# where `pip` and `python3` target different versions):
python3 -m pip install --break-system-packages faker
```

The generator takes CLI arguments:

```bash
# Dynamic (default demo flow): today - 2 and today - 1:
python3 scripts/generate_future_landing_data.py --relative-to-today

# A single explicit day:
python3 scripts/generate_future_landing_data.py --start 2026-04-02

# A date range (inclusive both ends):
python3 scripts/generate_future_landing_data.py --start 2026-04-02 --end 2026-04-10

# N consecutive days from a start date:
python3 scripts/generate_future_landing_data.py --start 2026-05-01 --days 5

# Also emit a monthly prd_info file for each month the range touches:
python3 scripts/generate_future_landing_data.py --start 2026-05-01 --days 5 --monthly

# Reproducibility: any integer seed pins Faker + random identically:
python3 scripts/generate_future_landing_data.py --start 2026-04-02 --seed 123
```

Output lands in `future_landing_data/` (this folder). Existing files with the same name are overwritten.

**The generator is safe to run multiple times** — it scans any files already in this folder to continue customer IDs, product IDs, and sales-order numbers monotonically. Run it once for April, again for May, and the IDs will pick up where April left off rather than colliding.

## Data guarantees

- Sales are always positive: `sls_sales = sls_price × sls_quantity`, with `sls_price` picked from a realistic tier list and `sls_quantity` in `[1, 10]`.
- Customer IDs are unique (new customers start at 30000, monotonic across invocations).
- Product IDs are unique (new products start at 1000, monotonic across invocations).
- Sales order numbers continue from `SO75124` upward.
- Country changes never re-select the customer's current country.
- Names come from Faker's `first_name()` / `last_name()` so you get realistic diversity rather than a handful of repeats.
