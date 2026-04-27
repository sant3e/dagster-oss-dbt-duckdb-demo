# Future landing data

Pre-generated **event-style** snapshot CSVs you can drop into `../data/landing/` to demo the sensors firing. Unlike the day-1 files in `data/landing/` (which are full-history initialization snapshots), these files contain **only the events that happened on the day they represent** — new orders placed, new customers signed up, countries changed, etc. Dagster's landing assets append them to DuckDB, and the dbt landing models dedup to the latest `snapshot_date` per business key.

## What's in here today

The shipped files were generated with:
```bash
python3 scripts/generate_future_landing_data.py --start 2026-04-02 --end 2026-04-05
python3 scripts/generate_future_landing_data.py --start 2026-05-01 --days 3 --monthly
```

That produces a chatty stream of events for April 2–5 (plus May 1–3 and a May monthly prd_info snapshot).

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
- `daily_monthly_bridge_sensor` fires the ELT downstream for each day once its three daily upstreams are ready **and** its month's monthly upstream exists.
- Example: if you copy April 2–5 + the May monthly, you'll get ELT runs for April 2–5. May days will stay idle until you also drop May daily files (which is exactly the "bridge" pattern the sensor exists to demonstrate).

## Regenerating / generating more days

Prereq:
```bash
pip install faker                    # if not already available
# or if you hit PEP-668 on macOS:
pip install --break-system-packages faker
```

The generator takes CLI arguments:

```bash
# A single day:
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
