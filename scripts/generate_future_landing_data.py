#!/usr/bin/env python3
"""Generate event-style landing snapshots for the Dagster demo.

Each day's CSV contains ONLY the events that happened on that day:
  - sales_details_YYYY_MM_DD.csv  — orders placed that day
  - cust_info_YYYY_MM_DD.csv      — customers created or modified that day
  - loc_a101_YYYY_MM_DD.csv       — locations created or changed that day

The monthly file (prd_info_YYYY_MM.csv) is emitted only when --monthly is
passed. It contains the product-catalog changes during that month:
  - product cost revisions
  - brand-new products

Usage examples
--------------
# Dynamic: today - 2 and today - 1 (pairs with scripts/rebase_day1_csvs.sh
# which sets day-1 = today - 3):
python3 scripts/generate_future_landing_data.py --relative-to-today

# A single day (just 2026-04-02):
python3 scripts/generate_future_landing_data.py --start 2026-04-02

# Five days, 2026-04-02 through 2026-04-06:
python3 scripts/generate_future_landing_data.py --start 2026-04-02 --days 5

# A date range (inclusive both ends):
python3 scripts/generate_future_landing_data.py --start 2026-04-02 --end 2026-04-10

# Also emit the May monthly product catalog:
python3 scripts/generate_future_landing_data.py --start 2026-05-01 --days 3 --monthly

# Deterministic output (same args -> same files) uses --seed (default: 42):
python3 scripts/generate_future_landing_data.py --start 2026-04-02 --seed 123

Output
------
All files land in ./future_landing_data/ next to this script's project
root. Existing files with the same name are overwritten.
"""


import argparse
import csv
import random
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

try:
    from faker import Faker
except ImportError:
    print(
        "ERROR: the `faker` package is required.\n"
        "Install it with:  pip install faker",
        file=sys.stderr,
    )
    sys.exit(1)


ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "data" / "landing"
DST = ROOT / "future_landing_data"


def _find_template(prefix: str) -> Path:
    """Find the single template CSV in data/landing/ matching ``<prefix>*.csv``.

    The filename's date stamp is not fixed — ``scripts/rebase_day1_csvs.sh``
    renames the git-tracked template files to ``<prefix>_<today-3>.csv``.
    We resolve the template by prefix at call time so this generator works
    regardless of what date the template currently carries.
    """
    matches = sorted(SRC.glob(f"{prefix}*.csv"))
    if not matches:
        raise FileNotFoundError(
            f"No template file matching '{prefix}*.csv' in {SRC}. "
            f"Run `make reset-demo` to restore the git-tracked templates, "
            f"then `./scripts/rebase_day1_csvs.sh` to date-stamp them."
        )
    if len(matches) > 1:
        raise RuntimeError(
            f"Multiple files match '{prefix}*.csv' in {SRC}: {matches}. "
            f"Run `make reset-demo` to clean up."
        )
    return matches[0]

# Volume defaults (chatty mode).
NEW_ORDERS_PER_DAY = (20, 40)            # min, max random.randint
NEW_CUSTOMERS_PER_DAY = (5, 8)
CUSTOMER_MODS_PER_DAY = (3, 6)           # existing-customer marital-status flips
LOCATION_MOVES_PER_DAY = (3, 5)
MONTHLY_NEW_PRODUCTS = (3, 6)
MONTHLY_PRICE_REVISIONS = (8, 15)

# Realistic ranges.
QUANTITY_RANGE = (1, 10)
PRICE_TIERS = [9, 22, 35, 89, 159, 269, 349, 499, 699, 1299, 1700, 2049, 2294, 3399, 3578]
COUNTRY_POOL = [
    "United States", "United Kingdom", "Germany", "France", "Canada",
    "Australia", "Netherlands", "Spain", "Italy", "Japan",
]
PRODUCT_LINES = ["R", "M", "T", "S"]
PRODUCT_NAME_ADJECTIVES = [
    "Pro", "Elite", "Lite", "Carbon", "Ultra", "Stealth", "Trail", "City",
    "Gravel", "Aero", "Endurance", "Alpine",
]
PRODUCT_NAME_NOUNS = [
    "Racer", "Commuter", "Cruiser", "Tourer", "Climber", "Sprinter",
    "Ranger", "Voyager", "Drifter",
]


# -------- helpers --------

def read_csv(path: Path) -> tuple[list[str], list[list[str]]]:
    with open(path, newline="") as f:
        r = list(csv.reader(f))
    return r[0], r[1:]


def write_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def daterange(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--start",
        required=False,
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        help="First day to generate (inclusive), format YYYY-MM-DD.",
    )
    span = p.add_mutually_exclusive_group()
    span.add_argument(
        "--end",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        help="Last day to generate (inclusive). Default: same as --start.",
    )
    span.add_argument(
        "--days",
        type=int,
        help="Generate N consecutive days starting at --start.",
    )
    p.add_argument(
        "--relative-to-today",
        action="store_true",
        help=(
            "Dynamic mode: generate (today - 2) and (today - 1) — exactly "
            "two consecutive days. Pairs with scripts/rebase_day1_csvs.sh "
            "which anchors day-1 at (today - 3). Overrides --start/--end/--days."
        ),
    )
    p.add_argument(
        "--monthly",
        action="store_true",
        help=(
            "Also emit a monthly prd_info file for the month of --start. "
            "One monthly file is emitted per distinct month touched by the "
            "date range."
        ),
    )
    p.add_argument("--seed", type=int, default=42, help="Random seed (default 42).")
    args = p.parse_args()
    if not args.relative_to_today and args.start is None:
        p.error("either --start or --relative-to-today is required")
    return args


# -------- data builders --------

def build_day_customers_event(
    day: date,
    fake: Faker,
    rng: random.Random,
    header: list[str],
    existing_customer_ids: set[int],
    next_customer_id: int,
    day1_indexed: dict[int, list[str]],
) -> tuple[list[list[str]], list[int], int]:
    """New customers + marital-status flips for `day`.

    Returns (rows_for_the_day, newly_created_ids, next_customer_id_after).
    """
    idx = {c: header.index(c) for c in header}
    day_str = day.isoformat()
    rows: list[list[str]] = []
    newly_created: list[int] = []

    # New customers.
    for _ in range(rng.randint(*NEW_CUSTOMERS_PER_DAY)):
        cst_id = next_customer_id
        next_customer_id += 1
        cst_key = f"AW{cst_id:08d}"
        fn = fake.first_name()
        ln = fake.last_name()
        ms = rng.choice(["S", "M"])
        gn = rng.choice(["M", "F"])
        rows.append([str(cst_id), cst_key, fn, ln, ms, gn, day_str, day_str])
        newly_created.append(cst_id)
        existing_customer_ids.add(cst_id)

    # Marital-status flips on existing customers.
    targets = rng.sample(
        sorted(existing_customer_ids),
        k=min(rng.randint(*CUSTOMER_MODS_PER_DAY), len(existing_customer_ids)),
    )
    for cst_id in targets:
        # Build the flipped row from the latest-known baseline (day-1 snapshot).
        baseline = day1_indexed.get(cst_id)
        if baseline is None:
            # The customer was created in a previous event-day; we can't
            # easily look it up (we don't re-read earlier output files).
            # Just emit a plausible flip using a synthetic baseline.
            cst_key = f"AW{cst_id:08d}"
            fn = fake.first_name()
            ln = fake.last_name()
            gn = rng.choice(["M", "F"])
            cur_ms = rng.choice(["S", "M"])
        else:
            cst_key = baseline[idx["cst_key"]]
            fn = baseline[idx["cst_firstname"]].strip()
            ln = baseline[idx["cst_lastname"]].strip()
            gn = baseline[idx["cst_gndr"]].strip()
            cur_ms = baseline[idx["cst_marital_status"]].strip().upper()
        new_ms = "M" if cur_ms.startswith("S") else "S"
        rows.append([str(cst_id), cst_key, fn, ln, new_ms, gn, day_str, day_str])

    return rows, newly_created, next_customer_id


def build_day_locations_event(
    day: date,
    rng: random.Random,
    header: list[str],
    existing_cids: set[str],
    new_customer_ids: list[int],
    day1_indexed_by_cid: dict[str, list[str]],
) -> list[list[str]]:
    """Locations for the customers created today + some 'moves' for existing customers."""
    idx = {c: header.index(c) for c in header}
    day_str = day.isoformat()
    rows: list[list[str]] = []

    # One location row per newly-created customer.
    for cst_id in new_customer_ids:
        cid_hyphen = f"AW-{cst_id:08d}"
        country = rng.choice(COUNTRY_POOL)
        rows.append([cid_hyphen, country, day_str])
        existing_cids.add(cid_hyphen)

    # Country moves on existing customers.
    move_count = min(rng.randint(*LOCATION_MOVES_PER_DAY), len(existing_cids))
    move_targets = rng.sample(sorted(existing_cids), k=move_count)
    for cid in move_targets:
        baseline = day1_indexed_by_cid.get(cid)
        cur_country = baseline[idx["CNTRY"]].strip() if baseline else None
        candidates = [c for c in COUNTRY_POOL if c != cur_country]
        rows.append([cid, rng.choice(candidates), day_str])

    return rows


def build_day_sales_event(
    day: date,
    rng: random.Random,
    next_order_number: int,
    existing_customer_ids: set[int],
    valid_prd_keys: list[str],
) -> tuple[list[list[str]], int]:
    """Orders placed on `day`. Returns (rows, next_order_number_after)."""
    day_compact = day.strftime("%Y%m%d")
    day_str = day.isoformat()
    ship = (day + timedelta(days=7)).strftime("%Y%m%d")
    due = (day + timedelta(days=14)).strftime("%Y%m%d")
    rows: list[list[str]] = []

    n_orders = rng.randint(*NEW_ORDERS_PER_DAY)
    for _ in range(n_orders):
        so = f"SO{next_order_number}"
        next_order_number += 1
        # prd_info.prd_key is in 5-segment form ("BI-RB-BK-R50R-58"). The
        # downstream staging model `stg_crm_prd_info` does
        # `REPLACE(SUBSTRING(prd_key, 7), '-', '_')` to produce the
        # 3-segment product_number ("BK_R50R_58") that `dim_products`
        # holds. `sales_details.sls_prd_key` must be in the *same
        # 3-segment form* (with dashes) so that staging and the mart join
        # line up. The day-1 template uses this form ("BK-R93R-62") too.
        full_prd_key = rng.choice(valid_prd_keys)
        prd_key = full_prd_key[6:] if len(full_prd_key) > 6 else full_prd_key
        cust_id = rng.choice(sorted(existing_customer_ids))
        qty = rng.randint(*QUANTITY_RANGE)
        price = rng.choice(PRICE_TIERS)
        sales = price * qty
        rows.append(
            [so, prd_key, str(cust_id), day_compact, ship, due,
             str(sales), str(qty), str(price), day_str]
        )
    return rows, next_order_number


def build_monthly_products_event(
    month_start: date,
    rng: random.Random,
    header: list[str],
    day1_products: list[list[str]],
    next_product_id: int,
) -> tuple[list[list[str]], int]:
    """Product-catalog changes for the given month."""
    idx = {c: header.index(c) for c in header}
    month_str = month_start.isoformat()
    rows: list[list[str]] = []

    # Price revisions.
    revision_count = min(rng.randint(*MONTHLY_PRICE_REVISIONS), len(day1_products))
    revision_targets = rng.sample(day1_products, k=revision_count)
    for original in revision_targets:
        revised = list(original)
        base_str = revised[idx["prd_cost"]]
        base = int(base_str) if base_str.isdigit() else rng.randint(20, 2500)
        pct = rng.uniform(0.7, 1.3)  # ±30% revision
        revised[idx["prd_cost"]] = str(max(1, int(base * pct)))
        revised[idx["snapshot_month"]] = month_str
        rows.append(revised)

    # Brand-new products.
    n_new = rng.randint(*MONTHLY_NEW_PRODUCTS)
    for _ in range(n_new):
        prd_id = next_product_id
        next_product_id += 1
        prd_key = f"BK-NEW-{prd_id:04d}"
        prd_nm = f"{rng.choice(PRODUCT_NAME_ADJECTIVES)} {rng.choice(PRODUCT_NAME_NOUNS)}"
        cost = rng.choice([350, 480, 780, 1200, 1850, 2400, 3100])
        line = rng.choice(PRODUCT_LINES)
        rows.append([
            str(prd_id),
            prd_key,
            prd_nm,
            str(cost),
            line,
            f"{month_str} 00:00:00",
            "",
            month_str,
        ])
    return rows, next_product_id


# -------- main --------

def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)
    Faker.seed(args.seed)
    fake = Faker()

    start = args.start
    if args.relative_to_today:
        today = date.today()
        start = today - timedelta(days=2)
        end = today - timedelta(days=1)
    elif args.end:
        end = args.end
    elif args.days:
        end = start + timedelta(days=args.days - 1)
    else:
        end = start

    if end < start:
        print(f"ERROR: --end ({end}) must be >= --start ({start})", file=sys.stderr)
        sys.exit(2)

    # Load day-1 baseline files (full snapshots) to seed our generators.
    # Templates are resolved by prefix, not a fixed filename, because
    # scripts/rebase_day1_csvs.sh renames them to today-3 before this
    # generator runs.
    cust_header, cust_day1 = read_csv(_find_template("cust_info_"))
    loc_header, loc_day1 = read_csv(_find_template("loc_a101_"))
    prd_header, prd_day1 = read_csv(_find_template("prd_info_"))

    cust_idx = {c: cust_header.index(c) for c in cust_header}
    loc_idx = {c: loc_header.index(c) for c in loc_header}
    prd_idx = {c: prd_header.index(c) for c in prd_header}

    existing_customer_ids = {
        int(r[cust_idx["cst_id"]])
        for r in cust_day1
        if r[cust_idx["cst_id"]].strip().isdigit()
    }
    existing_cids = {r[loc_idx["CID"]] for r in loc_day1}
    day1_indexed_by_cst = {
        int(r[cust_idx["cst_id"]]): r
        for r in cust_day1
        if r[cust_idx["cst_id"]].strip().isdigit()
    }
    day1_indexed_by_cid = {r[loc_idx["CID"]]: r for r in loc_day1}
    valid_prd_keys = [r[prd_idx["prd_key"]] for r in prd_day1 if r[prd_idx["prd_key"]]]

    # Pick starting id/counter values safely above anything in the baseline
    # AND above anything already generated in previous runs of this script.
    next_customer_id = max(existing_customer_ids, default=0) + 1
    next_customer_id = max(next_customer_id, 30_000)
    next_product_id = max(
        (int(r[prd_idx["prd_id"]]) for r in prd_day1 if r[prd_idx["prd_id"]].strip().isdigit()),
        default=0,
    ) + 1
    next_product_id = max(next_product_id, 1_000)
    # sales_details baseline ends around SO75123.
    next_order_number = 75_124

    # Scan existing generated files (if any) so re-running the script keeps
    # IDs monotonic across invocations — critical when generating more days.
    for prev in DST.glob("cust_info_*.csv"):
        try:
            _, rows = read_csv(prev)
            ids = [int(r[0]) for r in rows if r and r[0].strip().isdigit()]
            if ids:
                next_customer_id = max(next_customer_id, max(ids) + 1)
        except Exception:
            continue
    for prev in DST.glob("sales_details_*.csv"):
        try:
            _, rows = read_csv(prev)
            nums = [
                int(r[0][2:]) for r in rows
                if r and r[0].startswith("SO") and r[0][2:].isdigit()
            ]
            if nums:
                next_order_number = max(next_order_number, max(nums) + 1)
        except Exception:
            continue
    for prev in DST.glob("prd_info_*.csv"):
        try:
            _, rows = read_csv(prev)
            ids = [int(r[0]) for r in rows if r and r[0].strip().isdigit()]
            if ids:
                next_product_id = max(next_product_id, max(ids) + 1)
        except Exception:
            continue

    print(f"Generating events from {start} through {end} (inclusive) into {DST}")

    months_seen: set[date] = set()

    for day in daterange(start, end):
        # --- customers (events only) ---
        new_cust_rows, newly_created, next_customer_id = build_day_customers_event(
            day, fake, rng, cust_header,
            existing_customer_ids, next_customer_id, day1_indexed_by_cst,
        )

        # --- locations (events only) ---
        new_loc_rows = build_day_locations_event(
            day, rng, loc_header,
            existing_cids, newly_created, day1_indexed_by_cid,
        )

        # --- sales (events only) ---
        new_sales_rows, next_order_number = build_day_sales_event(
            day, rng,
            next_order_number, existing_customer_ids, valid_prd_keys,
        )

        date_stamp = day.strftime("%Y_%m_%d")
        write_csv(
            DST / f"cust_info_{date_stamp}.csv",
            cust_header, new_cust_rows,
        )
        write_csv(
            DST / f"loc_a101_{date_stamp}.csv",
            loc_header, new_loc_rows,
        )
        sales_header, _ = read_csv(_find_template("sales_details_"))
        write_csv(
            DST / f"sales_details_{date_stamp}.csv",
            sales_header, new_sales_rows,
        )

        print(
            f"  {day}: "
            f"{len(new_cust_rows)} cust events, "
            f"{len(new_loc_rows)} loc events, "
            f"{len(new_sales_rows)} sales events"
        )

        months_seen.add(day.replace(day=1))

    # --- monthly product-catalog files on demand ---
    if args.monthly:
        for month_start in sorted(months_seen):
            new_prd_rows, next_product_id = build_monthly_products_event(
                month_start, rng, prd_header, prd_day1, next_product_id,
            )
            month_stamp = month_start.strftime("%Y_%m")
            write_csv(
                DST / f"prd_info_{month_stamp}.csv",
                prd_header, new_prd_rows,
            )
            print(
                f"  {month_start.strftime('%Y-%m')}: "
                f"{len(new_prd_rows)} product events "
                f"(written to prd_info_{month_stamp}.csv)"
            )

    print(f"\nDone. Files in: {DST}")


if __name__ == "__main__":
    main()
