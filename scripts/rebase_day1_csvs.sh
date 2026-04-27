#!/usr/bin/env bash
# Rebase the day-1 landing CSVs to (today - 3).
#
# What this does:
#   1. Finds the 4 template CSVs in data/landing/ by their filename prefix
#      (cust_info_, loc_a101_, sales_details_, prd_info_). The current dates
#      in their names don't matter — whatever's there is treated as the
#      template and rebased to today - 3.
#   2. Renames each file:
#        cust_info_<OLD>.csv      → cust_info_<YYYY_MM_DD>.csv         (today-3)
#        loc_a101_<OLD>.csv       → loc_a101_<YYYY_MM_DD>.csv          (today-3)
#        sales_details_<OLD>.csv  → sales_details_<YYYY_MM_DD>.csv     (today-3)
#        prd_info_<OLD>.csv       → prd_info_<YYYY_MM>.csv             (1st of today-3's month)
#   3. Rewrites the snapshot_date column (daily files) or snapshot_month
#      column (monthly file) in every row to match.
#
# Idempotent: safe to re-run. If today-3 is already the date in the files,
# the script still rewrites them — the result is identical.
#
# Typical use: run this once, at the start of each demo session, before
# enabling landing_file_sensor in Step 2 of the README.
#
# Run:  ./scripts/rebase_day1_csvs.sh

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$HERE"

LANDING="data/landing"

if [[ ! -d "$LANDING" ]]; then
  echo "ERROR: $LANDING does not exist. Run from the repo root." >&2
  exit 1
fi

# Compute target dates.
# macOS BSD date and GNU date differ on -d / -v syntax; try both.
if TARGET_DAILY="$(date -v-3d +%Y-%m-%d 2>/dev/null)"; then
  # BSD date (macOS)
  TARGET_MONTHLY_FILE="$(date -v-3d +%Y_%m)"
  TARGET_MONTHLY_COL="$(date -v-3d +%Y-%m-01)"
else
  # GNU date (Linux)
  TARGET_DAILY="$(date -d '3 days ago' +%Y-%m-%d)"
  TARGET_MONTHLY_FILE="$(date -d '3 days ago' +%Y_%m)"
  TARGET_MONTHLY_COL="$(date -d '3 days ago' +%Y-%m-01)"
fi
TARGET_DAILY_FILE="${TARGET_DAILY//-/_}"  # YYYY_MM_DD (for filenames)

echo "Rebasing day-1 CSVs in $LANDING to today - 3:"
echo "  daily   snapshot_date  = $TARGET_DAILY"
echo "  daily   filename stamp = $TARGET_DAILY_FILE"
echo "  monthly snapshot_month = $TARGET_MONTHLY_COL"
echo "  monthly filename stamp = $TARGET_MONTHLY_FILE"
echo ""

# Find one file for each prefix. If there are multiple (e.g. previous partial
# runs left stragglers), bail loudly — the user should reset first.
find_one() {
  local prefix="$1"
  local suffix="$2"
  local matches
  # shellcheck disable=SC2207
  matches=($(ls "$LANDING"/${prefix}*.csv 2>/dev/null | grep -E "${prefix}[0-9_]+${suffix}\$" || true))
  if [[ ${#matches[@]} -eq 0 ]]; then
    echo "ERROR: no file matching ${prefix}*${suffix} in $LANDING" >&2
    echo "       Run 'make reset-demo' to restore the template CSVs." >&2
    return 1
  fi
  if [[ ${#matches[@]} -gt 1 ]]; then
    echo "ERROR: multiple files match ${prefix}*${suffix}:" >&2
    printf '         %s\n' "${matches[@]}" >&2
    echo "       Run 'make reset-demo' to restore the template CSVs." >&2
    return 1
  fi
  echo "${matches[0]}"
}

CUST_SRC="$(find_one 'cust_info_' '\.csv')"
LOC_SRC="$(find_one 'loc_a101_' '\.csv')"
SALES_SRC="$(find_one 'sales_details_' '\.csv')"
PRD_SRC="$(find_one 'prd_info_' '\.csv')"

CUST_DST="$LANDING/cust_info_${TARGET_DAILY_FILE}.csv"
LOC_DST="$LANDING/loc_a101_${TARGET_DAILY_FILE}.csv"
SALES_DST="$LANDING/sales_details_${TARGET_DAILY_FILE}.csv"
PRD_DST="$LANDING/prd_info_${TARGET_MONTHLY_FILE}.csv"

# Rewrite snapshot_date / snapshot_month in every row. We use python3 inline
# (not sed) because commas inside data fields would break a regex approach,
# and the CSV reader handles quoting correctly. The rewrite also tolerates
# rows that already hold the target date (idempotent).
rewrite_csv() {
  local src="$1"
  local dst="$2"
  local col="$3"          # "snapshot_date" or "snapshot_month"
  local value="$4"        # YYYY-MM-DD (daily) or YYYY-MM-01 (monthly)

  python3 - "$src" "$dst" "$col" "$value" <<'PY'
import csv
import os
import sys
import tempfile

src, dst, col, value = sys.argv[1:5]

with open(src, newline="") as f:
    reader = csv.reader(f)
    header = next(reader)
    if col not in header:
        print(f"ERROR: column {col!r} not found in {src}", file=sys.stderr)
        sys.exit(2)
    idx = header.index(col)
    rows = [header]
    for row in reader:
        if len(row) <= idx:
            # short row — pad so we can write the partition column
            row = row + [""] * (idx + 1 - len(row))
        row[idx] = value
        rows.append(row)

# Write to a tempfile in the destination directory then rename atomically.
dst_dir = os.path.dirname(dst) or "."
fd, tmp_path = tempfile.mkstemp(dir=dst_dir, prefix=".rebase-", suffix=".csv")
try:
    with os.fdopen(fd, "w", newline="") as f:
        w = csv.writer(f)
        w.writerows(rows)
    os.replace(tmp_path, dst)
except Exception:
    try:
        os.unlink(tmp_path)
    except OSError:
        pass
    raise

# If rename was a real move (source and dest differ), delete the original.
if os.path.abspath(src) != os.path.abspath(dst) and os.path.exists(src):
    os.unlink(src)

print(f"  {os.path.basename(src)} -> {os.path.basename(dst)}")
PY
}

rewrite_csv "$CUST_SRC"  "$CUST_DST"  "snapshot_date"  "$TARGET_DAILY"
rewrite_csv "$LOC_SRC"   "$LOC_DST"   "snapshot_date"  "$TARGET_DAILY"
rewrite_csv "$SALES_SRC" "$SALES_DST" "snapshot_date"  "$TARGET_DAILY"
rewrite_csv "$PRD_SRC"   "$PRD_DST"   "snapshot_month" "$TARGET_MONTHLY_COL"

echo ""
echo "Done. Landing folder now contains:"
ls -1 "$LANDING"/*.csv 2>/dev/null | sed 's#^#  #'
