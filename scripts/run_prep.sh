#!/usr/bin/env bash
# Convert the raw CSV to year-partitioned Parquet (one-time, ~1 minute).
set -euo pipefail
cd "$(dirname "$0")/.."
python -m src.prep.csv_to_parquet
