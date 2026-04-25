#!/usr/bin/env bash
# Download the Online Retail II dataset from UCI and write it to
# data/raw/online_retail_II.csv. Idempotent: re-running is a no-op
# once the file is in place.
set -euo pipefail
cd "$(dirname "$0")/.."
python -m src.prep.download_data
