#!/usr/bin/env bash
# Run every query 3 times across all three APIs and write performance.csv.
set -euo pipefail
cd "$(dirname "$0")/.."
python -m src.benchmark.run_all "$@"
