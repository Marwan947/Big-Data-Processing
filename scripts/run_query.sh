#!/usr/bin/env bash
# Run a single query.
#
# Usage:  scripts/run_query.sh <api> <query_id>
#   api      = rdd | dataframe | sql
#   query_id = 01 .. 10
set -euo pipefail
if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <rdd|dataframe|sql> <01..10>" >&2
  exit 1
fi
cd "$(dirname "$0")/.."
api="$1"
qid="$2"
script="src/${api}/q${qid}_*.py"
file=$(ls $script 2>/dev/null | head -n 1)
if [ -z "$file" ]; then
  echo "No script matching $script" >&2
  exit 1
fi
module="${file%.py}"
module="${module//\//.}"
python -m "$module"
