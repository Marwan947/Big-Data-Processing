"""Partition pruning demo — verifies that ``WHERE Year = 2010`` against
the year-partitioned Parquet only reads one partition directory.

Captures:
  * the physical plan's ``PartitionFilters`` line
  * the runtime delta between the pruned read and the full read
"""
from __future__ import annotations

import io
import json
import sys
from pathlib import Path

from src.common.schema import METRICS_DIR, PARQUET_PATH
from src.common.spark_session import spark_app
from src.common.timer import timed


def _explain(df) -> str:
    buf = io.StringIO()
    saved = sys.stdout
    sys.stdout = buf
    try:
        df.explain(mode="extended")
    finally:
        sys.stdout = saved
    return buf.getvalue()


def main() -> None:
    results = {}
    with spark_app("Optimization-PartitionPruning") as spark:
        full = spark.read.parquet(PARQUET_PATH)

        with timed("full_scan") as full_t:
            full_count = full.count()
        results["full_scan_seconds"] = full_t.elapsed
        results["full_scan_rows"] = full_count

        pruned = spark.read.parquet(PARQUET_PATH).filter("Year = 2010")
        with timed("pruned_scan") as pruned_t:
            pruned_count = pruned.count()
        results["pruned_scan_seconds"] = pruned_t.elapsed
        results["pruned_scan_rows"] = pruned_count
        results["speedup"] = (
            full_t.elapsed / pruned_t.elapsed if pruned_t.elapsed else float("inf")
        )

        plan = _explain(pruned)
        partition_filter_lines = [
            line.strip() for line in plan.splitlines() if "PartitionFilters" in line
        ]
        results["partition_filter_lines"] = partition_filter_lines

    out = METRICS_DIR / "partition_pruning_demo.json"
    out.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(json.dumps(results, indent=2))
    print(f"\nWrote {out}")


if __name__ == "__main__":
    main()
