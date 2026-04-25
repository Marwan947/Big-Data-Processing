"""Run every (query, api) pair N times and write per-run metrics to
``outputs/metrics/performance.csv``.

Each query is run in its own SparkSession so caches and broadcasted
state don't leak between queries.

Usage:
    python -m src.benchmark.run_all                # 3 runs (default)
    python -m src.benchmark.run_all --runs 1       # smoke
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

from pyspark.sql import DataFrame

from src.benchmark._registry import API_NAMES, QUERY_IDS, load_run
from src.common.schema import METRICS_DIR
from src.common.spark_session import EVENT_LOG_DIR, spark_app
from src.common.timer import append_metric, read_event_log_metrics, timed

PERFORMANCE_CSV = METRICS_DIR / "performance.csv"


def _materialize_result(api: str, result) -> int:
    """Force evaluation and return a row count for the result.

    Q2 (DataFrame/SQL) is a single-row summary; Q2 (RDD) returns a dict.
    All other queries return DataFrames or RDDs that support count() or
    are already collected lists.
    """
    if api == "rdd":
        if isinstance(result, dict):
            return 1
        if isinstance(result, list):
            return len(result)
        if hasattr(result, "count"):
            return result.count()
        return -1
    if isinstance(result, DataFrame):
        return result.count()
    return -1


def _run_one(qid: str, api: str, run_index: int) -> dict:
    run_fn = load_run(api, qid)
    app_id = ""
    with spark_app(f"Bench-{qid}-{api}-{run_index}") as spark:
        app_id = spark.sparkContext.applicationId
        with timed(f"{qid}-{api}-{run_index}") as t:
            ctx = spark.sparkContext if api == "rdd" else spark
            result = run_fn(ctx) if api == "rdd" else run_fn(spark)
            row_count = _materialize_result(api, result)
    # Event log is fully written only after the SparkContext stops, so read
    # it after the with-block exits.
    metrics = read_event_log_metrics(EVENT_LOG_DIR, app_id)
    return {
        "query_id": qid,
        "api": api,
        "run_index": run_index,
        "runtime_seconds": f"{t.elapsed:.4f}",
        "result_rows": row_count,
        "shuffle_read_bytes": metrics["shuffle_read_bytes"],
        "shuffle_write_bytes": metrics["shuffle_write_bytes"],
        "peak_memory_bytes": metrics["peak_memory_bytes"],
        "notes": "",
    }


def run(qids: Iterable[str], apis: Iterable[str], runs: int) -> None:
    if PERFORMANCE_CSV.exists():
        PERFORMANCE_CSV.unlink()
    for qid in qids:
        for api in apis:
            for r in range(1, runs + 1):
                row = _run_one(qid, api, r)
                append_metric(PERFORMANCE_CSV, row)
                print(
                    f"  {qid}/{api} run {r}: "
                    f"{row['runtime_seconds']}s rows={row['result_rows']} "
                    f"shuffle_w={row['shuffle_write_bytes']}B "
                    f"peak_mem={row['peak_memory_bytes']}B"
                )


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--queries", default="all")
    p.add_argument("--apis", default="all")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    qids = QUERY_IDS if args.queries == "all" else args.queries.split(",")
    apis = API_NAMES if args.apis == "all" else args.apis.split(",")
    print(
        f"Benchmark: {len(qids)} queries × {len(apis)} APIs × {args.runs} runs "
        f"= {len(qids) * len(apis) * args.runs} runs"
    )
    run(qids, apis, args.runs)
    print(f"\nWrote {PERFORMANCE_CSV}")


if __name__ == "__main__":
    main()
