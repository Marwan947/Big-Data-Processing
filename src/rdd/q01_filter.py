"""Q1 (RDD) — Cleaning filter."""
from __future__ import annotations

from src.common.spark_session import spark_app
from src.common.timer import timed
from src.rdd._cleaning import load_clean_sales

QUERY_ID = "q01"
API = "rdd"


def run(sc):
    return load_clean_sales(sc)


def main() -> None:
    with spark_app(f"{QUERY_ID}-{API}") as spark:
        sc = spark.sparkContext
        with timed(f"{QUERY_ID}-{API}") as t:
            sales = run(sc)
            count = sales.count()
        sample = sales.take(5)
        for s in sample:
            print(s)
        print(f"[{QUERY_ID} {API}] cleaned rows = {count} ({t.elapsed:.2f}s)")


if __name__ == "__main__":
    main()
