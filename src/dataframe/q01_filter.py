"""Q1 (DataFrame) — Cleaning filter with complex conditions.

Keep only rows that represent successful, non-cancelled, valid sales:
  Quantity > 0, Price > 0, Customer ID is not null,
  Invoice does not start with 'C', InvoiceDate is in [2009-12-01, 2011-12-09].

Demonstrates rubric requirement (1): filtering with complex conditions.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from src.common.cleaning import apply_clean_filter, load_raw
from src.common.spark_session import spark_app
from src.common.timer import timed


QUERY_ID = "q01"
API = "dataframe"


def run(spark: SparkSession) -> DataFrame:
    return apply_clean_filter(load_raw(spark, source="csv"))


def main() -> None:
    with spark_app(f"{QUERY_ID}-{API}") as spark:
        with timed(f"{QUERY_ID}-{API}") as t:
            df = run(spark)
            count = df.count()
        print(f"[{QUERY_ID} {API}] cleaned rows = {count} ({t.elapsed:.2f}s)")
        df.show(5, truncate=False)


if __name__ == "__main__":
    main()
