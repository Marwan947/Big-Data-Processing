"""Q4 (DataFrame) — Top-10 products by revenue.

Group by (StockCode, Description), sum revenue, sort descending, limit 10.

Demonstrates rubric requirement (4): sorting and ranking.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.cleaning import cleaned_dataframe
from src.common.spark_session import spark_app
from src.common.timer import timed

QUERY_ID = "q04"
API = "dataframe"
TOP_N = 10


def run(spark: SparkSession) -> DataFrame:
    df = cleaned_dataframe(spark)
    return (
        df.groupBy("StockCode", "Description")
        .agg(
            F.sum("Revenue").alias("revenue"),
            F.sum("Quantity").alias("units_sold"),
        )
        .orderBy(F.desc("revenue"))
        .limit(TOP_N)
    )


def main() -> None:
    with spark_app(f"{QUERY_ID}-{API}") as spark:
        with timed(f"{QUERY_ID}-{API}") as t:
            df = run(spark)
            df.show(TOP_N, truncate=False)
        print(f"[{QUERY_ID} {API}] elapsed = {t.elapsed:.2f}s")


if __name__ == "__main__":
    main()
