"""Q7 (DataFrame) — Customers whose total spend exceeds the global average.

In SQL form this is a scalar subquery; in DataFrame form we materialize
the average via a single-row collect (Catalyst broadcasts a scalar).

Demonstrates rubric requirement (6): subquery / nested aggregate.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.cleaning import cleaned_dataframe
from src.common.spark_session import spark_app
from src.common.timer import timed

QUERY_ID = "q07"
API = "dataframe"


def run(spark: SparkSession) -> DataFrame:
    df = cleaned_dataframe(spark)
    customer_totals = (
        df.groupBy("`Customer ID`")
        .agg(F.sum("Revenue").alias("total_spend"))
    )
    avg_spend_row = customer_totals.agg(
        F.avg("total_spend").alias("avg_spend")
    ).collect()[0]
    avg_spend = float(avg_spend_row["avg_spend"])
    return (
        customer_totals.filter(F.col("total_spend") > F.lit(avg_spend))
        .orderBy(F.desc("total_spend"))
        .withColumn("avg_spend_baseline", F.lit(avg_spend))
    )


def main() -> None:
    with spark_app(f"{QUERY_ID}-{API}") as spark:
        with timed(f"{QUERY_ID}-{API}") as t:
            df = run(spark)
            count = df.count()
            df.show(10, truncate=False)
        print(f"[{QUERY_ID} {API}] above-avg customers = {count} ({t.elapsed:.2f}s)")


if __name__ == "__main__":
    main()
