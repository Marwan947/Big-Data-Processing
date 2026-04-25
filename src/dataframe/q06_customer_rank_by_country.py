"""Q6 (DataFrame) — Top-5 customers per country by lifetime spend.

Per-customer total spend, then ROW_NUMBER() partitioned by Country,
ordered by total_spend descending; keep rank <= 5.

Demonstrates rubric requirement (5): window-based ranking.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common.cleaning import cleaned_dataframe
from src.common.spark_session import spark_app
from src.common.timer import timed

QUERY_ID = "q06"
API = "dataframe"


def run(spark: SparkSession) -> DataFrame:
    df = cleaned_dataframe(spark)
    customer_totals = (
        df.groupBy("Country", "`Customer ID`")
        .agg(F.sum("Revenue").alias("total_spend"))
    )
    w = Window.partitionBy("Country").orderBy(F.desc("total_spend"))
    return (
        customer_totals.withColumn("rank_in_country", F.row_number().over(w))
        .filter(F.col("rank_in_country") <= 5)
        .orderBy("Country", "rank_in_country")
    )


def main() -> None:
    with spark_app(f"{QUERY_ID}-{API}") as spark:
        with timed(f"{QUERY_ID}-{API}") as t:
            df = run(spark)
            df.show(30, truncate=False)
        print(f"[{QUERY_ID} {API}] elapsed = {t.elapsed:.2f}s")


if __name__ == "__main__":
    main()
