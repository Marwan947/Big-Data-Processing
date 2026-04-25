"""Q3 (DataFrame) — Revenue by Country × Year × Month.

Group by three attributes; output revenue and order count per cell.

Demonstrates rubric requirement (3): grouping by multiple attributes.
Also reused for the CSV-vs-Parquet and partition-pruning demos.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.cleaning import cleaned_dataframe
from src.common.spark_session import spark_app
from src.common.timer import timed

QUERY_ID = "q03"
API = "dataframe"


def run(spark: SparkSession, source: str = "csv") -> DataFrame:
    df = cleaned_dataframe(spark, source=source)
    return (
        df.withColumn("Year", F.year("InvoiceDate"))
        .withColumn("Month", F.month("InvoiceDate"))
        .groupBy("Country", "Year", "Month")
        .agg(
            F.sum("Revenue").alias("revenue"),
            F.countDistinct("Invoice").alias("order_count"),
        )
        .orderBy(F.desc("revenue"))
    )


def main() -> None:
    with spark_app(f"{QUERY_ID}-{API}") as spark:
        with timed(f"{QUERY_ID}-{API}") as t:
            df = run(spark)
            df.show(10, truncate=False)
        print(f"[{QUERY_ID} {API}] elapsed = {t.elapsed:.2f}s")


if __name__ == "__main__":
    main()
