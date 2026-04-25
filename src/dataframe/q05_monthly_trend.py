"""Q5 (DataFrame) — Monthly cumulative revenue + 3-month trailing moving avg.

Aggregate revenue per (year, month), then a window over the time axis to
compute cumulative revenue and a trailing 3-month moving average.

Demonstrates rubric requirement (5): window functions — cumulative sum
and moving average.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common.cleaning import cleaned_dataframe
from src.common.spark_session import spark_app
from src.common.timer import timed

QUERY_ID = "q05"
API = "dataframe"


def run(spark: SparkSession) -> DataFrame:
    df = cleaned_dataframe(spark)
    monthly = (
        df.groupBy(
            F.year("InvoiceDate").alias("Year"),
            F.month("InvoiceDate").alias("Month"),
        )
        .agg(F.sum("Revenue").alias("revenue"))
    )
    w_cumulative = (
        Window.orderBy("Year", "Month")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    w_moving = Window.orderBy("Year", "Month").rowsBetween(-2, 0)
    return monthly.withColumn(
        "cumulative_revenue", F.sum("revenue").over(w_cumulative)
    ).withColumn(
        "moving_avg_3mo", F.avg("revenue").over(w_moving)
    ).orderBy("Year", "Month")


def main() -> None:
    with spark_app(f"{QUERY_ID}-{API}") as spark:
        with timed(f"{QUERY_ID}-{API}") as t:
            df = run(spark)
            df.show(30, truncate=False)
        print(f"[{QUERY_ID} {API}] elapsed = {t.elapsed:.2f}s")


if __name__ == "__main__":
    main()
