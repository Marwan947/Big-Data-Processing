"""Q10 (DataFrame) — RFM segmentation using NTILE quintiles.

Per customer compute Recency (days since last purchase, relative to the
dataset's max date), Frequency (distinct invoice count) and Monetary
(total spend). Bucket each into quintiles via NTILE(5) and classify:

  * Champions: R>=4 AND F>=4 AND M>=4
  * Loyal:     R>=3 AND F>=3 AND M>=3 AND not Champion
  * At-Risk:   R<=2 AND F>=3
  * Lost:      R<=2 AND F<=2
  * Other:     everything else

Demonstrates rubric requirement (5): NTILE window function. This is the
project's headline analytical insight (segment counts + top customers
per segment).
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common.cleaning import cleaned_dataframe
from src.common.spark_session import spark_app
from src.common.timer import timed

QUERY_ID = "q10"
API = "dataframe"


def _segment_expr() -> "F.Column":
    r = F.col("R")
    f_ = F.col("F")
    m = F.col("M")
    return (
        F.when((r >= 4) & (f_ >= 4) & (m >= 4), F.lit("Champions"))
        .when((r >= 3) & (f_ >= 3) & (m >= 3), F.lit("Loyal"))
        .when((r <= 2) & (f_ >= 3), F.lit("At-Risk"))
        .when((r <= 2) & (f_ <= 2), F.lit("Lost"))
        .otherwise(F.lit("Other"))
    )


def run(spark: SparkSession) -> DataFrame:
    df = cleaned_dataframe(spark)
    reference_date = df.agg(F.max("InvoiceDate").alias("ref")).collect()[0]["ref"]

    rfm = (
        df.groupBy(F.col("`Customer ID`").alias("customer_id"))
        .agg(
            F.datediff(F.lit(reference_date), F.max("InvoiceDate")).alias("recency_days"),
            F.countDistinct("Invoice").alias("frequency"),
            F.sum("Revenue").alias("monetary"),
        )
    )
    # NTILE(5): higher recency days = worse, so reverse-rank for R.
    w_r = Window.orderBy(F.col("recency_days").asc())  # smaller = better → tile 5
    w_f = Window.orderBy(F.col("frequency").desc())
    w_m = Window.orderBy(F.col("monetary").desc())
    scored = (
        rfm.withColumn("R", F.lit(6) - F.ntile(5).over(w_r))  # invert so 5 = best
        .withColumn("F", F.lit(6) - F.ntile(5).over(w_f))
        .withColumn("M", F.lit(6) - F.ntile(5).over(w_m))
    )
    # The expressions above invert NTILE so that 5 is "best" for each axis.
    return scored.withColumn("segment", _segment_expr())


def run_segment_summary(spark: SparkSession) -> DataFrame:
    return run(spark).groupBy("segment").agg(
        F.count("*").alias("customer_count"),
        F.round(F.sum("monetary"), 2).alias("segment_revenue"),
    ).orderBy(F.desc("segment_revenue"))


def main() -> None:
    with spark_app(f"{QUERY_ID}-{API}") as spark:
        with timed(f"{QUERY_ID}-{API}") as t:
            scored = run(spark)
            scored.cache()
            print("--- segment summary ---")
            (
                scored.groupBy("segment")
                .agg(F.count("*").alias("customer_count"))
                .orderBy(F.desc("customer_count"))
                .show(truncate=False)
            )
            print("--- top 5 customers per segment by Monetary ---")
            w = Window.partitionBy("segment").orderBy(F.desc("monetary"))
            (
                scored.withColumn("r", F.row_number().over(w))
                .filter(F.col("r") <= 5)
                .orderBy("segment", "r")
                .select("segment", "customer_id", "recency_days", "frequency", "monetary")
                .show(50, truncate=False)
            )
        print(f"[{QUERY_ID} {API}] elapsed = {t.elapsed:.2f}s")


if __name__ == "__main__":
    main()
