"""Q9 (DataFrame) — Customer × month self-join (sort-merge).

For each customer compute monthly spend, then self-join the result on
``customer_id`` and ``month_index = month_index_other - 1`` to compute
month-over-month spend deltas. Both sides are large; neither is
broadcast-eligible. The physical plan must contain ``SortMergeJoin``
with ``Exchange hashpartitioning`` shuffle stages.

Demonstrates rubric requirement (7): sort-merge join optimization.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.cleaning import cleaned_dataframe
from src.common.spark_session import spark_app
from src.common.timer import timed

QUERY_ID = "q09"
API = "dataframe"


def run(spark: SparkSession) -> DataFrame:
    df = cleaned_dataframe(spark).withColumn(
        "month_index",
        F.year("InvoiceDate") * F.lit(12) + F.month("InvoiceDate"),
    )
    monthly = (
        df.groupBy(F.col("`Customer ID`").alias("customer_id"), "month_index")
        .agg(F.sum("Revenue").alias("month_spend"))
    )
    # Suppress auto-broadcast so we always observe a SortMergeJoin.
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    prev = monthly.alias("prev")
    curr = monthly.alias("curr")
    joined = curr.join(
        prev,
        (F.col("curr.customer_id") == F.col("prev.customer_id"))
        & (F.col("curr.month_index") == F.col("prev.month_index") + F.lit(1)),
        how="inner",
    )
    return joined.select(
        F.col("curr.customer_id").alias("customer_id"),
        F.col("curr.month_index").alias("month_index"),
        F.col("curr.month_spend").alias("month_spend"),
        F.col("prev.month_spend").alias("prev_month_spend"),
        (F.col("curr.month_spend") - F.col("prev.month_spend")).alias("delta"),
    ).orderBy(F.desc("delta"))


def main() -> None:
    with spark_app(f"{QUERY_ID}-{API}") as spark:
        with timed(f"{QUERY_ID}-{API}") as t:
            df = run(spark)
            df.show(10, truncate=False)
        print(f"[{QUERY_ID} {API}] elapsed = {t.elapsed:.2f}s")


if __name__ == "__main__":
    main()
