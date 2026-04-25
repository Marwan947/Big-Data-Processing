"""Q8 (DataFrame) — Country -> Region revenue via broadcast join.

A small in-memory country->region map (~40 rows) is broadcast to every
executor and joined with the (large) cleaned sales table. The physical
plan must contain ``BroadcastHashJoin``.

Demonstrates rubric requirement (7): broadcast join optimization.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

from src.common.cleaning import cleaned_dataframe
from src.common.regions import country_region_pairs
from src.common.spark_session import spark_app
from src.common.timer import timed

QUERY_ID = "q08"
API = "dataframe"


def build_region_df(spark: SparkSession) -> DataFrame:
    rows = country_region_pairs()
    return spark.createDataFrame(rows, schema=["Country", "Region"])


def run(spark: SparkSession) -> DataFrame:
    sales = cleaned_dataframe(spark)
    region_df = build_region_df(spark)
    return (
        sales.join(broadcast(region_df), on="Country", how="left")
        .groupBy("Region")
        .agg(
            F.sum("Revenue").alias("revenue"),
            F.countDistinct("`Customer ID`").alias("distinct_customers"),
        )
        .orderBy(F.desc("revenue"))
    )


def main() -> None:
    with spark_app(f"{QUERY_ID}-{API}") as spark:
        with timed(f"{QUERY_ID}-{API}") as t:
            df = run(spark)
            df.show(truncate=False)
        print(f"[{QUERY_ID} {API}] elapsed = {t.elapsed:.2f}s")


if __name__ == "__main__":
    main()
