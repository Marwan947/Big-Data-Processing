"""Q1 (Spark SQL) — Cleaning filter."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from src.common.cleaning import CLEAN_SQL_PREDICATE
from src.common.schema import RAW_CSV_PATH, RAW_SCHEMA, CSV_OPTIONS
from src.common.spark_session import spark_app
from src.common.timer import timed

QUERY_ID = "q01"
API = "sql"


def run(spark: SparkSession) -> DataFrame:
    reader = spark.read
    for k, v in CSV_OPTIONS.items():
        reader = reader.option(k, v)
    reader.schema(RAW_SCHEMA).csv(RAW_CSV_PATH).createOrReplaceTempView("raw")
    return spark.sql(
        f"""
        SELECT *, Quantity * Price AS Revenue
        FROM raw
        WHERE {CLEAN_SQL_PREDICATE}
        """
    )


def main() -> None:
    with spark_app(f"{QUERY_ID}-{API}") as spark:
        with timed(f"{QUERY_ID}-{API}") as t:
            df = run(spark)
            count = df.count()
        print(f"[{QUERY_ID} {API}] cleaned rows = {count} ({t.elapsed:.2f}s)")
        df.show(5, truncate=False)


if __name__ == "__main__":
    main()
