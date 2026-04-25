"""Q6 (Spark SQL) — Top-5 customers per country by lifetime spend."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from src.common.spark_session import spark_app
from src.common.timer import timed
from src.sql._views import register_sales

QUERY_ID = "q06"
API = "sql"


def run(spark: SparkSession) -> DataFrame:
    register_sales(spark)
    return spark.sql(
        """
        WITH customer_totals AS (
            SELECT Country,
                   `Customer ID` AS customer_id,
                   SUM(Revenue)  AS total_spend
            FROM sales
            GROUP BY Country, `Customer ID`
        ),
        ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY Country
                       ORDER BY total_spend DESC
                   ) AS rank_in_country
            FROM customer_totals
        )
        SELECT *
        FROM ranked
        WHERE rank_in_country <= 5
        ORDER BY Country, rank_in_country
        """
    )


def main() -> None:
    with spark_app(f"{QUERY_ID}-{API}") as spark:
        with timed(f"{QUERY_ID}-{API}") as t:
            df = run(spark)
            df.show(30, truncate=False)
        print(f"[{QUERY_ID} {API}] elapsed = {t.elapsed:.2f}s")


if __name__ == "__main__":
    main()
