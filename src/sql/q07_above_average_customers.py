"""Q7 (Spark SQL) — Customers above the global average via scalar subquery."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from src.common.spark_session import spark_app
from src.common.timer import timed
from src.sql._views import register_sales

QUERY_ID = "q07"
API = "sql"


def run(spark: SparkSession) -> DataFrame:
    register_sales(spark)
    return spark.sql(
        """
        WITH customer_totals AS (
            SELECT `Customer ID` AS customer_id,
                   SUM(Revenue)  AS total_spend
            FROM sales
            GROUP BY `Customer ID`
        )
        SELECT customer_id, total_spend,
               (SELECT AVG(total_spend) FROM customer_totals) AS avg_spend_baseline
        FROM customer_totals
        WHERE total_spend > (SELECT AVG(total_spend) FROM customer_totals)
        ORDER BY total_spend DESC
        """
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
