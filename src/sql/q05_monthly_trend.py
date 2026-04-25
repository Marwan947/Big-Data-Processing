"""Q5 (Spark SQL) — Monthly cumulative revenue + 3-month trailing moving avg."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from src.common.spark_session import spark_app
from src.common.timer import timed
from src.sql._views import register_sales

QUERY_ID = "q05"
API = "sql"


def run(spark: SparkSession) -> DataFrame:
    register_sales(spark)
    return spark.sql(
        """
        WITH monthly AS (
            SELECT year(InvoiceDate)  AS Year,
                   month(InvoiceDate) AS Month,
                   SUM(Revenue)       AS revenue
            FROM sales
            GROUP BY year(InvoiceDate), month(InvoiceDate)
        )
        SELECT
            Year, Month, revenue,
            SUM(revenue) OVER (
                ORDER BY Year, Month
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cumulative_revenue,
            AVG(revenue) OVER (
                ORDER BY Year, Month
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            ) AS moving_avg_3mo
        FROM monthly
        ORDER BY Year, Month
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
