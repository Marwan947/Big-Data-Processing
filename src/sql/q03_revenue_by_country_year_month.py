"""Q3 (Spark SQL) — Revenue by Country × Year × Month."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from src.common.spark_session import spark_app
from src.common.timer import timed
from src.sql._views import register_sales

QUERY_ID = "q03"
API = "sql"


def run(spark: SparkSession, source: str = "csv") -> DataFrame:
    register_sales(spark, source=source)
    return spark.sql(
        """
        SELECT
            Country,
            year(InvoiceDate)  AS Year,
            month(InvoiceDate) AS Month,
            SUM(Revenue)               AS revenue,
            COUNT(DISTINCT Invoice)    AS order_count
        FROM sales
        GROUP BY Country, year(InvoiceDate), month(InvoiceDate)
        ORDER BY revenue DESC
        """
    )


def main() -> None:
    with spark_app(f"{QUERY_ID}-{API}") as spark:
        with timed(f"{QUERY_ID}-{API}") as t:
            df = run(spark)
            df.show(10, truncate=False)
        print(f"[{QUERY_ID} {API}] elapsed = {t.elapsed:.2f}s")


if __name__ == "__main__":
    main()
