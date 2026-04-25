"""Q4 (Spark SQL) — Top-10 products by revenue."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from src.common.spark_session import spark_app
from src.common.timer import timed
from src.sql._views import register_sales

QUERY_ID = "q04"
API = "sql"


def run(spark: SparkSession) -> DataFrame:
    register_sales(spark)
    return spark.sql(
        """
        SELECT StockCode, Description,
               SUM(Revenue)  AS revenue,
               SUM(Quantity) AS units_sold
        FROM sales
        GROUP BY StockCode, Description
        ORDER BY revenue DESC
        LIMIT 10
        """
    )


def main() -> None:
    with spark_app(f"{QUERY_ID}-{API}") as spark:
        with timed(f"{QUERY_ID}-{API}") as t:
            df = run(spark)
            df.show(truncate=False)
        print(f"[{QUERY_ID} {API}] elapsed = {t.elapsed:.2f}s")


if __name__ == "__main__":
    main()
