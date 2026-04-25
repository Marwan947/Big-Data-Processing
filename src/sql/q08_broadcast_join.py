"""Q8 (Spark SQL) — Country -> Region revenue via broadcast join hint."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from src.common.spark_session import spark_app
from src.common.timer import timed
from src.sql._views import register_regions, register_sales

QUERY_ID = "q08"
API = "sql"


def run(spark: SparkSession) -> DataFrame:
    register_sales(spark)
    register_regions(spark)
    return spark.sql(
        """
        SELECT /*+ BROADCAST(country_region) */
            country_region.Region                AS Region,
            SUM(sales.Revenue)                   AS revenue,
            COUNT(DISTINCT sales.`Customer ID`)  AS distinct_customers
        FROM sales
        LEFT JOIN country_region
          ON sales.Country = country_region.Country
        GROUP BY country_region.Region
        ORDER BY revenue DESC
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
