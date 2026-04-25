"""Q10 (Spark SQL) — RFM segmentation using NTILE quintiles."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from src.common.spark_session import spark_app
from src.common.timer import timed
from src.sql._views import register_sales

QUERY_ID = "q10"
API = "sql"


def run(spark: SparkSession) -> DataFrame:
    register_sales(spark)
    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW rfm_raw AS
        SELECT `Customer ID` AS customer_id,
               DATEDIFF((SELECT MAX(InvoiceDate) FROM sales), MAX(InvoiceDate))
                                          AS recency_days,
               COUNT(DISTINCT Invoice)    AS frequency,
               SUM(Revenue)               AS monetary
        FROM sales
        GROUP BY `Customer ID`
        """
    )
    return spark.sql(
        """
        WITH scored AS (
            SELECT customer_id, recency_days, frequency, monetary,
                   6 - NTILE(5) OVER (ORDER BY recency_days ASC) AS R,
                   6 - NTILE(5) OVER (ORDER BY frequency  DESC) AS F,
                   6 - NTILE(5) OVER (ORDER BY monetary   DESC) AS M
            FROM rfm_raw
        )
        SELECT customer_id, recency_days, frequency, monetary, R, F, M,
               CASE
                   WHEN R >= 4 AND F >= 4 AND M >= 4 THEN 'Champions'
                   WHEN R >= 3 AND F >= 3 AND M >= 3 THEN 'Loyal'
                   WHEN R <= 2 AND F >= 3            THEN 'At-Risk'
                   WHEN R <= 2 AND F <= 2            THEN 'Lost'
                   ELSE 'Other'
               END AS segment
        FROM scored
        """
    )


def main() -> None:
    with spark_app(f"{QUERY_ID}-{API}") as spark:
        with timed(f"{QUERY_ID}-{API}") as t:
            df = run(spark)
            df.cache()
            print("--- segment summary ---")
            df.groupBy("segment").count().orderBy("count", ascending=False).show(
                truncate=False
            )
        print(f"[{QUERY_ID} {API}] elapsed = {t.elapsed:.2f}s")


if __name__ == "__main__":
    main()
