"""Q9 (Spark SQL) — Customer × month self-join (sort-merge)."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from src.common.spark_session import spark_app
from src.common.timer import timed
from src.sql._views import register_sales

QUERY_ID = "q09"
API = "sql"


def run(spark: SparkSession) -> DataFrame:
    register_sales(spark)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW monthly AS
        SELECT `Customer ID`                       AS customer_id,
               year(InvoiceDate)*12 + month(InvoiceDate) AS month_index,
               SUM(Revenue)                        AS month_spend
        FROM sales
        GROUP BY `Customer ID`, year(InvoiceDate)*12 + month(InvoiceDate)
        """
    )
    return spark.sql(
        """
        SELECT curr.customer_id,
               curr.month_index,
               curr.month_spend,
               prev.month_spend AS prev_month_spend,
               (curr.month_spend - prev.month_spend) AS delta
        FROM monthly curr
        JOIN monthly prev
          ON curr.customer_id = prev.customer_id
         AND curr.month_index = prev.month_index + 1
        ORDER BY delta DESC
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
