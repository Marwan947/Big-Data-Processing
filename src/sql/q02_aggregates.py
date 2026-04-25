"""Q2 (Spark SQL) — Top-line aggregates."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from src.common.spark_session import spark_app
from src.common.timer import timed
from src.sql._views import register_sales

QUERY_ID = "q02"
API = "sql"


def run(spark: SparkSession) -> DataFrame:
    register_sales(spark)
    return spark.sql(
        """
        WITH invoice_totals AS (
            SELECT Invoice, SUM(Revenue) AS invoice_total
            FROM sales
            GROUP BY Invoice
        )
        SELECT
            (SELECT SUM(Revenue) FROM sales)                       AS total_revenue,
            (SELECT COUNT(DISTINCT Invoice) FROM sales)            AS distinct_invoices,
            (SELECT COUNT(DISTINCT `Customer ID`) FROM sales)      AS distinct_customers,
            (SELECT MAX(Revenue) FROM sales)                       AS max_line_revenue,
            (SELECT MIN(InvoiceDate) FROM sales)                   AS min_invoice_date,
            (SELECT MAX(InvoiceDate) FROM sales)                   AS max_invoice_date,
            (SELECT AVG(invoice_total) FROM invoice_totals)        AS avg_order_value
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
