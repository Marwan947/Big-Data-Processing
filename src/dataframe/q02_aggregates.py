"""Q2 (DataFrame) — Top-line aggregates.

Single-row summary of the cleaned data:
  total revenue, distinct invoices, distinct customers, avg order value,
  max single-line revenue, min/max InvoiceDate.

Demonstrates rubric requirement (2): SUM, AVG, COUNT, MAX/MIN.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.cleaning import cleaned_dataframe
from src.common.spark_session import spark_app
from src.common.timer import timed

QUERY_ID = "q02"
API = "dataframe"


def run(spark: SparkSession) -> DataFrame:
    df = cleaned_dataframe(spark)
    invoice_totals = df.groupBy("Invoice").agg(F.sum("Revenue").alias("invoice_total"))
    avg_order_value = invoice_totals.agg(
        F.avg("invoice_total").alias("avg_order_value")
    )
    summary = df.agg(
        F.sum("Revenue").alias("total_revenue"),
        F.countDistinct("Invoice").alias("distinct_invoices"),
        F.countDistinct("`Customer ID`").alias("distinct_customers"),
        F.max("Revenue").alias("max_line_revenue"),
        F.min("InvoiceDate").alias("min_invoice_date"),
        F.max("InvoiceDate").alias("max_invoice_date"),
    )
    return summary.crossJoin(avg_order_value)


def main() -> None:
    with spark_app(f"{QUERY_ID}-{API}") as spark:
        with timed(f"{QUERY_ID}-{API}") as t:
            df = run(spark)
            df.show(truncate=False)
        print(f"[{QUERY_ID} {API}] elapsed = {t.elapsed:.2f}s")


if __name__ == "__main__":
    main()
