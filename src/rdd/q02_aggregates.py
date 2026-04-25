"""Q2 (RDD) — Top-line aggregates.

Showcases multi-aggregate computation in pure RDD form. Each metric is
a separate action, illustrating why the DataFrame API (single multi-agg
call) is much terser.
"""
from __future__ import annotations

from datetime import datetime

from src.common.spark_session import spark_app
from src.common.timer import timed
from src.rdd._cleaning import load_clean_sales

QUERY_ID = "q02"
API = "rdd"


def run(sc) -> dict:
    sales = load_clean_sales(sc).cache()
    revenues = sales.map(lambda s: s.revenue)
    invoices = sales.map(lambda s: s.invoice)
    customers = sales.map(lambda s: s.customer_id)
    dates = sales.map(lambda s: s.invoice_date)
    invoice_totals = sales.map(lambda s: (s.invoice, s.revenue)).reduceByKey(lambda a, b: a + b)

    total_revenue = revenues.sum()
    distinct_invoices = invoices.distinct().count()
    distinct_customers = customers.distinct().count()
    max_line_revenue = revenues.max()
    min_invoice_date = dates.min()
    max_invoice_date = dates.max()
    invoice_total_count = invoice_totals.count()
    invoice_total_sum = invoice_totals.map(lambda kv: kv[1]).sum()
    avg_order_value = invoice_total_sum / invoice_total_count if invoice_total_count else 0.0

    return {
        "total_revenue": total_revenue,
        "distinct_invoices": distinct_invoices,
        "distinct_customers": distinct_customers,
        "max_line_revenue": max_line_revenue,
        "min_invoice_date": min_invoice_date,
        "max_invoice_date": max_invoice_date,
        "avg_order_value": avg_order_value,
    }


def main() -> None:
    with spark_app(f"{QUERY_ID}-{API}") as spark:
        sc = spark.sparkContext
        with timed(f"{QUERY_ID}-{API}") as t:
            stats = run(sc)
        for k, v in stats.items():
            print(f"  {k}: {v}")
        print(f"[{QUERY_ID} {API}] elapsed = {t.elapsed:.2f}s")


if __name__ == "__main__":
    main()
