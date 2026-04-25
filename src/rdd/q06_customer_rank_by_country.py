"""Q6 (RDD) — Top-5 customers per country by lifetime spend.

Per-country ranking via groupByKey + Python-side sort. ``groupByKey``
materializes all customers per country in one partition, which is
fine on this dataset (max country size ~ 4M rows, but the *grouped*
RDD is keyed by country with iterables of (customer, total) pairs).
"""
from __future__ import annotations

from operator import add

from src.common.spark_session import spark_app
from src.common.timer import timed
from src.rdd._cleaning import load_clean_sales

QUERY_ID = "q06"
API = "rdd"


def run(sc):
    sales = load_clean_sales(sc)
    per_customer = sales.map(
        lambda s: ((s.country, s.customer_id), s.revenue)
    ).reduceByKey(add)
    # Re-key by country for grouping.
    keyed = per_customer.map(lambda kv: (kv[0][0], (kv[0][1], kv[1])))
    grouped = keyed.groupByKey()

    def top5(values):
        return sorted(values, key=lambda cv: -cv[1])[:5]

    return grouped.flatMapValues(top5).map(
        lambda kv: (kv[0], kv[1][0], kv[1][1])  # (country, customer_id, total_spend)
    )


def main() -> None:
    with spark_app(f"{QUERY_ID}-{API}") as spark:
        sc = spark.sparkContext
        with timed(f"{QUERY_ID}-{API}") as t:
            rdd = run(sc)
            sample = rdd.takeOrdered(30, key=lambda r: (r[0], -r[2]))
        for row in sample:
            print(row)
        print(f"[{QUERY_ID} {API}] elapsed = {t.elapsed:.2f}s")


if __name__ == "__main__":
    main()
