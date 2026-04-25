"""Q7 (RDD) — Customers above the global average spend.

The "subquery" form: compute the global average via an action, broadcast
it, then filter customer totals against it.
"""
from __future__ import annotations

from operator import add

from src.common.spark_session import spark_app
from src.common.timer import timed
from src.rdd._cleaning import load_clean_sales

QUERY_ID = "q07"
API = "rdd"


def run(sc):
    sales = load_clean_sales(sc)
    customer_totals = sales.map(lambda s: (s.customer_id, s.revenue)).reduceByKey(add).cache()
    total_spend, count = customer_totals.map(lambda kv: (kv[1], 1)).reduce(
        lambda a, b: (a[0] + b[0], a[1] + b[1])
    )
    avg_spend = total_spend / count if count else 0.0
    bcast = sc.broadcast(avg_spend)
    return customer_totals.filter(lambda kv: kv[1] > bcast.value).map(
        lambda kv: (kv[0], kv[1], bcast.value)
    )


def main() -> None:
    with spark_app(f"{QUERY_ID}-{API}") as spark:
        sc = spark.sparkContext
        with timed(f"{QUERY_ID}-{API}") as t:
            rdd = run(sc)
            count = rdd.count()
            sample = rdd.takeOrdered(10, key=lambda r: -r[1])
        for row in sample:
            print(row)
        print(f"[{QUERY_ID} {API}] above-avg customers = {count} ({t.elapsed:.2f}s)")


if __name__ == "__main__":
    main()
