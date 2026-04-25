"""Q8 (RDD) — Country -> Region revenue via broadcast lookup.

The RDD-equivalent of a broadcast join: build a small dict on the
driver, broadcast it to every executor, then map each sale's country
to a region in O(1). Reduces a multi-million row "join" to a single
map step with no shuffle.
"""
from __future__ import annotations

from operator import add

from src.common.regions import country_region_pairs
from src.common.spark_session import spark_app
from src.common.timer import timed
from src.rdd._cleaning import load_clean_sales

QUERY_ID = "q08"
API = "rdd"


def run(sc):
    region_map = dict(country_region_pairs())
    bcast = sc.broadcast(region_map)
    sales = load_clean_sales(sc)
    keyed = sales.map(
        lambda s: (bcast.value.get(s.country, "Unknown"), (s.revenue, s.customer_id))
    )
    revenue_by_region = keyed.map(lambda kv: (kv[0], kv[1][0])).reduceByKey(add)
    customers_by_region = (
        keyed.map(lambda kv: ((kv[0], kv[1][1]), 1))
        .reduceByKey(add)
        .map(lambda kv: (kv[0][0], 1))
        .reduceByKey(add)
    )
    return (
        revenue_by_region.join(customers_by_region)
        .map(lambda kv: (kv[0], kv[1][0], kv[1][1]))
        .sortBy(lambda r: -r[1])
    )


def main() -> None:
    with spark_app(f"{QUERY_ID}-{API}") as spark:
        sc = spark.sparkContext
        with timed(f"{QUERY_ID}-{API}") as t:
            rdd = run(sc)
            rows = rdd.collect()
        for row in rows:
            print(row)
        print(f"[{QUERY_ID} {API}] elapsed = {t.elapsed:.2f}s")


if __name__ == "__main__":
    main()
