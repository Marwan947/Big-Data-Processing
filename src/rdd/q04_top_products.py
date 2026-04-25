"""Q4 (RDD) — Top-10 products by revenue."""
from __future__ import annotations

from operator import add

from src.common.spark_session import spark_app
from src.common.timer import timed
from src.rdd._cleaning import load_clean_sales

QUERY_ID = "q04"
API = "rdd"


def run(sc):
    sales = load_clean_sales(sc)
    revenue = sales.map(lambda s: ((s.stock_code, s.description), s.revenue)).reduceByKey(add)
    units = sales.map(lambda s: ((s.stock_code, s.description), s.quantity)).reduceByKey(add)
    joined = revenue.join(units)
    # ((code, desc), (revenue, units))
    return (
        joined.map(lambda kv: (kv[0][0], kv[0][1], kv[1][0], kv[1][1]))
        .takeOrdered(10, key=lambda r: -r[2])
    )


def main() -> None:
    with spark_app(f"{QUERY_ID}-{API}") as spark:
        sc = spark.sparkContext
        with timed(f"{QUERY_ID}-{API}") as t:
            top = run(sc)
        for row in top:
            print(row)
        print(f"[{QUERY_ID} {API}] elapsed = {t.elapsed:.2f}s")


if __name__ == "__main__":
    main()
