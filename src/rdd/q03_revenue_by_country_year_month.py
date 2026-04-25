"""Q3 (RDD) — Revenue by Country × Year × Month."""
from __future__ import annotations

from src.common.spark_session import spark_app
from src.common.timer import timed
from src.rdd._cleaning import load_clean_sales

QUERY_ID = "q03"
API = "rdd"


def run(sc):
    sales = load_clean_sales(sc)
    return (
        sales.map(lambda s: (
            (s.country, s.invoice_date.year, s.invoice_date.month),
            (s.revenue, s.invoice),
        ))
        # Aggregate revenue and accumulate distinct invoice set per cell.
        .aggregateByKey(
            (0.0, set()),
            lambda acc, val: (acc[0] + val[0], acc[1] | {val[1]}),
            lambda a, b: (a[0] + b[0], a[1] | b[1]),
        )
        .map(lambda kv: (kv[0], kv[1][0], len(kv[1][1])))
        .sortBy(lambda x: -x[1])
    )


def main() -> None:
    with spark_app(f"{QUERY_ID}-{API}") as spark:
        sc = spark.sparkContext
        with timed(f"{QUERY_ID}-{API}") as t:
            rdd = run(sc)
            sample = rdd.take(10)
        for row in sample:
            print(row)
        print(f"[{QUERY_ID} {API}] elapsed = {t.elapsed:.2f}s")


if __name__ == "__main__":
    main()
