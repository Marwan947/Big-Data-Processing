"""Q5 (RDD) — Monthly cumulative revenue + 3-month trailing moving avg.

There is no direct RDD equivalent of a window function. We:

1. Aggregate revenue per (year, month) on the cluster.
2. ``collect()`` the ~25 monthly rows (negligible).
3. Compute cumulative sum and trailing 3-month average in the driver.
4. ``parallelize`` the result back into an RDD for symmetry with the
   DataFrame and SQL versions.

The whole point of this query is to *show* the verbosity that the
DataFrame Window API replaces.
"""
from __future__ import annotations

from operator import add

from src.common.spark_session import spark_app
from src.common.timer import timed
from src.rdd._cleaning import load_clean_sales

QUERY_ID = "q05"
API = "rdd"


def run(sc):
    sales = load_clean_sales(sc)
    monthly = (
        sales.map(lambda s: ((s.invoice_date.year, s.invoice_date.month), s.revenue))
        .reduceByKey(add)
        .sortByKey()
        .collect()
    )
    rows = []
    cumulative = 0.0
    for i, ((y, m), rev) in enumerate(monthly):
        cumulative += rev
        window_slice = [r for _, r in monthly[max(0, i - 2): i + 1]]
        moving_avg = sum(window_slice) / len(window_slice)
        rows.append((y, m, rev, cumulative, moving_avg))
    return sc.parallelize(rows)


def main() -> None:
    with spark_app(f"{QUERY_ID}-{API}") as spark:
        sc = spark.sparkContext
        with timed(f"{QUERY_ID}-{API}") as t:
            rdd = run(sc)
            collected = rdd.collect()
        for row in collected[:30]:
            print(row)
        print(f"[{QUERY_ID} {API}] elapsed = {t.elapsed:.2f}s")


if __name__ == "__main__":
    main()
