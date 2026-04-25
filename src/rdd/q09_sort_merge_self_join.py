"""Q9 (RDD) — Customer × month self-join (shuffle-based join).

The DataFrame/SQL versions force a SortMergeJoin. The RDD equivalent is
a hash-shuffled ``.join()`` between two PairRDDs of (key, value) pairs.
Spark RDD joins co-partition both sides on the join key — analogous to
the Exchange + Sort stages in the DataFrame plan.
"""
from __future__ import annotations

from operator import add

from src.common.spark_session import spark_app
from src.common.timer import timed
from src.rdd._cleaning import load_clean_sales

QUERY_ID = "q09"
API = "rdd"


def run(sc):
    sales = load_clean_sales(sc)
    monthly = (
        sales.map(lambda s: (
            (s.customer_id, s.invoice_date.year * 12 + s.invoice_date.month),
            s.revenue,
        ))
        .reduceByKey(add)
        .cache()
    )
    # Re-key by (customer, month_index) and (customer, month_index + 1).
    curr = monthly.map(lambda kv: ((kv[0][0], kv[0][1]), kv[1]))
    prev = monthly.map(lambda kv: ((kv[0][0], kv[0][1] + 1), kv[1]))
    joined = curr.join(prev)
    # ((customer, month_idx), (curr_spend, prev_spend))
    return joined.map(
        lambda kv: (
            kv[0][0],            # customer_id
            kv[0][1],            # month_index
            kv[1][0],            # curr_spend
            kv[1][1],            # prev_spend
            kv[1][0] - kv[1][1], # delta
        )
    ).sortBy(lambda r: -r[4])


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
