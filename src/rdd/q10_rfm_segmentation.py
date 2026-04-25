"""Q10 (RDD) — RFM segmentation with manual quintile bucketing.

NTILE has no native RDD equivalent. We:

1. Compute (recency_days, frequency, monetary) per customer with RDDs.
2. Sort by each axis and assign quintile ranks based on the sorted index.
3. Apply the same rule-based segmentation as the DataFrame/SQL versions.
"""
from __future__ import annotations

from operator import add

from src.common.spark_session import spark_app
from src.common.timer import timed
from src.rdd._cleaning import load_clean_sales

QUERY_ID = "q10"
API = "rdd"


def _quintile_assign(values, key, descending: bool):
    sorted_vals = sorted(values, key=lambda v: (-v[key]) if descending else v[key])
    n = len(sorted_vals)
    bucket_size = n / 5.0  # five buckets
    out = {}
    for i, v in enumerate(sorted_vals):
        bucket = min(5, int(i // bucket_size) + 1)  # 1..5, smallest index = best
        out[v["customer_id"]] = 6 - bucket  # invert so 5 = best
    return out


def _segment(r, f, m):
    if r >= 4 and f >= 4 and m >= 4:
        return "Champions"
    if r >= 3 and f >= 3 and m >= 3:
        return "Loyal"
    if r <= 2 and f >= 3:
        return "At-Risk"
    if r <= 2 and f <= 2:
        return "Lost"
    return "Other"


def run(sc):
    sales = load_clean_sales(sc).cache()
    # Reference date = max invoice date.
    ref_date = sales.map(lambda s: s.invoice_date).max()
    rfm = (
        sales.map(lambda s: (s.customer_id, (s.invoice_date, s.invoice, s.revenue)))
        .groupByKey()
        .mapValues(lambda triples: {
            "recency_days": (ref_date - max(t[0] for t in triples)).days,
            "frequency": len({t[1] for t in triples}),
            "monetary": sum(t[2] for t in triples),
        })
        .map(lambda kv: {
            "customer_id": kv[0],
            "recency_days": kv[1]["recency_days"],
            "frequency": kv[1]["frequency"],
            "monetary": kv[1]["monetary"],
        })
        .collect()
    )
    # Quintile assignment in driver (one-shot ~5 thousand customers).
    r_score = _quintile_assign(rfm, "recency_days", descending=False)
    f_score = _quintile_assign(rfm, "frequency", descending=True)
    m_score = _quintile_assign(rfm, "monetary", descending=True)
    rows = []
    for v in rfm:
        cid = v["customer_id"]
        R, F, M = r_score[cid], f_score[cid], m_score[cid]
        rows.append((cid, v["recency_days"], v["frequency"], v["monetary"], R, F, M, _segment(R, F, M)))
    return sc.parallelize(rows)


def main() -> None:
    with spark_app(f"{QUERY_ID}-{API}") as spark:
        sc = spark.sparkContext
        with timed(f"{QUERY_ID}-{API}") as t:
            rdd = run(sc).cache()
            counts = rdd.map(lambda r: (r[7], 1)).reduceByKey(add).sortBy(lambda kv: -kv[1]).collect()
        print("--- segment summary ---")
        for seg, n in counts:
            print(f"  {seg}: {n}")
        print(f"[{QUERY_ID} {API}] elapsed = {t.elapsed:.2f}s")


if __name__ == "__main__":
    main()
