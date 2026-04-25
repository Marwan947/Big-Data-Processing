"""Caching demo — measures the speedup from ``.cache()``.

Compute the per-customer revenue aggregate, then run two downstream
queries (Q6 ranking + Q7 above-average filter) twice each: once cold
(no cache) and once warm (with ``.cache()`` on the shared aggregate).
"""
from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import functions as F

from src.common.cleaning import cleaned_dataframe
from src.common.schema import METRICS_DIR
from src.common.spark_session import spark_app
from src.common.timer import timed


def _customer_totals(df):
    return df.groupBy("`Customer ID`").agg(F.sum("Revenue").alias("total_spend"))


def main() -> None:
    results = {}
    with spark_app("Optimization-Caching") as spark:
        df = cleaned_dataframe(spark)

        # Cold run: build totals, query A and B from scratch each time.
        with timed("cold_round_trip") as cold_t:
            totals_cold = _customer_totals(df)
            top_country = (
                totals_cold.orderBy(F.desc("total_spend")).limit(10).count()
            )
            avg_spend = totals_cold.agg(F.avg("total_spend")).collect()[0][0]
            above_avg = totals_cold.filter(
                F.col("total_spend") > F.lit(avg_spend)
            ).count()
        results["cold_seconds"] = cold_t.elapsed
        results["cold_top_country"] = top_country
        results["cold_above_avg"] = above_avg

        # Warm run: cache the shared aggregate, then run both queries.
        totals_warm = _customer_totals(df).cache()
        totals_warm.count()  # materialize the cache

        with timed("warm_round_trip") as warm_t:
            top_country = (
                totals_warm.orderBy(F.desc("total_spend")).limit(10).count()
            )
            avg_spend = totals_warm.agg(F.avg("total_spend")).collect()[0][0]
            above_avg = totals_warm.filter(
                F.col("total_spend") > F.lit(avg_spend)
            ).count()
        results["warm_seconds"] = warm_t.elapsed
        results["speedup"] = (
            cold_t.elapsed / warm_t.elapsed if warm_t.elapsed else float("inf")
        )

    out = METRICS_DIR / "caching_demo.json"
    out.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(json.dumps(results, indent=2))
    print(f"\nWrote {out}")


if __name__ == "__main__":
    main()
