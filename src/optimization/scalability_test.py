"""Scalability test — vary ``spark.master`` and ``spark.sql.shuffle.partitions``,
re-run Q5 (window) and Q9 (sort-merge join), record runtime per combination.

This stands in for the rubric's "different cluster sizes" requirement
since we can't spin up real Spark workers; varying the local parallelism
is the closest single-machine approximation.
"""
from __future__ import annotations

import json

from src.common.schema import METRICS_DIR
from src.common.spark_session import spark_app
from src.common.timer import timed
from src.dataframe.q05_monthly_trend import run as run_q5
from src.dataframe.q09_sort_merge_self_join import run as run_q9

MASTERS = ["local[1]", "local[2]", "local[*]"]
SHUFFLE_PARTITIONS = [4, 8, 50, 200]


def main() -> None:
    rows = []
    for master in MASTERS:
        for parts in SHUFFLE_PARTITIONS:
            for qname, run_fn in (("q05", run_q5), ("q09", run_q9)):
                with spark_app(
                    f"Scalability-{qname}-{master}-{parts}",
                    master=master,
                    shuffle_partitions=parts,
                ) as spark:
                    with timed(f"{qname}-{master}-{parts}") as t:
                        df = run_fn(spark)
                        df.count()
                rows.append({
                    "query": qname,
                    "master": master,
                    "shuffle_partitions": parts,
                    "runtime_seconds": round(t.elapsed, 4),
                })
                print(rows[-1])
    out = METRICS_DIR / "scalability_test.json"
    out.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    print(f"\nWrote {out}")


if __name__ == "__main__":
    main()
