"""Write the four-section ``.explain(mode='extended')`` output for every
DataFrame and Spark SQL implementation to ``outputs/explain/``.

For RDD implementations Spark does not produce a Catalyst plan; we
instead dump the lineage via ``rdd.toDebugString`` so the report can
contrast the two.

Run with:    python -m src.benchmark.capture_explain
"""
from __future__ import annotations

import io
import sys

from src.benchmark._registry import API_NAMES, QUERY_IDS, load_run
from src.common.schema import EXPLAIN_DIR
from src.common.spark_session import spark_app


def _capture_dataframe_explain(spark, qid: str, api: str) -> str:
    run_fn = load_run(api, qid)
    df = run_fn(spark)
    # Older PySpark prints to stdout; capture via redirect.
    buf = io.StringIO()
    sys_stdout = sys.stdout
    sys.stdout = buf
    try:
        df.explain(mode="extended")
    finally:
        sys.stdout = sys_stdout
    return buf.getvalue()


def _capture_rdd_lineage(spark, qid: str) -> str:
    run_fn = load_run("rdd", qid)
    rdd = run_fn(spark.sparkContext)
    if hasattr(rdd, "toDebugString"):
        return rdd.toDebugString().decode("utf-8", errors="replace")
    return f"(no lineage available: returned {type(rdd).__name__})"


def main() -> None:
    EXPLAIN_DIR.mkdir(parents=True, exist_ok=True)
    with spark_app("Benchmark-CaptureExplain") as spark:
        for qid in QUERY_IDS:
            for api in API_NAMES:
                out_path = EXPLAIN_DIR / f"{qid}_{api}.txt"
                try:
                    if api == "rdd":
                        content = _capture_rdd_lineage(spark, qid)
                    else:
                        content = _capture_dataframe_explain(spark, qid, api)
                except Exception as exc:  # pragma: no cover - fallthrough for explain failures
                    content = f"ERROR capturing plan for {qid}/{api}: {exc}\n"
                out_path.write_text(content, encoding="utf-8")
                print(f"  wrote {out_path.relative_to(EXPLAIN_DIR.parents[1])}")


if __name__ == "__main__":
    main()
