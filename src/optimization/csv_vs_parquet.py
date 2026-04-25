"""CSV vs Parquet demo — runs Q3 on each format and reports the delta."""
from __future__ import annotations

import json

from src.common.schema import METRICS_DIR
from src.common.spark_session import spark_app
from src.common.timer import timed
from src.dataframe.q03_revenue_by_country_year_month import run as run_q3


def main() -> None:
    results = {}
    for source in ("csv", "parquet"):
        with spark_app(f"CsvVsParquet-{source}") as spark:
            with timed(f"q03_{source}") as t:
                df = run_q3(spark, source=source)
                rows = df.count()
            results[f"{source}_seconds"] = t.elapsed
            results[f"{source}_rows"] = rows
    if results.get("csv_seconds") and results.get("parquet_seconds"):
        results["speedup_parquet_over_csv"] = (
            results["csv_seconds"] / results["parquet_seconds"]
        )
    out = METRICS_DIR / "csv_vs_parquet.json"
    out.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(json.dumps(results, indent=2))
    print(f"\nWrote {out}")


if __name__ == "__main__":
    main()
