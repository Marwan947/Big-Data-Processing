"""One-time CSV → Parquet conversion partitioned by Year.

Run with:    python -m src.prep.csv_to_parquet

The Parquet copy is used for:
  * the CSV-vs-Parquet performance comparison (Q3 on both formats)
  * the partition-pruning demo (`WHERE Year = 2010`)
  * faster benchmark runs in general
"""
from __future__ import annotations

from pyspark.sql import functions as F

from src.common.cleaning import load_raw
from src.common.schema import PARQUET_PATH
from src.common.spark_session import spark_app
from src.common.timer import timed


def main() -> None:
    with spark_app("Prep-CsvToParquet") as spark:
        with timed("csv_to_parquet") as t:
            df = load_raw(spark, source="csv").withColumn(
                "Year", F.year("InvoiceDate")
            )
            (
                df.write.mode("overwrite")
                .partitionBy("Year")
                .parquet(PARQUET_PATH)
            )
        print(f"Wrote partitioned parquet to {PARQUET_PATH} in {t.elapsed:.2f}s")


if __name__ == "__main__":
    main()
