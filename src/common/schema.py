"""Explicit schemas and shared paths for the Online Retail II dataset.

Using an explicit schema avoids the cost of Spark's CSV inference pass
and pins the exact types we expect downstream (Customer ID is read as
double because the source file represents it as a real-valued ID).
"""
from __future__ import annotations

from pathlib import Path

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_CSV_PATH = str(PROJECT_ROOT / "data" / "raw" / "online_retail_II.csv")
PARQUET_PATH = str(PROJECT_ROOT / "data" / "parquet" / "online_retail_II")
RESULTS_DIR = PROJECT_ROOT / "outputs" / "results"
EXPLAIN_DIR = PROJECT_ROOT / "outputs" / "explain"
METRICS_DIR = PROJECT_ROOT / "outputs" / "metrics"
PLOTS_DIR = PROJECT_ROOT / "outputs" / "plots"
INTERMEDIATE_DIR = PROJECT_ROOT / "outputs" / "intermediate"

for _d in (RESULTS_DIR, EXPLAIN_DIR, METRICS_DIR, PLOTS_DIR, INTERMEDIATE_DIR):
    _d.mkdir(parents=True, exist_ok=True)


RAW_SCHEMA = StructType(
    [
        StructField("Invoice", StringType(), True),
        StructField("StockCode", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("InvoiceDate", TimestampType(), True),
        StructField("Price", DoubleType(), True),
        StructField("Customer ID", DoubleType(), True),
        StructField("Country", StringType(), True),
    ]
)

CSV_OPTIONS = {
    "header": "true",
    "timestampFormat": "yyyy-MM-dd HH:mm:ss",
    "mode": "PERMISSIVE",
}
