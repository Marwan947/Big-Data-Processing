"""Shared cleaning logic used by all DataFrame/SQL query scripts.

The Q1 filter is the canonical "data cleaning" step. Q2-Q10 all start
from this cleaned view, so the function lives here once instead of
being copy-pasted into every query file.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.schema import CSV_OPTIONS, PARQUET_PATH, RAW_CSV_PATH, RAW_SCHEMA


def load_raw(spark: SparkSession, source: str = "csv") -> DataFrame:
    """Load the raw dataset.

    ``source="csv"`` reads the source file with the explicit schema.
    ``source="parquet"`` reads the partitioned Parquet copy produced by
    the prep step (faster for benchmark runs).
    """
    if source == "parquet":
        return spark.read.parquet(PARQUET_PATH)
    reader = spark.read
    for k, v in CSV_OPTIONS.items():
        reader = reader.option(k, v)
    return reader.schema(RAW_SCHEMA).csv(RAW_CSV_PATH)


CLEAN_SQL_PREDICATE = (
    "Quantity > 0 "
    "AND Price > 0 "
    "AND `Customer ID` IS NOT NULL "
    "AND Invoice NOT LIKE 'C%' "
    "AND InvoiceDate BETWEEN TIMESTAMP'2009-12-01 00:00:00' "
    "                    AND TIMESTAMP'2011-12-09 23:59:59'"
)


def apply_clean_filter(df: DataFrame) -> DataFrame:
    """Apply the Q1 cleaning predicate as a DataFrame transform."""
    return df.filter(
        (F.col("Quantity") > 0)
        & (F.col("Price") > 0)
        & F.col("`Customer ID`").isNotNull()
        & ~F.col("Invoice").startswith("C")
        & F.col("InvoiceDate").between("2009-12-01 00:00:00", "2011-12-09 23:59:59")
    ).withColumn("Revenue", F.col("Quantity") * F.col("Price"))


def cleaned_dataframe(spark: SparkSession, source: str = "csv") -> DataFrame:
    """Load + clean in one call. Used by Q2-Q10 implementations."""
    return apply_clean_filter(load_raw(spark, source=source))
