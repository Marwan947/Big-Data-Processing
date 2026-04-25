"""Helpers for registering temp views used across SQL query scripts."""
from __future__ import annotations

from pyspark.sql import SparkSession

from src.common.cleaning import cleaned_dataframe
from src.common.regions import country_region_pairs


def register_sales(spark: SparkSession, source: str = "csv") -> None:
    cleaned_dataframe(spark, source=source).createOrReplaceTempView("sales")


def register_regions(spark: SparkSession) -> None:
    rows = country_region_pairs()
    spark.createDataFrame(rows, schema=["Country", "Region"]).createOrReplaceTempView(
        "country_region"
    )
