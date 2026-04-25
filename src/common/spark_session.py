"""Builds a SparkSession with sensible defaults for this project.

All query scripts use ``build_session`` so they share a uniform configuration
(shuffle partitions, event log directory, broadcast threshold).
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Optional
from urllib.parse import quote

from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[2]
# Use a path without spaces — Spark's URI parsing chokes on whitespace
# in the project root (e.g. "Mini project2"), so we stash event logs
# under /tmp where the path is always clean.
EVENT_LOG_DIR = Path("/tmp/spark-events-retail")


def _ensure_event_log_dir() -> str:
    EVENT_LOG_DIR.mkdir(parents=True, exist_ok=True)
    return "file://" + quote(str(EVENT_LOG_DIR))


def build_session(
    app_name: str,
    master: str = "local[*]",
    shuffle_partitions: int = 8,
    extra_conf: Optional[dict] = None,
) -> SparkSession:
    """Return a configured ``SparkSession``.

    Defaults are tuned for a single laptop running the Online Retail II
    dataset: 8 shuffle partitions (vs. Spark's default 200) keeps small
    aggregations fast; broadcast threshold left at the default 10 MB so
    Q8's tiny region table is auto-broadcast as well as via the hint.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", "2g")
    )
    # Event log is opt-in (disabled by default because Spark struggles with
    # whitespace in the file:// URI on macOS local paths).
    if os.environ.get("SPARK_EVENT_LOG", "0") == "1":
        builder = (
            builder.config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", _ensure_event_log_dir())
        )
    if extra_conf:
        for k, v in extra_conf.items():
            builder = builder.config(k, str(v))

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark


@contextmanager
def spark_app(app_name: str, **kwargs) -> Iterator[SparkSession]:
    """Context manager that builds a session and stops it on exit."""
    spark = build_session(app_name, **kwargs)
    try:
        yield spark
    finally:
        spark.stop()
