"""Tiny timing utilities used by every query script and the benchmark harness."""
from __future__ import annotations

import json
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional


@dataclass
class TimerResult:
    label: str
    elapsed: float


@contextmanager
def timed(label: str) -> Iterator[TimerResult]:
    result = TimerResult(label=label, elapsed=0.0)
    start = time.perf_counter()
    try:
        yield result
    finally:
        result.elapsed = time.perf_counter() - start


def append_metric(metrics_csv: Path, row: dict) -> None:
    """Append a single benchmark row to ``performance.csv`` (creating it
    with a header if absent).
    """
    metrics_csv.parent.mkdir(parents=True, exist_ok=True)
    write_header = not metrics_csv.exists()
    cols = [
        "query_id",
        "api",
        "run_index",
        "runtime_seconds",
        "result_rows",
        "shuffle_read_bytes",
        "shuffle_write_bytes",
        "peak_memory_bytes",
        "notes",
    ]
    with metrics_csv.open("a", encoding="utf-8") as f:
        if write_header:
            f.write(",".join(cols) + "\n")
        f.write(",".join(str(row.get(c, "")) for c in cols) + "\n")


def read_event_log_metrics(event_log_dir: Path, app_id: str) -> dict:
    """Parse shuffle and peak-memory metrics from a Spark event log.

    Spark writes one file per application named like ``local-1234567890``.
    We sum task-level shuffle bytes and track the maximum task-level
    peak execution memory across the whole run. If the log can't be
    read for any reason we return zeros.
    """
    totals = {
        "shuffle_read_bytes": 0,
        "shuffle_write_bytes": 0,
        "peak_memory_bytes": 0,
    }
    if not event_log_dir.exists():
        return totals
    # The log filename embeds the app id; match any file containing it.
    candidates = sorted(event_log_dir.glob(f"*{app_id}*"))
    if not candidates:
        return totals
    log_path = candidates[-1]
    try:
        with log_path.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if obj.get("Event") != "SparkListenerTaskEnd":
                    continue
                metrics = obj.get("Task Metrics", {}) or {}
                shuf_read = metrics.get("Shuffle Read Metrics", {}) or {}
                shuf_write = metrics.get("Shuffle Write Metrics", {}) or {}
                totals["shuffle_read_bytes"] += int(
                    shuf_read.get("Remote Bytes Read", 0)
                ) + int(shuf_read.get("Local Bytes Read", 0))
                totals["shuffle_write_bytes"] += int(
                    shuf_write.get("Shuffle Bytes Written", 0)
                )
                peak = int(metrics.get("Peak Execution Memory", 0))
                if peak > totals["peak_memory_bytes"]:
                    totals["peak_memory_bytes"] = peak
    except OSError:
        pass
    return totals
