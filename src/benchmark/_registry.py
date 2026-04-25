"""Central registry mapping query IDs to their three implementations.

Keeps the benchmark harness, ``capture_explain``, and report generator
all driven from one source of truth.
"""
from __future__ import annotations

from importlib import import_module
from typing import Callable

QUERY_IDS = ["q01", "q02", "q03", "q04", "q05", "q06", "q07", "q08", "q09", "q10"]
API_NAMES = ["dataframe", "sql", "rdd"]

_MODULE_SUFFIX = {
    "q01": "q01_filter",
    "q02": "q02_aggregates",
    "q03": "q03_revenue_by_country_year_month",
    "q04": "q04_top_products",
    "q05": "q05_monthly_trend",
    "q06": "q06_customer_rank_by_country",
    "q07": "q07_above_average_customers",
    "q08": "q08_broadcast_join",
    "q09": "q09_sort_merge_self_join",
    "q10": "q10_rfm_segmentation",
}


def module_path(api: str, qid: str) -> str:
    return f"src.{api}.{_MODULE_SUFFIX[qid]}"


def load_run(api: str, qid: str) -> Callable:
    """Return the ``run`` callable for the given API + query."""
    mod = import_module(module_path(api, qid))
    return getattr(mod, "run")
