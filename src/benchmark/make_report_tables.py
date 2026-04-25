"""Aggregate ``performance.csv`` into Markdown tables and PNG bar charts
that the report embeds.

Outputs:
    outputs/metrics/performance_summary.md   (Markdown table, median per query/api)
    outputs/plots/runtime_by_query.png       (grouped bar chart)
    outputs/plots/runtime_winners.png        (bar chart highlighting best API per query)
"""
from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from src.common.schema import METRICS_DIR, PLOTS_DIR

PERFORMANCE_CSV = METRICS_DIR / "performance.csv"
SUMMARY_MD = METRICS_DIR / "performance_summary.md"
GROUPED_PNG = PLOTS_DIR / "runtime_by_query.png"
WINNER_PNG = PLOTS_DIR / "runtime_winners.png"


def _median_table(df: pd.DataFrame) -> pd.DataFrame:
    g = (
        df.groupby(["query_id", "api"])["runtime_seconds"]
        .median()
        .unstack(fill_value=float("nan"))
        .reindex(columns=["dataframe", "sql", "rdd"])
    )
    g.columns = [c.upper() for c in g.columns]
    g["best_api"] = g.idxmin(axis=1)
    return g


def _shuffle_memory_table(df: pd.DataFrame) -> pd.DataFrame:
    """For each query, take the *DataFrame* run's median shuffle-write and
    peak-memory values. We use a single API for the headline shuffle/memory
    column to keep the report table compact (the per-API numbers are in
    performance.csv for anyone who wants to dig deeper).
    """
    df_only = df[df["api"] == "dataframe"].copy()
    df_only["shuffle_write_bytes"] = pd.to_numeric(
        df_only["shuffle_write_bytes"], errors="coerce"
    )
    df_only["peak_memory_bytes"] = pd.to_numeric(
        df_only["peak_memory_bytes"], errors="coerce"
    )
    return df_only.groupby("query_id").agg(
        shuffle_write_kb=("shuffle_write_bytes", lambda s: s.median() / 1024),
        peak_memory_mb=("peak_memory_bytes", lambda s: s.median() / (1024 * 1024)),
    )


def _write_markdown(runtime_table: pd.DataFrame, shufmem: pd.DataFrame) -> None:
    rows = [
        "| Query | DF (s) | SQL (s) | RDD (s) | Best API | Shuffle write (KB) | Peak mem (MB) |",
        "|-------|--------|---------|---------|----------|--------------------|----------------|",
    ]
    for qid, row in runtime_table.iterrows():
        sw = shufmem.loc[qid, "shuffle_write_kb"] if qid in shufmem.index else float("nan")
        pm = shufmem.loc[qid, "peak_memory_mb"] if qid in shufmem.index else float("nan")
        sw_s = "n/a" if pd.isna(sw) else f"{sw:.1f}"
        pm_s = "n/a" if pd.isna(pm) else f"{pm:.1f}"
        rows.append(
            f"| {qid} | {row['DATAFRAME']:.2f} | {row['SQL']:.2f} | {row['RDD']:.2f} "
            f"| {row['best_api']} | {sw_s} | {pm_s} |"
        )
    SUMMARY_MD.write_text("\n".join(rows) + "\n", encoding="utf-8")


def _grouped_bar_chart(table: pd.DataFrame) -> None:
    apis = ["DATAFRAME", "SQL", "RDD"]
    qids = list(table.index)
    fig, ax = plt.subplots(figsize=(11, 5))
    width = 0.27
    x = range(len(qids))
    for i, api in enumerate(apis):
        offsets = [xi + (i - 1) * width for xi in x]
        ax.bar(offsets, table[api].tolist(), width=width, label=api)
    ax.set_xticks(list(x))
    ax.set_xticklabels(qids)
    ax.set_xlabel("Query")
    ax.set_ylabel("Median runtime (seconds)")
    ax.set_title("Median runtime per query across APIs")
    ax.legend()
    ax.grid(axis="y", linestyle=":", alpha=0.5)
    fig.tight_layout()
    fig.savefig(GROUPED_PNG, dpi=140)
    plt.close(fig)


def _winner_chart(table: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(9, 4))
    winners = table["best_api"].value_counts().reindex(["DATAFRAME", "SQL", "RDD"], fill_value=0)
    ax.bar(winners.index, winners.values, color=["#4C72B0", "#55A868", "#C44E52"])
    ax.set_ylabel("Number of queries won")
    ax.set_title("Fastest API by query count")
    for i, v in enumerate(winners.values):
        ax.text(i, v + 0.05, str(int(v)), ha="center")
    fig.tight_layout()
    fig.savefig(WINNER_PNG, dpi=140)
    plt.close(fig)


def main() -> None:
    if not PERFORMANCE_CSV.exists():
        raise SystemExit(f"missing {PERFORMANCE_CSV}; run src.benchmark.run_all first")
    df = pd.read_csv(PERFORMANCE_CSV)
    df["runtime_seconds"] = df["runtime_seconds"].astype(float)
    runtime_table = _median_table(df)
    shufmem_table = _shuffle_memory_table(df)
    _write_markdown(runtime_table, shufmem_table)
    _grouped_bar_chart(runtime_table)
    _winner_chart(runtime_table)
    print(f"  wrote {SUMMARY_MD}")
    print(f"  wrote {GROUPED_PNG}")
    print(f"  wrote {WINNER_PNG}")
    print()
    print(runtime_table.to_string())
    print()
    print(shufmem_table.to_string())


if __name__ == "__main__":
    main()
