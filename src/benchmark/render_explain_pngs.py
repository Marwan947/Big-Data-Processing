"""Render selected ``outputs/explain/*.txt`` files into PNG "screenshots"
that the report embeds.

We render with matplotlib at a monospace font so the output reads like a
terminal screen capture without needing actual screenshots from a UI.
"""
from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from src.common.schema import EXPLAIN_DIR, PLOTS_DIR

# Files to render (DataFrame plan for each headline query).
TARGETS = [
    ("q01_dataframe.txt", "Q1 — Cleaning filter (DataFrame) — execution plans"),
    ("q05_dataframe.txt", "Q5 — Monthly window (DataFrame) — execution plans"),
    ("q08_dataframe.txt", "Q8 — Broadcast join (DataFrame) — execution plans"),
    ("q09_dataframe.txt", "Q9 — Sort-merge self-join (DataFrame) — execution plans"),
    ("q10_dataframe.txt", "Q10 — RFM segmentation (DataFrame) — execution plans"),
]

# Width of each rendered "page" in characters before we wrap.
WRAP_AT = 200
# How many lines per PNG before we split (avoid one huge image).
LINES_PER_PAGE = 70


def _wrap_line(line: str, width: int) -> list[str]:
    """Hard-wrap a single line at ``width`` chars."""
    if len(line) <= width:
        return [line]
    out: list[str] = []
    i = 0
    while i < len(line):
        out.append(line[i : i + width])
        i += width
    return out


def _read_wrapped(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8", errors="replace")
    wrapped: list[str] = []
    for raw_line in text.splitlines():
        wrapped.extend(_wrap_line(raw_line, WRAP_AT))
    return wrapped


def _render_page(lines: list[str], title: str, out_path: Path) -> None:
    n_lines = len(lines)
    # Sizing: each line ~ 0.16 inch tall; width tuned for 200 chars at 8pt mono.
    fig_width = 14
    fig_height = max(2.5, 0.16 * (n_lines + 4))
    fig, ax = plt.subplots(figsize=(fig_width, fig_height))
    ax.set_axis_off()
    ax.set_title(title, loc="left", fontsize=12, weight="bold", pad=10)
    body = "\n".join(lines)
    ax.text(
        0.0, 1.0, body,
        ha="left", va="top", family="monospace", fontsize=7.5,
        transform=ax.transAxes,
    )
    fig.tight_layout(pad=0.4)
    fig.savefig(out_path, dpi=160, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    for fname, title in TARGETS:
        src = EXPLAIN_DIR / fname
        if not src.exists():
            print(f"  SKIP {src} (not found)")
            continue
        lines = _read_wrapped(src)
        # Split into pages so no one image is hundreds of lines tall.
        stem = src.stem  # e.g. "q01_dataframe"
        page = 1
        for start in range(0, len(lines), LINES_PER_PAGE):
            chunk = lines[start : start + LINES_PER_PAGE]
            page_title = title if page == 1 and len(lines) <= LINES_PER_PAGE else (
                f"{title} (page {page})"
            )
            out_path = PLOTS_DIR / (
                f"explain_{stem}.png" if len(lines) <= LINES_PER_PAGE
                else f"explain_{stem}_p{page}.png"
            )
            _render_page(chunk, page_title, out_path)
            print(f"  wrote {out_path}")
            page += 1


if __name__ == "__main__":
    main()
