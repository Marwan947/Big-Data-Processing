"""Download the Online Retail II dataset from UCI and convert it to CSV.

UCI ships the dataset as a single ``online_retail_II.xlsx`` file with two
sheets ("Year 2009-2010" and "Year 2010-2011"). We download the zip,
extract the workbook, concatenate the two sheets, and write a single
CSV at ``data/raw/online_retail_II.csv`` — the file the rest of the
pipeline expects.

Run with:    python -m src.prep.download_data
"""
from __future__ import annotations

import io
import sys
import urllib.request
import zipfile
from pathlib import Path

import pandas as pd

UCI_ZIP_URL = "https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
OUT_CSV = RAW_DIR / "online_retail_II.csv"


def _fetch_zip(url: str) -> bytes:
    print(f"Downloading {url} ...")
    with urllib.request.urlopen(url, timeout=120) as resp:
        return resp.read()


def _extract_xlsx(zip_bytes: bytes) -> bytes:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        xlsx_names = [n for n in zf.namelist() if n.lower().endswith(".xlsx")]
        if not xlsx_names:
            raise SystemExit(
                "No .xlsx file found in the UCI archive. "
                f"Archive contents: {zf.namelist()}"
            )
        with zf.open(xlsx_names[0]) as f:
            return f.read()


def _xlsx_to_csv(xlsx_bytes: bytes, csv_path: Path) -> int:
    print("Reading workbook (this can take ~30 s) ...")
    sheets = pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name=None, engine="openpyxl")
    print(f"Sheets found: {list(sheets.keys())}")
    combined = pd.concat(sheets.values(), ignore_index=True)
    combined.to_csv(csv_path, index=False)
    return len(combined)


def main() -> None:
    if OUT_CSV.exists():
        print(f"{OUT_CSV} already exists; skipping download.")
        return
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    try:
        zip_bytes = _fetch_zip(UCI_ZIP_URL)
    except Exception as exc:
        print(f"Download failed: {exc}", file=sys.stderr)
        print(
            "\nFallback: download manually from "
            "https://archive.ics.uci.edu/dataset/502/online+retail+ii\n"
            f"and place the combined CSV at {OUT_CSV}.",
            file=sys.stderr,
        )
        sys.exit(1)
    xlsx_bytes = _extract_xlsx(zip_bytes)
    rows = _xlsx_to_csv(xlsx_bytes, OUT_CSV)
    print(f"Wrote {rows:,} rows to {OUT_CSV}")


if __name__ == "__main__":
    main()
