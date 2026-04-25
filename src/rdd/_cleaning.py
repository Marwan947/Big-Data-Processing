"""RDD-only loader and cleaner for the Online Retail II CSV.

The DataFrame reader is intentionally avoided here so the RDD
implementations stay strictly within the RDD API.
"""
from __future__ import annotations

import csv
import io
from datetime import datetime
from typing import NamedTuple, Optional

from pyspark import RDD, SparkContext

from src.common.schema import RAW_CSV_PATH

DATE_FMT = "%Y-%m-%d %H:%M:%S"
MIN_DATE = datetime(2009, 12, 1, 0, 0, 0)
MAX_DATE = datetime(2011, 12, 9, 23, 59, 59)


class Sale(NamedTuple):
    invoice: str
    stock_code: str
    description: str
    quantity: int
    invoice_date: datetime
    price: float
    customer_id: float
    country: str

    @property
    def revenue(self) -> float:
        return self.quantity * self.price


def _parse_line(line: str) -> Optional[Sale]:
    try:
        fields = next(csv.reader(io.StringIO(line)))
    except StopIteration:
        return None
    if len(fields) != 8:
        return None
    invoice, stock, desc, qty_s, dt_s, price_s, cust_s, country = fields
    try:
        qty = int(qty_s)
        price = float(price_s)
        invoice_date = datetime.strptime(dt_s, DATE_FMT)
    except (ValueError, TypeError):
        return None
    if not cust_s:
        return None
    try:
        cust = float(cust_s)
    except ValueError:
        return None
    return Sale(invoice, stock, desc, qty, invoice_date, price, cust, country)


def _is_clean(s: Optional[Sale]) -> bool:
    if s is None:
        return False
    return (
        s.quantity > 0
        and s.price > 0
        and not s.invoice.startswith("C")
        and MIN_DATE <= s.invoice_date <= MAX_DATE
    )


def load_clean_sales(sc: SparkContext) -> "RDD[Sale]":
    """Return an RDD[Sale] of cleaned, type-checked sales records."""
    raw = sc.textFile(RAW_CSV_PATH)
    header = raw.first()
    return (
        raw.filter(lambda r: r != header)
        .map(_parse_line)
        .filter(_is_clean)
    )
