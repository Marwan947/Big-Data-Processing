"""Country -> Region lookup used by Q8.

A small in-memory map (~40 rows) — perfect candidate for a broadcast
join against the (large) cleaned sales table.
"""
from __future__ import annotations

EUROPE = {
    "United Kingdom", "France", "Germany", "EIRE", "Spain", "Netherlands",
    "Belgium", "Switzerland", "Portugal", "Italy", "Finland", "Austria",
    "Norway", "Denmark", "Channel Islands", "Cyprus", "Sweden", "Greece",
    "Iceland", "Malta", "Lithuania", "European Community", "Czech Republic",
    "Poland",
}
AMERICAS = {"USA", "Canada", "Brazil"}
ASIA = {"Japan", "Hong Kong", "Singapore", "Thailand", "Israel", "Lebanon",
        "United Arab Emirates", "Saudi Arabia", "Bahrain"}
OCEANIA = {"Australia", "RSA", "Nigeria"}


def country_region_pairs() -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for c in EUROPE:
        pairs.append((c, "Europe"))
    for c in AMERICAS:
        pairs.append((c, "Americas"))
    for c in ASIA:
        pairs.append((c, "Asia"))
    for c in OCEANIA:
        pairs.append((c, "Other"))
    return pairs
