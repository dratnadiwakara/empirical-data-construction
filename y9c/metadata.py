"""
FR Y-9C — metadata and configuration single source of truth.

FR Y-9C is the consolidated quarterly financial filing for US Bank Holding
Companies. Bulk ZIPs are placed manually by the user in raw/ from
https://www.ffiec.gov/npw/FinancialReport/FinancialDataDownload?selectedyear={YYYY}.

One ZIP per quarter, naming convention `BHCF{YYYYMMDD}.ZIP`, containing one
caret-delimited TXT file. Coverage in this build: 2000 Q1 -> 2025 Q3.
"""
from __future__ import annotations

import re
from typing import Iterable


# ── Source documentation ─────────────────────────────────────────────────────

SOURCE_PAGE_TEMPLATE = (
    "https://www.ffiec.gov/npw/FinancialReport/FinancialDataDownload"
    "?selectedyear={year}"
)
SOURCE_PRODUCT = "FR Y-9C — Consolidated Financial Statements for Bank Holding Companies"
SOURCE_FORMAT = "ZIP archive containing single caret-delimited TXT"

QUARTERS_START = (2000, 1)


# ── Filename convention ──────────────────────────────────────────────────────
#
# ZIP    : 'BHCF{YYYYMMDD}.ZIP' (case may vary)
# Inside : 'BHCF{YYYYMMDD}.txt'
# Field delimiter is caret (^). All MDRM columns retained as VARCHAR; arithmetic
# uses TRY_CAST(... AS DOUBLE).

ZIP_FILENAME_REGEX = re.compile(
    r"^BHCF(\d{4})(\d{2})(\d{2})\.zip$",
    re.IGNORECASE,
)

QUARTER_FROM_MMDD = {"0331": 1, "0630": 2, "0930": 3, "1231": 4}

# Identity columns in every Y-9C file.
#   RSSD9001 = entity RSSD ID (BIGINT-castable)
#   RSSD9999 = reporting period date (YYYYMMDD as VARCHAR)
IDENTITY_COLS = ("RSSD9001", "RSSD9999")


def parse_zip_filename(name: str) -> tuple[int, int] | None:
    """Extract (year, quarter) from a raw ZIP filename, or None if not matched."""
    m = ZIP_FILENAME_REGEX.match(name)
    if not m:
        return None
    yyyy, mm, dd = m.group(1), m.group(2), m.group(3)
    q = QUARTER_FROM_MMDD.get(f"{mm}{dd}")
    if q is None:
        return None
    return int(yyyy), q


def quarter_to_mmdd(quarter: int) -> str:
    return {1: "0331", 2: "0630", 3: "0930", 4: "1231"}[quarter]


def quarter_end_date(year: int, quarter: int) -> str:
    """Return YYYY-MM-DD quarter-end date string for a (year, quarter)."""
    mmdd = {1: "03-31", 2: "06-30", 3: "09-30", 4: "12-31"}[quarter]
    return f"{year}-{mmdd}"


def all_quarter_labels(start: tuple[int, int] = QUARTERS_START,
                       end: tuple[int, int] | None = None) -> Iterable[tuple[int, int]]:
    """Yield (year, quarter) tuples inclusive of the given range."""
    from datetime import date
    y, q = start
    if end is None:
        today = date.today()
        end = (today.year, (today.month - 1) // 3 + 1)
    while (y, q) <= end:
        yield y, q
        q += 1
        if q == 5:
            q = 1
            y += 1


# ── DuckDB DDL ───────────────────────────────────────────────────────────────

PANEL_METADATA_DDL = """
CREATE TABLE IF NOT EXISTS panel_metadata (
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    row_count BIGINT,
    n_columns INTEGER,
    source_zip VARCHAR,
    source_zip_sha256 VARCHAR,
    parquet_path VARCHAR,
    delimiter VARCHAR,
    built_at TIMESTAMP,
    PRIMARY KEY (year, quarter)
)
"""
