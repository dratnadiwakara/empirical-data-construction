"""
Metadata for the FDIC Summary of Deposits (SOD) pipeline.
Single source of truth: API config, field list, year range, schema DDL.
"""
from __future__ import annotations

from datetime import datetime
from typing import Final

# ── Year range ────────────────────────────────────────────────────────────────
# FDIC SOD API coverage starts in 1994; 1993 returns zero records.
FIRST_YEAR: Final[int] = 1994
LAST_YEAR: Final[int] = datetime.now().year
ALL_YEARS: Final[list[int]] = list(range(LAST_YEAR, FIRST_YEAR - 1, -1))

# ── API config ────────────────────────────────────────────────────────────────
API_BASE: Final[str] = "https://banks.data.fdic.gov/api/sod"
API_PAGE_SIZE: Final[int] = 10_000

# All fields fetched from the FDIC SOD API.
# STCNTYBR is already a 5-char state+county FIPS string (no construction needed).
# DEPSUMBR and ASSET are in $thousands.
API_FIELDS: Final[list[str]] = [
    "UNINUMBR",   # Unique branch identifier (FDIC)
    "YEAR",       # Survey year
    "CERT",       # FDIC certificate number (institution-level)
    "RSSDID",     # Fed RSSD ID of the branch institution
    "RSSDHCR",    # Fed RSSD ID of top-tier holding company
    "NAMEFULL",   # Institution full legal name
    "NAMEBR",     # Branch name
    "ADDRESBR",   # Branch street address
    "CITYBR",     # Branch city
    "STALP",      # State abbreviation
    "STNAME",     # State full name
    "ZIPBR",      # Branch ZIP code
    "STCNTYBR",   # State+county FIPS (5-char, e.g. "55047")
    "CNTYNAMB",   # County name
    "ASSET",      # Total assets of institution ($000s)
    "DEPSUMBR",   # Deposits at this branch ($000s)
    "BRNUM",      # Branch sequence number within institution
    "BRSERTYP",   # Branch service type code
    "CHRTAGNT",   # Charter agent (STATE, OCC, OTS, etc.)
    "ESTYMD",     # Branch establishment date (YYYY-MM-DD)
    "NAMEHCR",    # Top-tier holding company name
]

# Columns cast to BIGINT; all others stored as VARCHAR.
NUMERIC_COLS: Final[set[str]] = {
    "UNINUMBR",
    "YEAR",
    "CERT",
    "RSSDID",
    "RSSDHCR",
    "ASSET",
    "DEPSUMBR",
    "BRNUM",
    "BRSERTYP",
}

# ── Panel metadata DDL ────────────────────────────────────────────────────────
PANEL_METADATA_DDL: Final[str] = """
CREATE TABLE IF NOT EXISTS panel_metadata (
    year         INTEGER,
    row_count    BIGINT,
    source_url   VARCHAR,
    built_at     VARCHAR,
    parquet_path VARCHAR,
    PRIMARY KEY (year)
)
"""

# ── Variable descriptions (for agent reference) ───────────────────────────────
VARIABLE_DESCRIPTIONS: Final[dict[str, str]] = {
    "UNINUMBR":  "Unique FDIC branch identifier; stable across years.",
    "YEAR":      "Survey year (June 30 call date).",
    "CERT":      "FDIC certificate number; institution-level ID.",
    "RSSDID":    "Federal Reserve RSSD ID of the branch's institution. Join to NIC for org structure.",
    "RSSDHCR":   "RSSD ID of the top-tier holding company.",
    "NAMEFULL":  "Full legal name of the insured institution.",
    "NAMEBR":    "Branch office name.",
    "ADDRESBR":  "Branch street address.",
    "CITYBR":    "Branch city.",
    "STALP":     "Two-character state abbreviation.",
    "STNAME":    "Full state name.",
    "ZIPBR":     "Branch ZIP code (may be 5 or 9 digit).",
    "STCNTYBR":  "5-character state+county FIPS code (e.g. '36061'). Ready to join HMDA/CRA.",
    "CNTYNAMB":  "County name.",
    "ASSET":     "Total assets of the institution in $thousands.",
    "DEPSUMBR":  "Total deposits at this branch in $thousands. Multiply by 1000 for dollars.",
    "BRNUM":     "Branch sequence number within the institution (0 = main office).",
    "BRSERTYP":  "Branch service type: 11=full service brick & mortar, 12=full service retail, etc.",
    "CHRTAGNT":  "Charter agent: STATE=state-chartered, OCC=nationally chartered, OTS=thrift.",
    "ESTYMD":    "Branch establishment date (YYYY-MM-DD string).",
    "NAMEHCR":   "Name of the top-tier holding company.",
}
