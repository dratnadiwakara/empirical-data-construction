"""
FFIEC Call Reports — metadata and configuration single source of truth.

Covers Forms 031 / 041 / 051 (all merged in the same 'Call Bulk All Schedules'
quarterly ZIP published on the FFIEC CDR bulk download portal). No ASP.NET
scraping — ZIPs are placed manually by the user in the raw/ directory.
"""
from __future__ import annotations

import re
from typing import Iterable


# ── Source documentation ─────────────────────────────────────────────────────

CDR_SOURCE_URL = "https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx"
CDR_PRODUCT = "Call Reports -- Single Period"
CDR_FORMAT = "Tab Delimited"

MDRM_URL = "https://www.federalreserve.gov/apps/mdrm/pdf/MDRM.zip"

FORM_TYPES = ("031", "041", "051")

QUARTERS_START = (2001, 1)


# ── Filename convention ──────────────────────────────────────────────────────
#
# ZIP    : 'FFIEC CDR Call Bulk All Schedules {MMDDYYYY}.zip'
# Inside : 'FFIEC CDR Call Bulk POR {MMDDYYYY}.txt'
#          'FFIEC CDR Call Schedule {SCHEDULE} {MMDDYYYY}.txt'
#          'FFIEC CDR Call Schedule {SCHEDULE} {MMDDYYYY}(N of M).txt'
#
# SCHEDULE values observed 2001-2025: RC, RCA, RCB, RCCI, RCCII, RCD, RCE,
# RCEI, RCEII, RCF, RCG, RCH, RCI, RCK, RCL, RCM, RCN, RCO, RCP, RCQ, RCR,
# RCRI, RCRII, RCS, RCT, RCV, RI, RIA, RIBI, RIBII, RID, RIE,
# plus CI, ENT, GI, GL, LEO, NARR.
#
# Multi-part files are COLUMN splits of the same schedule (same IDRSSD set,
# different MDRM code columns). They must be FULL OUTER JOIN-ed on IDRSSD
# when materializing to Parquet. See construct.py.

ZIP_FILENAME_REGEX = re.compile(
    r"^FFIEC CDR Call Bulk All Schedules (\d{2})(\d{2})(\d{4})\.zip$"
)

INNER_FILENAME_REGEX = re.compile(
    r"^FFIEC CDR Call "
    r"(?:Bulk (?P<por>POR)|Schedule (?P<schedule>[A-Z]+)) "
    r"(?P<mm>\d{2})(?P<dd>\d{2})(?P<yyyy>\d{4})"
    r"(?:\((?P<part>\d+) of (?P<total>\d+)\))?"
    r"\.txt$"
)

QUARTER_FROM_MMDD = {"0331": 1, "0630": 2, "0930": 3, "1231": 4}


def parse_zip_filename(name: str) -> tuple[int, int] | None:
    """Extract (year, quarter) from a raw ZIP filename, or None if not matched."""
    m = ZIP_FILENAME_REGEX.match(name)
    if not m:
        return None
    mm, dd, yyyy = m.group(1), m.group(2), m.group(3)
    q = QUARTER_FROM_MMDD.get(f"{mm}{dd}")
    if q is None:
        return None
    return int(yyyy), q


def parse_inner_filename(name: str) -> dict | None:
    """
    Parse an extracted TSV filename.
    Returns dict with keys: schedule (str), year (int), quarter (int),
    part (int|None), total (int|None).
    schedule is 'POR' for the bulk POR file, otherwise the schedule suffix
    (e.g., 'RC', 'RCB', 'RCRII').
    """
    m = INNER_FILENAME_REGEX.match(name)
    if not m:
        return None
    schedule = m.group("schedule") or m.group("por")
    q = QUARTER_FROM_MMDD.get(f"{m.group('mm')}{m.group('dd')}")
    if q is None:
        return None
    return {
        "schedule": schedule,
        "year": int(m.group("yyyy")),
        "quarter": q,
        "part": int(m.group("part")) if m.group("part") else None,
        "total": int(m.group("total")) if m.group("total") else None,
    }


# ── Schedule registry ────────────────────────────────────────────────────────
#
# Maps the schedule suffix found in FFIEC filenames to:
#   (duckdb_view_name, human_description)
#
# Unknown schedules encountered in raw ZIPs are auto-registered at construct
# time with a placeholder description so new FFIEC schedules don't get
# silently dropped.

SCHEDULE_REGISTRY: dict[str, tuple[str, str]] = {
    "POR": ("call_filers", "Bulk Point-of-Record: filer identity, form type, address"),
    "RC":   ("schedule_rc",   "Balance Sheet"),
    "RCA":  ("schedule_rca",  "Cash and Balances Due From Depository Institutions"),
    "RCB":  ("schedule_rcb",  "Securities"),
    "RCCI": ("schedule_rcci", "Loans and Lease Financing Receivables (Part I)"),
    "RCCII":("schedule_rccii","Loans to Small Businesses and Small Farms (Part II)"),
    "RCD":  ("schedule_rcd",  "Trading Assets and Liabilities"),
    "RCE":  ("schedule_rce",  "Deposit Liabilities"),
    "RCEI": ("schedule_rcei", "Deposit Liabilities (Domestic Offices, Part I)"),
    "RCEII":("schedule_rceii","Deposit Liabilities (Foreign Offices, Part II)"),
    "RCF":  ("schedule_rcf",  "Other Assets"),
    "RCG":  ("schedule_rcg",  "Other Liabilities"),
    "RCH":  ("schedule_rch",  "Selected Balance Sheet Items for Domestic Offices"),
    "RCI":  ("schedule_rci",  "Assets and Liabilities of IBFs"),
    "RCK":  ("schedule_rck",  "Quarterly Averages"),
    "RCL":  ("schedule_rcl",  "Derivatives and Off-Balance-Sheet Items"),
    "RCM":  ("schedule_rcm",  "Memoranda"),
    "RCN":  ("schedule_rcn",  "Past Due and Nonaccrual Loans, Leases, and Other Assets"),
    "RCO":  ("schedule_rco",  "Other Data for Deposit Insurance Assessments"),
    "RCP":  ("schedule_rcp",  "1-4 Family Residential Mortgage Banking Activities"),
    "RCQ":  ("schedule_rcq",  "Assets and Liabilities Measured at Fair Value"),
    "RCR":  ("schedule_rcr",  "Regulatory Capital (pre-2014)"),
    "RCRI": ("schedule_rcri", "Regulatory Capital (Part I)"),
    "RCRII":("schedule_rcrii","Regulatory Capital (Part II)"),
    "RCS":  ("schedule_rcs",  "Servicing, Securitization, and Asset Sale Activities"),
    "RCT":  ("schedule_rct",  "Fiduciary and Related Services"),
    "RCV":  ("schedule_rcv",  "Variable Interest Entities"),
    "RI":   ("schedule_ri",   "Income Statement"),
    "RIA":  ("schedule_ria",  "Changes in Bank Equity Capital"),
    "RIBI": ("schedule_ribi", "Charge-offs and Recoveries on Loans and Leases (Part I)"),
    "RIBII":("schedule_ribii","Changes in Allowance for Loan and Lease Losses (Part II)"),
    "RID":  ("schedule_rid",  "Income from Foreign Offices"),
    "RIE":  ("schedule_rie",  "Explanations"),
    "CI":   ("schedule_ci",   "Contact Information"),
    "ENT":  ("schedule_ent",  "Entity Information"),
    "GI":   ("schedule_gi",   "General Information"),
    "GL":   ("schedule_gl",   "Glossary / General"),
    "LEO":  ("schedule_leo",  "Legal Entity / Other (pre-2003 era)"),
    "NARR": ("schedule_narr", "Narrative Statements"),
}


def schedule_view_name(schedule_id: str) -> str:
    """Return the DuckDB view name for a schedule id. Handles unknown ids."""
    if schedule_id in SCHEDULE_REGISTRY:
        return SCHEDULE_REGISTRY[schedule_id][0]
    # Auto-register unknown with lowercased suffix
    return f"schedule_{schedule_id.lower()}"


def schedule_description(schedule_id: str) -> str:
    if schedule_id in SCHEDULE_REGISTRY:
        return SCHEDULE_REGISTRY[schedule_id][1]
    return f"Auto-registered FFIEC schedule '{schedule_id}' (not in known registry)"


# ── DuckDB DDL ───────────────────────────────────────────────────────────────

PANEL_METADATA_DDL = """
CREATE TABLE IF NOT EXISTS panel_metadata (
    schedule VARCHAR NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    row_count BIGINT,
    source_zip VARCHAR,
    source_zip_sha256 VARCHAR,
    parquet_path VARCHAR,
    n_columns INTEGER,
    n_parts INTEGER,
    built_at TIMESTAMP,
    PRIMARY KEY (schedule, year, quarter)
)
"""

MDRM_DICTIONARY_DDL = """
CREATE TABLE IF NOT EXISTS mdrm_dictionary (
    mnemonic VARCHAR,
    item_code VARCHAR,
    start_date VARCHAR,
    end_date VARCHAR,
    item_name VARCHAR,
    confidentiality VARCHAR,
    item_type VARCHAR,
    reporting_form VARCHAR,
    description VARCHAR,
    series_glossary VARCHAR
)
"""


# ── Helpers ──────────────────────────────────────────────────────────────────

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


def quarter_end_date(year: int, quarter: int) -> str:
    """Return YYYY-MM-DD quarter-end date string for a (year, quarter)."""
    mmdd = {1: "03-31", 2: "06-30", 3: "09-30", 4: "12-31"}[quarter]
    return f"{year}-{mmdd}"
