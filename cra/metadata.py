"""
CRA pipeline metadata: download URLs, fixed-width field layouts for all eras,
table ID registry, and harmonized column names.

This module is the single source of truth for all cross-era harmonization logic.

Eras (based on field width changes):
  1996       – 4-char table_id, 4-digit MSA, smaller count/amount fields
  1997-2003  – 5-char table_id, 4-digit MSA, smaller count/amount fields
  2004-2024  – 5-char table_id, 5-digit MSA/MD, 10-digit count/amount fields

Post-2016 change: zip files contain per-table .dat files instead of one big file.
"""
from __future__ import annotations

from typing import Final

# ── Era boundaries ─────────────────────────────────────────────────────────────
FIRST_YEAR: Final[int] = 1996
LAST_YEAR: Final[int] = 2024

ALL_YEARS: Final[list[int]] = list(range(LAST_YEAR, FIRST_YEAR - 1, -1))

# Year when zip contents switched from single file to per-table .dat files
SPLIT_FILE_YEAR: Final[int] = 2016


# ── Download URL registry ────────────────────────────────────────────────────
# Pattern: https://www.ffiec.gov/cra/xls/{YY}exp_{type}.zip
# YY = 2-digit year, type = aggr | trans | discl
CRA_URL_BASE: Final[str] = "https://www.ffiec.gov/cra/xls"

FILE_TYPES: Final[list[str]] = ["aggr", "trans", "discl"]


def get_download_url(year: int, file_type: str) -> str:
    """Return the download URL for a CRA flat file."""
    if file_type not in FILE_TYPES:
        raise ValueError(f"file_type must be one of {FILE_TYPES}, got {file_type!r}")
    yy = f"{year % 100:02d}"
    return f"{CRA_URL_BASE}/{yy}exp_{file_type}.zip"


def get_zip_filename(year: int, file_type: str) -> str:
    """Return the zip filename for a CRA flat file."""
    yy = f"{year % 100:02d}"
    return f"{yy}exp_{file_type}.zip"


# ── Post-2016 per-table .dat filenames ────────────────────────────────────────

# Canonical table IDs and their post-2016 filename stems
AGGREGATE_TABLES: Final[dict[str, str]] = {
    "A1-1":  "A11",
    "A1-1a": "A11a",
    "A1-2":  "A12",
    "A1-2a": "A12a",
    "A2-1":  "A21",
    "A2-1a": "A21a",
    "A2-2":  "A22",
    "A2-2a": "A22a",
}

DISCLOSURE_TABLES: Final[dict[str, str]] = {
    "D1-1": "D11",
    "D1-2": "D12",
    "D2-1": "D21",
    "D2-2": "D22",
    "D3":   "D3",
    "D4":   "D4",
    "D5":   "D5",
    "D6":   "D6",
}

# Pre-2016 combined disclosure files use IDs such as D3-0 / D4-0; match with LIKE prefixes.
DISCLOSURE_TABLE_ID_PREFIXES: Final[list[str]] = [
    "D1-1",
    "D1-2",
    "D2-1",
    "D2-2",
    "D3",
    "D4",
    "D5",
    "D6",
]


def get_dat_filename(year: int, file_type: str, table_stem: str) -> str:
    """Return the .dat filename inside a post-2016 zip for a specific table."""
    type_label = {"aggr": "Aggr", "discl": "Discl"}[file_type]
    return f"cra{year}_{type_label}_{table_stem}.dat"


def is_split_file_year(year: int) -> bool:
    """Return True if the year uses per-table .dat files inside the zip."""
    return year >= SPLIT_FILE_YEAR


def get_era(year: int) -> str:
    """Return the era key for field layout dispatch."""
    if year == 1996:
        return "1996"
    elif year <= 2003:
        return "1997-2003"
    else:
        return "2004+"


# ── Fixed-width layouts ──────────────────────────────────────────────────────
# Format: list of (field_name, start_1indexed, end_1indexed)
# Positions are 1-indexed inclusive (matching the FFIEC specs and R code).

# --------------------------------------------------------------------------- #
# TRANSMITTAL SHEET
# --------------------------------------------------------------------------- #

TRANSMITTAL_LAYOUT_1996: Final[list[tuple[str, int, int]]] = [
    ("respondent_id",    1,  10),
    ("agency_code",     11,  11),
    ("activity_year",   12,  15),
    ("respondent_name", 16,  45),
    ("respondent_addr", 46,  85),
    ("respondent_city", 86, 110),
    ("respondent_state", 111, 112),
    ("respondent_zip",  113, 122),
    ("tax_id",          123, 132),
]

TRANSMITTAL_LAYOUT_1997_PLUS: Final[list[tuple[str, int, int]]] = [
    ("respondent_id",    1,  10),
    ("agency_code",     11,  11),
    ("activity_year",   12,  15),
    ("respondent_name", 16,  45),
    ("respondent_addr", 46,  85),
    ("respondent_city", 86, 110),
    ("respondent_state", 111, 112),
    ("respondent_zip",  113, 122),
    ("tax_id",          123, 132),
    ("rssdid",          133, 142),
    ("assets",          143, 152),
]

TRANSMITTAL_LAYOUTS: Final[dict[str, list]] = {
    "1996":      TRANSMITTAL_LAYOUT_1996,
    "1997-2003": TRANSMITTAL_LAYOUT_1997_PLUS,
    "2004+":     TRANSMITTAL_LAYOUT_1997_PLUS,
}

TRANSMITTAL_SCHEMA: Final[list[str]] = [
    "respondent_id", "agency_code", "activity_year",
    "respondent_name", "respondent_addr", "respondent_city",
    "respondent_state", "respondent_zip", "tax_id",
    "rssdid", "assets",
]

# --------------------------------------------------------------------------- #
# AGGREGATE DATA (tables A1-1, A1-2, A2-1, A2-2 — by tract/county)
# --------------------------------------------------------------------------- #

AGGREGATE_LAYOUT_1996: Final[list[tuple[str, int, int]]] = [
    ("table_id",            1,   4),
    ("activity_year",       5,   8),
    ("loan_type",           9,   9),
    ("action_taken",       10,  10),
    ("state",              11,  12),
    ("county",             13,  15),
    ("msamd",              16,  19),
    ("census_tract",       20,  26),
    ("split_county",       27,  27),
    ("pop_group",          28,  28),
    ("income_group",       29,  31),
    ("report_level",       32,  34),
    ("num_loans_lt_100k",  35,  40),
    ("amt_loans_lt_100k",  41,  48),
    ("num_loans_100k_250k", 49, 54),
    ("amt_loans_100k_250k", 55, 62),
    ("num_loans_250k_1m",  63,  68),
    ("amt_loans_250k_1m",  69,  76),
    ("num_loans_rev_lt_1m", 77, 82),
    ("amt_loans_rev_lt_1m", 83, 90),
]

AGGREGATE_LAYOUT_1997_2003: Final[list[tuple[str, int, int]]] = [
    ("table_id",            1,   5),
    ("activity_year",       6,   9),
    ("loan_type",          10,  10),
    ("action_taken",       11,  11),
    ("state",              12,  13),
    ("county",             14,  16),
    ("msamd",              17,  20),
    ("census_tract",       21,  27),
    ("split_county",       28,  28),
    ("pop_group",          29,  29),
    ("income_group",       30,  32),
    ("report_level",       33,  35),
    ("num_loans_lt_100k",  36,  41),
    ("amt_loans_lt_100k",  42,  49),
    ("num_loans_100k_250k", 50, 55),
    ("amt_loans_100k_250k", 56, 63),
    ("num_loans_250k_1m",  64,  69),
    ("amt_loans_250k_1m",  70,  77),
    ("num_loans_rev_lt_1m", 78, 83),
    ("amt_loans_rev_lt_1m", 84, 91),
]

AGGREGATE_LAYOUT_2004_PLUS: Final[list[tuple[str, int, int]]] = [
    ("table_id",            1,   5),
    ("activity_year",       6,   9),
    ("loan_type",          10,  10),
    ("action_taken",       11,  11),
    ("state",              12,  13),
    ("county",             14,  16),
    ("msamd",              17,  21),
    ("census_tract",       22,  28),
    ("split_county",       29,  29),
    ("pop_group",          30,  30),
    ("income_group",       31,  33),
    ("report_level",       34,  36),
    ("num_loans_lt_100k",  37,  46),
    ("amt_loans_lt_100k",  47,  56),
    ("num_loans_100k_250k", 57, 66),
    ("amt_loans_100k_250k", 67, 76),
    ("num_loans_250k_1m",  77,  86),
    ("amt_loans_250k_1m",  87,  96),
    ("num_loans_rev_lt_1m", 97, 106),
    ("amt_loans_rev_lt_1m", 107, 116),
]

AGGREGATE_LAYOUTS: Final[dict[str, list]] = {
    "1996":      AGGREGATE_LAYOUT_1996,
    "1997-2003": AGGREGATE_LAYOUT_1997_2003,
    "2004+":     AGGREGATE_LAYOUT_2004_PLUS,
}

AGGREGATE_SCHEMA: Final[list[str]] = [
    "table_id", "activity_year", "loan_type", "action_taken",
    "state", "county", "msamd", "census_tract",
    "split_county", "pop_group", "income_group", "report_level",
    "num_loans_lt_100k", "amt_loans_lt_100k",
    "num_loans_100k_250k", "amt_loans_100k_250k",
    "num_loans_250k_1m", "amt_loans_250k_1m",
    "num_loans_rev_lt_1m", "amt_loans_rev_lt_1m",
    "county_fips", "census_tract_fips",
]

# --------------------------------------------------------------------------- #
# DISCLOSURE DATA (tables D1-1, D1-2, D2-1, D2-2 — by bank/county)
# --------------------------------------------------------------------------- #

DISCLOSURE_LAYOUT_1996: Final[list[tuple[str, int, int]]] = [
    ("table_id",            1,   4),
    ("respondent_id",       5,  14),
    ("agency_code",        15,  15),
    ("activity_year",      16,  19),
    ("loan_type",          20,  20),
    ("action_taken",       21,  21),
    ("state",              22,  23),
    ("county",             24,  26),
    ("msamd",              27,  30),
    ("aa_num",             31,  34),
    ("partial_county",     35,  35),
    ("split_county",       36,  36),
    ("pop_group",          37,  37),
    ("income_group",       38,  40),
    ("report_level",       42,  43),
    ("num_loans_lt_100k",  44,  49),
    ("amt_loans_lt_100k",  50,  57),
    ("num_loans_100k_250k", 58, 63),
    ("amt_loans_100k_250k", 64, 71),
    ("num_loans_250k_1m",  72,  77),
    ("amt_loans_250k_1m",  78,  85),
    ("num_loans_rev_lt_1m", 86, 91),
    ("amt_loans_rev_lt_1m", 92, 99),
    ("num_loans_affiliate", 100, 105),
    ("amt_loans_affiliate", 106, 113),
]

DISCLOSURE_LAYOUT_1997_2003: Final[list[tuple[str, int, int]]] = [
    ("table_id",            1,   5),
    ("respondent_id",       6,  15),
    ("agency_code",        16,  16),
    ("activity_year",      17,  20),
    ("loan_type",          21,  21),
    ("action_taken",       22,  22),
    ("state",              23,  24),
    ("county",             25,  27),
    ("msamd",              28,  31),
    ("aa_num",             32,  35),
    ("partial_county",     36,  36),
    ("split_county",       37,  37),
    ("pop_group",          38,  38),
    ("income_group",       39,  41),
    ("report_level",       42,  44),
    ("num_loans_lt_100k",  45,  50),
    ("amt_loans_lt_100k",  51,  58),
    ("num_loans_100k_250k", 59, 64),
    ("amt_loans_100k_250k", 65, 72),
    ("num_loans_250k_1m",  73,  78),
    ("amt_loans_250k_1m",  79,  86),
    ("num_loans_rev_lt_1m", 87, 92),
    ("amt_loans_rev_lt_1m", 93, 100),
    ("num_loans_affiliate", 101, 106),
    ("amt_loans_affiliate", 107, 114),
]

DISCLOSURE_LAYOUT_2004_PLUS: Final[list[tuple[str, int, int]]] = [
    ("table_id",            1,   5),
    ("respondent_id",       6,  15),
    ("agency_code",        16,  16),
    ("activity_year",      17,  20),
    ("loan_type",          21,  21),
    ("action_taken",       22,  22),
    ("state",              23,  24),
    ("county",             25,  27),
    ("msamd",              28,  32),
    ("aa_num",             33,  36),
    ("partial_county",     37,  37),
    ("split_county",       38,  38),
    ("pop_group",          39,  39),
    ("income_group",       40,  42),
    ("report_level",       43,  45),
    ("num_loans_lt_100k",  46,  55),
    ("amt_loans_lt_100k",  56,  65),
    ("num_loans_100k_250k", 66, 75),
    ("amt_loans_100k_250k", 76, 85),
    ("num_loans_250k_1m",  86,  95),
    ("amt_loans_250k_1m",  96, 105),
    ("num_loans_rev_lt_1m", 106, 115),
    ("amt_loans_rev_lt_1m", 116, 125),
    ("num_loans_affiliate", 126, 135),
    ("amt_loans_affiliate", 136, 145),
]

DISCLOSURE_LAYOUTS: Final[dict[str, list]] = {
    "1996":      DISCLOSURE_LAYOUT_1996,
    "1997-2003": DISCLOSURE_LAYOUT_1997_2003,
    "2004+":     DISCLOSURE_LAYOUT_2004_PLUS,
}

DISCLOSURE_SCHEMA: Final[list[str]] = [
    "table_id", "respondent_id", "agency_code", "activity_year",
    "loan_type", "action_taken",
    "state", "county", "msamd", "aa_num",
    "partial_county", "split_county", "pop_group", "income_group",
    "report_level",
    "num_loans_lt_100k", "amt_loans_lt_100k",
    "num_loans_100k_250k", "amt_loans_100k_250k",
    "num_loans_250k_1m", "amt_loans_250k_1m",
    "num_loans_rev_lt_1m", "amt_loans_rev_lt_1m",
    "num_loans_affiliate", "amt_loans_affiliate",
    "county_fips", "census_tract_fips",
]


# ── Geographic SQL expressions ────────────────────────────────────────────────

COUNTY_FIPS_SQL: Final[str] = (
    "CASE WHEN TRIM(state) = '' OR state IS NULL THEN NULL"
    " ELSE LPAD(TRIM(state), 2, '0') || LPAD(TRIM(county), 3, '0')"
    " END"
)

CENSUS_TRACT_FIPS_SQL: Final[str] = (
    "CASE WHEN TRIM(census_tract) = '' OR census_tract IS NULL"
    " OR TRIM(state) = '' OR state IS NULL"
    " THEN NULL"
    " ELSE LPAD(TRIM(state), 2, '0')"
    " || LPAD(TRIM(county), 3, '0')"
    " || LPAD(REPLACE(TRIM(census_tract), '.', ''), 6, '0')"
    " END"
)


# ── Numeric columns (cast to BIGINT during ETL) ──────────────────────────────

AGGREGATE_NUMERIC_COLS: Final[list[str]] = [
    "num_loans_lt_100k", "amt_loans_lt_100k",
    "num_loans_100k_250k", "amt_loans_100k_250k",
    "num_loans_250k_1m", "amt_loans_250k_1m",
    "num_loans_rev_lt_1m", "amt_loans_rev_lt_1m",
]

DISCLOSURE_NUMERIC_COLS: Final[list[str]] = [
    "num_loans_lt_100k", "amt_loans_lt_100k",
    "num_loans_100k_250k", "amt_loans_100k_250k",
    "num_loans_250k_1m", "amt_loans_250k_1m",
    "num_loans_rev_lt_1m", "amt_loans_rev_lt_1m",
    "num_loans_affiliate", "amt_loans_affiliate",
]

TRANSMITTAL_NUMERIC_COLS: Final[list[str]] = [
    "rssdid", "assets",
]


# ── Table ID normalization ────────────────────────────────────────────────────

def normalize_table_id(raw_id: str) -> str:
    """Normalize a table ID to canonical form: strip whitespace."""
    return raw_id.strip()


# ── Panel metadata DDL ───────────────────────────────────────────────────────

PANEL_METADATA_DDL: Final[str] = """
CREATE TABLE IF NOT EXISTS panel_metadata (
    table_type      VARCHAR,
    year            INTEGER,
    row_count       BIGINT,
    source_url      VARCHAR,
    built_at        TIMESTAMP,
    parquet_path    VARCHAR,
    PRIMARY KEY (table_type, year)
)
"""


# ── 2024 Validation targets (from National Aggregate Table 1) ────────────────

VALIDATION_2024: Final[dict] = {
    "business_originations": {
        "table_id": "A1-1",
        "loan_type": 4,
        "action_taken": 1,
        "num_loans_lt_100k": 8_300_199,
        "num_loans_100k_250k": 243_526,
        "num_loans_250k_1m": 190_537,
        "total_num": 8_734_262,
        "amt_loans_lt_100k": 114_749_317,
        "amt_loans_100k_250k": 40_555_584,
        "amt_loans_250k_1m": 102_536_392,
        "total_amt": 257_841_293,
        "num_loans_rev_lt_1m": 4_700_002,
        "amt_loans_rev_lt_1m": 89_132_044,
    },
    "business_purchases": {
        "table_id": "A1-2",
        "loan_type": 4,
        "action_taken": 6,
        "num_loans_lt_100k": 332_194,
        "num_loans_100k_250k": 26_266,
        "num_loans_250k_1m": 13_486,
        "total_num": 371_946,
    },
    "farm_originations": {
        "table_id": "A2-1",
        "loan_type": 5,
        "action_taken": 1,
        "num_loans_lt_100k": 154_587,
        "num_loans_100k_250k": 24_914,
        "num_loans_250k_1m": 15_987,
        "total_num": 195_488,
    },
}
