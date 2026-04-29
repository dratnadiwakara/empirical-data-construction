"""
IRS SOI Individual Income Tax ZIP Code Data — metadata.
Single source of truth for URLs, era definitions, field mappings.
"""

# ── Year availability ─────────────────────────────────────────────────────────
# IRS does not publish every year; gaps at 1999, 2000, 2003.
AVAILABLE_YEARS: list[int] = (
    [1998, 2001, 2002]
    + list(range(2004, 2023))
)

# Era A: ZIP archive → state-level Excel files or national CSV inside ZIP
# 2010 uses ZIP archive format despite being recent
ERA_A_YEARS: list[int] = [y for y in AVAILABLE_YEARS if y <= 2010]

# Era B: single national CSV
ERA_B_YEARS: list[int] = [y for y in AVAILABLE_YEARS if y >= 2011]

# ── Source URLs ───────────────────────────────────────────────────────────────
def _era_a_url(year: int) -> str:
    return f"https://www.irs.gov/pub/irs-soi/{year}zipcode.zip"


def _era_b_url(year: int) -> str:
    return f"https://www.irs.gov/pub/irs-soi/{str(year)[2:]}zpallagi.csv"


SOURCE_URLS: dict[int, str] = {
    **{y: _era_a_url(y) for y in ERA_A_YEARS},
    **{y: _era_b_url(y) for y in ERA_B_YEARS},
    # 2010 uses ZIP archive despite being in the modern era
    2010: _era_a_url(2010),
}

# ── IRS field codes → harmonized column names ─────────────────────────────────
# These codes are stable across all years (1998–2022).
# All A-prefixed (amount) fields are in $thousands.
IRS_FIELD_MAP: dict[str, str] = {
    "n1":     "n_returns",
    "a00100": "agi_total",
    "n00200": "n_returns_wages",
    "a00200": "agi_wages",
    "n00600": "n_returns_dividend",
    "a00600": "agi_dividend",
    "n00900": "n_returns_business",
    "a00900": "agi_business",
    "n01000": "n_returns_capital_gain",
    "a01000": "agi_capital_gain",
}

# Lowercase source field names (after normalization) that map to harmonized names
SOURCE_FIELDS: list[str] = list(IRS_FIELD_MAP.keys())
HARMONIZED_FIELDS: list[str] = list(IRS_FIELD_MAP.values())

# Fields stored as BIGINT (count columns)
COUNT_FIELDS: set[str] = {
    "n_returns", "n_returns_wages", "n_returns_dividend",
    "n_returns_business", "n_returns_capital_gain",
}

# Fields stored as DOUBLE (dollar amounts, $thousands)
AMOUNT_FIELDS: set[str] = {
    "agi_total", "agi_wages", "agi_dividend", "agi_business", "agi_capital_gain",
}

# ── DuckDB schema ─────────────────────────────────────────────────────────────
PANEL_METADATA_DDL = """
CREATE TABLE IF NOT EXISTS panel_metadata (
    year         INTEGER PRIMARY KEY,
    row_count    BIGINT,
    source_url   VARCHAR,
    built_at     VARCHAR,
    parquet_path VARCHAR
)
"""

# ── Variable descriptions ─────────────────────────────────────────────────────
VARIABLE_DESCRIPTIONS: dict[str, str] = {
    "zipcode":                "5-digit ZIP code (ZCTA; 00000 = statewide total, excluded from panel)",
    "year":                   "Tax year (filing year minus one for most returns)",
    "n_returns":              "Total number of individual income tax returns (IRS field N1)",
    "agi_total":              "Total adjusted gross income, $thousands (IRS field A00100)",
    "n_returns_wages":        "Number of returns with wages and salaries (N00200)",
    "agi_wages":              "Wages and salaries amount, $thousands (A00200)",
    "n_returns_dividend":     "Number of returns with ordinary dividends (N00600)",
    "agi_dividend":           "Ordinary dividends amount, $thousands (A00600)",
    "n_returns_business":     "Number of returns with business/profession net income (N00900)",
    "agi_business":           "Business/profession net income, $thousands (A00900)",
    "n_returns_capital_gain": "Number of returns with net capital gain (N01000)",
    "agi_capital_gain":       "Net capital gain amount, $thousands (A01000)",
}

# ── AGI stub filter ───────────────────────────────────────────────────────────
# Stubs 1–6 are mutually exclusive income brackets; stub 0 is a duplicate total row.
# Always filter to stubs 1–6 before aggregating to avoid double-counting.
# Standard years use stubs 1-6; 2006-2007 use stubs 1-7 (extra split of top bracket).
# The pipeline uses BETWEEN 1 AND 8 to capture all income-bracket rows in all years.
# State/national summary rows use codes ≥9 in older formats and are always excluded.
VALID_AGI_STUBS = (1, 2, 3, 4, 5, 6, 7)  # 7 present in 2006-2007 only

# AGI stub labels (consistent 2004–2022; slightly different in 1998–2002 but
# we aggregate anyway so label differences don't affect numeric comparability)
# Years where IRS published A-series (dollar amount) fields in actual dollars
# rather than the standard $thousands convention used in all other years.
# The pipeline divides these amounts by 1000 at construct time.
AMOUNT_DOLLAR_YEARS: set[int] = {2007, 2008}

AGI_STUB_LABELS: dict[int, str] = {
    1: "No AGI or $1 under $25,000",
    2: "$25,000 under $50,000",
    3: "$50,000 under $75,000",
    4: "$75,000 under $100,000",
    5: "$100,000 under $200,000",
    6: "$200,000 or more",
}
