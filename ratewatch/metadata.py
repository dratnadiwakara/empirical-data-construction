"""
RateWatch metadata: source file paths, schema, product registry helpers.

Single source of truth for:
- where raw files live (D:\\RateWatch_PS_full layout)
- which years come unzipped vs zipped
- raw column list and numeric columns
- panel_metadata DDL
- product_registry.json schema and loader
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Final

from config import RATEWATCH_SOURCE_ROOT

# ── Year coverage ────────────────────────────────────────────────────────────

FIRST_YEAR: Final[int] = 2001
LAST_YEAR: Final[int] = 2024
ALL_YEARS: Final[list[int]] = list(range(LAST_YEAR, FIRST_YEAR - 1, -1))

# Source layouts:
#   PS_full:  D:\RateWatch_PS_full\RW_DepositDataFeedMASTER\  (unzipped 2001-2020)
#   PS_full zip: D:\RateWatch_PS_full\RW_DepositDataFeedMASTER.zip (2021-2023, partial 2023)
#   LSU:      D:\RateWatch_LSU\DepositRateData{YYYY}.txt (2022-2024, full coverage)
#
# Routing:
#   2001-2020: PS_full unzipped
#   2021:      PS_full zip
#   2022-2024: LSU (full year coverage; supersedes PS_full for these years)

UNZIPPED_DIR: Final[Path] = RATEWATCH_SOURCE_ROOT / "RW_DepositDataFeedMASTER"
ZIP_FILE_2021: Final[Path] = RATEWATCH_SOURCE_ROOT / "RW_DepositDataFeedMASTER.zip"
LSU_SOURCE_ROOT: Final[Path] = Path(r"D:\RateWatch_LSU")

UNZIPPED_YEARS: Final[set[int]] = set(range(2001, 2021))
ZIPPED_YEARS: Final[set[int]] = {2021}
LSU_YEARS: Final[set[int]] = {2022, 2023, 2024}


def raw_source_filename(year: int) -> str:
    """Filename of the rate-data text file for a year (no path)."""
    if year in UNZIPPED_YEARS:
        return f"depositRateData_clean_{year}.txt"
    if year in ZIPPED_YEARS or year in LSU_YEARS:
        return f"DepositRateData{year}.txt"
    raise ValueError(f"No source mapping for year {year}")


def raw_source_path(year: int) -> Path:
    """Full path of source rate-data file on D:\\ for unzipped sources."""
    if year in UNZIPPED_YEARS:
        return UNZIPPED_DIR / raw_source_filename(year)
    if year in LSU_YEARS:
        return LSU_SOURCE_ROOT / raw_source_filename(year)
    raise ValueError(f"raw_source_path not defined for year {year} (zipped)")


# ── Support tables (shared across all years) ────────────────────────────────

SUPPORT_FILES: Final[list[str]] = [
    "Deposit_InstitutionDetails.txt",
    "Deposit_acct_join.txt",
    "DepositCertChgHist.txt",
    "DepositsNameChgHist.txt",
]


def support_source_path(name: str) -> Path:
    return UNZIPPED_DIR / name


# ── Raw schema (rate data files) ────────────────────────────────────────────
#
# Two source eras with slightly different column sets:
#   Era A (2001-2020, depositRateData_clean_*): 14 cols with MINTOEARN, MAXTOEARN
#   Era B (2021-2023, DepositRateData*):        13 cols with AMOUNT (replaces MIN/MAX pair)
#
# Pipeline harmonizes both into a single 'amount' column representing the tier
# threshold (= MINTOEARN for era A, = AMOUNT for era B). MAXTOEARN is kept
# nullable, NULL for era B.

ERA_A_YEARS: Final[set[int]] = set(range(2001, 2021))
ERA_B_YEARS: Final[set[int]] = {2021, 2022, 2023, 2024}

RAW_COLUMNS_ERA_A: Final[list[str]] = [
    "ACCOUNTNUMBER", "PRD_TYP_JOIN", "PRODUCTDESCRIPTION", "PRODUCTTYPE",
    "PROD_NM", "PROMO", "MINTOEARN", "MAXTOEARN", "TERMLENGTH", "TERMTYPE",
    "RATE", "APY", "CMT", "DATESURVEYED",
]

RAW_COLUMNS_ERA_B: Final[list[str]] = [
    "ACCOUNTNUMBER", "PRD_TYP_JOIN", "PRODUCTDESCRIPTION", "PRODUCTTYPE",
    "PROD_NM", "PROMO", "AMOUNT", "TERMLENGTH", "TERMTYPE",
    "RATE", "APY", "CMT", "DATESURVEYED",
]

NUMERIC_COLS_ERA_A: Final[set[str]] = {"MINTOEARN", "MAXTOEARN", "TERMLENGTH", "RATE", "APY"}
NUMERIC_COLS_ERA_B: Final[set[str]] = {"AMOUNT", "TERMLENGTH", "RATE", "APY"}


def era_for_year(year: int) -> str:
    if year in ERA_A_YEARS:
        return "A"
    if year in ERA_B_YEARS:
        return "B"
    raise ValueError(f"No era mapping for year {year}")


def raw_columns(year: int) -> list[str]:
    return RAW_COLUMNS_ERA_A if era_for_year(year) == "A" else RAW_COLUMNS_ERA_B


def numeric_cols(year: int) -> set[str]:
    return NUMERIC_COLS_ERA_A if era_for_year(year) == "A" else NUMERIC_COLS_ERA_B


# Tier-defining columns per era (used to choose dominant tier in profile)
TIER_COLS_ERA_A: Final[list[str]] = ["PRODUCTDESCRIPTION", "MINTOEARN", "MAXTOEARN", "TERMLENGTH", "TERMTYPE"]
TIER_COLS_ERA_B: Final[list[str]] = ["PRODUCTDESCRIPTION", "AMOUNT", "TERMLENGTH", "TERMTYPE"]


def tier_cols(year: int) -> list[str]:
    return TIER_COLS_ERA_A if era_for_year(year) == "A" else TIER_COLS_ERA_B

# ── Institution details schema ──────────────────────────────────────────────

INST_DETAILS_COLUMNS: Final[list[str]] = [
    "ACCT_NBR", "INST_NM", "INST_TYP", "CERT_NBR", "UNINUMBR", "RSSD_ID",
    "BRNCH_SRV_TYP", "HO_UNINUMBR", "WEB", "PHONE", "ASSET_SZ", "HD_OFFC",
    "EST_DT", "RTNG_NBR", "ADDRESS", "CITY", "STATE", "ZIP", "COUNTY",
    "BRANCHES", "INSTITUTIONDEPOSITS", "BRANCHDEPOSITS", "LON", "LAT",
    "CNTY_FPS", "STATE_FPS", "MSA", "CBSA", "TM_ZONE",
]

INST_NUMERIC_COLS: Final[set[str]] = {
    "CERT_NBR", "UNINUMBR", "RSSD_ID", "HO_UNINUMBR", "BRNCH_SRV_TYP",
    "ASSET_SZ", "BRANCHES", "INSTITUTIONDEPOSITS", "BRANCHDEPOSITS",
    "LON", "LAT", "MSA", "CBSA",
}

ACCT_JOIN_COLUMNS: Final[list[str]] = [
    "ACCT_NBR_LOC", "ACCT_NBR_RT", "PRD_TYP_JOIN", "EFF_DATE",
]

# ── DuckDB DDL ──────────────────────────────────────────────────────────────

PANEL_METADATA_DDL: Final[str] = """
CREATE TABLE IF NOT EXISTS panel_metadata (
    year         INTEGER PRIMARY KEY,
    row_count    BIGINT,
    n_branches   INTEGER,
    n_products   INTEGER,
    n_weeks      INTEGER,
    source_file  VARCHAR,
    built_at     VARCHAR,
    parquet_path VARCHAR
)
"""

# ── Product registry (filled by profile.py) ─────────────────────────────────

PRODUCT_REGISTRY_PATH: Final[Path] = Path(__file__).parent / "product_registry.json"

# Minimum tier coverage to auto-retain a (product, productdescription) pair
# (fraction of distinct ratesetters in year). Mandatory tiers below ignore this.
MIN_TIER_SHARE: Final[float] = 0.10
MIN_BRANCH_SHARE: Final[float] = MIN_TIER_SHARE  # backward-compat alias

# Mandatory tier descriptions retained regardless of coverage. Map product -> list
# of PRODUCTDESCRIPTION values that must always appear in the panel when present
# in a given year's raw data.
MANDATORY_TIER_DESCRIPTIONS: Final[dict[str, list[str]]] = {
    "CD":    ["12MCD10K"],
    "MM":    ["MM10K", "MM25K"],
    "SAV":   ["SAV2.5K"],
    "INTCK": ["INTCK2.5K"],
}


def load_product_registry() -> dict:
    """Load the committed product registry JSON. Empty dict if missing."""
    if not PRODUCT_REGISTRY_PATH.exists():
        return {}
    return json.loads(PRODUCT_REGISTRY_PATH.read_text(encoding="utf-8"))


def save_product_registry(registry: dict) -> None:
    """Atomic write of product registry."""
    tmp = PRODUCT_REGISTRY_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(registry, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(PRODUCT_REGISTRY_PATH)
