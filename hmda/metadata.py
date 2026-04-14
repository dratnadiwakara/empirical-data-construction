"""
HMDA LAR pipeline metadata: download URLs, column mappings, unit-scaling rules,
regime-shift registry, and NARA fixed-width field layout.

This module is the single source of truth for all cross-era harmonization logic.
Every rename, unit conversion, and data-source URL is documented here so that
the ETL in construct.py is fully auditable.
"""
from __future__ import annotations

from typing import Final

# ── Era boundaries ─────────────────────────────────────────────────────────────
FIRST_YEAR: Final[int] = 2007
LAST_YEAR: Final[int] = 2024

ALL_YEARS: Final[list[int]] = list(range(LAST_YEAR, FIRST_YEAR - 1, -1))  # 2024 → 2007

# CFPB historic data portal years (comma-delimited CSV, labeled values, header row)
CFPB_HISTORIC_FIRST_YEAR: Final[int] = 2007
CFPB_HISTORIC_LAST_YEAR: Final[int] = 2016


# ── Download URL registry ──────────────────────────────────────────────────────
# For each year, URLs are tried in order; first non-404 with valid Content-Length wins.
# NARA years (2004-2006) use a separate scraping path (see download.py).

SNAPSHOT_FFIEC_TMPL: Final[str] = (
    "https://files.ffiec.cfpb.gov/static-data/snapshot"
    "/{year}/{year}_public_lar_pipe.zip"
)
# 2017 uses a different filename convention (_txt.zip, not _pipe.zip)
SNAPSHOT_2017_URL: Final[str] = (
    "https://files.ffiec.cfpb.gov/static-data/snapshot/2017/2017_public_lar_txt.zip"
)
# Legacy S3 templates kept as comments — bucket now returns 403
# SNAPSHOT_S3_TMPL = "https://s3.amazonaws.com/cfpb-hmda-public/prod/snapshot-data/{year}/..."
# THREE_YEAR_S3_TMPL = "https://s3.amazonaws.com/cfpb-hmda-public/prod/three-year-data/{year}/..."
FFIEC_NATIONAL_TMPL: Final[str] = (
    "https://www.ffiec.gov/hmdarawdata/LAR/National"
    "/{year}HMDALAR%20-%20National.zip"
)
CFPB_HISTORIC_TMPL: Final[str] = (
    "https://files.consumerfinance.gov/hmda-historic-loan-data"
    "/hmda_{year}_nationwide_all-records_labels.zip"
)
CFPB_HISTORIC_ALT_TMPL: Final[str] = (
    "https://files.consumerfinance.gov/hmda-historic-loan-data"
    "/hmda_{year}_nationwide_first-lien-owner-occupied-1-4-family-records_labels.zip"
)

# NARA National Archives catalog record IDs for 2004-2006 LAR ultimate files
NARA_ULTIMATE_IDS: Final[dict[int, int]] = {
    2006: 6850584,
    2005: 6850582,
    2004: 5716418,
}
NARA_CATALOG_TMPL: Final[str] = "https://catalog.archives.gov/id/{nara_id}"
# NARA OPA API (returns JSON with download URLs — more reliable than HTML scraping)
NARA_API_TMPL: Final[str] = (
    "https://catalog.archives.gov/api/v1/?naIds={nara_id}&type=object"
)


def get_source_urls(year: int) -> list[str]:
    """
    Return an ordered list of candidate download URLs for a given year.
    Supported range: 2007-2024.

    Era routing:
      2018-2024 → FFIEC snapshot pipe file
      2017      → FFIEC snapshot txt file (different filename convention)
      2007-2016 → CFPB historic data portal (comma-delimited labeled CSV)
    """
    if year > 2024 or year < 2007:
        raise ValueError(f"Year {year} outside supported range 2007-2024")
    if year == 2017:
        return [SNAPSHOT_2017_URL]
    if year >= 2018:
        return [SNAPSHOT_FFIEC_TMPL.format(year=year)]
    # 2007-2016: CFPB historic data portal
    return [CFPB_HISTORIC_TMPL.format(year=year)]


def is_pipe_delimited(year: int) -> bool:
    """2017-2024 are pipe-delimited; 2007-2016 CFPB historic files are comma-delimited."""
    return year >= 2017


def get_delimiter(year: int) -> str:
    """Return the field delimiter character for the raw LAR file of the given year."""
    return "|" if year >= 2017 else ","


def is_pre_2018(year: int) -> bool:
    """Return True for years using the pre-reform HMDA LAR schema (2017 and earlier)."""
    return year < 2018


def is_cfpb_historic(year: int) -> bool:
    """Return True for years sourced from the CFPB historic data portal (2007-2016).

    These files differ from the 2017 FFIEC file in three ways:
      1. Comma-delimited (not pipe)
      2. Have a header row (unlike the 2017 FFIEC no-header file)
      3. May use text labels for categorical values (e.g., 'Refinancing' not '3')
    """
    return CFPB_HISTORIC_FIRST_YEAR <= year <= CFPB_HISTORIC_LAST_YEAR


# ── Master schema (2024 anchor) ────────────────────────────────────────────────
# This list is the hardcoded fallback used when the 2024 CSV hasn't been
# downloaded yet. The actual master schema is always read from the 2024 file header
# at runtime by construct.py:get_master_schema_from_2024().
#
# Column order matches the 2024 CFPB public LAR pipe-delimited file.
# Source: https://ffiec.cfpb.gov/documentation/publications/loan-level-datasets/lar-data-fields/
MASTER_SCHEMA: Final[list[str]] = [
    "activity_year",
    "lei",
    "derived_msa_md",
    "state_code",
    "county_code",
    "census_tract",
    "derived_loan_product_type",
    "derived_dwelling_category",
    "conforming_loan_limit",
    "derived_ethnicity",
    "derived_race",
    "derived_sex",
    "action_taken",
    "purchaser_type",
    "loan_type",
    "loan_purpose",
    "lien_status",
    "reverse_mortgage",
    "open_end_line_of_credit",
    "business_or_commercial_purpose",
    "loan_amount",
    "combined_loan_to_value_ratio",
    "interest_rate",
    "rate_spread",
    "hoepa_status",
    "total_loan_costs",
    "total_points_and_fees",
    "origination_charges",
    "discount_points",
    "lender_credits",
    "loan_term",
    "prepayment_penalty_term",
    "intro_rate_period",
    "negative_amortization",
    "interest_only_payment",
    "balloon_payment",
    "other_nonamortizing_features",
    "property_value",
    "construction_method",
    "occupancy_type",
    "manufactured_home_secured_property_type",
    "manufactured_home_land_property_interest",
    "total_units",
    "multifamily_affordable_units",
    "income",
    "debt_to_income_ratio",
    "applicant_credit_score_type",
    "co_applicant_credit_score_type",
    "applicant_ethnicity_1",
    "applicant_ethnicity_2",
    "applicant_ethnicity_3",
    "applicant_ethnicity_4",
    "applicant_ethnicity_5",
    "co_applicant_ethnicity_1",
    "co_applicant_ethnicity_2",
    "co_applicant_ethnicity_3",
    "co_applicant_ethnicity_4",
    "co_applicant_ethnicity_5",
    "applicant_ethnicity_observed",
    "co_applicant_ethnicity_observed",
    "applicant_race_1",
    "applicant_race_2",
    "applicant_race_3",
    "applicant_race_4",
    "applicant_race_5",
    "co_applicant_race_1",
    "co_applicant_race_2",
    "co_applicant_race_3",
    "co_applicant_race_4",
    "co_applicant_race_5",
    "applicant_race_observed",
    "co_applicant_race_observed",
    "applicant_sex",
    "co_applicant_sex",
    "applicant_sex_observed",
    "co_applicant_sex_observed",
    "applicant_age",
    "co_applicant_age",
    "applicant_age_above_62",
    "co_applicant_age_above_62",
    "submission_of_application",
    "initially_payable_to_institution",
    "aus_1",
    "aus_2",
    "aus_3",
    "aus_4",
    "aus_5",
    "denial_reason_1",
    "denial_reason_2",
    "denial_reason_3",
    "denial_reason_4",
    "preapproval",
    "tract_population",
    "tract_minority_population_percent",
    "ffiec_msa_md_median_family_income",
    "tract_to_msa_income_percentage",
    "tract_owner_occupied_units",
    "tract_one_to_four_family_homes",
    "tract_median_age_of_housing_units",
]

# Supplemental identifier columns added by Avery join (appended after master schema)
AVERY_SUPPLEMENT_COLS: Final[list[str]] = [
    "respondent_id",    # pre-2018 LAR identifier (kept for traceability)
    "agency_code",      # pre-2018 regulator code
    "rssd_id",          # from Avery crosswalk
    "parent_rssd",      # from Avery crosswalk
    "top_holder_rssd",  # from Avery crosswalk
]

# Complete output schema = master + avery supplements
OUTPUT_SCHEMA: Final[list[str]] = MASTER_SCHEMA + AVERY_SUPPLEMENT_COLS


# ── Column rename mapping (pre-2018 source → master schema name) ───────────────
# Key   = column name as it appears in the raw source file
# Value = the corresponding master-schema column name
#
# Notes:
# - Many columns are identical across eras and do NOT appear here.
# - The 2017 CFPB three-year pipe file already uses some post-2018 names.
# - Raw FFIEC/NARA files use _000s suffix; CFPB labeled CSVs already use clean names.
COLUMN_RENAMES: Final[dict[str, str]] = {
    # ── Separator changes: underscore → hyphen ──────────────────────────────
    "applicant_ethnicity_1":        "applicant_ethnicity-1",
    "applicant_ethnicity_2":        "applicant_ethnicity-2",
    "applicant_ethnicity_3":        "applicant_ethnicity-3",
    "applicant_ethnicity_4":        "applicant_ethnicity-4",
    "applicant_ethnicity_5":        "applicant_ethnicity-5",
    "co_applicant_ethnicity_1":     "co-applicant_ethnicity-1",
    "co_applicant_ethnicity_2":     "co-applicant_ethnicity-2",
    "co_applicant_ethnicity_3":     "co-applicant_ethnicity-3",
    "co_applicant_ethnicity_4":     "co-applicant_ethnicity-4",
    "co_applicant_ethnicity_5":     "co-applicant_ethnicity-5",
    "applicant_race_1":             "applicant_race-1",
    "applicant_race_2":             "applicant_race-2",
    "applicant_race_3":             "applicant_race-3",
    "applicant_race_4":             "applicant_race-4",
    "applicant_race_5":             "applicant_race-5",
    "co_applicant_race_1":          "co-applicant_race-1",
    "co_applicant_race_2":          "co-applicant_race-2",
    "co_applicant_race_3":          "co-applicant_race-3",
    "co_applicant_race_4":          "co-applicant_race-4",
    "co_applicant_race_5":          "co-applicant_race-5",
    "co_applicant_sex":             "co-applicant_sex",
    "denial_reason_1":              "denial_reason-1",
    "denial_reason_2":              "denial_reason-2",
    "denial_reason_3":              "denial_reason-3",
    # ── Geographic ──────────────────────────────────────────────────────────
    "msa_md":                       "derived_msa-md",
    "msa":                          "derived_msa-md",   # alternate name in some years
    # ── Census tract ────────────────────────────────────────────────────────
    "tract_one_to_four_family_housing_units": "tract_one_to_four_family_homes",
    # ── Raw FFIEC / NARA field names ─────────────────────────────────────────
    # CFPB labeled CSVs already use "income" and "loan_amount"
    "applicant_income_000s":        "income",
    "loan_amount_000s":             "loan_amount",
    # ── Occupancy / property ─────────────────────────────────────────────────
    "owner_occupancy":              "occupancy_type",
    # ── 2017 transitional names (three-year pipe file) ───────────────────────
    # The 2017 CFPB file mostly uses post-2018 names already; no renames needed.
}

# ── 2017-specific column rename mapping ───────────────────────────────────────
# Maps raw 2017 FFIEC pipe-delimited column names → MASTER_SCHEMA column names.
# Only entries that DIFFER from the master schema name need to appear here.
# Columns that are identical in source and master schema are handled by identity.
#
# Note: The COLUMN_RENAMES dict above was designed for NARA 2004-2006 files and
# uses some non-standard target names. Use COLUMN_RENAMES_2017 for the 2017 file.
# ── 2017 file column order ─────────────────────────────────────────────────────
# The 2017 FFIEC pipe-delimited LAR file has NO header row.
# Columns must be supplied by name. Order confirmed from 2017_public_lar.txt.
# Source: FFIEC HMDA LAR Filing Instructions Guide 2017 + empirical inspection.
# Columns 37-38 (edit_status, sequence_number) are blank in the public file.
# Column 45 is a trailing null field with no documented meaning — dropped.
COLUMNS_2017: Final[list[str]] = [
    "as_of_year",                           #  1
    "respondent_id",                        #  2  10-char, zero-padded
    "agency_code",                          #  3  1=OCC,2=FRS,3=FDIC,5=NCUA,7=HUD,9=CFPB
    "loan_type",                            #  4
    "property_type",                        #  5  1=SFR,2=Manufactured,3=Multifamily
    "loan_purpose",                         #  6  1=Purchase,2=Improvement,3=Refinance
    "owner_occupancy",                      #  7  → occupancy_type
    "loan_amount_000s",                     #  8  in $000s; ETL scales ×1000
    "preapproval",                          #  9
    "action_taken",                         # 10
    "msa_md",                               # 11  5-char MSA/MD code
    "state_code",                           # 12  2-char FIPS
    "county_code",                          # 13  3-char FIPS
    "census_tract",                         # 14  XXXX.XX format; ETL constructs 11-char FIPS
    "applicant_ethnicity",                  # 15  → applicant_ethnicity_1
    "co_applicant_ethnicity",               # 16  → co_applicant_ethnicity_1
    "applicant_race_1",                     # 17
    "applicant_race_2",                     # 18
    "applicant_race_3",                     # 19
    "applicant_race_4",                     # 20
    "applicant_race_5",                     # 21
    "co_applicant_race_1",                  # 22
    "co_applicant_race_2",                  # 23
    "co_applicant_race_3",                  # 24
    "co_applicant_race_4",                  # 25
    "co_applicant_race_5",                  # 26
    "applicant_sex",                        # 27
    "co_applicant_sex",                     # 28
    "applicant_income_000s",                # 29  → income (kept in $000s same as 2018+)
    "purchaser_type",                       # 30
    "denial_reason_1",                      # 31
    "denial_reason_2",                      # 32
    "denial_reason_3",                      # 33
    "rate_spread",                          # 34
    "hoepa_status",                         # 35
    "lien_status",                          # 36
    "edit_status",                          # 37  blank in public file; dropped
    "sequence_number",                      # 38  blank in public file; dropped
    "tract_population",                     # 39
    "tract_minority_population_percent",    # 40
    "ffiec_msa_md_median_family_income",    # 41
    "tract_to_msa_income_percentage",       # 42
    "tract_owner_occupied_units",           # 43
    "tract_one_to_four_family_housing_units",  # 44 → tract_one_to_four_family_homes
    "col_45_unused",                        # 45  trailing null field; dropped
]


# ── 2017-specific column rename mapping ───────────────────────────────────────
# Maps raw 2017 FFIEC pipe-delimited column names → MASTER_SCHEMA column names.
COLUMN_RENAMES_2017: Final[dict[str, str]] = {
    # Year field
    "as_of_year":               "activity_year",
    # Occupancy
    "owner_occupancy":          "occupancy_type",
    # Loan amount (also scaled ×1000 in ETL; handled specially in construct.py)
    "loan_amount_000s":         "loan_amount",
    # Income (rename only; kept in $000s same as 2018+)
    "applicant_income_000s":    "income",
    # MSA/MD
    "msa_md":                   "derived_msa_md",
    "msa":                      "derived_msa_md",
    # Ethnicity (some 2017 files use singular form without _1 suffix)
    "applicant_ethnicity":      "applicant_ethnicity_1",
    "co_applicant_ethnicity":   "co_applicant_ethnicity_1",
    # Census tract supplemental rename
    "tract_one_to_four_family_housing_units": "tract_one_to_four_family_homes",
}


# ── CFPB historic (2007-2016) column rename mapping ───────────────────────────
# Maps raw CFPB CSV column names → MASTER_SCHEMA column names.
# The CFPB historic files have a header row; column names are read at runtime.
# Names below cover all known variants seen across different years.
# Identity mappings (raw == master) are omitted.
COLUMN_RENAMES_CFPB_HISTORIC: Final[dict[str, str]] = {
    # Year
    "as_of_year":                           "activity_year",
    "year":                                 "activity_year",
    # Occupancy
    "owner_occupancy":                      "occupancy_type",
    "occupancy":                            "occupancy_type",
    # Loan amount ($000s in CFPB historic — scaled ×1000 at ETL)
    "loan_amount_000s":                     "loan_amount",
    # Income (rename only; kept in $000s — same as 2018+)
    "applicant_income_000s":                "income",
    # MSA/MD — CFPB historic uses 'msamd' (without underscore), not 'msa_md'
    "msamd":                                "derived_msa_md",
    "msa_md":                               "derived_msa_md",
    "msa":                                  "derived_msa_md",
    # Census tract (various forms; also gets 11-char FIPS construction)
    "census_tract_number":                  "census_tract",
    "censustract":                          "census_tract",
    # Ethnicity (singular form → numbered form)
    "applicant_ethnicity":                  "applicant_ethnicity_1",
    "co_applicant_ethnicity":               "co_applicant_ethnicity_1",
    # Census data — CFPB historic uses different column names than master schema
    "population":                           "tract_population",
    "minority_population":                  "tract_minority_population_percent",
    "minority_population_percent":          "tract_minority_population_percent",
    "hud_median_family_income":             "ffiec_msa_md_median_family_income",
    "ffiec_median_family_income":           "ffiec_msa_md_median_family_income",
    "tract_to_msamd_income":                "tract_to_msa_income_percentage",
    "tract_to_msa_income_percent":          "tract_to_msa_income_percentage",
    "number_of_owner_occupied_units":       "tract_owner_occupied_units",
    "number_of_1_to_4_family_units":        "tract_one_to_four_family_homes",
    "tract_one_to_four_family_housing_units": "tract_one_to_four_family_homes",
}

# Columns in CFPB historic CSVs that have no master-schema equivalent → DROP.
# The CFPB "labels" files include both code columns AND label columns for each field.
# Label columns (e.g., loan_type_name, action_taken_name) are dropped — we keep codes.
# Geographic label columns (state_name, county_name, etc.) are also dropped.
# edit_status/sequence_number are blank in public files.
# application_date_indicator is CFPB-only; not in master schema.
COLS_TO_DROP_CFPB_HISTORIC: Final[set[str]] = {
    # Label (text) columns — parallel code columns are kept
    "agency_name", "agency_abbr",
    "loan_type_name",
    "property_type_name",
    "loan_purpose_name",
    "owner_occupancy_name",
    "preapproval_name",
    "action_taken_name",
    "msamd_name",
    "state_name", "state_abbr",
    "county_name",
    "applicant_ethnicity_name",
    "co_applicant_ethnicity_name",
    "applicant_race_name_1", "applicant_race_name_2", "applicant_race_name_3",
    "applicant_race_name_4", "applicant_race_name_5",
    "co_applicant_race_name_1", "co_applicant_race_name_2", "co_applicant_race_name_3",
    "co_applicant_race_name_4", "co_applicant_race_name_5",
    "applicant_sex_name",
    "co_applicant_sex_name",
    "purchaser_type_name",
    "denial_reason_name_1", "denial_reason_name_2", "denial_reason_name_3",
    "hoepa_status_name",
    "lien_status_name",
    "edit_status_name",
    # Internal/metadata columns
    "edit_status",
    "sequence_number",
    "application_date_indicator",
}

# ── Label-to-code mapping for CFPB historic (2007-2016) ───────────────────────
# The CFPB "labels" CSV files use text strings for categorical values.
# Example: loan_purpose = "Refinancing" instead of "3".
# The ETL converts these to numeric codes to match the 2017 FFIEC file format.
#
# Structure: {master_col_name: {text_label_lower: numeric_code}}
# All label keys are stored lowercase; matching at ETL time uses LOWER(TRIM(...)).
# "ELSE col" in the generated SQL handles values that are already numeric codes.
#
# Source: FFIEC LAR Record Codes PDF + CFPB HMDA documentation.
LABEL_TO_CODE: Final[dict[str, dict[str, str]]] = {
    "action_taken": {
        "loan originated": "1",
        "application approved but not accepted": "2",
        "application denied by financial institution": "3",
        "application denied": "3",
        "application withdrawn by applicant": "4",
        "application withdrawn": "4",
        "file closed for incompleteness": "5",
        "loan purchased by the institution": "6",
        "loan purchased": "6",
        "preapproval request denied by financial institution": "7",
        "preapproval request denied": "7",
        "preapproval request approved but not accepted (optional reporting)": "8",
        "preapproval request approved but not accepted": "8",
    },
    "loan_type": {
        "conventional": "1",
        "fha-insured": "2",
        "fha insured": "2",
        "va-guaranteed": "3",
        "va guaranteed": "3",
        "fsa/rhs-guaranteed": "4",
        "fsa/rhs guaranteed": "4",
        "fsa/rhs": "4",
    },
    "property_type": {
        "one to four-family (other than manufactured housing)": "1",
        "one-to-four family dwelling (other than manufactured housing)": "1",
        "one to four-family dwelling (other than manufactured housing)": "1",
        "1-4 family dwelling (other than manufactured housing)": "1",
        "manufactured housing": "2",
        "multifamily dwelling": "3",
        "multifamily": "3",
    },
    "loan_purpose": {
        "home purchase": "1",
        "home improvement": "2",
        "refinancing": "3",
    },
    "occupancy_type": {
        "owner-occupied as a principal dwelling": "1",
        "owner occupied as a principal dwelling": "1",
        "not owner-occupied as a principal dwelling": "2",
        "not owner occupied as a principal dwelling": "2",
        "not applicable": "3",
    },
    "preapproval": {
        "preapproval was requested": "1",
        "preapproval was not requested": "2",
        "not applicable": "3",
    },
    "purchaser_type": {
        "loan was not originated or was not sold in calendar year covered by register": "0",
        "loan was not originated or was not sold in calendar year": "0",
        "not originated or sold": "0",
        "fannie mae (fnma)": "1",
        "fannie mae": "1",
        "ginnie mae (gnma)": "2",
        "ginnie mae": "2",
        "freddie mac (fhlmc)": "3",
        "freddie mac": "3",
        "farmer mac (famc)": "4",
        "farmer mac": "4",
        "private securitization": "5",
        "commercial bank, savings bank or savings association": "6",
        "commercial bank or savings institution": "6",
        "life insurance company, credit union, mortgage bank, or finance company": "7",
        "life insurance company, credit union, mortgage company, or finance company": "7",
        "affiliate institution": "8",
        "other type of purchaser": "9",
    },
    "denial_reason_1": {
        "debt-to-income ratio": "1",
        "employment history": "2",
        "credit history": "3",
        "collateral": "4",
        "insufficient cash (downpayment, closing costs)": "5",
        "insufficient cash": "5",
        "unverifiable information": "6",
        "credit application incomplete": "7",
        "mortgage insurance denied": "8",
        "other": "9",
        "na": "0",
        "not applicable": "0",
    },
    "denial_reason_2": {
        "debt-to-income ratio": "1",
        "employment history": "2",
        "credit history": "3",
        "collateral": "4",
        "insufficient cash (downpayment, closing costs)": "5",
        "insufficient cash": "5",
        "unverifiable information": "6",
        "credit application incomplete": "7",
        "mortgage insurance denied": "8",
        "other": "9",
        "na": "0",
        "not applicable": "0",
    },
    "denial_reason_3": {
        "debt-to-income ratio": "1",
        "employment history": "2",
        "credit history": "3",
        "collateral": "4",
        "insufficient cash (downpayment, closing costs)": "5",
        "insufficient cash": "5",
        "unverifiable information": "6",
        "credit application incomplete": "7",
        "mortgage insurance denied": "8",
        "other": "9",
        "na": "0",
        "not applicable": "0",
    },
    "hoepa_status": {
        "hoepa loan": "1",
        "not a hoepa loan": "2",
    },
    "lien_status": {
        "secured by a first lien": "1",
        "secured by a subordinate lien": "2",
        "not secured by a lien": "3",
        "not applicable (purchased loans)": "4",
        "not applicable": "4",
    },
    "applicant_ethnicity_1": {
        "hispanic or latino": "1",
        "not hispanic or latino": "2",
        "information not provided by applicant in mail, internet, or telephone application": "3",
        "information not provided": "3",
        "not applicable": "4",
        "no co-applicant": "5",
    },
    "co_applicant_ethnicity_1": {
        "hispanic or latino": "1",
        "not hispanic or latino": "2",
        "information not provided by applicant in mail, internet, or telephone application": "3",
        "information not provided": "3",
        "not applicable": "4",
        "no co-applicant": "5",
    },
    "applicant_race_1": {
        "american indian or alaska native": "1",
        "asian": "2",
        "black or african american": "3",
        "native hawaiian or other pacific islander": "4",
        "white": "5",
        "information not provided by applicant in mail, internet, or telephone application": "6",
        "information not provided": "6",
        "not applicable": "7",
        "no co-applicant": "8",
    },
    "applicant_race_2": {
        "american indian or alaska native": "1",
        "asian": "2",
        "black or african american": "3",
        "native hawaiian or other pacific islander": "4",
        "white": "5",
        "information not provided by applicant in mail, internet, or telephone application": "6",
        "information not provided": "6",
        "not applicable": "7",
        "no co-applicant": "8",
    },
    "applicant_race_3": {
        "american indian or alaska native": "1",
        "asian": "2",
        "black or african american": "3",
        "native hawaiian or other pacific islander": "4",
        "white": "5",
        "information not provided by applicant in mail, internet, or telephone application": "6",
        "information not provided": "6",
        "not applicable": "7",
        "no co-applicant": "8",
    },
    "applicant_race_4": {
        "american indian or alaska native": "1",
        "asian": "2",
        "black or african american": "3",
        "native hawaiian or other pacific islander": "4",
        "white": "5",
        "information not provided by applicant in mail, internet, or telephone application": "6",
        "information not provided": "6",
        "not applicable": "7",
        "no co-applicant": "8",
    },
    "applicant_race_5": {
        "american indian or alaska native": "1",
        "asian": "2",
        "black or african american": "3",
        "native hawaiian or other pacific islander": "4",
        "white": "5",
        "information not provided by applicant in mail, internet, or telephone application": "6",
        "information not provided": "6",
        "not applicable": "7",
        "no co-applicant": "8",
    },
    "co_applicant_race_1": {
        "american indian or alaska native": "1",
        "asian": "2",
        "black or african american": "3",
        "native hawaiian or other pacific islander": "4",
        "white": "5",
        "information not provided by applicant in mail, internet, or telephone application": "6",
        "information not provided": "6",
        "not applicable": "7",
        "no co-applicant": "8",
    },
    "co_applicant_race_2": {
        "american indian or alaska native": "1",
        "asian": "2",
        "black or african american": "3",
        "native hawaiian or other pacific islander": "4",
        "white": "5",
        "information not provided by applicant in mail, internet, or telephone application": "6",
        "information not provided": "6",
        "not applicable": "7",
        "no co-applicant": "8",
    },
    "co_applicant_race_3": {
        "american indian or alaska native": "1",
        "asian": "2",
        "black or african american": "3",
        "native hawaiian or other pacific islander": "4",
        "white": "5",
        "information not provided by applicant in mail, internet, or telephone application": "6",
        "information not provided": "6",
        "not applicable": "7",
        "no co-applicant": "8",
    },
    "co_applicant_race_4": {
        "american indian or alaska native": "1",
        "asian": "2",
        "black or african american": "3",
        "native hawaiian or other pacific islander": "4",
        "white": "5",
        "information not provided by applicant in mail, internet, or telephone application": "6",
        "information not provided": "6",
        "not applicable": "7",
        "no co-applicant": "8",
    },
    "co_applicant_race_5": {
        "american indian or alaska native": "1",
        "asian": "2",
        "black or african american": "3",
        "native hawaiian or other pacific islander": "4",
        "white": "5",
        "information not provided by applicant in mail, internet, or telephone application": "6",
        "information not provided": "6",
        "not applicable": "7",
        "no co-applicant": "8",
    },
    "applicant_sex": {
        "male": "1",
        "female": "2",
        "information not provided by applicant in mail, internet, or telephone application": "3",
        "information not provided": "3",
        "not applicable": "4",
        "no co-applicant": "5",
    },
    "co_applicant_sex": {
        "male": "1",
        "female": "2",
        "information not provided by applicant in mail, internet, or telephone application": "3",
        "information not provided": "3",
        "not applicable": "4",
        "no co-applicant": "5",
    },
    "agency_code": {
        "occ": "1",
        "frs": "2",
        "fdic": "3",
        "ncua": "5",
        "hud": "7",
        "cfpb": "9",
    },
}


def build_label_case_sql(raw_col: str, master_col: str) -> str:
    """
    Generate a DuckDB SQL CASE expression that converts text labels to numeric codes.

    The generated CASE handles BOTH numeric codes (pass-through) AND text labels.
    This makes the ETL robust if a year uses codes instead of labels, or vice versa.

    Example output for loan_purpose:
        CASE
            WHEN TRIM("loan_purpose") IN ('1','2','3') THEN TRIM("loan_purpose")
            WHEN LOWER(TRIM("loan_purpose")) = 'home purchase' THEN '1'
            WHEN LOWER(TRIM("loan_purpose")) = 'home improvement' THEN '2'
            WHEN LOWER(TRIM("loan_purpose")) = 'refinancing' THEN '3'
            ELSE TRIM("loan_purpose")
        END
    """
    mapping = LABEL_TO_CODE.get(master_col, {})
    if not mapping:
        # No label conversion needed; just trim
        return f'TRIM("{raw_col}")'

    codes = sorted(set(mapping.values()))
    code_list = ", ".join(f"'{c}'" for c in codes)
    code_check = (
        f"WHEN TRIM(\"{raw_col}\") IN ({code_list}) THEN TRIM(\"{raw_col}\")"
    )
    label_checks = "\n            ".join(
        f"WHEN LOWER(TRIM(\"{raw_col}\")) = '{label}' THEN '{code}'"
        for label, code in mapping.items()
    )
    return (
        f"CASE\n"
        f"            {code_check}\n"
        f"            {label_checks}\n"
        f"            ELSE TRIM(\"{raw_col}\")\n"
        f"        END"
    )


# Columns in CFPB historic files that need label-to-code conversion.
# These are the master_col names (after renaming) that appear in LABEL_TO_CODE.
CFPB_HISTORIC_CATEGORICAL_COLS: Final[set[str]] = set(LABEL_TO_CODE.keys())


# Columns present in pre-2018 LAR that have no master-schema equivalent.
# These are retained as AVERY_SUPPLEMENT_COLS if used for joining; otherwise dropped.
PRE_2018_ONLY_COLS: Final[list[str]] = [
    "respondent_id",
    "agency_code",
    "property_type",          # subsumed by derived_dwelling_category in 2018+
]

# Columns in master schema that were NOT collected before 2018.
# For pre-2018 rows these will be NULL in the output.
POST_2018_ONLY_COLS: Final[list[str]] = [
    "lei",
    "derived_loan_product_type",
    "derived_dwelling_category",
    "conforming_loan_limit",
    "combined_loan_to_value_ratio",
    "property_value",
    "construction_method",
    "total_units",
    "debt_to_income_ratio",
    "interest_rate",
    "loan_term",
    "prepayment_penalty_term",
    "intro_rate_period",
    "reverse_mortgage",
    "open-end_line_of_credit",
    "business_or_commercial_purpose",
    "negative_amortization",
    "interest_only_payment",
    "balloon_payment",
    "other_nonamortizing_features",
    "total_loan_costs",
    "total_points_and_fees",
    "origination_charges",
    "discount_points",
    "lender_credits",
    "derived_ethnicity",
    "derived_race",
    "derived_sex",
    "applicant_ethnicity_observed",
    "co-applicant_ethnicity_observed",
    "applicant_race_observed",
    "co-applicant_race_observed",
    "applicant_sex_observed",
    "co-applicant_sex_observed",
    "applicant_age",
    "co-applicant_age",
    "applicant_age_above_62",
    "co-applicant_age_above_62",
    "applicant_credit_score_type",
    "co-applicant_credit_score_type",
    "submission_of_application",
    "initially_payable_to_institution",
    "aus-1", "aus-2", "aus-3", "aus-4", "aus-5",
    "denial_reason-4",
    "manufactured_home_secured_property_type",
    "manufactured_home_land_property_interest",
    "multifamily_affordable_units",
    "tract_median_age_of_housing_units",
]


# ── Unit-scaling rules ─────────────────────────────────────────────────────────
# Maps field_name → set of years where that field is stored in thousands of dollars.
# The ETL multiplies these fields × 1000 to produce whole-USD values.
#
# Pre-2018 loan_amount: field stores value in $000s (e.g., 167 = $167,000).
# Post-2018 loan_amount: field stores whole dollars already (CFPB rounding to $10K midpoint).
# Income: stored in $000s in ALL years (CFPB convention never changed).
DOLLAR_FIELDS_THOUSANDS: Final[dict[str, set[int]]] = {
    "loan_amount": set(range(2004, 2018)),   # pre-2018 ONLY
    "income":      set(range(2004, 2025)),   # ALL years
}


# ── Regime shift registry ──────────────────────────────────────────────────────
# Documents when fields were first introduced or last present.
# Used by panel_metadata to record columns_null_filled and columns_dropped.
REGIME_SHIFTS: Final[dict[str, dict]] = {
    # Post-2018 additions (NULL for all pre-2018 rows)
    "lei":                                  {"first_year": 2018},
    "derived_loan_product_type":            {"first_year": 2018},
    "derived_dwelling_category":            {"first_year": 2018},
    "conforming_loan_limit":                {"first_year": 2018},
    "combined_loan_to_value_ratio":         {"first_year": 2018},
    "property_value":                       {"first_year": 2018},
    "construction_method":                  {"first_year": 2018},
    "total_units":                          {"first_year": 2018},
    "debt_to_income_ratio":                 {"first_year": 2018},
    "interest_rate":                        {"first_year": 2018},
    "loan_term":                            {"first_year": 2018},
    "prepayment_penalty_term":              {"first_year": 2018},
    "intro_rate_period":                    {"first_year": 2018},
    "reverse_mortgage":                     {"first_year": 2018},
    "open-end_line_of_credit":              {"first_year": 2018},
    "business_or_commercial_purpose":       {"first_year": 2018},
    "negative_amortization":                {"first_year": 2018},
    "interest_only_payment":                {"first_year": 2018},
    "balloon_payment":                      {"first_year": 2018},
    "other_nonamortizing_features":         {"first_year": 2018},
    "total_loan_costs":                     {"first_year": 2018},
    "total_points_and_fees":                {"first_year": 2018},
    "origination_charges":                  {"first_year": 2018},
    "discount_points":                      {"first_year": 2018},
    "lender_credits":                       {"first_year": 2018},
    "derived_ethnicity":                    {"first_year": 2018},
    "derived_race":                         {"first_year": 2018},
    "derived_sex":                          {"first_year": 2018},
    "applicant_ethnicity_observed":         {"first_year": 2018},
    "co-applicant_ethnicity_observed":      {"first_year": 2018},
    "applicant_race_observed":              {"first_year": 2018},
    "co-applicant_race_observed":           {"first_year": 2018},
    "applicant_sex_observed":               {"first_year": 2018},
    "co-applicant_sex_observed":            {"first_year": 2018},
    "applicant_age":                        {"first_year": 2018},
    "co-applicant_age":                     {"first_year": 2018},
    "applicant_age_above_62":               {"first_year": 2018},
    "co-applicant_age_above_62":            {"first_year": 2018},
    "applicant_credit_score_type":          {"first_year": 2018},
    "co-applicant_credit_score_type":       {"first_year": 2018},
    "submission_of_application":            {"first_year": 2018},
    "initially_payable_to_institution":     {"first_year": 2018},
    "aus-1":                                {"first_year": 2018},
    "aus-2":                                {"first_year": 2018},
    "aus-3":                                {"first_year": 2018},
    "aus-4":                                {"first_year": 2018},
    "aus-5":                                {"first_year": 2018},
    "denial_reason-4":                      {"first_year": 2018},
    "manufactured_home_secured_property_type":   {"first_year": 2018},
    "manufactured_home_land_property_interest":  {"first_year": 2018},
    "multifamily_affordable_units":         {"first_year": 2018},
    "tract_median_age_of_housing_units":    {"first_year": 2018},
    # Pre-2018 fields removed after 2017
    "respondent_id":    {"last_year": 2017},
    "agency_code":      {"last_year": 2017},
    "property_type":    {"last_year": 2017},
}


# ── 2017 ETL helpers ──────────────────────────────────────────────────────────
# SQL expression to construct 11-char FIPS census tract from 2017 separate fields.
# Input: state_code (2-char), county_code (3-char), census_tract (7-char XXXX.XX).
# When census_tract is 'NA' or blank, output 'NA' to match the 2018+ convention.
CENSUS_TRACT_SQL_2017: Final[str] = (
    "CASE WHEN TRIM(census_tract) IN ('NA', '', 'na') THEN 'NA'"
    " ELSE LPAD(TRIM(state_code), 2, '0')"
    " || LPAD(TRIM(county_code), 3, '0')"
    " || LPAD(REPLACE(TRIM(census_tract), '.', ''), 6, '0')"
    " END"
)

# SQL expression to convert 2017 loan_amount from $000s to whole dollars.
# The source field is named loan_amount_000s in the raw CSV.
LOAN_AMOUNT_SCALE_SQL_2017: Final[str] = (
    "CASE WHEN TRIM(loan_amount_000s) IN ('NA', '', 'na') THEN NULL"
    " ELSE CAST(TRY_CAST(TRIM(loan_amount_000s) AS DOUBLE) * 1000 AS VARCHAR)"
    " END"
)

# Harmonized VIEW column expressions — applied to ALL years in lar_panel VIEW.
# These bridge categorical code differences across the 2017/2018 reform boundary.
# Key = output column name; Value = SQL CASE expression (references raw column).
HARMONIZED_VIEW_EXPRS: Final[dict[str, str]] = {
    # loan_purpose: 2017 code '3' = Refinancing (any type)
    #               2018+ codes '31'/'32' → collapse to '3' for cross-year queries
    #               Sub-type (cash-out vs not) only available 2018+ via raw loan_purpose
    "loan_purpose_harmonized": (
        "CASE WHEN loan_purpose IN ('31', '32') THEN '3'"
        " ELSE loan_purpose END"
    ),
    # purchaser_type: 2017 code '7' = non-bank financial institution
    #                 2018+ split into '71' (credit union/mortgage co.) and '72' (life ins.)
    "purchaser_type_harmonized": (
        "CASE WHEN purchaser_type IN ('71', '72') THEN '7'"
        " ELSE purchaser_type END"
    ),
    # denial_reason: 2017 uses '0' for Not Applicable; 2018+ uses '10'
    "denial_reason_1_harmonized": (
        "CASE WHEN denial_reason_1 = '0' THEN '10'"
        " ELSE denial_reason_1 END"
    ),
    "denial_reason_2_harmonized": (
        "CASE WHEN denial_reason_2 = '0' THEN '10'"
        " ELSE denial_reason_2 END"
    ),
    "denial_reason_3_harmonized": (
        "CASE WHEN denial_reason_3 = '0' THEN '10'"
        " ELSE denial_reason_3 END"
    ),
    # preapproval: 2017 code '3' = Not Applicable (no preapproval process used)
    #              2018+ code '3' removed; semantically equivalent to '2' (not requested)
    "preapproval_harmonized": (
        "CASE WHEN preapproval = '3' THEN '2'"
        " ELSE preapproval END"
    ),
}


# ── NARA fixed-width record layout ─────────────────────────────────────────────
# Used for 2004-2006 .DAT files from the National Archives.
# Format: (field_name, start_position_0indexed, width_chars)
#
# Source: FFIEC HMDA LAR Record Format specification.
# IMPORTANT: These positions are based on the standard FFIEC layout.
# The NARA files include both the LAR data AND appended census tract data.
# Records beginning with "1" are transmittal/header; "2" are LAR rows (keep).
#
# All values are ASCII text, right-or-left-justified depending on field type.
# Numeric fields are right-justified with leading spaces or zeros.
NARA_FIELD_LAYOUT: Final[list[tuple]] = [
    # Core LAR fields
    ("record_type",                          0,  1),   # "1"=header, "2"=LAR
    ("respondent_id",                        1, 10),   # right-justified
    ("agency_code",                         11,  1),   # 1=OCC,2=FRS,3=FDIC,5=NCUA,7=HUD
    ("loan_type",                           12,  1),
    ("property_type",                       13,  1),
    ("loan_purpose",                        14,  1),
    ("occupancy_type",                      15,  1),
    ("loan_amount",                         16,  7),   # in $000s, right-justified
    ("preapproval",                         23,  1),
    ("action_taken",                        24,  1),
    ("msa_md",                              25,  5),
    ("state_code",                          30,  2),
    ("county_code",                         32,  3),
    ("census_tract",                        35,  7),   # XXXX.XX format
    ("applicant_ethnicity_1",               42,  1),
    ("co_applicant_ethnicity_1",            43,  1),
    ("applicant_race_1",                    44,  1),
    ("applicant_race_2",                    45,  1),
    ("applicant_race_3",                    46,  1),
    ("applicant_race_4",                    47,  1),
    ("applicant_race_5",                    48,  1),
    ("co_applicant_race_1",                 49,  1),
    ("co_applicant_race_2",                 50,  1),
    ("co_applicant_race_3",                 51,  1),
    ("co_applicant_race_4",                 52,  1),
    ("co_applicant_race_5",                 53,  1),
    ("applicant_sex",                       54,  1),
    ("co_applicant_sex",                    55,  1),
    ("applicant_income_000s",               56,  4),   # in $000s
    ("purchaser_type",                      60,  1),
    ("denial_reason_1",                     61,  1),
    ("denial_reason_2",                     62,  1),
    ("denial_reason_3",                     63,  1),
    ("rate_spread",                         64,  5),
    ("hoepa_status",                        69,  1),
    ("lien_status",                         70,  1),
    # Census tract data (appended by FFIEC to LAR records)
    ("tract_population",                    71,  7),
    ("tract_minority_population_percent",   78,  7),
    ("ffiec_msa_md_median_family_income",   85,  7),
    ("tract_to_msa_income_percentage",      92,  6),
    ("tract_owner_occupied_units",          98,  6),
    ("tract_one_to_four_family_housing_units", 104, 5),
]

# Minimum record length for a valid NARA LAR line
NARA_MIN_RECORD_LENGTH: Final[int] = 71   # through lien_status


# ── Census tract construction rules ───────────────────────────────────────────
# Pre-2018: census_tract field contains only the tract code (e.g., "0614.00")
#   Final 11-char FIPS = LPAD(state_code,2) + LPAD(county_code,3) + normalize(tract,6)
#   Normalization: remove ".", left-pad the full number string to 6 chars.
#   Example: "614.00" → remove "." → "61400" → zfill(6) → "061400"
#            "1.02"   → remove "." → "102"   → zfill(6) → "000102"
#
# Post-2018: census_tract field IS the 11-char FIPS already; just validate length.
CENSUS_TRACT_RULES: Final[dict[str, dict]] = {
    "pre_2018": {
        "state_pad":  2,
        "county_pad": 3,
        "tract_pad":  6,
        "tract_source_chars": 7,  # incl. decimal point in NARA; 6 in CSV
    },
    "post_2018": {
        "already_11_chars": True,
    },
}


# ── Avery crosswalk DuckDB DDL ─────────────────────────────────────────────────
AVERY_TABLE_DDL: Final[str] = """
CREATE TABLE IF NOT EXISTS avery_crosswalk (
    respondent_id    VARCHAR,
    agency_code      INTEGER,
    lei              VARCHAR,
    rssd_id          INTEGER,
    parent_rssd      INTEGER,
    top_holder_rssd  INTEGER,
    respondent_name  VARCHAR,
    activity_year    INTEGER,
    assets           BIGINT
)
"""

# ── Panel metadata DuckDB DDL ──────────────────────────────────────────────────
PANEL_METADATA_DDL: Final[str] = """
CREATE TABLE IF NOT EXISTS panel_metadata (
    year                INTEGER PRIMARY KEY,
    row_count           BIGINT,
    columns_present     VARCHAR[],
    columns_null_filled VARCHAR[],
    columns_dropped     VARCHAR[],
    source_url          VARCHAR,
    input_file_sha256   VARCHAR,
    built_at            TIMESTAMP,
    parquet_path        VARCHAR
)
"""
