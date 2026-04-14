"""
HMDA LAR schema definitions.

Provides TypedDict classes for the harmonized output schema and the
panel metadata record, enabling static type checking and runtime validation.
"""
from __future__ import annotations

from typing import Optional
from typing import TypedDict


class LARRecord(TypedDict, total=False):
    """
    One row of the harmonized HMDA LAR panel.

    Fields follow the master schema anchored to the 2024 CFPB data dictionary.
    Pre-2018 rows carry NULL for post-2018-only fields (e.g., lei, property_value).
    Post-2018 rows carry NULL for pre-2018-only identifiers (respondent_id, agency_code).

    All values are stored as strings to preserve HMDA-specific sentinels
    ("NA", "Exempt", numeric codes, etc.). Cast to numeric types as needed
    for analysis.
    """
    # ── Identifiers ──────────────────────────────────────────────────────────
    activity_year:                          Optional[str]
    lei:                                    Optional[str]   # 2018+; NULL pre-2018
    respondent_id:                          Optional[str]   # pre-2018; NULL 2018+
    agency_code:                            Optional[str]   # pre-2018; NULL 2018+

    # ── Geography ─────────────────────────────────────────────────────────────
    derived_msa_md:                         Optional[str]   # stored as "derived_msa-md"
    state_code:                             Optional[str]
    county_code:                            Optional[str]
    census_tract:                           Optional[str]   # 11-char FIPS

    # ── Derived categorical ───────────────────────────────────────────────────
    derived_loan_product_type:              Optional[str]   # 2018+
    derived_dwelling_category:              Optional[str]   # 2018+
    conforming_loan_limit:                  Optional[str]   # 2018+
    derived_ethnicity:                      Optional[str]   # 2018+
    derived_race:                           Optional[str]   # 2018+
    derived_sex:                            Optional[str]   # 2018+

    # ── Application & action ──────────────────────────────────────────────────
    action_taken:                           Optional[str]
    purchaser_type:                         Optional[str]
    loan_type:                              Optional[str]
    loan_purpose:                           Optional[str]
    lien_status:                            Optional[str]
    preapproval:                            Optional[str]

    # ── Loan economics (whole dollars) ────────────────────────────────────────
    loan_amount:                            Optional[str]   # scaled to whole USD
    combined_loan_to_value_ratio:           Optional[str]   # 2018+
    interest_rate:                          Optional[str]   # 2018+
    rate_spread:                            Optional[str]
    hoepa_status:                           Optional[str]
    total_loan_costs:                       Optional[str]   # 2018+
    total_points_and_fees:                  Optional[str]   # 2018+
    origination_charges:                    Optional[str]   # 2018+
    discount_points:                        Optional[str]   # 2018+
    lender_credits:                         Optional[str]   # 2018+
    loan_term:                              Optional[str]   # 2018+
    prepayment_penalty_term:                Optional[str]   # 2018+
    intro_rate_period:                      Optional[str]   # 2018+

    # ── Loan features ─────────────────────────────────────────────────────────
    reverse_mortgage:                       Optional[str]   # 2018+
    open_end_line_of_credit:                Optional[str]   # stored as "open-end_line_of_credit"
    business_or_commercial_purpose:         Optional[str]   # 2018+
    negative_amortization:                  Optional[str]   # 2018+
    interest_only_payment:                  Optional[str]   # 2018+
    balloon_payment:                        Optional[str]   # 2018+
    other_nonamortizing_features:           Optional[str]   # 2018+

    # ── Property ──────────────────────────────────────────────────────────────
    property_value:                         Optional[str]   # 2018+
    construction_method:                    Optional[str]   # 2018+
    occupancy_type:                         Optional[str]
    manufactured_home_secured_property_type: Optional[str]  # 2018+
    manufactured_home_land_property_interest: Optional[str] # 2018+
    total_units:                            Optional[str]   # 2018+
    multifamily_affordable_units:           Optional[str]   # 2018+

    # ── Borrower ──────────────────────────────────────────────────────────────
    income:                                 Optional[str]   # scaled to whole USD (all years)
    debt_to_income_ratio:                   Optional[str]   # 2018+
    applicant_credit_score_type:            Optional[str]   # 2018+
    co_applicant_credit_score_type:         Optional[str]   # stored as "co-applicant_credit_score_type"

    # Ethnicity
    applicant_ethnicity_1:                  Optional[str]   # stored as "applicant_ethnicity-1"
    applicant_ethnicity_2:                  Optional[str]
    applicant_ethnicity_3:                  Optional[str]
    applicant_ethnicity_4:                  Optional[str]
    applicant_ethnicity_5:                  Optional[str]
    co_applicant_ethnicity_1:               Optional[str]
    co_applicant_ethnicity_2:               Optional[str]
    co_applicant_ethnicity_3:               Optional[str]
    co_applicant_ethnicity_4:               Optional[str]
    co_applicant_ethnicity_5:               Optional[str]
    applicant_ethnicity_observed:           Optional[str]   # 2018+
    co_applicant_ethnicity_observed:        Optional[str]   # 2018+

    # Race
    applicant_race_1:                       Optional[str]
    applicant_race_2:                       Optional[str]
    applicant_race_3:                       Optional[str]
    applicant_race_4:                       Optional[str]
    applicant_race_5:                       Optional[str]
    co_applicant_race_1:                    Optional[str]
    co_applicant_race_2:                    Optional[str]
    co_applicant_race_3:                    Optional[str]
    co_applicant_race_4:                    Optional[str]
    co_applicant_race_5:                    Optional[str]
    applicant_race_observed:                Optional[str]   # 2018+
    co_applicant_race_observed:             Optional[str]   # 2018+

    # Sex
    applicant_sex:                          Optional[str]
    co_applicant_sex:                       Optional[str]
    applicant_sex_observed:                 Optional[str]   # 2018+
    co_applicant_sex_observed:              Optional[str]   # 2018+

    # Age
    applicant_age:                          Optional[str]   # 2018+
    co_applicant_age:                       Optional[str]   # 2018+
    applicant_age_above_62:                 Optional[str]   # 2018+
    co_applicant_age_above_62:              Optional[str]   # 2018+

    # Application channel
    submission_of_application:              Optional[str]   # 2018+
    initially_payable_to_institution:       Optional[str]   # 2018+

    # AUS
    aus_1:                                  Optional[str]   # stored as "aus-1"
    aus_2:                                  Optional[str]
    aus_3:                                  Optional[str]
    aus_4:                                  Optional[str]
    aus_5:                                  Optional[str]

    # Denial
    denial_reason_1:                        Optional[str]   # stored as "denial_reason-1"
    denial_reason_2:                        Optional[str]
    denial_reason_3:                        Optional[str]
    denial_reason_4:                        Optional[str]   # 2018+

    # ── Census tract supplements ───────────────────────────────────────────────
    tract_population:                       Optional[str]
    tract_minority_population_percent:      Optional[str]
    ffiec_msa_md_median_family_income:      Optional[str]
    tract_to_msa_income_percentage:         Optional[str]
    tract_owner_occupied_units:             Optional[str]
    tract_one_to_four_family_homes:         Optional[str]
    tract_median_age_of_housing_units:      Optional[str]   # 2018+

    # ── Avery crosswalk supplements (pre-2018) ────────────────────────────────
    rssd_id:                                Optional[str]
    parent_rssd:                            Optional[str]
    top_holder_rssd:                        Optional[str]


class PanelMetadataRecord(TypedDict):
    """One row of the panel_metadata DuckDB table documenting a single year's build."""
    year:                       int
    row_count:                  int
    columns_present:            list[str]
    columns_null_filled:        list[str]
    columns_dropped:            list[str]
    unit_conversions_applied:   str        # JSON-ish repr of {field: [years]}
    avery_match_rate:           float      # 0–1; 1.0 for 2018+ (LEI-based)
    avery_match_count:          int
    source_url:                 str
    input_file_sha256:          str
    built_at:                   str        # ISO-8601 UTC timestamp
    parquet_path:               str


# ── Lightweight runtime validation ───────────────────────────────────────────

_REQUIRED_NUMERIC_COLS = ("loan_amount", "income", "action_taken")

def validate_row_sample(rows: list[dict]) -> list[str]:
    """
    Run lightweight checks on a sample of output rows.

    Returns a list of warning strings (empty = no issues found).
    Intended for quick sanity-checking after construction, not exhaustive validation.
    """
    warnings: list[str] = []
    if not rows:
        warnings.append("validate_row_sample: received empty sample")
        return warnings

    for col in _REQUIRED_NUMERIC_COLS:
        null_count = sum(1 for r in rows if r.get(col) in (None, "", "NA"))
        rate = null_count / len(rows)
        if rate > 0.5:
            warnings.append(
                f"{col}: {rate:.1%} of sampled rows are null/NA — "
                "check unit scaling and column mapping"
            )

    for r in rows[:5]:
        tract = r.get("census_tract", "")
        if tract and tract not in ("", "NA") and len(tract) != 11:
            warnings.append(
                f"census_tract '{tract}' is not 11 chars — "
                "check normalize_census_tract logic"
            )
            break

    return warnings
