"""
CRA schema definitions.

Provides TypedDict classes for the harmonized output schemas and
lightweight runtime validation helpers.
"""
from __future__ import annotations

from typing import Optional, TypedDict


class TransmittalRecord(TypedDict, total=False):
    """One row of the harmonized CRA transmittal panel."""
    respondent_id:    str
    agency_code:      str
    activity_year:    int
    respondent_name:  Optional[str]
    respondent_addr:  Optional[str]
    respondent_city:  Optional[str]
    respondent_state: Optional[str]
    respondent_zip:   Optional[str]
    tax_id:           Optional[str]
    rssdid:           Optional[int]   # NULL for 1996
    assets:           Optional[int]   # NULL for 1996


class AggregateRecord(TypedDict, total=False):
    """One row of the harmonized CRA aggregate panel (tables A1-1, A1-2, A2-1, A2-2)."""
    table_id:             str
    activity_year:        int
    loan_type:            str
    action_taken:         str
    state:                Optional[str]
    county:               Optional[str]
    msamd:                Optional[str]
    census_tract:         Optional[str]   # raw 7-char field
    split_county:         Optional[str]
    pop_group:            Optional[str]
    income_group:         Optional[str]
    report_level:         Optional[str]
    num_loans_lt_100k:    Optional[int]
    amt_loans_lt_100k:    Optional[int]   # thousands of dollars
    num_loans_100k_250k:  Optional[int]
    amt_loans_100k_250k:  Optional[int]
    num_loans_250k_1m:    Optional[int]
    amt_loans_250k_1m:    Optional[int]
    num_loans_rev_lt_1m:  Optional[int]
    amt_loans_rev_lt_1m:  Optional[int]
    county_fips:          Optional[str]   # computed: 5-char
    census_tract_fips:    Optional[str]   # computed: 11-char


class DisclosureRecord(TypedDict, total=False):
    """One row of the harmonized CRA disclosure panel (tables D1-1, D1-2, D2-1, D2-2)."""
    table_id:             str
    respondent_id:        str
    agency_code:          str
    activity_year:        int
    loan_type:            str
    action_taken:         str
    state:                Optional[str]
    county:               Optional[str]
    msamd:                Optional[str]
    aa_num:               Optional[str]
    partial_county:       Optional[str]
    split_county:         Optional[str]
    pop_group:            Optional[str]
    income_group:         Optional[str]
    report_level:         Optional[str]
    num_loans_lt_100k:    Optional[int]
    amt_loans_lt_100k:    Optional[int]
    num_loans_100k_250k:  Optional[int]
    amt_loans_100k_250k:  Optional[int]
    num_loans_250k_1m:    Optional[int]
    amt_loans_250k_1m:    Optional[int]
    num_loans_rev_lt_1m:  Optional[int]
    amt_loans_rev_lt_1m:  Optional[int]
    num_loans_affiliate:  Optional[int]
    amt_loans_affiliate:  Optional[int]
    county_fips:          Optional[str]   # computed: 5-char
    census_tract_fips:    Optional[str]   # computed: 11-char


class PanelMetadataRecord(TypedDict):
    """One row of the panel_metadata DuckDB table."""
    table_type:   str
    year:         int
    row_count:    int
    source_url:   str
    built_at:     str
    parquet_path: str


def validate_aggregate_sample(rows: list[dict]) -> list[str]:
    """Quick sanity checks on a sample of aggregate rows."""
    warnings: list[str] = []
    if not rows:
        warnings.append("validate_aggregate_sample: empty sample")
        return warnings

    for r in rows[:10]:
        ct = r.get("census_tract_fips", "")
        if ct and ct not in ("", None) and len(ct) != 11:
            warnings.append(f"census_tract_fips '{ct}' is not 11 chars")
            break

        cf = r.get("county_fips", "")
        if cf and cf not in ("", None) and len(cf) != 5:
            warnings.append(f"county_fips '{cf}' is not 5 chars")
            break

    return warnings
