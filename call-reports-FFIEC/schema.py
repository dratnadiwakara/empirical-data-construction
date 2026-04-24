"""
TypedDicts for FFIEC Call Reports harmonized records.

Only the POR (filer) record has a stable, human-readable column list worth
typing. The Schedule RC/RI/... files have 50-700 MDRM code columns each and
the set drifts every quarter — those are represented as Dict[str, str] at
the ingest layer and exposed as wide DuckDB views at the query layer.
"""
from __future__ import annotations

from typing import Dict, Optional, TypedDict


class FilerRecord(TypedDict, total=False):
    """One row of the FFIEC CDR Call Bulk POR file (per filer per quarter)."""
    idrssd: str
    fdic_certificate_number: Optional[str]
    occ_charter_number: Optional[str]
    ots_docket_number: Optional[str]
    primary_aba_routing_number: Optional[str]
    financial_institution_name: Optional[str]
    financial_institution_address: Optional[str]
    financial_institution_city: Optional[str]
    financial_institution_state: Optional[str]
    financial_institution_zip_code: Optional[str]
    financial_institution_filing_type: Optional[str]
    last_date_updated: Optional[str]
    activity_year: int
    activity_quarter: int


# Generic row type for all Schedule RC* and RI* files.
# Keys are MDRM codes (e.g. 'RCFD2170', 'RIAD4340') plus 'IDRSSD',
# 'activity_year', 'activity_quarter'. All values are VARCHAR — callers apply
# TRY_CAST(col AS DOUBLE) for arithmetic (same convention as HMDA lar_panel).
RawScheduleRow = Dict[str, str]
