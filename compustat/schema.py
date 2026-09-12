"""
TypedDict definitions for the Compustat Fundamentals Annual pipeline.

Only the headline columns are typed here. The full 976-variable dictionary is
queryable from the ``variable_dictionary`` table inside ``compustat.duckdb``.
"""
from __future__ import annotations

from datetime import date
from typing import Optional, TypedDict


class CompustatRecord(TypedDict, total=False):
    """One row of the Compustat Fundamentals Annual panel (standard filter)."""

    gvkey: str           # Global Company Key — stable firm ID
    datadate: date       # Fiscal year-end date
    fyear: int           # Fiscal year
    tic: Optional[str]   # Ticker symbol
    conm: Optional[str]  # Company name
    cusip: Optional[str] # CUSIP
    cik: Optional[str]   # SEC CIK
    exchg: Optional[int] # Stock exchange code
    sic: Optional[str]   # SIC code (industry)
    naicsh: Optional[int]  # NAICS historical
    at: Optional[float]    # Total assets ($M)
    sale: Optional[float]  # Sales / revenue ($M)
    ni: Optional[float]    # Net income ($M)
    ceq: Optional[float]   # Common equity ($M)
    lt: Optional[float]    # Total liabilities ($M)


class PanelMetadataRecord(TypedDict):
    """One row of the panel_metadata audit table."""

    year: int
    row_count: int
    source_url: str
    built_at: str
    parquet_path: str


class VariableDictionaryRecord(TypedDict):
    """One row of the variable_dictionary lookup table."""

    variable_name: str
    type: str
    description: str
