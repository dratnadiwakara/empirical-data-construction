"""
TypedDict definitions for the FDIC SOD pipeline.
"""
from __future__ import annotations

from typing import Optional, TypedDict


class SodRecord(TypedDict, total=False):
    """One harmonized branch row from the FDIC Summary of Deposits."""

    UNINUMBR: Optional[int]    # Unique branch identifier
    YEAR: int                  # Survey year
    CERT: Optional[int]        # FDIC certificate number
    RSSDID: Optional[int]      # RSSD ID of institution
    RSSDHCR: Optional[int]     # RSSD ID of top holding company
    NAMEFULL: Optional[str]    # Institution full legal name
    NAMEBR: Optional[str]      # Branch name
    ADDRESBR: Optional[str]    # Street address
    CITYBR: Optional[str]      # City
    STALP: Optional[str]       # State abbreviation
    STNAME: Optional[str]      # State name
    ZIPBR: Optional[str]       # ZIP code
    STCNTYBR: Optional[str]    # 5-char state+county FIPS
    CNTYNAMB: Optional[str]    # County name
    ASSET: Optional[int]       # Institution total assets ($000s)
    DEPSUMBR: Optional[int]    # Branch deposits ($000s)
    BRNUM: Optional[int]       # Branch sequence number
    BRSERTYP: Optional[int]    # Branch service type code
    CHRTAGNT: Optional[str]    # Charter agent
    ESTYMD: Optional[str]      # Establishment date (YYYY-MM-DD)
    NAMEHCR: Optional[str]     # Holding company name


class PanelMetadataRecord(TypedDict):
    """One row of the panel_metadata audit table."""

    year: int
    row_count: int
    source_url: str
    built_at: str
    parquet_path: str
