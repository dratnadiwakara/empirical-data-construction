"""TypedDict definitions for the RateWatch harmonized record."""
from __future__ import annotations

from typing import Optional, TypedDict


class RatewatchRecord(TypedDict, total=False):
    """One row in the harmonized RateWatch panel.

    Grain: (week_date, account_number, prd_typ_join) at the representative tier.
    Rates are in *percent* (5.0 = 5%), matching raw S&P RateWatch convention.
    """

    year: int
    week_date: str            # ISO date 'YYYY-MM-DD' from DATESURVEYED
    account_number: str       # Ratesetting branch ID, e.g. 'NY01100011'
    prd_typ_join: str         # Product code, e.g. 'CD', 'MM', 'SAV'
    productdescription: str   # Tier code, e.g. '12MCD10K'
    producttype: str
    prod_nm: str
    promo: Optional[str]
    mintoearn: Optional[float]
    maxtoearn: Optional[float]
    termlength: Optional[float]
    termtype: Optional[str]
    rate: Optional[float]     # Nominal rate, percent
    apy: Optional[float]      # Annual Percentage Yield, percent
    cmt: Optional[str]
    rssd_id: Optional[int]    # From institution details
    uninumbr: Optional[int]
    cert_nbr: Optional[int]


class PanelMetadataRecord(TypedDict):
    year: int
    row_count: int
    n_branches: int
    n_products: int
    n_weeks: int
    source_file: str
    built_at: str
    parquet_path: str
