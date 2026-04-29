"""TypedDict definition for a harmonized IRS SOI zip-year record."""
from typing import Optional
from typing_extensions import TypedDict


class IrsZipRecord(TypedDict):
    zipcode: str                          # 5-digit ZIP (ZCTA)
    year: int                             # tax year
    n_returns: Optional[int]              # total returns (N1)
    agi_total: Optional[float]            # total AGI, $thousands (A00100)
    n_returns_wages: Optional[int]        # returns with wages (N00200)
    agi_wages: Optional[float]            # wages amount, $thousands (A00200)
    n_returns_dividend: Optional[int]     # returns with dividends (N00600)
    agi_dividend: Optional[float]         # dividend amount, $thousands (A00600)
    n_returns_business: Optional[int]     # returns with biz income (N00900)
    agi_business: Optional[float]         # biz income, $thousands (A00900)
    n_returns_capital_gain: Optional[int] # returns with cap gain (N01000)
    agi_capital_gain: Optional[float]     # cap gain amount, $thousands (A01000)
