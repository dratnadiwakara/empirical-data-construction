"""TypedDict schema for the crsp_frb_link panel record."""
from __future__ import annotations

from typing import Optional
from typing_extensions import TypedDict


class CrspFrbRecord(TypedDict):
    permco:           int
    bhc_rssd:         int
    name:             str
    inst_type:        str
    quarter_end:      str            # YYYY-MM-DD
    confirmed:        int            # 1 = within original date range; 0 = extrapolated
    lead_bank_rssd:   Optional[int]
    lead_bank_assets: Optional[float]
    lead_bank_equity: Optional[float]
