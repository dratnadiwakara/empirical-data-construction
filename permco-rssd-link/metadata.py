"""
Metadata for the CRSP-FRB PERMCO-RSSD link dataset.
Source: NY Fed CRSP-FRB link table (single CSV, periodically updated).
"""
from __future__ import annotations

SOURCE_URL = "https://www.newyorkfed.org/research/banking_research/crsp-frb"

PANEL_METADATA_DDL = """
CREATE TABLE IF NOT EXISTS panel_metadata (
    dataset     VARCHAR PRIMARY KEY,
    row_count   BIGINT,
    source_csv  VARCHAR,
    file_sha256 VARCHAR,
    source_url  VARCHAR,
    built_at    VARCHAR
)
"""

VARIABLE_DESCRIPTIONS: dict[str, str] = {
    "permco":           "CRSP PERMCO identifier for the bank holding company",
    "bhc_rssd":         "RSSD identifier for the Bank Holding Company (entity column from NY Fed CSV)",
    "name":             "Entity name as reported in the source CSV",
    "inst_type":        "Institution type (e.g. Bank Holding Company, Commercial Bank, Domestic Entity Other)",
    "quarter_end":      "Quarter-end date (last calendar day of quarter) as DATE type",
    "confirmed":        "1 = quarter falls within original dt_end from source CSV; 0 = extrapolated forward to current quarter",
    "lead_bank_rssd":   "RSSD of the largest controlled subsidiary by total assets in this quarter (from NIC + call reports)",
    "lead_bank_assets": "Lead bank total assets in thousands of USD (from CFLV for pre-2001; FFIEC for 2001+)",
    "lead_bank_equity": "Lead bank total equity capital in thousands of USD",
}
