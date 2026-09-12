"""
Metadata for the Compustat Fundamentals Annual pipeline.
Single source of truth: source filenames, dedup filter, type mapping, DDLs.
"""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Final

# -- Source files (live in compustat/temp/, copied to HDD raw/) --------------─
SOURCE_FUND_FILE: Final[str] = "compustat_fund.gz"
SOURCE_VARS_FILE: Final[str] = "compustat_vars.csv"

# Canonical "one row per company-fiscal-year" filter for Compustat Fundamentals
# Annual. Drops financial-services format (FS), restated/preliminary (datafmt
# in {HIST_STD, ...}), and non-consolidated rows. popsrc is not in this WRDS
# extract; INDL+STD+C alone yields 0 (gvkey, datadate) duplicates.
STD_FUNDAMENTALS_WHERE: Final[str] = (
    "indfmt = 'INDL' AND datafmt = 'STD' AND consol = 'C'"
)

# Headline columns surfaced in README, schema.py TypedDict, inspect.py checks.
# Everything else is documented in the variable_dictionary DuckDB table.
KEY_COLUMNS: Final[list[str]] = [
    "gvkey", "datadate", "fyear",
    "tic", "conm", "cusip", "cik",
    "exchg", "sic", "naicsh",
    "at", "sale", "ni", "ceq", "lt",
]

# -- Variable type mapping (vars CSV "Type" -> DuckDB SQL type) ----------------
# Compustat vars CSV uses four Type values. Char ID-like fields (gvkey, cik,
# cusip) are kept as VARCHAR to preserve any leading zeros.
_TYPE_MAP: Final[dict[str, str]] = {
    "Char":    "VARCHAR",
    "Date":    "DATE",
    "Decimal": "DOUBLE",
    "Int":     "BIGINT",
}


# Columns the WRDS vars CSV omits but we still want strongly typed and
# documented. Order: (variable_name, vars-CSV-style "Type", description).
# datadate especially: it's the join key paired with gvkey.
CORE_VAR_OVERRIDES: Final[list[tuple[str, str, str]]] = [
    ("datadate", "Date", "(datadate) Data Date - fiscal period-end (PK with gvkey)"),
    ("indfmt",   "Char", "(indfmt) Industry Format: INDL=industrial, FS=financial-services"),
    ("datafmt",  "Char", "(datafmt) Data Format: STD=standardized, HIST_STD=restated, SUMM_STD=summary"),
    ("consol",   "Char", "(consol) Consolidation Level: C=consolidated, N=non-consolidated"),
    ("curcd",    "Char", "(curcd) ISO Currency Code (USD, CAD, etc.)"),
    ("costat",   "Char", "(costat) Company Status: A=active, I=inactive"),
]

CORE_TYPE_OVERRIDES: Final[dict[str, str]] = {
    name: _TYPE_MAP.get(t.strip(), "VARCHAR") for name, t, _ in CORE_VAR_OVERRIDES
}


def var_type_to_duckdb(vt: str) -> str:
    """Map a Compustat vars-CSV Type to a DuckDB SQL type. Default VARCHAR."""
    return _TYPE_MAP.get(vt.strip(), "VARCHAR")


def load_var_type_map(vars_csv_path: Path) -> dict[str, str]:
    """Read compustat_vars.csv -> {variable_name: duckdb_type}.

    The vars CSV has columns: ``Variable Name``, ``Type``, ``Description``.
    Core columns the vars CSV omits (e.g. datadate) are seeded from
    CORE_TYPE_OVERRIDES; vars-CSV entries take precedence over overrides.
    """
    out: dict[str, str] = dict(CORE_TYPE_OVERRIDES)
    with open(vars_csv_path, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            name = (row.get("Variable Name") or "").strip()
            if not name:
                continue
            out[name] = var_type_to_duckdb(row.get("Type") or "")
    return out


def load_var_descriptions(vars_csv_path: Path) -> list[dict]:
    """Read compustat_vars.csv -> list of {variable_name, type, description}.

    Used to populate the ``variable_dictionary`` DuckDB table. CORE_VAR_OVERRIDES
    are prepended so columns omitted by the vars CSV (datadate, indfmt, ...)
    still appear; vars-CSV entries take precedence on name collision.
    """
    out: dict[str, dict] = {}
    for name, vtype, desc in CORE_VAR_OVERRIDES:
        out[name] = {"variable_name": name, "type": vtype, "description": desc}
    with open(vars_csv_path, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            name = (row.get("Variable Name") or "").strip()
            if not name:
                continue
            out[name] = {
                "variable_name": name,
                "type": (row.get("Type") or "").strip(),
                "description": (row.get("Description") or "").strip(),
            }
    return list(out.values())


# -- DuckDB DDLs --------------------------------------------------------------
PANEL_METADATA_DDL: Final[str] = """
CREATE TABLE IF NOT EXISTS panel_metadata (
    year         INTEGER,
    row_count    BIGINT,
    source_url   VARCHAR,
    built_at     VARCHAR,
    parquet_path VARCHAR,
    PRIMARY KEY (year)
)
"""

VARIABLE_DICTIONARY_DDL: Final[str] = """
CREATE TABLE IF NOT EXISTS variable_dictionary (
    variable_name VARCHAR PRIMARY KEY,
    type          VARCHAR,
    description   VARCHAR
)
"""

# -- Source label used in panel_metadata.source_url --------------------------─
SOURCE_LABEL: Final[str] = "WRDS Compustat Fundamentals Annual (comp.funda)"


# -- Equity ratios view -------------------------------------------------------
# Annual equity ratios computed from compustat fundamentals. One row per
# (gvkey, fiscal year). Earnings cleaned of special items, non-operating
# income, and extraordinary / discontinued ops (Buffett-style look-through).
# PEG uses a 3-year trailing CAGR over the cleaned earnings series.
# public_date = datadate + 90 days (10-K filing-lag floor for PIT filters).
EQUITY_RATIOS_VIEW_SQL: Final[str] = """
CREATE OR REPLACE VIEW compustat_equity_ratios AS
WITH base AS (
    SELECT
        LPAD(gvkey, 6, '0')                                       AS gvkey,
        datadate,
        CAST(datadate + INTERVAL '90 days' AS TIMESTAMP)          AS public_date,
        fyear,
        tic,
        csho, prcc_f, mkvalt,
        ni, ib, spi, nopi, xido,
        epspx, epsfx,
        ebit, ebitda, oibdp,
        dltt, dlc, mib, che,
        "at", dvc, revt,
        ni
          - COALESCE(spi, 0)
          - COALESCE(nopi, 0)
          - COALESCE(xido, 0)                                     AS ni_clean
    FROM compustat
),
ratios AS (
    SELECT
        *,
        CASE WHEN csho > 0 THEN ni_clean / csho END               AS eps_clean,
        COALESCE(ebitda, oibdp)                                   AS ebitda_use,
        prcc_f * csho                                             AS mkt_cap,
        CASE WHEN csho > 0 THEN dvc / csho END                    AS dps,
        LAG(ni_clean, 3) OVER (
            PARTITION BY gvkey ORDER BY datadate
        )                                                         AS ni_clean_3prior,
        COUNT(ni_clean) OVER (
            PARTITION BY gvkey ORDER BY datadate
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        )                                                         AS n_in_window
    FROM base
)
SELECT
    gvkey,
    public_date,
    fyear,
    CASE
        WHEN eps_clean > 0 AND prcc_f > 0
        THEN prcc_f / eps_clean
    END                                                           AS pe_op_dil,
    CASE
        WHEN ebitda_use > 0 AND mkt_cap IS NOT NULL
        THEN (mkt_cap
              + COALESCE(dltt, 0) + COALESCE(dlc, 0)
              + COALESCE(mib, 0)  - COALESCE(che, 0)) / ebitda_use
    END                                                           AS evm,
    CASE
        WHEN dps > 0 AND prcc_f > 0 THEN dps / prcc_f
        WHEN dps = 0               THEN 0.0
    END                                                           AS divyield,
    CASE
        WHEN prcc_f > 0
         AND eps_clean > 0
         AND n_in_window = 4
         AND ni_clean_3prior > 0
         AND ni_clean > 0
        THEN
            (prcc_f / eps_clean)
            / ((POWER(ni_clean / ni_clean_3prior, 1.0 / 3.0) - 1) * 100)
    END                                                           AS PEG_trailing,
    CASE WHEN "at" > 0 THEN ni_clean / "at" END                   AS roa,
    CASE
        WHEN ebitda_use > 0
        THEN (COALESCE(dltt, 0) + COALESCE(dlc, 0) - COALESCE(che, 0))
             / ebitda_use
    END                                                           AS debt_ebitda,
    tic                                                           AS "Ticker",
    TRUE                                                          AS _synth,
    ni_clean,
    ni                                                            AS ni_raw,
    CASE WHEN ni > 0 THEN ni_clean / ni END                       AS ni_clean_pct
FROM ratios
"""
