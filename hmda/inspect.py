"""
HMDA data inspection — run after construct.py to verify the build.

Usage
-----
    python -m hmda.inspect              # 2024 summary (default)
    python -m hmda.inspect --year 2017  # explicit year (pre-reform)
    python -m hmda.inspect --sql "SELECT COUNT(*) FROM lar_panel"  # ad-hoc query
    python -m hmda.inspect --compare-years 2017 2018  # cross-year variable comparison

Prints:
  - Row count  (compare against CFPB expected)
  - Sample rows (first 5, non-NA fields only)
  - Null rates for key columns
  - Loan amount distribution
  - Action taken distribution
  - Lender coverage (LEI for 2018+, respondent_id for 2017)
  - panel_metadata row for the year
  - [2017] Additional census_tract length check and harmonized column spot-check
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import (
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_THREADS,
    get_duckdb_path,
    get_staging_path,
)
from utils.logging_utils import get_logger

logger = get_logger(__name__)

# Expected row counts from CFPB Data Browser / CFPB historic data portal.
# 2018-2024: FFIEC snapshot files.  2007-2017: CFPB data portal (validated).
EXPECTED_ROWS: dict[int, int] = {
    2024: 12_229_298,
    2023: 11_564_178,
    2022: 16_099_307,
    2021: 26_269_980,
    2020: 25_699_043,
    2019: 17_573_963,
    2018: 15_138_510,
    2017: 14_285_496,
    2016: 16_332_987,
    2015: 14_374_184,
    2014: 12_049_341,
    2013: 17_016_159,
    2012: 18_691_551,
    2011: 14_873_415,
    2010: 16_348_557,
    2009: 19_493_491,
    2008: 17_391_570,
    2007: 26_605_695,
}

# Variables where code sets differ between 2017 and 2018+
# Used by --compare-years to generate empirical frequency tables
CATEGORICAL_VARS_TO_COMPARE = [
    "loan_purpose",
    "loan_purpose_harmonized",
    "purchaser_type",
    "purchaser_type_harmonized",
    "lien_status",
    "preapproval",
    "preapproval_harmonized",
    "hoepa_status",
    "action_taken",
    "loan_type",
    "occupancy_type",
    "applicant_ethnicity_1",
    "applicant_race_1",
    "applicant_sex",
    "denial_reason_1",
    "denial_reason_1_harmonized",
]


def _conn() -> duckdb.DuckDBPyConnection:
    db = get_duckdb_path()
    if not db.exists():
        print(f"[ERROR] DuckDB not found at {db}")
        print("        Run: python -m hmda.construct --year 2024")
        sys.exit(1)
    conn = duckdb.connect(str(db), read_only=True)
    conn.execute(f"PRAGMA threads={DUCKDB_THREADS}")
    conn.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    return conn


def _hr(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def inspect_year(year: int) -> None:
    conn = _conn()

    # 1. Row count
    _hr(f"Row count — year {year}")
    row_count = conn.execute(
        "SELECT COUNT(*) FROM lar_panel WHERE year = ?", [year]
    ).fetchone()[0]
    expected = EXPECTED_ROWS.get(year)
    pct = (row_count / expected * 100) if expected else None
    print(f"  Rows in staging Parquet : {row_count:>15,}")
    if expected:
        print(f"  CFPB expected           : {expected:>15,}")
        print(f"  Match                   : {pct:.2f}%")
        if pct and abs(pct - 100) > 1:
            print("  [WARN] Row count differs by >1% from CFPB expected")

    # 2. Schema
    _hr("Columns")
    cols = conn.execute("DESCRIBE lar_panel").fetchall()
    print(f"  {len(cols)} columns in lar_panel VIEW")
    for c in cols:
        print(f"    {c[0]:45s} {c[1]}")

    # 3. Sample rows
    _hr(f"Sample 3 rows (year={year})")
    cur = conn.execute("SELECT * FROM lar_panel WHERE year = ? LIMIT 3", [year])
    col_names = [d[0] for d in cur.description]
    rows = cur.fetchall()
    for row in rows:
        print()
        for col, val in zip(col_names, row):
            if val is not None and str(val).strip() not in ("", "NA", "Exempt"):
                print(f"    {col:50s}: {val}")

    # 4. Null rates for key columns
    _hr("Null rates — key columns")
    key_cols = [
        "lei", "loan_amount", "income", "census_tract",
        "action_taken", "loan_purpose", "loan_type",
        "applicant_race_1", "applicant_sex",
    ]
    col_names_in_view = {c[0] for c in cols}
    for col in key_cols:
        if col not in col_names_in_view:
            print(f"  {col:45s} [not in schema]")
            continue
        null_pct = conn.execute(
            f"SELECT AVG(CASE WHEN \"{col}\" IS NULL THEN 1.0 ELSE 0.0 END) * 100"
            f" FROM lar_panel WHERE year = ?", [year]
        ).fetchone()[0]
        flag = "  [WARN]" if null_pct and null_pct > 5 else ""
        print(f"  {col:45s} {null_pct:6.2f}% null{flag}")

    # 5. Loan amount distribution
    _hr("Loan amount distribution (as VARCHAR — pre-cast check)")
    dist = conn.execute(
        """
        SELECT
            COUNT(*)                                             AS total,
            SUM(CASE WHEN loan_amount IS NULL THEN 1 END)       AS nulls,
            MIN(TRY_CAST(loan_amount AS DOUBLE))                AS min_amt,
            APPROX_QUANTILE(TRY_CAST(loan_amount AS DOUBLE), 0.25) AS p25,
            APPROX_QUANTILE(TRY_CAST(loan_amount AS DOUBLE), 0.50) AS p50,
            APPROX_QUANTILE(TRY_CAST(loan_amount AS DOUBLE), 0.75) AS p75,
            MAX(TRY_CAST(loan_amount AS DOUBLE))                AS max_amt
        FROM lar_panel WHERE year = ?
        """,
        [year],
    ).fetchone()
    labels = ["total", "nulls", "min", "p25", "p50", "p75", "max"]
    for label, val in zip(labels, dist):
        print(f"  {label:8s}: {val}")

    # 6. Action taken distribution
    _hr("Action taken distribution")
    action = conn.execute(
        """
        SELECT action_taken, COUNT(*) AS n
        FROM lar_panel WHERE year = ?
        GROUP BY action_taken ORDER BY n DESC
        """,
        [year],
    ).fetchall()
    for code, n in action:
        print(f"  action_taken={code:>5}  {n:>10,}")

    # 7. Lender coverage (method depends on era)
    if year >= 2018:
        _hr("LEI coverage vs avery_crosswalk")
        try:
            cov = conn.execute(
                """
                SELECT
                    COUNT(*)                                            AS total_rows,
                    SUM(CASE WHEN av.lei IS NOT NULL THEN 1 END)       AS matched,
                    ROUND(
                        100.0 * SUM(CASE WHEN av.lei IS NOT NULL THEN 1 END)
                        / COUNT(*), 2
                    )                                                   AS match_pct
                FROM lar_panel AS l
                LEFT JOIN avery_crosswalk AS av
                    ON l.lei = av.lei AND av.activity_year = l.year
                WHERE l.year = ?
                """,
                [year],
            ).fetchone()
            print(f"  Total rows : {cov[0]:,}")
            print(f"  LEI matched: {cov[1]:,}")
            print(f"  Match rate : {cov[2]:.2f}%")
            if cov[2] and cov[2] < 80:
                print("  [WARN] Match rate below 80% — check avery_crosswalk was loaded")
        except Exception as exc:
            print(f"  [SKIP] avery_crosswalk not loaded or query failed: {exc}")
    else:
        # 2007-2017: join via respondent_id + agency_code
        _hr(f"RSSD coverage via respondent_id + agency_code ({year})")
        try:
            cov = conn.execute(
                """
                SELECT
                    COUNT(*)                                                AS total_rows,
                    SUM(CASE WHEN av.rssd_id IS NOT NULL THEN 1 END)       AS matched,
                    ROUND(
                        100.0 * SUM(CASE WHEN av.rssd_id IS NOT NULL THEN 1 END)
                        / COUNT(*), 2
                    )                                                       AS match_pct
                FROM lar_panel AS l
                LEFT JOIN avery_crosswalk AS av
                    ON l.respondent_id = av.respondent_id
                    AND TRY_CAST(l.agency_code AS INTEGER) = av.agency_code
                    AND av.activity_year = l.year
                WHERE l.year = ?
                """,
                [year],
            ).fetchone()
            print(f"  Total rows      : {cov[0]:,}")
            print(f"  RSSD matched    : {cov[1]:,}")
            print(f"  Match rate      : {cov[2]:.2f}%")
            if cov[2] and cov[2] < 70:
                print("  [WARN] Match rate below 70% — check avery_crosswalk was loaded")
        except Exception as exc:
            print(f"  [SKIP] avery_crosswalk query failed: {exc}")

        # Census tract format check (pre-2018)
        _hr(f"Census tract format ({year} — should be 11-char FIPS)")
        try:
            lengths = conn.execute(
                """
                SELECT LENGTH(census_tract) AS len, COUNT(*) AS n
                FROM lar_panel
                WHERE year = ? AND census_tract IS NOT NULL AND census_tract != 'NA'
                GROUP BY 1 ORDER BY 1
                """,
                [year],
            ).fetchall()
            for length, n in lengths:
                flag = "" if length == 11 else "  [WARN] expected 11 chars"
                print(f"  len={length}: {n:,} rows{flag}")
            if not lengths:
                print("  [WARN] No non-NA census_tract values found")
        except Exception as exc:
            print(f"  [SKIP] census_tract check failed: {exc}")

        # Harmonized column spot-check
        _hr(f"Harmonized column spot-check (year={year})")
        try:
            rows = conn.execute(
                """
                SELECT loan_purpose, loan_purpose_harmonized, COUNT(*) AS n
                FROM lar_panel WHERE year = ?
                GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 10
                """,
                [year],
            ).fetchall()
            print(f"  {'loan_purpose':>15} | {'harmonized':>15} | {'n':>12}")
            print(f"  {'-'*15}-+-{'-'*15}-+-{'-'*12}")
            for lp, lph, n in rows:
                print(f"  {str(lp):>15} | {str(lph):>15} | {n:>12,}")
        except Exception as exc:
            print(f"  [SKIP] harmonized column check failed: {exc}")

    # 8. panel_metadata
    _hr("panel_metadata row")
    try:
        meta = conn.execute(
            "SELECT * FROM panel_metadata WHERE year = ?", [year]
        ).fetchall()
        if meta:
            row = meta[0]
            desc = conn.execute("DESCRIBE panel_metadata").fetchall()
            for col_info, val in zip(desc, row):
                print(f"  {col_info[0]:30s}: {val}")
        else:
            print("  [not found] — run python -m hmda.construct --year 2024")
    except Exception as exc:
        print(f"  [SKIP] panel_metadata not found: {exc}")

    conn.close()
    print()


def compare_years(year_a: int, year_b: int) -> None:
    """
    Print side-by-side frequency tables for categorical variables across two years.
    Useful for empirically verifying code differences at the 2017/2018 boundary.
    """
    conn = _conn()
    _hr(f"Cross-year variable comparison: {year_a} vs {year_b}")
    print(f"  Uses a random 10,000-row sample from each year for speed.")

    # Check which columns actually exist in the view
    view_cols = {c[0] for c in conn.execute("DESCRIBE lar_panel").fetchall()}

    for var in CATEGORICAL_VARS_TO_COMPARE:
        if var not in view_cols:
            print(f"\n  {var}: [not in lar_panel — skipping]")
            continue

        print(f"\n  -- {var} --")
        print(f"  {'Code':>8} | {str(year_a):>12} | {str(year_b):>12}")
        print(f"  {'-'*8}-+-{'-'*12}-+-{'-'*12}")

        try:
            rows = conn.execute(
                f"""
                WITH
                    s{year_a} AS (
                        SELECT "{var}" AS code, COUNT(*) AS n
                        FROM (SELECT * FROM lar_panel WHERE year = {year_a}
                              USING SAMPLE 10000 ROWS)
                        GROUP BY 1
                    ),
                    s{year_b} AS (
                        SELECT "{var}" AS code, COUNT(*) AS n
                        FROM (SELECT * FROM lar_panel WHERE year = {year_b}
                              USING SAMPLE 10000 ROWS)
                        GROUP BY 1
                    ),
                    all_codes AS (
                        SELECT code FROM s{year_a}
                        UNION SELECT code FROM s{year_b}
                    )
                SELECT
                    a.code,
                    COALESCE(s{year_a}.n, 0) AS n_{year_a},
                    COALESCE(s{year_b}.n, 0) AS n_{year_b}
                FROM all_codes AS a
                LEFT JOIN s{year_a} ON a.code = s{year_a}.code
                LEFT JOIN s{year_b} ON a.code = s{year_b}.code
                ORDER BY COALESCE(s{year_a}.n, 0) + COALESCE(s{year_b}.n, 0) DESC
                """
            ).fetchall()
            for code, na, nb in rows:
                print(f"  {str(code):>8} | {na:>12,} | {nb:>12,}")
        except Exception as exc:
            print(f"  [ERROR] {exc}")

    conn.close()
    print()


def run_adhoc(sql: str) -> None:
    conn = _conn()
    try:
        result = conn.execute(sql)
        rows = result.fetchall()
        cols = [d[0] for d in result.description] if result.description else []
        if cols:
            print("  " + " | ".join(f"{c:>20}" for c in cols))
            print("  " + "-" * (23 * len(cols)))
        for row in rows:
            print("  " + " | ".join(f"{str(v):>20}" for v in row))
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Inspect the HMDA LAR panel in DuckDB."
    )
    p.add_argument("--year", type=int, default=2024,
                   help="Year to inspect (default: 2024)")
    p.add_argument("--sql", type=str, default=None,
                   help="Run an ad-hoc SQL query against the DuckDB and exit")
    p.add_argument(
        "--compare-years", type=int, nargs=2, metavar=("YEAR_A", "YEAR_B"),
        help="Print side-by-side frequency tables across two years (e.g. 2017 2018)"
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.sql:
        run_adhoc(args.sql)
    elif args.compare_years:
        compare_years(*args.compare_years)
    else:
        inspect_year(args.year)


if __name__ == "__main__":
    main()
