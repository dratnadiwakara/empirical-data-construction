"""
Post-build sanity checks for the Compustat Fundamentals Annual dataset.

Run after construct.py. Logs WARN/ERROR for any check that fails. Exits 0
even on warnings; nonzero only on a hard error (e.g. missing duckdb).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import get_compustat_duckdb_path
from utils.logging_utils import get_logger

logger = get_logger(__name__)

# Sanity bounds
MIN_GVKEYS_PER_FYEAR = 6_000     # WRDS extract has ~7–13k firms per fyear, 1980+
MAX_YOY_DROP = 0.20              # warn if row count drops >20% YoY
NULL_RATE_PK = 0.0               # PK columns must be 0% null
# `at`/`sale` fill declines secularly: ~95% in the 1980s to ~53% by 2025, as
# shells, SPACs, trusts and pre-revenue listings (which file no total-assets
# figure) grew as a share of indfmt='INDL'. Not data loss — threshold set below
# the 2025 floor so the check flags real breakage, not the known trend.
KEY_NUMERIC_MIN_FILL = 0.45      # at/sale non-null share per fyear

# Fiscal years at the extract edges are partial by construction: firms with
# non-December fiscal year-ends fall into an fyear whose calendar window the
# extract only partly covers. Rows are valid; year-level aggregates are not.
PARTIAL_EDGE_FYEARS = (1979, 2026)


def _connect():
    db_path = get_compustat_duckdb_path()
    if not db_path.exists():
        raise FileNotFoundError(f"{db_path} not found — run compustat.construct first")
    return duckdb.connect(str(db_path), read_only=True)


def check_panel_metadata(conn) -> None:
    logger.info("-- panel_metadata -----------------------------------------")
    rows = conn.execute(
        "SELECT year, row_count, built_at FROM panel_metadata ORDER BY year"
    ).fetchall()
    if not rows:
        logger.error("panel_metadata is empty")
        return
    prev: Optional[int] = None
    for year, n, built in rows:
        marker = ""
        if prev is not None and prev > 0:
            drop = (prev - n) / prev
            if drop > MAX_YOY_DROP:
                marker = f" <- WARN: dropped {drop:.0%} YoY"
        print(f"  fyear={year}  rows={n:>7,}  built_at={built}{marker}")
        prev = n


def check_null_rates(conn) -> None:
    logger.info("-- NULL rates on key columns ------------------------------")
    row = conn.execute("""
        SELECT
            AVG(CASE WHEN gvkey    IS NULL THEN 1.0 ELSE 0.0 END) AS nr_gvkey,
            AVG(CASE WHEN datadate IS NULL THEN 1.0 ELSE 0.0 END) AS nr_datadate,
            AVG(CASE WHEN fyear    IS NULL THEN 1.0 ELSE 0.0 END) AS nr_fyear
        FROM compustat
    """).fetchone()
    nr_gvkey, nr_datadate, nr_fyear = row
    print(f"  null rate gvkey:    {nr_gvkey:.4%}")
    print(f"  null rate datadate: {nr_datadate:.4%}")
    print(f"  null rate fyear:    {nr_fyear:.4%}")
    for name, rate in (("gvkey", nr_gvkey), ("datadate", nr_datadate), ("fyear", nr_fyear)):
        if rate > NULL_RATE_PK:
            logger.error("PK column %s has %.4f%% NULLs (expected 0)", name, rate * 100)


def check_uniqueness(conn) -> None:
    logger.info("-- (gvkey, datadate) uniqueness ---------------------------")
    dup = conn.execute(
        "SELECT COUNT(*) - COUNT(DISTINCT (gvkey, datadate)) FROM compustat"
    ).fetchone()[0]
    print(f"  duplicate (gvkey, datadate) pairs: {dup}")
    if dup > 0:
        logger.error("STD filter failed — %d duplicate (gvkey, datadate) pairs remain", dup)


def check_distribution(conn) -> None:
    logger.info("-- firms and fill rate by fyear ---------------------------")
    # "at" is a DuckDB reserved keyword (AT TIME ZONE) — must be quoted.
    rows = conn.execute("""
        SELECT
            fyear,
            COUNT(DISTINCT gvkey)                                   AS n_firms,
            AVG(CASE WHEN "at"   IS NOT NULL THEN 1.0 ELSE 0.0 END) AS fill_at,
            AVG(CASE WHEN "sale" IS NOT NULL THEN 1.0 ELSE 0.0 END) AS fill_sale,
            AVG(CASE WHEN "ni"   IS NOT NULL THEN 1.0 ELSE 0.0 END) AS fill_ni,
            SUM(CASE WHEN "at"   < 0 THEN 1 ELSE 0 END)             AS n_neg_at,
            SUM(CASE WHEN "sale" < 0 THEN 1 ELSE 0 END)             AS n_neg_sale
        FROM compustat
        GROUP BY fyear ORDER BY fyear
    """).fetchall()
    print(f"  {'fyear':>6} {'firms':>7} {'%at':>6} {'%sale':>6} {'%ni':>6} {'<0 at':>6} {'<0 sale':>8}")
    for r in rows:
        fy, nf, fa, fs, fn_, neg_at, neg_sale = r
        flag = ""
        if fy in PARTIAL_EDGE_FYEARS:
            flag = " <- partial fiscal-year edge (expected)"
        else:
            if nf < MIN_GVKEYS_PER_FYEAR:
                flag = " <- WARN: low firm count"
            if fa < KEY_NUMERIC_MIN_FILL:
                flag += " <- WARN: low `at` fill"
        print(f"  {fy:>6} {nf:>7,} {fa:>6.1%} {fs:>6.1%} {fn_:>6.1%} {neg_at:>6} {neg_sale:>8}{flag}")
        if neg_sale > 0:
            logger.warning("fyear=%d has %d rows with sale<0", fy, neg_sale)


def check_variable_dictionary(conn) -> None:
    logger.info("-- variable_dictionary ------------------------------------")
    n = conn.execute("SELECT COUNT(*) FROM variable_dictionary").fetchone()[0]
    print(f"  variable_dictionary rows: {n}")
    if n == 0:
        logger.error("variable_dictionary is empty")


def run_all() -> None:
    conn = _connect()
    try:
        check_panel_metadata(conn)
        check_null_rates(conn)
        check_uniqueness(conn)
        check_distribution(conn)
        check_variable_dictionary(conn)
    finally:
        conn.close()


def run_sql(query: str) -> None:
    conn = _connect()
    try:
        result = conn.execute(query).fetchall()
        cols = [d[0] for d in conn.description] if conn.description else []
        if cols:
            print("  " + " | ".join(cols))
        for row in result:
            print("  " + " | ".join(str(v) for v in row))
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Inspect the Compustat DuckDB.")
    p.add_argument("--sql", type=str, help="Run an ad-hoc SQL query against compustat.duckdb.")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.sql:
        run_sql(args.sql)
    else:
        run_all()


if __name__ == "__main__":
    main()
