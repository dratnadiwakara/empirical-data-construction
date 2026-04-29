"""
RateWatch validation: row counts, NULL rates, plausibility, and parity check
against prior reference dataset at C:\\Users\\dimut\\OneDrive\\research-data\\RateWatch.

Run after construct.py.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import DUCKDB_MEMORY_LIMIT, DUCKDB_THREADS, get_ratewatch_duckdb_path
from ratewatch.metadata import LAST_YEAR, load_product_registry
from utils.duckdb_utils import get_connection
from utils.logging_utils import get_logger

logger = get_logger(__name__)

REFERENCE_RDS = Path(r"C:\Users\dimut\OneDrive\research-data\RateWatch\rate_summary_2021_2024.rds")


def check_row_counts(conn: duckdb.DuckDBPyConnection, year: int) -> None:
    row = conn.execute(
        "SELECT row_count, n_branches, n_products, n_weeks FROM panel_metadata WHERE year=?",
        [year],
    ).fetchone()
    if not row:
        logger.error("[%d] No panel_metadata row", year)
        return
    rc, nb, np_, nw = row
    logger.info("[%d] panel_metadata: rows=%d branches=%d products=%d weeks=%d",
                year, rc, nb, np_, nw)
    if rc <= 0:
        logger.error("[%d] Empty Parquet", year)


def check_null_rates(conn: duckdb.DuckDBPyConnection, year: int) -> None:
    row = conn.execute("""
        SELECT
            COUNT(*) AS n,
            SUM(CASE WHEN week_date IS NULL THEN 1 ELSE 0 END) AS null_week,
            SUM(CASE WHEN account_number IS NULL THEN 1 ELSE 0 END) AS null_acct,
            SUM(CASE WHEN prd_typ_join IS NULL THEN 1 ELSE 0 END) AS null_prd,
            SUM(CASE WHEN apy IS NULL THEN 1 ELSE 0 END) AS null_apy,
            SUM(CASE WHEN rate IS NULL THEN 1 ELSE 0 END) AS null_rate,
            SUM(CASE WHEN rssd_id IS NULL THEN 1 ELSE 0 END) AS null_rssd
        FROM ratewatch WHERE year=?
    """, [year]).fetchone()
    n, null_week, null_acct, null_prd, null_apy, null_rate, null_rssd = row
    if not n:
        return
    logger.info("[%d] NULL rates: week=%.4f acct=%.4f prd=%.4f apy=%.4f rate=%.4f rssd=%.4f",
                year, null_week / n, null_acct / n, null_prd / n,
                null_apy / n, null_rate / n, null_rssd / n)
    if null_week or null_acct or null_prd:
        logger.error("[%d] Unexpected NULL in key column(s)", year)
    if null_rssd / n > 0.1:
        logger.warning("[%d] >10%% rows lack RSSD_ID join", year)


def check_plausibility(conn: duckdb.DuckDBPyConnection, year: int) -> None:
    row = conn.execute("""
        SELECT
            MIN(apy), MAX(apy), AVG(apy), MEDIAN(apy),
            MIN(rate), MAX(rate)
        FROM ratewatch WHERE year=?
    """, [year]).fetchone()
    min_apy, max_apy, avg_apy, med_apy, min_rate, max_rate = row
    logger.info("[%d] APY: min=%.4f max=%.4f avg=%.4f median=%.4f | RATE: min=%.4f max=%.4f",
                year, min_apy or 0, max_apy or 0, avg_apy or 0,
                med_apy or 0, min_rate or 0, max_rate or 0)
    if min_apy is not None and min_apy < 0:
        logger.error("[%d] Negative APY", year)
    if max_apy is not None and max_apy > 25:
        logger.warning("[%d] APY > 25%% — investigate outliers", year)

    by_prd = conn.execute("""
        SELECT prd_typ_join, COUNT(*) AS n, AVG(apy) AS mean_apy
        FROM ratewatch WHERE year=?
        GROUP BY 1 ORDER BY n DESC
    """, [year]).fetchall()
    for prd, n, mean_apy in by_prd:
        logger.info("[%d]   %-10s rows=%-9d mean_apy=%.4f", year, prd, n, mean_apy or 0)


def check_parity_quarterly(conn: duckdb.DuckDBPyConnection, year: int) -> None:
    """Aggregate new build to quarterly (qtr, branch, product) and compare to prior RDS."""
    if not REFERENCE_RDS.exists():
        logger.warning("Reference RDS missing: %s", REFERENCE_RDS)
        return

    try:
        import pyreadr  # type: ignore
    except ImportError:
        logger.warning("pyreadr not installed; skipping parity check. "
                       "Install: pip install pyreadr")
        return

    logger.info("Loading reference RDS: %s", REFERENCE_RDS)
    res = pyreadr.read_r(str(REFERENCE_RDS))
    ref = next(iter(res.values()))  # first (and only) dataframe
    ref = ref[ref["qtr"].astype(str).str.startswith(str(year))].copy()
    ref["qtr"] = ref["qtr"].astype(str)
    logger.info("Reference rows for %d: %d (across %d products)",
                year, len(ref), ref["PRD_TYP_JOIN"].nunique())

    # Restrict reference to our kept products
    kept_products = [p["prd_typ_join"]
                     for p in load_product_registry()[str(year)]["kept"]]
    ref_kept = ref[ref["PRD_TYP_JOIN"].isin(kept_products)].copy()
    logger.info("Reference rows after restricting to kept products: %d", len(ref_kept))

    # Build quarterly aggregate from our data
    agg = conn.execute("""
        WITH q AS (
            SELECT
                date_trunc('quarter', week_date) + INTERVAL '3 months' - INTERVAL '1 day' AS qtr,
                account_number,
                prd_typ_join,
                AVG(apy)    AS new_mean_apy,
                MEDIAN(apy) AS new_median_apy
            FROM ratewatch
            WHERE year=?
            GROUP BY 1, 2, 3
        )
        SELECT * FROM q
    """, [year]).fetchdf()
    agg["qtr"] = agg["qtr"].astype(str).str[:10]
    logger.info("New build quarterly rows: %d", len(agg))

    merged = ref_kept.merge(
        agg,
        left_on=["qtr", "ACCOUNTNUMBER", "PRD_TYP_JOIN"],
        right_on=["qtr", "account_number", "prd_typ_join"],
        how="inner",
    )
    logger.info("Joined rows (qtr,branch,product) match: %d / ref %d / new %d",
                len(merged), len(ref_kept), len(agg))

    if len(merged) == 0:
        logger.error("Parity check produced 0 joined rows")
        return

    # Reference rates appear to be percent; check scale by sampling
    # If ref mean_apy ~ 0.001-12, both percent and decimal possible. Compare
    # both raw and ×100.
    diff_raw = (merged["new_mean_apy"] - merged["mean_apy"]).abs()
    diff_x100 = (merged["new_mean_apy"] - merged["mean_apy"] * 100).abs()
    logger.info("Mean abs diff raw:  median=%.6f  p95=%.6f",
                diff_raw.median(), diff_raw.quantile(0.95))
    logger.info("Mean abs diff x100: median=%.6f  p95=%.6f",
                diff_x100.median(), diff_x100.quantile(0.95))

    # Pick smaller-diff scaling
    if diff_raw.median() < diff_x100.median():
        diff = diff_raw
        scale = "same"
    else:
        diff = diff_x100
        scale = "ref-decimal-vs-new-percent"
    pct_close = (diff < 0.01).mean()
    logger.info("Parity (%s): %.2f%% of triples within 0.01 APY",
                scale, pct_close * 100)


def main() -> None:
    p = argparse.ArgumentParser(description="Validate RateWatch construction.")
    p.add_argument("--year", type=int, default=LAST_YEAR)
    p.add_argument("--no-parity", action="store_true",
                   help="Skip parity check vs prior reference RDS")
    args = p.parse_args()

    conn = get_connection(
        get_ratewatch_duckdb_path(),
        read_only=True,
        threads=DUCKDB_THREADS,
        memory_limit=DUCKDB_MEMORY_LIMIT,
    )
    try:
        check_row_counts(conn, args.year)
        check_null_rates(conn, args.year)
        check_plausibility(conn, args.year)
        if not args.no_parity:
            check_parity_quarterly(conn, args.year)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
