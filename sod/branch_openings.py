"""
Identify branch openings from FDIC SOD data.

A branch "opens" in the first year its UNINUMBR appears in the dataset.

OTS-to-FDIC adjustment (2011):
    The Office of Thrift Supervision (OTS) was abolished under Dodd-Frank
    (effective July 21, 2011). OTS-chartered thrift branches were transferred
    to OCC supervision and newly appear in SOD from 2011 onward — despite
    having existed for years. Without adjustment, ~2,900 phantom "openings"
    are recorded in 2011.

    Exclusion rule (mirrors Dratnadiwakara et al.):
    Drop any RSSDID that shows new-branch activity in 2011 but had ZERO
    new branches in 2004-2010. These are regulatory transfers, not true opens.

Usage:
    python -m sod.branch_openings [--start YEAR] [--end YEAR] [--no-ots-adjust]
"""
from __future__ import annotations

import argparse

import duckdb

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import DUCKDB_MEMORY_LIMIT, DUCKDB_THREADS, get_sod_duckdb_path
from utils.logging_utils import get_logger

logger = get_logger(__name__)

# Years used to detect OTS transfers: no genuine openings in [PRE_START, PRE_END]
# but first appearance in OTS_TRANSFER_YEAR.
OTS_TRANSFER_YEAR = 2011
OTS_PRE_START = 2004
OTS_PRE_END = 2010


def get_ots_rssds(conn: duckdb.DuckDBPyConnection) -> list[int]:
    """
    Return list of RSSDs that are OTS-to-OCC regulatory transfers.

    Criteria: first new UNINUMBR in 2011, zero new UNINUMBRs in 2004-2010.
    These institutions weren't in SOD pre-2011 due to OTS supervision —
    their 2011 "openings" are data artifacts, not true branch expansions.
    """
    rows = conn.execute(f"""
        WITH first_year AS (
            SELECT UNINUMBR, RSSDID, MIN(YEAR) AS first_yr
            FROM sod
            GROUP BY UNINUMBR, RSSDID
        ),
        rssd_window AS (
            SELECT
                RSSDID,
                MAX(CASE WHEN first_yr = {OTS_TRANSFER_YEAR} THEN 1 ELSE 0 END)
                    AS opened_{OTS_TRANSFER_YEAR},
                MAX(CASE WHEN first_yr BETWEEN {OTS_PRE_START} AND {OTS_PRE_END} THEN 1 ELSE 0 END)
                    AS opened_pre
            FROM first_year
            WHERE first_yr BETWEEN {OTS_PRE_START} AND {OTS_TRANSFER_YEAR}
            GROUP BY RSSDID
        )
        SELECT RSSDID
        FROM rssd_window
        WHERE opened_{OTS_TRANSFER_YEAR} = 1
          AND opened_pre = 0
    """).fetchall()

    rssds = [int(r[0]) for r in rows]
    logger.info("OTS adjustment: %d RSSDs excluded as regulatory transfers", len(rssds))
    return rssds


def get_branch_openings(
    conn: duckdb.DuckDBPyConnection,
    start_year: int | None = None,
    end_year: int | None = None,
    adjust_ots: bool = True,
) -> "duckdb.DuckDBPyRelation":
    """
    Return a DuckDB relation of branch opening events.

    Columns: UNINUMBR, YEAR, RSSDID, RSSDHCR, NAMEFULL, NAMEBR,
             STALP, STCNTYBR, CITYBR, ZIPBR, DEPSUMBR, ASSET, BRSERTYP

    Parameters
    ----------
    start_year : int, optional
        First year to include (inclusive).
    end_year : int, optional
        Last year to include (inclusive).
    adjust_ots : bool
        If True (default), exclude RSSDs identified as OTS-to-OCC transfers.
    """
    ots_rssds = get_ots_rssds(conn) if adjust_ots else []

    year_filter = ""
    if start_year and end_year:
        year_filter = f"AND s.YEAR BETWEEN {start_year} AND {end_year}"
    elif start_year:
        year_filter = f"AND s.YEAR >= {start_year}"
    elif end_year:
        year_filter = f"AND s.YEAR <= {end_year}"

    ots_exclusion = ""
    if ots_rssds:
        ids = ", ".join(str(r) for r in ots_rssds)
        ots_exclusion = f"AND s.RSSDID NOT IN ({ids})"

    sql = f"""
        WITH first_year AS (
            SELECT UNINUMBR, MIN(YEAR) AS first_yr
            FROM sod
            GROUP BY UNINUMBR
        )
        SELECT
            s.UNINUMBR,
            s.YEAR,
            s.RSSDID,
            s.RSSDHCR,
            s.NAMEFULL,
            s.NAMEBR,
            s.STALP,
            s.STCNTYBR,
            s.CITYBR,
            s.ZIPBR,
            s.DEPSUMBR,
            s.ASSET,
            s.BRSERTYP
        FROM sod s
        JOIN first_year fy
          ON s.UNINUMBR = fy.UNINUMBR
         AND s.YEAR = fy.first_yr
        WHERE 1=1
          {year_filter}
          {ots_exclusion}
        ORDER BY s.YEAR, s.RSSDID, s.UNINUMBR
    """
    return conn.execute(sql)


def summarize_openings(
    conn: duckdb.DuckDBPyConnection,
    adjust_ots: bool = True,
) -> "duckdb.DuckDBPyRelation":
    """Institution-year count of branch openings, OTS-adjusted."""
    ots_rssds = get_ots_rssds(conn) if adjust_ots else []

    ots_exclusion = ""
    if ots_rssds:
        ids = ", ".join(str(r) for r in ots_rssds)
        ots_exclusion = f"AND s.RSSDID NOT IN ({ids})"

    return conn.execute(f"""
        WITH first_year AS (
            SELECT UNINUMBR, MIN(YEAR) AS first_yr
            FROM sod
            GROUP BY UNINUMBR
        ),
        openings AS (
            SELECT s.YEAR, s.RSSDID, s.NAMEFULL, COUNT(*) AS new_branches
            FROM sod s
            JOIN first_year fy ON s.UNINUMBR = fy.UNINUMBR AND s.YEAR = fy.first_yr
            WHERE 1=1 {ots_exclusion}
            GROUP BY s.YEAR, s.RSSDID, s.NAMEFULL
        )
        SELECT * FROM openings ORDER BY YEAR, new_branches DESC
    """)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Identify branch openings in FDIC SOD.")
    parser.add_argument("--start", type=int, help="First year to include")
    parser.add_argument("--end", type=int, help="Last year to include")
    parser.add_argument("--no-ots-adjust", action="store_true",
                        help="Skip OTS-to-OCC transfer exclusion")
    parser.add_argument("--summary", action="store_true",
                        help="Print institution-year opening counts instead of branch list")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    adjust = not args.no_ots_adjust

    conn = duckdb.connect(str(get_sod_duckdb_path()), read_only=True)
    conn.execute(f"PRAGMA threads={DUCKDB_THREADS}")
    conn.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY_LIMIT}'")

    try:
        if args.summary:
            df = summarize_openings(conn, adjust_ots=adjust).df()
        else:
            df = get_branch_openings(
                conn,
                start_year=args.start,
                end_year=args.end,
                adjust_ots=adjust,
            ).df()

        print(df.to_string())
        logger.info("Branch openings: %d rows", len(df))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
