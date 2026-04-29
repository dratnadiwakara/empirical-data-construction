"""
SOD ETL: raw CSV per year -> Parquet staging -> DuckDB view.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import duckdb

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import (
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_THREADS,
    PARQUET_COMPRESSION,
    get_sod_duckdb_path,
    get_sod_raw_path,
    get_sod_staging_path,
    get_sod_storage_path,
)
from sod.metadata import (
    ALL_YEARS,
    API_BASE,
    API_FIELDS,
    FIRST_YEAR,
    LAST_YEAR,
    NUMERIC_COLS,
    PANEL_METADATA_DDL,
)
from utils.duckdb_utils import ensure_table_exists, get_connection, upsert_row
from utils.logging_utils import get_logger

logger = get_logger(__name__)


def _sql_path(p: Path) -> str:
    return str(p).replace("\\", "/")


def _build_select_sql(csv_path: Path) -> str:
    exprs = []
    for col in API_FIELDS:
        q = f'"{col}"'
        if col in NUMERIC_COLS:
            exprs.append(
                f"CASE WHEN TRIM({q}) = '' OR {q} IS NULL THEN NULL "
                f"ELSE TRY_CAST(TRIM({q}) AS BIGINT) END AS {q}"
            )
        else:
            exprs.append(f"NULLIF(TRIM({q}), '') AS {q}")
    return (
        "SELECT\n    "
        + ",\n    ".join(exprs)
        + f"\nFROM read_csv('{_sql_path(csv_path)}', header=true, all_varchar=true)"
    )


def construct_year(year: int, force: bool = False) -> Optional[int]:
    """Parse raw CSV for one year into a snappy Parquet file."""
    staging_dir = get_sod_staging_path(year)
    parquet_path = staging_dir / "data.parquet"

    if parquet_path.exists() and not force:
        logger.info("[%d] Parquet exists; skipping (--force to rebuild)", year)
        return None

    raw_dir = get_sod_raw_path(year)
    csv_files = sorted(raw_dir.glob(f"sod_{year}.csv"))
    if not csv_files:
        logger.warning("[%d] No raw CSV in %s; run download.py first", year, raw_dir)
        return None

    csv_path = csv_files[0]
    conn = duckdb.connect(":memory:")
    conn.execute(f"PRAGMA threads={DUCKDB_THREADS}")
    conn.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    try:
        select_sql = _build_select_sql(csv_path)
        tmp = parquet_path.with_suffix(".parquet.tmp")
        conn.execute(f"""
            COPY ({select_sql})
            TO '{_sql_path(tmp)}'
            (FORMAT PARQUET, COMPRESSION '{PARQUET_COMPRESSION}')
        """)
        row_count: int = conn.execute(
            f"SELECT count(*) FROM read_parquet('{_sql_path(tmp)}')"
        ).fetchone()[0]
        tmp.replace(parquet_path)
    finally:
        conn.close()

    logger.info("[%d] Wrote %d rows -> %s", year, row_count, parquet_path)
    _upsert_metadata(year, row_count, parquet_path)
    return row_count


def _upsert_metadata(year: int, row_count: int, parquet_path: Path) -> None:
    db_conn = get_connection(
        get_sod_duckdb_path(),
        threads=DUCKDB_THREADS,
        memory_limit=DUCKDB_MEMORY_LIMIT,
    )
    try:
        ensure_table_exists(db_conn, "panel_metadata", PANEL_METADATA_DDL)
        upsert_row(
            db_conn,
            "panel_metadata",
            {
                "year": year,
                "row_count": row_count,
                "source_url": API_BASE,
                "built_at": datetime.now(timezone.utc).isoformat(),
                "parquet_path": str(parquet_path),
            },
            ["year"],
        )
    finally:
        db_conn.close()


def recreate_views() -> None:
    """Create or replace the `sod` view over all year-partitioned Parquets."""
    staging_root = get_sod_storage_path("staging")
    parquets = sorted(staging_root.glob("year=*/data.parquet"))
    if not parquets:
        logger.warning("No staging Parquets found; skipping view creation")
        return

    paths_sql = ", ".join(f"'{_sql_path(p)}'" for p in parquets)
    db_conn = get_connection(
        get_sod_duckdb_path(),
        threads=DUCKDB_THREADS,
        memory_limit=DUCKDB_MEMORY_LIMIT,
    )
    try:
        db_conn.execute(f"""
            CREATE OR REPLACE VIEW sod AS
            SELECT *
            FROM read_parquet([{paths_sql}], hive_partitioning=true, union_by_name=true)
        """)
        logger.info("sod VIEW created over %d parquets", len(parquets))
    finally:
        db_conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Construct SOD Parquet and DuckDB views.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--year", type=int, help=f"Single year ({FIRST_YEAR}–{LAST_YEAR})")
    group.add_argument("--all", action="store_true", help="Process all years")
    parser.add_argument("--force", action="store_true", help="Reprocess even if Parquet exists")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    years = ALL_YEARS if args.all else [args.year]

    for year in years:
        construct_year(year, force=args.force)

    recreate_views()
    logger.info("SOD construct step complete.")


if __name__ == "__main__":
    main()
