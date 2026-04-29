"""
IRS SOI ZIP code data constructor.

For each year: raw CSV → snappy Parquet (zip-year aggregated) → DuckDB view.

Aggregation: sum IRS fields across agi_stubs 1–6 per zipcode (stub 0 excluded
to avoid double-counting the totals row present in some years).

Usage:
    python -m irs.construct --year 2019
    python -m irs.construct --all
    python -m irs.construct --all --force
"""

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import duckdb

from config import (
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_THREADS,
    PARQUET_COMPRESSION,
    get_irs_duckdb_path,
    get_irs_manifest_path,
    get_irs_raw_path,
    get_irs_staging_path,
    get_irs_storage_path,
)
from irs.metadata import (
    AMOUNT_DOLLAR_YEARS,
    AVAILABLE_YEARS,
    ERA_A_YEARS,
    IRS_FIELD_MAP,
    PANEL_METADATA_DDL,
    SOURCE_URLS,
)
from utils.duckdb_utils import ensure_table_exists, get_connection, upsert_row

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_csv_path(year: int) -> Optional[Path]:
    """Locate the raw CSV for a given year. Checks manifest first, then fallback paths."""
    manifest_path = get_irs_manifest_path()
    if manifest_path.exists():
        import json as _json
        manifest = _json.loads(manifest_path.read_text(encoding="utf-8"))
        entry = manifest.get(str(year), {})
        csv_str = entry.get("csv_path")
        if csv_str:
            p = Path(csv_str)
            if p.exists():
                return p

    # Fallback: well-known paths
    raw_dir = get_irs_raw_path(year)
    if year in ERA_A_YEARS:
        for name in ["combined.csv", f"zipcode{str(year)[2:]}.csv"]:
            p = raw_dir / name
            if p.exists():
                return p
    else:
        yy = str(year)[2:]
        p = raw_dir / f"{yy}zpallagi.csv"
        if p.exists():
            return p
    return None


def _build_select_sql(csv_path: Path) -> str:
    """
    Build DuckDB SQL that:
    1. Reads the raw CSV with all_varchar=true (avoids type inference failures)
    2. Normalizes column names to lowercase via a column alias layer
    3. Filters to agi_stub 1–6 (exclude stub 0 total rows)
    4. Excludes non-ZIP rows (00000 = statewide, 99999 = other/foreign)
    5. Groups by zipcode and sums all target fields
    6. Casts to final types
    """
    # Build the SUM expressions with explicit casts
    sum_exprs = []
    for src, dst in IRS_FIELD_MAP.items():
        if dst.startswith("n_"):
            cast = "BIGINT"
        else:
            cast = "DOUBLE"
        sum_exprs.append(
            f"    SUM(TRY_CAST({src} AS {cast})) AS {dst}"
        )

    sums_sql = ",\n".join(sum_exprs)

    csv_str = str(csv_path).replace("\\", "/")
    return f"""
SELECT
    LPAD(CAST(zipcode AS VARCHAR), 5, '0') AS zipcode,
{sums_sql}
FROM read_csv(
    '{csv_str}',
    header = true,
    all_varchar = true,
    ignore_errors = true
)
WHERE TRY_CAST(
        COALESCE(agi_stub, agistub) AS INTEGER
      ) BETWEEN 1 AND 6
  AND zipcode NOT IN ('0', '00000', '99999', '')
  AND zipcode IS NOT NULL
GROUP BY zipcode
"""


def _build_select_sql_case_insensitive(csv_path: Path, conn: duckdb.DuckDBPyConnection,
                                       year: Optional[int] = None) -> str:
    """
    Probe actual column names in the CSV, then build select SQL with correct
    case-matched field references. Handles the mixed-case headers in older files.
    """
    csv_str = str(csv_path).replace("\\", "/")
    probe = conn.execute(
        f"SELECT * FROM read_csv('{csv_str}', header=true, all_varchar=true, "
        f"ignore_errors=true) LIMIT 0"
    ).description
    actual_cols = {desc[0].lower(): desc[0] for desc in probe}

    # Map each IRS code to its actual column name in this file
    field_map_actual: dict[str, str] = {}
    for src in IRS_FIELD_MAP:
        if src in actual_cols:
            field_map_actual[src] = actual_cols[src]

    # Handle zipcode column variations
    zip_col = actual_cols.get("zipcode") or actual_cols.get("zip") or actual_cols.get("zipcd")
    if not zip_col:
        raise ValueError(f"No zipcode column found in {csv_path}. Columns: {list(actual_cols)}")

    # Handle agi_stub column variations (older files use "agi_class";
    # 1998/2001/2002 combined CSV has no stub column at all — already aggregated)
    stub_col = (actual_cols.get("agi_stub") or actual_cols.get("agistub")
                or actual_cols.get("agi_class"))
    no_stub = stub_col is None  # report-style years: already zip-level totals, no filter needed

    # Some years (2007, 2008) published A-series in actual dollars not $thousands
    scale_amounts = year in AMOUNT_DOLLAR_YEARS

    sum_exprs = []
    for src, dst in IRS_FIELD_MAP.items():
        actual = field_map_actual.get(src)
        is_amount = not dst.startswith("n_")
        if actual:
            cast = "BIGINT" if not is_amount else "DOUBLE"
            if is_amount and scale_amounts:
                expr = f"SUM(TRY_CAST(\"{actual}\" AS DOUBLE)) / 1000.0"
            else:
                expr = f"SUM(TRY_CAST(\"{actual}\" AS {cast}))"
            sum_exprs.append(f"    {expr} AS {dst}")
        else:
            null_val = "NULL::BIGINT" if not is_amount else "NULL::DOUBLE"
            sum_exprs.append(f"    {null_val} AS {dst}")
            logger.warning("  column %s not found in %s — will be NULL", src, csv_path.name)

    sums_sql = ",\n".join(sum_exprs)

    if no_stub:
        # 1998/2001/2002: one row per ZIP already, no stub filter needed
        where_clause = f"""
WHERE \"{zip_col}\" NOT IN ('0', '00000', '99999', '')
  AND \"{zip_col}\" IS NOT NULL"""
        group_by = f'GROUP BY \"{zip_col}\"'
    else:
        # Use BETWEEN 1 AND 8 to capture both 6-class years (most years)
        # and 7-class years (2006-2007 where class 7 = top income tier).
        # State/national total codes (≥9 in 2006) are excluded.
        where_clause = f"""
WHERE TRY_CAST(\"{stub_col}\" AS INTEGER) BETWEEN 1 AND 8
  AND \"{zip_col}\" NOT IN ('0', '00000', '99999', '')
  AND \"{zip_col}\" IS NOT NULL"""
        group_by = f'GROUP BY \"{zip_col}\"'

    return f"""
SELECT
    LPAD(CAST(\"{zip_col}\" AS VARCHAR), 5, '0') AS zipcode,
{sums_sql}
FROM read_csv(
    '{csv_str}',
    header = true,
    all_varchar = true,
    ignore_errors = true
)
{where_clause}
{group_by}
"""


# ── Main construction ─────────────────────────────────────────────────────────

def construct_year(year: int, force: bool = False) -> Optional[int]:
    staging_dir = get_irs_staging_path(year)
    parquet_path = staging_dir / "data.parquet"

    if not force and parquet_path.exists():
        logger.info("year %d: parquet exists, skipping (use --force to rebuild)", year)
        return None

    csv_path = _get_csv_path(year)
    if csv_path is None:
        logger.error("year %d: raw CSV not found — run download first", year)
        return None

    logger.info("year %d: constructing from %s", year, csv_path.name)

    conn = duckdb.connect()
    conn.execute(f"PRAGMA threads={DUCKDB_THREADS}")
    conn.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY_LIMIT}'")

    select_sql = _build_select_sql_case_insensitive(csv_path, conn, year=year)

    # Inject year column and write to Parquet (atomic)
    tmp_parquet = parquet_path.with_suffix(".parquet.tmp")
    conn.execute(f"""
        COPY (
            SELECT {year} AS year, * FROM ({select_sql})
        )
        TO '{str(tmp_parquet).replace(chr(92), '/')}' (
            FORMAT PARQUET,
            COMPRESSION '{PARQUET_COMPRESSION}'
        )
    """)
    tmp_parquet.replace(parquet_path)

    row_count = conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('{str(parquet_path).replace(chr(92), '/')}')"
    ).fetchone()[0]
    conn.close()

    logger.info("year %d: %d zip codes written → %s", year, row_count, parquet_path)

    # Upsert panel_metadata
    db_conn = get_connection(
        get_irs_duckdb_path(),
        threads=DUCKDB_THREADS,
        memory_limit=DUCKDB_MEMORY_LIMIT,
    )
    ensure_table_exists(db_conn, "panel_metadata", PANEL_METADATA_DDL)
    upsert_row(db_conn, "panel_metadata", {
        "year": year,
        "row_count": row_count,
        "source_url": SOURCE_URLS[year],
        "built_at": datetime.now(timezone.utc).isoformat(),
        "parquet_path": str(parquet_path),
    }, ["year"])
    db_conn.close()

    return row_count


def recreate_views() -> None:
    """Rebuild the `irs` view over all year-partitioned Parquet files."""
    staging_root = get_irs_storage_path("staging")
    parquets = sorted(staging_root.glob("year=*/data.parquet"))
    if not parquets:
        logger.warning("No Parquet files found; view not created")
        return

    paths_sql = ", ".join(
        f"'{str(p).replace(chr(92), '/')}'" for p in parquets
    )

    db_conn = get_connection(
        get_irs_duckdb_path(),
        threads=DUCKDB_THREADS,
        memory_limit=DUCKDB_MEMORY_LIMIT,
    )
    ensure_table_exists(db_conn, "panel_metadata", PANEL_METADATA_DDL)

    db_conn.execute(f"""
        CREATE OR REPLACE VIEW irs AS
        SELECT *,
            CASE WHEN n_returns > 0 THEN n_returns_wages::DOUBLE / n_returns ELSE NULL END
                AS salary_frac,
            CASE WHEN n_returns > 0 THEN n_returns_dividend::DOUBLE / n_returns ELSE NULL END
                AS dividend_frac,
            CASE WHEN n_returns > 0 THEN n_returns_business::DOUBLE / n_returns ELSE NULL END
                AS business_frac,
            CASE WHEN n_returns > 0 THEN n_returns_capital_gain::DOUBLE / n_returns ELSE NULL END
                AS capital_gain_frac
        FROM read_parquet([{paths_sql}], hive_partitioning = true, union_by_name = true)
    """)
    db_conn.close()
    logger.info("view `irs` recreated over %d year partitions", len(parquets))


def construct_all(force: bool = False) -> None:
    for year in AVAILABLE_YEARS:
        try:
            construct_year(year, force=force)
        except Exception as exc:
            logger.error("year %d: FAILED — %s", year, exc)
    recreate_views()


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Construct IRS SOI ZIP code Parquet + DuckDB")
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument("--year", type=int, help="Single tax year to construct")
    grp.add_argument("--all", action="store_true", help="Construct all available years")
    parser.add_argument("--force", action="store_true", help="Rebuild even if Parquet exists")
    parser.add_argument("--views-only", action="store_true",
                        help="Only recreate DuckDB views (skip Parquet rebuild)")
    args = parser.parse_args()

    if args.views_only:
        recreate_views()
    elif args.all:
        construct_all(force=args.force)
    else:
        construct_year(args.year, force=args.force)
        recreate_views()


if __name__ == "__main__":
    main()
