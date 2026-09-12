"""
Compustat ETL: gzipped CSV -> Parquet (Hive-partitioned by fyear) -> DuckDB view.

Pipeline:
  raw/compustat_fund.gz  ──read_csv─▶ typed SELECT ──COPY──▶
    staging/fyear=YYYY/data_*.parquet
  -> CREATE OR REPLACE VIEW compustat over the parquet tree
  -> reload variable_dictionary table from raw/compustat_vars.csv

The canonical "one row per company-fiscal-year" filter
(indfmt='INDL' AND datafmt='STD' AND consol='C') is applied during construct.
Raw duplicates (FS, restated, non-consolidated rows) are dropped here so
downstream queries do not have to.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import shutil
from datetime import datetime, timezone
from pathlib import Path

import duckdb

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import (
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_THREADS,
    PARQUET_COMPRESSION,
    get_compustat_duckdb_path,
    get_compustat_raw_path,
    get_compustat_staging_path,
)
from compustat.metadata import (
    EQUITY_RATIOS_VIEW_SQL,
    PANEL_METADATA_DDL,
    SOURCE_FUND_FILE,
    SOURCE_LABEL,
    SOURCE_VARS_FILE,
    STD_FUNDAMENTALS_WHERE,
    VARIABLE_DICTIONARY_DDL,
    load_var_descriptions,
    load_var_type_map,
)
from utils.duckdb_utils import ensure_table_exists, get_connection, upsert_row
from utils.logging_utils import get_logger

logger = get_logger(__name__)


def _sql_path(p: Path) -> str:
    """Forward-slash path string for embedding in DuckDB SQL literals."""
    return str(p).replace("\\", "/")


def _read_gz_header(gz_path: Path) -> list[str]:
    with gzip.open(gz_path, "rt", encoding="utf-8", newline="") as fh:
        return next(csv.reader(fh))


def _build_select_sql(gz_path: Path, header: list[str], type_map: dict[str, str]) -> str:
    """Build the typed SELECT driving COPY ... PARTITION_BY (fyear).

    All raw values are first read as VARCHAR (all_varchar=true) for safety:
    Compustat occasionally emits unparseable date/number tokens. Each column
    is then TRY_CAST to its target type, so bad values land as NULL instead
    of aborting the build.
    """
    exprs: list[str] = []
    for col in header:
        ddb_type = type_map.get(col, "VARCHAR")
        q = f'"{col}"'
        trimmed = f"NULLIF(TRIM({q}), '')"
        if ddb_type == "VARCHAR":
            exprs.append(f"{trimmed} AS {q}")
        else:
            exprs.append(f"TRY_CAST({trimmed} AS {ddb_type}) AS {q}")
    return (
        "SELECT\n    "
        + ",\n    ".join(exprs)
        + f"\nFROM read_csv('{_sql_path(gz_path)}',\n"
        + "                 header=true, all_varchar=true,\n"
        + "                 compression='gzip', nullstr=['', 'NA'])\n"
        + f"WHERE {STD_FUNDAMENTALS_WHERE}\n"
        + "  AND fyear IS NOT NULL"
    )


def build_parquets(force: bool = False) -> bool:
    """Rewrite the partitioned parquet tree from raw/compustat_fund.gz.

    Returns True if a rebuild ran, False if skipped because staging already exists.
    """
    raw_dir = get_compustat_raw_path()
    gz_path = raw_dir / SOURCE_FUND_FILE
    vars_path = raw_dir / SOURCE_VARS_FILE
    if not gz_path.exists():
        raise FileNotFoundError(f"{gz_path} missing — run compustat.download first")
    if not vars_path.exists():
        raise FileNotFoundError(f"{vars_path} missing — run compustat.download first")

    staging_dir = get_compustat_staging_path()
    tmp_dir = staging_dir.parent / "staging.tmp"

    if any(staging_dir.glob("fyear=*")) and not force:
        logger.info("staging exists with partitions; skipping (use --force to rebuild)")
        return False

    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)

    type_map = load_var_type_map(vars_path)
    header = _read_gz_header(gz_path)
    missing = [c for c in header if c not in type_map]
    if missing:
        logger.warning(
            "%d gz column(s) absent from vars CSV; defaulting to VARCHAR: %s%s",
            len(missing), missing[:10], "..." if len(missing) > 10 else "",
        )

    select_sql = _build_select_sql(gz_path, header, type_map)
    copy_sql = (
        f"COPY (\n{select_sql}\n)\n"
        f"TO '{_sql_path(tmp_dir)}' "
        f"(FORMAT PARQUET, PARTITION_BY (fyear), COMPRESSION '{PARQUET_COMPRESSION}')"
    )

    logger.info("COPY ... PARTITION_BY (fyear) -> %s", tmp_dir)
    conn = duckdb.connect(":memory:")
    conn.execute(f"PRAGMA threads={DUCKDB_THREADS}")
    conn.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    try:
        conn.execute(copy_sql)
    finally:
        conn.close()

    # Atomic-ish swap: remove old staging, rename tmp into place.
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    tmp_dir.rename(staging_dir)
    logger.info("staging rebuilt at %s", staging_dir)
    return True


def _count_rows_per_fyear() -> dict[int, int]:
    """Count rows in each fyear partition by scanning the staging tree."""
    staging_dir = get_compustat_staging_path()
    parquets = sorted(staging_dir.glob("fyear=*/*.parquet"))
    if not parquets:
        return {}
    paths_sql = ", ".join(f"'{_sql_path(p)}'" for p in parquets)
    conn = duckdb.connect(":memory:")
    try:
        rows = conn.execute(f"""
            SELECT CAST(REGEXP_EXTRACT(filename, 'fyear=(\\d+)', 1) AS INTEGER) AS fyear,
                   COUNT(*) AS row_count
            FROM read_parquet([{paths_sql}], filename=true)
            GROUP BY 1 ORDER BY 1
        """).fetchall()
    finally:
        conn.close()
    return {int(y): int(n) for (y, n) in rows if y is not None}


def upsert_panel_metadata(counts: dict[int, int]) -> None:
    db_conn = get_connection(
        get_compustat_duckdb_path(),
        threads=DUCKDB_THREADS,
        memory_limit=DUCKDB_MEMORY_LIMIT,
    )
    try:
        ensure_table_exists(db_conn, "panel_metadata", PANEL_METADATA_DDL)
        staging_dir = get_compustat_staging_path()
        built = datetime.now(timezone.utc).isoformat()
        for year, n in sorted(counts.items()):
            upsert_row(
                db_conn,
                "panel_metadata",
                {
                    "year": int(year),
                    "row_count": int(n),
                    "source_url": SOURCE_LABEL,
                    "built_at": built,
                    "parquet_path": str(staging_dir / f"fyear={year}"),
                },
                ["year"],
            )
        logger.info("panel_metadata upserted for %d fyear(s)", len(counts))
    finally:
        db_conn.close()


def reload_variable_dictionary() -> None:
    raw_dir = get_compustat_raw_path()
    vars_path = raw_dir / SOURCE_VARS_FILE
    if not vars_path.exists():
        logger.warning("vars CSV missing at %s; skipping variable_dictionary reload", vars_path)
        return
    rows = load_var_descriptions(vars_path)
    db_conn = get_connection(
        get_compustat_duckdb_path(),
        threads=DUCKDB_THREADS,
        memory_limit=DUCKDB_MEMORY_LIMIT,
    )
    try:
        ensure_table_exists(db_conn, "variable_dictionary", VARIABLE_DICTIONARY_DDL)
        db_conn.execute("DELETE FROM variable_dictionary")
        if rows:
            db_conn.executemany(
                "INSERT INTO variable_dictionary (variable_name, type, description) VALUES (?, ?, ?)",
                [(r["variable_name"], r["type"], r["description"]) for r in rows],
            )
        logger.info("variable_dictionary reloaded (%d rows)", len(rows))
    finally:
        db_conn.close()


def recreate_views() -> None:
    """Create or replace the `compustat` view over the partitioned parquet tree."""
    staging_dir = get_compustat_staging_path()
    parquets = sorted(staging_dir.glob("fyear=*/*.parquet"))
    if not parquets:
        logger.warning("no staging parquets found; skipping view creation")
        return
    paths_sql = ", ".join(f"'{_sql_path(p)}'" for p in parquets)
    db_conn = get_connection(
        get_compustat_duckdb_path(),
        threads=DUCKDB_THREADS,
        memory_limit=DUCKDB_MEMORY_LIMIT,
    )
    try:
        db_conn.execute(f"""
            CREATE OR REPLACE VIEW compustat AS
            SELECT *
            FROM read_parquet(
                [{paths_sql}],
                hive_partitioning = true,
                union_by_name = true
            )
        """)
        logger.info("compustat VIEW created over %d parquet file(s)", len(parquets))
        db_conn.execute(EQUITY_RATIOS_VIEW_SQL)
        logger.info("compustat_equity_ratios VIEW created")
    finally:
        db_conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Construct Compustat Parquet + DuckDB views.")
    parser.add_argument("--force", action="store_true",
                        help="Rebuild staging parquets even if present.")
    parser.add_argument("--views-only", action="store_true",
                        help="Skip parquet rebuild; refresh variable_dictionary and views only.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.views_only:
        build_parquets(force=args.force)
    counts = _count_rows_per_fyear()
    if counts:
        upsert_panel_metadata(counts)
    reload_variable_dictionary()
    recreate_views()
    logger.info("Compustat construct step complete.")


if __name__ == "__main__":
    main()
