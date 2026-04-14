"""
CRA ETL (1996-2024).

Uses DuckDB to parse fixed-width .dat files via substr() expressions,
writes Hive-partitioned Parquet, and creates panel views in the master
CRA DuckDB database.

Processing order: 2024 first (with validation), then backfill on request.

Usage
-----
    python -m cra.construct --year 2024          # process single year
    python -m cra.construct --year 2024 --force  # reprocess even if Parquet exists
    python -m cra.construct --all                # process all years 2024 -> 1996
    python -m cra.construct --validate 2024      # run validation only
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import (
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_THREADS,
    PARQUET_COMPRESSION,
    get_cra_duckdb_path,
    get_cra_raw_path,
    get_cra_staging_path,
)
from cra.metadata import (
    AGGREGATE_LAYOUTS,
    AGGREGATE_NUMERIC_COLS,
    AGGREGATE_TABLES,
    ALL_YEARS,
    CENSUS_TRACT_FIPS_SQL,
    COUNTY_FIPS_SQL,
    DISCLOSURE_LAYOUTS,
    DISCLOSURE_NUMERIC_COLS,
    DISCLOSURE_TABLE_ID_PREFIXES,
    DISCLOSURE_TABLES,
    PANEL_METADATA_DDL,
    SPLIT_FILE_YEAR,
    TRANSMITTAL_LAYOUTS,
    TRANSMITTAL_NUMERIC_COLS,
    VALIDATION_2024,
    get_dat_filename,
    get_download_url,
    get_era,
    is_split_file_year,
)
from utils.duckdb_utils import ensure_table_exists, get_connection, upsert_row
from utils.logging_utils import get_logger, log_step

logger = get_logger(__name__)


def _sql_path(p: Path) -> str:
    """Forward-slash path for DuckDB SQL."""
    return str(p).replace("\\", "/")


# ── .dat file discovery ───────────────────────────────────────────────────────

def _find_dat_files(
    year: int, file_type: str, table_stems: dict[str, str], raw_dir: Path
) -> list[Path]:
    """
    Find .dat files for a given year and file type.

    Pre-2016: single file in the raw dir (the extracted dat)
    2016+: per-table .dat files named cra{year}_{Type}_{Stem}.dat
    """
    if is_split_file_year(year):
        files = []
        for canonical_id, stem in table_stems.items():
            expected = get_dat_filename(year, file_type, stem)
            candidate = raw_dir / expected
            if candidate.exists():
                files.append(candidate)
            else:
                alt = _find_case_insensitive(raw_dir, expected)
                if alt:
                    files.append(alt)
                else:
                    logger.warning("Missing expected file: %s", expected)
        return files
    else:
        # Pre-2016: single combined file; find largest .dat
        candidates = list(raw_dir.glob("*.dat")) + list(raw_dir.glob("*.DAT"))
        type_hint = file_type.lower()
        matched = [p for p in candidates if type_hint in p.name.lower()]
        if matched:
            return [max(matched, key=lambda p: p.stat().st_size)]
        if candidates:
            return [max(candidates, key=lambda p: p.stat().st_size)]
        return []


def _find_case_insensitive(directory: Path, filename: str) -> Optional[Path]:
    """Find a file case-insensitively."""
    lower = filename.lower()
    for p in directory.iterdir():
        if p.name.lower() == lower:
            return p
    return None


# ── Fixed-width parsing via DuckDB ────────────────────────────────────────────

def _build_substr_select(
    layout: list[tuple[str, int, int]],
    numeric_cols: list[str],
    include_geo: bool = True,
) -> str:
    """
    Build SQL SELECT expressions to parse fixed-width fields from a single
    VARCHAR column 'line' using substr().

    Numeric columns are cast to BIGINT. Geography columns get county_fips
    and census_tract_fips computed columns.
    """
    exprs: list[str] = []
    has_state = False
    has_county = False
    has_tract = False

    for field_name, start, end in layout:
        width = end - start + 1
        raw_expr = f"TRIM(substr(line, {start}, {width}))"

        if field_name in numeric_cols:
            exprs.append(
                f"CASE WHEN TRIM(substr(line, {start}, {width})) = '' THEN NULL"
                f" ELSE TRY_CAST(TRIM(substr(line, {start}, {width})) AS BIGINT) END"
                f' AS "{field_name}"'
            )
        elif field_name == "table_id":
            exprs.append(f'{raw_expr} AS "{field_name}"')
        else:
            exprs.append(f'{raw_expr} AS "{field_name}"')

        if field_name == "state":
            has_state = True
        if field_name == "county":
            has_county = True
        if field_name == "census_tract":
            has_tract = True

    if include_geo and has_state and has_county:
        exprs.append(f"({COUNTY_FIPS_SQL}) AS county_fips")
        if has_tract:
            exprs.append(f"({CENSUS_TRACT_FIPS_SQL}) AS census_tract_fips")

    return ",\n        ".join(exprs)


def _table_id_field_width(layout: list[tuple[str, int, int]]) -> int:
    """Width of the raw table_id field (chars 1..w on each line)."""
    if layout and layout[0][0] == "table_id":
        _, start, end = layout[0]
        return end - start + 1
    return 5


def _build_fwf_sql(
    dat_path: Path,
    layout: list[tuple[str, int, int]],
    numeric_cols: list[str],
    table_id_filter: Optional[list[str]] = None,
    table_id_prefixes: Optional[list[str]] = None,
    include_geo: bool = True,
) -> str:
    """
    Build a full SQL query that reads a fixed-width .dat file and parses it.

    The file is read as a single VARCHAR column 'line' with no delimiter,
    then substr() expressions extract each field.
    """
    select_exprs = _build_substr_select(layout, numeric_cols, include_geo)
    sql_path = _sql_path(dat_path)

    # Read file as a single column. Use a null byte delimiter so the entire
    # line is captured in one column. auto_detect=false prevents DuckDB from
    # guessing a delimiter.
    # FFIEC .dat files are not always UTF-8; latin-1 accepts all byte values (e.g. Ñ in addresses).
    from_clause = (
        f"read_csv('{sql_path}',"
        f" columns={{'line': 'VARCHAR'}},"
        f" delim='\\0',"
        f" header=false,"
        f" auto_detect=false,"
        f" quote='',"
        f" escape='',"
        f" encoding='latin-1')"
    )

    where_clause = ""
    tw = _table_id_field_width(layout)
    if table_id_prefixes:
        parts = [
            f"TRIM(substr(line, 1, {tw})) LIKE '{pfx}%'" for pfx in table_id_prefixes
        ]
        where_clause = "\n    WHERE " + " OR ".join(parts)
    elif table_id_filter:
        unique_ids = sorted({t.strip()[:tw] for t in table_id_filter})
        trimmed_conditions = " OR ".join(
            f"TRIM(substr(line, 1, {tw})) = '{tid}'" for tid in unique_ids
        )
        where_clause = f"\n    WHERE {trimmed_conditions}"

    return f"""
    SELECT
        {select_exprs}
    FROM {from_clause}
    AS raw(line){where_clause}
    """


# ── Parquet writing ───────────────────────────────────────────────────────────

def _write_parquet(
    conn: duckdb.DuckDBPyConnection,
    sql: str,
    parquet_path: Path,
) -> int:
    """Execute sql and write results to parquet. Returns row count."""
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = parquet_path.with_suffix(".parquet.tmp")
    sql_out = _sql_path(tmp)

    copy_sql = f"""
    COPY (
        {sql}
    ) TO '{sql_out}' (FORMAT PARQUET, COMPRESSION '{PARQUET_COMPRESSION}')
    """
    conn.execute(copy_sql)

    row_count = conn.execute(
        f"SELECT count(*) FROM read_parquet('{sql_out}')"
    ).fetchone()[0]

    tmp.replace(parquet_path)
    return row_count


# ── Per-table-type construction ───────────────────────────────────────────────

def construct_transmittal(year: int, conn: duckdb.DuckDBPyConnection, force: bool = False) -> Optional[int]:
    """Parse transmittal .dat file and write to Parquet."""
    staging_dir = get_cra_staging_path("transmittal", year)
    parquet_path = staging_dir / "data.parquet"

    if parquet_path.exists() and not force:
        logger.info("[%d/transmittal] Parquet exists — skipping (use --force to reprocess)", year)
        return None

    raw_dir = get_cra_raw_path(year)
    era = get_era(year)
    layout = TRANSMITTAL_LAYOUTS[era]

    # Find the transmittal .dat file
    candidates = list(raw_dir.glob("*trans*")) + list(raw_dir.glob("*Trans*"))
    dat_files = [p for p in candidates if p.suffix.lower() in (".dat", ".txt")]
    if not dat_files:
        all_dats = list(raw_dir.glob("*.dat")) + list(raw_dir.glob("*.DAT"))
        dat_files = [p for p in all_dats if "trans" in p.name.lower()]

    if not dat_files:
        logger.warning("[%d/transmittal] No .dat file found in %s", year, raw_dir)
        return None

    dat_path = max(dat_files, key=lambda p: p.stat().st_size)
    logger.info("[%d/transmittal] Parsing %s (era=%s)", year, dat_path.name, era)

    numeric_cols = TRANSMITTAL_NUMERIC_COLS if era != "1996" else []
    sql = _build_fwf_sql(dat_path, layout, numeric_cols, include_geo=False)
    row_count = _write_parquet(conn, sql, parquet_path)

    logger.info("[%d/transmittal] Wrote %d rows to %s", year, row_count, parquet_path)
    return row_count


def construct_aggregate(year: int, conn: duckdb.DuckDBPyConnection, force: bool = False) -> Optional[int]:
    """Parse aggregate .dat file(s) and write to Parquet."""
    staging_dir = get_cra_staging_path("aggregate", year)
    parquet_path = staging_dir / "data.parquet"

    if parquet_path.exists() and not force:
        logger.info("[%d/aggregate] Parquet exists — skipping", year)
        return None

    raw_dir = get_cra_raw_path(year)
    era = get_era(year)
    layout = AGGREGATE_LAYOUTS[era]

    if is_split_file_year(year):
        # Post-2016: separate .dat files per table; union them
        dat_files = _find_dat_files(year, "aggr", AGGREGATE_TABLES, raw_dir)
        if not dat_files:
            logger.warning("[%d/aggregate] No .dat files found", year)
            return None

        sqls = []
        for dat in dat_files:
            sqls.append(_build_fwf_sql(dat, layout, AGGREGATE_NUMERIC_COLS))
        union_sql = "\n    UNION ALL\n    ".join(sqls)
        row_count = _write_parquet(conn, union_sql, parquet_path)
    else:
        # Pre-2016: single combined file; filter by table_id
        candidates = list(raw_dir.glob("*.dat")) + list(raw_dir.glob("*.DAT"))
        aggr_files = [p for p in candidates if "aggr" in p.name.lower() or "exp_aggr" in p.name.lower()]
        if not aggr_files:
            aggr_files = candidates
        if not aggr_files:
            logger.warning("[%d/aggregate] No .dat file found in %s", year, raw_dir)
            return None

        dat_path = max(aggr_files, key=lambda p: p.stat().st_size)
        table_ids = list(AGGREGATE_TABLES.keys())
        sql = _build_fwf_sql(dat_path, layout, AGGREGATE_NUMERIC_COLS, table_id_filter=table_ids)
        row_count = _write_parquet(conn, sql, parquet_path)

    logger.info("[%d/aggregate] Wrote %d rows to %s", year, row_count, parquet_path)
    return row_count


def construct_disclosure(year: int, conn: duckdb.DuckDBPyConnection, force: bool = False) -> Optional[int]:
    """Parse disclosure .dat file(s) and write to Parquet."""
    staging_dir = get_cra_staging_path("disclosure", year)
    parquet_path = staging_dir / "data.parquet"

    if parquet_path.exists() and not force:
        logger.info("[%d/disclosure] Parquet exists — skipping", year)
        return None

    raw_dir = get_cra_raw_path(year)
    era = get_era(year)
    layout = DISCLOSURE_LAYOUTS[era]

    if is_split_file_year(year):
        dat_files = _find_dat_files(year, "discl", DISCLOSURE_TABLES, raw_dir)
        if not dat_files:
            logger.warning("[%d/disclosure] No .dat files found", year)
            return None

        sqls = []
        for dat in dat_files:
            sqls.append(_build_fwf_sql(dat, layout, DISCLOSURE_NUMERIC_COLS))
        union_sql = "\n    UNION ALL\n    ".join(sqls)
        row_count = _write_parquet(conn, union_sql, parquet_path)
    else:
        candidates = list(raw_dir.glob("*.dat")) + list(raw_dir.glob("*.DAT"))
        discl_files = [p for p in candidates if "discl" in p.name.lower() or "exp_discl" in p.name.lower()]
        if not discl_files:
            discl_files = candidates
        if not discl_files:
            logger.warning("[%d/disclosure] No .dat file found in %s", year, raw_dir)
            return None

        dat_path = max(discl_files, key=lambda p: p.stat().st_size)
        sql = _build_fwf_sql(
            dat_path,
            layout,
            DISCLOSURE_NUMERIC_COLS,
            table_id_prefixes=list(DISCLOSURE_TABLE_ID_PREFIXES),
        )
        row_count = _write_parquet(conn, sql, parquet_path)

    logger.info("[%d/disclosure] Wrote %d rows to %s", year, row_count, parquet_path)
    return row_count


# ── DuckDB views ──────────────────────────────────────────────────────────────

def _find_staging_parquets(table_type: str) -> list[Path]:
    """Find all year-partitioned Parquet files for a table type."""
    from config import get_cra_storage_path
    staging_root = get_cra_storage_path("staging") / table_type
    if not staging_root.exists():
        return []
    return sorted(staging_root.glob("year=*/data.parquet"))


def recreate_views(conn: duckdb.DuckDBPyConnection) -> None:
    """Create or replace panel views over all staged Parquet files."""
    for table_type in ("aggregate", "disclosure", "transmittal"):
        parquets = _find_staging_parquets(table_type)
        if not parquets:
            logger.info("No parquets for %s — skipping view", table_type)
            continue

        paths_sql = ", ".join(
            f"'{_sql_path(p)}'" for p in parquets
        )
        view_name = f"{table_type}_panel"

        view_sql = f"""
        CREATE OR REPLACE VIEW {view_name} AS
        SELECT *
        FROM read_parquet(
            [{paths_sql}],
            hive_partitioning = true,
            union_by_name = true
        )
        """
        conn.execute(view_sql)
        logger.info("%s view created over %d parquet files", view_name, len(parquets))


# ── Metadata upsert ───────────────────────────────────────────────────────────

def _upsert_metadata(
    conn: duckdb.DuckDBPyConnection,
    table_type: str,
    year: int,
    row_count: int,
    parquet_path: Path,
) -> None:
    ensure_table_exists(conn, "panel_metadata", PANEL_METADATA_DDL)
    row = {
        "table_type": table_type,
        "year": year,
        "row_count": row_count,
        "source_url": get_download_url(year, {"aggregate": "aggr", "disclosure": "discl", "transmittal": "trans"}[table_type]),
        "built_at": datetime.now(timezone.utc).isoformat(),
        "parquet_path": str(parquet_path),
    }
    upsert_row(conn, "panel_metadata", row, ["table_type", "year"])


# ── 2024 Validation ──────────────────────────────────────────────────────────

def validate_2024(conn: duckdb.DuckDBPyConnection) -> bool:
    """
    Validate 2024 aggregate data against known National Aggregate Table 1 totals.
    Returns True if all checks pass.
    """
    logger.info("=" * 60)
    logger.info("VALIDATING 2024 AGGREGATE DATA")
    logger.info("=" * 60)

    all_pass = True

    for label, expected in VALIDATION_2024.items():
        table_id = expected["table_id"]
        loan_type = expected["loan_type"]
        action_taken = expected["action_taken"]

        sql = f"""
        SELECT
            COALESCE(SUM(num_loans_lt_100k), 0) AS sum_num_lt_100k,
            COALESCE(SUM(num_loans_100k_250k), 0) AS sum_num_100k_250k,
            COALESCE(SUM(num_loans_250k_1m), 0) AS sum_num_250k_1m,
            COALESCE(SUM(num_loans_lt_100k), 0) + COALESCE(SUM(num_loans_100k_250k), 0) + COALESCE(SUM(num_loans_250k_1m), 0) AS total_num,
            COALESCE(SUM(amt_loans_lt_100k), 0) AS sum_amt_lt_100k,
            COALESCE(SUM(amt_loans_100k_250k), 0) AS sum_amt_100k_250k,
            COALESCE(SUM(amt_loans_250k_1m), 0) AS sum_amt_250k_1m,
            COALESCE(SUM(amt_loans_lt_100k), 0) + COALESCE(SUM(amt_loans_100k_250k), 0) + COALESCE(SUM(amt_loans_250k_1m), 0) AS total_amt,
            COALESCE(SUM(num_loans_rev_lt_1m), 0) AS sum_num_rev,
            COALESCE(SUM(amt_loans_rev_lt_1m), 0) AS sum_amt_rev
        FROM aggregate_panel
        WHERE year = 2024
          AND TRIM(table_id) = '{table_id}'
          AND CAST(loan_type AS INTEGER) = {loan_type}
          AND CAST(action_taken AS INTEGER) = {action_taken}
          AND TRIM(report_level) = '200'
        """

        result = conn.execute(sql).fetchone()
        if not result:
            logger.error("[%s] No data found!", label)
            all_pass = False
            continue

        actual = {
            "num_loans_lt_100k": result[0],
            "num_loans_100k_250k": result[1],
            "num_loans_250k_1m": result[2],
            "total_num": result[3],
            "amt_loans_lt_100k": result[4],
            "amt_loans_100k_250k": result[5],
            "amt_loans_250k_1m": result[6],
            "total_amt": result[7],
            "num_loans_rev_lt_1m": result[8],
            "amt_loans_rev_lt_1m": result[9],
        }

        logger.info("--- %s ---", label)
        for key in ["num_loans_lt_100k", "num_loans_100k_250k", "num_loans_250k_1m",
                     "total_num", "amt_loans_lt_100k", "amt_loans_100k_250k",
                     "amt_loans_250k_1m", "total_amt", "num_loans_rev_lt_1m", "amt_loans_rev_lt_1m"]:
            if key not in expected:
                continue
            exp_val = expected[key]
            act_val = actual.get(key, 0)
            match = "OK" if exp_val == act_val else "MISMATCH"
            if match != "OK":
                all_pass = False
            logger.info("  %-25s expected=%15s  actual=%15s  %s",
                        key, f"{exp_val:,}", f"{act_val:,}", match)

    if all_pass:
        logger.info("ALL VALIDATION CHECKS PASSED")
    else:
        logger.error("SOME VALIDATION CHECKS FAILED")

    return all_pass


# ── Main orchestration ────────────────────────────────────────────────────────

def construct_year(year: int, force: bool = False) -> None:
    """Process all three CRA file types for a single year."""
    logger.info("=" * 60)
    logger.info("Constructing CRA year %d", year)
    logger.info("=" * 60)

    conn = duckdb.connect(":memory:")
    conn.execute(f"PRAGMA threads={DUCKDB_THREADS}")
    conn.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY_LIMIT}'")

    try:
        # Transmittal
        rc = construct_transmittal(year, conn, force)
        if rc is not None:
            db_conn = get_connection(get_cra_duckdb_path(), threads=DUCKDB_THREADS, memory_limit=DUCKDB_MEMORY_LIMIT)
            try:
                _upsert_metadata(db_conn, "transmittal", year, rc, get_cra_staging_path("transmittal", year) / "data.parquet")
            finally:
                db_conn.close()

        # Aggregate
        rc = construct_aggregate(year, conn, force)
        if rc is not None:
            db_conn = get_connection(get_cra_duckdb_path(), threads=DUCKDB_THREADS, memory_limit=DUCKDB_MEMORY_LIMIT)
            try:
                _upsert_metadata(db_conn, "aggregate", year, rc, get_cra_staging_path("aggregate", year) / "data.parquet")
            finally:
                db_conn.close()

        # Disclosure
        rc = construct_disclosure(year, conn, force)
        if rc is not None:
            db_conn = get_connection(get_cra_duckdb_path(), threads=DUCKDB_THREADS, memory_limit=DUCKDB_MEMORY_LIMIT)
            try:
                _upsert_metadata(db_conn, "disclosure", year, rc, get_cra_staging_path("disclosure", year) / "data.parquet")
            finally:
                db_conn.close()

    finally:
        conn.close()

    # Recreate views
    db_conn = get_connection(get_cra_duckdb_path(), threads=DUCKDB_THREADS, memory_limit=DUCKDB_MEMORY_LIMIT)
    try:
        recreate_views(db_conn)
    finally:
        db_conn.close()

    logger.info("Year %d complete.", year)


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CRA ETL: fixed-width → Parquet → DuckDB.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--year", type=int, help="Process a single year")
    group.add_argument("--all", action="store_true", help="Process all years (2024 -> 1996)")
    group.add_argument("--validate", type=int, metavar="YEAR", help="Run validation only")
    parser.add_argument("--force", action="store_true", help="Reprocess even if Parquet exists")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.validate:
        db_conn = get_connection(get_cra_duckdb_path(), threads=DUCKDB_THREADS, memory_limit=DUCKDB_MEMORY_LIMIT)
        try:
            recreate_views(db_conn)
            validate_2024(db_conn)
        finally:
            db_conn.close()
        return

    if args.all:
        years = ALL_YEARS
    else:
        years = [args.year]

    for year in years:
        construct_year(year, force=args.force)

    # Run validation if 2024 was processed
    if 2024 in years:
        db_conn = get_connection(get_cra_duckdb_path(), threads=DUCKDB_THREADS, memory_limit=DUCKDB_MEMORY_LIMIT)
        try:
            recreate_views(db_conn)
            validate_2024(db_conn)
        finally:
            db_conn.close()


if __name__ == "__main__":
    main()
