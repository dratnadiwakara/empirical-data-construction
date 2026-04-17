"""
construct.py — CFLV Call Reports ETL

Converts Stata .dta source files (balance sheets, income statements) to:
  1. Year-partitioned snappy Parquet in staging/
  2. DuckDB tables (balance_sheets, income_statements) + panel_metadata table

Source date format: Stata quarterly integer (tq) — quarters since Q1 1960.
  Stored in source as first-day-of-quarter convention.
  Converted here to quarter-end DATE: Q1 → Mar 31, Q2 → Jun 30, Q3 → Sep 30, Q4 → Dec 31.

Usage:
    C:\\envs\\.basic_venv\\Scripts\\python.exe construct.py [--force]

    --force  Re-extract ZIPs and reprocess Parquet staging even if already present.
"""
import argparse
import calendar
import importlib.util
import json
import shutil
import sys
import zipfile
from datetime import date
from pathlib import Path

import duckdb
import polars as pl
import pyreadstat

# ── Path setup ────────────────────────────────────────────────────────────────
_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# Import local metadata without requiring a package name (folder has hyphens)
_meta_spec = importlib.util.spec_from_file_location("cflv_metadata", _HERE / "metadata.py")
_meta_mod = importlib.util.module_from_spec(_meta_spec)
_meta_spec.loader.exec_module(_meta_mod)  # type: ignore[attr-defined]

BALANCE_SHEET_VARS: dict = _meta_mod.BALANCE_SHEET_VARS
INCOME_STATEMENT_VARS: dict = _meta_mod.INCOME_STATEMENT_VARS
METADATA_VARS: dict = _meta_mod.METADATA_VARS
SOURCE_FILES: dict = _meta_mod.SOURCE_FILES

from config import (
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_THREADS,
    PARQUET_COMPRESSION,
    PARQUET_ROW_GROUP_SIZE,
    get_cflv_duckdb_path,
    get_cflv_raw_path,
    get_cflv_staging_path,
)
from utils.logging_utils import get_logger

logger = get_logger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
DOWNLOADS_DIR = Path(r"C:\Users\dimut\Downloads\call-reports-CFLV")
CHUNK_SIZE = 100_000


# ── Date helpers ──────────────────────────────────────────────────────────────

def _tq_to_quarter_end(tq: int) -> date:
    """Convert Stata tq integer to quarter-end date.

    Stata tq encodes quarters since Q1-1960 (0 = Q1 1960, 1 = Q2 1960, -1 = Q4 1959).
    Returns the last calendar day of that quarter.
    """
    year = 1960 + tq // 4
    quarter = tq % 4 + 1   # 1..4
    month = quarter * 3     # 3, 6, 9, or 12
    day = calendar.monthrange(year, month)[1]
    return date(year, month, day)


# ── Step 1: Extract ZIPs ──────────────────────────────────────────────────────

def extract_zip(table: str, force: bool = False) -> Path:
    info = SOURCE_FILES[table]
    raw_dir = get_cflv_raw_path()
    dta_path = raw_dir / info["dta"]
    if dta_path.exists() and not force:
        logger.info("DTA already present, skipping extract: %s", dta_path.name)
        return dta_path
    zip_path = DOWNLOADS_DIR / info["zip"]
    if not zip_path.exists():
        raise FileNotFoundError(
            f"Source ZIP not found: {zip_path}\n"
            f"Download from: {_meta_mod.SOURCE_URL}"
        )
    logger.info("Extracting %s -> %s", zip_path.name, raw_dir)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(raw_dir)
    logger.info("Extracted: %s", dta_path.name)
    return dta_path


# ── Step 2: DTA → Staging Parquet ────────────────────────────────────────────

def _staging_has_data(table: str) -> bool:
    return any(get_cflv_staging_path(table).rglob("*.parquet"))


def dta_to_parquet(table: str, dta_path: Path, force: bool = False) -> None:
    staging_dir = get_cflv_staging_path(table)
    if _staging_has_data(table) and not force:
        logger.info("Staging Parquet already exists for %s — skipping", table)
        return
    if force and staging_dir.exists():
        logger.info("Removing existing staging for %s", table)
        shutil.rmtree(staging_dir)
    staging_dir.mkdir(parents=True, exist_ok=True)

    logger.info(
        "Converting %s DTA -> year-partitioned Parquet (may take several minutes)...", table
    )
    part_counters: dict[int, int] = {}

    for chunk_pd, _meta in pyreadstat.read_file_in_chunks(
        pyreadstat.read_dta, str(dta_path), chunksize=CHUNK_SIZE
    ):
            # Bridge pandas → polars (pandas used only here as pyreadstat output)
            df = pl.from_pandas(chunk_pd)

            # Convert Stata tq int → quarter-end Date
            df = df.with_columns(
                pl.col("date")
                .map_elements(_tq_to_quarter_end, return_dtype=pl.Date)
                .alias("date")
            )

            # Extract year for Hive partitioning
            df = df.with_columns(pl.col("date").dt.year().alias("year"))

            # Write per-year shards
            for (year_val,), group in df.group_by(["year"], maintain_order=False):
                year_dir = staging_dir / f"year={year_val}"
                year_dir.mkdir(parents=True, exist_ok=True)
                part_idx = part_counters.get(year_val, 0)
                out_path = year_dir / f"part-{part_idx}.parquet"
                group.drop("year").write_parquet(
                    out_path,
                    compression=PARQUET_COMPRESSION,
                    row_group_size=PARQUET_ROW_GROUP_SIZE,
                )
                part_counters[year_val] = part_idx + 1

    total_parts = sum(part_counters.values())
    logger.info(
        "%s: wrote %d Parquet shards across %d years",
        table, total_parts, len(part_counters),
    )


# ── Step 3: Parquet → DuckDB ─────────────────────────────────────────────────

def load_to_duckdb(conn: duckdb.DuckDBPyConnection, table: str) -> None:
    staging_dir = get_cflv_staging_path(table)
    # Use forward slashes — DuckDB requires them even on Windows
    glob_pattern = str(staging_dir / "**" / "*.parquet").replace("\\", "/")
    logger.info("Loading %s from Parquet -> DuckDB...", table)
    conn.execute(f"""
        CREATE OR REPLACE TABLE {table} AS
        SELECT * EXCLUDE (year)
        FROM read_parquet('{glob_pattern}', hive_partitioning = true)
    """)
    n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    logger.info("Loaded %s: %d rows", table, n)


# ── Step 4: panel_metadata ────────────────────────────────────────────────────

def create_panel_metadata(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE OR REPLACE TABLE panel_metadata (
            variable_name VARCHAR NOT NULL,
            source_table  VARCHAR NOT NULL,
            description   VARCHAR,
            mdrm_codes    VARCHAR,
            unit          VARCHAR,
            notes         VARCHAR
        )
    """)
    rows = []
    for varname, info in BALANCE_SHEET_VARS.items():
        rows.append((
            varname, "balance_sheets",
            info["description"],
            json.dumps(info["mdrm"]),
            info["unit"],
            info.get("notes") or "",
        ))
    for varname, info in INCOME_STATEMENT_VARS.items():
        rows.append((
            varname, "income_statements",
            info["description"],
            json.dumps(info["mdrm"]),
            info["unit"],
            info.get("notes") or "",
        ))
    for varname, info in METADATA_VARS.items():
        rows.append((
            varname, "both",
            info["description"],
            "",
            info["unit"],
            "",
        ))
    conn.executemany("INSERT INTO panel_metadata VALUES (?, ?, ?, ?, ?, ?)", rows)
    logger.info("panel_metadata: %d entries", len(rows))


# ── Step 5: Validation ────────────────────────────────────────────────────────

def validate(conn: duckdb.DuckDBPyConnection) -> None:
    checks = {
        "balance_sheets": ["assets", "deposits", "equity", "ln_tot"],
        "income_statements": ["ytdnetinc", "ytdint_inc", "ytdint_exp", "ytdnonint_inc"],
    }
    all_ok = True
    for table, key_cols in checks.items():
        row = conn.execute(f"""
            SELECT COUNT(*) AS n,
                   MIN(date)              AS min_date,
                   MAX(date)              AS max_date,
                   COUNT(DISTINCT id_rssd) AS n_banks
            FROM {table}
        """).fetchone()
        logger.info(
            "VALIDATE %s: %d rows | %s to %s | %d banks",
            table, row[0], row[1], row[2], row[3],
        )
        if row[0] < 1_000_000:
            logger.warning("  Row count suspiciously low for %s: %d", table, row[0])
            all_ok = False
        for col in key_cols:
            null_rate = conn.execute(
                f"SELECT AVG(CASE WHEN {col} IS NULL THEN 1.0 ELSE 0.0 END) FROM {table}"
            ).fetchone()[0]
            flag = " ⚠" if null_rate > 0.5 else ""
            logger.info("  null(%s): %.1f%%%s", col, 100 * null_rate, flag)

    meta_count = conn.execute("SELECT COUNT(*) FROM panel_metadata").fetchone()[0]
    logger.info("panel_metadata: %d entries", meta_count)
    if all_ok:
        logger.info("Validation passed.")
    else:
        logger.warning("Validation warnings detected — review logs above.")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="CFLV Call Reports ETL: Stata DTA -> Parquet staging -> DuckDB"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-extract ZIPs and reprocess staging Parquet even if already present",
    )
    args = parser.parse_args()

    # Step 1 + 2: extract and convert each table
    for table in ("balance_sheets", "income_statements"):
        dta_path = extract_zip(table, force=args.force)
        dta_to_parquet(table, dta_path, force=args.force)

    # Steps 3–5: load to DuckDB, build metadata, validate
    db_path = get_cflv_duckdb_path()
    logger.info("Opening DuckDB at %s", db_path)
    conn = duckdb.connect(str(db_path))
    conn.execute(f"PRAGMA threads={DUCKDB_THREADS}")
    conn.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY_LIMIT}'")

    try:
        for table in ("balance_sheets", "income_statements"):
            load_to_duckdb(conn, table)
        create_panel_metadata(conn)
        validate(conn)
    finally:
        conn.close()

    logger.info("Done. Database: %s", db_path)


if __name__ == "__main__":
    main()
