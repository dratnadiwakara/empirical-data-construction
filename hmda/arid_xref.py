"""
ARID2017 -> LEI crosswalk downloader and DuckDB ingestion.

The FFIEC published a pipe-delimited crosswalk file that maps pre-2018 HMDA
respondent identifiers (ARID) to their post-2018 LEIs. This enables linking
pre-2018 and post-2018 HMDA records for the same institution.

Source:
  https://files.ffiec.cfpb.gov/static-data/snapshot/2017/arid2017tolei/arid2017_to_lei_xref_psv.zip

Usage
-----
    python -m hmda.arid_xref           # download + ingest (idempotent)
    python -m hmda.arid_xref --force   # re-download even if file exists
"""
from __future__ import annotations

import argparse
import sys
import zipfile
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import (
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_THREADS,
    HTTP_TIMEOUT,
    USER_AGENT,
    get_duckdb_path,
    get_storage_path,
)
from utils.duckdb_utils import ensure_table_exists, get_connection
from utils.logging_utils import get_logger, log_step

logger = get_logger(__name__)

ARID_XREF_URL = (
    "https://files.ffiec.cfpb.gov/static-data/snapshot/2017"
    "/arid2017tolei/arid2017_to_lei_xref_psv.zip"
)

ARID_XREF_DDL = """
CREATE TABLE IF NOT EXISTS arid2017_to_lei (
    arid             VARCHAR,
    lei              VARCHAR,
    respondent_name  VARCHAR
)
"""


def get_arid_path() -> Path:
    p = get_storage_path("arid_xref")
    p.mkdir(parents=True, exist_ok=True)
    return p


def fetch_arid_xref(dest_dir: Path, force: bool = False) -> Path:
    """
    Download and extract the ARID2017->LEI crosswalk ZIP.
    Returns the path to the extracted pipe-delimited file.
    """
    import httpx

    zip_dest = dest_dir / "arid2017_to_lei_xref_psv.zip"
    # Look for already-extracted file
    existing = list(dest_dir.glob("*.txt")) + list(dest_dir.glob("*.csv")) + list(dest_dir.glob("*.psv"))
    if existing and not force:
        logger.info("ARID xref file already exists - skipping download")
        return max(existing, key=lambda p: p.stat().st_size)

    logger.info("Downloading ARID2017->LEI crosswalk: %s", ARID_XREF_URL)
    client = httpx.Client(
        headers={"User-Agent": USER_AGENT},
        follow_redirects=True,
        timeout=HTTP_TIMEOUT,
    )
    try:
        resp = client.get(ARID_XREF_URL, timeout=HTTP_TIMEOUT)
        if resp.status_code != 200:
            raise RuntimeError(f"GET {ARID_XREF_URL} returned HTTP {resp.status_code}")
        zip_dest.write_bytes(resp.content)
        log_step(logger, "arid_xref_downloaded", bytes=len(resp.content))
    finally:
        client.close()

    # Extract
    with zipfile.ZipFile(zip_dest, "r") as zf:
        names = [n for n in zf.namelist() if not n.endswith("/")]
        if not names:
            raise RuntimeError("ARID xref ZIP is empty")
        target = max(names, key=lambda n: zf.getinfo(n).file_size)
        logger.info("Extracting '%s' from ZIP", target)
        extracted = zf.extract(target, path=dest_dir)

    return Path(extracted)


def parse_arid_xref(path: Path) -> pl.DataFrame:
    """
    Parse the ARID->LEI crosswalk file.
    The file is pipe-delimited with a header row.
    """
    logger.info("Parsing ARID xref: %s", path.name)

    # Try pipe-delimited first, fall back to comma
    for sep in ("|", ",", "\t"):
        try:
            df = pl.read_csv(
                path,
                separator=sep,
                infer_schema_length=0,
                encoding="utf8-lossy",
                truncate_ragged_lines=True,
                ignore_errors=True,
            )
            if len(df.columns) >= 2:
                break
        except Exception:
            continue

    logger.info("Raw columns: %s", df.columns)

    # Normalise column names (lowercase, strip whitespace)
    df = df.rename({c: c.strip().lower().replace(" ", "_") for c in df.columns})
    cols = df.columns

    # Identify the ARID column (pre-2018 respondent ID)
    arid_col = next((c for c in cols if "arid" in c), None)
    if arid_col:
        df = df.rename({arid_col: "arid"})

    # Identify name column
    name_col = next((c for c in df.columns if "name" in c), None)
    if name_col and name_col != "respondent_name":
        df = df.rename({name_col: "respondent_name"})

    # LEI columns: could be "lei", "lei_2018", "lei_2019", etc.
    # Use the most recent non-null LEI available per row (coalesce descending year order)
    lei_cols = sorted(
        [c for c in df.columns if c.startswith("lei")],
        reverse=True,   # most recent year first
    )
    if not lei_cols:
        df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias("lei"))
    elif len(lei_cols) == 1:
        df = df.rename({lei_cols[0]: "lei"})
    else:
        # coalesce: pick first non-null/non-empty across years (most recent first)
        df = df.with_columns(
            pl.coalesce([
                pl.when(pl.col(c).str.strip_chars().str.len_chars().gt(0))
                .then(pl.col(c).str.strip_chars())
                .otherwise(pl.lit(None))
                for c in lei_cols
            ]).alias("lei")
        )

    # Ensure all expected columns exist
    for col in ("arid", "lei", "respondent_name"):
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))

    df = df.select(["arid", "lei", "respondent_name"])

    # Clean up
    df = df.with_columns([
        pl.col("arid").cast(pl.Utf8, strict=False).str.strip_chars(),
        pl.col("lei").cast(pl.Utf8, strict=False).str.strip_chars(),
        pl.col("respondent_name").cast(pl.Utf8, strict=False).str.strip_chars(),
    ])
    df = df.filter(
        pl.col("arid").is_not_null() & pl.col("arid").str.len_chars().gt(0)
    )

    logger.info("Parsed %d ARID->LEI rows", len(df))
    return df


def ingest_arid_to_duckdb(df: pl.DataFrame, db_path: Path) -> int:
    """Load the ARID->LEI crosswalk into DuckDB. Replaces any existing data."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = get_connection(db_path, threads=DUCKDB_THREADS, memory_limit=DUCKDB_MEMORY_LIMIT)
    try:
        ensure_table_exists(conn, "arid2017_to_lei", ARID_XREF_DDL)
        conn.execute("DELETE FROM arid2017_to_lei")
        conn.register("arid_df", df)
        conn.execute("""
            INSERT INTO arid2017_to_lei (arid, lei, respondent_name)
            SELECT arid, lei, respondent_name FROM arid_df
        """)
        n = conn.execute("SELECT COUNT(*) FROM arid2017_to_lei").fetchone()[0]
        log_step(logger, "arid_xref_ingested", rows=n)
        return n
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Download and ingest ARID2017->LEI crosswalk.")
    p.add_argument("--force", action="store_true", help="Re-download even if file exists")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    dest_dir = get_arid_path()
    db_path  = get_duckdb_path()

    psv_path = fetch_arid_xref(dest_dir, force=args.force)
    df       = parse_arid_xref(psv_path)
    n        = ingest_arid_to_duckdb(df, db_path)
    logger.info("ARID->LEI crosswalk loaded: %d rows -> %s", n, db_path)


if __name__ == "__main__":
    main()
