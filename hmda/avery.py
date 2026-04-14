"""
HMDA Avery/Philadelphia Fed lender crosswalk downloader and DuckDB ingestion.

The Philadelphia Fed HMDA Lender File links pre-2018 respondent_id / agency_code
pairs to RSSD IDs, and post-2018 LEIs to RSSD IDs, enabling time-consistent
lender identification across the pre/post-2018 regime boundary.

Sources (two XLSX files from Philly Fed):
  - hmda-1990-2017.xlsx  → pre-2018 reporter file
  - hmda-2018-present.xlsx → post-2018 reporter file (has LEI column)

Column mapping (Philly Fed internal names → our schema):
  YEAR    → activity_year
  HMPRID  → respondent_id
  CODE    → agency_code
  LEI     → lei              (2018+ file only)
  RSSD    → rssd_id
  RSSDP   → parent_rssd
  RSSDHH  → top_holder_rssd
  NAMET   → respondent_name
  ASSETS  → assets

Usage
-----
    python -m hmda.avery                  # download + ingest (idempotent)
    python -m hmda.avery --force          # re-download even if files exist
    python -m hmda.avery --replace        # drop and recreate avery_crosswalk table
"""
from __future__ import annotations

import argparse
import io
import sys
from pathlib import Path
from typing import Optional

import httpx
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import (
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_THREADS,
    HTTP_TIMEOUT,
    USER_AGENT,
    get_avery_path,
    get_duckdb_path,
)
from hmda.metadata import AVERY_TABLE_DDL
from utils.duckdb_utils import ensure_table_exists, get_connection
from utils.logging_utils import get_logger, log_step

logger = get_logger(__name__)

# ── Source URLs ────────────────────────────────────────────────────────────────

PHILLY_FED_BASE = "https://www.philadelphiafed.org/-/media/FRBP/Assets/Surveys-And-Data/hmda"
XLSX_PRE2018  = f"{PHILLY_FED_BASE}/hmda-1990-2017.xlsx"
XLSX_POST2018 = f"{PHILLY_FED_BASE}/hmda-2018-present.xlsx"

# Philly Fed internal column name → our canonical name
_COL_MAP: dict[str, str] = {
    "YEAR":   "activity_year",
    "HMPRID": "respondent_id",
    "CODE":   "agency_code",
    "LEI":    "lei",
    "RSSD":   "rssd_id",
    "RSSDP":  "parent_rssd",
    "RSSDHH": "top_holder_rssd",
    "NAMET":  "respondent_name",
    "ASSETS": "assets",
}

# ── HTTP helpers ───────────────────────────────────────────────────────────────

def _make_client() -> httpx.Client:
    return httpx.Client(
        headers={"User-Agent": USER_AGENT},
        follow_redirects=True,
        timeout=HTTP_TIMEOUT,
    )


# ── Download ───────────────────────────────────────────────────────────────────

def fetch_avery_files(
    dest_dir: Path,
    client: Optional[httpx.Client] = None,
    force: bool = False,
) -> tuple[Path, Path]:
    """
    Download the two Philly Fed HMDA lender XLSX files.
    Returns (pre2018_path, post2018_path).
    Skips download if files already exist (unless force=True).
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    pre_dest  = dest_dir / "hmda_lender_1990_2017.xlsx"
    post_dest = dest_dir / "hmda_lender_2018_present.xlsx"

    own_client = client is None
    if own_client:
        client = _make_client()

    try:
        for url, dest in [(XLSX_PRE2018, pre_dest), (XLSX_POST2018, post_dest)]:
            if dest.exists() and not force:
                logger.info("Avery file exists — skipping: %s", dest.name)
                continue
            logger.info("Downloading %s -> %s", url, dest.name)
            resp = client.get(url, timeout=HTTP_TIMEOUT)
            if resp.status_code != 200:
                raise RuntimeError(
                    f"GET {url} returned HTTP {resp.status_code}"
                )
            dest.write_bytes(resp.content)
            log_step(logger, "avery_downloaded", url=url, bytes=len(resp.content))
    finally:
        if own_client:
            client.close()

    return pre_dest, post_dest


# ── Parsing ────────────────────────────────────────────────────────────────────

def _parse_xlsx(path: Path, has_lei: bool) -> pl.DataFrame:
    """
    Read one Philly Fed XLSX file and return a normalised DataFrame with
    columns: activity_year, respondent_id, agency_code, lei, rssd_id,
             parent_rssd, top_holder_rssd, respondent_name, assets.
    """
    logger.info("Parsing %s", path.name)
    raw = pl.read_excel(path, infer_schema_length=0)

    # Keep only the columns we care about (ignore missing ones gracefully)
    keep = {src: dst for src, dst in _COL_MAP.items() if src in raw.columns}
    df = raw.select(list(keep.keys())).rename(keep)

    # Ensure all expected columns exist
    expected_schema: dict[str, type] = {
        "activity_year":   pl.Int32,
        "respondent_id":   pl.Utf8,
        "agency_code":     pl.Int32,
        "lei":             pl.Utf8,
        "rssd_id":         pl.Int32,
        "parent_rssd":     pl.Int32,
        "top_holder_rssd": pl.Int32,
        "respondent_name": pl.Utf8,
        "assets":          pl.Int64,
    }
    for col, dtype in expected_schema.items():
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).cast(dtype).alias(col))

    # Cast numeric columns — the XLSX values may be strings or mixed types
    int32_cols = ["activity_year", "agency_code", "rssd_id", "parent_rssd", "top_holder_rssd"]
    for col in int32_cols:
        df = df.with_columns(
            pl.col(col)
            .cast(pl.Utf8, strict=False)
            .str.strip_chars()
            .str.replace_all(r"[,\s]", "")
            .cast(pl.Int32, strict=False)
            .alias(col)
        )

    df = df.with_columns(
        pl.col("assets")
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .str.replace_all(r"[,\s]", "")
        .cast(pl.Int64, strict=False)
        .alias("assets")
    )

    # respondent_id: zero-pad to 10 chars
    df = df.with_columns(
        pl.col("respondent_id")
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .str.zfill(10)
        .alias("respondent_id")
    )

    # lei: clean empty strings to null
    df = df.with_columns(
        pl.when(
            pl.col("lei").is_null()
            | pl.col("lei").cast(pl.Utf8, strict=False).str.strip_chars().str.len_chars().eq(0)
        )
        .then(pl.lit(None, dtype=pl.Utf8))
        .otherwise(pl.col("lei").cast(pl.Utf8, strict=False).str.strip_chars())
        .alias("lei")
    )

    # respondent_name: cast to string
    df = df.with_columns(
        pl.col("respondent_name").cast(pl.Utf8, strict=False).alias("respondent_name")
    )

    # Drop rows where both respondent_id and lei are empty/null
    df = df.filter(
        pl.col("respondent_id").str.strip_chars().str.len_chars().gt(0)
        | pl.col("lei").is_not_null()
    )

    return df.select(list(expected_schema.keys()))


def parse_avery_xlsx(pre_path: Path, post_path: Path) -> pl.DataFrame:
    """
    Parse both Philly Fed XLSX files and return a combined DataFrame.
    """
    pre  = _parse_xlsx(pre_path,  has_lei=False)
    post = _parse_xlsx(post_path, has_lei=True)
    combined = pl.concat([pre, post], how="diagonal")
    logger.info(
        "Parsed Avery crosswalk: %d rows (%d pre-2018, %d post-2018)",
        len(combined), len(pre), len(post),
    )
    return combined


# ── DuckDB ingestion ───────────────────────────────────────────────────────────

def ingest_avery_to_duckdb(
    df: pl.DataFrame,
    db_path: Path,
    replace: bool = False,
) -> int:
    """
    Insert the Avery DataFrame into the avery_crosswalk DuckDB table.
    If replace=True, drops and recreates the table first.
    Returns the number of rows inserted.

    Strategy: write Polars DataFrame to a temp Parquet (Polars native writer,
    no pyarrow needed) then let DuckDB read that file directly.
    """
    import tempfile
    import os

    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Write to a temp Parquet in the same dir as the DB so it's on the same drive
    tmp_parquet = db_path.parent / "_avery_tmp.parquet"
    try:
        df.write_parquet(tmp_parquet)
        tmp_fwd = str(tmp_parquet).replace("\\", "/")

        conn = get_connection(db_path, threads=DUCKDB_THREADS, memory_limit=DUCKDB_MEMORY_LIMIT)
        try:
            if replace:
                conn.execute("DROP TABLE IF EXISTS avery_crosswalk")
                logger.info("Dropped existing avery_crosswalk table")

            ensure_table_exists(conn, "avery_crosswalk", AVERY_TABLE_DDL)

            if not replace:
                conn.execute("DELETE FROM avery_crosswalk")

            conn.execute(f"""
                INSERT INTO avery_crosswalk
                    (respondent_id, agency_code, lei, rssd_id, parent_rssd,
                     top_holder_rssd, respondent_name, activity_year, assets)
                SELECT respondent_id, agency_code, lei, rssd_id, parent_rssd,
                       top_holder_rssd, respondent_name, activity_year, assets
                FROM read_parquet('{tmp_fwd}')
            """)
            n = conn.execute("SELECT COUNT(*) FROM avery_crosswalk").fetchone()[0]
            log_step(logger, "avery_ingested", rows=n, db=str(db_path))
            return n
        finally:
            conn.close()
    finally:
        if tmp_parquet.exists():
            tmp_parquet.unlink()


# ── CLI ────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Download and ingest the Philly Fed HMDA lender crosswalk."
    )
    p.add_argument("--force", action="store_true",
                   help="Re-download XLSX files even if they already exist")
    p.add_argument("--replace", action="store_true",
                   help="Drop and recreate avery_crosswalk table in DuckDB")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    avery_dir = get_avery_path()
    db_path   = get_duckdb_path()

    pre_path, post_path = fetch_avery_files(avery_dir, force=args.force)
    df = parse_avery_xlsx(pre_path, post_path)
    n  = ingest_avery_to_duckdb(df, db_path, replace=args.replace)
    logger.info("Avery crosswalk loaded: %d rows -> %s", n, db_path)


if __name__ == "__main__":
    main()
