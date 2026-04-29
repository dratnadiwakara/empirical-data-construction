"""
construct.py — Y-9C ETL.

Pipeline per quarter:
  1. Find raw/BHCF{YYYYMMDD}.zip (case-insensitive), extract single TXT into
     raw/{YYYY}Q{Q}/.
  2. Sniff first line for delimiter (caret '^' for all observed years; comma
     fallback for sanity).
  3. Read with DuckDB read_csv, all columns VARCHAR (matches FFIEC convention).
     Append activity_year + activity_quarter + date columns.
  4. Write to staging/year={YYYY}/quarter={Q}/data.parquet (atomic rename).
  5. Upsert panel_metadata row.
  6. After all requested quarters, refresh `y9c_raw` view + harmonized layer
     (`bs_panel_y9c`, `is_panel_y9c`, `harmonized_metadata_y9c`).

Usage:
    python y9c/construct.py --quarter 2024Q4
    python y9c/construct.py --year 2024
    python y9c/construct.py --all
    python y9c/construct.py --all --skip-views
    python y9c/construct.py --refresh-views
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb

# ── Path setup ───────────────────────────────────────────────────────────────

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

_meta_spec = importlib.util.spec_from_file_location(
    "y9c_metadata", _HERE / "metadata.py"
)
_meta = importlib.util.module_from_spec(_meta_spec)
_meta_spec.loader.exec_module(_meta)  # type: ignore[attr-defined]

from config import (
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_THREADS,
    PARQUET_COMPRESSION,
    PARQUET_ROW_GROUP_SIZE,
    get_y9c_duckdb_path,
    get_y9c_manifest_path,
    get_y9c_raw_path,
    get_y9c_staging_path,
    get_y9c_storage_path,
)
from utils.duckdb_utils import (
    ensure_table_exists,
    get_connection,
    transactional_connection,
    upsert_row,
)
from utils.logging_utils import get_logger

logger = get_logger(__name__)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _sql_path(p: Path | str) -> str:
    s = str(p).replace("\\", "/").replace("'", "''")
    return s


def _read_manifest() -> dict:
    p = get_y9c_manifest_path()
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def _write_manifest(manifest: dict) -> None:
    p = get_y9c_manifest_path()
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, p)


def _find_raw_zip(year: int, quarter: int) -> Path:
    """Find BHCF{YYYYMMDD}.zip case-insensitively."""
    raw_root = get_y9c_storage_path("raw")
    mmdd = _meta.quarter_to_mmdd(quarter)
    target = f"BHCF{year}{mmdd}.zip"
    for p in raw_root.iterdir():
        if p.is_file() and p.name.lower() == target.lower():
            return p
    raise FileNotFoundError(
        f"Missing raw ZIP for {year}Q{quarter}: expected {target} in {raw_root}"
    )


def _sniff_delimiter(path: Path) -> str:
    """Return '^' if first line contains caret, else ','."""
    with path.open("rb") as fh:
        first = fh.readline().decode("utf-8", errors="replace")
    if first.count("^") > first.count(","):
        return "^"
    return ","


# ── Extraction ───────────────────────────────────────────────────────────────

def _extract_zip_for_quarter(year: int, quarter: int, force: bool = False) -> Path:
    """Extract the raw ZIP for (year, quarter) into raw/{YYYY}Q{Q}/.
    Returns the path to the extracted TXT file."""
    zip_path = _find_raw_zip(year, quarter)
    extract_dir = get_y9c_raw_path(year, quarter)

    existing = list(extract_dir.glob("*.txt"))
    if existing and not force:
        logger.info("extraction cached for %dQ%d (%s)", year, quarter, existing[0].name)
        return existing[0]

    if force and existing:
        for f in existing:
            f.unlink()

    logger.info("extracting %s -> %s", zip_path.name, extract_dir)
    with zipfile.ZipFile(zip_path) as z:
        txt_members = [n for n in z.namelist() if n.lower().endswith(".txt")]
        if not txt_members:
            raise RuntimeError(f"no .txt inside {zip_path}: {z.namelist()}")
        chosen = max(txt_members, key=lambda n: z.getinfo(n).file_size)
        z.extract(chosen, extract_dir)

    out = next(extract_dir.glob("*.txt"))

    # Update manifest extract_status
    manifest = _read_manifest()
    zips = manifest.setdefault("zips", {})
    key = f"{year}Q{quarter}"
    zips.setdefault(key, {})
    zips[key].update({
        "filename": zip_path.name,
        "year": year,
        "quarter": quarter,
        "path": str(zip_path),
        "extract_status": "extracted",
        "extracted_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    })
    _write_manifest(manifest)
    return out


# ── Parquet write ────────────────────────────────────────────────────────────

def _write_quarter_parquet(
    conn: duckdb.DuckDBPyConnection,
    year: int,
    quarter: int,
    txt_path: Path,
    delim: str,
) -> tuple[Path, int, int]:
    """Materialize one quarter to staging parquet.

    Returns (parquet_path, row_count, n_columns).
    """
    out_dir = get_y9c_staging_path(year, quarter)
    out_path = out_dir / "data.parquet"
    tmp_out = out_dir / "data.parquet.tmp"

    qend = _meta.quarter_end_date(year, quarter)

    # all_varchar=true matches FFIEC convention; arithmetic via TRY_CAST.
    conn.execute(f"""
        COPY (
            SELECT *,
                   CAST({year} AS INTEGER) AS activity_year,
                   CAST({quarter} AS INTEGER) AS activity_quarter,
                   DATE '{qend}' AS date
            FROM read_csv(
                '{_sql_path(txt_path)}',
                delim = '{delim}',
                header = true,
                quote = '',
                escape = '',
                all_varchar = true,
                ignore_errors = true,
                null_padding = true,
                parallel = false
            )
        ) TO '{_sql_path(tmp_out)}'
        (FORMAT PARQUET,
         COMPRESSION '{PARQUET_COMPRESSION}',
         ROW_GROUP_SIZE {PARQUET_ROW_GROUP_SIZE})
    """)
    os.replace(tmp_out, out_path)

    n_rows = conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('{_sql_path(out_path)}')"
    ).fetchone()[0]
    n_cols = len(conn.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{_sql_path(out_path)}')"
    ).fetchall())
    return out_path, int(n_rows), int(n_cols)


# ── Per-quarter orchestrator ─────────────────────────────────────────────────

def process_quarter(
    conn: duckdb.DuckDBPyConnection,
    year: int,
    quarter: int,
    force: bool = False,
) -> tuple[int, int]:
    """Extract + parse + write parquet for one quarter. Upserts panel_metadata.
    Returns (n_rows, n_cols)."""
    t0 = time.time()
    txt_path = _extract_zip_for_quarter(year, quarter, force=force)
    delim = _sniff_delimiter(txt_path)

    out_dir = get_y9c_staging_path(year, quarter)
    out_path = out_dir / "data.parquet"

    if out_path.exists() and not force:
        n_rows = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{_sql_path(out_path)}')"
        ).fetchone()[0]
        n_cols = len(conn.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{_sql_path(out_path)}')"
        ).fetchall())
        logger.info("cached %dQ%d rows=%d cols=%d", year, quarter, n_rows, n_cols)
    else:
        _, n_rows, n_cols = _write_quarter_parquet(conn, year, quarter, txt_path, delim)
        logger.info("wrote %dQ%d rows=%d cols=%d delim=%r", year, quarter, n_rows, n_cols, delim)

    manifest = _read_manifest()
    zip_meta = manifest.get("zips", {}).get(f"{year}Q{quarter}", {})

    upsert_row(
        conn,
        "panel_metadata",
        {
            "year": year,
            "quarter": quarter,
            "row_count": int(n_rows),
            "n_columns": int(n_cols),
            "source_zip": zip_meta.get("filename"),
            "source_zip_sha256": zip_meta.get("sha256"),
            "parquet_path": str(out_path),
            "delimiter": delim,
            "built_at": datetime.now(timezone.utc),
        },
        key_columns=["year", "quarter"],
    )
    logger.info("quarter %dQ%d done in %.1fs", year, quarter, time.time() - t0)
    return int(n_rows), int(n_cols)


# ── View (re)creation ────────────────────────────────────────────────────────

def refresh_views(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the raw `y9c_raw` view over Hive-partitioned parquet, then build
    the harmonized layer (`bs_panel_y9c`, `is_panel_y9c`, harmonized_metadata_y9c).
    """
    staging_root = get_y9c_storage_path("staging")
    glob_path = f"{staging_root}/year=*/quarter=*/data.parquet"

    has_data = conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('{_sql_path(glob_path)}', "
        f"hive_partitioning=true, union_by_name=true) LIMIT 1"
    ).fetchone()
    if not has_data:
        logger.warning("no staging parquet found — skipping view refresh")
        return

    conn.execute(f"""
        CREATE OR REPLACE VIEW y9c_raw AS
        SELECT *
        FROM read_parquet(
            '{_sql_path(glob_path)}',
            hive_partitioning = true,
            union_by_name = true
        )
    """)
    logger.info("refreshed y9c_raw view")

    # Harmonized layer
    try:
        _hv_spec = importlib.util.spec_from_file_location(
            "y9c_harmonized_views", _HERE / "harmonized" / "views.py"
        )
        _hv = importlib.util.module_from_spec(_hv_spec)
        _hv_spec.loader.exec_module(_hv)  # type: ignore[attr-defined]
        built = _hv.build_views(conn)
        logger.info("built harmonized views: %s", ", ".join(built))
    except Exception as exc:
        logger.warning("harmonized view build failed: %s", exc)


# ── CLI plumbing ─────────────────────────────────────────────────────────────

def _enumerate_available_quarters() -> list[tuple[int, int]]:
    raw_root = get_y9c_storage_path("raw")
    qs: list[tuple[int, int]] = []
    for p in raw_root.iterdir():
        if not p.is_file() or p.suffix.lower() != ".zip":
            continue
        parsed = _meta.parse_zip_filename(p.name)
        if parsed:
            qs.append(parsed)
    return sorted(set(qs))


_QUARTER_RE = re.compile(r"^(\d{4})Q([1-4])$", re.IGNORECASE)


def _parse_quarter_arg(s: str) -> tuple[int, int]:
    m = _QUARTER_RE.match(s.strip())
    if not m:
        raise argparse.ArgumentTypeError(f"bad --quarter: {s} (expected YYYYQN)")
    return int(m.group(1)), int(m.group(2))


def main() -> int:
    ap = argparse.ArgumentParser(description="Y-9C ETL")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--quarter", type=_parse_quarter_arg,
                   help="single quarter, e.g. 2024Q4")
    g.add_argument("--year", type=int, help="all quarters for a single year")
    g.add_argument("--all", action="store_true",
                   help="every quarter present in raw/")
    g.add_argument("--refresh-views", action="store_true",
                   help="only rebuild views — no parquet work")
    ap.add_argument("--force", action="store_true",
                    help="rebuild staging parquet even if present")
    ap.add_argument("--skip-views", action="store_true",
                    help="skip view refresh (useful during bulk loads)")
    ap.add_argument("--inspect", action="store_true",
                    help="run inspect.py validation after build")

    args = ap.parse_args()

    db_path = get_y9c_duckdb_path()

    if args.refresh_views:
        with transactional_connection(
            db_path,
            threads=DUCKDB_THREADS,
            memory_limit=DUCKDB_MEMORY_LIMIT,
        ) as conn:
            ensure_table_exists(conn, "panel_metadata", _meta.PANEL_METADATA_DDL)
            refresh_views(conn)
        return 0

    available = _enumerate_available_quarters()
    if args.quarter:
        targets = [args.quarter]
    elif args.year:
        targets = [(args.year, q) for q in (1, 2, 3, 4)
                   if (args.year, q) in available]
        if not targets:
            raise SystemExit(f"no raw ZIPs for year {args.year}")
    elif args.all:
        targets = available
        if not targets:
            raise SystemExit("no raw ZIPs present — run download.py --status for help")
    else:
        ap.print_help()
        return 1

    logger.info("targets: %d quarter(s) — %s..%s",
                len(targets),
                f"{targets[0][0]}Q{targets[0][1]}",
                f"{targets[-1][0]}Q{targets[-1][1]}")

    conn = get_connection(
        db_path,
        threads=DUCKDB_THREADS,
        memory_limit=DUCKDB_MEMORY_LIMIT,
    )
    ensure_table_exists(conn, "panel_metadata", _meta.PANEL_METADATA_DDL)

    try:
        for year, quarter in targets:
            try:
                process_quarter(conn, year, quarter, force=args.force)
            except Exception as exc:
                logger.exception("failed %dQ%d: %s", year, quarter, exc)
                raise
        if not args.skip_views:
            refresh_views(conn)
    finally:
        conn.close()

    if args.inspect:
        _v_spec = importlib.util.spec_from_file_location(
            "y9c_validate", _HERE / "validate.py"
        )
        _v = importlib.util.module_from_spec(_v_spec)
        _v_spec.loader.exec_module(_v)  # type: ignore[attr-defined]
        _v.run_all_checks()

    return 0


if __name__ == "__main__":
    sys.exit(main())
