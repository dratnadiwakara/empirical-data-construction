"""
construct.py — FFIEC Call Reports ETL.

Pipeline per quarter:
  1. Extract the `FFIEC CDR Call Bulk All Schedules MMDDYYYY.zip` into
     raw/{YYYY}Q{Q}/ (stdlib zipfile; CDR ZIPs use standard Deflate).
  2. Group inner TSVs by schedule suffix (e.g. RC, RCB, RCRII). Multi-part
     files (…(1 of 2).txt / (2 of 2).txt) are COLUMN-split splits of the same
     schedule and get FULL OUTER JOINed on IDRSSD.
  3. Parse each schedule group with DuckDB read_csv, drop the second header
     row (human-readable descriptions; filtered via TRY_CAST(IDRSSD AS BIGINT)),
     tag each row with activity_year + activity_quarter, and COPY to
     staging/{SCHEDULE}/year={YYYY}/quarter={Q}/data.parquet.
  4. Upsert a panel_metadata row.
  5. After all requested quarters are written, rebuild one DuckDB VIEW per
     schedule over the full Hive-partitioned parquet glob.
  6. If MDRM.csv is present, load it into mdrm_dictionary as a reference table.

Usage:
    python call-reports-FFIEC/construct.py --quarter 2024Q4
    python call-reports-FFIEC/construct.py --year 2024
    python call-reports-FFIEC/construct.py --all
    python call-reports-FFIEC/construct.py --all --skip-views     # batch load
    python call-reports-FFIEC/construct.py --refresh-views        # views only
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import shutil
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as papq

# ── Path setup ───────────────────────────────────────────────────────────────

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

_meta_spec = importlib.util.spec_from_file_location(
    "ffiec_metadata", _HERE / "metadata.py"
)
_meta = importlib.util.module_from_spec(_meta_spec)
_meta_spec.loader.exec_module(_meta)  # type: ignore[attr-defined]

from config import (
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_THREADS,
    PARQUET_COMPRESSION,
    PARQUET_ROW_GROUP_SIZE,
    get_ffiec_duckdb_path,
    get_ffiec_manifest_path,
    get_ffiec_mdrm_path,
    get_ffiec_raw_path,
    get_ffiec_staging_path,
    get_ffiec_storage_path,
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

def _sql_path(p: Path) -> str:
    """DuckDB SQL-safe path literal (forward slashes, escape single quotes)."""
    s = str(p).replace("\\", "/").replace("'", "''")
    return s


def _read_manifest() -> dict:
    p = get_ffiec_manifest_path()
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def _write_manifest(manifest: dict) -> None:
    p = get_ffiec_manifest_path()
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, p)


# ── ZIP → raw/{YYYY}Q{Q}/ extraction ─────────────────────────────────────────

def _extract_zip_for_quarter(year: int, quarter: int, force: bool = False) -> Path:
    """
    Find the raw ZIP for (year, quarter), extract to raw/{YYYY}Q{Q}/.
    Returns the extraction directory.
    """
    raw_root = get_ffiec_storage_path("raw")
    # Expected filename pattern: 'FFIEC CDR Call Bulk All Schedules MMDDYYYY.zip'
    mmdd = {1: "0331", 2: "0630", 3: "0930", 4: "1231"}[quarter]
    expected = raw_root / f"FFIEC CDR Call Bulk All Schedules {mmdd}{year}.zip"
    if not expected.exists():
        raise FileNotFoundError(f"Missing raw ZIP for {year}Q{quarter}: {expected}")

    extract_dir = get_ffiec_raw_path(year, quarter)
    # Heuristic: if extract_dir has at least one Schedule file already, skip
    existing = list(extract_dir.glob("FFIEC CDR Call *.txt"))
    if existing and not force:
        logger.info("extraction cached for %dQ%d (%d files)",
                    year, quarter, len(existing))
        return extract_dir

    if force and existing:
        for f in existing:
            f.unlink()

    logger.info("extracting %s -> %s", expected.name, extract_dir)
    with zipfile.ZipFile(expected) as z:
        for member in z.namelist():
            if member.lower().endswith(".txt") and "Readme" not in member:
                z.extract(member, extract_dir)

    # Update manifest
    manifest = _read_manifest()
    zips = manifest.setdefault("zips", {})
    key = f"{year}Q{quarter}"
    zips.setdefault(key, {})
    zips[key].update({
        "filename": expected.name,
        "year": year,
        "quarter": quarter,
        "path": str(expected),
        "extract_status": "extracted",
        "extracted_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    })
    _write_manifest(manifest)
    return extract_dir


# ── Group extracted TSVs by schedule ─────────────────────────────────────────

def _group_schedules(extract_dir: Path) -> dict[str, list[Path]]:
    """
    Return {schedule_id: [Path, Path, ...]} where the list is ordered by part
    number. Single-file schedules have list of length 1.
    """
    groups: dict[str, list[tuple[int, Path]]] = {}
    for p in sorted(extract_dir.glob("FFIEC CDR Call *.txt")):
        parsed = _meta.parse_inner_filename(p.name)
        if parsed is None:
            logger.warning("unrecognized TSV filename, skipping: %s", p.name)
            continue
        sid = parsed["schedule"]
        part = parsed["part"] or 1
        groups.setdefault(sid, []).append((part, p))

    return {
        sid: [p for _, p in sorted(parts)]
        for sid, parts in groups.items()
    }


# ── Parquet write ────────────────────────────────────────────────────────────

def _read_header_columns(path: Path) -> list[str]:
    """
    Read the first line of a TSV and return the tab-split column names with
    surrounding double-quotes stripped. Tries common encodings.
    """
    with path.open("rb") as f:
        raw = f.readline()
    for enc in ("utf-8", "latin-1", "cp1252"):
        try:
            line = raw.decode(enc).rstrip("\r\n")
            break
        except UnicodeDecodeError:
            continue
    else:
        raise RuntimeError(f"cannot decode header of {path}")

    cols: list[str] = []
    seen: set[str] = set()
    for tok in line.split("\t"):
        name = tok.strip().strip('"').strip()
        if not name:
            name = f"col_{len(cols)}"
        base = name
        k = 1
        while name in seen:
            k += 1
            name = f"{base}_{k}"
        seen.add(name)
        cols.append(name)
    return cols


def _read_tsv_to_arrow(path: Path) -> pa.Table:
    """
    Parse one CDR TSV into a pyarrow Table (all columns as string).

    Why pyarrow and not duckdb.read_csv: the CDR narrative schedules (RIE,
    NARR) contain embedded quotes, control characters, and occasional
    embedded newlines inside quoted TEXT* columns. DuckDB's sniffer aborts
    on these files even with explicit `columns` + `strict_mode=false`, while
    pyarrow's arrow-native CSV parser handles them fine.

    Handles both UTF-8 and Latin-1 — retries with Latin-1 if UTF-8 fails.
    Drops the CDR description row (second header) via IDRSSD numeric check.
    """
    col_names = _read_header_columns(path)
    read_opts = pacsv.ReadOptions(
        column_names=col_names,
        skip_rows=1,              # drop raw header (we supplied names)
        encoding="utf-8",
        block_size=1 << 24,        # 16 MiB reader blocks
    )
    parse_opts = pacsv.ParseOptions(
        delimiter="\t",
        quote_char='"',
        double_quote=True,
        escape_char=False,
        newlines_in_values=True,
        invalid_row_handler=lambda _row: "skip",
    )
    # all-string schema: skip type inference entirely
    conv_opts = pacsv.ConvertOptions(
        column_types={c: pa.string() for c in col_names},
        strings_can_be_null=True,
        null_values=[""],
    )

    try:
        tbl = pacsv.read_csv(path, read_options=read_opts,
                             parse_options=parse_opts, convert_options=conv_opts)
    except (pa.ArrowInvalid, UnicodeDecodeError):
        read_opts_l1 = pacsv.ReadOptions(
            column_names=col_names, skip_rows=1,
            encoding="latin-1", block_size=1 << 24,
        )
        tbl = pacsv.read_csv(path, read_options=read_opts_l1,
                             parse_options=parse_opts, convert_options=conv_opts)

    # Drop CDR description row: row where IDRSSD is not numeric
    idrssd = tbl.column("IDRSSD")
    keep_mask = pc.match_substring_regex(idrssd, r"^\d+$")
    keep_mask = pc.fill_null(keep_mask, False)
    tbl = tbl.filter(keep_mask)
    return tbl


def _write_part_parquet(tbl: pa.Table, out_path: Path) -> None:
    """Write an arrow Table to a snappy Parquet file atomically."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(".tmp")
    papq.write_table(
        tbl, tmp,
        compression=PARQUET_COMPRESSION,
        row_group_size=PARQUET_ROW_GROUP_SIZE,
    )
    os.replace(tmp, out_path)


def _build_join_sql(part_parquets: list[Path]) -> str:
    """
    Build a DuckDB SELECT that full-outer-joins per-part parquets on IDRSSD.
    Callers wrap with COPY.
    """
    parts_sql = []
    for i, pp in enumerate(part_parquets):
        parts_sql.append(
            f"part{i} AS (SELECT * FROM read_parquet('{_sql_path(pp)}'))"
        )
    with_clause = "WITH " + ", ".join(parts_sql)
    join_sql = "part0"
    for i in range(1, len(part_parquets)):
        join_sql += f' FULL OUTER JOIN part{i} USING ("IDRSSD")'
    return f"{with_clause} SELECT * FROM {join_sql}"


def _write_schedule_parquet(
    conn: duckdb.DuckDBPyConnection,
    schedule_id: str,
    year: int,
    quarter: int,
    tsv_paths: list[Path],
) -> tuple[Path, int, int]:
    """
    Materialize one schedule group to staging parquet. Returns
    (parquet_path, row_count, n_columns).

    Flow: pyarrow reads each TSV part to an in-memory Arrow Table (robust
    against narrative text quirks) → writes each part to a per-part .parquet
    → DuckDB full-outer-joins parts on IDRSSD and writes the final parquet.
    """
    out_dir = get_ffiec_staging_path(schedule_id, year, quarter)
    out_path = out_dir / "data.parquet"
    tmp_out = out_dir / "data.parquet.tmp"

    # Per-part temp parquets (cleaned up after final join)
    part_paths: list[Path] = []
    try:
        if len(tsv_paths) == 1:
            # Single-part fast path — parse and write directly
            tbl = _read_tsv_to_arrow(tsv_paths[0])
            tbl = tbl.append_column(
                "activity_year",
                pa.array([year] * tbl.num_rows, pa.int32()),
            ).append_column(
                "activity_quarter",
                pa.array([quarter] * tbl.num_rows, pa.int32()),
            )
            _write_part_parquet(tbl, tmp_out)
        else:
            for i, src in enumerate(tsv_paths):
                pp = out_dir / f"_part{i}.parquet"
                tbl = _read_tsv_to_arrow(src)
                _write_part_parquet(tbl, pp)
                part_paths.append(pp)

            join_select = _build_join_sql(part_paths)
            conn.execute(f"""
                COPY (
                    SELECT *,
                           CAST({year} AS INTEGER) AS activity_year,
                           CAST({quarter} AS INTEGER) AS activity_quarter
                    FROM ({join_select})
                ) TO '{_sql_path(tmp_out)}'
                (FORMAT PARQUET,
                 COMPRESSION '{PARQUET_COMPRESSION}',
                 ROW_GROUP_SIZE {PARQUET_ROW_GROUP_SIZE})
            """)

        os.replace(tmp_out, out_path)
    finally:
        for pp in part_paths:
            try:
                pp.unlink()
            except OSError:
                pass

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
) -> dict[str, tuple[int, int]]:
    """
    Extract, parse, write parquet for every schedule of one quarter. Upserts
    panel_metadata rows. Returns {schedule_id: (n_rows, n_cols)}.
    """
    t0 = time.time()
    extract_dir = _extract_zip_for_quarter(year, quarter, force=force)
    groups = _group_schedules(extract_dir)
    if not groups:
        logger.warning("no recognized TSVs in %s", extract_dir)
        return {}

    # Source-zip metadata (optional sha256 from manifest if present)
    manifest = _read_manifest()
    zip_meta = manifest.get("zips", {}).get(f"{year}Q{quarter}", {})

    results: dict[str, tuple[int, int]] = {}
    for sid, tsvs in groups.items():
        out_dir = get_ffiec_staging_path(sid, year, quarter)
        out_path = out_dir / "data.parquet"
        if out_path.exists() and not force:
            n_rows = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{_sql_path(out_path)}')"
            ).fetchone()[0]
            n_cols = len(conn.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{_sql_path(out_path)}')"
            ).fetchall())
            results[sid] = (int(n_rows), int(n_cols))
            logger.info("cached %s %dQ%d rows=%d cols=%d", sid, year, quarter, n_rows, n_cols)
        else:
            _, n_rows, n_cols = _write_schedule_parquet(
                conn, sid, year, quarter, tsvs
            )
            results[sid] = (n_rows, n_cols)
            logger.info("wrote %s %dQ%d rows=%d cols=%d parts=%d",
                        sid, year, quarter, n_rows, n_cols, len(tsvs))

        upsert_row(
            conn,
            "panel_metadata",
            {
                "schedule": sid,
                "year": year,
                "quarter": quarter,
                "row_count": results[sid][0],
                "source_zip": zip_meta.get("filename"),
                "source_zip_sha256": zip_meta.get("sha256"),
                "parquet_path": str(out_path),
                "n_columns": results[sid][1],
                "n_parts": len(tsvs),
                "built_at": datetime.now(timezone.utc),
            },
            key_columns=["schedule", "year", "quarter"],
        )

    dt = time.time() - t0
    logger.info("quarter %dQ%d done in %.1fs (%d schedules)",
                year, quarter, dt, len(groups))
    return results


# ── View (re)creation ────────────────────────────────────────────────────────

def _schedules_in_db(conn: duckdb.DuckDBPyConnection) -> list[str]:
    return [r[0] for r in conn.execute(
        "SELECT DISTINCT schedule FROM panel_metadata ORDER BY schedule"
    ).fetchall()]


def refresh_views(conn: duckdb.DuckDBPyConnection) -> None:
    """Create-or-replace one DuckDB view per schedule present in panel_metadata,
    then build the v2 harmonized layer on top (bs_panel / is_panel /
    filers_panel / call_reports_panel + harmonized_metadata table).
    """
    schedules = _schedules_in_db(conn)
    if not schedules:
        logger.warning("no schedules in panel_metadata — nothing to view")
        return

    staging_root = get_ffiec_storage_path("staging")
    for sid in schedules:
        glob_path = f"{staging_root}/{sid}/year=*/quarter=*/data.parquet"
        view_name = _meta.schedule_view_name(sid)
        sql = f"""
            CREATE OR REPLACE VIEW {view_name} AS
            SELECT *
            FROM read_parquet(
                '{_sql_path(Path(glob_path))}',
                hive_partitioning = true,
                union_by_name = true
            )
        """
        conn.execute(sql)
    logger.info("refreshed %d views: %s",
                len(schedules),
                ", ".join(_meta.schedule_view_name(s) for s in schedules))

    # ── Harmonized layer (v2) ────────────────────────────────────────────────
    # Import lazily via importlib because the hyphen in the package name
    # prevents a normal `from call-reports-FFIEC.harmonized.views import ...`.
    try:
        _hv_spec = importlib.util.spec_from_file_location(
            "ffiec_harmonized_views", _HERE / "harmonized" / "views.py"
        )
        _hv = importlib.util.module_from_spec(_hv_spec)
        _hv_spec.loader.exec_module(_hv)  # type: ignore[attr-defined]
        built = _hv.build_views(conn)
        logger.info("built harmonized views: %s", ", ".join(built))
    except Exception as exc:
        logger.warning("harmonized view build failed: %s", exc)


# ── MDRM dictionary load ─────────────────────────────────────────────────────

def load_mdrm(conn: duckdb.DuckDBPyConnection) -> None:
    """Load MDRM.csv into the mdrm_dictionary table (REPLACE semantics)."""
    csv_path = get_ffiec_mdrm_path() / "MDRM.csv"
    if not csv_path.exists():
        logger.info("mdrm: MDRM.csv not found — run download.py --mdrm first")
        return

    # MDRM.csv from the Fed has a leading 'PUBLIC' description line then header.
    # Skip the first line via `skip=1, header=true`.
    conn.execute(
        f"""
        CREATE OR REPLACE TABLE mdrm_dictionary AS
        SELECT * FROM read_csv(
            '{_sql_path(csv_path)}',
            delim = ',',
            quote = '"',
            escape = '"',
            header = true,
            skip = 1,
            all_varchar = true,
            ignore_errors = true,
            null_padding = true,
            parallel = false
        )
        """
    )
    n = conn.execute("SELECT COUNT(*) FROM mdrm_dictionary").fetchone()[0]
    logger.info("mdrm_dictionary loaded: %d rows", n)


# ── CLI plumbing ─────────────────────────────────────────────────────────────

def _enumerate_available_quarters() -> list[tuple[int, int]]:
    """List quarters for which a raw ZIP exists in raw/."""
    raw_root = get_ffiec_storage_path("raw")
    qs: list[tuple[int, int]] = []
    for zp in raw_root.glob("*.zip"):
        parsed = _meta.parse_zip_filename(zp.name)
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
    ap = argparse.ArgumentParser(description="FFIEC Call Reports ETL")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--quarter", type=_parse_quarter_arg,
                   help="single quarter, e.g. 2024Q4")
    g.add_argument("--year", type=int, help="all quarters for a single year")
    g.add_argument("--all", action="store_true",
                   help="every quarter present in raw/")
    g.add_argument("--refresh-views", action="store_true",
                   help="only rebuild views + load MDRM — no parquet work")
    ap.add_argument("--force", action="store_true",
                    help="rebuild staging parquet even if present")
    ap.add_argument("--skip-views", action="store_true",
                    help="skip view refresh (useful during bulk loads)")

    args = ap.parse_args()

    db_path = get_ffiec_duckdb_path()

    if args.refresh_views:
        with transactional_connection(
            db_path,
            threads=DUCKDB_THREADS,
            memory_limit=DUCKDB_MEMORY_LIMIT,
        ) as conn:
            ensure_table_exists(conn, "panel_metadata", _meta.PANEL_METADATA_DDL)
            refresh_views(conn)
            load_mdrm(conn)
        return 0

    # Determine target quarters
    if args.quarter:
        targets = [args.quarter]
    elif args.year:
        targets = [(args.year, q) for q in (1, 2, 3, 4)
                   if (args.year, q) in _enumerate_available_quarters()]
        if not targets:
            raise SystemExit(f"no raw ZIPs for year {args.year}")
    elif args.all:
        targets = _enumerate_available_quarters()
        if not targets:
            raise SystemExit("no raw ZIPs present — run download.py --status for help")
    else:
        ap.print_help()
        return 1

    logger.info("targets: %s", ", ".join(f"{y}Q{q}" for y, q in targets))

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
            load_mdrm(conn)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
