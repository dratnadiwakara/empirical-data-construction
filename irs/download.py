"""
IRS SOI ZIP code data downloader.

Era A (1998–2009): ZIP archive → extract state Excel files → write combined CSV.
Era B (2010–2022): Download single national CSV directly.

Usage:
    python -m irs.download --year 2019
    python -m irs.download --all
    python -m irs.download --all --force
"""

import argparse
import csv
import io
import json
import logging
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx
import polars as pl

from config import (
    HTTP_BACKOFF_BASE,
    HTTP_CHUNK_SIZE,
    HTTP_RETRIES,
    HTTP_TIMEOUT,
    IRS_USER_AGENT,
    get_irs_manifest_path,
    get_irs_raw_path,
)
from irs.metadata import AVAILABLE_YEARS, ERA_A_YEARS, ERA_B_YEARS, SOURCE_URLS

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# ── Manifest helpers ──────────────────────────────────────────────────────────

def _load_manifest() -> dict:
    p = get_irs_manifest_path()
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    return {}


def _save_manifest(manifest: dict) -> None:
    p = get_irs_manifest_path()
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    tmp.replace(p)


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _backoff_wait(attempt: int) -> None:
    delay = min(HTTP_BACKOFF_BASE ** attempt, 120.0)
    logger.info("  backoff %.1fs (attempt %d)", delay, attempt)
    time.sleep(delay)


def _get_remote_size(client: httpx.Client, url: str) -> Optional[int]:
    try:
        resp = client.head(url, follow_redirects=True, timeout=30)
        cl = resp.headers.get("content-length")
        return int(cl) if cl else None
    except Exception:
        return None


def _get_remote_etag(client: httpx.Client, url: str) -> Optional[str]:
    try:
        resp = client.head(url, follow_redirects=True, timeout=30)
        return resp.headers.get("etag")
    except Exception:
        return None


def _download_file(client: httpx.Client, url: str, dest: Path) -> None:
    """Stream download url → dest (atomic via .tmp)."""
    tmp = dest.with_suffix(dest.suffix + ".tmp")
    for attempt in range(HTTP_RETRIES):
        try:
            with client.stream("GET", url, follow_redirects=True, timeout=HTTP_TIMEOUT) as resp:
                resp.raise_for_status()
                with open(tmp, "wb") as fh:
                    for chunk in resp.iter_bytes(chunk_size=HTTP_CHUNK_SIZE):
                        fh.write(chunk)
            tmp.replace(dest)
            return
        except Exception as exc:
            logger.warning("  attempt %d failed: %s", attempt + 1, exc)
            if attempt < HTTP_RETRIES - 1:
                _backoff_wait(attempt)
            else:
                raise


# ── Era B: national CSV (2010–2022) ──────────────────────────────────────────

def _download_era_b(year: int, force: bool, manifest: dict) -> Optional[Path]:
    url = SOURCE_URLS[year]
    raw_dir = get_irs_raw_path(year)
    yy = str(year)[2:]
    csv_path = raw_dir / f"{yy}zpallagi.csv"

    with httpx.Client(headers={"User-Agent": IRS_USER_AGENT}) as client:
        remote_size = _get_remote_size(client, url)
        entry = manifest.get(str(year), {})

        if not force and csv_path.exists():
            if remote_size and entry.get("file_size") == remote_size:
                logger.info("year %d: up-to-date, skipping", year)
                return csv_path

        logger.info("year %d: downloading %s", year, url)
        _download_file(client, url, csv_path)

    file_size = csv_path.stat().st_size
    manifest[str(year)] = {
        "era": "B",
        "url": url,
        "csv_path": str(csv_path),
        "file_size": file_size,
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
    }
    logger.info("year %d: saved %s (%.1f MB)", year, csv_path.name, file_size / 1e6)
    return csv_path


# ── Era A: ZIP archive → Excel → combined CSV (1998–2009) ────────────────────

# Known column aliases in older Excel files (normalize to lowercase IRS codes).
# The Excel files in older archives use various header capitalizations.
_ERA_A_COLUMN_ALIASES: dict[str, str] = {
    "zip": "zipcode",
    "zipcd": "zipcode",
    "zip_code": "zipcode",
    "agistub": "agi_stub",
    "agi_stub": "agi_stub",
    "statefips": "statefips",
    "state": "state",
}


def _normalize_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Lowercase all column names and apply known aliases."""
    renamed = {c: c.lower() for c in df.columns}
    renamed.update({c.lower(): _ERA_A_COLUMN_ALIASES[c.lower()]
                    for c in df.columns if c.lower() in _ERA_A_COLUMN_ALIASES})
    return df.rename(renamed)


def _clean_numeric(val) -> Optional[float]:
    """Convert a cell value to float; return None for suppressed/missing codes."""
    if val is None:
        return None
    s = str(val).strip()
    if s in ("", "--", "N/A"):
        return None
    # IRS suppresses small counts with ** or prefixes like *   123
    if s.startswith("**"):
        return None
    if s.startswith("*"):
        s = s.lstrip("* ")
    try:
        return float(s)
    except ValueError:
        return None


def _parse_zip_val(cell_val) -> Optional[str]:
    """
    Extract a 5-digit ZIP string from a cell value, or return None.
    Handles both string ('35001   ') and float (35001.0) representations.
    """
    if cell_val is None:
        return None
    s = str(cell_val).strip()
    if s.endswith(".0"):
        s = s[:-2].strip()
    if 4 <= len(s) <= 5 and s.isdigit():
        return s.zfill(5)
    return None


# Column index maps for each report-style era (col index → IRS field code).
# 1998–2002: narrow format (14–19 cols), no dividends/capgains
# 2004–2009: wide format (≥30 cols), includes all target fields
_COL_MAP_NARROW = {1: "n1", 4: "a00100", 5: "n00200", 6: "a00200"}
_COL_MAP_WIDE = {
    1: "n1", 4: "a00100", 5: "n00200", 6: "a00200",
    9: "n00600", 10: "a00600", 11: "n01000", 12: "a01000",
    13: "n00900", 14: "a00900",
}


def _read_report_style_xls(path: Path) -> Optional[pl.DataFrame]:
    """
    Parse IRS report-style XLS files (1998–2009).

    Format: rows = {header, state_total, N class-rows, blank, zip_total, N class-rows, blank, ...}
    Only ZIP total rows are extracted (col-0 is a numeric ZIP code).
    Column positions differ: narrow map for 1998-2002, wide map for 2004-2009.
    """
    try:
        import xlrd  # noqa: PLC0415
        wb = xlrd.open_workbook(str(path), formatting_info=False)
        ws = wb.sheets()[0]
    except Exception as exc:
        logger.warning("  xlrd cannot open %s: %s", path.name, exc)
        return None

    col_map = _COL_MAP_WIDE if ws.ncols >= 30 else _COL_MAP_NARROW
    rows = []
    for r in range(ws.nrows):
        zipcode = _parse_zip_val(ws.cell_value(r, 0))
        if zipcode is None:
            continue
        row: dict = {"zipcode": zipcode}
        for col_idx, field in col_map.items():
            raw = ws.cell_value(r, col_idx) if col_idx < ws.ncols else None
            row[field] = _clean_numeric(raw)
        rows.append(row)

    if not rows:
        logger.warning("  no ZIP rows found in %s", path.name)
        return None

    return pl.DataFrame(rows, infer_schema_length=len(rows))


def _read_openpyxl_flat(path: Path) -> Optional[pl.DataFrame]:
    """Read a flat-table Excel file (xlsx or XML-based xls) via openpyxl."""
    try:
        import openpyxl  # noqa: PLC0415
        wb = openpyxl.load_workbook(str(path), read_only=True, data_only=True)
        ws = wb.active
        rows_iter = ws.iter_rows(values_only=True)
        headers = [str(h).strip().lower() if h is not None else "" for h in next(rows_iter)]
        rows = []
        for row in rows_iter:
            rows.append({h: row[i] for i, h in enumerate(headers) if h})
        wb.close()
        if not rows:
            return None
        df = pl.DataFrame(rows, infer_schema_length=500)
        return _normalize_columns(df)
    except Exception as exc:
        logger.debug("  openpyxl flat read failed for %s: %s", path.name, exc)
        return None


def _excel_to_df(path: Path) -> Optional[pl.DataFrame]:
    """Read one state Excel file; return None on failure."""
    ext = path.suffix.lower()
    if ext == ".xlsx":
        try:
            df = pl.read_excel(path, engine="openpyxl")
            return _normalize_columns(df)
        except Exception as exc:
            logger.warning("  could not read %s: %s", path.name, exc)
            return None
    else:
        # .xls: try report-style (OLE2 BIFF) first
        df = _read_report_style_xls(path)
        if df is not None:
            return df
        # Fallback: XML-based .xls (Excel 2003 XML) — try openpyxl
        return _read_openpyxl_flat(path)


def _read_excel_file(path: Path) -> Optional[pl.DataFrame]:
    return _excel_to_df(path)


def _find_national_csv(raw_dir: Path, year: int) -> Optional[Path]:
    """
    Look for a pre-built national CSV inside the extracted ZIP directory.
    IRS archives often include a national-level CSV (e.g. zipcode05.csv or 05zpallagi.csv)
    alongside state-level Excel files.
    """
    yy = str(year)[2:]
    # Check common naming patterns at top level and one level down
    candidates = []
    for search_dir in [raw_dir, *[d for d in raw_dir.iterdir() if d.is_dir()]]:
        for pat in [f"{yy}zpallagi.csv", f"zipcode{yy}.csv", f"{year}zpallagi.csv"]:
            p = search_dir / pat
            if p.exists():
                candidates.append(p)
        # Also pick any CSV with >1000 rows (national-scale), excluding our derived output
        for p in search_dir.glob("*.csv"):
            if p not in candidates and p.name.lower() != "combined.csv":
                try:
                    with open(p, "r", encoding="utf-8", errors="replace") as f:
                        lines = sum(1 for _ in f)
                    if lines > 1000:
                        candidates.append(p)
                except Exception:
                    pass
    return candidates[0] if candidates else None


def _combine_state_excels(raw_dir: Path, year: int) -> Path:
    """Fallback: concatenate all state Excel files into one combined CSV."""
    # Search top-level and one subdirectory deep for Excel files
    excel_files: list[Path] = []
    for search_dir in [raw_dir, *[d for d in raw_dir.iterdir() if d.is_dir()]]:
        excel_files.extend(
            f for f in sorted(search_dir.iterdir())
            if f.suffix.lower() in (".xls", ".xlsx") and f.stem.lower() not in ("combined",)
            and "doc" not in f.stem.lower()
        )

    if not excel_files:
        raise FileNotFoundError(f"No Excel files found under {raw_dir}")

    frames: list[pl.DataFrame] = []
    for ef in excel_files:
        df = _read_excel_file(ef)
        if df is not None and len(df) > 0 and "zipcode" in df.columns:
            frames.append(df)
        elif df is not None and len(df) > 0:
            logger.warning("  skipping %s (no zipcode column)", ef.name)
        else:
            logger.debug("  skipping %s (unreadable or empty)", ef.name)

    if not frames:
        raise ValueError(f"year {year}: no usable Excel frames")

    combined = pl.concat(frames, how="diagonal_relaxed")
    combined_csv = raw_dir / "combined.csv"
    tmp = raw_dir / "combined.csv.tmp"
    combined.write_csv(tmp)
    tmp.replace(combined_csv)
    logger.info("year %d: combined %d state files → %s (%d rows)",
                year, len(frames), combined_csv.name, len(combined))
    return combined_csv


def _download_era_a(year: int, force: bool, manifest: dict) -> Optional[Path]:
    url = SOURCE_URLS[year]
    raw_dir = get_irs_raw_path(year)
    zip_path = raw_dir / f"{year}zipcode.zip"
    combined_csv = raw_dir / "combined.csv"

    with httpx.Client(headers={"User-Agent": IRS_USER_AGENT}) as client:
        remote_size = _get_remote_size(client, url)
        entry = manifest.get(str(year), {})

        if not force and combined_csv.exists():
            if remote_size and entry.get("zip_size") == remote_size:
                logger.info("year %d: up-to-date, skipping", year)
                return combined_csv

        logger.info("year %d: downloading ZIP %s", year, url)
        _download_file(client, url, zip_path)

    # Extract ZIP
    logger.info("year %d: extracting ZIP", year)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(raw_dir)

    # Prefer pre-built national CSV; fall back to combining state Excel files
    national_csv = _find_national_csv(raw_dir, year)
    if national_csv:
        logger.info("year %d: found national CSV %s", year, national_csv.name)
        combined_csv = national_csv
    else:
        combined_csv = _combine_state_excels(raw_dir, year)

    zip_size = zip_path.stat().st_size
    manifest[str(year)] = {
        "era": "A",
        "url": url,
        "zip_path": str(zip_path),
        "csv_path": str(combined_csv),
        "zip_size": zip_size,
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
    }
    return combined_csv


# ── Public API ────────────────────────────────────────────────────────────────

def download_year(year: int, force: bool = False) -> Optional[Path]:
    if year not in AVAILABLE_YEARS:
        raise ValueError(f"Year {year} not in IRS SOI available years: {AVAILABLE_YEARS}")

    manifest = _load_manifest()
    if year in ERA_A_YEARS:
        result = _download_era_a(year, force, manifest)
    else:
        result = _download_era_b(year, force, manifest)

    _save_manifest(manifest)
    return result


def download_all(force: bool = False) -> None:
    for year in AVAILABLE_YEARS:
        try:
            download_year(year, force=force)
        except Exception as exc:
            logger.error("year %d: FAILED — %s", year, exc)


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Download IRS SOI ZIP code data")
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument("--year", type=int, help="Single tax year to download")
    grp.add_argument("--all", action="store_true", help="Download all available years")
    parser.add_argument("--force", action="store_true", help="Re-download even if up-to-date")
    args = parser.parse_args()

    if args.all:
        download_all(force=args.force)
    else:
        download_year(args.year, force=args.force)


if __name__ == "__main__":
    main()
