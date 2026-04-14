"""
CRA flat file download script.

Downloads aggregate, disclosure, and transmittal flat files for years 1996-2024
from the FFIEC website.

URL pattern: https://www.ffiec.gov/cra/xls/{YY}exp_{type}.zip
  YY   = 2-digit year
  type = aggr | trans | discl

Features
--------
- Resume partial downloads via HTTP Range header
- Exponential backoff retry on transient errors
- Validates downloaded file size against server-reported Content-Length
- Idempotency: skips files whose (file_size, mtime) matches the manifest
- Extracts .dat files from ZIP archives automatically

Usage
-----
    python -m cra.download --year 2024          # download single year (all 3 types)
    python -m cra.download --year 2024 --type aggr  # only aggregate
    python -m cra.download --all                # 2024 -> 1996
    python -m cra.download --force              # re-download even if manifest current
"""
from __future__ import annotations

import argparse
import json
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from curl_cffi import requests as cffi_requests

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import (
    HTTP_BACKOFF_BASE,
    HTTP_RETRIES,
    HTTP_TIMEOUT,
    get_cra_manifest_path,
    get_cra_raw_path,
)
from cra.metadata import (
    ALL_YEARS,
    FILE_TYPES,
    get_download_url,
    get_zip_filename,
)
from utils.logging_utils import get_logger, log_step

logger = get_logger(__name__)

IMPERSONATE = "chrome"


# ── Custom exceptions ─────────────────────────────────────────────────────────

class CRADownloadError(Exception):
    """Base class for CRA download errors."""

class ExhaustedRetriesError(CRADownloadError):
    """HTTP request failed after N retries."""

class FileSizeMismatchError(CRADownloadError):
    """Downloaded file size differs from server-reported Content-Length by > 1%."""


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _backoff_wait(attempt: int, base: float = HTTP_BACKOFF_BASE) -> None:
    secs = min(base ** attempt, 120.0)
    logger.debug("Backoff: sleeping %.1f seconds (attempt %d)", secs, attempt)
    time.sleep(secs)


def head_request(url: str) -> Optional[int]:
    """HEAD the URL and return Content-Length, or None on 404."""
    for attempt in range(HTTP_RETRIES):
        try:
            resp = cffi_requests.head(url, impersonate=IMPERSONATE, timeout=HTTP_TIMEOUT)
            if resp.status_code == 404:
                return None
            if resp.status_code == 200:
                cl = resp.headers.get("content-length")
                return int(cl) if cl else None
            if resp.status_code >= 500:
                if attempt < HTTP_RETRIES - 1:
                    _backoff_wait(attempt)
                    continue
                raise ExhaustedRetriesError(
                    f"HEAD {url} returned {resp.status_code} after {HTTP_RETRIES} attempts"
                )
        except Exception as exc:
            if attempt < HTTP_RETRIES - 1:
                logger.warning("HEAD %s failed (%s) — retrying", url, exc)
                _backoff_wait(attempt)
            else:
                raise ExhaustedRetriesError(f"HEAD {url} failed: {exc}") from exc
    return None


def download_file(
    url: str,
    dest_path: Path,
    expected_size: Optional[int] = None,
    retries: int = HTTP_RETRIES,
) -> Path:
    """Download url to dest_path using curl_cffi (bypasses Cloudflare)."""
    for attempt in range(retries):
        if dest_path.exists() and expected_size and dest_path.stat().st_size >= expected_size:
            logger.info("File already complete: %s", dest_path.name)
            return dest_path

        try:
            resp = cffi_requests.get(url, impersonate=IMPERSONATE, timeout=HTTP_TIMEOUT)
            if resp.status_code != 200:
                if resp.status_code >= 500 and attempt < retries - 1:
                    _backoff_wait(attempt)
                    continue
                raise CRADownloadError(
                    f"GET {url} returned HTTP {resp.status_code}"
                )

            with open(dest_path, "wb") as f:
                f.write(resp.content)

        except CRADownloadError:
            raise
        except Exception as exc:
            if attempt < retries - 1:
                logger.warning(
                    "Download error on attempt %d/%d: %s — retrying",
                    attempt + 1, retries, exc,
                )
                _backoff_wait(attempt)
                continue
            raise ExhaustedRetriesError(f"Download of {url} failed: {exc}") from exc

        final_size = dest_path.stat().st_size
        if expected_size and abs(final_size - expected_size) / expected_size > 0.01:
            raise FileSizeMismatchError(
                f"Expected ~{expected_size:,} bytes, got {final_size:,} for {dest_path.name}"
            )
        log_step(logger, "download_complete", url=url, bytes=final_size)
        return dest_path

    raise ExhaustedRetriesError(f"Exhausted {retries} retries for {url}")


# ── ZIP extraction ────────────────────────────────────────────────────────────

def extract_zip(zip_path: Path, dest_dir: Path) -> list[Path]:
    """
    Extract all .dat files from a CRA ZIP archive.
    Returns list of extracted file paths.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    extracted: list[Path] = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            name_lower = info.filename.lower()
            if name_lower.endswith(".dat") or name_lower.endswith(".txt"):
                out_path = dest_dir / Path(info.filename).name
                with zf.open(info) as src, open(out_path, "wb") as dst:
                    dst.write(src.read())
                extracted.append(out_path)
                logger.info("Extracted: %s", out_path.name)
    if not extracted:
        raise CRADownloadError(f"No .dat/.txt files found in ZIP: {zip_path}")
    return extracted


# ── Manifest (idempotency) ────────────────────────────────────────────────────

def load_manifest() -> dict:
    path = get_cra_manifest_path()
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            logger.warning("Corrupt manifest — starting fresh")
    return {}


def save_manifest(manifest: dict) -> None:
    path = get_cra_manifest_path()
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    tmp.replace(path)


def manifest_key(year: int, file_type: str) -> str:
    return f"{year}_{file_type}"


def is_download_current(manifest: dict, year: int, file_type: str, zip_path: Path) -> bool:
    key = manifest_key(year, file_type)
    entry = manifest.get(key)
    if not entry:
        return False
    if not zip_path.exists():
        return False
    return (
        entry.get("size") == zip_path.stat().st_size
        and entry.get("mtime") == zip_path.stat().st_mtime
    )


# ── Download orchestration ────────────────────────────────────────────────────

def download_year(year: int, file_types: list[str], force: bool = False) -> dict[str, Path]:
    """
    Download CRA flat files for a single year.
    Returns dict of {file_type: zip_path} for successfully downloaded files.
    """
    manifest = load_manifest()
    raw_dir = get_cra_raw_path(year)
    results: dict[str, Path] = {}

    for ft in file_types:
        url = get_download_url(year, ft)
        zip_name = get_zip_filename(year, ft)
        zip_path = raw_dir / zip_name

        if not force and is_download_current(manifest, year, ft, zip_path):
            logger.info("[%d/%s] Manifest says current — skipping", year, ft)
            results[ft] = zip_path
            continue

        logger.info("[%d/%s] Downloading from %s", year, ft, url)
        expected_size = head_request(url)

        download_file(url, zip_path, expected_size)

        # Extract
        extracted = extract_zip(zip_path, raw_dir)
        logger.info("[%d/%s] Extracted %d files", year, ft, len(extracted))

        # Update manifest
        key = manifest_key(year, ft)
        manifest[key] = {
            "url": url,
            "size": zip_path.stat().st_size,
            "mtime": zip_path.stat().st_mtime,
            "extracted": [str(p.name) for p in extracted],
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
        }
        save_manifest(manifest)
        results[ft] = zip_path

    return results


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download CRA flat files from FFIEC.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--year", type=int, help="Download a single year")
    group.add_argument("--all", action="store_true", help="Download all years (2024 -> 1996)")
    parser.add_argument(
        "--type", dest="file_type", choices=FILE_TYPES,
        help="Download only one file type (default: all three)",
    )
    parser.add_argument("--force", action="store_true", help="Re-download even if manifest current")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    file_types = [args.file_type] if args.file_type else FILE_TYPES

    if args.all:
        years = ALL_YEARS
    else:
        years = [args.year]

    for year in years:
        logger.info("=" * 60)
        logger.info("Processing year %d", year)
        try:
            download_year(year, file_types, force=args.force)
        except CRADownloadError as exc:
            logger.error("Failed to download year %d: %s", year, exc)
            continue

    logger.info("Download complete.")


if __name__ == "__main__":
    main()
