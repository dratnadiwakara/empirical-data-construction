"""
HMDA LAR download script.

Downloads LAR files for years 2007-2024 (newest first) from CFPB/FFIEC sources.

Era routing (delegated to hmda.metadata.get_source_urls):
  2018-2024 → FFIEC snapshot pipe file
  2017      → FFIEC snapshot txt file (no header)
  2007-2016 → CFPB historic data portal (comma-delimited labeled CSV)

Features
--------
- Resume partial downloads via HTTP Range header
- Exponential backoff retry on transient errors
- Validates downloaded file size against server-reported Content-Length
- Idempotency: skips years whose (file_size, mtime) matches the manifest
- Extracts the inner file from ZIP archives automatically

Usage
-----
    python -m hmda.download --all            # 2024 -> 2007 (default)
    python -m hmda.download --year 2016      # single year
    python -m hmda.download --update         # only re-check for newer releases
    python -m hmda.download --force          # re-download even if manifest current
    python -m hmda.download --delete-raw     # delete ZIP after extraction
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import (
    HTTP_BACKOFF_BASE,
    HTTP_CHUNK_SIZE,
    HTTP_RETRIES,
    HTTP_TIMEOUT,
    USER_AGENT,
    get_manifest_path,
    get_raw_path,
)
from hmda.metadata import (
    ALL_YEARS,
    NARA_API_TMPL,
    NARA_CATALOG_TMPL,
    NARA_ULTIMATE_IDS,
    get_source_urls,
    is_pipe_delimited,
)
from utils.logging_utils import get_logger, log_step

logger = get_logger(__name__)


# ── Custom exceptions ──────────────────────────────────────────────────────────

class HMDADownloadError(Exception):
    """Base class for download errors."""


class SourceNotFoundError(HMDADownloadError):
    """All candidate URLs returned 404 for a given year."""


class FileSizeMismatchError(HMDADownloadError):
    """Downloaded file size differs from server-reported Content-Length by > 1%."""


class ExhaustedRetriesError(HMDADownloadError):
    """HTTP request failed after N retries (5xx or connection errors)."""


# ── HTTP client ────────────────────────────────────────────────────────────────

def make_client() -> httpx.Client:
    """Return a configured httpx.Client with User-Agent and redirect following."""
    return httpx.Client(
        headers={"User-Agent": USER_AGENT},
        follow_redirects=True,
        timeout=HTTP_TIMEOUT,
    )


def _backoff_wait(attempt: int, base: float = HTTP_BACKOFF_BASE) -> None:
    """Sleep base^attempt seconds, capped at 120 seconds."""
    secs = min(base ** attempt, 120.0)
    logger.debug("Backoff: sleeping %.1f seconds (attempt %d)", secs, attempt)
    time.sleep(secs)


def head_request(client: httpx.Client, url: str) -> Optional[int]:
    """
    HEAD the URL and return Content-Length as int, or None on 404.
    Raises ExhaustedRetriesError on persistent 5xx.
    """
    for attempt in range(HTTP_RETRIES):
        try:
            resp = client.head(url)
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
        except (httpx.TimeoutException, httpx.ConnectError) as exc:
            if attempt < HTTP_RETRIES - 1:
                logger.warning("HEAD %s failed (%s) — retrying", url, exc)
                _backoff_wait(attempt)
            else:
                raise ExhaustedRetriesError(f"HEAD {url} failed: {exc}") from exc
    return None


def download_with_resume(
    client: httpx.Client,
    url: str,
    dest_path: Path,
    expected_size: Optional[int] = None,
    retries: int = HTTP_RETRIES,
) -> Path:
    """
    Stream-download url to dest_path with Range-header resume support.

    If dest_path already exists as a partial file, resumes from the current
    file size. Retries transient failures with exponential backoff.
    Validates final file size against expected_size if known (1% tolerance).
    """
    for attempt in range(retries):
        existing = dest_path.stat().st_size if dest_path.exists() else 0

        if expected_size and existing >= expected_size:
            logger.info("File already complete: %s", dest_path.name)
            return dest_path

        headers: dict[str, str] = {}
        if existing > 0:
            headers["Range"] = f"bytes={existing}-"
            logger.info(
                "Resuming download at byte %d: %s", existing, dest_path.name
            )

        try:
            with client.stream("GET", url, headers=headers) as resp:
                if resp.status_code == 416:
                    # Range not satisfiable — file may already be complete
                    logger.info("Server returned 416 (range not satisfiable) — assuming complete")
                    return dest_path
                if resp.status_code not in (200, 206):
                    if resp.status_code >= 500 and attempt < retries - 1:
                        _backoff_wait(attempt)
                        continue
                    raise HMDADownloadError(
                        f"GET {url} returned HTTP {resp.status_code}"
                    )

                mode = "ab" if existing > 0 and resp.status_code == 206 else "wb"
                with open(dest_path, mode) as f:
                    for chunk in resp.iter_bytes(chunk_size=HTTP_CHUNK_SIZE):
                        f.write(chunk)

        except (httpx.TimeoutException, httpx.ConnectError, httpx.RemoteProtocolError) as exc:
            if attempt < retries - 1:
                logger.warning(
                    "Download error on attempt %d/%d: %s — retrying",
                    attempt + 1, retries, exc,
                )
                _backoff_wait(attempt)
                continue
            raise ExhaustedRetriesError(f"Download of {url} failed: {exc}") from exc

        # Success — validate size
        final_size = dest_path.stat().st_size
        if expected_size and abs(final_size - expected_size) / expected_size > 0.01:
            raise FileSizeMismatchError(
                f"Expected ~{expected_size:,} bytes, got {final_size:,} for {dest_path.name}"
            )
        log_step(logger, "download_complete", url=url, bytes=final_size)
        return dest_path

    raise ExhaustedRetriesError(f"Exhausted {retries} retries for {url}")


# ── ZIP extraction ─────────────────────────────────────────────────────────────

def extract_zip(zip_path: Path, dest_dir: Path) -> Path:
    """
    Extract the primary data file from a ZIP archive into dest_dir.
    Returns the path to the extracted file.

    Looks for the largest file inside the ZIP (the LAR data file).
    If only one file exists, extracts that.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        names = zf.namelist()
        if not names:
            raise HMDADownloadError(f"Empty ZIP archive: {zip_path}")

        # Pick the largest member (the LAR data file)
        info_list = [zf.getinfo(n) for n in names if not n.endswith("/")]
        if not info_list:
            raise HMDADownloadError(f"No files inside ZIP: {zip_path}")

        target_info = max(info_list, key=lambda i: i.file_size)
        inner_name = target_info.filename
        logger.info("Extracting '%s' from %s", inner_name, zip_path.name)

        extracted = zf.extract(target_info, path=dest_dir)
        return Path(extracted)


def find_raw_file(raw_dir: Path, year: int) -> Optional[Path]:
    """Return the extracted LAR file in raw_dir, or None if not found."""
    for ext in ("*.txt", "*.csv", "*.dat", "*.DAT", "*.pipe"):
        matches = list(raw_dir.glob(ext))
        if matches:
            return max(matches, key=lambda p: p.stat().st_size)
    return None


# ── NARA scraping ──────────────────────────────────────────────────────────────

def fetch_nara_download_url(nara_id: int, client: httpx.Client) -> str:
    """
    Return the direct download URL for a NARA catalog record.

    Strategy:
    1. Try the NARA API (JSON response with object URLs).
    2. Fall back to HTML scraping of the catalog page.
    """
    # Try JSON API first
    api_url = NARA_API_TMPL.format(nara_id=nara_id)
    try:
        resp = client.get(api_url, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            # Navigate NARA API response to find file URL
            results = data.get("opaResponse", {}).get("results", {}).get("result", [])
            if isinstance(results, dict):
                results = [results]
            for result in results:
                objects = result.get("objects", {}).get("object", [])
                if isinstance(objects, dict):
                    objects = [objects]
                for obj in objects:
                    file_url = obj.get("file", {}).get("@url", "")
                    if file_url:
                        logger.info("NARA API resolved URL: %s", file_url)
                        return file_url
    except Exception as exc:
        logger.warning("NARA API failed for id %d: %s — falling back to HTML", nara_id, exc)

    # Fall back to HTML scraping
    catalog_url = NARA_CATALOG_TMPL.format(nara_id=nara_id)
    resp = client.get(catalog_url, timeout=30)
    if resp.status_code != 200:
        raise SourceNotFoundError(
            f"NARA catalog page returned {resp.status_code} for id {nara_id}"
        )

    try:
        from bs4 import BeautifulSoup
    except ImportError as e:
        raise SourceNotFoundError(
            "beautifulsoup4 is required for NARA HTML scraping (years 2004-2006). "
            "Install with: pip install beautifulsoup4 lxml"
        ) from e

    soup = BeautifulSoup(resp.text, "lxml")

    # Look for download links — NARA uses various HTML patterns
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if any(ext in href.lower() for ext in (".dat", ".zip", ".txt", ".csv")):
            if "download" in href.lower() or "file" in href.lower():
                if href.startswith("http"):
                    return href
                return "https://catalog.archives.gov" + href

    # Try data-url attributes
    for tag in soup.find_all(attrs={"data-url": True}):
        url = tag["data-url"]
        if any(ext in url.lower() for ext in (".dat", ".zip")):
            return url

    raise SourceNotFoundError(
        f"Could not find download URL on NARA catalog page for id {nara_id}. "
        f"Please manually download from: {catalog_url}"
    )


# ── Manifest (idempotency) ─────────────────────────────────────────────────────

def load_manifest() -> dict:
    """Load the download manifest JSON; return {} if not found."""
    path = get_manifest_path()
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Could not read manifest (%s) — starting fresh", exc)
    return {}


def save_manifest(manifest: dict) -> None:
    """Atomically write the manifest (temp file + rename)."""
    path = get_manifest_path()
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    tmp.replace(path)


def _file_fingerprint(path: Path) -> dict:
    """Return {size, mtime} for a file."""
    stat = path.stat()
    return {"size": stat.st_size, "mtime": stat.st_mtime}


def is_download_current(year: int, manifest: dict, local_path: Path) -> bool:
    """
    True if the local file exists and its (size, mtime) matches the manifest entry.
    """
    if not local_path.exists():
        return False
    entry = manifest.get(str(year), {})
    if not entry:
        return False
    fp = _file_fingerprint(local_path)
    return (
        entry.get("size") == fp["size"]
        and abs(entry.get("mtime", 0) - fp["mtime"]) < 2.0
    )


def update_manifest_entry(
    year: int,
    manifest: dict,
    local_path: Path,
    source_url: str,
    expected_size: Optional[int],
) -> None:
    """Record a completed download in the manifest."""
    fp = _file_fingerprint(local_path)
    manifest[str(year)] = {
        "url": source_url,
        "local_path": str(local_path),
        "size": fp["size"],
        "mtime": fp["mtime"],
        "expected_size": expected_size,
        "downloaded_at": datetime.now(tz=timezone.utc).isoformat(),
    }


def sha256_file(path: Path) -> str:
    """Compute SHA-256 of a file in streaming chunks."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


# ── Year-level orchestration ───────────────────────────────────────────────────

def download_year(
    year: int,
    manifest: dict,
    force: bool = False,
    delete_raw: bool = False,
    client: Optional[httpx.Client] = None,
) -> Optional[Path]:
    """
    Download and extract a single year's LAR file.

    Returns the path to the extracted data file, or None if skipped.
    Updates manifest in-place (caller must save after all years complete).
    """
    raw_dir = get_raw_path(year)
    existing_file = find_raw_file(raw_dir, year)

    if not force and existing_file and is_download_current(year, manifest, existing_file):
        logger.info("[%d] Skipping — already current (%s)", year, existing_file.name)
        return existing_file

    own_client = client is None
    if own_client:
        client = make_client()

    try:
        if year <= 2006:
            return _download_nara_year(year, raw_dir, manifest, delete_raw, client)
        else:
            # 2007-2024: standard CFPB/FFIEC download
            return _download_standard_year(year, raw_dir, manifest, delete_raw, client)
    finally:
        if own_client:
            client.close()


def _download_standard_year(
    year: int,
    raw_dir: Path,
    manifest: dict,
    delete_raw: bool,
    client: httpx.Client,
) -> Optional[Path]:
    """Download from CFPB S3 / FFIEC national / CFPB historic sources."""
    candidates = get_source_urls(year)
    if not candidates:
        raise SourceNotFoundError(f"No download URLs configured for year {year}")

    url = None
    expected_size = None
    for candidate in candidates:
        logger.info("[%d] Trying: %s", year, candidate)
        size = head_request(client, candidate)
        if size is not None or _url_exists(client, candidate):
            url = candidate
            expected_size = size
            break

    if url is None:
        raise SourceNotFoundError(
            f"[{year}] All sources returned 404: {candidates}"
        )

    zip_path = raw_dir / f"{year}_lar.zip"
    download_with_resume(client, url, zip_path, expected_size=expected_size)

    # Extract inner file
    extracted = extract_zip(zip_path, raw_dir)
    log_step(logger, "extracted", year=year, file=extracted.name)

    update_manifest_entry(year, manifest, extracted, url, expected_size)

    if delete_raw and zip_path.exists():
        zip_path.unlink()
        logger.info("[%d] Deleted raw ZIP", year)

    return extracted


def _url_exists(client: httpx.Client, url: str) -> bool:
    """GET the first byte to check if URL is accessible when HEAD fails."""
    try:
        resp = client.get(url, headers={"Range": "bytes=0-0"})
        return resp.status_code in (200, 206)
    except Exception:
        return False


def _download_nara_year(
    year: int,
    raw_dir: Path,
    manifest: dict,
    delete_raw: bool,
    client: httpx.Client,
) -> Optional[Path]:
    """Fetch a 2004-2006 LAR file from NARA."""
    nara_id = NARA_ULTIMATE_IDS.get(year)
    if not nara_id:
        raise SourceNotFoundError(f"No NARA ID configured for year {year}")

    logger.info("[%d] Fetching NARA download URL for record id %d", year, nara_id)
    try:
        file_url = fetch_nara_download_url(nara_id, client)
    except SourceNotFoundError:
        logger.error(
            "[%d] NARA scraping failed. Manual download required.\n"
            "  Visit: %s\n"
            "  Save the .DAT or .zip file to: %s",
            year,
            NARA_CATALOG_TMPL.format(nara_id=nara_id),
            raw_dir,
        )
        return None

    suffix = Path(file_url.split("?")[0]).suffix or ".dat"
    dest = raw_dir / f"{year}_lar{suffix}"

    expected_size = head_request(client, file_url)
    download_with_resume(client, file_url, dest, expected_size=expected_size)

    # If it's a ZIP, extract it
    if dest.suffix.lower() == ".zip":
        extracted = extract_zip(dest, raw_dir)
        update_manifest_entry(year, manifest, extracted, file_url, expected_size)
        if delete_raw:
            dest.unlink()
        return extracted

    update_manifest_entry(year, manifest, dest, file_url, expected_size)
    return dest


def download_all(
    years: list,
    manifest: dict,
    force: bool = False,
    delete_raw: bool = False,
) -> dict:
    """
    Download all specified years sequentially.
    Returns dict mapping year → extracted file path (or None if skipped/failed).
    """
    results: dict[int, Optional[Path]] = {}
    failed: list[int] = []

    with make_client() as client:
        for year in years:
            try:
                path = download_year(year, manifest, force=force, delete_raw=delete_raw, client=client)
                results[year] = path
                save_manifest(manifest)  # persist after each successful download
            except SourceNotFoundError as exc:
                logger.error("[%d] Source not found: %s", year, exc)
                results[year] = None
                failed.append(year)
            except HMDADownloadError as exc:
                logger.error("[%d] Download failed: %s", year, exc)
                results[year] = None
                failed.append(year)

    if failed:
        logger.warning("Failed years: %s", failed)
    return results


# ── CLI ────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Download HMDA LAR files for 2007-2024."
    )
    grp = p.add_mutually_exclusive_group()
    grp.add_argument("--year", type=int, help="Download a single year (e.g., 2016)")
    grp.add_argument(
        "--all", dest="all_years", action="store_true",
        help="Download all years 2024 -> 2007 (default)"
    )
    p.add_argument(
        "--update", action="store_true",
        help="Only re-download years where server has a different Content-Length"
    )
    p.add_argument(
        "--force", action="store_true",
        help="Re-download even if manifest says file is current"
    )
    p.add_argument(
        "--delete-raw", action="store_true",
        help="Delete raw ZIP archive after successful extraction"
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    manifest = load_manifest()

    if args.year:
        years = [args.year]
    else:
        years = ALL_YEARS  # 2024 first → 2004

    logger.info("Starting HMDA LAR download for %d year(s): %s", len(years), years)
    results = download_all(years, manifest, force=args.force, delete_raw=args.delete_raw)

    success = sum(1 for v in results.values() if v is not None)
    logger.info("Download complete: %d/%d years successful", success, len(years))
    save_manifest(manifest)


if __name__ == "__main__":
    main()
