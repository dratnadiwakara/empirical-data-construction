"""
Download FDIC Summary of Deposits (SOD) data via the FDIC BankFind API.

Paginates the SOD endpoint year by year (1994-present).
Idempotency: compares meta.total from API against stored total_rows in manifest.
"""
from __future__ import annotations

import argparse
import csv
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import (
    HTTP_BACKOFF_BASE,
    HTTP_RETRIES,
    HTTP_TIMEOUT,
    get_sod_manifest_path,
    get_sod_raw_path,
)
from sod.metadata import ALL_YEARS, API_BASE, API_FIELDS, API_PAGE_SIZE, FIRST_YEAR, LAST_YEAR
from utils.logging_utils import get_logger

logger = get_logger(__name__)

SOD_USER_AGENT = (
    "SOD-Research-Pipeline/1.0 "
    "(academic; empirical-data-construction; "
    "https://github.com/dratnadiwakara/empirical-data-construction)"
)


class SODDownloadError(Exception):
    """Raised on unrecoverable SOD download failures."""


def _backoff_wait(attempt: int) -> None:
    time.sleep(min(HTTP_BACKOFF_BASE ** attempt, 120.0))


def _make_client() -> httpx.Client:
    return httpx.Client(
        timeout=HTTP_TIMEOUT,
        headers={"User-Agent": SOD_USER_AGENT},
        follow_redirects=True,
    )


def _get_total(client: httpx.Client, year: int) -> int:
    """Return meta.total from FDIC API for the given survey year."""
    for attempt in range(HTTP_RETRIES):
        try:
            resp = client.get(
                API_BASE,
                params={"filters": f"YEAR:{year}", "limit": 1, "format": "json", "fields": "YEAR"},
            )
            resp.raise_for_status()
            return resp.json()["meta"]["total"]
        except Exception as exc:
            if attempt < HTTP_RETRIES - 1:
                logger.warning("[%d] meta.total fetch failed (%s); retrying", year, exc)
                _backoff_wait(attempt)
            else:
                raise SODDownloadError(f"Cannot fetch meta.total for {year}: {exc}") from exc
    return 0


def _fetch_page(client: httpx.Client, year: int, offset: int) -> list[dict]:
    """Fetch one page of SOD records as a list of field dicts."""
    for attempt in range(HTTP_RETRIES):
        try:
            resp = client.get(
                API_BASE,
                params={
                    "filters": f"YEAR:{year}",
                    "limit": API_PAGE_SIZE,
                    "offset": offset,
                    "format": "json",
                    "fields": ",".join(API_FIELDS),
                },
            )
            resp.raise_for_status()
            return [rec["data"] for rec in resp.json().get("data", [])]
        except Exception as exc:
            if attempt < HTTP_RETRIES - 1:
                logger.warning(
                    "[%d] Page fetch failed at offset=%d (%s); retrying", year, offset, exc
                )
                _backoff_wait(attempt)
            else:
                raise SODDownloadError(
                    f"Page fetch failed year={year} offset={offset}: {exc}"
                ) from exc
    return []


def load_manifest() -> dict:
    path = get_sod_manifest_path()
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            logger.warning("SOD manifest unreadable; starting fresh")
    return {}


def save_manifest(manifest: dict) -> None:
    path = get_sod_manifest_path()
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    tmp.replace(path)


def download_year(year: int, force: bool = False) -> Optional[Path]:
    """Download all SOD records for one survey year to a CSV file."""
    raw_dir = get_sod_raw_path(year)
    csv_path = raw_dir / f"sod_{year}.csv"

    manifest = load_manifest()
    entry = manifest.get(str(year), {})

    with _make_client() as client:
        remote_total = _get_total(client, year)

        if remote_total == 0:
            logger.info("[%d] API reports 0 records; skipping", year)
            return None

        if (
            not force
            and csv_path.exists()
            and entry.get("total_rows") == remote_total
        ):
            logger.info("[%d] Manifest current (%d rows); skipping", year, remote_total)
            return csv_path

        logger.info("[%d] Downloading %d rows", year, remote_total)

        all_rows: list[dict] = []
        offset = 0
        while offset < remote_total:
            page = _fetch_page(client, year, offset)
            if not page:
                break
            all_rows.extend(page)
            offset += len(page)
            logger.info("[%d] Fetched %d / %d rows", year, len(all_rows), remote_total)

    # Atomic write: tmp → rename
    tmp_csv = csv_path.with_suffix(".csv.tmp")
    with open(tmp_csv, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=API_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)
    tmp_csv.replace(csv_path)

    row_count = len(all_rows)
    manifest[str(year)] = {
        "total_rows": remote_total,
        "row_count": row_count,
        "csv_path": csv_path.name,
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
    }
    save_manifest(manifest)
    logger.info("[%d] Saved %d rows -> %s", year, row_count, csv_path)
    return csv_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download FDIC SOD data via BankFind API.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--year", type=int, help=f"Single survey year ({FIRST_YEAR}–{LAST_YEAR})")
    group.add_argument("--all", action="store_true", help="Download all years")
    parser.add_argument("--force", action="store_true", help="Re-download even if manifest current")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    years = ALL_YEARS if args.all else [args.year or LAST_YEAR]

    for year in years:
        try:
            download_year(year, force=args.force)
        except SODDownloadError as exc:
            logger.error("[%d] Download failed: %s", year, exc)

    logger.info("SOD download step complete.")


if __name__ == "__main__":
    main()
