"""
Download FFIEC NIC snapshot ZIP files.

Downloads:
- CSV_RELATIONSHIPS.ZIP
- CSV_TRANSFORMATIONS.ZIP

Update behavior is version-based (not year-based). The manifest tracks
sha256/content-length/etag so `--update` can skip unchanged files.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from curl_cffi import requests as cffi_requests

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import HTTP_BACKOFF_BASE, HTTP_RETRIES, HTTP_TIMEOUT, get_nic_manifest_path, get_nic_raw_path
from nic.metadata import BASE_URL, DATASETS
from utils.logging_utils import get_logger

logger = get_logger(__name__)
IMPERSONATE = "chrome"


class NICDownloadError(Exception):
    """Base class for NIC download failures."""


def _backoff_wait(attempt: int, base: float = HTTP_BACKOFF_BASE) -> None:
    time.sleep(min(base ** attempt, 120.0))


def compute_sha256(path: Path) -> str:
    """Compute SHA-256 for file content."""
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def load_manifest() -> dict:
    """Load manifest from disk."""
    path = get_nic_manifest_path()
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            logger.warning("NIC manifest unreadable; starting new manifest")
    return {}


def save_manifest(manifest: dict) -> None:
    """Atomically persist manifest."""
    path = get_nic_manifest_path()
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    tmp.replace(path)


def head_request(url: str) -> dict[str, Optional[str]]:
    """Return lightweight remote metadata from HEAD."""
    for attempt in range(HTTP_RETRIES):
        try:
            resp = cffi_requests.head(url, impersonate=IMPERSONATE, timeout=HTTP_TIMEOUT)
            if resp.status_code == 404:
                logger.warning("HEAD returned 404 for %s; continuing without remote metadata", url)
                return {"content_length": None, "etag": None, "last_modified": None}
            if resp.status_code != 200:
                raise NICDownloadError(f"HEAD {url} returned HTTP {resp.status_code}")
            return {
                "content_length": resp.headers.get("content-length"),
                "etag": resp.headers.get("etag"),
                "last_modified": resp.headers.get("last-modified"),
            }
        except Exception as exc:
            if attempt < HTTP_RETRIES - 1:
                logger.warning("HEAD failed for %s (%s); retrying", url, exc)
                _backoff_wait(attempt)
            else:
                raise NICDownloadError(f"HEAD failed for {url}: {exc}") from exc
    return {"content_length": None, "etag": None, "last_modified": None}


def download_file(url: str, dest_path: Path, retries: int = HTTP_RETRIES) -> Path:
    """Download URL to dest_path."""
    for attempt in range(retries):
        try:
            resp = cffi_requests.get(url, impersonate=IMPERSONATE, timeout=HTTP_TIMEOUT)
            if resp.status_code != 200:
                raise NICDownloadError(f"GET {url} returned HTTP {resp.status_code}")
            with open(dest_path, "wb") as fh:
                fh.write(resp.content)
            return dest_path
        except Exception as exc:
            if attempt < retries - 1:
                logger.warning("Download failed for %s (%s); retrying", url, exc)
                _backoff_wait(attempt)
            else:
                raise NICDownloadError(f"Download failed for {url}: {exc}") from exc
    raise NICDownloadError(f"Exhausted retries for {url}")


def check_remote_changed(manifest_entry: Optional[dict], remote_meta: dict[str, Optional[str]]) -> bool:
    """Return True if remote appears changed relative to manifest entry."""
    if not manifest_entry:
        return True
    if remote_meta.get("etag") and manifest_entry.get("etag"):
        return remote_meta["etag"] != manifest_entry["etag"]
    if remote_meta.get("content_length") and manifest_entry.get("content_length"):
        return remote_meta["content_length"] != manifest_entry["content_length"]
    if remote_meta.get("last_modified") and manifest_entry.get("last_modified"):
        return remote_meta["last_modified"] != manifest_entry["last_modified"]
    # If no comparable metadata, default to update attempt.
    return True


def download_dataset(dataset: str, force: bool = False, update: bool = False) -> Optional[Path]:
    """Download one NIC dataset ZIP if required."""
    if dataset not in DATASETS:
        raise NICDownloadError(f"Unknown dataset: {dataset}")

    meta = DATASETS[dataset]
    filename = meta["filename"]
    url = f"{BASE_URL}{meta['download_path']}"
    raw_dir = get_nic_raw_path()
    out_path = raw_dir / filename

    manifest = load_manifest()
    entry = manifest.get(dataset)
    remote_meta = head_request(url) if update else {"content_length": None, "etag": None, "last_modified": None}

    if not force and out_path.exists():
        if update and not check_remote_changed(entry, remote_meta):
            logger.info("[%s] No remote change detected; skipping", dataset)
            return out_path
        if not update and entry and entry.get("sha256") == compute_sha256(out_path):
            logger.info("[%s] Local file matches manifest; skipping", dataset)
            return out_path

    logger.info("[%s] Downloading %s", dataset, filename)
    download_file(url, out_path)
    sha256 = compute_sha256(out_path)

    manifest[dataset] = {
        "dataset": dataset,
        "filename": filename,
        "url": url,
        "sha256": sha256,
        "size": out_path.stat().st_size,
        "content_length": remote_meta.get("content_length"),
        "etag": remote_meta.get("etag"),
        "last_modified": remote_meta.get("last_modified"),
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
    }
    save_manifest(manifest)

    logger.info("[%s] Downloaded %s (sha256=%s)", dataset, filename, sha256[:12])
    return out_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download FFIEC NIC snapshot ZIP files.")
    parser.add_argument(
        "--dataset",
        choices=sorted(DATASETS.keys()),
        help="Download only one dataset (default: both)",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Check remote metadata and only download changed files",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-download regardless of manifest state",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    targets = [args.dataset] if args.dataset else sorted(DATASETS.keys())

    for dataset in targets:
        try:
            download_dataset(dataset=dataset, force=args.force, update=args.update)
        except NICDownloadError as exc:
            logger.error("[%s] Download failed: %s", dataset, exc)

    logger.info("NIC download step complete.")


if __name__ == "__main__":
    main()
