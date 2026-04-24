"""
PERMCO-RSSD link download manager.

The NY Fed CRSP-FRB link CSV must be downloaded manually from:
  https://www.newyorkfed.org/research/banking_research/crsp-frb

Place the downloaded CSV in:
  C:\\empirical-data-construction\\permco-rssd-link\\raw\\

Then run this script to register it in the manifest:
  python permco-rssd-link\\download.py

When a new version is released, replace the CSV in raw/ and re-run.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import get_permco_rssd_manifest_path, get_permco_rssd_raw_path
from utils.logging_utils import get_logger

logger = get_logger(__name__)

# Load metadata from sibling file (hyphen in folder name prevents normal import)
_meta_spec = importlib.util.spec_from_file_location(
    "_permco_rssd_metadata", Path(__file__).resolve().parent / "metadata.py"
)
_meta = importlib.util.module_from_spec(_meta_spec)  # type: ignore[arg-type]
_meta_spec.loader.exec_module(_meta)  # type: ignore[union-attr]
SOURCE_URL: str = _meta.SOURCE_URL


def _sha256(path: Path, chunk: int = 8 * 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


def _load_manifest() -> dict:
    p = get_permco_rssd_manifest_path()
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _save_manifest(data: dict) -> None:
    p = get_permco_rssd_manifest_path()
    p.write_text(json.dumps(data, indent=2), encoding="utf-8")


def find_latest_csv() -> Path | None:
    """Return the most recently modified crsp_*.csv in raw/."""
    raw = get_permco_rssd_raw_path()
    csvs = sorted(raw.glob("crsp_*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    return csvs[0] if csvs else None


def scan(force: bool = False) -> Path | None:
    """Scan raw/ for the newest CRSP-FRB CSV and update the manifest if changed."""
    csv_path = find_latest_csv()
    if csv_path is None:
        logger.warning(
            "No crsp_*.csv found in %s. Download from %s and place in raw/.",
            get_permco_rssd_raw_path(),
            SOURCE_URL,
        )
        print(
            f"\nNo CSV found. Download from:\n  {SOURCE_URL}\n"
            f"Save as: {get_permco_rssd_raw_path() / 'crsp_YYYYMMDD.csv'}\n"
        )
        return None

    manifest = _load_manifest()
    prev_sha = manifest.get("sha256", "")
    prev_name = manifest.get("filename", "")

    if not force and csv_path.name == prev_name and prev_sha:
        current_sha = _sha256(csv_path)
        if current_sha == prev_sha:
            logger.info("[download] %s unchanged (SHA-256 match); skipping.", csv_path.name)
            print(f"File unchanged: {csv_path.name}")
            return csv_path

    sha = _sha256(csv_path)
    stat = csv_path.stat()
    manifest.update(
        {
            "filename": csv_path.name,
            "sha256": sha,
            "size_bytes": stat.st_size,
            "mtime": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
            "scanned_at": datetime.now(timezone.utc).isoformat(),
            "source_url": SOURCE_URL,
        }
    )
    _save_manifest(manifest)
    logger.info("[download] Registered %s (SHA-256 %s…)", csv_path.name, sha[:12])
    print(f"Registered: {csv_path.name}  SHA-256: {sha[:16]}…")
    return csv_path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Scan raw/ for new CRSP-FRB CSV and update manifest.")
    p.add_argument("--force", action="store_true", help="Re-hash even if filename unchanged.")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    scan(force=args.force)


if __name__ == "__main__":
    main()
