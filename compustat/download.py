"""
Compustat ingest step.

There is no public WRDS download endpoint, so this script "downloads" by
snapshotting the WRDS extract from compustat/temp/ into the HDD raw/
directory. Idempotency is via sha256 of the source files.

Future updates: drop a refreshed compustat_fund.gz into compustat/temp/ and
rerun. The manifest detects the sha256 change and recopies.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Final, Optional

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import get_compustat_manifest_path, get_compustat_raw_path
from compustat.metadata import SOURCE_FUND_FILE, SOURCE_VARS_FILE
from utils.logging_utils import get_logger

logger = get_logger(__name__)

TEMP_DIR: Final[Path] = Path(__file__).resolve().parent / "temp"
SOURCE_FILES: Final[tuple[str, ...]] = (SOURCE_FUND_FILE, SOURCE_VARS_FILE)
CHUNK: Final[int] = 8 * 1024 * 1024


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


def load_manifest() -> dict:
    path = get_compustat_manifest_path()
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            logger.warning("Compustat manifest unreadable; starting fresh")
    return {}


def save_manifest(manifest: dict) -> None:
    path = get_compustat_manifest_path()
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    tmp.replace(path)


def _atomic_copy(src: Path, dst: Path) -> None:
    """Stream-copy src -> dst.tmp -> rename(dst). Never leaves a partial file."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    with open(src, "rb") as fin, open(tmp, "wb") as fout:
        for chunk in iter(lambda: fin.read(CHUNK), b""):
            fout.write(chunk)
    tmp.replace(dst)


def ingest_file(name: str, force: bool = False) -> Optional[Path]:
    src = TEMP_DIR / name
    if not src.exists():
        raise FileNotFoundError(
            f"Source file missing: {src}. Drop the WRDS extract into "
            f"compustat/temp/ before running download."
        )

    raw_dir = get_compustat_raw_path()
    dst = raw_dir / name

    src_sha = _sha256(src)
    src_size = src.stat().st_size
    src_mtime = datetime.fromtimestamp(src.stat().st_mtime, tz=timezone.utc).isoformat()

    manifest = load_manifest()
    entry = manifest.get(name, {})

    if not force and dst.exists() and entry.get("sha256") == src_sha:
        logger.info("[%s] manifest current (sha256 match); skipping", name)
        return dst

    logger.info("[%s] copying %s -> %s (%.1f MB)", name, src, dst, src_size / 1e6)
    _atomic_copy(src, dst)

    manifest[name] = {
        "sha256": src_sha,
        "size": src_size,
        "source_path": str(src),
        "source_mtime": src_mtime,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }
    save_manifest(manifest)
    logger.info("[%s] ingested (sha256=%s...)", name, src_sha[:12])
    return dst


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Snapshot Compustat WRDS extract from compustat/temp/ to HDD raw/."
    )
    p.add_argument("--force", action="store_true",
                   help="Recopy even if manifest sha256 matches.")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    for name in SOURCE_FILES:
        ingest_file(name, force=args.force)
    logger.info("Compustat download step complete.")


if __name__ == "__main__":
    main()
