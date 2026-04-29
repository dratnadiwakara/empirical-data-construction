"""
RateWatch 'download' step: extract raw text files from local source on D:\\.

Two modes:
- UNZIPPED_YEARS (2001-2020): copy depositRateData_clean_{year}.txt -> raw/{year}/
- ZIPPED_YEARS  (2021-2023):  extract DepositRateData{year}.txt from
                              RW_DepositDataFeedMASTER.zip -> raw/{year}/

Also stages the four shared support tables once into raw/support/.

Idempotency: manifest tracks size + sha256 of each staged file. --force overrides.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import (
    get_ratewatch_manifest_path,
    get_ratewatch_raw_path,
    get_ratewatch_support_path,
)
from ratewatch.metadata import (
    ALL_YEARS,
    FIRST_YEAR,
    LAST_YEAR,
    LSU_YEARS,
    SUPPORT_FILES,
    UNZIPPED_YEARS,
    ZIP_FILE_2021,
    ZIPPED_YEARS,
    raw_source_filename,
    raw_source_path,
    support_source_path,
)
from utils.logging_utils import get_logger

logger = get_logger(__name__)

CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(CHUNK_SIZE), b""):
            h.update(chunk)
    return h.hexdigest()


def load_manifest() -> dict:
    path = get_ratewatch_manifest_path()
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            logger.warning("RateWatch manifest unreadable; starting fresh")
    return {}


def save_manifest(manifest: dict) -> None:
    path = get_ratewatch_manifest_path()
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def _atomic_copy(src: Path, dst: Path) -> None:
    """Stream-copy src -> dst.tmp then rename."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    with open(src, "rb") as fin, open(tmp, "wb") as fout:
        shutil.copyfileobj(fin, fout, length=CHUNK_SIZE)
    tmp.replace(dst)


def _atomic_extract(zf: zipfile.ZipFile, member: str, dst: Path) -> None:
    """Stream-extract a zip member to dst.tmp then rename."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    with zf.open(member, "r") as fin, open(tmp, "wb") as fout:
        shutil.copyfileobj(fin, fout, length=CHUNK_SIZE)
    tmp.replace(dst)


def stage_support_files(force: bool = False) -> None:
    """Copy the four support tables to raw/support/."""
    manifest = load_manifest()
    support_entries = manifest.setdefault("support", {})

    for name in SUPPORT_FILES:
        src = support_source_path(name)
        if not src.exists():
            logger.warning("Support file missing: %s", src)
            continue

        dst = get_ratewatch_support_path() / name
        src_size = src.stat().st_size
        entry = support_entries.get(name, {})

        if (
            not force
            and dst.exists()
            and entry.get("size") == src_size
            and entry.get("dst_size") == dst.stat().st_size
        ):
            logger.info("[support] %s up-to-date (%d bytes)", name, src_size)
            continue

        logger.info("[support] Copying %s (%.1f MB)", name, src_size / 1e6)
        _atomic_copy(src, dst)
        support_entries[name] = {
            "src": str(src),
            "size": src_size,
            "dst_size": dst.stat().st_size,
            "sha256": _sha256(dst),
            "staged_at": datetime.now(timezone.utc).isoformat(),
        }
        save_manifest(manifest)


def download_year(year: int, force: bool = False) -> Optional[Path]:
    """Stage the raw rate-data text file for `year` into raw/{year}/."""
    raw_dir = get_ratewatch_raw_path(year)
    fname = raw_source_filename(year)
    dst = raw_dir / fname

    manifest = load_manifest()
    year_entries = manifest.setdefault("years", {})
    entry = year_entries.get(str(year), {})

    if year in UNZIPPED_YEARS or year in LSU_YEARS:
        src = raw_source_path(year)
        if not src.exists():
            logger.warning("[%d] Source file missing: %s", year, src)
            return None
        src_size = src.stat().st_size

        if (
            not force
            and dst.exists()
            and entry.get("src_size") == src_size
            and entry.get("dst_size") == dst.stat().st_size
        ):
            logger.info("[%d] Manifest current (%.1f MB); skipping", year, src_size / 1e6)
            return dst

        logger.info("[%d] Copying %s (%.1f GB) from %s", year, fname, src_size / 1e9, src.parent)
        _atomic_copy(src, dst)

    elif year in ZIPPED_YEARS:
        if not ZIP_FILE_2021.exists():
            logger.error("[%d] Master zip missing: %s", year, ZIP_FILE_2021)
            return None

        with zipfile.ZipFile(ZIP_FILE_2021, "r") as zf:
            try:
                info = zf.getinfo(fname)
            except KeyError:
                logger.error("[%d] %s not in zip", year, fname)
                return None

            if (
                not force
                and dst.exists()
                and entry.get("zip_uncompressed_size") == info.file_size
                and entry.get("dst_size") == dst.stat().st_size
            ):
                logger.info("[%d] Manifest current (%.1f GB); skipping",
                            year, info.file_size / 1e9)
                return dst

            logger.info("[%d] Extracting %s from zip (%.1f GB uncompressed)",
                        year, fname, info.file_size / 1e9)
            _atomic_extract(zf, fname, dst)

    else:
        logger.error("[%d] No source mapping", year)
        return None

    dst_size = dst.stat().st_size
    src_size_val: Optional[int] = None
    zip_size_val: Optional[int] = None
    if year in UNZIPPED_YEARS or year in LSU_YEARS:
        src_size_val = raw_source_path(year).stat().st_size
    else:
        with zipfile.ZipFile(ZIP_FILE_2021, "r") as zf:
            zip_size_val = zf.getinfo(fname).file_size

    year_entries[str(year)] = {
        "filename": fname,
        "src_size": src_size_val,
        "zip_uncompressed_size": zip_size_val,
        "dst_size": dst_size,
        "sha256": _sha256(dst),
        "staged_at": datetime.now(timezone.utc).isoformat(),
    }

    save_manifest(manifest)
    logger.info("[%d] Staged -> %s (%.1f GB)", year, dst, dst_size / 1e9)
    return dst


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Stage RateWatch raw text files from D:\\.")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--year", type=int, help=f"Single year ({FIRST_YEAR}-{LAST_YEAR})")
    g.add_argument("--all", action="store_true", help="All years")
    p.add_argument("--force", action="store_true", help="Re-stage even if manifest current")
    p.add_argument("--skip-support", action="store_true", help="Skip staging support tables")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    if not args.skip_support:
        stage_support_files(force=args.force)

    years = ALL_YEARS if args.all else [args.year or LAST_YEAR]
    for y in years:
        try:
            download_year(y, force=args.force)
        except Exception as exc:
            logger.exception("[%d] Stage failed: %s", y, exc)

    logger.info("RateWatch download/stage step complete.")


if __name__ == "__main__":
    main()
