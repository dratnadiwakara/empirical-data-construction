"""
download.py — FFIEC Call Reports raw-asset manager.

The FFIEC CDR bulk download portal is an ASP.NET form with no public API, so
the quarterly ZIPs are placed **manually** by the user in raw/. This script:

  --mdrm    Download the MDRM dictionary ZIP from federalreserve.gov and
            extract the CSV next to it.
  --scan    Enumerate raw/*.zip, parse (year, quarter) from filenames, and
            write/update the manifest (size, mtime, sha256 lazy).
  --status  Print a table of quarters: raw ZIP present / extracted / loaded.

Manifest: C:\\empirical-data-construction\\call-reports-FFIEC\\download_manifest.json

Manual download instructions printed by --status on first run (when raw is
empty). See README.md for the canonical procedure.

Usage:
    C:\\envs\\.basic_venv\\Scripts\\python.exe call-reports-FFIEC/download.py --mdrm
    C:\\envs\\.basic_venv\\Scripts\\python.exe call-reports-FFIEC/download.py --scan
    C:\\envs\\.basic_venv\\Scripts\\python.exe call-reports-FFIEC/download.py --status
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import shutil
import sys
import time
import zipfile
from pathlib import Path

import httpx

# ── Path setup (hyphen in package name prevents `python -m` imports) ─────────

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
    FFIEC_USER_AGENT,
    HTTP_BACKOFF_BASE,
    HTTP_CHUNK_SIZE,
    HTTP_RETRIES,
    HTTP_TIMEOUT,
    get_ffiec_duckdb_path,
    get_ffiec_manifest_path,
    get_ffiec_mdrm_path,
    get_ffiec_raw_path,
    get_ffiec_storage_path,
)
from utils.logging_utils import get_logger

logger = get_logger(__name__)


# ── Manifest utilities ───────────────────────────────────────────────────────

def _read_manifest() -> dict:
    p = get_ffiec_manifest_path()
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("manifest read failed (%s) — starting fresh", exc)
        return {}


def _write_manifest(manifest: dict) -> None:
    p = get_ffiec_manifest_path()
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, p)


def _sha256_file(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            buf = fh.read(chunk)
            if not buf:
                break
            h.update(buf)
    return h.hexdigest()


# ── MDRM dictionary download ─────────────────────────────────────────────────

def download_mdrm(force: bool = False) -> Path:
    """Download MDRM.zip, extract, return path to the resulting CSV."""
    mdrm_dir = get_ffiec_mdrm_path()
    zip_path = mdrm_dir / "MDRM.zip"
    csv_path = mdrm_dir / "MDRM.csv"

    if csv_path.exists() and not force:
        logger.info("MDRM already present at %s", csv_path)
        return csv_path

    logger.info("downloading MDRM dictionary from %s", _meta.MDRM_URL)
    for attempt in range(HTTP_RETRIES):
        try:
            with httpx.stream(
                "GET",
                _meta.MDRM_URL,
                headers={"User-Agent": FFIEC_USER_AGENT},
                timeout=HTTP_TIMEOUT,
                follow_redirects=True,
            ) as resp:
                resp.raise_for_status()
                tmp = zip_path.with_suffix(".tmp")
                with tmp.open("wb") as fh:
                    for chunk in resp.iter_bytes(HTTP_CHUNK_SIZE):
                        fh.write(chunk)
                os.replace(tmp, zip_path)
            break
        except (httpx.HTTPError, OSError) as exc:
            if attempt < HTTP_RETRIES - 1:
                sleep = min(HTTP_BACKOFF_BASE ** attempt, 120)
                logger.warning("MDRM download failed (attempt %d/%d): %s — retrying in %.1fs",
                               attempt + 1, HTTP_RETRIES, exc, sleep)
                time.sleep(sleep)
            else:
                raise

    logger.info("extracting %s", zip_path)
    with zipfile.ZipFile(zip_path) as z:
        csv_members = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if not csv_members:
            raise RuntimeError(f"no CSV inside MDRM zip: {z.namelist()}")
        chosen = max(csv_members, key=lambda n: z.getinfo(n).file_size)
        with z.open(chosen) as src, csv_path.open("wb") as dst:
            shutil.copyfileobj(src, dst)

    logger.info("MDRM.csv extracted (%d bytes)", csv_path.stat().st_size)
    return csv_path


# ── Raw-ZIP scanning ─────────────────────────────────────────────────────────

def scan_raw(compute_sha: bool = False) -> dict:
    """
    Walk raw/ for FFIEC Call Bulk ZIPs, update the manifest with file metadata,
    and return the updated manifest.
    """
    raw_root = get_ffiec_storage_path("raw")
    manifest = _read_manifest()
    zips_entry = manifest.setdefault("zips", {})

    found = 0
    for zip_path in sorted(raw_root.glob("*.zip")):
        parsed = _meta.parse_zip_filename(zip_path.name)
        if parsed is None:
            logger.warning("skipping unrecognized ZIP name: %s", zip_path.name)
            continue
        year, quarter = parsed
        key = f"{year}Q{quarter}"
        stat = zip_path.stat()
        prior = zips_entry.get(key, {})

        entry = {
            "filename": zip_path.name,
            "year": year,
            "quarter": quarter,
            "size": stat.st_size,
            "mtime": stat.st_mtime,
            "path": str(zip_path),
        }
        unchanged = (
            prior.get("size") == entry["size"]
            and prior.get("mtime") == entry["mtime"]
        )
        entry["sha256"] = prior.get("sha256") if unchanged else None
        if compute_sha and entry["sha256"] is None:
            logger.info("sha256: %s", zip_path.name)
            entry["sha256"] = _sha256_file(zip_path)

        # Preserve extract status from prior scans
        entry["extract_status"] = prior.get("extract_status", "not_extracted") \
            if unchanged else "not_extracted"

        zips_entry[key] = entry
        found += 1

    _write_manifest(manifest)
    logger.info("scanned %d zips; manifest -> %s", found, get_ffiec_manifest_path())
    return manifest


# ── Status reporting ─────────────────────────────────────────────────────────

_MANUAL_DOWNLOAD_INSTRUCTIONS = """
No Call Report ZIPs found in raw/.

Manual download procedure:
  1. Go to {url}
  2. Select product: '{product}'
  3. Select reporting period: desired quarter-end (e.g., 12/31/2024)
  4. Select format: '{fmt}'
  5. Click 'Download' and save the ZIP as-is (do NOT rename or unzip) into:
       {raw}

The expected filename pattern is:
  'FFIEC CDR Call Bulk All Schedules MMDDYYYY.zip'

Then run:
  python call-reports-FFIEC/download.py --scan
"""


def _render_status() -> str:
    manifest = _read_manifest()
    zips_entry = manifest.get("zips", {})
    if not zips_entry:
        return _MANUAL_DOWNLOAD_INSTRUCTIONS.format(
            url=_meta.CDR_SOURCE_URL,
            product=_meta.CDR_PRODUCT,
            fmt=_meta.CDR_FORMAT,
            raw=get_ffiec_storage_path("raw"),
        )

    # Pull loaded quarters from DuckDB if present
    loaded: set[tuple[int, int]] = set()
    db_path = get_ffiec_duckdb_path()
    if db_path.exists():
        try:
            import duckdb
            con = duckdb.connect(str(db_path), read_only=True)
            rows = con.execute(
                "SELECT DISTINCT year, quarter FROM panel_metadata"
            ).fetchall()
            loaded = {(int(y), int(q)) for y, q in rows}
            con.close()
        except Exception as exc:
            logger.warning("could not query panel_metadata: %s", exc)

    lines = [
        f"FFIEC Call Reports raw/ inventory ({len(zips_entry)} quarterly zips)",
        "",
        f"{'Quarter':<10}{'Size (MB)':>12}{'Extracted':>14}{'Loaded':>10}",
        "-" * 46,
    ]
    for key in sorted(zips_entry.keys(), key=lambda k: (int(k[:4]), int(k[5:]))):
        e = zips_entry[key]
        yr, q = e["year"], e["quarter"]
        size_mb = e["size"] / (1024 * 1024)
        extracted = "yes" if e.get("extract_status") == "extracted" else "no"
        is_loaded = "yes" if (yr, q) in loaded else "no"
        lines.append(
            f"{key:<10}{size_mb:>12.1f}{extracted:>14}{is_loaded:>10}"
        )

    lines.append("")
    lines.append(f"Manifest: {get_ffiec_manifest_path()}")
    lines.append(f"DuckDB  : {db_path}")
    return "\n".join(lines)


# ── CLI ──────────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(description="FFIEC Call Reports raw asset manager")
    ap.add_argument("--mdrm", action="store_true",
                    help="download + extract MDRM dictionary")
    ap.add_argument("--scan", action="store_true",
                    help="scan raw/ and update manifest")
    ap.add_argument("--status", action="store_true",
                    help="print status table of discovered/extracted/loaded quarters")
    ap.add_argument("--force", action="store_true",
                    help="force re-download of MDRM")
    ap.add_argument("--sha256", action="store_true",
                    help="with --scan, also compute sha256 for each zip (slow)")

    args = ap.parse_args()

    if not any((args.mdrm, args.scan, args.status)):
        ap.print_help()
        return 1

    if args.mdrm:
        download_mdrm(force=args.force)
    if args.scan:
        scan_raw(compute_sha=args.sha256)
    if args.status:
        print(_render_status())
    return 0


if __name__ == "__main__":
    sys.exit(main())
