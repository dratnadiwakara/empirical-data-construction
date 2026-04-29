"""
download.py — Y-9C raw-asset manager.

The FFIEC NIC FinancialDataDownload page is an interactive form with no public
API, so quarterly ZIPs are placed **manually** by the user in raw/. This script:

  --scan    Enumerate raw/*.zip, parse (year, quarter) from filenames, and
            write/update the manifest.
  --status  Print a table of quarters: raw ZIP present / extracted / loaded.

Manifest: C:\\empirical-data-construction\\y9c\\download_manifest.json

Usage:
    C:\\envs\\.basic_venv\\Scripts\\python.exe y9c/download.py --scan
    C:\\envs\\.basic_venv\\Scripts\\python.exe y9c/download.py --status
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import sys
from pathlib import Path

# ── Path setup ───────────────────────────────────────────────────────────────

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

_meta_spec = importlib.util.spec_from_file_location(
    "y9c_metadata", _HERE / "metadata.py"
)
_meta = importlib.util.module_from_spec(_meta_spec)
_meta_spec.loader.exec_module(_meta)  # type: ignore[attr-defined]

from config import (
    get_y9c_duckdb_path,
    get_y9c_manifest_path,
    get_y9c_storage_path,
)
from utils.logging_utils import get_logger

logger = get_logger(__name__)


# ── Manifest utilities ───────────────────────────────────────────────────────

def _read_manifest() -> dict:
    p = get_y9c_manifest_path()
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("manifest read failed (%s) — starting fresh", exc)
        return {}


def _write_manifest(manifest: dict) -> None:
    p = get_y9c_manifest_path()
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


# ── Raw-ZIP scanning ─────────────────────────────────────────────────────────

def scan_raw(compute_sha: bool = False) -> dict:
    """
    Walk raw/ for Y-9C bulk ZIPs, update the manifest with file metadata,
    and return the updated manifest.
    """
    raw_root = get_y9c_storage_path("raw")
    manifest = _read_manifest()
    zips_entry = manifest.setdefault("zips", {})

    found = 0
    # Match both lowercase and uppercase .zip extension on Windows-cased filesystems
    for zip_path in sorted(p for p in raw_root.iterdir()
                           if p.is_file() and p.suffix.lower() == ".zip"):
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

        entry["extract_status"] = (
            prior.get("extract_status", "not_extracted") if unchanged else "not_extracted"
        )

        zips_entry[key] = entry
        found += 1

    _write_manifest(manifest)
    logger.info("scanned %d zips; manifest -> %s", found, get_y9c_manifest_path())
    return manifest


# ── Status reporting ─────────────────────────────────────────────────────────

_MANUAL_DOWNLOAD_INSTRUCTIONS = """
No Y-9C ZIPs found in raw/.

Manual download procedure:
  1. Go to https://www.ffiec.gov/npw/FinancialReport/FinancialDataDownload
  2. Choose a year via the year selector (or use ?selectedyear=YYYY URL param)
  3. Download each quarterly Y-9C bulk ZIP (BHCF{YYYYMMDD}.ZIP)
  4. Drop the ZIPs unchanged into:
       {raw}

Then run:
  python y9c/download.py --scan
"""


def _render_status() -> str:
    manifest = _read_manifest()
    zips_entry = manifest.get("zips", {})
    if not zips_entry:
        return _MANUAL_DOWNLOAD_INSTRUCTIONS.format(raw=get_y9c_storage_path("raw"))

    loaded: set[tuple[int, int]] = set()
    db_path = get_y9c_duckdb_path()
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
        f"Y-9C raw/ inventory ({len(zips_entry)} quarterly zips)",
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

    # Coverage gap report
    placed = {(e["year"], e["quarter"]) for e in zips_entry.values()}
    if placed:
        ys = sorted({y for y, _ in placed})
        all_q = [(y, q) for y in range(min(ys), max(ys) + 1) for q in (1, 2, 3, 4)]
        gaps = [(y, q) for y, q in all_q if (y, q) not in placed]
        if gaps:
            lines.append("")
            lines.append(f"Missing quarters in [{min(ys)}Q1..{max(ys)}Q4]: "
                         + ", ".join(f"{y}Q{q}" for y, q in gaps))

    lines.append("")
    lines.append(f"Manifest: {get_y9c_manifest_path()}")
    lines.append(f"DuckDB  : {db_path}")
    return "\n".join(lines)


# ── CLI ──────────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(description="Y-9C raw asset manager")
    ap.add_argument("--scan", action="store_true",
                    help="scan raw/ and update manifest")
    ap.add_argument("--status", action="store_true",
                    help="print status table of discovered/extracted/loaded quarters")
    ap.add_argument("--sha256", action="store_true",
                    help="with --scan, also compute sha256 for each zip (slow)")

    args = ap.parse_args()

    if not any((args.scan, args.status)):
        ap.print_help()
        return 1

    if args.scan:
        scan_raw(compute_sha=args.sha256)
    if args.status:
        print(_render_status())
    return 0


if __name__ == "__main__":
    sys.exit(main())
