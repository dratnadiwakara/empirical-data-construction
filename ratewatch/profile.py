"""
Profile a year of RateWatch raw data: enumerate (product, productdescription)
tiers and decide which to retain.

Retention rule (per (PRD_TYP_JOIN, PRODUCTDESCRIPTION) pair in year):
  KEEP if either:
    - (product, description) is listed in MANDATORY_TIER_DESCRIPTIONS, OR
    - distinct branch count / total ratesetters in year >= MIN_TIER_SHARE

The dropped "single dominant tier" rule is gone. Multiple tiers per product
are allowed and expected.

Writes ratewatch/product_registry.json grouped by year, then by product, with
list of retained tier_descriptions and per-tier branch / row counts.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import DUCKDB_MEMORY_LIMIT, DUCKDB_THREADS, get_ratewatch_raw_path
from ratewatch.metadata import (
    ALL_YEARS,
    FIRST_YEAR,
    LAST_YEAR,
    LSU_YEARS,
    MANDATORY_TIER_DESCRIPTIONS,
    MIN_TIER_SHARE,
    UNZIPPED_YEARS,
    era_for_year,
    load_product_registry,
    raw_source_filename,
    raw_source_path,
    save_product_registry,
)
from utils.logging_utils import get_logger

logger = get_logger(__name__)


def _sql_path(p: Path) -> str:
    return str(p).replace("\\", "/")


def _connect() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    conn.execute(f"PRAGMA threads={DUCKDB_THREADS}")
    conn.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    return conn


def profile_year(year: int, min_share: float = MIN_TIER_SHARE) -> dict:
    """Return registry entry for one year."""
    raw = get_ratewatch_raw_path(year) / raw_source_filename(year)
    if not raw.exists():
        if year in UNZIPPED_YEARS or year in LSU_YEARS:
            try:
                src = raw_source_path(year)
            except ValueError:
                src = None
            if src and src.exists():
                logger.info("[%d] Reading source directly: %s", year, src)
                raw = src
            else:
                raise FileNotFoundError(
                    f"Raw missing local ({raw}) and source ({src})"
                )
        else:
            raise FileNotFoundError(f"Raw missing: {raw}. Run download.py first.")

    conn = _connect()
    try:
        conn.execute(f"""
            CREATE TEMP VIEW src AS
            SELECT *
            FROM read_csv(
                '{_sql_path(raw)}',
                delim='|', header=true, all_varchar=true,
                ignore_errors=true
            )
        """)

        total_branches: int = conn.execute(
            "SELECT COUNT(DISTINCT ACCOUNTNUMBER) FROM src"
        ).fetchone()[0]
        logger.info("[%d] Distinct ratesetters: %d", year, total_branches)

        # Tier-level coverage: per (PRD_TYP_JOIN, PRODUCTDESCRIPTION)
        tiers = conn.execute("""
            SELECT
                PRD_TYP_JOIN,
                PRODUCTDESCRIPTION,
                COUNT(DISTINCT ACCOUNTNUMBER) AS n_branches,
                COUNT(*)                      AS n_rows
            FROM src
            WHERE PRD_TYP_JOIN IS NOT NULL AND PRD_TYP_JOIN <> ''
              AND PRODUCTDESCRIPTION IS NOT NULL
            GROUP BY 1, 2
        """).fetchall()

        # Group by product
        per_product: dict[str, list[dict]] = {}
        for prd, desc, n_br, n_rows in tiers:
            share = n_br / total_branches if total_branches else 0.0
            mandatory = (
                prd in MANDATORY_TIER_DESCRIPTIONS
                and desc in MANDATORY_TIER_DESCRIPTIONS[prd]
            )
            if not mandatory and share < min_share:
                continue
            per_product.setdefault(prd, []).append({
                "productdescription": desc,
                "n_branches": int(n_br),
                "n_rows": int(n_rows),
                "branch_share": round(share, 4),
                "mandatory": mandatory,
            })

        # Force-add mandatory tiers that didn't even appear in raw — surface as
        # zero-coverage entries so consumers see they were intended.
        for prd, mand_list in MANDATORY_TIER_DESCRIPTIONS.items():
            present = {t["productdescription"] for t in per_product.get(prd, [])}
            for desc in mand_list:
                if desc not in present:
                    per_product.setdefault(prd, []).append({
                        "productdescription": desc,
                        "n_branches": 0,
                        "n_rows": 0,
                        "branch_share": 0.0,
                        "mandatory": True,
                        "absent": True,
                    })

        kept: list[dict] = []
        for prd, tier_entries in per_product.items():
            tier_entries.sort(key=lambda t: t["n_rows"], reverse=True)
            tier_descriptions = [t["productdescription"] for t in tier_entries]
            kept.append({
                "prd_typ_join": prd,
                "era": era_for_year(year),
                "n_tiers": len(tier_descriptions),
                "tier_descriptions": tier_descriptions,
                "tiers": tier_entries,
            })
        kept.sort(key=lambda p: sum(t["n_rows"] for t in p["tiers"]), reverse=True)

        logger.info("[%d] Kept %d products with %d tier-entries (>=%.0f%% or mandatory)",
                    year, len(kept), sum(p["n_tiers"] for p in kept), min_share * 100)
        for p in kept:
            descs = ", ".join(
                f"{t['productdescription']}{'*' if t.get('mandatory') else ''}"
                f"({t['branch_share']:.2f})"
                for t in p["tiers"]
            )
            logger.info("  %-10s %s", p["prd_typ_join"], descs)

        return {
            "total_branches": int(total_branches),
            "min_tier_share": min_share,
            "kept": kept,
        }
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Profile RateWatch tier retention per year.")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--year", type=int, help=f"Single year ({FIRST_YEAR}-{LAST_YEAR})")
    g.add_argument("--all", action="store_true", help="All years")
    p.add_argument("--min-share", type=float, default=MIN_TIER_SHARE,
                   help=f"Per-tier branch coverage threshold (default {MIN_TIER_SHARE})")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    years = ALL_YEARS if args.all else [args.year]

    registry = load_product_registry()
    for y in years:
        registry[str(y)] = profile_year(y, min_share=args.min_share)

    save_product_registry(registry)
    logger.info("Wrote product registry for years: %s", sorted(registry.keys()))


if __name__ == "__main__":
    main()
