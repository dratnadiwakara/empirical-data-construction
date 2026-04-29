"""
inspect.py — Y-9C validation and sanity checks.

Run automatically from `construct.py --inspect`. Reports row counts, NULL
rates on key columns, and economic plausibility (system asset aggregate vs
FFIEC subsidiary system).
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import duckdb

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from config import (
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_THREADS,
    get_ffiec_duckdb_path,
    get_y9c_duckdb_path,
)
from utils.logging_utils import get_logger

logger = get_logger(__name__)


def _connect() -> duckdb.DuckDBPyConnection:
    db = get_y9c_duckdb_path()
    if not db.exists():
        raise SystemExit(f"Y-9C DuckDB not found at {db} — build first.")
    con = duckdb.connect(str(db), read_only=True)
    con.execute(f"PRAGMA threads={DUCKDB_THREADS}")
    con.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    return con


def check_row_counts(con: duckdb.DuckDBPyConnection) -> int:
    rows = con.execute("""
        SELECT activity_year, activity_quarter, COUNT(*) AS n_filers
        FROM bs_panel_y9c
        GROUP BY 1, 2
        ORDER BY 1, 2
    """).fetchall()
    print(f"\nRow counts ({len(rows)} quarters):")
    failures = 0
    for y, q, n in rows:
        flag = ""
        if n < 100:
            flag = "  <-- WARN: low filer count"
            failures += 1
        elif n > 5000:
            flag = "  <-- WARN: unexpectedly high"
            failures += 1
        print(f"  {y}Q{q}: {n:>6} filers{flag}")
    return failures


def check_null_rates(con: duckdb.DuckDBPyConnection) -> int:
    failures = 0
    print("\nNULL rates on key columns (should be ~0%):")
    res = con.execute("""
        SELECT
            AVG(CASE WHEN id_rssd IS NULL THEN 1.0 ELSE 0.0 END) AS null_id_rssd,
            AVG(CASE WHEN assets   IS NULL THEN 1.0 ELSE 0.0 END) AS null_assets,
            AVG(CASE WHEN equity   IS NULL THEN 1.0 ELSE 0.0 END) AS null_equity,
            COUNT(*) AS n
        FROM bs_panel_y9c
    """).fetchone()
    null_id, null_assets, null_equity, n = res
    print(f"  total rows: {n}")
    print(f"  id_rssd null: {null_id*100:.2f}%")
    print(f"  assets null:  {null_assets*100:.2f}%")
    print(f"  equity null:  {null_equity*100:.2f}%")
    if null_id > 0.0001:
        print("  WARN: id_rssd should be 0% null")
        failures += 1
    if null_assets > 0.05:
        print("  WARN: assets nullity > 5% — investigate prefix coverage")
        failures += 1
    return failures


def check_plausibility(con: duckdb.DuckDBPyConnection) -> int:
    """BHC consolidated assets should exceed subsidiary-bank total per quarter
    (consolidated = banks + nonbank subs + holding-company assets)."""
    failures = 0
    rows = con.execute("""
        SELECT activity_year, activity_quarter,
               COUNT(*) AS n_bhcs,
               SUM(assets) / 1e9 AS total_assets_tn
        FROM bs_panel_y9c
        WHERE activity_quarter = 4
        GROUP BY 1, 2
        ORDER BY 1
    """).fetchall()
    print("\nSystem aggregate (Q4 snapshots):")
    for y, q, n, tn in rows:
        print(f"  {y}Q{q}: {n} BHCs, ${tn:.2f} trillion total assets")
        if tn is not None and (tn < 0.5 or tn > 80):
            print(f"  WARN: implausible system total {tn:.1f}T for {y}Q{q}")
            failures += 1

    # Cross-check vs FFIEC subsidiary system if available
    ffiec_path = get_ffiec_duckdb_path()
    if ffiec_path.exists():
        try:
            con.execute(f"ATTACH '{str(ffiec_path).replace(chr(92), '/')}' "
                        f"AS ffiec (READ_ONLY)")
            comp = con.execute("""
                WITH y AS (
                    SELECT activity_year, SUM(assets)/1e9 AS y9c_tn
                    FROM bs_panel_y9c WHERE activity_quarter = 4
                    GROUP BY 1
                ),
                f AS (
                    SELECT activity_year, SUM(assets)/1e9 AS ffiec_tn
                    FROM ffiec.bs_panel WHERE activity_quarter = 4
                    GROUP BY 1
                )
                SELECT y.activity_year, y9c_tn, ffiec_tn, y9c_tn / ffiec_tn AS ratio
                FROM y JOIN f USING (activity_year)
                ORDER BY 1
            """).fetchall()
            print("\nY-9C vs FFIEC subsidiary system ratio (Y-9C/FFIEC):")
            for y, ytn, ftn, r in comp:
                flag = ""
                if r is not None and (r < 0.5 or r > 1.6):
                    flag = "  <-- WARN: ratio outside [0.5, 1.6]"
                    failures += 1
                print(f"  {y}: BHC={ytn:.2f}T  Subs={ftn:.2f}T  ratio={r:.2f}{flag}")
        except Exception as exc:
            print(f"  (skipping FFIEC cross-check: {exc})")
    return failures


def run_all_checks() -> int:
    con = _connect()
    try:
        f1 = check_row_counts(con)
        f2 = check_null_rates(con)
        f3 = check_plausibility(con)
    finally:
        con.close()
    total = f1 + f2 + f3
    print(f"\nTotal warnings: {total}")
    return total


if __name__ == "__main__":
    sys.exit(0 if run_all_checks() == 0 else 1)
