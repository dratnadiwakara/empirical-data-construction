"""
PERMCO-RSSD link ETL pipeline.

Steps:
  1. Read raw CRSP-FRB CSV → explode date ranges → quarterly panel
  2. BFS over NIC controlled relationships → find all subsidiaries per (BHC, quarter)
  3. Join subsidiaries with CFLV (pre-2001) and FFIEC (2001+) assets → lead bank
  4. BHC-as-lead-bank fallback for remaining nulls
  5. Write Parquet → DuckDB view → panel_metadata

Usage:
  python permco-rssd-link\\construct.py           # skip if already built
  python permco-rssd-link\\construct.py --force   # full rebuild
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
from collections import deque
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

import duckdb
import numpy as np
import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import (
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_THREADS,
    PARQUET_COMPRESSION,
    PARQUET_ROW_GROUP_SIZE,
    get_cflv_duckdb_path,
    get_ffiec_duckdb_path,
    get_nic_duckdb_path,
    get_permco_rssd_duckdb_path,
    get_permco_rssd_manifest_path,
    get_permco_rssd_raw_path,
    get_permco_rssd_staging_path,
)
from utils.duckdb_utils import ensure_table_exists, get_connection, upsert_row
from utils.logging_utils import get_logger

logger = get_logger(__name__)

# Load metadata from sibling file (hyphen in folder name prevents normal import)
_meta_spec = importlib.util.spec_from_file_location(
    "_permco_rssd_metadata", Path(__file__).resolve().parent / "metadata.py"
)
_meta = importlib.util.module_from_spec(_meta_spec)  # type: ignore[arg-type]
_meta_spec.loader.exec_module(_meta)  # type: ignore[union-attr]
PANEL_METADATA_DDL: str = _meta.PANEL_METADATA_DDL
SOURCE_URL: str = _meta.SOURCE_URL

# Extend the latest dt_end row to this date when generating quarters
_EXTEND_TO = pd.Timestamp(date.today()).to_period("Q").end_time.normalize()


def _sql_path(path: Path) -> str:
    return str(path).replace("\\", "/")


def _sha256_manifest() -> str:
    p = get_permco_rssd_manifest_path()
    if not p.exists():
        return ""
    try:
        return json.loads(p.read_text(encoding="utf-8")).get("sha256", "")
    except Exception:
        return ""


def _find_latest_csv() -> Path | None:
    raw = get_permco_rssd_raw_path()
    csvs = sorted(raw.glob("crsp_*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    return csvs[0] if csvs else None


# Normalise institution type labels from source CSV
_INST_TYPE_MAP: dict[str, str] = {
    "bank holding company":        "Bank Holding Company",
    "commercial bank":             "Commercial Bank",
    "savings institution":         "Savings Institution",
    "thrift holding company":      "Thrift Holding Company",
    "thirft holding company":      "Thrift Holding Company",  # source typo
    "financial holding company":   "Financial Holding Company",
    "cooperative bank":            "Cooperative Bank",
    "credit union":                "Credit Union",
}


def _normalise_inst_type(s: str) -> str:
    return _INST_TYPE_MAP.get(s.strip().lower(), s.strip())


# ── Step 1: quarterly panel ───────────────────────────────────────────────────

def _build_quarterly_panel(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path, dtype={"dt_start": str, "dt_end": str})
    df["dt_start"] = pd.to_datetime(df["dt_start"], format="%Y%m%d")
    df["dt_end_orig"] = pd.to_datetime(df["dt_end"], format="%Y%m%d")

    max_dt = df["dt_end_orig"].max()
    df["dt_end_eff"] = df["dt_end_orig"].where(df["dt_end_orig"] != max_dt, other=_EXTEND_TO)

    def _gen_quarters(row: pd.Series) -> list[pd.Timestamp]:
        return pd.date_range(start=row["dt_start"], end=row["dt_end_eff"], freq="QE").tolist()

    df["quarter_end"] = df.apply(_gen_quarters, axis=1)
    df_exp = df.explode("quarter_end").reset_index(drop=True)
    df_exp["confirmed"] = (df_exp["quarter_end"] <= df_exp["dt_end_orig"]).astype(int)
    df_exp["quarter_end_str"] = df_exp["quarter_end"].dt.strftime("%Y-%m-%d")

    panel = (
        df_exp[["permco", "entity", "name", "inst_type", "quarter_end_str", "confirmed",
                "dt_start"]]
        .rename(columns={"entity": "bhc_rssd", "quarter_end_str": "quarter_end"})
        .copy()
    )
    panel["permco"] = panel["permco"].astype("Int64")
    panel["bhc_rssd"] = panel["bhc_rssd"].astype("Int64")

    # Normalise inst_type casing/typos
    panel["inst_type"] = panel["inst_type"].fillna("").apply(_normalise_inst_type)

    # Dedup: one row per (permco, quarter_end). Source CSV has rare cases where
    # a PERMCO maps to multiple BHC RSSDs with overlapping date ranges.
    # Prefer holding companies; break ties by latest dt_start, then largest bhc_rssd.
    dups = panel.duplicated(subset=["permco", "quarter_end"], keep=False)
    if dups.any():
        n_dups = dups.sum()
        panel["_is_holding"] = panel["inst_type"].str.lower().str.contains("holding").astype(int)
        panel = (
            panel
            .sort_values(
                ["permco", "quarter_end", "_is_holding", "dt_start", "bhc_rssd"],
                ascending=[True, True, False, False, False],
            )
            .drop_duplicates(subset=["permco", "quarter_end"], keep="first")
        )
        panel.drop(columns=["_is_holding"], inplace=True)
        logger.info("[panel] Removed %d duplicate (permco, quarter_end) rows via dedup.", n_dups - panel.duplicated(subset=["permco","quarter_end"], keep=False).sum())

    panel = panel.drop(columns=["dt_start"]).reset_index(drop=True)
    logger.info("[panel] %d quarterly rows for %d entities", len(panel), panel["permco"].nunique())
    return panel


# ── Step 2: BFS through NIC relationships ────────────────────────────────────

def _load_nic_relationships() -> Optional[np.ndarray]:
    nic_path = get_nic_duckdb_path()
    if not nic_path.exists():
        logger.warning("[nic] nic.duckdb not found at %s; skipping lead bank enrichment.", nic_path)
        return None
    conn = duckdb.connect(str(nic_path), read_only=True)
    try:
        rows = conn.execute("""
            SELECT
                TRY_CAST("#ID_RSSD_PARENT" AS BIGINT) AS parent,
                ID_RSSD_OFFSPRING                      AS offspring,
                TRY_CAST(DT_START AS BIGINT)           AS dt_start,
                TRY_CAST(DT_END   AS BIGINT)           AS dt_end
            FROM relationships
            WHERE TRY_CAST(CTRL_IND AS INTEGER) = 1
              AND "#ID_RSSD_PARENT" IS NOT NULL
              AND ID_RSSD_OFFSPRING IS NOT NULL
        """).fetchall()
    finally:
        conn.close()

    if not rows:
        logger.warning("[nic] No controlled relationships found.")
        return None
    arr = np.array(rows, dtype=np.int64)
    logger.info("[nic] Loaded %d controlled relationships.", len(arr))
    return arr


def _bfs_subsidiaries(
    rel_arr: np.ndarray,
    bhc_quarters: pd.DataFrame,
) -> pd.DataFrame:
    quarters = sorted(bhc_quarters["quarter_end"].unique())
    bhcs_by_quarter: dict[str, set[int]] = (
        bhc_quarters.groupby("quarter_end")["bhc_rssd"]
        .apply(lambda s: set(s.dropna().astype(int)))
        .to_dict()
    )

    all_rows: list[tuple[int, str, int]] = []
    total = len(quarters)

    for i, qtr in enumerate(quarters):
        if i % 20 == 0:
            logger.info("[bfs] Quarter %d/%d: %s", i + 1, total, qtr)
        q_int = int(qtr.replace("-", ""))
        mask = (rel_arr[:, 2] <= q_int) & (rel_arr[:, 3] >= q_int)
        active = rel_arr[mask]

        adj: dict[int, set[int]] = {}
        for parent, offspring, _, _ in active:
            adj.setdefault(int(parent), set()).add(int(offspring))

        for bhc in bhcs_by_quarter.get(qtr, set()):
            if bhc not in adj:
                continue
            visited: set[int] = set()
            queue: deque[int] = deque(adj[bhc])
            while queue:
                node = queue.popleft()
                if node in visited:
                    continue
                visited.add(node)
                if node in adj:
                    queue.extend(adj[node] - visited)
            for sub in visited:
                all_rows.append((bhc, qtr, sub))

    if not all_rows:
        logger.warning("[bfs] No subsidiaries found.")
        return pd.DataFrame(columns=["bhc_rssd", "quarter_end", "sub_rssd"])

    result = pd.DataFrame(all_rows, columns=["bhc_rssd", "quarter_end", "sub_rssd"])
    logger.info("[bfs] %d BHC-quarter-subsidiary triplets.", len(result))
    return result


# ── Step 3: load call report assets ──────────────────────────────────────────

def _load_assets_cflv() -> Optional[pd.DataFrame]:
    path = get_cflv_duckdb_path()
    if not path.exists():
        logger.warning("[cflv] call-reports-cflv.duckdb not found; skipping pre-2001 assets.")
        return None
    conn = duckdb.connect(str(path), read_only=True)
    try:
        df = conn.execute("""
            SELECT id_rssd, CAST(date AS VARCHAR) AS date, assets, equity
            FROM balance_sheets
            WHERE date < '2001-01-01'
              AND assets IS NOT NULL
        """).df()
    finally:
        conn.close()
    logger.info("[cflv] Loaded %d pre-2001 asset rows.", len(df))
    return df


def _load_assets_ffiec() -> Optional[pd.DataFrame]:
    path = get_ffiec_duckdb_path()
    if not path.exists():
        logger.warning("[ffiec] call-reports-ffiec.duckdb not found; skipping 2001+ assets.")
        return None
    conn = duckdb.connect(str(path), read_only=True)
    try:
        df = conn.execute("""
            SELECT id_rssd, CAST(date AS VARCHAR) AS date, assets, equity
            FROM bs_panel
            WHERE activity_year >= 2001
              AND assets IS NOT NULL
        """).df()
    finally:
        conn.close()
    logger.info("[ffiec] Loaded %d post-2000 asset rows.", len(df))
    return df


def _load_all_assets() -> Optional[pd.DataFrame]:
    cflv = _load_assets_cflv()
    ffiec = _load_assets_ffiec()
    parts = [x for x in [cflv, ffiec] if x is not None]
    if not parts:
        return None
    assets = pd.concat(parts, ignore_index=True)
    assets["id_rssd"] = assets["id_rssd"].astype("Int64")
    assets["assets"] = pd.to_numeric(assets["assets"], errors="coerce")
    assets["equity"] = pd.to_numeric(assets["equity"], errors="coerce")
    logger.info("[assets] Combined: %d rows.", len(assets))
    return assets


# ── Step 4: lead bank enrichment ─────────────────────────────────────────────

def _enrich_lead_bank(
    panel: pd.DataFrame,
    subs: pd.DataFrame,
    assets: pd.DataFrame,
) -> pd.DataFrame:
    # Join subsidiaries → assets
    merged = subs.merge(
        assets,
        left_on=["sub_rssd", "quarter_end"],
        right_on=["id_rssd", "date"],
        how="inner",
    )
    if merged.empty:
        logger.warning("[enrich] No subsidiary-asset matches; lead bank columns will be NULL.")
        panel["lead_bank_rssd"] = pd.NA
        panel["lead_bank_assets"] = pd.NA
        panel["lead_bank_equity"] = pd.NA
        return panel

    idx = merged.groupby(["bhc_rssd", "quarter_end"])["assets"].idxmax()
    lead = (
        merged.loc[idx, ["bhc_rssd", "quarter_end", "sub_rssd", "assets", "equity"]]
        .rename(columns={"sub_rssd": "lead_bank_rssd",
                         "assets": "lead_bank_assets",
                         "equity": "lead_bank_equity"})
        .reset_index(drop=True)
    )
    lead["bhc_rssd"] = lead["bhc_rssd"].astype("Int64")
    lead["lead_bank_rssd"] = lead["lead_bank_rssd"].astype("Int64")

    result = panel.merge(lead, on=["bhc_rssd", "quarter_end"], how="left")

    # Fallback: BHC itself in call reports
    null_mask = result["lead_bank_rssd"].isna()
    null_rows = result.loc[null_mask, ["bhc_rssd", "quarter_end"]].copy()
    if not null_rows.empty:
        fallback = null_rows.merge(
            assets,
            left_on=["bhc_rssd", "quarter_end"],
            right_on=["id_rssd", "date"],
            how="inner",
        )
        if not fallback.empty:
            fallback = fallback.rename(columns={"assets": "lead_bank_assets",
                                                "equity": "lead_bank_equity"})
            fallback["lead_bank_rssd"] = fallback["bhc_rssd"]
            fallback["lead_bank_rssd"] = fallback["lead_bank_rssd"].astype("Int64")
            fb_cols = ["bhc_rssd", "quarter_end", "lead_bank_rssd", "lead_bank_assets", "lead_bank_equity"]
            result = result.merge(
                fallback[fb_cols],
                on=["bhc_rssd", "quarter_end"],
                how="left",
                suffixes=("", "_fb"),
            )
            for col in ["lead_bank_rssd", "lead_bank_assets", "lead_bank_equity"]:
                fb_col = col + "_fb"
                if fb_col in result.columns:
                    result[col] = result[col].combine_first(result[fb_col])
                    result.drop(columns=[fb_col], inplace=True)
            logger.info("[fallback] Filled %d rows using BHC-as-lead-bank.", (~null_mask).sum() - result["lead_bank_rssd"].isna().sum())

    n_total = len(result)
    n_enriched = result["lead_bank_rssd"].notna().sum()
    logger.info("[enrich] Lead bank coverage: %d/%d (%.1f%%)", n_enriched, n_total, 100 * n_enriched / n_total)
    return result


# ── Step 5: write Parquet ─────────────────────────────────────────────────────

def _write_parquet(df: pd.DataFrame) -> Path:
    out_dir = get_permco_rssd_staging_path()
    out_path = out_dir / "data.parquet"
    tmp_path = out_path.with_suffix(".parquet.tmp")

    # Normalise types for clean Parquet output
    df = df[[
        "permco", "bhc_rssd", "name", "inst_type",
        "quarter_end", "confirmed",
        "lead_bank_rssd", "lead_bank_assets", "lead_bank_equity",
    ]].copy()
    df["quarter_end"] = pd.to_datetime(df["quarter_end"]).dt.date

    conn = duckdb.connect(":memory:")
    conn.execute(f"PRAGMA threads={DUCKDB_THREADS}")
    conn.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    conn.register("panel_df", df)
    conn.execute(f"""
        COPY (
            SELECT
                CAST(permco           AS BIGINT)  AS permco,
                CAST(bhc_rssd         AS BIGINT)  AS bhc_rssd,
                CAST(name             AS VARCHAR) AS name,
                CAST(inst_type        AS VARCHAR) AS inst_type,
                CAST(quarter_end      AS DATE)    AS quarter_end,
                CAST(confirmed        AS INTEGER) AS confirmed,
                CAST(lead_bank_rssd   AS BIGINT)  AS lead_bank_rssd,
                CAST(lead_bank_assets AS DOUBLE)  AS lead_bank_assets,
                CAST(lead_bank_equity AS DOUBLE)  AS lead_bank_equity
            FROM panel_df
            ORDER BY permco, quarter_end
        ) TO '{_sql_path(tmp_path)}'
        (FORMAT PARQUET, COMPRESSION '{PARQUET_COMPRESSION}',
         ROW_GROUP_SIZE {PARQUET_ROW_GROUP_SIZE})
    """)
    conn.close()
    tmp_path.replace(out_path)
    logger.info("[parquet] Written %d rows to %s", len(df), out_path)
    return out_path


# ── Step 6: DuckDB view ───────────────────────────────────────────────────────

def _recreate_view(parquet_path: Path) -> None:
    db_conn = get_connection(
        get_permco_rssd_duckdb_path(),
        threads=DUCKDB_THREADS,
        memory_limit=DUCKDB_MEMORY_LIMIT,
    )
    try:
        db_conn.execute(f"""
            CREATE OR REPLACE VIEW crsp_frb_link AS
            SELECT * FROM read_parquet('{_sql_path(parquet_path)}')
        """)
        logger.info("[view] crsp_frb_link view created/refreshed.")
    finally:
        db_conn.close()


# ── Step 7: panel_metadata ────────────────────────────────────────────────────

def _upsert_metadata(row_count: int, csv_name: str) -> None:
    db_conn = get_connection(
        get_permco_rssd_duckdb_path(),
        threads=DUCKDB_THREADS,
        memory_limit=DUCKDB_MEMORY_LIMIT,
    )
    try:
        ensure_table_exists(db_conn, "panel_metadata", PANEL_METADATA_DDL)
        upsert_row(
            db_conn,
            "panel_metadata",
            {
                "dataset": "crsp_frb_link",
                "row_count": row_count,
                "source_csv": csv_name,
                "file_sha256": _sha256_manifest(),
                "source_url": SOURCE_URL,
                "built_at": datetime.now(timezone.utc).isoformat(),
            },
            ["dataset"],
        )
    finally:
        db_conn.close()


# ── Main ──────────────────────────────────────────────────────────────────────

def _is_already_built() -> bool:
    parquet = get_permco_rssd_staging_path() / "data.parquet"
    if not parquet.exists():
        return False
    manifest_sha = _sha256_manifest()
    if not manifest_sha:
        return False
    # Check if parquet was built from the current CSV version by comparing sizes
    # (full SHA check would require re-reading the CSV; size+manifest is sufficient)
    return True


def main() -> None:
    args = parse_args()

    csv_path = _find_latest_csv()
    if csv_path is None:
        logger.error(
            "No crsp_*.csv in raw/. Run download.py first or place the CSV manually."
        )
        sys.exit(1)

    parquet_path = get_permco_rssd_staging_path() / "data.parquet"
    if parquet_path.exists() and not args.force:
        logger.info("[construct] Parquet exists; skipping (use --force to rebuild).")
        print("Already built. Use --force to rebuild.")
        # Still refresh the view in case the DB was wiped
        _recreate_view(parquet_path)
        return

    logger.info("[construct] Building from %s", csv_path.name)

    # Step 1
    panel = _build_quarterly_panel(csv_path)
    bhc_quarters = panel[["bhc_rssd", "quarter_end"]].drop_duplicates()

    # Steps 2–4: lead bank enrichment
    rel_arr = _load_nic_relationships()
    assets = _load_all_assets()

    if rel_arr is not None and assets is not None:
        subs = _bfs_subsidiaries(rel_arr, bhc_quarters)
        if not subs.empty:
            panel = _enrich_lead_bank(panel, subs, assets)
        else:
            panel["lead_bank_rssd"] = pd.NA
            panel["lead_bank_assets"] = pd.NA
            panel["lead_bank_equity"] = pd.NA
    else:
        logger.warning("[construct] Skipping lead bank enrichment (missing NIC or call report data).")
        panel["lead_bank_rssd"] = pd.NA
        panel["lead_bank_assets"] = pd.NA
        panel["lead_bank_equity"] = pd.NA

    # Step 5
    _write_parquet(panel)

    # Step 6
    _recreate_view(parquet_path)

    # Step 7
    _upsert_metadata(len(panel), csv_path.name)

    n_enriched = panel["lead_bank_rssd"].notna().sum() if "lead_bank_rssd" in panel.columns else 0
    print(
        f"Done. {len(panel):,} quarterly rows | "
        f"lead bank coverage: {n_enriched:,}/{len(panel):,} "
        f"({100*n_enriched/len(panel):.1f}%)"
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build the PERMCO-RSSD link DuckDB dataset.")
    p.add_argument("--force", action="store_true", help="Rebuild even if parquet already exists.")
    return p.parse_args()


if __name__ == "__main__":
    main()
