"""
NIC ETL: ZIP CSV snapshots -> Parquet staging -> DuckDB views.
"""
from __future__ import annotations

import argparse
import json
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import (
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_THREADS,
    PARQUET_COMPRESSION,
    get_nic_duckdb_path,
    get_nic_manifest_path,
    get_nic_raw_path,
    get_nic_staging_path,
)
from nic.metadata import BASE_URL, DATASETS, DATE_COLUMNS, ID_COLUMNS, NUMERIC_ID_COLUMNS, PANEL_METADATA_DDL
from utils.duckdb_utils import ensure_table_exists, get_connection, upsert_row
from utils.logging_utils import get_logger

logger = get_logger(__name__)


def _sql_path(path: Path) -> str:
    return str(path).replace("\\", "/")


def _manifest_sha256(dataset: str) -> str:
    manifest_path = get_nic_manifest_path()
    if not manifest_path.exists():
        return ""
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return ""
    return str(payload.get(dataset, {}).get("sha256", ""))


def _extract_csvs(zip_path: Path, tmp_dir: Path) -> list[Path]:
    csvs: list[Path] = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        for info in zf.infolist():
            if info.is_dir() or not info.filename.lower().endswith(".csv"):
                continue
            out = tmp_dir / Path(info.filename).name
            with zf.open(info) as src, open(out, "wb") as dst:
                dst.write(src.read())
            csvs.append(out)
    if not csvs:
        raise ValueError(f"No CSV files found in {zip_path}")
    return csvs


def _ordered_columns(dataset: str, columns: list[str]) -> list[str]:
    id_cols = [c for c in ID_COLUMNS.get(dataset, []) if c in columns]
    date_cols = [c for c in DATE_COLUMNS if c in columns and c not in id_cols]
    remaining = sorted([c for c in columns if c not in id_cols and c not in date_cols])
    return id_cols + date_cols + remaining


def _build_standardized_select(dataset: str, columns: list[str]) -> str:
    ordered = _ordered_columns(dataset, columns)
    exprs: list[str] = []
    for col in ordered:
        quoted = f'"{col}"'
        if col in NUMERIC_ID_COLUMNS:
            exprs.append(
                f"TRY_CAST(NULLIF(TRIM({quoted}), '') AS BIGINT) AS {quoted}"
            )
        else:
            exprs.append(f"NULLIF(TRIM({quoted}), '') AS {quoted}")
    return ",\n        ".join(exprs)


def _write_dataset_parquet(dataset: str, force: bool = False) -> int | None:
    zip_path = get_nic_raw_path() / DATASETS[dataset]["filename"]
    if not zip_path.exists():
        logger.warning("[%s] Missing ZIP file: %s", dataset, zip_path)
        return None

    out_dir = get_nic_staging_path(dataset)
    out_path = out_dir / "data.parquet"
    if out_path.exists() and not force:
        logger.info("[%s] Staging parquet exists; skipping (use --force)", dataset)
        return None

    with tempfile.TemporaryDirectory() as td:
        tmp_dir = Path(td)
        csv_files = _extract_csvs(zip_path, tmp_dir)

        conn = duckdb.connect(":memory:")
        conn.execute(f"PRAGMA threads={DUCKDB_THREADS}")
        conn.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY_LIMIT}'")
        try:
            union_parts = []
            for csv_path in csv_files:
                union_parts.append(
                    f"SELECT * FROM read_csv_auto('{_sql_path(csv_path)}', all_varchar=true, header=true)"
                )
            union_sql = "\nUNION ALL\n".join(union_parts)
            conn.execute(f"CREATE TEMP VIEW raw_nic AS {union_sql}")

            cols = [row[0].upper().strip() for row in conn.execute("DESCRIBE raw_nic").fetchall()]
            rename_expr = ", ".join([f'"{row[0]}" AS "{row[0].upper().strip()}"' for row in conn.execute("DESCRIBE raw_nic").fetchall()])
            conn.execute(f"CREATE TEMP VIEW raw_nic_upper AS SELECT {rename_expr} FROM raw_nic")

            select_sql = _build_standardized_select(dataset, cols)
            tmp_parquet = out_path.with_suffix(".parquet.tmp")
            copy_sql = f"""
            COPY (
                SELECT
                    {select_sql}
                FROM raw_nic_upper
            ) TO '{_sql_path(tmp_parquet)}'
            (FORMAT PARQUET, COMPRESSION '{PARQUET_COMPRESSION}')
            """
            conn.execute(copy_sql)
            row_count = conn.execute(
                f"SELECT count(*) FROM read_parquet('{_sql_path(tmp_parquet)}')"
            ).fetchone()[0]
            tmp_parquet.replace(out_path)
            return row_count
        finally:
            conn.close()


def _upsert_panel_metadata(dataset: str, row_count: int) -> None:
    db_conn = get_connection(
        get_nic_duckdb_path(),
        threads=DUCKDB_THREADS,
        memory_limit=DUCKDB_MEMORY_LIMIT,
    )
    try:
        ensure_table_exists(db_conn, "panel_metadata", PANEL_METADATA_DDL)
        download_path = DATASETS[dataset]["download_path"]
        row = {
            "dataset": dataset,
            "row_count": row_count,
            "file_sha256": _manifest_sha256(dataset),
            "source_url": f"{BASE_URL}{download_path}",
            "built_at": datetime.now(timezone.utc).isoformat(),
            "parquet_path": str(get_nic_staging_path(dataset) / "data.parquet"),
        }
        upsert_row(db_conn, "panel_metadata", row, ["dataset"])
    finally:
        db_conn.close()


def recreate_views() -> None:
    db_conn = get_connection(
        get_nic_duckdb_path(),
        threads=DUCKDB_THREADS,
        memory_limit=DUCKDB_MEMORY_LIMIT,
    )
    try:
        for dataset, meta in DATASETS.items():
            parquet_path = get_nic_staging_path(dataset) / "data.parquet"
            if not parquet_path.exists():
                continue
            view_name = meta["view_name"]
            db_conn.execute(
                f"""
                CREATE OR REPLACE VIEW {view_name} AS
                SELECT *
                FROM read_parquet('{_sql_path(parquet_path)}', union_by_name=true)
                """
            )
    finally:
        db_conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Construct NIC Parquet and DuckDB views.")
    parser.add_argument(
        "--dataset",
        choices=sorted(DATASETS.keys()),
        help="Process only one dataset (default: both).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reprocess even if staging parquet already exists.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    targets = [args.dataset] if args.dataset else sorted(DATASETS.keys())

    for dataset in targets:
        logger.info("[%s] Constructing dataset", dataset)
        row_count = _write_dataset_parquet(dataset, force=args.force)
        if row_count is None:
            continue
        _upsert_panel_metadata(dataset, row_count)
        logger.info("[%s] Wrote %d rows", dataset, row_count)

    recreate_views()
    logger.info("NIC construct step complete.")


if __name__ == "__main__":
    main()
