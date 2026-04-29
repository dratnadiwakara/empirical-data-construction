"""
RateWatch construct: raw text -> harmonized Parquet -> DuckDB views.

Pipeline per year:
  1. Read pipe-delimited raw rate file (era-aware columns).
  2. Filter to kept products + dominant tier from product_registry.json.
  3. Cast types, harmonize columns (MINTOEARN/AMOUNT -> amount).
  4. Left-join Deposit_InstitutionDetails for RSSD_ID/UNINUMBR/CERT_NBR.
  5. Write atomic snappy Parquet to staging/year={year}/data.parquet.
  6. Update panel_metadata.

Also (one-time per run): build support staging Parquet for institution
details and acct_join, then create main `ratewatch` view + helpers.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import (
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_THREADS,
    PARQUET_COMPRESSION,
    get_ratewatch_duckdb_path,
    get_ratewatch_raw_path,
    get_ratewatch_staging_path,
    get_ratewatch_storage_path,
    get_ratewatch_support_path,
    get_ratewatch_support_staging_path,
)
from ratewatch.metadata import (
    ACCT_JOIN_COLUMNS,
    ALL_YEARS,
    FIRST_YEAR,
    INST_DETAILS_COLUMNS,
    INST_NUMERIC_COLS,
    LAST_YEAR,
    LSU_YEARS,
    PANEL_METADATA_DDL,
    UNZIPPED_YEARS,
    era_for_year,
    load_product_registry,
    raw_source_filename,
    raw_source_path,
)
from utils.duckdb_utils import ensure_table_exists, get_connection, upsert_row
from utils.logging_utils import get_logger

logger = get_logger(__name__)


def _sql_path(p: Path) -> str:
    return str(p).replace("\\", "/")


def _connect_mem() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    conn.execute(f"PRAGMA threads={DUCKDB_THREADS}")
    conn.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    return conn


# ── Support tables: parse once per run ─────────────────────────────────────

def build_support_parquet(force: bool = False) -> None:
    """Parse Deposit_InstitutionDetails.txt and Deposit_acct_join.txt -> Parquet."""
    support_dir = get_ratewatch_support_path()
    staging_dir = get_ratewatch_support_staging_path()

    inst_src = support_dir / "Deposit_InstitutionDetails.txt"
    inst_dst = staging_dir / "institution_details.parquet"
    join_src = support_dir / "Deposit_acct_join.txt"
    join_dst = staging_dir / "acct_join.parquet"

    conn = _connect_mem()
    try:
        if inst_src.exists() and (force or not inst_dst.exists()):
            exprs = []
            for col in INST_DETAILS_COLUMNS:
                q = f'"{col}"'
                if col in INST_NUMERIC_COLS:
                    exprs.append(
                        f"CASE WHEN TRIM({q})='' OR {q} IS NULL THEN NULL "
                        f"ELSE TRY_CAST(TRIM({q}) AS DOUBLE) END AS {col.lower()}"
                    )
                elif col == "EST_DT":
                    exprs.append(
                        f"TRY_CAST(NULLIF(TRIM({q}), '') AS DATE) AS {col.lower()}"
                    )
                else:
                    exprs.append(f"NULLIF(TRIM({q}), '') AS {col.lower()}")
            sql = (
                "SELECT " + ", ".join(exprs)
                + f" FROM read_csv('{_sql_path(inst_src)}', delim='|', header=true, all_varchar=true, encoding='latin-1')"
            )
            tmp = inst_dst.with_suffix(".parquet.tmp")
            conn.execute(f"COPY ({sql}) TO '{_sql_path(tmp)}' "
                         f"(FORMAT PARQUET, COMPRESSION '{PARQUET_COMPRESSION}')")
            tmp.replace(inst_dst)
            n = conn.execute(f"SELECT count(*) FROM read_parquet('{_sql_path(inst_dst)}')").fetchone()[0]
            logger.info("Wrote institution_details Parquet: %d rows", n)

        if join_src.exists() and (force or not join_dst.exists()):
            exprs = []
            for col in ACCT_JOIN_COLUMNS:
                q = f'"{col}"'
                if col == "EFF_DATE":
                    exprs.append(
                        f"TRY_CAST(NULLIF(TRIM({q}), '') AS DATE) AS {col.lower()}"
                    )
                else:
                    exprs.append(f"NULLIF(TRIM({q}), '') AS {col.lower()}")
            sql = (
                "SELECT " + ", ".join(exprs)
                + f" FROM read_csv('{_sql_path(join_src)}', delim='|', header=true, all_varchar=true)"
            )
            tmp = join_dst.with_suffix(".parquet.tmp")
            conn.execute(f"COPY ({sql}) TO '{_sql_path(tmp)}' "
                         f"(FORMAT PARQUET, COMPRESSION '{PARQUET_COMPRESSION}')")
            tmp.replace(join_dst)
            n = conn.execute(f"SELECT count(*) FROM read_parquet('{_sql_path(join_dst)}')").fetchone()[0]
            logger.info("Wrote acct_join Parquet: %d rows", n)
    finally:
        conn.close()


# ── Per-year construct ─────────────────────────────────────────────────────

def _tier_filter_sql(year: int) -> tuple[str, list]:
    """Build WHERE clause: (PRD_TYP_JOIN, TRIM(PRODUCTDESCRIPTION)) IN <list>.

    Tier identity is the (product, productdescription) pair only — the
    description string already encodes term + balance info, so we don't
    re-match MIN/MAX/AMOUNT/TERMLENGTH/TERMTYPE columns.
    """
    registry = load_product_registry().get(str(year))
    if not registry:
        raise ValueError(f"No product_registry entry for {year}; run profile.py first.")

    pairs: list[tuple[str, str]] = []
    for p in registry["kept"]:
        prd = p["prd_typ_join"]
        for desc in p.get("tier_descriptions", []):
            if desc is None:
                continue
            pairs.append((prd, str(desc)))

    if not pairs:
        # No retained tiers at all for this year (shouldn't happen if mandatory list non-empty)
        return "1=0", []

    placeholders = ", ".join("(?, ?)" for _ in pairs)
    flat: list = []
    for prd, desc in pairs:
        flat.extend([prd, desc])
    where = f"(PRD_TYP_JOIN, TRIM(PRODUCTDESCRIPTION)) IN ({placeholders})"
    return where, flat


def _build_select_sql(year: int, raw_path: Path, inst_parquet: Path) -> tuple[str, list]:
    """Build full transform SELECT for one year."""
    era = era_for_year(year)
    where_sql, params = _tier_filter_sql(year)

    if era == "A":
        amount_expr = "TRY_CAST(TRIM(MINTOEARN) AS DOUBLE) AS amount"
        maxtoearn_expr = "TRY_CAST(TRIM(MAXTOEARN) AS DOUBLE) AS maxtoearn"
    else:
        amount_expr = "TRY_CAST(TRIM(AMOUNT) AS DOUBLE) AS amount"
        maxtoearn_expr = "CAST(NULL AS DOUBLE) AS maxtoearn"

    sql = f"""
        WITH raw AS (
            SELECT *
            FROM read_csv(
                '{_sql_path(raw_path)}',
                delim='|', header=true, all_varchar=true,
                ignore_errors=true
            )
            WHERE {where_sql}
        ),
        typed AS (
            SELECT
                {year} AS year,
                TRY_CAST(NULLIF(TRIM(DATESURVEYED), '') AS DATE) AS week_date,
                NULLIF(TRIM(ACCOUNTNUMBER), '') AS account_number,
                NULLIF(TRIM(PRD_TYP_JOIN), '')  AS prd_typ_join,
                NULLIF(TRIM(PRODUCTDESCRIPTION), '') AS productdescription,
                NULLIF(TRIM(PRODUCTTYPE), '')   AS producttype,
                NULLIF(TRIM(PROD_NM), '')       AS prod_nm,
                NULLIF(TRIM(PROMO), '')         AS promo,
                {amount_expr},
                {maxtoearn_expr},
                TRY_CAST(TRIM(TERMLENGTH) AS DOUBLE) AS termlength,
                NULLIF(TRIM(TERMTYPE), '')      AS termtype,
                TRY_CAST(TRIM(RATE) AS DOUBLE)  AS rate,
                TRY_CAST(TRIM(APY) AS DOUBLE)   AS apy,
                NULLIF(TRIM(CMT), '')           AS cmt
            FROM raw
        )
        SELECT
            t.year,
            t.week_date,
            t.account_number,
            t.prd_typ_join,
            t.productdescription,
            t.producttype,
            t.prod_nm,
            t.promo,
            t.amount,
            t.maxtoearn,
            t.termlength,
            t.termtype,
            t.rate,
            t.apy,
            t.cmt,
            CAST(i.rssd_id  AS BIGINT) AS rssd_id,
            CAST(i.uninumbr AS BIGINT) AS uninumbr,
            CAST(i.cert_nbr AS BIGINT) AS cert_nbr,
            i.inst_nm,
            i.inst_typ,
            i.state    AS inst_state,
            i.zip      AS inst_zip,
            i.cnty_fps AS inst_cnty_fps,
            i.state_fps AS inst_state_fps,
            i.msa,
            i.cbsa
        FROM typed t
        LEFT JOIN read_parquet('{_sql_path(inst_parquet)}') i
            ON t.account_number = i.acct_nbr
    """
    return sql, params


def construct_year(year: int, force: bool = False) -> Optional[int]:
    raw_path = get_ratewatch_raw_path(year) / raw_source_filename(year)
    if not raw_path.exists():
        # Fall back to source-of-truth on D:\ (avoids 100 GB local-stage duplication)
        if year in UNZIPPED_YEARS or year in LSU_YEARS:
            try:
                src = raw_source_path(year)
            except ValueError:
                src = None
            if src and src.exists():
                logger.info("[%d] Reading source directly: %s", year, src)
                raw_path = src
            else:
                logger.warning("[%d] Raw missing local and remote", year)
                return None
        else:
            logger.warning("[%d] Raw missing: %s. Run download.py first.", year, raw_path)
            return None

    inst_parquet = get_ratewatch_support_staging_path() / "institution_details.parquet"
    if not inst_parquet.exists():
        logger.info("[%d] Building support Parquets first", year)
        build_support_parquet(force=False)

    parquet_path = get_ratewatch_staging_path(year) / "data.parquet"
    if parquet_path.exists() and not force:
        logger.info("[%d] Parquet exists; skipping (--force to rebuild)", year)
        return None

    sql, params = _build_select_sql(year, raw_path, inst_parquet)

    conn = _connect_mem()
    try:
        tmp = parquet_path.with_suffix(".parquet.tmp")
        # DuckDB COPY does not accept parameters. Inline-bind safely:
        # all params here are non-user-supplied registry values that we string-quote.
        bound_sql = _bind_inline(sql, params)
        conn.execute(f"""
            COPY ({bound_sql})
            TO '{_sql_path(tmp)}'
            (FORMAT PARQUET, COMPRESSION '{PARQUET_COMPRESSION}', ROW_GROUP_SIZE 260000)
        """)
        row_count: int = conn.execute(
            f"SELECT count(*) FROM read_parquet('{_sql_path(tmp)}')"
        ).fetchone()[0]
        n_branches: int = conn.execute(
            f"SELECT count(DISTINCT account_number) FROM read_parquet('{_sql_path(tmp)}')"
        ).fetchone()[0]
        n_products: int = conn.execute(
            f"SELECT count(DISTINCT prd_typ_join) FROM read_parquet('{_sql_path(tmp)}')"
        ).fetchone()[0]
        n_weeks: int = conn.execute(
            f"SELECT count(DISTINCT week_date) FROM read_parquet('{_sql_path(tmp)}')"
        ).fetchone()[0]
        tmp.replace(parquet_path)
    finally:
        conn.close()

    logger.info(
        "[%d] Wrote %d rows | %d branches | %d products | %d weeks -> %s",
        year, row_count, n_branches, n_products, n_weeks, parquet_path,
    )
    _upsert_metadata(year, row_count, n_branches, n_products, n_weeks, raw_path, parquet_path)
    return row_count


def _bind_inline(sql: str, params: list) -> str:
    """Replace ? placeholders with SQL-escaped string literals.

    All values originate from product_registry.json (we generated it ourselves);
    no user input flows here. Single quotes inside values are escaped.
    """
    out = []
    i = 0
    pi = 0
    while i < len(sql):
        ch = sql[i]
        if ch == "?":
            v = params[pi]
            pi += 1
            esc = v.replace("'", "''")
            out.append(f"'{esc}'")
        else:
            out.append(ch)
        i += 1
    if pi != len(params):
        raise RuntimeError(f"Param count mismatch: used {pi}, given {len(params)}")
    return "".join(out)


def _upsert_metadata(
    year: int, row_count: int, n_branches: int, n_products: int,
    n_weeks: int, raw_path: Path, parquet_path: Path,
) -> None:
    db_conn = get_connection(
        get_ratewatch_duckdb_path(),
        threads=DUCKDB_THREADS,
        memory_limit=DUCKDB_MEMORY_LIMIT,
    )
    try:
        ensure_table_exists(db_conn, "panel_metadata", PANEL_METADATA_DDL)
        upsert_row(
            db_conn,
            "panel_metadata",
            {
                "year": year,
                "row_count": row_count,
                "n_branches": n_branches,
                "n_products": n_products,
                "n_weeks": n_weeks,
                "source_file": raw_path.name,
                "built_at": datetime.now(timezone.utc).isoformat(),
                "parquet_path": str(parquet_path),
            },
            ["year"],
        )
    finally:
        db_conn.close()


# ── Views ──────────────────────────────────────────────────────────────────

def recreate_views() -> None:
    staging_root = get_ratewatch_storage_path("staging")
    parquets = sorted(staging_root.glob("year=*/data.parquet"))
    if not parquets:
        logger.warning("No staging Parquets found; skipping views")
        return

    inst_parquet = get_ratewatch_support_staging_path() / "institution_details.parquet"
    join_parquet = get_ratewatch_support_staging_path() / "acct_join.parquet"

    paths_sql = ", ".join(f"'{_sql_path(p)}'" for p in parquets)

    db_conn = get_connection(
        get_ratewatch_duckdb_path(),
        threads=DUCKDB_THREADS,
        memory_limit=DUCKDB_MEMORY_LIMIT,
    )
    try:
        db_conn.execute(f"""
            CREATE OR REPLACE VIEW ratewatch AS
            SELECT *
            FROM read_parquet([{paths_sql}], hive_partitioning=true, union_by_name=true)
        """)
        logger.info("ratewatch VIEW created over %d parquets", len(parquets))

        if inst_parquet.exists():
            db_conn.execute(f"""
                CREATE OR REPLACE VIEW ratewatch_institutions AS
                SELECT * FROM read_parquet('{_sql_path(inst_parquet)}')
            """)
            logger.info("ratewatch_institutions VIEW created")

        if join_parquet.exists():
            db_conn.execute(f"""
                CREATE OR REPLACE VIEW ratewatch_acct_join AS
                SELECT * FROM read_parquet('{_sql_path(join_parquet)}')
            """)
            logger.info("ratewatch_acct_join VIEW created")

            # Branch-fanout convenience view
            db_conn.execute("""
                CREATE OR REPLACE VIEW ratewatch_branch_fanout AS
                SELECT
                    j.acct_nbr_loc AS branch_account,
                    r.*
                FROM ratewatch r
                JOIN ratewatch_acct_join j
                    ON r.account_number = j.acct_nbr_rt
                   AND r.prd_typ_join   = j.prd_typ_join
            """)
            logger.info("ratewatch_branch_fanout VIEW created")
    finally:
        db_conn.close()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Construct RateWatch Parquet + DuckDB views.")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--year", type=int, help=f"Single year ({FIRST_YEAR}-{LAST_YEAR})")
    g.add_argument("--all", action="store_true", help="All years")
    p.add_argument("--force", action="store_true", help="Reprocess even if Parquet exists")
    p.add_argument("--rebuild-support", action="store_true",
                   help="Rebuild support Parquets (institution_details, acct_join)")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    build_support_parquet(force=args.rebuild_support)

    years = ALL_YEARS if args.all else [args.year]
    for y in years:
        try:
            construct_year(y, force=args.force)
        except Exception:
            logger.exception("[%d] Construct failed", y)

    recreate_views()
    logger.info("RateWatch construct step complete.")


if __name__ == "__main__":
    main()
