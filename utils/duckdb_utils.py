"""
DuckDB connection management and helper utilities.
"""
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Optional

import duckdb

from utils.logging_utils import get_logger

logger = get_logger(__name__)


def get_connection(
    db_path: Path,
    read_only: bool = False,
    threads: int = 4,
    memory_limit: str = "8GB",
) -> duckdb.DuckDBPyConnection:
    """
    Open (or create) a DuckDB database at db_path.
    Sets thread count, memory limit, and enables progress bars.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path), read_only=read_only)
    conn.execute(f"PRAGMA threads={threads}")
    conn.execute(f"PRAGMA memory_limit='{memory_limit}'")
    return conn


@contextmanager
def transactional_connection(
    db_path: Path,
    threads: int = 4,
    memory_limit: str = "8GB",
) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """
    Context manager yielding a DuckDB connection inside an explicit transaction.
    Commits on clean exit; rolls back and re-raises on any exception.
    """
    conn = get_connection(db_path, threads=threads, memory_limit=memory_limit)
    conn.execute("BEGIN")
    try:
        yield conn
        conn.execute("COMMIT")
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        conn.close()


def ensure_table_exists(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    schema_sql: str,
) -> None:
    """
    Execute a CREATE TABLE IF NOT EXISTS statement.
    schema_sql should be the full DDL (table name + column list).
    """
    conn.execute(schema_sql)


def upsert_row(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    row: dict,
    key_columns: list,
) -> None:
    """
    Upsert a single row: DELETE matching key columns then INSERT.
    DuckDB lacks native UPSERT; this two-step is the safe approach.
    """
    if not row:
        return

    # Build WHERE clause for deletion
    where_parts = [f"{col} = ?" for col in key_columns]
    where_vals = [row[col] for col in key_columns]
    conn.execute(
        f"DELETE FROM {table_name} WHERE {' AND '.join(where_parts)}",
        where_vals,
    )

    # Build INSERT
    cols = list(row.keys())
    placeholders = ", ".join(["?" for _ in cols])
    col_str = ", ".join(cols)
    conn.execute(
        f"INSERT INTO {table_name} ({col_str}) VALUES ({placeholders})",
        [row[c] for c in cols],
    )


def recreate_lar_view(
    conn: duckdb.DuckDBPyConnection,
    parquet_paths: list,
) -> None:
    """
    Drop and recreate the lar_panel VIEW over the given list of Parquet files.

    Uses union_by_name=true so columns absent in older-year files are NULL-filled,
    and hive_partitioning=true so year-filtered queries skip irrelevant files.

    Six harmonized columns are appended as SQL CASE expressions to bridge
    categorical code differences across the 2017/2018 reform boundary:
      loan_purpose_harmonized, purchaser_type_harmonized,
      denial_reason_1/2/3_harmonized, preapproval_harmonized
    """
    if not parquet_paths:
        logger.warning("recreate_lar_view called with empty parquet_paths list — skipping")
        return

    # Import here to avoid circular imports at module load time
    from hmda.metadata import HARMONIZED_VIEW_EXPRS

    # DuckDB needs forward-slash paths in SQL strings even on Windows
    paths_sql = ", ".join(
        f"'{str(p).replace(chr(92), '/')}'" for p in parquet_paths
    )

    harmonized_sql = ",\n        ".join(
        f"({expr}) AS {col}"
        for col, expr in HARMONIZED_VIEW_EXPRS.items()
    )

    # state_code: always 2-char zero-padded FIPS.
    #   pre-2018: LPAD raw state_code (source often drops leading zeros)
    #   2018+:    CFPB county_code already contains state+county -> take first 2 chars
    state_code_harmonized = """
        CASE
            WHEN year >= 2018 AND county_code IS NOT NULL
                 AND TRIM(county_code) NOT IN ('', 'NA', 'na')
                THEN SUBSTR(LPAD(TRIM(county_code), 5, '0'), 1, 2)
            WHEN state_code IS NULL OR TRIM(state_code) IN ('', 'NA', 'na')
                THEN NULL
            ELSE LPAD(TRIM(state_code), 2, '0')
        END
    """

    # county_code: always 3-char zero-padded county-within-state.
    #   pre-2018: LPAD to 3 (source drops leading zeros)
    #   2018+:    CFPB county_code is 5-char state+county -> keep last 3
    county_code_harmonized = """
        CASE
            WHEN county_code IS NULL OR TRIM(county_code) IN ('', 'NA', 'na')
                THEN NULL
            WHEN year >= 2018
                THEN SUBSTR(LPAD(TRIM(county_code), 5, '0'), 3, 3)
            ELSE LPAD(TRIM(county_code), 3, '0')
        END
    """

    # county_fips: derived full 5-char state+county FIPS
    county_fips_derived = f"""
        CASE
            WHEN ({state_code_harmonized}) IS NULL
              OR ({county_code_harmonized}) IS NULL
                THEN NULL
            ELSE ({state_code_harmonized}) || ({county_code_harmonized})
        END
    """

    view_sql = f"""
        CREATE OR REPLACE VIEW lar_panel AS
        SELECT
            * REPLACE (
                ({state_code_harmonized})  AS state_code,
                ({county_code_harmonized}) AS county_code
            ),
            ({county_fips_derived}) AS county_fips,
            {harmonized_sql}
        FROM read_parquet(
            [{paths_sql}],
            hive_partitioning = true,
            union_by_name = true
        )
    """
    conn.execute(view_sql)
    logger.info("lar_panel VIEW recreated over %d parquet files", len(parquet_paths))


def run_validation_query(
    conn: duckdb.DuckDBPyConnection,
    year: int,
) -> dict:
    """
    Run basic data quality checks for a single year partition.
    Returns a dict with row_count, null rates, and amount ranges.
    """
    try:
        result = conn.execute(
            """
            SELECT
                COUNT(*)                                     AS row_count,
                AVG(CASE WHEN loan_amount IS NULL THEN 1.0 ELSE 0.0 END) AS null_rate_loan_amount,
                AVG(CASE WHEN income IS NULL THEN 1.0 ELSE 0.0 END)      AS null_rate_income,
                AVG(CASE WHEN census_tract IS NULL THEN 1.0 ELSE 0.0 END) AS null_rate_census_tract,
                AVG(CASE WHEN lei IS NULL THEN 1.0 ELSE 0.0 END)          AS null_rate_lei,
                MIN(TRY_CAST(loan_amount AS DOUBLE))         AS min_loan_amount,
                MAX(TRY_CAST(loan_amount AS DOUBLE))         AS max_loan_amount,
                MIN(TRY_CAST(income AS DOUBLE))              AS min_income,
                MAX(TRY_CAST(income AS DOUBLE))              AS max_income
            FROM lar_panel
            WHERE year = ?
            """,
            [year],
        ).fetchone()
        cols = [
            "row_count", "null_rate_loan_amount", "null_rate_income",
            "null_rate_census_tract", "null_rate_lei",
            "min_loan_amount", "max_loan_amount", "min_income", "max_income",
        ]
        return dict(zip(cols, result)) if result else {}
    except Exception as exc:
        logger.warning("Validation query failed for year %d: %s", year, exc)
        return {}


def retry_on_io_error(
    fn,
    retries: int = 3,
    delay: float = 5.0,
):
    """
    Call fn(); on duckdb.IOException retry up to `retries` times with `delay` seconds sleep.
    """
    for attempt in range(retries):
        try:
            return fn()
        except duckdb.IOException as exc:
            if attempt < retries - 1:
                logger.warning(
                    "DuckDB IO error (attempt %d/%d): %s — retrying in %.0fs",
                    attempt + 1, retries, exc, delay,
                )
                time.sleep(delay)
            else:
                raise
