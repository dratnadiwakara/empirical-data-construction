"""
HMDA LAR ETL (2000-2024).

Uses DuckDB's out-of-core engine to read the pipe-delimited or comma-delimited LAR
CSV and write directly to Parquet — without loading the full dataset into Python
memory.  Safe on 8 GB machines.

Era dispatch:
  2018-2024  Post-reform: pipe-delimited, header, 99 cols, no transforms.
  2017       Pre-reform FFIEC: pipe-delimited, NO header (use COLUMNS_2017), 45 cols.
             loan_amount ×1000, census_tract FIPS construction, column renames.
  2007-2016  Pre-reform CFPB: comma-delimited, header, ~45 cols.
             Same transforms as 2017.  Text labels converted to numeric codes via
             LABEL_TO_CODE CASE expressions (handles both labeled and coded files).
  2000-2006  ICPSR: pipe-delimited, header, pure numeric codes.
             2004-2006: 38 cols (post-2004 reform — has ethnicity, race_2-5, etc.)
             2000-2003: 23 cols (pre-2004 reform — only race_1, no ethnicity, etc.)
             loan_amount ×1000, census_tract FIPS construction, column renames.

Usage
-----
    python -m hmda.construct --year 2024          # process single year (start here)
    python -m hmda.construct --year 2016          # process 2016 (CFPB historic)
    python -m hmda.construct --year 2017          # process 2017 (FFIEC pre-reform)
    python -m hmda.construct --year 2006          # process 2006 (ICPSR)
    python -m hmda.construct --year 2024 --force  # reprocess even if Parquet exists
    python -m hmda.construct --all                # 2024 -> 2000 (after 2024 confirmed)
"""
from __future__ import annotations

import argparse
import hashlib
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
    PARQUET_ROW_GROUP_SIZE,
    get_duckdb_path,
    get_raw_path,
    get_staging_path,
)
from hmda.metadata import (
    ALL_YEARS,
    CENSUS_TRACT_SQL_2017,
    CFPB_HISTORIC_CATEGORICAL_COLS,
    COLS_TO_DROP_CFPB_HISTORIC,
    COLS_TO_DROP_ICPSR,
    COLUMN_RENAMES_2017,
    COLUMN_RENAMES_CFPB_HISTORIC,
    COLUMN_RENAMES_ICPSR,
    COLUMNS_2017,
    LOAN_AMOUNT_SCALE_SQL_2017,
    MASTER_SCHEMA,
    PANEL_METADATA_DDL,
    build_label_case_sql,
    get_delimiter,
    is_cfpb_historic,
    is_icpsr,
    is_pre_2018,
)
from utils.duckdb_utils import (
    ensure_table_exists,
    get_connection,
    recreate_lar_view,
    run_validation_query,
    upsert_row,
)
from utils.logging_utils import get_logger, log_step

logger = get_logger(__name__)


# ── Raw file discovery ─────────────────────────────────────────────────────────

def find_raw_file(year: int) -> Optional[Path]:
    """Return the extracted pipe-delimited LAR file for a given year, or None."""
    raw_dir = get_raw_path(year)
    for ext in ("*.txt", "*.csv", "*.pipe"):
        matches = list(raw_dir.glob(ext))
        if matches:
            return max(matches, key=lambda p: p.stat().st_size)
    return None


# ── Schema helpers ────────────────────────────────────────────────────────────

def _get_csv_columns(csv_path: Path, conn: duckdb.DuckDBPyConnection, year: int) -> list[str]:
    """
    Return the column names of the LAR CSV file.

    2017 (FFIEC): no header row → return hardcoded COLUMNS_2017 list.
    2018+:        pipe-delimited with header → read header from file.
    2007-2016:    comma-delimited with header → read header from file.
    """
    if year == 2017:
        return list(COLUMNS_2017)
    sep = get_delimiter(year)
    result = conn.execute(
        f"SELECT * FROM read_csv('{_sql_path(csv_path)}', sep='{sep}', header=true, "
        f"all_varchar=true, ignore_errors=true) LIMIT 0"
    ).description
    return [col[0] for col in result]


def _sql_path(p: Path) -> str:
    """Return a forward-slash path string safe for DuckDB SQL literals."""
    return str(p).replace("\\", "/")


def _build_select_exprs(
    csv_cols: list[str],
    year: int,
) -> tuple[str, list[str], list[str], list[str]]:
    """
    Build the SELECT expression list for MASTER_SCHEMA columns only.

    Dispatches to era-specific builders:
      2018+      → _build_select_exprs_post2018  (pass-through, NULL-fill missing)
      2017       → _build_select_exprs_2017      (FFIEC no-header, pipe-delimited)
      2007-2016  → _build_select_exprs_cfpb_historic  (CFPB comma-CSV, label→code)
      2000-2006  → _build_select_exprs_icpsr     (ICPSR pipe-delimited, pure codes)

    Returns:
        select_sql       : comma-joined SQL expressions for the SELECT clause
        cols_present     : MASTER_SCHEMA cols found (after renames) in the CSV
        cols_null_filled : MASTER_SCHEMA cols absent in CSV (will be NULL)
        cols_dropped     : CSV cols not in MASTER_SCHEMA (silently ignored)
    """
    if is_icpsr(year):
        return _build_select_exprs_icpsr(csv_cols)
    if is_cfpb_historic(year):
        return _build_select_exprs_cfpb_historic(csv_cols)
    if year == 2017:
        return _build_select_exprs_2017(csv_cols)
    return _build_select_exprs_post2018(csv_cols)


def _build_select_exprs_post2018(
    csv_cols: list[str],
) -> tuple[str, list[str], list[str], list[str]]:
    """SELECT expression builder for 2018-2024 (no renames, no scaling)."""
    csv_set = set(csv_cols)
    master_set = set(MASTER_SCHEMA)

    cols_present     = [c for c in MASTER_SCHEMA if c in csv_set]
    cols_null_filled = [c for c in MASTER_SCHEMA if c not in csv_set]
    cols_dropped     = [c for c in csv_cols if c not in master_set]

    exprs = []
    for col in MASTER_SCHEMA:
        safe = f'"{col}"'
        if col in csv_set:
            exprs.append(f'lar.{safe}')
        else:
            exprs.append(f'NULL AS {safe}')

    return ", ".join(exprs), cols_present, cols_null_filled, cols_dropped


def _build_select_exprs_2017(
    csv_cols: list[str],
) -> tuple[str, list[str], list[str], list[str]]:
    """
    SELECT expression builder for 2017 (pre-reform HMDA schema).

    Applies COLUMN_RENAMES_2017 (raw CSV name → master schema name),
    scales loan_amount ×1000, constructs 11-char census_tract FIPS,
    and NULL-fills all POST_2018_ONLY_COLS.
    """
    # raw_to_master maps: raw CSV column name → master schema column name.
    # COLUMN_RENAMES_2017 = {raw_col: master_col} — use directly.
    # Add identity mappings for columns not in the rename dict.
    raw_to_master: dict[str, str] = {}
    for col in csv_cols:
        # Look up the master schema name; default to the column name itself
        raw_to_master[col] = COLUMN_RENAMES_2017.get(col, col)

    master_set = set(MASTER_SCHEMA)
    master_sql: dict[str, str] = {}   # master_col → SQL expression

    for raw_col in csv_cols:
        master_col = raw_to_master[raw_col]
        if master_col not in master_set:
            continue   # raw column has no master schema target; goes into cols_dropped

        if master_col == "loan_amount":
            # Source stores loan_amount in $000s — scale to whole dollars at ETL time
            # Substitute the actual raw column name into the template expression
            scaled_expr = (
                LOAN_AMOUNT_SCALE_SQL_2017
                .replace("loan_amount_000s", f'"{raw_col}"')
            )
            master_sql[master_col] = f'({scaled_expr}) AS "loan_amount"'

        elif master_col == "census_tract":
            # Deferred: needs state_code + county_code + raw tract; handled below
            master_sql[master_col] = "__census_tract_placeholder__"

        else:
            # Standard rename (or identity)
            safe_raw = f'"{raw_col}"'
            safe_master = f'"{master_col}"'
            if raw_col == master_col:
                master_sql[master_col] = f'lar.{safe_raw}'
            else:
                master_sql[master_col] = f'lar.{safe_raw} AS {safe_master}'

    # Census tract: construct 11-char FIPS from three separate 2017 fields.
    raw_state  = next((c for c in csv_cols if c.lower() in ("state_code",  "state")),  None)
    raw_county = next((c for c in csv_cols if c.lower() in ("county_code", "county")), None)
    raw_tract  = next((c for c in csv_cols if c.lower() in ("census_tract", "tract")), None)

    if raw_tract:
        if raw_state and raw_county:
            # The template uses bare (unquoted) column names as placeholders.
            # Replace them with quoted actual column names from the CSV.
            # census_tract appears in two places; replace both occurrences.
            tract_expr = (
                CENSUS_TRACT_SQL_2017
                .replace("state_code",   f'"{raw_state}"')
                .replace("county_code",  f'"{raw_county}"')
                .replace("census_tract", f'"{raw_tract}"')
            )
            master_sql["census_tract"] = f'({tract_expr}) AS "census_tract"'
        else:
            logger.warning(
                "[2017] state_code or county_code not found in CSV; "
                "census_tract will be passed through as-is (not padded to 11 chars)"
            )
            master_sql["census_tract"] = f'lar."{raw_tract}" AS "census_tract"'

    # Pre-2018 identifier columns — not in MASTER_SCHEMA but needed for analysis-time
    # joins (respondent_id + agency_code → avery_crosswalk for RSSD linkage, and
    # property_type for dwelling-category research). Append them after MASTER_SCHEMA
    # expressions; union_by_name=true means they'll be NULL for 2018-2024 rows.
    pre2018_extra = ["respondent_id", "agency_code", "property_type"]
    extra_exprs: list[str] = []
    for col in pre2018_extra:
        if col in csv_cols:
            extra_exprs.append(f'lar."{col}" AS "{col}"')
            # Move from cols_dropped to cols_present tracking (for metadata)
            master_sql[col] = f'lar."{col}"'  # sentinel so it's counted as present

    # Classify for metadata reporting
    cols_present     = [c for c in MASTER_SCHEMA if c in master_sql]
    # Also count the extra pre-2018 cols as "present" in metadata
    cols_present_extra = [c for c in pre2018_extra if c in master_sql]
    cols_null_filled = [c for c in MASTER_SCHEMA if c not in master_sql]
    cols_dropped = [
        c for c in csv_cols
        if raw_to_master.get(c, c) not in master_set and c not in pre2018_extra
    ]

    exprs = []
    for col in MASTER_SCHEMA:
        if col in master_sql:
            expr = master_sql[col]
            if expr == "__census_tract_placeholder__":
                exprs.append(f'NULL AS "census_tract"')
            else:
                exprs.append(expr)
        else:
            exprs.append(f'NULL AS "{col}"')

    # Append pre-2018 identifier columns
    exprs.extend(extra_exprs)

    all_present = cols_present + cols_present_extra
    return ", ".join(exprs), all_present, cols_null_filled, cols_dropped


def _build_select_exprs_icpsr(
    csv_cols: list[str],
) -> tuple[str, list[str], list[str], list[str]]:
    """
    SELECT expression builder for ICPSR 2000-2006 pipe-delimited files.

    Two sub-eras (column count differs; the builder handles both generically):
      2004-2006: 38 cols — includes ethnicity, race_2-5, preapproval, property_type,
                           rate_spread, hoepa_status, lien_status.
      2000-2003: 23 cols — only race_1 per applicant/co-applicant; no ethnicity;
                           no preapproval, property_type, rate_spread, hoepa_status,
                           lien_status, or census tract demographics.

    All categorical values are already numeric codes (no label conversion).
    loan_amount is stored in $000s → scaled ×1000 at ETL time.
    census_tract is XXXX.XX format → 11-char FIPS constructed from state+county+tract.
    """
    raw_to_master: dict[str, str] = {}
    for col in csv_cols:
        raw_to_master[col] = COLUMN_RENAMES_ICPSR.get(col, col)

    master_set = set(MASTER_SCHEMA)
    master_sql: dict[str, str] = {}

    for raw_col in csv_cols:
        master_col = raw_to_master[raw_col]

        if raw_col in COLS_TO_DROP_ICPSR:
            continue

        if master_col not in master_set:
            continue

        if master_col == "loan_amount":
            # Source stores loan_amount in $000s — scale to whole dollars.
            # The column is named 'loan_amount' (not 'loan_amount_000s') but same logic.
            scaled_expr = (
                LOAN_AMOUNT_SCALE_SQL_2017
                .replace("loan_amount_000s", f'"{raw_col}"')
            )
            master_sql[master_col] = f'({scaled_expr}) AS "loan_amount"'

        elif master_col == "census_tract":
            master_sql[master_col] = "__census_tract_placeholder__"

        else:
            safe_raw    = f'"{raw_col}"'
            safe_master = f'"{master_col}"'
            if raw_col == master_col:
                master_sql[master_col] = f'lar.{safe_raw}'
            else:
                master_sql[master_col] = f'lar.{safe_raw} AS {safe_master}'

    # Census tract: construct 11-char FIPS from state + county + raw tract (XXXX.XX).
    raw_state  = next((c for c in csv_cols if c.lower() == "state_code"),   None)
    raw_county = next((c for c in csv_cols if c.lower() == "county_code"),  None)
    raw_tract  = next((c for c in csv_cols if c.lower() == "census_tract"), None)

    if raw_tract:
        if raw_state and raw_county:
            tract_expr = (
                CENSUS_TRACT_SQL_2017
                .replace("state_code",   f'"{raw_state}"')
                .replace("county_code",  f'"{raw_county}"')
                .replace("census_tract", f'"{raw_tract}"')
            )
            master_sql["census_tract"] = f'({tract_expr}) AS "census_tract"'
        else:
            logger.warning(
                "[ICPSR] state_code or county_code not found; "
                "census_tract will be passed through as-is"
            )
            master_sql["census_tract"] = f'lar."{raw_tract}" AS "census_tract"'

    # Pre-2018 identifier columns — kept for analysis-time RSSD join.
    pre2018_extra = ["respondent_id", "agency_code", "property_type"]
    extra_exprs: list[str] = []
    for col in pre2018_extra:
        if col in csv_cols:
            extra_exprs.append(f'lar."{col}" AS "{col}"')
            master_sql[col] = f'lar."{col}"'   # sentinel for metadata counting

    # Classify for metadata reporting
    cols_present_extra = [c for c in pre2018_extra if c in master_sql]
    cols_present       = [c for c in MASTER_SCHEMA if c in master_sql]
    cols_null_filled   = [c for c in MASTER_SCHEMA if c not in master_sql]
    cols_dropped       = [
        c for c in csv_cols
        if raw_to_master.get(c, c) not in master_set
        and c not in pre2018_extra
        and c not in COLS_TO_DROP_ICPSR
    ]

    exprs = []
    for col in MASTER_SCHEMA:
        if col in master_sql:
            expr = master_sql[col]
            if expr == "__census_tract_placeholder__":
                exprs.append(f'NULL AS "census_tract"')
            else:
                exprs.append(expr)
        else:
            exprs.append(f'NULL AS "{col}"')
    exprs.extend(extra_exprs)

    all_present = cols_present + cols_present_extra
    return ", ".join(exprs), all_present, cols_null_filled, cols_dropped


def _build_select_exprs_cfpb_historic(
    csv_cols: list[str],
) -> tuple[str, list[str], list[str], list[str]]:
    """
    SELECT expression builder for 2007-2016 CFPB historic CSV files.

    These files are comma-delimited with a header row. Column names differ from
    both the 2017 FFIEC file and the master schema. Categorical values may be
    text labels (e.g., 'Refinancing') instead of numeric codes ('3').

    Transformations applied:
      - COLUMN_RENAMES_CFPB_HISTORIC: maps raw CSV name → master schema name
      - LABEL_TO_CODE CASE expressions: converts text labels to numeric codes
        (also passes through values that are already codes, so works for both)
      - loan_amount: ×1000 scaling (same as 2017 — source is in $000s)
      - census_tract: 11-char FIPS construction from state + county + raw tract
      - Columns in COLS_TO_DROP_CFPB_HISTORIC: dropped
      - POST_2018_ONLY_COLS: NULL-filled
      - respondent_id, agency_code, property_type: kept as extra pre-2018 columns
    """
    # Build raw_col → master_col mapping (identity where not in rename dict)
    raw_to_master: dict[str, str] = {}
    for col in csv_cols:
        raw_to_master[col] = COLUMN_RENAMES_CFPB_HISTORIC.get(col, col)

    master_set = set(MASTER_SCHEMA)
    master_sql: dict[str, str] = {}   # master_col → SQL expression

    for raw_col in csv_cols:
        master_col = raw_to_master[raw_col]

        # Drop internal/metadata columns
        if raw_col in COLS_TO_DROP_CFPB_HISTORIC:
            continue

        if master_col not in master_set:
            continue   # no target in master schema → goes into cols_dropped

        if master_col == "loan_amount":
            # Scale from $000s to whole dollars (same SQL template as 2017)
            scaled_expr = (
                LOAN_AMOUNT_SCALE_SQL_2017
                .replace("loan_amount_000s", f'"{raw_col}"')
            )
            master_sql[master_col] = f'({scaled_expr}) AS "loan_amount"'

        elif master_col == "census_tract":
            # Deferred: needs state + county + raw tract; handled below
            master_sql[master_col] = "__census_tract_placeholder__"

        elif master_col in CFPB_HISTORIC_CATEGORICAL_COLS:
            # Generate label-to-code CASE expression (handles codes AND labels)
            case_expr = build_label_case_sql(raw_col, master_col)
            master_sql[master_col] = f'({case_expr}) AS "{master_col}"'

        else:
            # Standard pass-through (with optional rename)
            safe_raw    = f'"{raw_col}"'
            safe_master = f'"{master_col}"'
            if raw_col == master_col:
                master_sql[master_col] = f'lar.{safe_raw}'
            else:
                master_sql[master_col] = f'lar.{safe_raw} AS {safe_master}'

    # Census tract: construct 11-char FIPS from three separate fields.
    # Field names may vary across years; try known variants.
    raw_state  = next(
        (c for c in csv_cols if c.lower() in ("state_code",  "state")),  None
    )
    raw_county = next(
        (c for c in csv_cols if c.lower() in ("county_code", "county")), None
    )
    raw_tract  = next(
        (c for c in csv_cols
         if c.lower() in ("census_tract", "census_tract_number", "censustract", "tract")),
        None,
    )

    if raw_tract:
        if raw_state and raw_county:
            tract_expr = (
                CENSUS_TRACT_SQL_2017
                .replace("state_code",   f'"{raw_state}"')
                .replace("county_code",  f'"{raw_county}"')
                .replace("census_tract", f'"{raw_tract}"')
            )
            master_sql["census_tract"] = f'({tract_expr}) AS "census_tract"'
        else:
            logger.warning(
                "[CFPB historic] state_code or county_code not found; "
                "census_tract will be passed through as-is"
            )
            master_sql["census_tract"] = f'lar."{raw_tract}" AS "census_tract"'

    # Pre-2018 identifier columns — not in MASTER_SCHEMA but needed for
    # analysis-time RSSD linkage and dwelling-type research.
    pre2018_extra = ["respondent_id", "agency_code", "property_type"]
    extra_exprs: list[str] = []
    for col in pre2018_extra:
        if col in csv_cols:
            if col == "agency_code" and col in CFPB_HISTORIC_CATEGORICAL_COLS:
                # agency_code may also be a text label in CFPB files
                case_expr = build_label_case_sql(col, col)
                extra_exprs.append(f'({case_expr}) AS "agency_code"')
            else:
                extra_exprs.append(f'lar."{col}" AS "{col}"')
            master_sql[col] = f'lar."{col}"'   # sentinel for metadata counting

    # Classify for metadata reporting
    cols_present_extra = [c for c in pre2018_extra if c in master_sql]
    cols_present     = [c for c in MASTER_SCHEMA if c in master_sql]
    cols_null_filled = [c for c in MASTER_SCHEMA if c not in master_sql]
    cols_dropped = [
        c for c in csv_cols
        if raw_to_master.get(c, c) not in master_set
        and c not in pre2018_extra
        and c not in COLS_TO_DROP_CFPB_HISTORIC
    ]

    # Build final SELECT list (MASTER_SCHEMA order, then pre-2018 extras)
    exprs = []
    for col in MASTER_SCHEMA:
        if col in master_sql:
            expr = master_sql[col]
            if expr == "__census_tract_placeholder__":
                exprs.append(f'NULL AS "census_tract"')
            else:
                exprs.append(expr)
        else:
            exprs.append(f'NULL AS "{col}"')
    exprs.extend(extra_exprs)

    all_present = cols_present + cols_present_extra
    return ", ".join(exprs), all_present, cols_null_filled, cols_dropped


# ── DuckDB-based ETL ──────────────────────────────────────────────────────────

def construct_year_duckdb(
    year: int,
    raw_path: Path,
    db_path: Path,
    force: bool = False,
) -> dict:
    """
    Full out-of-core ETL for one year using DuckDB.

    Steps:
      1. Attach the main DuckDB (has avery_crosswalk, panel_metadata)
      2. Read CSV via read_csv() — DuckDB streams it, no RAM spike
      3. LEFT JOIN avery_crosswalk ON lei
      4. COPY result directly to Parquet (out-of-core, spills to disk if needed)
      5. Upsert panel_metadata row
      6. Rebuild lar_panel VIEW

    Returns metadata dict with row_count, match stats, etc.
    """
    staging_dir = get_staging_path(year)
    dest        = staging_dir / "data.parquet"
    tmp         = staging_dir / "data.parquet.tmp"

    # Use a fresh in-memory DuckDB for the ETL work, then attach main DB for metadata
    etl_conn = duckdb.connect(":memory:")
    etl_conn.execute(f"PRAGMA threads={DUCKDB_THREADS}")
    etl_conn.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY_LIMIT}'")

    csv_fwd = _sql_path(raw_path)

    # 1. Get CSV column list (2017 has no header; use hardcoded COLUMNS_2017)
    logger.info("[%d] Reading CSV schema: %s", year, raw_path.name)
    csv_cols = _get_csv_columns(raw_path, etl_conn, year)
    logger.info("[%d] CSV has %d columns", year, len(csv_cols))

    select_exprs, cols_present, cols_null_filled, cols_dropped = _build_select_exprs(csv_cols, year)

    if cols_null_filled:
        logger.info("[%d] NULL-filling %d missing cols: %s", year,
                    len(cols_null_filled), cols_null_filled[:5])
    if cols_dropped:
        logger.info("[%d] Dropping %d extra cols: %s", year,
                    len(cols_dropped), cols_dropped[:5])

    # 2. Build and execute the COPY ... TO PARQUET (pure loan-level passthrough)
    # Lender variables (rssd_id etc.) are NOT joined here.
    # Join avery_crosswalk at analysis time:
    #   2018+: ON lar.lei = av.lei AND lar.year = av.activity_year
    #   2017:  ON lar.respondent_id = av.respondent_id AND ... AND av.activity_year = 2017
    sep = get_delimiter(year)
    if year == 2017:
        # 2017 FFIEC file has no header row; supply column names explicitly
        names_sql = "[" + ", ".join(f"'{c}'" for c in COLUMNS_2017) + "]"
        read_csv_opts = (
            f"sep='|', header=false, names={names_sql}, "
            f"all_varchar=true, ignore_errors=true"
        )
    else:
        # 2018-2024: pipe-delimited with header
        # 2007-2016: comma-delimited with header
        read_csv_opts = f"sep='{sep}', header=true, all_varchar=true, ignore_errors=true"

    copy_sql = f"""
        COPY (
            SELECT
                {select_exprs},
                {year}::INTEGER AS year
            FROM read_csv(
                '{csv_fwd}',
                {read_csv_opts}
            ) AS lar
        )
        TO '{_sql_path(tmp)}'
        (FORMAT PARQUET, COMPRESSION '{PARQUET_COMPRESSION}',
         ROW_GROUP_SIZE {PARQUET_ROW_GROUP_SIZE})
    """

    logger.info("[%d] Writing Parquet (out-of-core) ...", year)
    etl_conn.execute(copy_sql)
    etl_conn.close()

    # Atomic replace (rename fails on Windows if dest exists)
    tmp.replace(dest)
    log_step(logger, "staging_written", year=year, path=str(dest))

    # 3. Count rows using a lightweight scan
    count_conn = duckdb.connect(":memory:")
    count_conn.execute(f"PRAGMA threads={DUCKDB_THREADS}")
    count_conn.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    row_count = count_conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('{_sql_path(dest)}')"
    ).fetchone()[0]
    count_conn.close()

    log_step(logger, "staging_complete", year=year, rows=row_count)

    return {
        "row_count":           row_count,
        "columns_present":     cols_present,
        "columns_null_filled": cols_null_filled,
        "columns_dropped":     cols_dropped,
        "parquet_path":        dest,
    }


# ── Checksum ──────────────────────────────────────────────────────────────────

def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


# ── Year-level orchestration ──────────────────────────────────────────────────

def is_already_built(year: int) -> bool:
    return (get_staging_path(year) / "data.parquet").exists()


def construct_year(year: int, force: bool = False) -> bool:
    """
    Full ETL for a single year. Returns True on success, False if skipped/missing.
    """
    if not force and is_already_built(year):
        logger.info("[%d] Skipping - staging Parquet already exists", year)
        return True

    raw_path = find_raw_file(year)
    if raw_path is None:
        logger.error("[%d] Raw file not found - run `python -m hmda.download` first", year)
        return False

    logger.info("[%d] Constructing from: %s", year, raw_path.name)
    input_sha = _sha256(raw_path)

    db_path = get_duckdb_path()

    # Ensure DuckDB exists with required tables before ETL
    prep_conn = get_connection(db_path, threads=DUCKDB_THREADS, memory_limit=DUCKDB_MEMORY_LIMIT)
    try:
        ensure_table_exists(prep_conn, "panel_metadata", PANEL_METADATA_DDL)
    finally:
        prep_conn.close()

    # Run out-of-core ETL
    stats = construct_year_duckdb(year, raw_path, db_path, force=force)

    # Load metadata into DuckDB and rebuild VIEW
    from hmda.download import load_manifest
    source_url = load_manifest().get(str(year), {}).get("url", "")

    meta_conn = get_connection(db_path, threads=DUCKDB_THREADS, memory_limit=DUCKDB_MEMORY_LIMIT)
    try:
        meta = {
            "year":               year,
            "row_count":          stats["row_count"],
            "columns_present":    stats["columns_present"],
            "columns_null_filled": stats["columns_null_filled"],
            "columns_dropped":    stats["columns_dropped"],
            "source_url":         source_url,
            "input_file_sha256":  input_sha,
            "built_at":           datetime.now(tz=timezone.utc),
            "parquet_path":       str(stats["parquet_path"]),
        }
        upsert_row(meta_conn, "panel_metadata", meta, key_columns=["year"])

        # Rebuild lar_panel VIEW
        staging_root = get_staging_path(year).parent
        all_parquets = sorted(staging_root.glob("year=*/data.parquet"))
        recreate_lar_view(meta_conn, all_parquets)
    finally:
        meta_conn.close()

    # Validation
    val_conn = get_connection(db_path, threads=DUCKDB_THREADS, memory_limit=DUCKDB_MEMORY_LIMIT)
    try:
        val_stats = run_validation_query(val_conn, year)
    finally:
        val_conn.close()

    log_step(logger, "validation", year=year, **val_stats)
    row_count = val_stats.get("row_count", stats["row_count"])
    logger.info(
        "[%d] Done - %s rows | loan_amount [%s, %s]",
        year,
        f"{row_count:,}" if isinstance(row_count, int) else row_count,
        val_stats.get("min_loan_amount", "?"),
        val_stats.get("max_loan_amount", "?"),
    )
    return True


# ── CLI ────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Construct HMDA LAR panel (2000-2024): raw -> Parquet -> DuckDB."
    )
    grp = p.add_mutually_exclusive_group()
    grp.add_argument("--year", type=int, help="Process a single year (2000-2024)")
    grp.add_argument("--all", dest="all_years", action="store_true",
                     help="Process all years 2024 -> 2000")
    p.add_argument("--force", action="store_true",
                   help="Rebuild even if staging Parquet already exists")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    years = [args.year] if args.year else ALL_YEARS

    logger.info("Starting HMDA construction for %d year(s): %s", len(years), years)
    success = failed = skipped = 0

    for year in years:
        try:
            ok = construct_year(year, force=args.force)
            if ok:
                success += 1
            else:
                skipped += 1
        except Exception as exc:
            logger.error("[%d] Construction failed: %s", year, exc, exc_info=True)
            failed += 1

    logger.info(
        "Construction complete: %d succeeded, %d skipped, %d failed",
        success, skipped, failed,
    )


if __name__ == "__main__":
    main()
