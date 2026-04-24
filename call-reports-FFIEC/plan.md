# Plan: `call-reports-FFIEC` Dataset Module

## Context

This repo already has `call-reports-CFLV` — an academic curated snapshot (Correia/Fermin/Luck/Verner, 1959–2025) of bank balance sheets and income statements. The user wants a **separate, parallel** dataset module, `call-reports-FFIEC`, that pulls directly from the **official FFIEC CDR bulk download** source, preserving raw MDRM codes (RCFD/RCON/RIAD/etc.) at the view layer. CFLV stays as-is.

Coverage: FFIEC Forms 031 + 041 + 051, quarterly, 2001-Q1 forward.
Downloads are **manual** (user drops quarterly ZIPs into `raw/` — no ASP.NET viewstate scraping, since the CDR bulk page has no public API).
Harmonization depth for v1: **raw MDRM only**. Architect so a concept-level harmonized view (CFLV-style variable names like `assets`, `deposits`, `ln_tot`) can be added later as a pure SQL layer on top of the raw tables.

Heavy data goes to `C:\empirical-data-construction\call-reports-FFIEC\` per repo convention; code stays in the OneDrive repo.

---

## Architecture

### Directory layout

**Code** (`C:\Users\dimut\OneDrive\github\empirical-data-construction\call-reports-FFIEC\`):
```
__init__.py         # package marker
README.md           # quickstart, schedule registry, manual-download instructions, example queries
NOTES.md            # implementation notes, gotchas
plan.md             # this plan
metadata.py         # single source of truth: forms, schedules, URLs, MDRM config, filename regex
schema.py           # TypedDict for POR/filer record; generic schedule row helper
download.py         # MDRM dictionary auto-download; raw-folder scanner; manifest builder
construct.py        # ZIP extraction → TSV → Parquet → DuckDB views; panel_metadata writer
```

**Data** (`C:\empirical-data-construction\call-reports-FFIEC\`):
```
call-reports-ffiec.duckdb         # master DB: views + panel_metadata + mdrm_dictionary
download_manifest.json            # tracks ZIPs found in raw/ (sha256, mtime, size, extract status)
mdrm\
  MDRM.zip                        # downloaded from federalreserve.gov (auto)
  MDRM.csv                        # extracted
raw\
  {YYYY}Q{Q}\
    FFIEC CDR Call Bulk POR {MMDDYYYY}.txt
    FFIEC CDR Call Schedule RC {MMDDYYYY}.txt
    FFIEC CDR Call Schedule RC-B {MMDDYYYY}.txt
    ...
    FFIEC CDR Call Schedule RI {MMDDYYYY}.txt
    ...
staging\
  {schedule}\year={YYYY}\quarter={Q}\data.parquet    # Hive-partitioned, snappy
```

Each quarterly CDR bulk ZIP, once extracted, produces one TSV per schedule (RC, RC-A … RC-V, RI, RI-A … RI-E, plus POR). Each TSV has `IDRSSD` as filer key and MDRM codes (e.g. `RCON2170`, `RCFD2170`, `RIAD4340`) as column headers. All three form types (031/041/051) are merged inside the same TSV, discriminated by MDRM code prefix (RCON = domestic, RCFD = consolidated-with-foreign). FFIEC 051 filers have most RCFD columns NULL — that's expected.

### DuckDB output

One VIEW per schedule over the Hive-partitioned Parquet:

| View | Source schedule |
|------|-----------------|
| `call_filers` | Bulk POR (filer-level identity + form_type) |
| `schedule_rc` | Schedule RC (balance sheet) |
| `schedule_rc_a` … `schedule_rc_v` | RC sub-schedules |
| `schedule_ri` | Schedule RI (income statement) |
| `schedule_ri_a` … `schedule_ri_e` | RI sub-schedules |

Views use `read_parquet(..., union_by_name=true, hive_partitioning=true)` so new quarters with added MDRM codes auto-NULL-fill historical columns. All MDRM columns kept as VARCHAR (numeric casting done by the user with `TRY_CAST`, same convention as HMDA).

Plus reference tables:
- `mdrm_dictionary` — MDRM.csv loaded as-is (item_code, description, reporting_form, start_date, end_date, …) — bridges raw codes to human-readable descriptions.
- `panel_metadata` — per (schedule, year, quarter): row_count, source_zip, source_zip_sha256, parquet_path, built_at.

### Future harmonized layer (out of scope for v1, but architecture supports it)

A `concepts.sql` file under `call-reports-FFIEC/harmonized/` will later define a `call_reports_panel` VIEW that:
- Joins `schedule_rc` + `schedule_ri` on (idrssd, year, quarter)
- Introduces CFLV-style concept names via `COALESCE(RCFD2170, RCON2170) AS assets` etc.
- Bridges 031/041/051 schedule differences
- Exposes same `date` (quarter-end DATE) convention as CFLV for joinable cross-dataset queries

This can be added without changing the v1 raw-layer ETL — purely a SQL view file applied at construct time.

---

## Files to create

### 1. `config.py` — additions

Add after existing CFLV block (around line ~180 of `C:\Users\dimut\OneDrive\github\empirical-data-construction\config.py`):

```python
# ── Call Reports FFIEC path helpers ──────────────────────────────────────────

FFIEC_DATASET = "call-reports-FFIEC"

def get_ffiec_storage_path(subdataset: str = "") -> Path:
    path = HDD_PATH / FFIEC_DATASET / subdataset if subdataset else HDD_PATH / FFIEC_DATASET
    path.mkdir(parents=True, exist_ok=True)
    return path

def get_ffiec_duckdb_path() -> Path:
    return get_ffiec_storage_path() / "call-reports-ffiec.duckdb"

def get_ffiec_raw_path(year: int, quarter: int) -> Path:
    p = get_ffiec_storage_path("raw") / f"{year}Q{quarter}"
    p.mkdir(parents=True, exist_ok=True)
    return p

def get_ffiec_staging_path(schedule: str, year: int, quarter: int) -> Path:
    p = get_ffiec_storage_path("staging") / schedule / f"year={year}" / f"quarter={quarter}"
    p.mkdir(parents=True, exist_ok=True)
    return p

def get_ffiec_manifest_path() -> Path:
    return get_ffiec_storage_path() / "download_manifest.json"

def get_ffiec_mdrm_path() -> Path:
    return get_ffiec_storage_path("mdrm")
```

### 2. `call-reports-FFIEC/metadata.py`

- `FORM_TYPES = ["031", "041", "051"]`
- `QUARTERS_START = (2001, 1)` and helper `all_quarters()` yielding (year, q) through current quarter
- `MDRM_URL = "https://www.federalreserve.gov/apps/mdrm/pdf/MDRM.zip"`
- `CDR_SOURCE_URL = "https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx"` (for documentation)
- `FILENAME_REGEX`: match `FFIEC CDR Call (Bulk (POR)|Schedule (\S+)) (\d{8})\.txt` → captures schedule_id + report_date
- `SCHEDULE_REGISTRY`: ordered dict mapping schedule_id → (view_name, human_description). Start with the known set (RC, RC-A…V, RI, RI-A…E, POR); `construct.py` will also auto-register any unrecognized schedules found in raw ZIPs so pipeline doesn't silently drop new ones.
- `PANEL_METADATA_DDL`: `CREATE TABLE IF NOT EXISTS panel_metadata (schedule VARCHAR, year INTEGER, quarter INTEGER, row_count BIGINT, source_zip VARCHAR, source_zip_sha256 VARCHAR, parquet_path VARCHAR, built_at TIMESTAMP, PRIMARY KEY (schedule, year, quarter))`
- `MDRM_DICTIONARY_DDL` for the reference table

### 3. `call-reports-FFIEC/schema.py`

- `FilerRecord` TypedDict (POR fields: idrssd, form_type, report_date, fi_name, address, city, state, zip, primary_aba, primary_reg, charter_type, financial_sub_indicator, etc. — field list from actual POR header)
- No per-schedule TypedDict (200+ MDRM codes per schedule; dynamic). Document this decision inline — `RawScheduleRow = Dict[str, str]`.

### 4. `call-reports-FFIEC/download.py`

CLI:
- `--mdrm` — download + extract `MDRM.zip` to `get_ffiec_mdrm_path()`. Uses `httpx` streaming with existing retry/backoff config (`HTTP_RETRIES`, `HTTP_BACKOFF_BASE`, `HTTP_CHUNK_SIZE`). Skip if `MDRM.csv` present unless `--force`.
- `--scan` — walk `get_ffiec_storage_path("raw")`, find `*.zip` at the root level, rename/move each to its `{YYYY}Q{Q}/` subfolder based on the report date in the filename, then update `download_manifest.json` with (zip_path, size, mtime, sha256, extract_status). Prints summary of found vs missing quarters.
- `--check` — print status table: which quarters have raw ZIPs, which are extracted, which are loaded into DuckDB (joined with `panel_metadata`).

Reuses patterns from `hmda/download.py`: `atomic_json_write()` for manifest, `retry_on_io_error()` from `utils/duckdb_utils.py`, same `httpx.Client` config block.

Prints user-facing instructions on first `--check` run: where to go on cdr.ffiec.gov, which product/period/format to select, where to save the ZIPs.

### 5. `call-reports-FFIEC/construct.py`

CLI:
- `--quarter 2024Q1` — process one quarter
- `--year 2024` — process all quarters for one year
- `--all` — process every quarter present in raw/
- `--force` — rebuild staging even if Parquet exists
- `--skip-views` — only write staging + panel_metadata, don't refresh views (useful when loading many quarters)

Pipeline per quarter:
1. **Extract** (if not already): unzip `raw/{YYYY}Q{Q}/*.zip` in-place. Uses stdlib `zipfile` (CDR ZIPs use standard Deflate, not Deflate64). Skip if inner TSVs already present.
2. **Discover schedules**: glob `raw/{YYYY}Q{Q}/FFIEC CDR Call *.txt`, parse each filename with `FILENAME_REGEX` → schedule_id.
3. **For each schedule TSV**:
   - Read via DuckDB `read_csv(delim='\t', header=true, all_varchar=true, ignore_errors=false)`
   - Skip leading "PUBLIC" description row if present (the CDR bulk files have a second header row describing column meaning in long text; detect by checking if second row starts with a non-MDRM-pattern value)
   - `COPY (SELECT ..., {year} AS activity_year, {quarter} AS activity_quarter FROM raw_tsv) TO '{staging_path}/data.parquet' (FORMAT PARQUET, COMPRESSION '{PARQUET_COMPRESSION}', ROW_GROUP_SIZE {PARQUET_ROW_GROUP_SIZE})`
   - Atomic: write to `data.parquet.tmp`, fsync, rename
4. **Upsert `panel_metadata`** via `upsert_row()` from `utils/duckdb_utils.py:70`.
5. **Recreate views** (unless `--skip-views`): for each schedule present in panel_metadata, `CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet('staging/{schedule}/year=*/quarter=*/data.parquet', union_by_name=true, hive_partitioning=true)`.
6. **Load MDRM dictionary** (if present and not yet loaded): `CREATE OR REPLACE TABLE mdrm_dictionary AS SELECT * FROM read_csv('mdrm/MDRM.csv', skip=1, all_varchar=true)`.

Reuses:
- `utils/duckdb_utils.py::get_connection`, `transactional_connection`, `ensure_table_exists`, `upsert_row`
- `utils/logging_utils.py` JSON structured logger
- Existing `DUCKDB_THREADS`, `DUCKDB_MEMORY_LIMIT`, `PARQUET_COMPRESSION`, `PARQUET_ROW_GROUP_SIZE` from `config.py`

### 6. `call-reports-FFIEC/README.md`

Sections, following the repo's README convention (see `cra/README.md`, `call-reports-CFLV/README.md`):
- Overview + source (FFIEC CDR bulk download, URL, product/period/format instructions for manual download)
- Coverage (forms, period)
- File layout
- How to connect + PRAGMA config
- Schedule registry table (schedule_id → view_name → description)
- Key conventions (IDRSSD join key, RCFD vs RCON prefix meaning, all-VARCHAR columns + TRY_CAST, YTD income semantics on RI)
- Common queries (single bank time series, top N by assets, join to `mdrm_dictionary` for column descriptions, join to `nic/relationships`)
- Known quirks (051 filers have RCFD NULL; POR "PUBLIC" header row; quarter-end date derived from filename not a column; schedule RC-R Basel capital columns change under regulatory reforms)
- Roadmap: harmonized concept view to be added as `call_reports_panel`

### 7. `call-reports-FFIEC/NOTES.md`

Operational notes: manual-download workflow, how to add a new schedule to the registry, CDR filename examples for 2001/2010/2020/2024 quarters (they differ slightly across eras), handling of amended filings (CDR replaces prior snapshots).

---

## Critical existing files referenced

| File | Why |
|------|-----|
| `config.py` | Edit to add `FFIEC_DATASET` + 6 path helpers (see section 1 above) |
| `utils/duckdb_utils.py:33-98` | Reuse `transactional_connection`, `ensure_table_exists`, `upsert_row` verbatim |
| `utils/logging_utils.py` | Reuse JSON logger |
| `hmda/download.py:88-228` | Pattern for `httpx` client + retry/backoff + ZIP extraction (used for MDRM download only) |
| `hmda/download.py:313-329` | Atomic manifest write pattern |
| `cra/construct.py` | Closest construct.py template (multiple tables per dataset, Hive-partitioned Parquet per table, panel_metadata pattern) |
| `call-reports-CFLV/README.md` | README style to match |
| `cra/metadata.py` | metadata.py structure reference |

No files outside `call-reports-FFIEC/` and `config.py` need modification.

---

## Verification (end-to-end)

Run these after implementation, with at least one quarterly CDR ZIP manually placed in `C:\empirical-data-construction\call-reports-FFIEC\raw\`:

1. **MDRM download**:
   ```bash
   C:\envs\.basic_venv\Scripts\python.exe -m call-reports-FFIEC.download --mdrm
   ```
   Expect `MDRM.csv` under `…\mdrm\` (~10 MB, ~30k rows).

2. **Raw scan**:
   ```bash
   C:\envs\.basic_venv\Scripts\python.exe -m call-reports-FFIEC.download --scan
   C:\envs\.basic_venv\Scripts\python.exe -m call-reports-FFIEC.download --check
   ```
   Expect table of discovered quarters and their extract/load status.

3. **Construct one quarter** (pick recent: e.g. 2024Q4):
   ```bash
   C:\envs\.basic_venv\Scripts\python.exe -m call-reports-FFIEC.construct --quarter 2024Q4
   ```
   Expect ~20+ Parquet files under `staging/{schedule}/year=2024/quarter=4/`.

4. **DuckDB query sanity**:
   ```sql
   -- row counts
   SELECT * FROM panel_metadata ORDER BY schedule, year, quarter;

   -- filer count (should be ~4,500 for 2024Q4)
   SELECT COUNT(DISTINCT idrssd) FROM call_filers WHERE activity_year = 2024 AND activity_quarter = 4;

   -- Bank of America total assets (RCFD2170, in thousands)
   SELECT idrssd, TRY_CAST(rcfd2170 AS DOUBLE) / 1e6 AS assets_bn
   FROM schedule_rc
   WHERE activity_year = 2024 AND activity_quarter = 4
     AND idrssd = '480228';
   ```

5. **Cross-validate against CFLV**: for the same (id_rssd, date), total-asset values from `schedule_rc.rcfd2170` should match `balance_sheets.assets` in `call-reports-CFLV/call-reports-cflv.duckdb` within rounding.

6. **Multi-quarter smoke** (optional, after loading 4+ quarters): `SELECT activity_year, activity_quarter, COUNT(*) FROM schedule_rc GROUP BY 1,2 ORDER BY 1,2` — confirms Hive partition pruning works and no quarter is missing.

7. **MDRM join**: `SELECT * FROM mdrm_dictionary WHERE item_code IN ('RCFD2170','RCON2170')` — confirms dictionary loaded and codes are findable.

If all six checks pass, v1 is ready. Future work (harmonized concept view) tracked in `NOTES.md`.
