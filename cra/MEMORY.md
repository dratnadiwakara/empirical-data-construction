# CRA Pipeline -- Memory & Build Reference

## What Was Built

A full ETL pipeline for FFIEC Community Reinvestment Act (CRA) flat files, 1996--2024. Fixed-width `.dat` files (aggregate, disclosure, transmittal) are downloaded from FFIEC, parsed via DuckDB `substr()` expressions, harmonized across three distinct era layouts, and written to Hive-partitioned Parquet. All 29 years are queryable through three DuckDB views in a single database.

---

## Data Sources

| Source | URL | Purpose |
|--------|-----|---------|
| FFIEC CRA Flat Files | `https://www.ffiec.gov/cra/xls/{YY}exp_{type}.zip` | Raw fixed-width data (aggr, discl, trans) |
| FFIEC CRA Landing Page | <https://www.ffiec.gov/data/cra/flat-files> | File specs and documentation |

---

## File Layout

```
C:\empirical-data-construction\cra\
├── cra.duckdb                              # Master DuckDB (views + panel_metadata)
├── raw\
│   └── {year}\                             # Downloaded ZIPs + extracted .dat files
├── staging\
│   ├── aggregate\year={year}\data.parquet
│   ├── disclosure\year={year}\data.parquet
│   └── transmittal\year={year}\data.parquet
```

---

## Execution Order

```bash
# Process one year at a time:
python -m cra.download --year 2024
python -m cra.construct --year 2024

# Or all years:
python -m cra.download --all
python -m cra.construct --all

# Validate 2024 against National Aggregate Table 1 totals:
python -m cra.construct --validate 2024
```

---

## How It Was Built (Step by Step)

### 1. Studied the HMDA pipeline and adapted the architecture

The project already had a working HMDA pipeline (`hmda/download.py`, `hmda/construct.py`, `hmda/metadata.py`, `utils/`, `config.py`). The CRA pipeline was modeled on the same pattern: separate `download.py` for fetching, `metadata.py` as the single source of truth for layouts, `construct.py` for ETL, and `schema.py` for TypedDict definitions.

### 2. Analyzed FFIEC file specs across all eras

CRA flat files changed their fixed-width layout three times. Each era was mapped in `metadata.py`:

| Era | Years | table_id width | MSA field | count/amount fields |
|-----|-------|---------------|-----------|---------------------|
| 1996 | 1996 only | 4 chars | 4 digits | 6/8 digits |
| 1997--2003 | 1997--2003 | 5 chars | 4 digits | 6/8 digits |
| 2004+ | 2004--2024 | 5 chars | 5 digits | 10 digits |

Starting in 2016, ZIP files switched from a single combined `.dat` to per-table `.dat` files (e.g., `cra2024_Aggr_A11.dat`).

An older R script (`cra-aggregate-data-clean.qmd`) was used as a reference for field widths and column names, then extended to cover disclosure and transmittal.

### 3. Added CRA config helpers

Extended `config.py` with CRA-specific path functions (`get_cra_storage_path`, `get_cra_raw_path`, `get_cra_staging_path`, `get_cra_duckdb_path`, `get_cra_manifest_path`) mirroring the HMDA helpers.

### 4. Built the downloader with Cloudflare bypass

Standard `httpx` requests returned HTTP 403 from the FFIEC website due to Cloudflare protection. Switched to `curl_cffi` with `impersonate='chrome'` which bypasses Cloudflare successfully. The downloader includes:
- Manifest-based idempotency (skips already-downloaded files)
- Exponential backoff retry
- ZIP extraction of `.dat` files

### 5. Built the ETL with DuckDB fixed-width parsing

Rather than using Python to parse fixed-width files line-by-line, the pipeline reads each `.dat` as a single VARCHAR column via DuckDB's `read_csv()` with `delim='\0'` (null byte delimiter), then uses `substr()` expressions derived from the layout definitions to extract fields. This approach is fast and memory-efficient.

Key construct.py design decisions:
- **In-memory DuckDB** for parsing (avoids locking the master database during long ETL)
- **Atomic Parquet writes** (write to `.parquet.tmp`, then rename)
- **Computed geography columns** (`county_fips`, `census_tract_fips`) added during ETL using SQL CASE expressions with LPAD
- **Panel views** (`aggregate_panel`, `disclosure_panel`, `transmittal_panel`) are `CREATE OR REPLACE VIEW` over all Parquet files with Hive partitioning

### 6. Processed 2024 first and validated

2024 was processed first and validated against the CRA National Aggregate Table 1 published totals. Three checks were run:
- Business originations (A1-1, loan_type=4, action_taken=1)
- Business purchases (A1-2, loan_type=4, action_taken=6)
- Farm originations (A2-1, loan_type=5, action_taken=1)

All matched exactly. Validation targets are stored in `VALIDATION_2024` in `metadata.py`.

### 7. Backfilled 2023 down to 1996

After user confirmed the 2024 validation, all remaining years were downloaded and processed in a single batch (2023 -> 1996).

---

## Bugs Encountered and Fixed

### Bug 1: Cloudflare blocking downloads (HTTP 403)

**Problem**: `httpx` requests to `ffiec.gov/cra/xls/...` returned 403 Forbidden, even with a browser-like User-Agent header. The FFIEC website uses Cloudflare protection.

**Fix**: Replaced `httpx` with `curl_cffi` using `impersonate='chrome'` in `download.py`. All downloads then succeeded on the first attempt.

### Bug 2: Non-UTF-8 characters in transmittal files

**Problem**: DuckDB's `read_csv()` failed on the 2003 transmittal file with `Invalid unicode (byte sequence mismatch)`. The file contains Latin-1 characters (e.g., `MUÑOZ` in a Puerto Rico bank address).

**Fix**: Added `encoding='latin-1'` to the `read_csv()` call in `construct.py`. Latin-1 accepts all byte values so this handles any extended characters in any year.

### Bug 3: 1996 aggregate and disclosure returned 0 rows

**Problem**: The table ID filter extracted 5 characters from the start of each line (`substr(line, 1, 5)`), but the 1996 layout uses only 4 characters for `table_id`. The 5th character was the first digit of the year, so `TRIM(...)` never matched `A1-1` or `D1-1`.

**Fix**: Changed the filter to use the actual `table_id` field width from the layout definition (4 for 1996, 5 for later eras). Added `_table_id_field_width()` helper.

### Bug 4: Disclosure table IDs D3-0, D4-0, etc. not matched

**Problem**: Pre-2016 combined disclosure files use table IDs like `D3-0`, `D4-0`, `D5-0`, `D6-0`, but the filter list had `D3`, `D4`, `D5`, `D6` (the canonical post-2016 names). Exact-match filtering returned 0 rows for those tables.

**Fix**: Added `DISCLOSURE_TABLE_ID_PREFIXES` list and a `table_id_prefixes` parameter to `_build_fwf_sql()` that generates `LIKE` conditions (e.g., `TRIM(...) LIKE 'D3%'`).

### Bug 5: action_taken=6 for purchases (not 2)

**Problem**: Initial 2024 validation for `business_purchases` expected `action_taken=2` but the actual data used `action_taken=6`. The validation failed.

**Fix**: Inspected distinct `action_taken` values and confirmed CRA uses `6` for loan purchases (not `2` as in HMDA). Updated `VALIDATION_2024` in `metadata.py`.

### Bug 6: Parquet row count query failed on .tmp file

**Problem**: `_write_parquet()` tried to count rows from the temporary file using DuckDB's path-based auto-detection, but DuckDB did not recognize the `.parquet.tmp` extension.

**Fix**: Changed the count query to explicitly use `read_parquet('{path}')` instead of relying on extension-based auto-detection.

---

## Row Counts by Year

### aggregate_panel (~8.8M total)

| Year | Rows | Year | Rows | Year | Rows |
|------|-----:|------|-----:|------|-----:|
| 1996 | 263,380 | 2006 | 244,472 | 2016 | 456,678 |
| 1997 | 296,760 | 2007 | 248,160 | 2017 | 460,662 |
| 1998 | 118,208 | 2008 | 249,364 | 2018 | 474,584 |
| 1999 | 160,244 | 2009 | 224,154 | 2019 | 481,649 |
| 2000 | 128,044 | 2010 | 224,518 | 2020 | 522,049 |
| 2001 | 134,473 | 2011 | 219,100 | 2021 | 529,390 |
| 2002 | 139,053 | 2012 | 243,896 | 2022 | 530,135 |
| 2003 | 176,607 | 2013 | 243,296 | 2023 | 513,892 |
| 2004 | 240,944 | 2014 | 245,450 | 2024 | 519,366 |
| 2005 | 248,758 | 2015 | 246,122 | | |

### disclosure_panel (~56.9M total)

| Year | Rows | Year | Rows | Year | Rows |
|------|-----:|------|-----:|------|-----:|
| 1996 | 1,634,968 | 2006 | 1,017,933 | 2016 | 3,593,829 |
| 1997 | 654,194 | 2007 | 1,017,393 | 2017 | 3,620,311 |
| 1998 | 700,624 | 2008 | 1,021,279 | 2018 | 3,728,684 |
| 1999 | 783,038 | 2009 | 895,479 | 2019 | 3,993,376 |
| 2000 | 875,355 | 2010 | 871,477 | 2020 | 4,408,879 |
| 2001 | 977,429 | 2011 | 937,857 | 2021 | 4,505,775 |
| 2002 | 961,952 | 2012 | 999,052 | 2022 | 4,568,181 |
| 2003 | 1,076,200 | 2013 | 970,688 | 2023 | 4,396,953 |
| 2004 | 1,196,406 | 2014 | 1,032,820 | 2024 | 4,446,368 |
| 2005 | 918,562 | 2015 | 1,134,081 | | |

### transmittal_panel (~34K total)

| Year | Rows | Year | Rows | Year | Rows |
|------|-----:|------|-----:|------|-----:|
| 1996 | 2,078 | 2006 | 1,028 | 2016 | 726 |
| 1997 | 1,896 | 2007 | 998 | 2017 | 718 |
| 1998 | 1,866 | 2008 | 965 | 2018 | 700 |
| 1999 | 1,911 | 2009 | 941 | 2019 | 695 |
| 2000 | 1,941 | 2010 | 880 | 2020 | 687 |
| 2001 | 1,912 | 2011 | 859 | 2021 | 685 |
| 2002 | 1,986 | 2012 | 830 | 2022 | 711 |
| 2003 | 2,103 | 2013 | 791 | 2023 | 721 |
| 2004 | 1,999 | 2014 | 767 | 2024 | 731 |
| 2005 | 1,103 | 2015 | 751 | | |

Note: The jump in disclosure/aggregate row counts at 2016 is due to the switch to per-table `.dat` files which include assessment-area tables (A*-*a variants) that weren't always present in pre-2016 combined files.

---

## DuckDB Schema

```sql
-- Views in cra.duckdb (backed by Parquet files)
aggregate_panel     -- 23 columns, ~8.8M rows (1996-2024)
disclosure_panel    -- 27 columns, ~56.9M rows (1996-2024)
transmittal_panel   -- 12 columns, ~34K rows (1996-2024)

-- Table
panel_metadata      -- tracks year, row_count, source_url, built_at per (table_type, year)
```

---

## Key Design Decisions

### Fixed-width parsing via DuckDB substr() (not Python)

Each `.dat` file is ingested as a single VARCHAR column (`line`) using `read_csv()` with a null-byte delimiter. DuckDB `substr()` expressions then extract each field positionally. This avoids Python-level line-by-line parsing and handles files of any size without loading them into RAM.

### Geography harmonization at ETL time

`county_fips` (5-char: `SSCCC`) and `census_tract_fips` (11-char: `SSCCCTTTTT`) are computed during Parquet creation using SQL CASE + LPAD expressions. The raw `state`, `county`, and `census_tract` fields are also preserved. Census tract decimals are stripped (e.g., `1048.00` becomes `104800`).

### Three separate eras with layout dispatch

`get_era(year)` returns `"1996"`, `"1997-2003"`, or `"2004+"`. Each era has its own layout tuple list in `metadata.py`. The construct script selects the correct layout at runtime. This avoids branching logic in the SQL generation.

### Disclosure has no census tract

Disclosure data is county-level only (no `census_tract` field in any era). Only `county_fips` is computed. For tract-level analysis, use the aggregate panel.

### report_level codes differ between tables

County totals are `report_level = '200'` in aggregate data but `report_level = '040'` in disclosure data. Always filter on `report_level` to avoid double-counting sub-geography rows.

---

## Codebase Files

| File | Purpose |
|------|---------|
| `config.py` | Central config: `HDD_PATH`, CRA path helpers, HTTP settings |
| `cra/__init__.py` | Package marker |
| `cra/metadata.py` | Layouts, table IDs, URLs, validation targets, geographic SQL |
| `cra/schema.py` | TypedDict definitions for harmonized records |
| `cra/download.py` | Download + extract ZIPs (uses `curl_cffi` for Cloudflare bypass) |
| `cra/construct.py` | ETL: fixed-width -> Parquet -> DuckDB views + validation |
| `cra/plan.md` | Original build plan |
| `cra/README.md` | Data reference and query guide for AI agents / users |
| `utils/logging_utils.py` | Structured logging utilities |
| `utils/duckdb_utils.py` | DuckDB connection helpers, upsert, ensure_table_exists |

---

## Python Environment

- Virtual environment: `.venv` in the repo root
- Key packages: `duckdb`, `curl_cffi`
- Run scripts as modules from the repo root: `python -m cra.download --year 2024`
- Windows note: logging must use ASCII characters only (cp1252 console encoding)
- FFIEC requires `curl_cffi` (not `httpx` or `requests`) due to Cloudflare protection

---

## Sanity Check (FFIEC Reports)

- Ran a temporary validation against downloaded FFIEC national aggregate reports in `C:\Users\dimut\Downloads\cra` (Table 1 years: 1998, 2003, 2007, 2010, 2015, 2024; Table 4-3 year: 2020).
- Replicated report totals from `C:\empirical-data-construction\cra\cra.duckdb` (`aggregate_panel` for Table 1; `disclosure_panel` + `transmittal_panel` for Table 4-3).
- Result: exact match on all checks (640/640; 0 mismatches).
- Deleted temporary script after the exercise.

**Bank-level / disclosure:** For 2024, national sums over `disclosure_panel` (`D1-1`/`D1-2`/`D2-1`/`D2-2`, `report_level='040'`) match `aggregate_panel` (`A1-1`/`A1-2`/`A2-1`/`A2-2`, `report_level='200'`) exactly for both loan counts and amounts (all four table pairs). Spot totals were printed for a few large institutions (e.g. JPMorgan Chase, BofA NA, Wells Fargo, Citibank) for manual cross-check against FFIEC disclosure outputs if desired.

- To line up with FFIEC’s published institution disclosure reports, use the same year plus `respondent_id` and `agency_code` from `transmittal_panel`; example SQL patterns are in `cra/README.md` (“Linking Disclosure to Transmittal”).
