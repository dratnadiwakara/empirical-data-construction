# CLAUDE.md

## Project Overview
Modular ETL framework converting publicly available US regulatory financial data into research-ready DuckDB datasets. Goal: AI agents can construct empirical research samples (e.g., mortgage lending panels, bank structure panels) without manual data wrangling.

**Current datasets:**
| Dataset | Description | Years | Scale |
|---------|-------------|-------|-------|
| **HMDA** | Home Mortgage Disclosure Act loan application records | 2000–2024 | ~536M rows |
| **CRA** | Community Reinvestment Act disclosure filings | 1996–2024 | aggregate/disclosure/transmittal |
| **NIC** | FFIEC National Information Center entity relationships & structural changes | Snapshots (version-based) | — |

## Architecture & Directory Structure

```
{dataset}/           # e.g., hmda/, cra/, nic/
  download.py        # Fetch raw files with resume, retry, idempotency manifest
  construct.py       # ETL: raw → Parquet staging → DuckDB
  metadata.py        # URLs, era boundaries, field layouts, column mappings (single source of truth)
  schema.py          # TypedDict definitions for harmonized records
  README.md          # Dataset-specific notes
  MEMORY.md          # Agent memory for that dataset
  plan.md            # Construction plan/notes
utils/
  duckdb_utils.py    # Connection management, upsert/ensure_table helpers
  logging_utils.py   # JSON structured logging
config.py            # Central config: HDD paths, HTTP settings, Parquet/DuckDB tuning
```

**HMDA-only extra modules:**
- `arid_xref.py` — FFIEC crosswalk bridging pre-2018 ARID → post-2018 LEI
- `avery.py` — Philadelphia Fed lender crosswalk mapping respondent_id/LEI → RSSD ID
- `inspect.py` — Post-build validation (row counts, null rates, loan distributions)

## Data Storage Strategy (Mandatory)
- **Codebase**: Local machine (GitHub repo).
- **Data**: All heavy files **MUST** live under `C:\empirical-data-construction`.
- **Path Resolution**: Never hardcode paths. Use `FIN_DATA_ROOT` env var (default: `C:\empirical-data-construction`) via `config.py`.

**HDD layout per dataset:**
```
C:\empirical-data-construction\{hmda|cra|nic}\
  {dataset}.duckdb              # Master DB: harmonized views + panel_metadata table
  download_manifest.json        # Idempotency tracking (size, mtime, sha256, etag)
  raw\                          # Downloaded ZIPs + extracted files
  staging\                      # Hive-partitioned snappy Parquet (intermediate)
```

## Python Environment
- **Always use**: `C:\envs\.basic_venv`
  - Run: `C:\envs\.basic_venv\Scripts\python.exe <script>`
  - Install: `C:\envs\.basic_venv\Scripts\pip.exe install <package>`
  - Activate (bash): `source C:/envs/.basic_venv/Scripts/activate`
- Never use system Python or other venvs.

## R Environment
- **Installation**: `C:\Program Files\R\R-4.5.3`
  - Run scripts: `"C:\Program Files\R\R-4.5.3\bin\Rscript.exe" <script.R>`

## Technical Stack & Constraints
- **Languages**: Python 3.10+, SQL
- **Processing**: DuckDB (out-of-core SQL, storage, 100M+ row joins), Polars (in-memory wrangling where needed)
- **Download**: `httpx` / `requests` with streaming; 8 MB chunks, 5 retries, exponential backoff (base 2.0, cap 120s)
- **NO pandas** on large data — always `polars.scan_parquet()` or DuckDB native Parquet readers
- **DuckDB config**: 4 threads, 6 GB memory limit (tuned for 8 GB machine)
- **Parquet config**: snappy compression, 260k row-group size

## ETL Pipeline Patterns

### download.py
- HTTP Range header resume
- `download_manifest.json` tracks size/mtime/sha256/etag → skip unchanged files
- `--force` flag overrides idempotency check
- Automatic ZIP extraction after download

### construct.py
- Pipeline: Raw → Staging Parquet (snappy, Hive-partitioned by year) → DuckDB views
- All transforms via DuckDB SQL (out-of-core, no full DataFrame in memory)
- Era dispatch: each dataset has multiple schema eras handled via metadata-driven CASE/mapping
- Atomic writes: write temp file, rename to prevent corruption

### metadata.py (single source of truth)
- URL registry per year/era
- Era boundary definitions (year cutoffs where source schema changed)
- Field layout specs (fixed-width offsets for CRA, column name maps for HMDA)
- Unit-scaling rules (e.g., HMDA pre-2018 loan_amount ×1000)
- Table/dataset registry with descriptions

## Dataset-Specific Notes

### HMDA
- **4 schema eras**: 2018–2024 (CFPB), 2017 (transition), 2007–2016 (FFIEC), 2000–2006 (ICPSR pipe-delimited)
- **Key harmonization**: pre-2018 uses ARID+agency_code as lender ID; post-2018 uses LEI → `arid_xref.py` bridges them
- **Lender linking**: `avery.py` maps any lender ID to RSSD (Fed bank structure ID)
- **Census tract**: constructed FIPS from state/county/tract fields across eras

### CRA
- **3 field-width eras**: 1996 | 1997–2003 | 2004–2024; file split changed at 2016
- **3 tables**: aggregate, disclosure, transmittal (parsed via DuckDB `substr()` from fixed-width .dat)
- **Source**: FFIEC ZIP files

### NIC
- **Snapshot-based** (not year-partitioned): version detection via SHA-256 + etag
- **2 datasets**: relationships, transformations
- **Source**: FFIEC NIC CSVs

## Construction & Update Principles
1. **Raw-to-Staging**: ZIP/CSV → compressed Parquet immediately after download
2. **Idempotent**: manifest prevents re-download; `--force` rebuilds; `--update` checks for new releases only
3. **Schema Harmonization**: metadata.py maps all eras to unified column names/types; numeric codes → string labels
4. **Self-Documenting**: every DuckDB has `panel_metadata` table with harmonized variable names, source names, types, value ranges, year availability

## Agent Instructions
- **Before querying**: check `panel_metadata` table in the `.duckdb` to understand harmonized schema
- **Path resolution**: use `config.py` helpers, never hardcode `C:\empirical-data-construction`
- **Safe writes**: always atomic (temp file → rename) when updating master DuckDB
- **Ingestion**: use DuckDB `COPY` or `INSERT INTO SELECT` from Parquet
- **Validation**: row counts + null checks after each construction run (`inspect.py` for HMDA)
- **New dataset**: follow existing pattern — create `download.py`, `construct.py`, `metadata.py`, `schema.py`, `README.md`, `MEMORY.md`

## config.py Reference

```python
import os
from pathlib import Path

HDD_PATH = Path(os.getenv("FIN_DATA_ROOT", r"C:\empirical-data-construction"))

def get_storage_path(dataset: str) -> Path:
    path = HDD_PATH / dataset
    path.mkdir(parents=True, exist_ok=True)
    return path
```

Path helpers follow pattern: `get_{dataset}_{type}_path(year?)` → e.g., `get_hmda_staging_path("lar", 2020)`.
