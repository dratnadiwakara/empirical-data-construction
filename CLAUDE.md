# CLAUDE.md

## Project Overview
Modular ETL framework converting publicly available US regulatory financial data into research-ready DuckDB datasets. Goal: AI agents can construct empirical research samples (e.g., mortgage lending panels, bank structure panels) without manual data wrangling.

**Current datasets:**
| Dataset | Description | Years | Scale | DuckDB |
|---------|-------------|-------|-------|--------|
| **HMDA** | Home Mortgage Disclosure Act loan application records | 2000–2024 | ~536M rows | `hmda/hmda.duckdb` → view `hmda` |
| **CRA** | Community Reinvestment Act disclosure filings | 1996–2024 | aggregate/disclosure/transmittal | `cra/cra.duckdb` |
| **NIC** | FFIEC National Information Center entity relationships & structural changes | Snapshots (version-based) | — | `nic/nic.duckdb` |
| **SOD** | FDIC Summary of Deposits — branch-level deposits for all FDIC-insured institutions | 1994–present | ~2.6M rows | `sod/sod.duckdb` → view `sod` |
| **IRS** | IRS SOI individual income tax ZIP code panel — returns, AGI, wages, dividends, business income, capital gains | 1998–2022 (gaps: 1999/2000/2003) | ~27k–40k ZIPs/year | `irs/irs.duckdb` → view `irs` |
| **Y-9C** | FR Y-9C consolidated quarterly financial filings by US Bank Holding Companies | 2000-Q1 → 2025-Q4 | ~104 quarters, ~350-1,800 filers/qtr | `y9c/y9c.duckdb` → views `y9c_raw`, `bs_panel_y9c`, `is_panel_y9c`, `y9c_panel` |

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

### IRS SOI
- **2 source eras**: Era A (1998–2010) ZIP archive → state Excel files; Era B (2011–2022) national CSV
- **Missing years**: 1999, 2000, 2003 — IRS never published; treat as gaps in panels, not zeros
- **AGI stubs**: raw files have 6–7 income-bracket rows per ZIP; pipeline sums all bracket stubs and excludes stub 0 (duplicate total)
- **Column gaps**: 1998/2001/2002 lack dividend/business/capgain columns; 2008 lacks all N-series count columns — these are NULL, not bugs
- **Join key**: `zipcode` (VARCHAR 5-digit, LPAD-padded) — joins to SOD, HMDA, CRA on zipcode/year
- **Units**: all `agi_*` columns in **$thousands** throughout entire series (pipeline corrects 2007/2008 IRS anomaly)

## Construction & Update Principles
1. **Raw-to-Staging**: ZIP/CSV → compressed Parquet immediately after download
2. **Idempotent**: manifest prevents re-download; `--force` rebuilds; `--update` checks for new releases only
3. **Schema Harmonization**: metadata.py maps all eras to unified column names/types; numeric codes → string labels
4. **Self-Documenting**: every DuckDB has `panel_metadata` table with harmonized variable names, source names, types, value ranges, year availability

## Validation & Sanity Checks (Required for Every Dataset)

Data quality is critical — this data is used across multiple research projects by AI agents. Every dataset must pass the following checks after construction. Catch problems at build time, not at analysis time.

### Row count checks
- Total row count must be non-zero and within expected range for each year
- Year-over-year row counts should not drop >20% unexpectedly (flag, don't fail)
- Compare against `panel_metadata.row_count` from prior build; alert on large deviations

### NULL rate checks
- For columns that should always be populated (primary keys, join keys, count fields): NULL rate must be 0
- For columns with known year-specific gaps (documented in README): NULL rate should be ~100% for those years and ~0% for others
- Flag unexpected NULLs in columns that should be populated

### Economic/statistical plausibility checks
- National aggregates should fall within published IRS/FDIC/FFIEC totals ±5%
- Per-unit averages (e.g., AGI per return, deposits per branch) should be in plausible range
- Year-over-year changes in national totals >30% warrant investigation (crisis years are documented exceptions)
- No negative values in count or amount fields

### Cross-year consistency checks
- Same observation unit (ZIP, branch, tract) should have smooth time series — large jumps may indicate unit change or scaling error
- Check at least one well-known stable unit (e.g., a large-city ZIP) across all years
- Verify derived fractions (salary_frac, etc.) are bounded [0, 1]

### Implementation
- Add an `inspect.py` module to each dataset (HMDA already has one) with functions: `check_row_counts()`, `check_null_rates()`, `check_plausibility()`
- Run automatically at end of `construct --all`
- Log WARN for soft failures, ERROR for hard failures; never silently pass bad data
- Document known legitimate anomalies in dataset README so future agents don't re-investigate them

## Agent Instructions

### Before querying any dataset
1. Read the dataset's `README.md` first — it documents schema, units, column availability by year, known anomalies, and example queries
2. Run `SELECT * FROM panel_metadata ORDER BY year` to verify years are built and get row counts
3. Check column availability for the years you need — many datasets have NULL columns in early years
4. Always open DuckDB **read-only** unless you are running the ETL pipeline: `duckdb.connect(path, read_only=True)`

### Connecting to a dataset
```python
import duckdb
from config import get_irs_duckdb_path  # or get_sod_duckdb_path, etc.

db = duckdb.connect(str(get_irs_duckdb_path()), read_only=True)
# Main view is always named after the dataset: irs, sod, hmda, cra
```

### Units and join keys
- **IRS**: `agi_*` in $thousands; join key `zipcode` (VARCHAR 5-digit) + `year`
- **SOD**: deposits in $thousands; join key `uninumbr` (RSSD), `cert` (FDIC cert), `zipbr` (branch ZIP) + `year`
- **HMDA**: loan amounts vary by era (harmonized in view); join key `lei`/`respondent_id` + `year`; geography via `census_tract` or ZIP
- **CRA**: dollar amounts in $thousands; join key `respondent_id` + `year` + `activity_year`
- **NIC**: entity relationships; join key `rssd_id`

### Writing correct time-series queries
- Missing years are gaps, not zeros — use `LEFT JOIN` or handle NULLs explicitly
- Deflate nominal dollar amounts before comparing levels across years
- Filter out suppressed small cells: `WHERE n_returns >= 10` (IRS), `WHERE depsumbr > 0` (SOD)
- For panel regressions: include year fixed effects; mark gap years as missing

### Path resolution
- Never hardcode `C:\empirical-data-construction` — use `config.py` helpers
- Pattern: `get_{dataset}_duckdb_path()`, `get_{dataset}_raw_path(year)`, `get_{dataset}_staging_path(year)`

### Safe writes
- Always atomic writes: write to `.tmp` file, then `rename()` — never write directly to final path
- Use `duckdb_utils.upsert_row()` and `ensure_table_exists()` for panel_metadata updates

### Adding a new dataset
Create: `{dataset}/download.py`, `construct.py`, `metadata.py`, `schema.py`, `README.md`, `__init__.py`
- `metadata.py`: URL registry, era boundaries, field maps, DDL — single source of truth
- `README.md`: must include Quick Start block, full schema table with units, column availability by year, key constraints, 4+ example queries, join examples with other datasets
- `inspect.py`: validation checks (row counts, NULL rates, plausibility) — run at end of `construct --all`
- Follow IRS or SOD as the reference implementation

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
