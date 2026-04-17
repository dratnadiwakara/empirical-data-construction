# CFLV Call Reports -- Implementation Plan

## Context

Convert the Correia-Fermin-Luck-Verner (CFLV) historical Call Reports dataset (FRBNY
Liberty Street, Jan 2026 release) from Stata .dta files into a research-ready DuckDB
database following the project's standard ETL pattern.

**Goal:** One-time conversion. No download.py (user pre-downloaded). No update logic.
Focus: clean schema, full variable documentation with MDRM codes, agent-queryable metadata.

**Source:** Two Stata .dta files (balance sheets ~2.6 GB, income statements ~719 MB uncompressed)
plus an Excel data dictionary (12 MB, 4 sheets) containing MDRM code mappings.

**User decisions:**
- Date stored as quarter-end DATE (Mar 31, Jun 30, Sep 30, Dec 31)
- Income statement YTD values kept as-is (no quarterly decomposition)
- Source date format: Stata tq integer (quarters since Q1-1960) → must convert

---

## Files Created

| File | Purpose |
|------|---------|
| `construct.py` | ETL: ZIP extract, DTA→Parquet, Parquet→DuckDB, panel_metadata, validation |
| `metadata.py` | Complete variable registry: 158 balance sheet + 69 income statement vars with MDRM codes |
| `schema.py` | TypedDicts: BalanceSheetRecord, IncomeStatementRecord |
| `variables.md` | Human-readable docs with MDRM codes, valid periods, sample queries |
| `README.md` | Quick-start guide |
| `MEMORY.md` | Operational memory for future agents |
| `plan.md` | This file |

**Also modified:** `config.py` — added `get_cflv_storage_path()`, `get_cflv_duckdb_path()`,
`get_cflv_raw_path()`, `get_cflv_staging_path()` path helpers.

---

## ETL Architecture

```
Downloads/call-reports-CFLV/*.zip
    ↓ zipfile.ZipFile.extractall()
raw/*.dta  (2.6 GB + 719 MB)
    ↓ pyreadstat.read_file_in_chunks() [100K rows/chunk]
    ↓ pl.from_pandas() → date conversion → group_by(year)
staging/{balance_sheets,income_statements}/year=YYYY/part-N.parquet
    ↓ DuckDB read_parquet(..., hive_partitioning=true)
call-reports-cflv.duckdb
    Tables: balance_sheets (190 cols), income_statements (71 cols), panel_metadata (258 rows)
```

### Date Conversion (critical)

Source Stata tq format: integer = quarters since Q1-1960.
- 0 = Q1 1960 → 1960-03-31
- -1 = Q4 1959 → 1959-12-31
- 104 = Q1 1986 → 1986-03-31

Python formula:
```python
year = 1960 + tq // 4
quarter = tq % 4 + 1   # 1..4
month = quarter * 3     # 3, 6, 9, 12
day = calendar.monthrange(year, month)[1]
```

Python floor division handles negatives correctly: `-1 // 4 = -1` (not 0).

### Parquet Write Pattern

Each chunk grouped by year → year-partitioned shards.
Part counters per year tracked to avoid overwriting shards from previous chunks
of the same year.

### DuckDB Load

```sql
CREATE OR REPLACE TABLE balance_sheets AS
SELECT * EXCLUDE (year)
FROM read_parquet('staging/balance_sheets/**/*.parquet', hive_partitioning = true)
```

`EXCLUDE (year)` drops the Hive partition column from the table
(year is derivable from `date`).

---

## Metadata Source

All variable definitions derived from `historical_call_data_dictionary.xlsx`:
- Sheet "balance sheet": 221 rows, col order: Dataset Label | Description | Mnemonic | Field | Valid Period | Notes
- Sheet "income statement": 134 rows, same structure
- Multi-row variables: first row has label+description, continuation rows have None for label (same variable, different era codes)
- Dates in Valid Period use YYYYMMDD integers: `dt >= 19591231 & dt < 19840331`

---

## Technical Notes

- `pyreadstat` required (not pre-installed): `pip install pyreadstat`
- `pyreadstat.read_file_in_chunks()` returns a **generator**, not a context manager — do NOT use `with` statement
- Windows cp1252 terminal cannot render Unicode arrows `→` or em-dashes `—` in log messages — use ASCII only
- Folder name `call-reports-CFLV` has hyphens → not a valid Python package name → import `metadata.py` via `importlib.util.spec_from_file_location()`
- Forward slashes required in DuckDB glob patterns even on Windows

---

## Validation Results (Jan 2026 run)

```
balance_sheets:     2,548,675 rows | 1959-12-31 to 2025-09-30 | 24,716 banks
income_statements:  2,548,675 rows | 1959-12-31 to 2025-09-30 | 24,716 banks
panel_metadata:     258 entries

Null rates (balance_sheets):  assets=3.4%, deposits=3.4%, equity=0.0%, ln_tot=23.6%
Null rates (income_statements): ytdnetinc=23.0%, ytdint_inc=22.9%, ytdint_exp=22.9%
```

High null rates for `ln_tot` and income items expected: pre-1976Q1 and pre-1969Q4
data uses different field structures; these variables not available in early periods.

---

## Future Updates

When a new CFLV release is published:
1. Download new ZIP files to `C:\Users\dimut\Downloads\call-reports-CFLV\`
2. Update `SOURCE_FILES` dict in `metadata.py` to point to new filenames
3. Run: `python construct.py --force`

No incremental update logic — full rebuild from new source files each release.
