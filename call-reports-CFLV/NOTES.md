# CFLV Call Reports Pipeline -- Memory & Reference

## What Was Built

Full one-time ETL pipeline converting the Correia-Fermin-Luck-Verner (CFLV) historical
Call Reports dataset (FRBNY Liberty Street, Jan 2026 release) from Stata .dta files
to a research-ready DuckDB database.

**Coverage:** 1959-Q4 through 2025-Q3, **2,548,675** quarterly bank observations,
**24,716** unique banks.

**Source:** Correia, Fermin, Luck, Verner (2025). *A Long-Run History of Bank Balance
Sheets and Income Statements.* FRBNY Liberty Street Economics, December 22, 2025.
https://libertystreeteconomics.newyorkfed.org/2025/12/a-long-run-history-of-bank-balance-sheets-and-income-statements/

---

## Source Files

Downloaded Jan 2026 release from the Liberty Street article page:

```
C:\Users\dimut\Downloads\call-reports-CFLV\
  call-reports-balance-sheets-Jan2026.zip    # 488 MB zip, 2.6 GB DTA
  call-reports-income-statements-Jan2026.zip # 172 MB zip, 719 MB DTA
  historical_call_data_dictionary.xlsx       # 12 MB; 4 sheets: balance sheet, income statement, raw variables, metadata
```

The data dictionary xlsx is the authoritative source for MDRM code mappings. Its structure:
- **Sheet "balance sheet"**: 221 rows, maps 158+ dataset variables to MDRM codes with valid periods
- **Sheet "income statement"**: 134 rows, maps 69+ variables
- **Sheet "raw variables"**: 348 rows, IADX/RIAD/RCFD/RCON raw field definitions
- **Sheet "metadata"**: institution identifiers and NIC-sourced metadata variables

---

## File Layout on Disk

```
C:\empirical-data-construction\call-reports-CFLV\
  call-reports-cflv.duckdb              -- Master DuckDB (~2 GB)
  raw\
    call-reports-balance-sheets-Jan2026.dta
    call-reports-income-statements-Jan2026.dta
  staging\
    balance_sheets\year=1959\part-0.parquet ... year=2025\...
    income_statements\year=1959\part-0.parquet ... year=2025\...
```

Staging Parquet: snappy compression, 260K row-group size, Hive-partitioned by year.
1699 Parquet shards per table across 67 years.

---

## DuckDB Schema

```
Tables:
  balance_sheets      2,548,675 rows x 190 cols
  income_statements   2,548,675 rows x 71 cols
  panel_metadata      258 rows (variable documentation)
```

### DuckDB Connection

```python
import duckdb
from config import get_cflv_duckdb_path, DUCKDB_THREADS, DUCKDB_MEMORY_LIMIT

conn = duckdb.connect(str(get_cflv_duckdb_path()), read_only=True)
conn.execute(f"PRAGMA threads={DUCKDB_THREADS}")
conn.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY_LIMIT}'")
```

Direct path: `C:\empirical-data-construction\call-reports-CFLV\call-reports-cflv.duckdb`

---

## Critical Data Conventions

### Date Column

- Stored as `DATE` type, **quarter-end**: 1959-12-31, 1960-03-31, ... 2025-09-30
- Source Stata files encode as Stata tq integer (quarters since Q1-1960)
- `construct.py` converts: tq=0 → 1960-03-31, tq=-1 → 1959-12-31, etc.
- Formula: `year = 1960 + tq // 4`, `quarter = tq % 4 + 1`, `month = quarter * 3`, last day of month

### Monetary Unit

All financial variables: **thousands of USD** (as sourced; no conversion applied).
To get dollars: multiply by 1000. To get billions: divide by 1,000,000.

### Income Statement YTD Convention

All `ytd*` columns are **year-to-date cumulative**:
- Q1 value = Jan–Mar total
- Q2 value = Jan–Jun total
- Q3 value = Jan–Sep total
- Q4 value = Jan–Dec total (full year)

To get a single-quarter income: `Q2_inc = Q2_ytd − Q1_ytd` etc.
`num_employees` is the **only** non-YTD income statement variable.

### FFIEC Form Types

- **031**: Large banks with foreign offices → RCFN* columns populated
- **041/051**: Domestic-only → RCFN* columns are NULL
- `foreign_dep`, `for_deposit_ib`, `for_deposit_nib`, `qtr_avg_fgn_dep` = NULL for 041/051

### MDRM Series Prefixes

| Prefix | Meaning |
|--------|---------|
| RCFD | Foreign + Domestic combined |
| RCON | Domestic only |
| RCFN | Foreign only (031 filers) |
| RIAD | Income statement (1969–present) |
| IADX | Historical income statement (1960–1968) |

---

## Key Variables for Research

### Balance Sheet Core

| Variable | Description |
|----------|-------------|
| `assets` | Total Assets (RCFD 2170) |
| `deposits` | Total Deposits (RCFD 2200) |
| `equity` | Total Equity (RCFD 3210) |
| `ln_tot` | Net Loans (RCFD 2122, from 1976Q1) |
| `ln_ci` | C&I Loans (RCFD 1766, from 1984Q1) |
| `ln_re` | Real Estate Loans (RCFD 1410) |
| `securities` | Total Securities (era-adjusted) |
| `llres` | Loan Loss Reserve |
| `npl_tot` | Non-Performing Loans (from 1982Q4) |

### Income Statement Core

| Variable | Description |
|----------|-------------|
| `ytdnetinc` | Net Income (RIAD 4340) |
| `ytdint_inc` | Total Interest Income (RIAD 4107 from 1984Q1) |
| `ytdint_exp` | Total Interest Expense (RIAD 4073 from 1984Q1) |
| `ytdint_inc_net` | Net Interest Income (RIAD 4074 from 1984Q1) |
| `ytdnonint_inc` | Total Noninterest Income (RIAD 4079 from 1984Q1) |
| `ytdnonint_exp` | Total Noninterest Expense (RIAD 4093 from 1984Q1) |
| `ytdllprov` | Loan Loss Provisions (RIAD 4230) |
| `num_employees` | FTE Employees (NOT YTD — point-in-time) |

---

## Null Rate Notes

From validation run (Jan 2026 data):
- `assets`: 3.4% null (mostly early years with limited reporting)
- `deposits`: 3.4% null
- `equity`: 0.0% null (always reported)
- `ln_tot`: 23.6% null (not available pre-1976Q1)
- `ytdnetinc`: 23.0% null (not available pre-1969Q4)
- `ytdint_inc`: 22.9% null

High null rates for income statement granular variables (e.g., `ytdint_inc_ln_re`)
are expected — these were introduced later in reporting history.

---

## Execution (Rebuild)

```bash
# One-time conversion (takes ~4 minutes)
C:\envs\.basic_venv\Scripts\python.exe construct.py

# Force rebuild from scratch
C:\envs\.basic_venv\Scripts\python.exe construct.py --force
```

If a newer release (e.g., Apr2026) is downloaded, update `SOURCE_FILES` in `metadata.py`
and rename/point the zip paths, then re-run with `--force`.

---

## Code Modules

| File | Purpose |
|------|---------|
| `construct.py` | ETL: ZIP extract → DTA → Parquet staging → DuckDB tables + panel_metadata |
| `metadata.py` | All 158 BS + 69 IS variable definitions with MDRM codes, valid periods, units, notes |
| `schema.py` | TypedDicts for BalanceSheetRecord and IncomeStatementRecord |
| `variables.md` | Human-readable documentation with sample queries |
| `README.md` | Quick-start for future use |

---

## Known Quirks / Caveats

- `id_rssd_hd_off`, `reg_hh_1_id`, `fin_hh_id`, `reg_dh_1_id` sourced from NIC (Federal Reserve Board).
  These can be cross-referenced with the NIC dataset (`nic/`) for bank structure linkage.
- `dt_open` is stored as YYYYMMDD integer in source → string in DuckDB. Cast as needed.
- Some variables (e.g., `tot_sav_dep`, `ytdnet_op_earn_1960`) are derived items computed by CFLV authors
  from other variables, not direct MDRM line items.
- Loan variables reported semiannually between 1973–1975; Q1/Q3 values imputed as missing.
- Historical IADX codes (1960–1968) may have OCR data quality issues in `num_employees` for 1967–1968
  (trailing zeros adjusted by CFLV authors).
- `insured_deposits_alt` multiplied by $100K before 2009Q3, $250K after (FDIC insurance increase).
- `foreign_dep` and related RCFN* fields may show NULL for all rows for 041/051 filers (expected).
