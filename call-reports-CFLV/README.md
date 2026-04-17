# Call Reports CFLV

Historical bank balance sheets and income statements, 1959–2025.

**Source:** Correia, Fermin, Luck, Verner (2025). *A Long-Run History of Bank Balance Sheets and Income Statements.*
FRBNY Liberty Street Economics — [data and article](https://libertystreeteconomics.newyorkfed.org/2025/12/a-long-run-history-of-bank-balance-sheets-and-income-statements/)

**Coverage:** 1959-Q4 through 2025-Q3 · 2,548,675 quarterly observations · 24,716 unique banks · Jan 2026 release

---

## Quick Start

```python
import duckdb
from config import get_cflv_duckdb_path, DUCKDB_THREADS, DUCKDB_MEMORY_LIMIT

conn = duckdb.connect(str(get_cflv_duckdb_path()), read_only=True)
conn.execute(f"PRAGMA threads={DUCKDB_THREADS}")
conn.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY_LIMIT}'")

# What tables exist?
conn.execute("SHOW TABLES").df()

# Look up a variable
conn.execute("""
    SELECT variable_name, description, mdrm_codes, unit, notes
    FROM panel_metadata
    WHERE variable_name = 'assets'
""").df()
```

---

## Tables

| Table | Rows | Description |
|-------|------|-------------|
| `balance_sheets` | 2,548,675 | Assets, liabilities, equity, loans, deposits, securities, derivatives, quarterly averages — 190 columns |
| `income_statements` | 2,548,675 | Interest income/expense, noninterest items, provisions, net income (all YTD) — 71 columns |
| `panel_metadata` | 258 | Variable documentation: description, MDRM codes, unit, valid period, notes |

---

## Key Conventions

### Date
`date` = **last day of quarter** as `DATE` type: `1960-03-31`, `1960-06-30`, etc.

### Monetary unit
All financial variables in **thousands of USD**.
```python
# Total banking system assets in 2024 Q4, in trillions
conn.execute("""
    SELECT SUM(assets) / 1e9 AS total_assets_tn
    FROM balance_sheets
    WHERE date = '2024-12-31'
""").fetchone()
```

### Income statement YTD
All `ytd*` values are **year-to-date cumulative**. Q2 = Jan–Jun total; Q4 = full year.
To get a single quarter's income:
```sql
-- Q2 2020 net income (quarterly increment)
SELECT id_rssd,
       ytdnetinc - LAG(ytdnetinc) OVER (PARTITION BY id_rssd ORDER BY date) AS q_netinc
FROM income_statements
WHERE YEAR(date) = 2020
```

`num_employees` is the **only** non-YTD column — it's a point-in-time headcount.

### FFIEC form types
Large banks filing FFIEC 031 (foreign offices) have `foreign_dep` and other `RCFN*` columns populated.
Domestic-only banks (041/051) have these as NULL — expected, not missing data.

---

## Common Queries

```sql
-- Single bank time series (Bank of America, RSSD=480228)
SELECT date,
       assets / 1e6        AS assets_tn,
       deposits / 1e6      AS deposits_tn,
       equity / 1e6        AS equity_tn,
       ln_tot / 1e6        AS loans_tn
FROM balance_sheets
WHERE id_rssd = 480228
ORDER BY date DESC
LIMIT 20;

-- Aggregate U.S. banking system: annual total assets (Q4 snapshot)
SELECT YEAR(date) AS year,
       COUNT(*)              AS n_banks,
       SUM(assets) / 1e6    AS total_assets_tn
FROM balance_sheets
WHERE MONTH(date) = 12
GROUP BY 1
ORDER BY 1;

-- Net interest margin approximation (Q4 full-year income)
SELECT b.date,
       b.id_rssd,
       b.nm_short,
       (i.ytdint_inc - i.ytdint_exp) / NULLIF(b.qtr_avg_assets, 0) AS nim
FROM balance_sheets b
JOIN income_statements i USING (id_rssd, date)
WHERE MONTH(b.date) = 12          -- Q4 = full-year YTD income
  AND b.qtr_avg_assets IS NOT NULL
  AND b.qtr_avg_assets > 0
  AND YEAR(b.date) >= 1984         -- ytdint_inc/exp line items available from 1984
ORDER BY b.date DESC, nim DESC
LIMIT 50;

-- Loan portfolio composition: share of C&I loans for large banks in 2024
SELECT b.id_rssd,
       b.nm_short,
       b.assets / 1e6       AS assets_bn,
       b.ln_ci / NULLIF(b.ln_tot, 0) AS ci_share
FROM balance_sheets b
WHERE date = '2024-12-31'
  AND assets > 10e6          -- > $10 billion
ORDER BY assets DESC
LIMIT 20;

-- Find a bank by name
SELECT DISTINCT id_rssd, nm_lgl, nm_short, city, state_abbr_nm, entity_type
FROM balance_sheets
WHERE LOWER(nm_lgl) LIKE '%jpmorgan%'
LIMIT 10;

-- Link to NIC dataset for bank structure
-- (requires nic/nic.duckdb to be built)
SELECT b.id_rssd, b.nm_short, b.assets,
       b.reg_hh_1_id   AS holding_company_rssd
FROM balance_sheets b
WHERE date = '2024-12-31'
  AND assets > 1e6     -- > $1 billion
ORDER BY assets DESC
LIMIT 20;

-- Variable lookup
SELECT variable_name, source_table, description, mdrm_codes, notes
FROM panel_metadata
WHERE source_table = 'income_statements'
ORDER BY variable_name;
```

---

## Variable Reference

Full documentation: [`variables.md`](variables.md)

Key balance sheet variables:

| Variable | Description | Available From |
|----------|-------------|----------------|
| `assets` | Total Assets | 1959-Q4 |
| `deposits` | Total Deposits | 1959-Q4 |
| `equity` | Total Equity | 1959-Q4 |
| `ln_tot` | Net Loans | 1976-Q1 |
| `ln_re` | Real Estate Loans | 1959-Q4 |
| `ln_ci` | C&I Loans | 1959-Q4 (RCFD 1766 from 1984-Q1) |
| `ln_cc` | Credit Card Loans | 1967-Q4 |
| `securities` | Total Securities | 1959-Q4 |
| `htmsec_ac` | HTM Securities (amortized cost) | 1994-Q1 |
| `afssec_fv` | AFS Securities (fair value) | 1994-Q1 |
| `npl_tot` | Non-Performing Loans | 1982-Q4 |
| `llres` | Loan Loss Reserve | 1959-Q4 |
| `qtr_avg_assets` | Quarterly Average Total Assets | 1978-Q4 |

Key income statement variables (all YTD):

| Variable | Description | Available From |
|----------|-------------|----------------|
| `ytdnetinc` | Net Income | 1960-Q4 |
| `ytdint_inc` | Total Interest Income | 1960-Q4 |
| `ytdint_exp` | Total Interest Expense | 1960-Q4 |
| `ytdint_inc_net` | Net Interest Income | 1984-Q1 |
| `ytdnonint_inc` | Total Noninterest Income | 1960-Q4 |
| `ytdnonint_exp` | Total Noninterest Expense | 1960-Q4 |
| `ytdllprov` | Loan Loss Provisions | 1969-Q4 |
| `ytdtradrev_inc` | Trading Revenue | 1984-Q1 |
| `num_employees` | FTE Employees (NOT YTD) | 1960-Q4 |

---

## Rebuild

```bash
# Requires source ZIPs in C:\Users\dimut\Downloads\call-reports-CFLV\
C:\envs\.basic_venv\Scripts\python.exe construct.py

# Force rebuild from scratch (re-extract + reprocess)
C:\envs\.basic_venv\Scripts\python.exe construct.py --force
```

Runtime: ~4 minutes on a standard machine.

Output: `C:\empirical-data-construction\call-reports-CFLV\call-reports-cflv.duckdb`

---

## Files

| File | Purpose |
|------|---------|
| `construct.py` | ETL pipeline |
| `metadata.py` | Variable definitions + MDRM codes (single source of truth) |
| `schema.py` | TypedDicts for both tables |
| `variables.md` | Full variable documentation |
| `MEMORY.md` | Operational notes for AI agents |
| `plan.md` | Implementation decisions and architecture |
