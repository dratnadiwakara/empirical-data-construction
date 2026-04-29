# IRS SOI Individual Income Tax — ZIP Code Data

Harmonized zip-year panel from the IRS Statistics of Income (SOI) program. Each row is one 5-digit ZIP code for one tax year. AGI size-class stubs are aggregated to the ZIP level. Dollar amounts are in **$thousands** throughout the entire series.

**Source:** [IRS SOI ZIP Code Data](https://www.irs.gov/statistics/soi-tax-stats-individual-income-tax-statistics-zip-code-data-soi)

---

## Quick Start for Agents

```python
import duckdb
from config import get_irs_duckdb_path

db = duckdb.connect(str(get_irs_duckdb_path()), read_only=True)

# Verify dataset
db.execute("SELECT year, row_count FROM panel_metadata ORDER BY year").fetchall()

# Query the main view
db.execute("""
    SELECT year, n_returns, agi_total / n_returns AS avg_agi_k
    FROM irs
    WHERE zipcode = '28277'
    ORDER BY year
""").df()
```

**Always open read-only** unless writing. The `irs` view is the only entry point needed — do not query Parquet files directly.

---

## Coverage

- **22 years:** 1998, 2001, 2002, 2004–2022
- **~27,000–40,000 ZIP codes per year** (varies by IRS suppression rules)
- **Missing years (no IRS data):** 1999, 2000, 2003 — treat as missing, not zero

---

## Schema

### `irs` view — main query target

| Column | Type | Description |
|--------|------|-------------|
| `zipcode` | VARCHAR(5) | 5-digit ZIP code (ZCTA) |
| `year` | INTEGER | Tax year |
| `n_returns` | BIGINT | Total individual returns filed |
| `agi_total` | DOUBLE | Total adjusted gross income, **$thousands** |
| `n_returns_wages` | BIGINT | Returns with wages & salaries |
| `agi_wages` | DOUBLE | Wages & salaries, **$thousands** |
| `n_returns_dividend` | BIGINT | Returns with ordinary dividends |
| `agi_dividend` | DOUBLE | Ordinary dividends, **$thousands** |
| `n_returns_business` | BIGINT | Returns with business/profession income |
| `agi_business` | DOUBLE | Business/profession net income, **$thousands** |
| `n_returns_capital_gain` | BIGINT | Returns with net capital gain |
| `agi_capital_gain` | DOUBLE | Net capital gain, **$thousands** |
| `salary_frac` | DOUBLE | `n_returns_wages / n_returns` — share of returns with wage income |
| `dividend_frac` | DOUBLE | `n_returns_dividend / n_returns` |
| `business_frac` | DOUBLE | `n_returns_business / n_returns` |
| `capital_gain_frac` | DOUBLE | `n_returns_capital_gain / n_returns` |

**Units:** all `agi_*` columns are in **$thousands**. To get dollars: multiply by 1000. To get per-return average in $thousands: `agi_total / n_returns`.

### `panel_metadata` table

| Column | Description |
|--------|-------------|
| `year` | Tax year |
| `row_count` | ZIP code rows in that year's Parquet |
| `source_url` | IRS download URL |
| `built_at` | ISO-8601 UTC timestamp |
| `parquet_path` | Absolute path to staging Parquet |

---

## Column Availability by Year

Some columns are NULL for specific years due to IRS source file limitations — not a pipeline bug.

| Years | Columns that are NULL |
|-------|-----------------------|
| 1998, 2001, 2002 | `n_returns_dividend`, `agi_dividend`, `n_returns_business`, `agi_business`, `n_returns_capital_gain`, `agi_capital_gain` |
| 2006, 2007 | `n_returns_capital_gain`, `agi_capital_gain` |
| 2008 | `n_returns_wages`, `n_returns_dividend`, `n_returns_business`, `n_returns_capital_gain`, `agi_capital_gain` |

**Best coverage for time series requiring all columns:** 2004–2022 excluding 2008.

**Always-available columns** across all 22 years: `zipcode`, `year`, `n_returns`, `agi_total`, `agi_wages`.

---

## Key Constraints for Correct Analysis

1. **Filter `n_returns >= 10` (or similar)** before computing per-return averages. IRS suppresses small cells; some ZIPs have very few returns.
2. **Do not sum `agi_total` across years** as a level comparison without deflating — these are nominal dollars.
3. **ZIP codes change over time.** The same 5-digit code may cover different geographies in 1998 vs 2022. Use HUD or USPS crosswalks for strict longitudinal spatial comparisons.
4. **Missing years are gaps, not zeros.** A panel regression should use year fixed effects and handle 1999/2000/2003 as missing observations.
5. **`n_returns` is filer count, not population.** One return can cover multiple people (married filing jointly).

---

## Common Queries

### Time series for one ZIP
```sql
SELECT year, n_returns,
       agi_total / n_returns AS avg_agi_k,
       salary_frac,
       capital_gain_frac
FROM irs
WHERE zipcode = '10001'
ORDER BY year;
```

### Cross-section: top-income ZIPs in a given year
```sql
SELECT zipcode, n_returns,
       agi_total / n_returns AS avg_agi_k,
       salary_frac,
       capital_gain_frac
FROM irs
WHERE year = 2019
  AND n_returns >= 100
ORDER BY avg_agi_k DESC
LIMIT 20;
```

### National totals by year (sanity check)
```sql
SELECT year,
       SUM(n_returns) / 1e6             AS total_returns_M,
       SUM(agi_total) / 1e9             AS total_agi_T,
       SUM(agi_wages) / SUM(agi_total)  AS wage_share,
       SUM(agi_capital_gain) / SUM(agi_total) AS capgain_share
FROM irs
GROUP BY year
ORDER BY year;
-- Expected: ~120–155M returns, $5–14T AGI, rising over time
```

### Income composition for a set of ZIPs
```sql
SELECT zipcode, year,
       agi_total / n_returns            AS avg_agi_k,
       agi_wages / NULLIF(agi_total,0)  AS wage_share,
       agi_capital_gain / NULLIF(agi_total,0) AS capgain_share
FROM irs
WHERE zipcode IN ('10001','28277','90210')
  AND year >= 2010
ORDER BY zipcode, year;
```

### Join with SOD (bank deposits by ZIP)
```sql
SELECT i.zipcode, i.year,
       i.agi_total / i.n_returns AS avg_agi_k,
       s.total_deposits_k
FROM irs i
JOIN (
    SELECT uninumbr AS rssd, year,
           SUM(depsumbr) AS total_deposits_k
    FROM sod
    GROUP BY uninumbr, year
) s ON i.zipcode = s.zipcode AND i.year = s.year
WHERE i.year = 2019;
```

### Join with HMDA (mortgage lending by income tier)
```sql
SELECT h.zipcode,
       i.avg_agi_k,
       COUNT(*) AS originations
FROM hmda h
JOIN (
    SELECT zipcode,
           agi_total / n_returns AS avg_agi_k
    FROM irs
    WHERE year = 2019
) i ON h.zipcode = i.zipcode
WHERE h.year = 2019
  AND h.action_taken = 1
GROUP BY h.zipcode, i.avg_agi_k
ORDER BY avg_agi_k DESC;
```

---

## Comparability Notes

### Dollar units
All A-series fields are in **$thousands** across the full 1998–2022 series. The 2007 and 2008 IRS source files published amounts in actual dollars; the pipeline divides by 1000 at construction time. No adjustment needed by the analyst.

### AGI stub aggregation
Raw files contain 6–7 AGI size-class rows per ZIP per year (stub 1 = <$25k through stub 6 or 7 = $200k+), plus a stub 0 duplicate total. The pipeline sums all income-bracket stubs and excludes stub 0 to prevent double-counting. The 2006–2007 7-stub structure (extra split of top bracket) is handled correctly.

### AGI definition changes
Broadly stable across 1998–2022. Minor changes from EGTRRA (2001), JGTRRA (2003), TCJA (2017). Negligible for most ZIP-level analyses.

---

## Data Storage Layout

```
C:\empirical-data-construction\irs\
  irs.duckdb                    # DuckDB: irs view + panel_metadata table
  download_manifest.json        # Download idempotency tracking
  raw\
    {year}\
      {YYYY}zipcode.zip         # Era A: ZIP archive (1998–2010)
      *.xls / *.xlsx            # Era A: extracted state files
      {YY}zpall*.csv            # National CSV (inside archive or direct)
  staging\
    year={year}\
      data.parquet              # Snappy Parquet, one row per ZIP code
```

---

## Pipeline

```bash
# Full pipeline (download + construct all years)
python -m irs.download --all && python -m irs.construct --all

# Single year
python -m irs.download --year 2019 && python -m irs.construct --year 2019

# Rebuild view only (Parquets already exist)
python -m irs.construct --views-only

# Force rebuild a year
python -m irs.construct --year 2008 --force
```
