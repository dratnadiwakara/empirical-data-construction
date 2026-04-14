# CRA Pipeline -- Reference & Query Guide

## Overview

This pipeline processes Community Reinvestment Act (CRA) flat files published by the FFIEC into a harmonized DuckDB database spanning **1996--2024**. Three panel views expose all years through a single SQL interface:

| View | Granularity | Rows | Description |
|------|-------------|------|-------------|
| `aggregate_panel` | tract/county x table x year | ~8.8M | Market-wide loan counts & amounts by geography |
| `disclosure_panel` | lender x county x table x year | ~56.9M | Per-lender loan counts & amounts by geography |
| `transmittal_panel` | lender x year | ~34K | Lender identity: name, address, RSSD, assets |

---

## Data Source

| Item | Value |
|------|-------|
| Publisher | FFIEC |
| Landing page | <https://www.ffiec.gov/data/cra/flat-files> |
| URL pattern | `https://www.ffiec.gov/cra/xls/{YY}exp_{type}.zip` |
| File types | `aggr` (aggregate), `discl` (disclosure), `trans` (transmittal) |
| Format | Fixed-width `.dat` inside ZIP |
| Encoding | Latin-1 (some files contain extended characters) |

---

## File Layout on Disk

```
C:\empirical-data-construction\cra\
├── cra.duckdb                              # Master DuckDB (views + panel_metadata table)
├── raw\
│   └── {year}\                             # Downloaded ZIP + extracted .dat files
├── staging\
│   ├── aggregate\year={year}\data.parquet  # Hive-partitioned Parquet (snappy)
│   ├── disclosure\year={year}\data.parquet
│   └── transmittal\year={year}\data.parquet
```

---

## How to Connect

```python
import duckdb

conn = duckdb.connect(r"C:\empirical-data-construction\cra\cra.duckdb", read_only=True)
```

All three panel views (`aggregate_panel`, `disclosure_panel`, `transmittal_panel`) are available immediately. No additional setup is needed.

---

## Schema Reference

### aggregate_panel (23 columns)

Geography-level aggregates across all lenders. One row per (table_id, year, geography, loan_type, action_taken, report_level).

| Column | Type | Description |
|--------|------|-------------|
| `table_id` | VARCHAR | CRA table: `A1-1` (small business originations), `A1-2` (purchases), `A2-1` (small farm originations), `A2-2` (purchases), plus `a` variants for assessment areas |
| `activity_year` | VARCHAR | Filing year (same as `year` but stored as string from the raw file) |
| `year` | BIGINT | Filing year (integer, from Hive partition) |
| `loan_type` | VARCHAR | `4` = small business, `5` = small farm |
| `action_taken` | VARCHAR | `1` = originations, `6` = purchases |
| `state` | VARCHAR | 2-digit FIPS state code (raw) |
| `county` | VARCHAR | 3-digit FIPS county code (raw) |
| `msamd` | VARCHAR | MSA/MD code (4 digits pre-2004, 5 digits 2004+) |
| `census_tract` | VARCHAR | 7-character census tract (raw, may contain decimal) |
| `split_county` | VARCHAR | Split county indicator |
| `pop_group` | VARCHAR | Population classification |
| `income_group` | VARCHAR | Tract income group (coded) |
| `report_level` | VARCHAR | **Key filter**: `200` = county total, `210`/`220`/`230`/`240` = income-group breakdowns |
| `num_loans_lt_100k` | BIGINT | Count of loans <= $100K |
| `amt_loans_lt_100k` | BIGINT | Amount of loans <= $100K (thousands of dollars) |
| `num_loans_100k_250k` | BIGINT | Count of loans $100K--$250K |
| `amt_loans_100k_250k` | BIGINT | Amount of loans $100K--$250K (thousands) |
| `num_loans_250k_1m` | BIGINT | Count of loans $250K--$1M |
| `amt_loans_250k_1m` | BIGINT | Amount of loans $250K--$1M (thousands) |
| `num_loans_rev_lt_1m` | BIGINT | Count of loans to borrowers with revenue < $1M |
| `amt_loans_rev_lt_1m` | BIGINT | Amount of loans to borrowers with revenue < $1M (thousands) |
| `county_fips` | VARCHAR | **Computed**: 5-character FIPS (2-digit state + 3-digit county, zero-padded) |
| `census_tract_fips` | VARCHAR | **Computed**: 11-character FIPS (2-digit state + 3-digit county + 6-digit tract, zero-padded, decimal removed) |

### disclosure_panel (27 columns)

Per-lender data at the county level. One row per (table_id, respondent_id, agency_code, year, geography, loan_type, action_taken, report_level).

| Column | Type | Description |
|--------|------|-------------|
| `table_id` | VARCHAR | CRA table: `D1-1`, `D1-2`, `D2-1`, `D2-2`, `D3-0`/`D3`, `D4-0`/`D4`, `D5-0`/`D5`, `D6-0`/`D6` |
| `respondent_id` | VARCHAR | 10-character lender ID (join key to transmittal) |
| `agency_code` | VARCHAR | Federal agency code (join key to transmittal) |
| `activity_year` | VARCHAR | Filing year |
| `year` | BIGINT | Filing year (integer, from Hive partition) |
| `loan_type` | VARCHAR | `4` = small business, `5` = small farm |
| `action_taken` | VARCHAR | `1` = originations, `6` = purchases |
| `state` | VARCHAR | 2-digit FIPS state code (raw) |
| `county` | VARCHAR | 3-digit FIPS county code (raw) |
| `msamd` | VARCHAR | MSA/MD code |
| `aa_num` | VARCHAR | Assessment area number |
| `partial_county` | VARCHAR | Partial county indicator |
| `split_county` | VARCHAR | Split county indicator |
| `pop_group` | VARCHAR | Population classification |
| `income_group` | VARCHAR | Tract income group |
| `report_level` | VARCHAR | **Key filter**: `040` = county total, other values = sub-county breakdowns |
| `num_loans_lt_100k` | BIGINT | Count of loans <= $100K |
| `amt_loans_lt_100k` | BIGINT | Amount (thousands) |
| `num_loans_100k_250k` | BIGINT | Count $100K--$250K |
| `amt_loans_100k_250k` | BIGINT | Amount (thousands) |
| `num_loans_250k_1m` | BIGINT | Count $250K--$1M |
| `amt_loans_250k_1m` | BIGINT | Amount (thousands) |
| `num_loans_rev_lt_1m` | BIGINT | Count to borrowers revenue < $1M |
| `amt_loans_rev_lt_1m` | BIGINT | Amount (thousands) |
| `num_loans_affiliate` | BIGINT | Affiliate loan count |
| `amt_loans_affiliate` | BIGINT | Affiliate loan amount (thousands) |
| `county_fips` | VARCHAR | **Computed**: 5-character county FIPS |

**Note**: Disclosure data does not include census tract fields. Geography is at the county level only.

### transmittal_panel (12 columns)

One row per lender per year. Use this to identify who a `respondent_id` / `agency_code` pair refers to.

| Column | Type | Description |
|--------|------|-------------|
| `respondent_id` | VARCHAR | 10-character lender ID |
| `agency_code` | VARCHAR | Federal agency code: `1`=OCC, `2`=FRS, `3`=FDIC, `4`=OTS, `5`=NCUA |
| `activity_year` | VARCHAR | Filing year |
| `year` | BIGINT | Filing year (integer) |
| `respondent_name` | VARCHAR | Institution name |
| `respondent_addr` | VARCHAR | Street address |
| `respondent_city` | VARCHAR | City |
| `respondent_state` | VARCHAR | 2-letter state |
| `respondent_zip` | VARCHAR | ZIP code |
| `tax_id` | VARCHAR | Tax identification number |
| `rssdid` | BIGINT | Federal Reserve RSSD ID (NULL for 1996) |
| `assets` | BIGINT | Total assets (NULL for 1996) |

---

## Important Concepts

### report_level -- Filtering to the Right Granularity

Both aggregate and disclosure data contain rows at multiple levels of geographic detail. Always filter on `report_level` to avoid double-counting.

| View | report_level | Meaning |
|------|-------------|---------|
| aggregate_panel | `200` | County total (use this for county-level aggregation) |
| aggregate_panel | `210`, `220`, `230`, `240` | By tract income group within a county |
| disclosure_panel | `040` | County total (use this for county-level aggregation) |
| disclosure_panel | Other values | Sub-county breakdowns |

### Table IDs -- What Each Table Measures

| ID | Content |
|----|---------|
| `A1-1` / `D1-1` | Small business loans -- originations |
| `A1-2` / `D1-2` | Small business loans -- purchases |
| `A2-1` / `D2-1` | Small farm loans -- originations |
| `A2-2` / `D2-2` | Small farm loans -- purchases |
| `A*-*a` | Assessment area versions of the above (aggregate only) |
| `D3` | Community development loans |
| `D4` | Consumer loans in low/moderate-income geographies |
| `D5` | Consumer loans to low/moderate-income borrowers |
| `D6` | Other consumer loans |

### Linking Disclosure to Transmittal (Identifying Lenders)

The join key is `(respondent_id, agency_code, year)`:

```sql
SELECT
    d.year,
    t.respondent_name,
    t.rssdid,
    d.county_fips,
    SUM(d.num_loans_lt_100k + d.num_loans_100k_250k + d.num_loans_250k_1m) AS total_loans,
    SUM(d.amt_loans_lt_100k + d.amt_loans_100k_250k + d.amt_loans_250k_1m) AS total_amount
FROM disclosure_panel AS d
JOIN transmittal_panel AS t
    ON d.respondent_id = t.respondent_id
    AND d.agency_code  = t.agency_code
    AND d.year         = t.year
WHERE TRIM(d.table_id) = 'D1-1'
  AND TRIM(d.report_level) = '040'
  AND d.year = 2024
GROUP BY d.year, t.respondent_name, t.rssdid, d.county_fips
ORDER BY total_loans DESC
LIMIT 20;
```

### Amount Columns Are in Thousands of Dollars

All `amt_*` columns are reported in **thousands of dollars**. Multiply by 1,000 for actual dollar values.

### Geographic Identifiers

| Column | Format | Example | Available In |
|--------|--------|---------|-------------|
| `county_fips` | 5 chars: SS + CCC | `06037` (Los Angeles) | aggregate, disclosure |
| `census_tract_fips` | 11 chars: SS + CCC + TTTTTT | `06037204820` | aggregate only |

These are computed during ETL from the raw `state`, `county`, and `census_tract` fields with leading-zero padding. NULL when any underlying component is blank.

---

## Common Query Patterns

### 1. National aggregate totals for small business originations by year

```sql
SELECT
    year,
    SUM(num_loans_lt_100k + num_loans_100k_250k + num_loans_250k_1m) AS total_num_loans,
    SUM(amt_loans_lt_100k + amt_loans_100k_250k + amt_loans_250k_1m) AS total_amt_thousands
FROM aggregate_panel
WHERE TRIM(table_id) = 'A1-1'
  AND CAST(loan_type AS INTEGER) = 4
  AND CAST(action_taken AS INTEGER) = 1
  AND TRIM(report_level) = '200'
GROUP BY year
ORDER BY year;
```

### 2. County-level small business lending for a specific state and year

```sql
SELECT
    county_fips,
    SUM(num_loans_lt_100k + num_loans_100k_250k + num_loans_250k_1m) AS total_loans,
    SUM(amt_loans_lt_100k + amt_loans_100k_250k + amt_loans_250k_1m) AS total_amt_thousands
FROM aggregate_panel
WHERE TRIM(table_id) = 'A1-1'
  AND CAST(action_taken AS INTEGER) = 1
  AND TRIM(report_level) = '200'
  AND TRIM(state) = '06'
  AND year = 2024
GROUP BY county_fips
ORDER BY total_loans DESC;
```

### 3. Top 10 lenders nationally (by number of small business originations, 2024)

```sql
SELECT
    t.respondent_name,
    t.rssdid,
    SUM(d.num_loans_lt_100k + d.num_loans_100k_250k + d.num_loans_250k_1m) AS total_loans
FROM disclosure_panel AS d
JOIN transmittal_panel AS t
    ON d.respondent_id = t.respondent_id
    AND d.agency_code  = t.agency_code
    AND d.year         = t.year
WHERE TRIM(d.table_id) = 'D1-1'
  AND CAST(d.action_taken AS INTEGER) = 1
  AND TRIM(d.report_level) = '040'
  AND d.year = 2024
GROUP BY t.respondent_name, t.rssdid
ORDER BY total_loans DESC
LIMIT 10;
```

### 4. Time series of a specific lender's lending (by RSSD ID)

```sql
SELECT
    d.year,
    SUM(d.num_loans_lt_100k + d.num_loans_100k_250k + d.num_loans_250k_1m) AS total_loans,
    SUM(d.amt_loans_lt_100k + d.amt_loans_100k_250k + d.amt_loans_250k_1m) AS total_amt_thousands
FROM disclosure_panel AS d
JOIN transmittal_panel AS t
    ON d.respondent_id = t.respondent_id
    AND d.agency_code  = t.agency_code
    AND d.year         = t.year
WHERE TRIM(d.table_id) = 'D1-1'
  AND CAST(d.action_taken AS INTEGER) = 1
  AND TRIM(d.report_level) = '040'
  AND t.rssdid = 852218           -- example: JPMorgan Chase
GROUP BY d.year
ORDER BY d.year;
```

### 5. Census-tract-level small business lending (aggregate only)

```sql
SELECT
    census_tract_fips,
    year,
    SUM(num_loans_lt_100k + num_loans_100k_250k + num_loans_250k_1m) AS total_loans,
    SUM(num_loans_rev_lt_1m) AS loans_to_small_biz
FROM aggregate_panel
WHERE TRIM(table_id) = 'A1-1'
  AND CAST(action_taken AS INTEGER) = 1
  AND TRIM(report_level) = '200'
  AND census_tract_fips IS NOT NULL
  AND year = 2024
GROUP BY census_tract_fips, year
ORDER BY total_loans DESC
LIMIT 20;
```

### 6. Small farm lending by state over time

```sql
SELECT
    year,
    LPAD(TRIM(state), 2, '0') AS state_fips,
    SUM(num_loans_lt_100k + num_loans_100k_250k + num_loans_250k_1m) AS total_loans
FROM aggregate_panel
WHERE TRIM(table_id) = 'A2-1'
  AND CAST(action_taken AS INTEGER) = 1
  AND TRIM(report_level) = '200'
GROUP BY year, state_fips
ORDER BY year, state_fips;
```

### 7. Export a county-year panel to CSV

```sql
COPY (
    SELECT
        year,
        county_fips,
        SUM(num_loans_lt_100k + num_loans_100k_250k + num_loans_250k_1m) AS total_loans,
        SUM(amt_loans_lt_100k + amt_loans_100k_250k + amt_loans_250k_1m) AS total_amt_thousands,
        SUM(num_loans_rev_lt_1m) AS loans_rev_lt_1m,
        SUM(amt_loans_rev_lt_1m) AS amt_rev_lt_1m_thousands
    FROM aggregate_panel
    WHERE TRIM(table_id) = 'A1-1'
      AND CAST(action_taken AS INTEGER) = 1
      AND TRIM(report_level) = '200'
      AND county_fips IS NOT NULL
    GROUP BY year, county_fips
    ORDER BY year, county_fips
) TO 'cra_county_panel.csv' (HEADER, DELIMITER ',');
```

---

## Era Differences

The FFIEC changed the fixed-width file spec several times. The pipeline harmonizes these automatically, but be aware:

| Era | Years | table_id width | MSA/MD width | count/amount field width | Notes |
|-----|-------|---------------|-------------|------------------------|-------|
| 1996 | 1996 | 4 chars | 4 digits | 6/8 digits | No RSSD or assets in transmittal |
| 1997--2003 | 1997--2003 | 5 chars | 4 digits | 6/8 digits | RSSD and assets added to transmittal |
| 2004+ | 2004--2024 | 5 chars | 5 digits | 10 digits | Wider numeric fields |

Starting in **2016**, ZIP files contain per-table `.dat` files instead of one combined file. The pipeline handles this transparently.

---

## Pipeline Scripts

All scripts run from the repository root as Python modules.

### Download

```bash
python -m cra.download --year 2024         # single year, all 3 file types
python -m cra.download --year 2024 --type aggr  # only aggregate
python -m cra.download --all               # all years (2024 -> 1996)
python -m cra.download --force             # re-download even if manifest is current
```

### Construct (ETL)

```bash
python -m cra.construct --year 2024        # process single year
python -m cra.construct --year 2024 --force  # reprocess even if Parquet exists
python -m cra.construct --all              # process all years
python -m cra.construct --validate 2024    # run 2024 validation only
```

### Full Rebuild

```bash
python -m cra.download --all
python -m cra.construct --all
```

---

## Codebase Structure

```
cra/
├── __init__.py      # Package marker
├── README.md        # This file
├── plan.md          # Original build plan
├── metadata.py      # Fixed-width layouts, table IDs, URL patterns, validation targets
├── schema.py        # TypedDict definitions for harmonized records
├── download.py      # Download + extract ZIP files from FFIEC (uses curl_cffi)
└── construct.py     # ETL: fixed-width -> Parquet -> DuckDB views
```

---

## Known Quirks

| Issue | Details |
|-------|---------|
| `table_id` has trailing spaces | Always use `TRIM(table_id)` in WHERE clauses |
| `report_level` has leading zeros | Use `TRIM(report_level)` and compare as string: `'200'`, `'040'` |
| 1996 transmittal lacks `rssdid` and `assets` | These columns are NULL for 1996 |
| Disclosure has no census tract | Only county-level geography; use aggregate for tract-level analysis |
| Disclosure `report_level = '040'` vs aggregate `'200'` | County totals use different codes in the two tables |
| Amount columns in thousands | All `amt_*` values are in thousands of dollars |
| `action_taken = 6` for purchases | Not `2` -- the CRA uses `6` for loan purchases |
| Pre-2016 disclosure table IDs | Tables like D3, D4 appear as `D3-0`, `D4-0` in the raw files |
| Non-UTF-8 characters | Some files (e.g., 2003 transmittal) have Latin-1 encoded names; handled by the pipeline |
