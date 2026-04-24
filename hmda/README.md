# HMDA LAR Panel — AI Agent Query Reference

This document is for AI agents answering questions about U.S. mortgage markets
using the HMDA (Home Mortgage Disclosure Act) panel built in this pipeline.

---

## ⚠️ CRITICAL: Always Query the `lar_panel` VIEW — Never Raw Parquet

**DO**: query the `lar_panel` VIEW inside `hmda.duckdb`.
**DO NOT**: run `read_parquet()` directly on files under
`C:\empirical-data-construction\hmda\staging\year=*/data.parquet`.

Raw Parquet is faithful to source — it is **not harmonized**:

- `state_code` / `county_code` — pre-2018 drops leading zeros
  (`'1'` instead of `'01'`); 2018+ stores full 5-digit FIPS in `county_code`.
  The VIEW LPADs pre-2018 and splits 2018+ into consistent 2-char state + 3-char
  county. The VIEW also exposes a derived `county_fips` (5-char) column that
  does not exist in Parquet.
- Harmonized cross-era categorical columns
  (`loan_purpose_harmonized`, `purchaser_type_harmonized`,
  `denial_reason_1/2/3_harmonized`, `preapproval_harmonized`) are
  VIEW-only — they bridge 2017/2018 code differences and do not exist
  in raw Parquet.

Harmonization logic lives in `utils/duckdb_utils.py::recreate_lar_view` and is
re-applied every time `construct.py` runs (including when new years are added).
Bypassing the VIEW silently gives era-inconsistent results.

```sql
-- GOOD
SELECT state_code, county_code, county_fips FROM lar_panel WHERE year = 2024;

-- BAD — unharmonized, no county_fips, pre-2018 lost zero-padding
SELECT * FROM read_parquet('C:/empirical-data-construction/hmda/staging/year=2003/data.parquet');
```

---

## Quick Connection

```python
import duckdb, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import get_duckdb_path, DUCKDB_THREADS, DUCKDB_MEMORY_LIMIT

conn = duckdb.connect(str(get_duckdb_path()), read_only=True)
conn.execute(f"PRAGMA threads={DUCKDB_THREADS}")
conn.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY_LIMIT}'")
# Query lar_panel, avery_crosswalk, panel_metadata, arid2017_to_lei
```

**Note on running scripts**: place temporary scripts at the repo root
(`C:/OneDrive/github/empirical-data-construction/`), not inside `hmda/`, because
`hmda/inspect.py` shadows Python's standard library `inspect` module.

---

## Coverage

- **Years**: 2000-2024 (25 years)
- **Rows**: ~536 million
- **View**: `lar_panel` (hive-partitioned Parquet, always use `WHERE year = ...` first)
- **All columns**: VARCHAR -- use `TRY_CAST(col AS DOUBLE)` for arithmetic

---

## Four-Era Data Source Architecture

| Era | Years | Source | Delimiter | Header | Cols |
|-----|-------|--------|-----------|--------|------|
| Post-reform | 2018-2024 | FFIEC snapshot `_pipe.zip` | Pipe `\|` | Yes | 99 |
| Pre-reform FFIEC | 2017 | FFIEC snapshot `_txt.zip` | Pipe `\|` | **No** | 45 |
| Pre-reform CFPB | 2007-2016 | CFPB historic portal `_labels.zip` | Comma `,` | Yes | 78 (45 kept) |
| ICPSR pre-CFPB | 2000-2006 | OpenICPSR project 151921 (manual) | Pipe `\|` | Yes | 38 or 23 |

ICPSR files have two sub-eras due to the 2004 HMDA reform:
- **2004-2006**: 38 columns (added ethnicity, race_2-5, preapproval, property_type, rate_spread, hoepa_status, lien_status)
- **2000-2003**: 23 columns (only race_1, no ethnicity, no lien/hoepa/preapproval)

All ICPSR values are pure numeric codes (no label columns). ZIPs use Deflate64 compression;
extract with `unzip` from Git (`C:\Program Files\Git\usr\bin\unzip.exe`) -- Python's
`zipfile` module and PowerShell do not support Deflate64.

---

## Critical Unit Conventions

| Column | Unit | Rule |
|--------|------|------|
| `loan_amount` | Whole dollars | Ready to use. Pre-2018 source was $000s; ETL already scaled x1000 |
| `income` | **$000s in ALL years** | Must multiply x1000 to get dollar income |
| `census_tract` | 11-char FIPS string | Matches across all years |
| `interest_rate` | Percent string, e.g. "4.5" | 2018+ only; NULL pre-2018 |

---

## Always Filter `year` First (Performance)

The Parquet files are hive-partitioned on `year`. Always include a year predicate:

```sql
-- GOOD (partition pruning)
SELECT * FROM lar_panel WHERE year = 2023 AND action_taken = '1'

-- BAD (full scan of all 536M rows)
SELECT * FROM lar_panel WHERE action_taken = '1'
```

---

## Common Research Filters

### Standard "originated mortgage" sample
```sql
WHERE action_taken = '1'          -- originated loans only
```

### Closed-end first-lien purchase or refi (CFPB standard sample)
```sql
WHERE action_taken IN ('1','2','3')       -- originated / approved-not-accepted / denied
  AND loan_purpose IN ('1','31','32')     -- purchase or refi (use loan_purpose_harmonized='1' or '3' for cross-year)
  AND lien_status = '1'                  -- first lien
  AND open_end_line_of_credit = '2'      -- closed-end (2018+ only; omit for pre-2018)
  AND reverse_mortgage = '2'             -- not reverse (2018+ only)
  AND occupancy_type = '1'               -- principal residence
  AND construction_method = '1'          -- site-built (2018+ only)
  AND total_units = '1'                  -- single-family (2018+ only)
```

### Denial rate denominator
```sql
-- Denial rate = applications denied / (originated + approved-not-accepted + denied)
WHERE action_taken IN ('1','2','3')
```

---

## Cross-Year Queries -- Use Harmonized Columns

Six columns bridge categorical differences across the 2017/2018 reform boundary.
**Always use the harmonized version for cross-year comparisons.**

```sql
-- Cross-year refinance query (CORRECT)
WHERE loan_purpose_harmonized = '3'    -- all refinancings, all years 2000-2024

-- Year-specific (only correct for one era)
WHERE loan_purpose = '3'               -- 2000-2017 only
WHERE loan_purpose IN ('31','32')      -- 2018+ only

-- Cash-out vs rate-term (2018+ only -- not available pre-2018)
WHERE loan_purpose = '31'              -- cash-out refi
WHERE loan_purpose = '32'             -- non-cash-out refi
```

| Harmonized Column | Rule |
|-------------------|------|
| `loan_purpose_harmonized` | '31'/'32' -> '3' (all refi = '3') |
| `purchaser_type_harmonized` | '71'/'72' -> '7' |
| `denial_reason_1_harmonized` | '0' -> '10' (N/A code) |
| `denial_reason_2_harmonized` | same |
| `denial_reason_3_harmonized` | same |
| `preapproval_harmonized` | '3' -> '2' (not requested) |

---

## Era-Specific Column Availability

### 2018+ only (NULL for 2000-2017)
`lei`, `conforming_loan_limit`, `combined_loan_to_value_ratio`, `interest_rate`,
`total_loan_costs`, `loan_term`, `debt_to_income_ratio`, `property_value`,
`open_end_line_of_credit`, `reverse_mortgage`, `business_or_commercial_purpose`,
`construction_method`, `total_units`, `applicant_credit_score_type`,
`co_applicant_credit_score_type`, `applicant_age`, `co_applicant_age`,
`submission_of_application`, `initially_payable_to_institution`,
`aus_1` thru `aus_5`, `derived_loan_product_type`, `derived_dwelling_category`

### Pre-2018 only (NULL for 2018+)
`respondent_id`, `agency_code`, `property_type`

### 2004+ only (NULL for 2000-2003 ICPSR sub-era)
`applicant_ethnicity_1`, `co_applicant_ethnicity_1`, `applicant_race_2` thru `_5`,
`co_applicant_race_2` thru `_5`, `preapproval`, `rate_spread`, `hoepa_status`, `lien_status`

---

## Key Code Tables

### action_taken
```
1 = Originated
2 = Approved, not accepted
3 = Denied
4 = Withdrawn
5 = Incomplete
6 = Purchased by institution
7 = Preapproval denied
8 = Preapproval approved, not accepted
```

### loan_purpose (raw; prefer loan_purpose_harmonized)
```
2000-2017:  1=Purchase  2=Improvement  3=Refinance
2018+:      1=Purchase  2=Improvement  31=Cash-out refi  32=Non-cash-out refi  4=Other  5=N/A
```

### loan_type
```
1=Conventional  2=FHA  3=VA  4=FSA/RHS
```

### lien_status (2004+ only; NULL for 2000-2003)
```
1=First lien  2=Subordinate lien
3=Not secured (pre-2018 only)  4=N/A purchased (pre-2018 only)
```

### conforming_loan_limit (2018+ only)
```
C=Conforming  NC=Jumbo/Non-conforming  NCB=Non-conforming below limit
```

### occupancy_type
```
1=Principal residence  2=Second home  3=Investment property
```

### purchaser_type (raw; prefer purchaser_type_harmonized)
```
0=Not sold  1=Fannie Mae  2=Ginnie Mae  3=Freddie Mac
4=Farmer Mac  5=Private securitization  6=Bank/thrift
7=Non-bank financial (pre-2018)  71=Credit union/mortgage co (2018+)  72=Life insurance (2018+)
```

### applicant_race_1
```
1=AIAN  2=Asian (21-27=sub-codes)  3=Black/AA
4=NHPI (41-44=sub-codes)  5=White  6=Not provided  7=N/A
```

### applicant_ethnicity_1 (2004+ only)
```
1=Hispanic (11=Mexican,12=Puerto Rican,13=Cuban,14=Other Hispanic)
2=Not Hispanic (22=alt code)
3=Not provided  4=N/A
```

### construction_method (2018+ only)
```
1=Site-built  2=Manufactured home
```

---

## Race/Ethnicity Classification (CFPB Enhanced Methodology)

For denial rate analysis by race, the CFPB uses an "enhanced" classification.
**Note**: ethnicity is only available from 2004+; for 2000-2003 use `applicant_race_1` only.

```sql
CASE
    WHEN applicant_ethnicity_1 IN ('1','11','12','13','14')
         AND applicant_race_1 = '5'
         AND (applicant_race_2 IS NULL OR applicant_race_2 = '')
         THEN 'Hispanic White'
    WHEN applicant_ethnicity_1 IN ('2','22')
         AND applicant_race_1 = '5'
         AND (applicant_race_2 IS NULL OR applicant_race_2 = '')
         THEN 'Non-Hispanic White'
    WHEN applicant_race_1 IN ('2','21','22','23','24','25','26','27')
         AND (applicant_race_2 IS NULL OR applicant_race_2 = '')
         AND applicant_ethnicity_1 NOT IN ('1','11','12','13','14')
         THEN 'Asian'
    WHEN applicant_race_1 = '3'
         AND (applicant_race_2 IS NULL OR applicant_race_2 = '')
         AND applicant_ethnicity_1 NOT IN ('1','11','12','13','14')
         THEN 'Black'
    WHEN applicant_race_1 IN ('1','4','41','42','43','44')
         AND (applicant_race_2 IS NULL OR applicant_race_2 = '')
         THEN 'Other minority'
    WHEN applicant_race_2 IS NOT NULL AND applicant_race_2 != ''
         THEN 'Joint'    -- multiple races selected
    ELSE 'Missing'
END AS race_eth
```

---

## Geographic Queries

### By census tract (11-char FIPS)
```sql
-- County 48201 = Harris County, TX (FIPS starts with 48201)
WHERE LEFT(census_tract, 5) = '48201'

-- Specific tract
WHERE census_tract = '48201222100'
```

### By state (2-char FIPS)
```sql
WHERE state_code = '48'   -- Texas
WHERE state_code = '06'   -- California
```

### By county
Three harmonized geography columns in `lar_panel`:
- `state_code` — always 2-char state FIPS (e.g. `'06'`)
- `county_code` — always 3-char county-within-state FIPS (e.g. `'037'`)
- `county_fips` — derived 5-char full state+county FIPS (e.g. `'06037'`)

```sql
-- Los Angeles County, CA
WHERE county_fips = '06037'

-- All counties in Texas
WHERE state_code = '48'
```

Semantics harmonized across all years via the `lar_panel` VIEW (see
`utils/duckdb_utils.py::recreate_lar_view`): pre-2018 raw Parquet has
unpadded state/county (source drops leading zeros); 2018+ raw Parquet
has full 5-digit FIPS in the `county_code` column. The VIEW pads
pre-2018 and splits 2018+ so all years follow the same 2/3/5-char
convention.

### By MSA/MD
```sql
WHERE derived_msa_md = '26420'   -- Houston-The Woodlands-Sugar Land, TX
```

---

## RSSD / Lender Linkage

The `avery_crosswalk` table links each lender application to its Federal Reserve
RSSD institution ID, parent, and top holder.

```sql
-- Pre-2018: join via respondent_id + agency_code
LEFT JOIN avery_crosswalk AS av
    ON l.respondent_id = av.respondent_id
    AND TRY_CAST(l.agency_code AS INTEGER) = av.agency_code
    AND av.activity_year = l.year
WHERE l.year BETWEEN 2000 AND 2017

-- Post-2018: join via LEI
LEFT JOIN avery_crosswalk AS av
    ON l.lei = av.lei AND av.activity_year = l.year
WHERE l.year >= 2018
```

Avery columns available after join: `rssd_id`, `parent_rssd`, `top_holder_rssd`,
`respondent_name`, `assets`

---

## Sample Queries

### Total originations and volume by year
```sql
SELECT
    year,
    COUNT(*)                                              AS applications,
    SUM(CASE WHEN action_taken = '1' THEN 1 ELSE 0 END)  AS originated,
    SUM(CASE WHEN action_taken = '1'
              THEN TRY_CAST(loan_amount AS DOUBLE) ELSE 0 END) / 1e9 AS volume_billions
FROM lar_panel
GROUP BY year ORDER BY year
```

### Denial rate by year and race (cross-year safe, 2004+ for full race/ethnicity)
```sql
SELECT
    year,
    CASE
        WHEN applicant_ethnicity_1 IN ('1','11','12','13','14') AND applicant_race_1 = '5'
             THEN 'Hispanic White'
        WHEN applicant_ethnicity_1 IN ('2','22') AND applicant_race_1 = '5'
             THEN 'Non-Hispanic White'
        WHEN applicant_race_1 = '3' THEN 'Black'
        WHEN applicant_race_1 IN ('2','21','22','23','24','25','26','27') THEN 'Asian'
        ELSE 'Other/Missing'
    END AS race_eth,
    ROUND(100.0 * SUM(CASE WHEN action_taken = '3' THEN 1 ELSE 0 END)
          / COUNT(*), 1) AS denial_rate_pct,
    COUNT(*) AS applications
FROM lar_panel
WHERE year IN (2018,2019,2020)
  AND action_taken IN ('1','2','3')
  AND loan_purpose IN ('1','31','32')
  AND lien_status = '1'
GROUP BY year, race_eth
ORDER BY year, race_eth
```

### Purchase vs refinance volume by year (harmonized -- all 25 years)
```sql
SELECT
    year,
    loan_purpose_harmonized,
    SUM(TRY_CAST(loan_amount AS DOUBLE)) / 1e12 AS volume_trillions,
    COUNT(*)                                     AS count
FROM lar_panel
WHERE action_taken = '1'
  AND loan_purpose_harmonized IN ('1','3')
GROUP BY year, loan_purpose_harmonized
ORDER BY year, loan_purpose_harmonized
```

### Median income by county (remember: income is in $000s)
```sql
SELECT
    year,
    APPROX_QUANTILE(TRY_CAST(income AS DOUBLE), 0.50) * 1000 AS median_income_dollars,
    AVG(TRY_CAST(income AS DOUBLE)) * 1000                   AS mean_income_dollars,
    COUNT(*)                                                  AS n
FROM lar_panel
WHERE LEFT(census_tract, 5) = '48201'   -- Harris County, TX
  AND income IS NOT NULL
  AND income NOT IN ('', 'NA', 'Exempt')
  AND action_taken = '1'
GROUP BY year ORDER BY year
```

### FHA vs Conventional share for first-time-homebuyer proxy (FHA purchase loans)
```sql
SELECT
    year,
    loan_type,
    COUNT(*) AS n,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY year), 1) AS share_pct
FROM lar_panel
WHERE action_taken = '1'
  AND loan_purpose = '1'           -- purchase (raw; same code all years)
  AND lien_status = '1'            -- 2004+ only; NULL for 2000-2003
  AND occupancy_type = '1'
GROUP BY year, loan_type
ORDER BY year, loan_type
```

### Top lenders by origination count (2023)
```sql
SELECT
    l.lei,
    av.respondent_name,
    COUNT(*) AS originations,
    SUM(TRY_CAST(l.loan_amount AS DOUBLE)) / 1e9 AS volume_billions
FROM lar_panel AS l
LEFT JOIN avery_crosswalk AS av ON l.lei = av.lei AND av.activity_year = l.year
WHERE l.year = 2023 AND l.action_taken = '1'
GROUP BY l.lei, av.respondent_name
ORDER BY originations DESC
LIMIT 20
```

### GSE purchase rates by loan type (2018-2024)
```sql
SELECT
    year,
    loan_type,
    ROUND(100.0 * SUM(CASE WHEN purchaser_type_harmonized IN ('1','3') THEN 1 ELSE 0 END)
          / COUNT(*), 1) AS gse_share_pct   -- 1=Fannie, 3=Freddie
FROM lar_panel
WHERE action_taken = '1' AND year >= 2018
GROUP BY year, loan_type
ORDER BY year, loan_type
```

---

## Validated Row Counts (All 25 Years)

| Year | Rows | Source |
|------|------|--------|
| 2024 | 12,229,298 | FFIEC snapshot |
| 2023 | 11,483,889 | FFIEC snapshot |
| 2022 | 16,080,210 | FFIEC snapshot |
| 2021 | 26,124,552 | FFIEC snapshot |
| 2020 | 25,551,868 | FFIEC snapshot |
| 2019 | 17,545,457 | FFIEC snapshot |
| 2018 | 15,119,651 | FFIEC snapshot |
| 2017 | 14,285,496 | FFIEC snapshot |
| 2016 | 16,332,987 | CFPB historic |
| 2015 | 14,374,184 | CFPB historic |
| 2014 | 12,049,341 | CFPB historic |
| 2013 | 17,016,159 | CFPB historic |
| 2012 | 18,691,551 | CFPB historic |
| 2011 | 14,873,415 | CFPB historic |
| 2010 | 16,348,557 | CFPB historic |
| 2009 | 19,493,491 | CFPB historic |
| 2008 | 17,391,570 | CFPB historic |
| 2007 | 26,605,695 | CFPB historic |
| 2006 | 34,155,358 | ICPSR (38 cols) |
| 2005 | 36,457,234 | ICPSR (38 cols) |
| 2004 | 33,630,472 | ICPSR (38 cols) |
| 2003 | 41,579,147 | ICPSR (23 cols) |
| 2002 | 31,310,408 | ICPSR (23 cols) |
| 2001 | 27,643,161 | ICPSR (23 cols) |
| 2000 | 19,250,595 | ICPSR (23 cols) |

2018-2023 gaps vs CFPB API are due to snapshot-timing lag (late amendments absorbed
into CFPB live DB after snapshot cut). Not pipeline issues.

---

## Performance Tips

1. **Always filter `year` first** -- triggers partition pruning (critical for speed)
2. **Use `APPROX_QUANTILE`** instead of `QUANTILE` for median on large year slices
3. **Sample for exploration**: `SELECT * FROM lar_panel WHERE year=2023 USING SAMPLE 10000 ROWS`
4. **Avoid `SELECT *` on full years** -- 99+ VARCHAR columns, 10-41M rows per year
5. **Cast once in a CTE** rather than repeating `TRY_CAST` in WHERE and SELECT
6. **Avoid cross-year scans without year filter** -- 536M rows total

---

## Common Mistakes to Avoid

| Mistake | Correction |
|---------|-----------|
| `WHERE loan_purpose = '3' AND year = 2020` | Use `loan_purpose_harmonized = '3'` for 2018+ (code '3' doesn't exist) |
| `SUM(income)` to get dollars | `SUM(TRY_CAST(income AS DOUBLE)) * 1000` (income is $000s all years) |
| `COUNT(*) = applications` for small lenders | Small lender rows are replicated ~7x in snapshot -- use CFPB API for true counts |
| `loan_amount` in $000s for any year | ETL already scaled pre-2018 to whole dollars -- `loan_amount` is always whole dollars |
| Joining avery_crosswalk on `lei` only | Must include `AND av.activity_year = l.year` (one row per lender per year) |
| Querying `open_end_line_of_credit` for 2000-2017 | Field is NULL pre-2018; filter 2018+ only or omit |
| Using `lien_status`, `hoepa_status`, `rate_spread` for 2000-2003 | These fields are NULL for 2000-2003 (added by 2004 HMDA reform) |
| Using `applicant_ethnicity_1` for 2000-2003 | Ethnicity field is NULL for 2000-2003 |
