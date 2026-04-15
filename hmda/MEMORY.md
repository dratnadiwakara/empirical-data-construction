# HMDA Pipeline -- Memory & Reference

## What Was Built

A full ETL pipeline for the HMDA (Home Mortgage Disclosure Act) public LAR dataset
covering **2000-2024** (25 years, ~536 million rows). Raw files are downloaded from
FFIEC and the CFPB historic data portal, or manually obtained from OpenICPSR, then
harmonized across four data source eras and written to hive-partitioned Parquet.
All years are queryable via a DuckDB `lar_panel` VIEW that includes six computed
harmonization columns bridging categorical code differences across the 2017/2018
reform boundary.

---

## Data Sources (Four-Era Architecture)

| Era | Years | Source | Format | Header | Cols |
|-----|-------|--------|--------|--------|------|
| Post-reform | 2018-2024 | FFIEC snapshot `_pipe.zip` | Pipe `\|` | Yes | 99 |
| Pre-reform FFIEC | 2017 | FFIEC snapshot `_txt.zip` | Pipe `\|` | **No** (hardcoded `COLUMNS_2017`) | 45 |
| Pre-reform CFPB | 2007-2016 | CFPB historic portal `_labels.zip` | Comma `,` | Yes | 78 (45 kept) |
| ICPSR pre-CFPB | 2000-2006 | OpenICPSR project 151921 (manual) | Pipe `\|` | Yes | 38 or 23 |

The CFPB historic files (2007-2016) have **78 columns** -- each categorical field has BOTH a
code column (e.g., `loan_type=1`) AND a label column (e.g., `loan_type_name="Conventional"`).
The ETL keeps codes and drops all label columns via `COLS_TO_DROP_CFPB_HISTORIC`.

ICPSR files (2000-2006) have **two sub-eras** due to the 2004 HMDA reform:
- **2004-2006**: 38 columns (post-reform -- added ethnicity, race_2-5, preapproval, property_type, rate_spread, hoepa_status, lien_status)
- **2000-2003**: 23 columns (pre-reform -- only race_1, no ethnicity, no lien/hoepa/preapproval)

All ICPSR values are pure numeric codes (no label columns). ICPSR ZIPs use **Deflate64**
compression; extract with `unzip` from Git (`C:\Program Files\Git\usr\bin\unzip.exe`) --
Python's `zipfile` module and PowerShell's `Expand-Archive` do NOT support Deflate64.

### Download URL Templates

```
2018-2024: https://files.ffiec.cfpb.gov/static-data/snapshot/{year}/{year}_public_lar_pipe.zip
2017:      https://files.ffiec.cfpb.gov/static-data/snapshot/2017/2017_public_lar_txt.zip
2007-2016: https://files.consumerfinance.gov/hmda-historic-loan-data/hmda_{year}_nationwide_all-records_labels.zip
2000-2006: Manual download from https://www.openicpsr.org/openicpsr/project/151921/version/V1/view
```

---

## File Layout on Disk

```
C:\empirical-data-construction\hmda\
  hmda.duckdb                       -- Master DuckDB (all tables + lar_panel VIEW)
  raw\{year}\                       -- Downloaded ZIP + extracted TXT (delete after staging)
  staging\year={year}\data.parquet  -- Hive-partitioned Parquet (snappy)
  avery\                            -- Philly Fed Avery XLSX files
```

ICPSR raw ZIPs (2000-2006) remain in `raw\` as they were manually downloaded.
Extracted TXT files can be deleted after staging to reclaim disk space.

---

## DuckDB Tables

| Table | Rows | Description |
|-------|------|-------------|
| `lar_panel` | ~536M | VIEW over 25 hive-partitioned Parquets |
| `avery_crosswalk` | ~264K | Philly Fed lender file; one row per lender per year |
| `panel_metadata` | 25 | One row per year: row_count, match_rate, build info |
| `arid2017_to_lei` | ~5.4K | Pre-2018 respondent_id -> LEI crosswalk |

### DuckDB Connection

```python
import duckdb
conn = duckdb.connect("C:/empirical-data-construction/hmda/hmda.duckdb", read_only=True)
conn.execute("PRAGMA threads=8")
conn.execute("PRAGMA memory_limit='16GB'")
```

Or via config.py:
```python
from config import get_duckdb_path, DUCKDB_THREADS, DUCKDB_MEMORY_LIMIT
conn = duckdb.connect(str(get_duckdb_path()), read_only=True)
```

---

## Column Schema

All columns are stored as **VARCHAR**. Cast to numeric at query time using
`TRY_CAST(col AS DOUBLE)` or `TRY_CAST(col AS INTEGER)`.

### Base columns from MASTER_SCHEMA (99 VARCHAR columns)

Core identifiers: `activity_year`, `lei`, `derived_msa_md`, `state_code`, `county_code`, `census_tract`

Loan characteristics: `loan_type`, `loan_purpose`, `lien_status`, `loan_amount`, `interest_rate`,
`rate_spread`, `hoepa_status`, `total_loan_costs`, `loan_term`, `conforming_loan_limit`,
`combined_loan_to_value_ratio`, `property_value`, `debt_to_income_ratio`,
`open_end_line_of_credit`, `reverse_mortgage`, `business_or_commercial_purpose`,
`negative_amortization`, `interest_only_payment`, `balloon_payment`

Applicant demographics: `applicant_ethnicity_1` thru `_5`, `co_applicant_ethnicity_1` thru `_5`,
`applicant_race_1` thru `_5`, `co_applicant_race_1` thru `_5`,
`applicant_sex`, `co_applicant_sex`, `applicant_age`, `co_applicant_age`,
`applicant_credit_score_type`, `co_applicant_credit_score_type`

Application outcome: `action_taken`, `purchaser_type`, `preapproval`,
`denial_reason_1` thru `_4`, `submission_of_application`, `initially_payable_to_institution`

Property: `construction_method`, `occupancy_type`, `total_units`, `manufactured_home_secured_property_type`

AUS / underwriting: `aus_1` thru `_5`

Tract-level: `tract_population`, `tract_minority_population_percent`, `ffiec_msa_md_median_family_income`,
`tract_to_msa_income_percentage`, `tract_owner_occupied_units`, `tract_one_to_four_family_homes`,
`tract_median_age_of_housing_units`

Derived CFPB fields: `derived_loan_product_type`, `derived_dwelling_category`, `derived_ethnicity`,
`derived_race`, `derived_sex`

### Pre-2018 extra columns (NULL for 2018+)

`respondent_id` -- 10-char pre-2018 lender identifier
`agency_code` -- 1=OCC, 2=FRS, 3=FDIC, 5=NCUA, 7=HUD
`property_type` -- 1=SFR, 2=Manufactured, 3=Multifamily (present 2004+; NULL for 2000-2003)

### Avery supplement columns (joined at ETL time)

`rssd_id`, `parent_rssd`, `top_holder_rssd`

### Harmonized VIEW columns (computed at query time for ALL years)

See Harmonization section below.

### `year` column

INTEGER partition column added by the pipeline.

---

## Unit Conventions -- Critical

| Column | Unit | Notes |
|--------|------|-------|
| `loan_amount` | **Whole dollars** | Pre-2018 source stored $000s; ETL scales x1000 |
| `income` | **$000s** | ALL years -- never changed. Multiply x1000 to get dollars |
| `census_tract` | 11-char FIPS | Pre-2018 constructed from state+county+tract components |
| `interest_rate` | Percent (e.g., "4.5") | 2018+ only; NULL for 2000-2017 |
| `rate_spread` | Percent above APOR | Available 2004+; NULL for 2000-2003 |

---

## Harmonized VIEW Columns (Bridge 2017/2018 Boundary)

Six computed columns in `lar_panel` VIEW work correctly for ALL years 2000-2024:

```sql
loan_purpose_harmonized
  -- Maps 2018+ codes 31/32 -> '3' (all refinancings = '3')
  -- Use this for cross-year refinance queries
  -- Raw loan_purpose still available; '31'=cash-out refi, '32'=rate-term refi (2018+ only)

purchaser_type_harmonized
  -- Maps 2018+ codes 71/72 -> '7'

denial_reason_1_harmonized
denial_reason_2_harmonized
denial_reason_3_harmonized
  -- Maps 2017 code '0' (N/A) -> '10' (2018+ N/A code)
  -- Also applies correctly to 2000-2016 (same '0' N/A code)

preapproval_harmonized
  -- Maps 2017 code '3' (Not Applicable) -> '2' (not requested)
  -- Also applies correctly to 2007-2016; 2000-2003 preapproval is NULL
```

---

## Key Categorical Code Reference

### action_taken
| Code | Meaning |
|------|---------|
| 1 | Loan originated |
| 2 | Approved, not accepted |
| 3 | Denied |
| 4 | Withdrawn by applicant |
| 5 | File closed for incompleteness |
| 6 | Purchased by institution |
| 7 | Preapproval denied |
| 8 | Preapproval approved, not accepted |

### loan_purpose (use loan_purpose_harmonized for cross-year queries)
| Code | 2000-2017 | 2018+ |
|------|-----------|-------|
| 1 | Home purchase | Home purchase (same) |
| 2 | Home improvement | Home improvement (same) |
| 3 | Refinancing | Does not exist (split into 31/32) |
| 31 | -- | Cash-out refinancing |
| 32 | -- | Non-cash-out refinancing |
| 4 | -- | Other purpose |
| 5 | -- | Not applicable |

### loan_type
| Code | Meaning |
|------|---------|
| 1 | Conventional |
| 2 | FHA-insured |
| 3 | VA-guaranteed |
| 4 | FSA/RHS-guaranteed |

### lien_status (2004+ only; NULL for 2000-2003)
| Code | Meaning |
|------|---------|
| 1 | First lien |
| 2 | Subordinate lien |
| 3 | Not secured (2004-2017 only) |
| 4 | Not applicable/purchased (2004-2017 only) |

### conforming_loan_limit (2018+ only)
| Code | Meaning |
|------|---------|
| C | Conforming |
| NC | Non-conforming (jumbo) |
| NCB | Non-conforming, below limit |
| U | Undetermined |
| NA | Not applicable |

### occupancy_type
| Code | Meaning |
|------|---------|
| 1 | Principal residence |
| 2 | Second residence |
| 3 | Investment property |

### purchaser_type (use purchaser_type_harmonized for cross-year queries)
| Code | Meaning |
|------|---------|
| 0 | Not sold |
| 1 | Fannie Mae |
| 2 | Ginnie Mae |
| 3 | Freddie Mac |
| 4 | Farmer Mac |
| 5 | Private securitization |
| 6 | Commercial bank/savings institution |
| 7 | Non-bank financial institution (2000-2017) |
| 71 | Credit union/mortgage company (2018+) |
| 72 | Life insurance company (2018+) |

### applicant_race_1
| Code | Meaning |
|------|---------|
| 1 | American Indian or Alaska Native |
| 2 | Asian |
| 21-27 | Asian sub-categories (2018+) |
| 3 | Black or African American |
| 4 | Native Hawaiian or Other Pacific Islander |
| 41-44 | Pacific Islander sub-categories (2018+) |
| 5 | White |
| 6 | Information not provided by applicant |
| 7 | Not applicable |

### applicant_ethnicity_1 (2004+ only; NULL for 2000-2003)
| Code | Meaning |
|------|---------|
| 1 | Hispanic or Latino |
| 11-14 | Hispanic sub-categories (2018+): Mexican, Puerto Rican, Cuban, Other |
| 2 | Not Hispanic or Latino |
| 22 | Not Hispanic (alternate code) |
| 3 | Information not provided |
| 4 | Not applicable |

### construction_method (2018+ only)
| Code | Meaning |
|------|---------|
| 1 | Site-built |
| 2 | Manufactured home |

### open_end_line_of_credit (2018+ only)
| Code | Meaning |
|------|---------|
| 1 | Open-end line of credit |
| 2 | Not an open-end line of credit (closed-end) |

### reverse_mortgage (2018+ only)
| Code | Meaning |
|------|---------|
| 1 | Reverse mortgage |
| 2 | Not a reverse mortgage |

---

## RSSD Lender Linkage

### Pre-2018 (2000-2017) -- join via respondent_id + agency_code

```sql
LEFT JOIN avery_crosswalk AS av
    ON l.respondent_id = av.respondent_id
    AND TRY_CAST(l.agency_code AS INTEGER) = av.agency_code
    AND av.activity_year = l.year
```

100% match rate validated for all 2007-2017 years. Avery crosswalk covers 1990-present.

### Post-2018 -- join via LEI

```sql
LEFT JOIN avery_crosswalk AS av ON l.lei = av.lei AND av.activity_year = l.year
```

### Universal join (handles both eras in one query)

```sql
LEFT JOIN avery_crosswalk AS av
    ON av.activity_year = l.year
    AND CASE
        WHEN l.lei IS NOT NULL AND l.lei NOT IN ('', 'NA', 'Exempt')
            THEN l.lei = av.lei
        ELSE
            l.respondent_id = av.respondent_id
            AND TRY_CAST(l.agency_code AS INTEGER) = av.agency_code
    END
```

`avery_crosswalk` columns: `activity_year`, `lei`, `respondent_id`, `agency_code`,
`rssd_id`, `parent_rssd`, `top_holder_rssd`, `respondent_name`, `assets`

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

## Avery Crosswalk Structure

One row per lender per year (not one row per lender).

- Pre-2018 section: ~230K rows, keyed on `respondent_id + agency_code + activity_year`
- Post-2018 section: ~34K rows, keyed on `lei + activity_year`
- Total: ~264K rows
- Coverage: 1990-present (Philly Fed HMDA Lender File)

---

## Execution Order

```bash
python -m hmda.avery            # Load Philly Fed lender crosswalk -> avery_crosswalk
python -m hmda.arid_xref        # Load ARID2017->LEI crosswalk -> arid2017_to_lei

# For each year (process one at a time to conserve disk):
python -m hmda.download --year 2024   # not needed for 2000-2006 (manual ICPSR ZIPs)
python -m hmda.construct --year 2024
# delete raw\ after each year to reclaim disk (raw TXT: 2-11 GB each)

# 2000-2006: ZIPs must be pre-placed in C:\empirical-data-construction\hmda\raw\
# Extract with: "C:\Program Files\Git\usr\bin\unzip.exe" HMDA_LAR_{year}.zip
# Then: python -m hmda.construct --year 2006

python -m hmda.inspect --year 2024   # validate
```

---

## Python Environment

- Virtual env: `.venv` in repo root
- Packages: `duckdb`, `polars`, `httpx`, `fastexcel`, `openpyxl`
- Run as modules from repo root: `python -m hmda.construct --year 2024`
- Windows: use ASCII-only logging output (cp1252 terminal -- no em-dash, arrows, etc.)

---

## Key Code Modules

| File | Purpose |
|------|---------|
| `config.py` | `FIN_DATA_ROOT` env var, path helpers, DuckDB settings |
| `hmda/metadata.py` | URL registry, MASTER_SCHEMA, all column rename maps, LABEL_TO_CODE, HARMONIZED_VIEW_EXPRS, ICPSR era constants |
| `hmda/download.py` | FFIEC/CFPB download with resume and idempotency manifest (2000-2006 not downloadable -- manual ICPSR) |
| `hmda/construct.py` | Out-of-core DuckDB ETL: four era-specific builders (_post2018, _2017, _cfpb_historic, _icpsr) |
| `hmda/avery.py` | Philly Fed lender crosswalk ingestion |
| `hmda/arid_xref.py` | ARID2017->LEI crosswalk loader |
| `hmda/inspect.py` | Interactive verification (row counts, nulls, RSSD coverage, harmonized cols) |

---

## Known Data Quirks

| Year | Issue |
|------|-------|
| 2018 | `loan_amount` max = 2,147,483,647 (INT32_MAX cap in source) |
| 2019 | 1 record with negative loan_amount (raw data error) |
| 2020 | 1 record with negative loan_amount (raw data error) |
| 2023 | ~80K fewer rows than CFPB live API (late amendments absorbed post-snapshot) |
| 2000-2003 | `lien_status`, `hoepa_status`, `rate_spread`, `preapproval`, `ethnicity` are NULL (pre-reform) |
| 2000-2003 | Only `applicant_race_1` available; `applicant_race_2` thru `_5` are NULL |

---

## Privacy Replication Warning

The FFIEC **snapshot** public LAR is NOT a 1-record-per-application file. Small lenders
have each application group replicated ~7x for privacy protection. `COUNT(*)` overcounts
small-lender applications. For true application counts use the CFPB Data Browser API.
Large lenders are not replicated (enough volume for anonymity). This applies to the
FFIEC snapshot years (2017-2024); CFPB historic (2007-2016) and ICPSR (2000-2006) files
are true 1-record-per-application files.

---

## Analytical Lessons from External Replication Work

### Lender Classification (Bank vs Nonbank)

Agency code alone is an imperfect proxy for depository status. Some large
independent mortgage companies file HMDA under FDIC (agency_code=3) despite
accepting no deposits. For research requiring a clean bank/nonbank split, supplement
agency_code with external lists (e.g., FDIC institution directory or manual review
of top lenders). Rule of thumb: agency_code IN ('1','2','3','5','9') covers
most depositories, but verify any lender with agency_code=3 that is known to be
a mortgage company.

### Lender-Level Aggregation (Holding Company Consolidation)

A single institution can file under multiple respondent_ids (e.g., the bank's main
charter plus a home-mortgage subsidiary). When computing institution-level market
shares or loan volumes, summing by respondent_id undercounts large multi-charter
banks. Use `avery_crosswalk.top_holder_rssd` to roll up to the holding-company level.
Individual respondent-level volumes will match published figures closely for standalone
entities (nonbanks, smaller banks) but diverge for large banking groups.

### Lender Identification Across Years (Avery Name Lookup)

The same institution can appear under 3-5 different `respondent_name` strings in the
Avery crosswalk across years (e.g., "LOANDEPOT.COM", "LOANDEPOT.COM LLC",
"LOANDEPOT.COM, LLC"). Always use `LIKE`/`LOWER()` pattern matching when searching
by name, and return `DISTINCT respondent_id` to collapse duplicates. Confirm the
match is unique by checking the respondent_id is the same across all name variants.

### Time-Conditional Lender Attributes

Lender characteristics that change over time (ownership type, product mix, regulatory
status) must be applied as year-conditional flags, not static labels. Assigning a
time-varying attribute for all years when a lender only had that attribute after a
specific year inflates counts by 15-50% depending on how far back the history extends.
Always record the adoption/change year and include `AND l.year >= {adoption_year}` in
the CASE expression.

### Jumbo Loan Classification (Pre-2018)

The `conforming_loan_limit` column is only available 2018+. For 2000-2017, the
national baseline conforming loan limit (CLL) was $417,000 (2006-2016) and varies by
year before that. However, high-cost MSAs have local CLLs up to $625,500 during
2009-2016. Using the flat national CLL will overcount jumbos by ~3pp in the originated
sample because loans of $418K-$625K in high-cost areas are conforming. For a precise
jumbo flag pre-2018, bring in the FHFA county-level CLL file and join on state_code +
county_code + year.

### No Co-Applicant Identification (Pre-2018)

In the CFPB historic (2007-2016) and ICPSR (2000-2006) data, a solo application
has `co_applicant_sex = '4'` ("Not applicable"). Use `co_applicant_sex IN ('4','5')`
to be safe across slightly varying file encodings. Do NOT use NULL to detect solo
applications; co_applicant_sex is populated for all records.

### LTI Computation

Loan-to-income ratio: `loan_amount / (income * 1000)` because loan_amount is whole
dollars and income is $000s. Always winsorize at symmetric tails (0.5%/99.5%) before
computing means; the raw distribution has extreme outliers from very low income reports.
Filter `income > 0 AND loan_amount > 0` before the LTI calculation (not just IS NOT
NULL, since 0 income records exist in the data).

### Sample Period Truncation in Annual Data

Public HMDA records activity_year only (not the exact application or action date).
Research using half-year or quarterly HMDA cuts cannot be exactly replicated with the
public data. Full-year samples will have ~5-10% more observations than a H1-only or
H2-only sample of the same year, and the composition shifts because purchase loan
activity is seasonal (peaks in spring/summer) while refinance activity responds to
rate movements throughout the year.

---

## Sanity Check Completed

Replicated CFPB Table 4 (Home-Purchase and Refinance Loan Denial Rates by Enhanced
Loan Types and Race/Ethnicity, 2018-2020) using the built data. Results match
published values within 0.1-0.3pp for aggregate rows. Slightly larger gaps (~1pp)
for "Other minority" and "Joint" rows are attributable to the CFPB's proprietary
"enhanced race" classification methodology (documented in Table 2, Note 1 of the
CFPB HMDA Data Overview report) which applies a specific priority ordering for
multi-race and mixed-household records not fully specified in public documentation.

2006/2007 cross-era validation: census tract count (65,871 vs 65,787), median income
($72.1K vs $72.3K), and code systems confirmed consistent. 2006 has more rows (34.2M
vs 26.6M) consistent with the housing bubble peak.
