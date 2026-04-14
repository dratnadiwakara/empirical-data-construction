# HMDA LAR Panel 2007-2024 — Implementation Plan

## Context
Build a harmonized loan-level HMDA LAR panel (2007-2024) from CFPB/FFIEC snapshot
flat files into a DuckDB-backed Parquet pipeline.

**Three data source eras:**
| Era | Years | Source | Format | Lender ID |
|-----|-------|--------|--------|-----------|
| Post-reform | 2018–2024 | FFIEC snapshot | Pipe-delimited, header, 99 cols | LEI |
| Pre-reform FFIEC | 2017 | FFIEC snapshot | Pipe-delimited, **no header**, 45 cols | respondent_id + agency_code |
| Pre-reform CFPB | 2007–2016 | CFPB historic portal | **Comma-delimited**, header, 45 cols | respondent_id + agency_code |

All three eras land in the same `lar_panel` VIEW (union_by_name=true). Harmonized
computed columns bridge categorical code differences across all boundaries. The stack
is Python + DuckDB (no pandas). All data lives under `C:\empirical-data-construction`.

---

## Two-Table Architecture

### Table 1: LAR Loan-Level Data (`lar_panel` VIEW over Parquet files)
- One row per mortgage application record (matching the raw file row count exactly)
- Contains all application-level fields from the HMDA LAR
- **No lender variables** — keeps the row count clean and the table lean

### Table 2: Lender Crosswalk (`avery_crosswalk` in DuckDB)
- One row per lender per year
- From Philly Fed HMDA Lender File (covers 1990–present)
  - `hmda-1990-2017.xlsx` → respondent_id/agency_code → RSSD (2007-2017 covered)
  - `hmda-2018-present.xlsx` → LEI → RSSD (2018-2024 covered)
- Join key: `lei + year` (2018+) or `respondent_id + agency_code + year` (pre-2018)

---

## Scope & Simplifications

- **2018–2024:** Pipe-delimited, header=true, 99 cols, no renames, no scaling.
- **2017 (FFIEC):** Pipe-delimited, **no header** → supply `COLUMNS_2017` list (45 names).
  Column renames, loan_amount ×1000, census_tract FIPS construction applied at ETL.
- **2007–2016 (CFPB):** Comma-delimited, header=true, ~45 cols.
  Same transforms as 2017. **Labels file** — categorical values may be text labels
  (e.g., "Refinancing") instead of codes (e.g., "3"). ETL CASE expressions handle both.
- **`loan_amount` scaling pre-2018:** Source stores values in $000s → ETL multiplies ×1000.
- **`income` never scaled:** $000s in all years (CFPB convention unchanged).
- **Census tract construction pre-2018:** 7-char XXXX.XX + state + county → 11-char FIPS.
- **Harmonized VIEW columns:** Six `*_harmonized` columns bridge code differences across
  the 2017/2018 boundary. They also apply correctly to 2007-2016 (same code set as 2017).
- **`application_date_indicator`** (45th field in 2007-2016 CFPB files): dropped at ETL.
- **No ETL-time lender join:** Join avery_crosswalk at query time.

---

## Files

```
empirical-data-construction/
├── config.py
├── utils/
│   ├── logging_utils.py
│   └── duckdb_utils.py
└── hmda/
    ├── metadata.py
    ├── download.py
    ├── avery.py
    ├── construct.py
    ├── inspect.py
    └── plan.md
```

**Data layout (`C:\empirical-data-construction\hmda\`):**
```
raw/{year}/                        ← downloaded ZIP + extracted file (delete after staging)
staging/year={year}/data.parquet   ← loan-level Parquet (hive-partitioned)
avery/                             ← Philly Fed XLSX files
hmda.duckdb                        ← DuckDB: lar_panel VIEW + avery_crosswalk + metadata
```

---

## Disk Space Management (CRITICAL for 8GB machine)

Raw files are 1–12 GB each. Process one year at a time:

```bash
python -m hmda.download --year 2016
python -m hmda.construct --year 2016
# verify, then:
rm -rf "C:\empirical-data-construction\hmda\raw\2016"
```

---

## 1. Download URLs

| Year | URL | Notes |
|------|-----|-------|
| 2018–2024 | `https://files.ffiec.cfpb.gov/static-data/snapshot/{year}/{year}_public_lar_pipe.zip` | pipe, header |
| **2017** | `https://files.ffiec.cfpb.gov/static-data/snapshot/2017/2017_public_lar_txt.zip` | pipe, **no header** |
| **2007–2016** | `https://files.consumerfinance.gov/hmda-historic-loan-data/hmda_{year}_nationwide_all-records_labels.zip` | comma, header, labels |

The 2017 CFPB file (`hmda_2017_nationwide_all-records_labels.zip`) exists for validation only —
it has the same 14,285,496 rows as our FFIEC 2017 build.

---

## 2. `config.py` (unchanged)

---

## 3. `hmda/metadata.py`

Key additions for 2007-2016 extension:
- `FIRST_YEAR = 2007`, `ALL_YEARS = [2024, ..., 2007]`
- `CFPB_HISTORIC_TMPL` — download URL template for 2007-2016
- `CFPB_HISTORIC_FIRST_YEAR = 2007`, `CFPB_HISTORIC_LAST_YEAR = 2016`
- `is_cfpb_historic(year)` — True for 2007-2016
- `get_delimiter(year)` — `','` for 2007-2016, `'|'` for 2017-2024
- `COLUMN_RENAMES_CFPB_HISTORIC` — comprehensive mapping of CFPB CSV headers → master schema
- `LABEL_TO_CODE` — categorical text label → numeric code mapping for all 2007-2016 fields
- Updated `get_source_urls()` to return CFPB URL for 2007-2016

---

## 4. `hmda/download.py` (no changes required)

The `get_source_urls()` update in metadata.py routes correctly. The download infrastructure
(`_download_standard_year`, ZIP extraction, idempotency manifest) works unchanged.

---

## 5. `hmda/avery.py`

The Philly Fed `hmda-1990-2017.xlsx` already covers 2007-2017. No changes needed.

**Coverage check after loading:**
```sql
SELECT activity_year, COUNT(*) AS lenders
FROM avery_crosswalk
WHERE activity_year BETWEEN 2007 AND 2017
GROUP BY activity_year ORDER BY activity_year;
```

---

## 6. `hmda/construct.py`

### ETL pipeline — CFPB historic (2007-2016)

```
find_raw_file(year)                → comma-delimited CSV path
    ↓
_get_csv_columns(path, year)       → list of column names from CSV header
    ↓
_build_select_exprs_cfpb_historic(csv_cols)
  - Map raw column names via COLUMN_RENAMES_CFPB_HISTORIC
  - For categorical fields: CASE expression handling both codes AND labels
  - loan_amount: scale ×1000 from $000s to whole dollars
  - census_tract: construct 11-char FIPS from state + county + raw tract
  - Drop: edit_status, sequence_number, application_date_indicator
  - Keep: respondent_id, agency_code, property_type (pre-2018 extras)
  - NULL-fill: all POST_2018_ONLY_COLS (lei, interest_rate, etc.)
    ↓
COPY (
  SELECT {master_schema_cols}, {year}::INTEGER AS year
  FROM read_csv(path, sep=',', header=true, all_varchar=true, ignore_errors=true)
) TO 'staging/year={year}/data.parquet.tmp'
    ↓
atomic rename → data.parquet
    ↓
upsert panel_metadata row
    ↓
recreate lar_panel VIEW over all staging Parquets
```

### Label-to-code conversion strategy

The CFPB "labels" CSV files (2007-2016) use text labels for categorical values. The ETL
applies SQL CASE expressions that handle **both codes and labels** transparently:

```sql
-- Example for loan_purpose
CASE
  WHEN TRIM("loan_purpose") IN ('1','2','3') THEN TRIM("loan_purpose")  -- already a code
  WHEN LOWER(TRIM("loan_purpose")) = 'home purchase' THEN '1'
  WHEN LOWER(TRIM("loan_purpose")) = 'home improvement' THEN '2'
  WHEN LOWER(TRIM("loan_purpose")) = 'refinancing' THEN '3'
  ELSE TRIM("loan_purpose")
END AS "loan_purpose"
```

This design means the same ETL code works if CFPB switches from labels to codes
(or if a year happens to use codes already).

---

## 7. DuckDB Schema (unchanged)

The `lar_panel` VIEW already applies `union_by_name=true` and the six harmonized columns.
Adding years 2007-2016 requires only adding their Parquet paths to the VIEW — no SQL changes.

The harmonized columns already work for 2007-2016:
- `loan_purpose_harmonized`: 2007-2016 code '3' stays '3' (no change needed)
- `purchaser_type_harmonized`: 2007-2016 code '7' stays '7'
- `denial_reason_N_harmonized`: maps '0' → '10' (2007-2016 use '0' for N/A)
- `preapproval_harmonized`: maps '3' → '2' (2007-2016 use '3' for N/A)

---

## 8. Analysis-Time RSSD Join

| Era | LAR identifier | Join to avery_crosswalk on |
|-----|---------------|---------------------------|
| 2018–2024 | `lei` | `lei + year` |
| 2007–2017 | `respondent_id` + `agency_code` | `respondent_id + agency_code + year` |

```sql
-- Universal join (all years)
SELECT l.*, av.rssd_id, av.parent_rssd, av.top_holder_rssd, av.respondent_name, av.assets
FROM lar_panel AS l
LEFT JOIN avery_crosswalk AS av
    ON av.activity_year = l.year
    AND CASE
        WHEN l.lei IS NOT NULL AND l.lei != ''
            THEN l.lei = av.lei
        ELSE
            l.respondent_id = av.respondent_id
            AND TRY_CAST(l.agency_code AS INTEGER) = av.agency_code
    END;
```

---

## 9. Acceptance Validation

```sql
-- 1. Row counts per year (compare to table below)
SELECT year, COUNT(*) AS n FROM lar_panel GROUP BY year ORDER BY year;

-- 2. loan_amount range — all years should be whole dollars ($1K–$10B+)
SELECT year, MIN(TRY_CAST(loan_amount AS DOUBLE)), MAX(TRY_CAST(loan_amount AS DOUBLE))
FROM lar_panel WHERE year < 2018 GROUP BY year ORDER BY year;

-- 3. census_tract length — all pre-2018 rows should be 11 chars (or 'NA')
SELECT year, LENGTH(census_tract) AS len, COUNT(*) AS n
FROM lar_panel WHERE year BETWEEN 2007 AND 2017 AND census_tract != 'NA'
GROUP BY year, len ORDER BY year, len;

-- 4. loan_purpose codes are consistent across years
SELECT year, loan_purpose, COUNT(*) AS n
FROM lar_panel WHERE year BETWEEN 2007 AND 2019
GROUP BY year, loan_purpose ORDER BY year, loan_purpose;
-- Expect: 2007-2017 use codes 1,2,3; 2018-2019 use 1,2,31,32,4,5

-- 5. RSSD linkage for 2007-2016
SELECT year,
    COUNT(*) AS total,
    SUM(CASE WHEN av.rssd_id IS NOT NULL THEN 1 END) AS matched,
    ROUND(100.0 * SUM(CASE WHEN av.rssd_id IS NOT NULL THEN 1 END) / COUNT(*), 2) AS pct
FROM lar_panel AS l
LEFT JOIN avery_crosswalk AS av
    ON l.respondent_id = av.respondent_id
    AND TRY_CAST(l.agency_code AS INTEGER) = av.agency_code
    AND av.activity_year = l.year
WHERE l.year BETWEEN 2007 AND 2016
GROUP BY year ORDER BY year;
```

**Expected row counts (CFPB Data Browser):**

| Year | Expected Rows | Source |
|------|-------------|--------|
| 2024 | 12,229,298 | FFIEC snapshot |
| 2023 | 11,564,178 | FFIEC snapshot |
| 2022 | 16,099,307 | FFIEC snapshot |
| 2021 | 26,269,980 | FFIEC snapshot |
| 2020 | 25,699,043 | FFIEC snapshot |
| 2019 | 17,573,963 | FFIEC snapshot |
| 2018 | 15,138,510 | FFIEC snapshot |
| 2017 | 14,285,496 | FFIEC snapshot (validated) |
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

---

## 10. Execution Order

```bash
# Step 0: Lender crosswalk (already done; covers 1990-2024)
# python -m hmda.avery   ← run once if not already done

# Step 1: 2018-2024 already built

# Step 2: 2017 already built (FFIEC, validated, 14,285,496 rows)

# Step 3: CFPB historic years, one at a time (newest first)
# For each year 2016 → 2007:
python -m hmda.download --year 2016
python -m hmda.construct --year 2016
python -m hmda.inspect --year 2016
rm -rf "C:\empirical-data-construction\hmda\raw\2016"

# Continue for 2015, 2014, 2013, 2012, 2011, 2010, 2009, 2008, 2007

# Optional: validate 2017 CFPB vs our FFIEC build
python -m hmda.inspect --compare-years 2017 2016
```

---

## 11. 2007-2016 Extension Details

### 11.1 CFPB Historic File Format

- **Delimiter:** Comma (`,`)
- **Header row:** Yes (column names in first row)
- **Encoding:** ASCII / Latin-1
- **45 fields** (same logical content as 2017 FFIEC, different names)
- **Extra field:** `application_date_indicator` (field 45 in CFPB files, absent from 2017 FFIEC) → dropped at ETL

### 11.2 Column Name Mapping (CFPB CSV → master schema)

| CFPB CSV Column (expected) | Master Schema Column | Transform |
|---------------------------|---------------------|-----------|
| `as_of_year` | `activity_year` | rename |
| `respondent_id` | `respondent_id` | pass through (pre-2018 extra) |
| `agency_code` | `agency_code` | label→code + pass through |
| `loan_type` | `loan_type` | label→code |
| `property_type` | `property_type` | label→code + pass through |
| `loan_purpose` | `loan_purpose` | label→code |
| `owner_occupancy` / `occupancy` | `occupancy_type` | rename + label→code |
| `loan_amount_000s` / `loan_amount` | `loan_amount` | rename + ×1000 scaling |
| `preapproval` | `preapproval` | label→code |
| `action_taken` | `action_taken` | label→code |
| `msa_md` / `msa` | `derived_msa_md` | rename |
| `state_code` | `state_code` | pass through |
| `county_code` | `county_code` | pass through |
| `census_tract_number` / `census_tract` | `census_tract` | rename + 11-char FIPS construction |
| `applicant_ethnicity` | `applicant_ethnicity_1` | rename + label→code |
| `co_applicant_ethnicity` | `co_applicant_ethnicity_1` | rename + label→code |
| `applicant_race_1`–`_5` | same | label→code |
| `co_applicant_race_1`–`_5` | same | label→code |
| `applicant_sex` | same | label→code |
| `co_applicant_sex` | same | label→code |
| `applicant_income_000s` / `income` | `income` | rename (keep in $000s) |
| `purchaser_type` | same | label→code |
| `denial_reason_1`–`_3` | same | label→code |
| `rate_spread` | same | pass through |
| `hoepa_status` | same | label→code |
| `lien_status` | same | label→code |
| `edit_status` | — | DROP |
| `sequence_number` | — | DROP |
| `population` / `tract_population` | `tract_population` | rename |
| `minority_population_percent` / `tract_minority_population_percent` | `tract_minority_population_percent` | rename |
| `ffiec_median_family_income` / `ffiec_msa_md_median_family_income` | `ffiec_msa_md_median_family_income` | rename |
| `tract_to_msa_income_percent` / `tract_to_msa_income_percentage` | `tract_to_msa_income_percentage` | rename |
| `number_of_owner_occupied_units` / `tract_owner_occupied_units` | `tract_owner_occupied_units` | rename |
| `number_of_1_to_4_family_units` / `tract_one_to_four_family_housing_units` | `tract_one_to_four_family_homes` | rename |
| `application_date_indicator` | — | DROP |

**47 POST_2018_ONLY_COLS** → NULL in 2007-2016 Parquets (same as 2017).

### 11.3 Categorical Codes: 2007-2016 vs 2017

The categorical code system is **identical** for 2007-2016 and 2017. Both use:
- `loan_purpose`: 1=Purchase, 2=Improvement, 3=Refinancing
- `purchaser_type`: 0-9 (same set)
- `denial_reason`: 0=N/A, 1-9=specific reasons
- `preapproval`: 1=Requested, 2=Not requested, 3=Not applicable
- `lien_status`: 1-4
- `hoepa_status`: 1-2

The six `HARMONIZED_VIEW_EXPRS` already work correctly for 2007-2016 without modification.

### 11.4 Census Tract (2007-2016 identical to 2017)

Same 7-char `XXXX.XX` format, same 11-char FIPS construction rule. `CENSUS_TRACT_SQL_2017`
is reused directly (it's era-agnostic; the name is historical).

### 11.5 loan_amount Scaling (2007-2016 identical to 2017)

Source stores in $000s in all years 2007-2017. `LOAN_AMOUNT_SCALE_SQL_2017` is reused
(the column name in the SQL template is substituted with the actual raw column name).

### 11.6 RSSD Linkage for 2007-2016

The `avery_crosswalk` table (from Philly Fed `hmda-1990-2017.xlsx`) contains all lenders
for years 2007-2017. The join pattern is identical to 2017:

```sql
LEFT JOIN avery_crosswalk AS av
    ON l.respondent_id = av.respondent_id
    AND TRY_CAST(l.agency_code AS INTEGER) = av.agency_code
    AND av.activity_year = l.year
```

Expected match rate: >95% (some lenders file in years not covered by the crosswalk).

---

## 12. Files Modified (2007-2016 Extension)

| File | Change |
|------|--------|
| `hmda/metadata.py` | FIRST_YEAR→2007; get_source_urls for 2007-2016; COLUMN_RENAMES_CFPB_HISTORIC; LABEL_TO_CODE; is_cfpb_historic(); get_delimiter() |
| `hmda/construct.py` | _build_select_exprs_cfpb_historic(); comma delimiter for 2007-2016; dispatch in _build_select_exprs() |
| `hmda/inspect.py` | EXPECTED_ROWS for 2007-2016 |
| `hmda/plan.md` | This document |

---

## 13. 2017 Pre-Reform Extension (historical reference)

See §13.1–13.6 of the previous plan for full details on the 2017 FFIEC file structure.
Key differences vs 2007-2016 CFPB: pipe-delimited, **no header row**, column names
supplied via `COLUMNS_2017` hardcoded list.

### 13.1 Variable Mapping (2017 raw → master schema)

| 2017 Raw Column | Master Schema Column | Transform |
|----------------|---------------------|-----------|
| `as_of_year` | `activity_year` | rename |
| `respondent_id` | `respondent_id` | pass through |
| `agency_code` | `agency_code` | pass through |
| `loan_type` | `loan_type` | pass through |
| `property_type` | `property_type` | pass through (pre-2018 extra) |
| `loan_purpose` | `loan_purpose` | pass through (codes differ — see §13.2) |
| `owner_occupancy` | `occupancy_type` | rename |
| `loan_amount_000s` | `loan_amount` | rename + ×1000 scaling |
| `preapproval` | `preapproval` | pass through |
| `action_taken` | `action_taken` | pass through |
| `msa_md` / `msa` | `derived_msa_md` | rename |
| `state_code` | `state_code` | pass through |
| `county_code` | `county_code` | pass through |
| `census_tract` | `census_tract` | 11-char FIPS construction |
| `applicant_ethnicity` | `applicant_ethnicity_1` | rename |
| `co_applicant_ethnicity` | `co_applicant_ethnicity_1` | rename |
| `applicant_race_1`–`_5` | same | pass through |
| `co_applicant_race_1`–`_5` | same | pass through |
| `applicant_sex` | same | pass through |
| `co_applicant_sex` | same | pass through |
| `applicant_income_000s` | `income` | rename (keep in $000s) |
| `purchaser_type` | same | pass through |
| `denial_reason_1`–`_3` | same | pass through |
| `rate_spread` | same | pass through |
| `hoepa_status` | same | pass through |
| `lien_status` | same | pass through |
| `tract_one_to_four_family_housing_units` | `tract_one_to_four_family_homes` | rename |

**47 POST_2018_ONLY_COLS → NULL in 2017 Parquet**

### 13.2 Categorical Code Differences and Harmonized Columns

See §7 (DuckDB Schema) for the six `HARMONIZED_VIEW_EXPRS`.

The key 2017/2018 boundary differences:
- `loan_purpose`: 2017 uses '3' (Refinancing); 2018+ uses '31'/'32'
- `purchaser_type`: 2017 uses '7'; 2018+ uses '71'/'72'
- `denial_reason`: 2017 uses '0' for N/A; 2018+ uses '10'
- `preapproval`: 2017 uses '3' for N/A; removed in 2018+
