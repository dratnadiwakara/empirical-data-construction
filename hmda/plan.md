# HMDA LAR Panel 2000-2024 -- Implementation Plan

## Context
Build a harmonized loan-level HMDA LAR panel (2000-2024) from CFPB/FFIEC snapshot
flat files and OpenICPSR archives into a DuckDB-backed Parquet pipeline.

**Four data source eras:**
| Era | Years | Source | Format | Lender ID |
|-----|-------|--------|--------|-----------|
| Post-reform | 2018-2024 | FFIEC snapshot | Pipe-delimited, header, 99 cols | LEI |
| Pre-reform FFIEC | 2017 | FFIEC snapshot | Pipe-delimited, **no header**, 45 cols | respondent_id + agency_code |
| Pre-reform CFPB | 2007-2016 | CFPB historic portal | **Comma-delimited**, header, 78 cols (45 kept) | respondent_id + agency_code |
| ICPSR pre-CFPB | 2000-2006 | OpenICPSR project 151921 (manual) | Pipe-delimited, header | respondent_id + agency_code |

All four eras land in the same `lar_panel` VIEW (`union_by_name=true`). Harmonized
computed columns bridge categorical code differences across all era boundaries. The
stack is Python + DuckDB (no pandas). All data lives under `C:\empirical-data-construction`.

### ICPSR Sub-Era Boundary (2004 HMDA Reform)

Within the ICPSR era there are two sub-eras with different column counts:
- **2004-2006**: 38 columns -- added ethnicity, race_2-5, preapproval, property_type,
  rate_spread, hoepa_status, lien_status after the 2004 HMDA reform
- **2000-2003**: 23 columns -- only race_1, no ethnicity, no lien/hoepa/preapproval

A single `_build_select_exprs_icpsr()` builder handles both sub-eras generically:
columns absent from 2000-2003 are simply not in the source and get NULL-filled.

---

## Two-Table Architecture

### Table 1: LAR Loan-Level Data (`lar_panel` VIEW over Parquet files)
- One row per mortgage application record (matching the raw file row count exactly)
- Contains all application-level fields from the HMDA LAR
- **No lender variables** -- keeps the row count clean and the table lean

### Table 2: Lender Crosswalk (`avery_crosswalk` in DuckDB)
- One row per lender per year
- From Philly Fed HMDA Lender File (covers 1990-present)
  - `hmda-1990-2017.xlsx` -> respondent_id/agency_code -> RSSD (2000-2017 covered)
  - `hmda-2018-present.xlsx` -> LEI -> RSSD (2018-2024 covered)
- Join key: `lei + year` (2018+) or `respondent_id + agency_code + year` (pre-2018)

---

## Scope & Simplifications

- **2018-2024:** Pipe-delimited, header=true, 99 cols, no renames, no scaling.
- **2017 (FFIEC):** Pipe-delimited, **no header** -> supply `COLUMNS_2017` list (45 names).
  Column renames, loan_amount x1000, census_tract FIPS construction applied at ETL.
- **2007-2016 (CFPB):** Comma-delimited, header=true, ~78 cols (45 kept).
  Same transforms as 2017. **Labels file** -- categorical values are text labels
  (e.g., "Refinancing") instead of codes (e.g., "3"). ETL CASE expressions handle both.
- **2000-2006 (ICPSR):** Pipe-delimited, header=true, 38 cols (2004-2006) or 23 cols (2000-2003).
  Pure numeric codes (no label columns). Same loan_amount x1000 and census_tract FIPS
  construction as 2017. ZIPs use Deflate64; extract with `unzip` from Git (not Python
  `zipfile` or PowerShell).
- **`loan_amount` scaling pre-2018:** Source stores values in $000s -> ETL multiplies x1000.
- **`income` never scaled:** $000s in all years (CFPB convention unchanged).
- **Census tract construction pre-2018:** 7-char XXXX.XX + state + county -> 11-char FIPS.
- **Harmonized VIEW columns:** Six `*_harmonized` columns bridge code differences across
  the 2017/2018 boundary. They also apply correctly to 2000-2016 (same code set as 2017).
- **`application_date_indicator`** (45th field in 2007-2016 CFPB files): dropped at ETL.
- **`edit_status`, `sequence_number`** (ICPSR files): dropped at ETL.
- **No ETL-time lender join:** Join avery_crosswalk at query time.

---

## Files

```
empirical-data-construction/
+-- config.py
+-- utils/
|   +-- logging_utils.py
|   +-- duckdb_utils.py
+-- hmda/
    +-- metadata.py
    +-- download.py
    +-- avery.py
    +-- arid_xref.py
    +-- construct.py
    +-- inspect.py
    +-- plan.md
    +-- README.md
    +-- MEMORY.md
```

**Data layout (`C:\empirical-data-construction\hmda\`):**
```
raw/{year}/                        <- downloaded ZIP + extracted file (delete after staging)
staging/year={year}/data.parquet   <- loan-level Parquet (hive-partitioned)
avery/                             <- Philly Fed XLSX files
hmda.duckdb                        <- DuckDB: lar_panel VIEW + avery_crosswalk + metadata
```

---

## Disk Space Management (CRITICAL for 8GB machine)

Raw files are 1-12 GB each. Process one year at a time:

```bash
python -m hmda.download --year 2016
python -m hmda.construct --year 2016
# verify, then:
rm -rf "C:\empirical-data-construction\hmda\raw\2016"
```

ICPSR files (2000-2006) are manually placed in `raw\`. Extract and process:
```bash
"C:\Program Files\Git\usr\bin\unzip.exe" -o HMDA_LAR_2006.zip -d raw/2006/
python -m hmda.construct --year 2006
rm -rf "C:\empirical-data-construction\hmda\raw\2006"  # keep ZIP, delete extracted TXT
```

---

## 1. Download URLs

| Year | URL | Notes |
|------|-----|-------|
| 2018-2024 | `https://files.ffiec.cfpb.gov/static-data/snapshot/{year}/{year}_public_lar_pipe.zip` | pipe, header |
| **2017** | `https://files.ffiec.cfpb.gov/static-data/snapshot/2017/2017_public_lar_txt.zip` | pipe, **no header** |
| **2007-2016** | `https://files.consumerfinance.gov/hmda-historic-loan-data/hmda_{year}_nationwide_all-records_labels.zip` | comma, header, labels |
| **2000-2006** | Manual: https://www.openicpsr.org/openicpsr/project/151921/version/V1/view | pipe, header, Deflate64 ZIP |

The 2017 CFPB file (`hmda_2017_nationwide_all-records_labels.zip`) exists for validation only --
it has the same 14,285,496 rows as our FFIEC 2017 build.

---

## 2. `config.py` (unchanged)

---

## 3. `hmda/metadata.py`

Key constants and functions:

```python
FIRST_YEAR: Final[int] = 2000
LAST_YEAR:  Final[int] = 2024
ALL_YEARS = list(range(LAST_YEAR, FIRST_YEAR - 1, -1))

CFPB_HISTORIC_FIRST_YEAR: Final[int] = 2007
CFPB_HISTORIC_LAST_YEAR:  Final[int] = 2016

ICPSR_FIRST_YEAR:  Final[int] = 2000
ICPSR_LAST_YEAR:   Final[int] = 2006
ICPSR_REFORM_YEAR: Final[int] = 2004   # 38-col sub-era starts here

def is_icpsr(year: int) -> bool:
    return ICPSR_FIRST_YEAR <= year <= ICPSR_LAST_YEAR

def is_cfpb_historic(year: int) -> bool:
    return CFPB_HISTORIC_FIRST_YEAR <= year <= CFPB_HISTORIC_LAST_YEAR

def get_delimiter(year: int) -> str:
    if year >= 2017 or year <= 2006:
        return "|"
    return ","

def get_source_urls(year: int) -> list[str]:
    if year <= 2006:
        return []   # ICPSR: manual download only
    ...
```

Key additions for the ICPSR era:

```python
COLUMN_RENAMES_ICPSR: Final[dict[str, str]] = {
    "occupancy":              "occupancy_type",
    "msamd":                  "derived_msa_md",
    "applicant_ethnicity":    "applicant_ethnicity_1",
    "co_applicant_ethnicity": "co_applicant_ethnicity_1",
}

COLS_TO_DROP_ICPSR: Final[set[str]] = {
    "edit_status",
    "sequence_number",
}
```

---

## 4. `hmda/download.py`

No changes needed for ICPSR years (manual download). `get_source_urls()` returns `[]`
for years <= 2006 so the download module gracefully skips them. All other infrastructure
(resume, idempotency manifest, ZIP extraction) works unchanged for 2007-2024.

**Note on Deflate64 ZIP extraction (ICPSR files):**
Python's `zipfile` module raises `NotImplementedError` for compression type 9 (Deflate64).
PowerShell's `Expand-Archive` also fails. Use Git's bundled `unzip`:
```bash
"C:\Program Files\Git\usr\bin\unzip.exe" -o HMDA_LAR_{year}.zip -d raw/{year}/
```

---

## 5. `hmda/avery.py`

The Philly Fed `hmda-1990-2017.xlsx` covers 2000-2017. No changes needed.

---

## 6. `hmda/construct.py`

### Builder dispatch

```python
def _build_select_exprs(csv_cols: list[str], year: int) -> list[str]:
    if is_icpsr(year):
        return _build_select_exprs_icpsr(csv_cols)
    if is_cfpb_historic(year):
        return _build_select_exprs_cfpb_historic(csv_cols)
    if year == 2017:
        return _build_select_exprs_2017(csv_cols)
    return _build_select_exprs_post2018(csv_cols)
```

### ETL pipeline -- ICPSR (2000-2006)

```
find_raw_file(year)                -> pipe-delimited CSV path (unzip'd from Deflate64 ZIP)
    |
_get_csv_columns(path, year)       -> list of column names from CSV header
    |
_build_select_exprs_icpsr(csv_cols)
  - Map raw column names via COLUMN_RENAMES_ICPSR
  - Drop COLS_TO_DROP_ICPSR (edit_status, sequence_number)
  - loan_amount: scale x1000 (source in $000s for all ICPSR years)
  - census_tract: construct 11-char FIPS from state + county + raw tract
  - NULL-fill: all MASTER_SCHEMA columns not present in source
  - Appended extras: respondent_id, agency_code, property_type (NULL for 2000-2003)
  - 2000-2003 sub-era: ethnicity, race_2-5, lien_status, hoepa_status, rate_spread
    naturally NULL (absent from source, NULL-filled)
    |
COPY (
  SELECT {master_schema_cols}, {year}::INTEGER AS year
  FROM read_csv(path, sep='|', header=true, all_varchar=true, ignore_errors=true)
) TO 'staging/year={year}/data.parquet.tmp'
    |
atomic rename -> data.parquet
    |
upsert panel_metadata row
    |
recreate lar_panel VIEW over all staging Parquets
```

### Key implementation notes for ICPSR builder

- Pure numeric codes -- no CASE expressions for label-to-code conversion needed
- `loan_amount` column name in ICPSR files is `loan_amount` (not `loan_amount_000s`),
  but values ARE in $000s -> must still scale x1000
- Census tract format identical to 2007-2017: 7-char `XXXX.XX` -> 11-char FIPS
- `CENSUS_TRACT_SQL_2017` and `LOAN_AMOUNT_SCALE_SQL_2017` constants are reused
  (the name is historical; the logic is era-agnostic)

---

## 7. DuckDB Schema

The `lar_panel` VIEW already applies `union_by_name=true` and the six harmonized columns.
Adding ICPSR years 2000-2006 requires only adding their Parquet paths to the VIEW.

The harmonized columns work correctly for 2000-2006:
- `loan_purpose_harmonized`: 2000-2016 code '3' stays '3' (no change needed)
- `purchaser_type_harmonized`: 2000-2016 code '7' stays '7'
- `denial_reason_N_harmonized`: maps '0' -> '10' (2000-2016 use '0' for N/A)
- `preapproval_harmonized`: maps '3' -> '2' (2000-2016 use '3' for N/A; NULL for 2000-2003)

---

## 8. Analysis-Time RSSD Join

| Era | LAR identifier | Join to avery_crosswalk on |
|-----|---------------|---------------------------|
| 2018-2024 | `lei` | `lei + year` |
| 2000-2017 | `respondent_id` + `agency_code` | `respondent_id + agency_code + year` |

```sql
-- Universal join (all years)
SELECT l.*, av.rssd_id, av.parent_rssd, av.top_holder_rssd, av.respondent_name, av.assets
FROM lar_panel AS l
LEFT JOIN avery_crosswalk AS av
    ON av.activity_year = l.year
    AND CASE
        WHEN l.lei IS NOT NULL AND l.lei NOT IN ('', 'NA', 'Exempt')
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

-- 2. loan_amount range -- all years should be whole dollars ($1K-$10B+)
SELECT year, MIN(TRY_CAST(loan_amount AS DOUBLE)), MAX(TRY_CAST(loan_amount AS DOUBLE))
FROM lar_panel WHERE year < 2018 GROUP BY year ORDER BY year;

-- 3. census_tract length -- all pre-2018 rows should be 11 chars (or 'NA')
SELECT year, LENGTH(census_tract) AS len, COUNT(*) AS n
FROM lar_panel WHERE year BETWEEN 2000 AND 2017 AND census_tract != 'NA'
GROUP BY year, len ORDER BY year, len;

-- 4. loan_purpose codes are consistent across years
SELECT year, loan_purpose, COUNT(*) AS n
FROM lar_panel WHERE year BETWEEN 2000 AND 2019
GROUP BY year, loan_purpose ORDER BY year, loan_purpose;
-- Expect: 2000-2017 use codes 1,2,3; 2018-2019 use 1,2,31,32,4,5

-- 5. ICPSR sub-era: check that 2000-2003 have NULL lien_status/ethnicity
SELECT year,
    SUM(CASE WHEN lien_status IS NULL THEN 1 ELSE 0 END)           AS null_lien,
    SUM(CASE WHEN applicant_ethnicity_1 IS NULL THEN 1 ELSE 0 END) AS null_eth,
    COUNT(*) AS total
FROM lar_panel WHERE year BETWEEN 2000 AND 2005
GROUP BY year ORDER BY year;
-- Expect: 2000-2003 -> all null; 2004-2005 -> mostly non-null

-- 6. RSSD linkage for 2000-2016
SELECT year,
    COUNT(*) AS total,
    SUM(CASE WHEN av.rssd_id IS NOT NULL THEN 1 END) AS matched,
    ROUND(100.0 * SUM(CASE WHEN av.rssd_id IS NOT NULL THEN 1 END) / COUNT(*), 2) AS pct
FROM lar_panel AS l
LEFT JOIN avery_crosswalk AS av
    ON l.respondent_id = av.respondent_id
    AND TRY_CAST(l.agency_code AS INTEGER) = av.agency_code
    AND av.activity_year = l.year
WHERE l.year BETWEEN 2000 AND 2016
GROUP BY year ORDER BY year;
```

**Expected row counts (all 25 years validated):**

| Year | Rows | Source |
|------|------|--------|
| 2024 | 12,229,298 | FFIEC snapshot |
| 2023 | 11,483,889 | FFIEC snapshot (snapshot lag) |
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

---

## 10. Execution Order

```bash
# Step 0: Lender crosswalk (run once; covers 1990-2024)
python -m hmda.avery
python -m hmda.arid_xref

# Step 1: 2018-2024 (automated download + construct)
for year in 2024 2023 2022 2021 2020 2019 2018; do
    python -m hmda.download --year $year
    python -m hmda.construct --year $year
    python -m hmda.inspect --year $year
done

# Step 2: 2017 FFIEC
python -m hmda.download --year 2017
python -m hmda.construct --year 2017
python -m hmda.inspect --year 2017

# Step 3: 2007-2016 CFPB historic (comma-delimited, labels)
for year in 2016 2015 2014 2013 2012 2011 2010 2009 2008 2007; do
    python -m hmda.download --year $year
    python -m hmda.construct --year $year
    python -m hmda.inspect --year $year
    # rm -rf "C:\empirical-data-construction\hmda\raw\$year"
done

# Step 4: 2000-2006 ICPSR (manual ZIPs, Deflate64)
# Manually place HMDA_LAR_{year}.zip files in C:\empirical-data-construction\hmda\raw\
# Extract each with Git's unzip (Python zipfile and PowerShell do NOT support Deflate64):
for year in 2006 2005 2004 2003 2002 2001 2000; do
    "C:\Program Files\Git\usr\bin\unzip.exe" -o "raw/HMDA_LAR_${year}.zip" -d "raw/${year}/"
    python -m hmda.construct --year $year
    python -m hmda.inspect --year $year
    # rm -rf "C:\empirical-data-construction\hmda\raw\$year"  # delete extracted TXT
done
```

---

## 11. Column Mapping Reference

### 11.1 ICPSR Column Renames (raw -> master schema)

| ICPSR Raw Column | Master Schema Column | Transform |
|-----------------|---------------------|-----------|
| `as_of_year` | `activity_year` | rename |
| `respondent_id` | `respondent_id` | pass through (pre-2018 extra) |
| `agency_code` | `agency_code` | pass through (pre-2018 extra) |
| `loan_type` | `loan_type` | pass through (numeric code) |
| `property_type` | `property_type` | pass through (2004-2006 only) |
| `loan_purpose` | `loan_purpose` | pass through (numeric code) |
| `occupancy` | `occupancy_type` | rename |
| `loan_amount` | `loan_amount` | x1000 scaling (source in $000s) |
| `preapproval` | `preapproval` | pass through (2004-2006 only) |
| `action_taken` | `action_taken` | pass through |
| `msamd` | `derived_msa_md` | rename |
| `state_code` | `state_code` | pass through |
| `county_code` | `county_code` | pass through |
| `census_tract_number` | `census_tract` | 11-char FIPS construction |
| `applicant_ethnicity` | `applicant_ethnicity_1` | rename (2004-2006 only) |
| `co_applicant_ethnicity` | `co_applicant_ethnicity_1` | rename (2004-2006 only) |
| `applicant_race_1`-`_5` | same | pass through (race_2-5 only 2004-2006) |
| `co_applicant_race_1`-`_5` | same | pass through |
| `applicant_sex` | same | pass through |
| `co_applicant_sex` | same | pass through |
| `applicant_income_000s` | `income` | rename (keep in $000s) |
| `purchaser_type` | same | pass through |
| `denial_reason_1`-`_3` | same | pass through |
| `rate_spread` | same | pass through (2004-2006 only) |
| `hoepa_status` | same | pass through (2004-2006 only) |
| `lien_status` | same | pass through (2004-2006 only) |
| `edit_status` | -- | DROP |
| `sequence_number` | -- | DROP |
| `population` | `tract_population` | rename |
| `minority_population` | `tract_minority_population_percent` | rename |
| `hud_median_family_income` | `ffiec_msa_md_median_family_income` | rename |
| `tract_to_msamd_income` | `tract_to_msa_income_percentage` | rename |
| `number_of_owner_occupied_units` | `tract_owner_occupied_units` | rename |
| `number_of_1_to_4_family_units` | `tract_one_to_four_family_homes` | rename |

**99 POST_2018_ONLY_COLS + any missing pre-2018 cols** -> NULL in ICPSR Parquets.

### 11.2 CFPB Historic Column Renames (2007-2016)

| CFPB CSV Column | Master Schema Column |
|----------------|---------------------|
| `as_of_year` | `activity_year` |
| `owner_occupancy` | `occupancy_type` |
| `loan_amount_000s` | `loan_amount` (x1000) |
| `msa_md` / `msamd` | `derived_msa_md` |
| `census_tract_number` | `census_tract` (11-char FIPS) |
| `applicant_ethnicity` | `applicant_ethnicity_1` |
| `co_applicant_ethnicity` | `co_applicant_ethnicity_1` |
| `applicant_income_000s` | `income` |
| `minority_population` | `tract_minority_population_percent` |
| `hud_median_family_income` | `ffiec_msa_md_median_family_income` |
| `tract_to_msamd_income` | `tract_to_msa_income_percentage` |
| `number_of_1_to_4_family_units` | `tract_one_to_four_family_homes` |

### 11.3 2017 FFIEC Column Renames

| 2017 Raw Column | Master Schema Column |
|----------------|---------------------|
| `as_of_year` | `activity_year` |
| `owner_occupancy` | `occupancy_type` |
| `loan_amount_000s` | `loan_amount` (x1000) |
| `msa_md` | `derived_msa_md` |
| `census_tract` | `census_tract` (11-char FIPS) |
| `applicant_ethnicity` | `applicant_ethnicity_1` |
| `co_applicant_ethnicity` | `co_applicant_ethnicity_1` |
| `applicant_income_000s` | `income` |
| `tract_one_to_four_family_housing_units` | `tract_one_to_four_family_homes` |

---

## 12. Files Modified (ICPSR 2000-2006 Extension)

| File | Change |
|------|--------|
| `hmda/metadata.py` | FIRST_YEAR->2000; ICPSR_FIRST/LAST/REFORM_YEAR; is_icpsr(); get_delimiter() updated; COLUMN_RENAMES_ICPSR; COLS_TO_DROP_ICPSR; get_source_urls() returns [] for <=2006 |
| `hmda/construct.py` | _build_select_exprs_icpsr(); dispatch updated (ICPSR checked first); module docstring updated |
| `hmda/inspect.py` | EXPECTED_ROWS for 2000-2006 |
| `hmda/plan.md` | This document |
| `hmda/README.md` | Coverage updated to 2000-2024; ICPSR era table; era-specific column availability; row count table |
| `hmda/MEMORY.md` | Four-era architecture; ICPSR sub-era details; Deflate64 note; updated row counts |

---

## 13. Known Issues & Workarounds

### Deflate64 ZIP Compression (ICPSR files)
ICPSR ZIPs use Deflate64 (compression type 9), unsupported by:
- Python `zipfile` module (`NotImplementedError: That compression method is not supported`)
- PowerShell `Expand-Archive`
- `pip install zipfile-deflate64` fails without MSVC build tools

**Workaround**: Use `unzip.exe` bundled with Git for Windows:
```bash
"C:\Program Files\Git\usr\bin\unzip.exe" -o HMDA_LAR_{year}.zip -d raw/{year}/
```

### Python Heredoc Quoting on Windows
Using `python -c "..."` with embedded SQL single quotes causes `SyntaxError`.
**Workaround**: Use heredoc pattern `python - << 'PYEOF' ... PYEOF` in bash sessions.

### Windows Console Encoding (cp1252)
Unicode characters like checkmarks cause `UnicodeEncodeError` on Windows cp1252 terminal.
**Workaround**: Use ASCII-only strings in all print/logging output.
