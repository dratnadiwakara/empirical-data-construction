# HMDA Pipeline — Memory & Reference

## What Was Built

A full ETL pipeline for the HMDA (Home Mortgage Disclosure Act) public LAR dataset, 2018–2024. Raw pipe-delimited ZIP files are downloaded from FFIEC, joined with the Avery/Philadelphia Fed lender crosswalk, and written to Parquet. All years are queryable via a DuckDB `lar_panel` VIEW.

---

## Data Sources

| Source | URL | Purpose |
|--------|-----|---------|
| FFIEC Snapshot LAR | `https://files.ffiec.cfpb.gov/static-data/snapshot/{year}/{year}_public_lar_pipe.zip` | Raw LAR data, 2018–2024 |
| Philadelphia Fed Avery | `https://www.philadelphiafed.org/-/media/FRBP/Assets/Surveys-And-Data/hmda/hmda-2018-present.xlsx` | LEI → RSSD crosswalk (post-2018) |
| Philadelphia Fed Avery | `https://www.philadelphiafed.org/-/media/FRBP/Assets/Surveys-And-Data/hmda/hmda-1990-2017.xlsx` | Respondent ID → RSSD crosswalk (pre-2018) |
| FFIEC ARID→LEI | `https://files.ffiec.cfpb.gov/static-data/snapshot/2017/arid2017tolei/arid2017_to_lei_xref_psv.zip` | Maps pre-2018 ARID to post-2018 LEI |
| CFPB Data Browser API | `https://ffiec.cfpb.gov/v2/data-browser-api/view/nationwide/aggregations` | Official application counts for validation |

---

## File Layout

```
C:\empirical-data-construction\hmda\
├── hmda.duckdb                     # Master DuckDB (avery_crosswalk, panel_metadata, lar_panel VIEW)
├── raw\
│   └── {year}\                     # Downloaded ZIP + extracted TXT (can delete after staging)
├── staging\
│   └── year={year}\data.parquet    # Hive-partitioned Parquet (snappy compressed)
└── avery\                          # Avery XLSX files
```

---

## Execution Order

```bash
python -m hmda.avery                        # Load Avery RSSD crosswalk into DuckDB
python -m hmda.arid_xref                    # Load ARID2017->LEI crosswalk into DuckDB
python -m hmda.download --year 2024         # Download one year
python -m hmda.construct --year 2024        # Stage to Parquet + rebuild VIEW
# ... repeat download + construct for each year
# After each year: delete the raw TXT to free disk space before downloading the next year
```

Or for all years at once (after individual validation):
```bash
python -m hmda.download --all
python -m hmda.construct --all
```

---

## CRITICAL BUG FIXED: Avery JOIN must include activity_year

**Bug**: The original `construct.py` joined `avery_crosswalk` on `lei` only:
```sql
LEFT JOIN main_db.avery_crosswalk AS av ON lar."lei" = av.lei
```
The Avery crosswalk has **one row per lender per year** (e.g., a lender active 2018–2024 has 7 rows). Joining on LEI alone fans each LAR record out to all matching Avery years, inflating row counts by ~6-7×.

**Fix** (already applied in `construct.py`):
```sql
LEFT JOIN main_db.avery_crosswalk AS av ON lar."lei" = av.lei AND av.activity_year = {year}
```

**How to verify**: After building, the Parquet row count should match the raw file line count minus 1 (the header). Cross-check against the CFPB Data Browser API — the ratio of snapshot rows to API application counts should be ~1×, not ~7×.

CFPB API validation query:
```
GET https://ffiec.cfpb.gov/v2/data-browser-api/view/nationwide/aggregations
    ?years={year}&actions_taken=1,2,3,4,5,6,7,8&aggregation_fields=action_taken
```

Expected raw file row counts (from CFPB API — these are true application counts, which equal raw snapshot rows minus 1 header):

| Year | CFPB API (true apps) | Expected raw rows |
|------|----------------------|-------------------|
| 2018 | 15,138,510 | ~15,138,510 |
| 2019 | 17,573,963 | ~17,573,963 |
| 2020 | 25,699,043 | ~25,699,043 |
| 2021 | 26,269,980 | ~26,269,980 |
| 2022 | 16,099,307 | ~16,099,307 |
| 2023 | 11,564,178 | ~11,564,178 |
| 2024 | 12,229,298 | ~12,229,298 |

Note: The CFPB "snapshot" public LAR applies ~7× privacy replication for small lenders at the **record level within the file** — so the raw file itself already contains replicated rows. The API counts above reflect the true underlying applications. The raw snapshot file row counts should approximately match these API numbers (the replication is baked into the file we download). See "Privacy Replication" section below for details.

---

## Disk Space Management

**The C: drive filled up during the first build attempt.** Raw files are large:
- Each year's extracted TXT: 2–11 GB
- Delete the TXT (and ZIP) immediately after `construct` succeeds for that year
- Do not download/extract multiple years simultaneously

**Recommended workflow** (process one year at a time):
```bash
python -m hmda.download --year 2024
python -m hmda.construct --year 2024
rm -rf C:\empirical-data-construction\hmda\raw\2024\   # free space before next year
python -m hmda.download --year 2023
# ...
```

The `download.py` script already deletes the ZIP after extraction if `--delete-raw` flag is used. Consider using that flag.

---

## Critical: Privacy Replication in the Snapshot Public LAR

The CFPB **snapshot** public LAR is **not** a 1-record-per-application file. For privacy protection, small lenders have each application group replicated **~7 times** before publication. A lender with 1 application appears 7 times with identical fields.

**Evidence**: A lender with LEI `549300D0TGZMG03GNM36` has exactly 7 rows in the 2024 snapshot, all identical (same action_taken, loan_type, loan_amount, census_tract, race, sex).

**Implications for research**:
- `COUNT(*)` overcounts applications for small lenders
- There is **no unique loan identifier** in the public snapshot (removed deliberately)
- For application counts, use the CFPB Data Browser API
- Large lenders are **not** replicated (enough volume for anonymity)
- The 2018 snapshot used a smaller replication factor than later years

---

## Avery Crosswalk Structure

The `avery_crosswalk` table has **one row per lender per year** (not one row per lender). Columns:

| Column | Type | Notes |
|--------|------|-------|
| activity_year | INTEGER | Year of the Avery record |
| lei | VARCHAR | LEI (post-2018 lenders only; NULL for pre-2018) |
| respondent_id | VARCHAR | Pre-2018 HMDA respondent ID (also present for some post-2018) |
| agency_code | INTEGER | Federal agency code (OCC=1, Fed=2, FDIC=3, OTS=4, NCUA=5, HUD=7) |
| rssd_id | INTEGER | Fed Reserve institution ID |
| parent_rssd | INTEGER | RSSD of parent |
| top_holder_rssd | INTEGER | RSSD of top holder |
| respondent_name | VARCHAR | |
| assets | BIGINT | |

Post-2018 section: 34,721 rows, 6,490 unique LEIs (lenders span multiple years).
Pre-2018 section: ~230,000 rows (no LEI column).

**Total loaded**: 264,721 rows.

## Analysis-Time Join Pattern

The join key depends on the era:

| Era | LAR has | Join on |
|-----|---------|---------|
| 2018–2024 | `lei` | `lei + year` |
| Pre-2018 (future) | `respondent_id`, `agency_code` | `respondent_id + agency_code + year` |

**Universal join (handles both eras in one query):**
```sql
LEFT JOIN avery_crosswalk AS av
    ON av.activity_year = l.year
    AND CASE
        WHEN l.lei IS NOT NULL AND l.lei != ''
            THEN l.lei = av.lei
        ELSE
            l.respondent_id = av.respondent_id
            AND TRY_CAST(l.agency_code AS INTEGER) = av.agency_code
    END
```

**Post-2018 only (simpler):**
```sql
LEFT JOIN avery_crosswalk AS av ON l.lei = av.lei AND av.activity_year = l.year
```

**Cross-era institution linkage** (same bank, pre- and post-2018):
```sql
-- arid2017_to_lei maps pre-2018 respondent_id → post-2018 LEI
JOIN arid2017_to_lei AS x ON l.respondent_id = x.arid
```

---

## Known Data Quirks

| Year | Issue | Notes |
|------|-------|-------|
| 2018 | `loan_to_value_ratio` in CSV, `combined_loan_to_value_ratio` in schema | Pipeline NULL-fills `combined_loan_to_value_ratio` and drops `loan_to_value_ratio` |
| 2020 | Min loan_amount = -$1.86B | Negative loan amounts in raw file; known CFPB data quality issue |
| 2019 | Min loan_amount = -$1.4B | Same issue |

---

## DuckDB Schema

```sql
-- Tables in hmda.duckdb
avery_crosswalk      -- one row per lender per year (264,721 rows total)
arid2017_to_lei      -- pre-2018 ARID -> LEI (5,399 rows)
panel_metadata       -- one row per year: row_count, avery_match_rate, built_at, parquet_path, etc.

-- View
lar_panel            -- UNION ALL over all staging/year=*/data.parquet files (union_by_name=true)
```

---

## Column Schema

Columns come from `MASTER_SCHEMA` in `hmda/metadata.py` (96 columns, all VARCHAR from CSV) plus:

- `year` — INTEGER, added by the pipeline
- `rssd_id`, `parent_rssd`, `top_holder_rssd` — VARCHAR, joined from `avery_crosswalk`

All columns stored as VARCHAR. Cast to numeric at query time as needed.

---

## Python Environment

- Virtual environment: `.venv` in the repo root
- Key packages: `duckdb`, `polars`, `httpx`, `fastexcel`, `openpyxl`
- Run scripts as modules from the repo root: `python -m hmda.construct --year 2024`
- Windows note: logging must use ASCII characters only (no em-dash, arrows, etc.) — cp1252 encoding on the console
