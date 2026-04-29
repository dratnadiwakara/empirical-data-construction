# NIC Pipeline — Agent Reference

## What This Is

FFIEC National Information Center (NIC) data: ownership relationships between US bank holding companies, banks, and other regulated entities, plus structural change events (mergers, acquisitions, failures). Two snapshot datasets, version-based (not annual).

**Source**: `https://www.ffiec.gov` — full population of regulated entities, no sampling.

---

## Data Locations

All data lives under `C:\empirical-data-construction\nic\` (controlled by `FIN_DATA_ROOT` env var; see `config.py`).

```
C:\empirical-data-construction\nic\
├── nic.duckdb                          ← query this
├── download_manifest.json              ← idempotency tracking (sha256, etag, size)
├── raw\
│   ├── CSV_RELATIONSHIPS.ZIP
│   └── CSV_TRANSFORMATIONS.ZIP
└── staging\
    ├── relationships\data.parquet      ← intermediate; DuckDB views point here
    └── transformations\data.parquet
```

**Connect to DuckDB:**

```python
import duckdb
conn = duckdb.connect(r"C:\empirical-data-construction\nic\nic.duckdb", read_only=True)
```

---

## Available Views

### `relationships`

Parent-offspring ownership links between entities. One row = one directed ownership relationship.

Key columns (always present, ordered first):

| Column | Type | Description |
|---|---|---|
| `ID_RSSD_PARENT` | BIGINT | RSSD ID of parent/controlling entity |
| `ID_RSSD_OFFSPRING` | BIGINT | RSSD ID of subsidiary/controlled entity |
| `D_DT_START` | VARCHAR | Relationship start date (`MM/DD/YY` string) |
| `D_DT_END` | VARCHAR | Relationship end date; NULL or blank = still active |

Active relationships: `D_DT_END IS NULL OR TRIM(D_DT_END) = ''`

### `transformations`

Structural change events: mergers, acquisitions, failures, conversions. One row = one event.

Key columns (always present, ordered first):

| Column | Type | Description |
|---|---|---|
| `ID_RSSD_PREDECESSOR` | BIGINT | RSSD ID of the entity that ceased to exist |
| `ID_RSSD_SUCCESSOR` | BIGINT | RSSD ID of the surviving/acquiring entity |
| `D_DT_TRANS` | VARCHAR | Transformation date (`MM/DD/YY` string) |

---

## Schema Details

**Date format**: All date fields are VARCHAR strings in `MM/DD/YY` format (e.g., `04/14/26` = April 14, 2026). Parse with DuckDB's `STRPTIME(col, '%m/%d/%y')` before year extraction or comparisons.

**RSSD IDs**: Cast to BIGINT via `TRY_CAST`; malformed values become NULL. These are the Fed's unique institution identifiers — use them to join with HMDA (`avery.py` crosswalk), Call Reports, SOD, and permco-rssd-link datasets.

**Blank values**: All fields use `NULLIF(TRIM(val), '')` — empty strings are stored as NULL.

**Column order in Parquet**: ID columns first, then date columns, then remaining columns sorted alphabetically.

**Other ID columns** that may appear in source data:
- `ID_RSSD` — entity's own RSSD ID
- `ID_RSSD_HD_OFF` — RSSD of head office

---

## Commands

Run from repo root using the project venv (`C:\envs\.basic_venv`).

```bash
# Download only changed files (check remote etag/content-length/last-modified)
python -m nic.download --update

# Force re-download regardless of manifest
python -m nic.download --force

# Build/rebuild staging Parquet and DuckDB views
python -m nic.construct

# Force rebuild staging even if Parquet already exists
python -m nic.construct --force

# Single dataset only
python -m nic.download --dataset relationships --update
python -m nic.construct --dataset relationships
```

---

## Update Behavior

NIC is a snapshot dataset — FFIEC replaces the full file, no annual cadence.

- `download_manifest.json` tracks `sha256`, `etag`, `content_length`, `last_modified` per dataset.
- `--update`: issues HEAD request → compares etag/content-length/last-modified → skips if unchanged.
- `--force`: bypasses all manifest checks, always downloads.
- `construct.py`: skips Parquet rebuild if `data.parquet` exists unless `--force`.
- Views are always recreated at end of `construct` run (idempotent).

---

## `panel_metadata` Table

Query for dataset provenance:

```sql
SELECT * FROM panel_metadata;
```

| Column | Description |
|---|---|
| `dataset` | `"relationships"` or `"transformations"` |
| `row_count` | Row count at last build |
| `file_sha256` | SHA-256 of source ZIP at download time |
| `source_url` | FFIEC download URL |
| `built_at` | UTC ISO timestamp of last construct run |
| `parquet_path` | Absolute path to staging Parquet file |

---

## Example Queries

**Check what's built:**
```sql
SELECT dataset, row_count, built_at FROM panel_metadata;
```

**Active parent-child links:**
```sql
SELECT ID_RSSD_PARENT, ID_RSSD_OFFSPRING, D_DT_START
FROM relationships
WHERE D_DT_END IS NULL OR TRIM(D_DT_END) = ''
LIMIT 100;
```

**All subsidiaries of a given parent (current):**
```sql
SELECT ID_RSSD_OFFSPRING, D_DT_START
FROM relationships
WHERE ID_RSSD_PARENT = 1039502
  AND (D_DT_END IS NULL OR TRIM(D_DT_END) = '');
```

**Transformation history for one entity:**
```sql
SELECT ID_RSSD_PREDECESSOR, ID_RSSD_SUCCESSOR, D_DT_TRANS
FROM transformations
WHERE ID_RSSD_PREDECESSOR = 123456
ORDER BY D_DT_TRANS;
```

**Transformation event counts by year:**
```sql
SELECT
    EXTRACT(YEAR FROM STRPTIME(D_DT_TRANS, '%m/%d/%y')) AS year,
    COUNT(*) AS events
FROM transformations
WHERE D_DT_TRANS IS NOT NULL AND TRIM(D_DT_TRANS) <> ''
GROUP BY 1
ORDER BY 1 DESC;
```

**Full relationship history for an entity (as parent or offspring):**
```sql
SELECT *
FROM relationships
WHERE ID_RSSD_PARENT = 1039502 OR ID_RSSD_OFFSPRING = 1039502
ORDER BY D_DT_START;
```

---

## Joining to Other Datasets

RSSD IDs (`ID_RSSD_PARENT`, `ID_RSSD_OFFSPRING`, `ID_RSSD_PREDECESSOR`, `ID_RSSD_SUCCESSOR`) are the Fed's institution identifiers. They link to:

| Dataset | Join key | DuckDB path |
|---|---|---|
| HMDA (via Avery) | `rssd_id` in `avery` view | `C:\empirical-data-construction\hmda\hmda.duckdb` |
| Call Reports FFIEC | `RSSD9001` | `C:\empirical-data-construction\call-reports-FFIEC\call-reports-ffiec.duckdb` |
| Call Reports CFLV | `rssd` | `C:\empirical-data-construction\call-reports-CFLV\call-reports-cflv.duckdb` |
| PERMCO-RSSD link | `rssd` | `C:\empirical-data-construction\permco-rssd-link\permco-rssd-link.duckdb` |
| SOD | `RSSDID` | `C:\empirical-data-construction\sod\sod.duckdb` |

---

## Source Files

| Dataset | Source ZIP | FFIEC URL path |
|---|---|---|
| relationships | `CSV_RELATIONSHIPS.ZIP` | `/npw/FinancialReport/ReturnRelationshipsZipFileCSV` |
| transformations | `CSV_TRANSFORMATIONS.ZIP` | `/npw/FinancialReport/ReturnTransformationZipFileCSV` |

ZIPs contain one or more CSVs; `construct.py` unions all CSVs found inside.
