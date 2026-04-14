# NIC Pipeline -- Reference & Query Guide

## Overview

This module constructs an FFIEC National Information Center (NIC) DuckDB database from two snapshot ZIP files:

- `CSV_RELATIONSHIPS.ZIP`
- `CSV_TRANSFORMATIONS.ZIP`

Output database:

- `C:\empirical-data-construction\nic\nic.duckdb`

Output views:

- `relationships`
- `transformations`

## Key Difference from HMDA/CRA

NIC is not annual. FFIEC publishes periodic snapshot replacements, so updates are detected by file version metadata and SHA-256 fingerprints, not by year.

## File Layout

```
C:\empirical-data-construction\nic\
├── nic.duckdb
├── download_manifest.json
├── raw\
│   ├── CSV_RELATIONSHIPS.ZIP
│   └── CSV_TRANSFORMATIONS.ZIP
└── staging\
    ├── relationships\data.parquet
    └── transformations\data.parquet
```

## Commands

Download latest changed files only:

```bash
python -m nic.download --update
```

Download all files regardless of manifest state:

```bash
python -m nic.download --force
```

Construct both datasets:

```bash
python -m nic.construct
```

Force rebuild both staging files:

```bash
python -m nic.construct --force
```

Single dataset:

```bash
python -m nic.download --dataset relationships --update
python -m nic.construct --dataset relationships
```

## Connecting to DuckDB

```python
import duckdb

conn = duckdb.connect(r"C:\empirical-data-construction\nic\nic.duckdb", read_only=True)
```

## Core Identifiers

Relationships:

- `ID_RSSD_PARENT`
- `ID_RSSD_OFFSPRING`
- `D_DT_START`
- `D_DT_END`

Transformations:

- `ID_RSSD_PREDECESSOR`
- `ID_RSSD_SUCCESSOR`
- `D_DT_TRANS`

Date fields in NIC source files are string dates in `MM/DD/YY` format (for example, `04/14/26`), so year logic should parse date strings before aggregation.

## Example Queries

Active parent-child links:

```sql
SELECT
    ID_RSSD_PARENT,
    ID_RSSD_OFFSPRING,
    D_DT_START,
    D_DT_END
FROM relationships
WHERE D_DT_END IS NULL OR D_DT_END = ''
LIMIT 100;
```

Transformation history for one predecessor:

```sql
SELECT
    ID_RSSD_PREDECESSOR,
    ID_RSSD_SUCCESSOR,
    D_DT_TRANS
FROM transformations
WHERE ID_RSSD_PREDECESSOR = 123456
ORDER BY D_DT_TRANS;
```

Recent transformation counts by year:

```sql
SELECT
    EXTRACT(YEAR FROM STRPTIME(D_DT_TRANS, '%m/%d/%y')) AS year,
    COUNT(*) AS events
FROM transformations
WHERE D_DT_TRANS IS NOT NULL
  AND TRIM(D_DT_TRANS) <> ''
GROUP BY 1
ORDER BY 1 DESC;
```

## Metadata Table

`panel_metadata` in `nic.duckdb` tracks:

- `dataset`
- `row_count`
- `file_sha256`
- `source_url`
- `built_at`
- `parquet_path`
