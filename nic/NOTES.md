# NIC Pipeline -- Memory & Build Reference

## Purpose

This module builds a DuckDB-backed NIC dataset from FFIEC snapshot ZIP files:

- `CSV_RELATIONSHIPS.ZIP`
- `CSV_TRANSFORMATIONS.ZIP`

Unlike HMDA/CRA, NIC is not annual. The updater is file-version driven.

## Current Architecture

- Raw files stored in `C:\empirical-data-construction\nic\raw\`
- Staging Parquet in `C:\empirical-data-construction\nic\staging\{dataset}\data.parquet`
- Master DB in `C:\empirical-data-construction\nic\nic.duckdb`
- `panel_metadata` table records row counts and source file hash

## Operational Commands

```bash
python -m nic.download --update
python -m nic.construct
```

Force refresh:

```bash
python -m nic.download --force
python -m nic.construct --force
```

## Update Behavior

- `--update` checks remote HEAD metadata (`etag`, `content-length`, `last-modified`) against manifest.
- If changed, downloader refreshes ZIP and updates manifest SHA-256.
- Construct step rebuilds affected Parquet and recreates DuckDB views.

## Notes for Future Maintenance

- If FFIEC introduces schema drift, `nic.construct` normalizes column names to uppercase and reorders by dataset priorities.
- Numeric identifier columns are cast with `TRY_CAST`; malformed IDs become `NULL`.
- If ZIP internals change (nested folders, multiple CSVs), parser unions all CSV files found in the archive.
