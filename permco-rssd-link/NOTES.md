# NOTES — PERMCO-RSSD Link Pipeline

Build log and operational notes. User-facing docs in `README.md`.

---

## Initial build (2026-04-23)

Dataset module created from scratch, porting the logic in
`C:\Users\dimut\OneDrive\github\np_pd\permco-rssd-link\create_crsp_frb_sqlite.py`
to the DuckDB/Parquet ETL pattern used across this repo.

**Source CSV examined:** `crsp_20240930v2.csv` — 1,495 rows, columns:
`name`, `inst_type`, `entity` (BHC RSSD), `permco`, `dt_start`, `dt_end` (YYYYMMDD integers).

**Existing SQLite DB:** `crsp_frb_link.db` — 97,674 quarterly rows with
`PERMCO`, `BHC_RSSD`, `quarter_end`, `confirmed`, `lead_bank_rssd`, `assets`, `equity`.
This was the validation target for row count and lead bank coverage.

### Design choices

- **No year partitioning**: Dataset is small (~100K rows); single Parquet file is
  simpler and faster for this scale.
- **Full rebuild on new CSV**: The source file may change date ranges for existing
  entities (not just add new ones), so `--force` rebuild is required on update.
- **CFLV for pre-2001, FFIEC for 2001+**: FFIEC CDR starts at 2001-Q1; CFLV covers
  1959-Q4 through current, so pre-2001 quarters use CFLV `balance_sheets`.
- **BFS in Python**: DuckDB recursive CTEs could do transitive closure, but running
  one CTE per (BHC, quarter) pair would be slow at scale. The numpy-array BFS from
  the original script is O(E) per quarter and ~5 min total — acceptable.
- **Fallback logic**: BHC-as-lead-bank fills cases where the BHC is itself a
  commercial bank with no discovered subsidiaries in the call report universe.
- **importlib for metadata**: Folder name `permco-rssd-link` has a hyphen, preventing
  Python module import. `importlib.util.spec_from_file_location` loads `metadata.py`
  by file path.

### NIC relationship columns used

From `nic.duckdb` → `relationships` view:
- `ID_RSSD_PARENT` (BIGINT)
- `ID_RSSD_OFFSPRING` (BIGINT)
- `DT_START` (BIGINT, YYYYMMDD integer — cast from VARCHAR in view)
- `DT_END` (BIGINT, YYYYMMDD integer)
- `CTRL_IND` (VARCHAR — cast to INTEGER, filter to 1 = controlled relationship)

### Call report columns used

CFLV (`call-reports-CFLV/call-reports-cflv.duckdb` → `balance_sheets`):
- `id_rssd` BIGINT
- `date` DATE (quarter-end)
- `assets` DOUBLE (thousands USD)
- `equity` DOUBLE (thousands USD)

FFIEC (`call-reports-FFIEC/call-reports-ffiec.duckdb` → `bs_panel`):
- Same column names/types; filtered to `activity_year >= 2001`

---

## Operational notes

### Raw CSV naming

The script scans `raw/` for any file matching `crsp_*.csv`. The newest by mtime
is used. Rename the downloaded file to include the release date:
`crsp_20240930v2.csv` → `crsp_YYYYMMDD.csv`.

### Manifest

`download_manifest.json` tracks: `filename`, `sha256`, `size_bytes`, `mtime`,
`scanned_at`, `source_url`. `construct.py` reads `sha256` from the manifest for
the `panel_metadata` table; does not recompute SHA at build time.

### Idempotency

`construct.py` (without `--force`) skips rebuild if `staging/crsp_frb_link/data.parquet`
exists. The view is always refreshed even on skip (in case the DuckDB was wiped).
Use `--force` after any new CSV is placed in `raw/`.

### Performance expectations

- Step 1 (CSV → quarterly panel): < 1 s (1,495 rows input)
- Step 2 (BFS, all quarters): ~3–8 min (162 quarters × BHC BFS)
- Step 3 (asset load from CFLV + FFIEC): ~30–60 s
- Step 4 (lead bank join): < 5 s
- Step 5 (Parquet write): < 2 s
- Total: ~5–10 min

### Row count vs existing SQLite

The existing `crsp_frb_link.db` SQLite (in `np_pd/permco-rssd-link/`) has 97,674 rows starting
from 1959-Q4. Our DuckDB has 85,973 rows starting from 1986-Q2. The difference is explained by
`backfill_crsp_frb_sqlite.py` (in that repo), which extends coverage backward to 1959 using CRSP
market cap data — a step beyond the NY Fed CSV. Our pipeline is built strictly from the NY Fed CSV
(`dt_start` minimum = 1986-06-30). The backfill is not replicated here; if needed it can be added
as a separate step. Lead bank values for shared rows match the SQLite.

### Dependency availability

If `nic.duckdb` or the call report databases are unavailable (not yet built),
`construct.py` will warn and still produce the base quarterly panel
(`lead_bank_rssd`, `lead_bank_assets`, `lead_bank_equity` all NULL). Run
the NIC and call report ETLs first for full enrichment.
