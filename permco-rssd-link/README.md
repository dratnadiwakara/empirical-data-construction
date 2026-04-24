# PERMCO-RSSD Link

Quarterly panel linking CRSP PERMCO identifiers to Federal Reserve RSSD identifiers for US Bank Holding Companies, enriched with lead-bank identification.

**Source:** NY Fed CRSP-FRB Link Table — https://www.newyorkfed.org/research/banking_research/crsp-frb  
**Coverage:** 1986-Q2 through current quarter · ~97,000+ quarterly observations · 1,495 unique BHCs  
**Lead bank:** Identified via NIC controlled-subsidiary relationships + CFLV/FFIEC call report assets

---

## Quick Start

```python
import duckdb
import sys
sys.path.insert(0, r"C:\Users\dimut\OneDrive\github\empirical-data-construction")
from config import get_permco_rssd_duckdb_path, DUCKDB_THREADS, DUCKDB_MEMORY_LIMIT

conn = duckdb.connect(str(get_permco_rssd_duckdb_path()), read_only=True)
conn.execute(f"PRAGMA threads={DUCKDB_THREADS}")
conn.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY_LIMIT}'")

# What's in here?
conn.execute("SHOW TABLES").df()

# Metadata
conn.execute("SELECT * FROM panel_metadata").df()

# Sample rows
conn.execute("SELECT * FROM crsp_frb_link LIMIT 10").df()
```

Hard-coded path (if config.py unavailable):
```
C:\empirical-data-construction\permco-rssd-link\permco-rssd-link.duckdb
```

---

## Tables

| Object | Kind | Description |
|--------|------|-------------|
| `crsp_frb_link` | view | Quarterly PERMCO → BHC RSSD → lead bank RSSD panel |
| `panel_metadata` | table | Build metadata (row count, source CSV, SHA-256, built_at) |

---

## Variable Reference

| Column | Type | Description |
|--------|------|-------------|
| `permco` | BIGINT | CRSP PERMCO identifier |
| `bhc_rssd` | BIGINT | BHC RSSD identifier (Federal Reserve) |
| `name` | VARCHAR | Entity name from source CSV |
| `inst_type` | VARCHAR | Institution type (Bank Holding Company, Commercial Bank, etc.) |
| `quarter_end` | DATE | Quarter-end date (e.g. 2024-09-30) |
| `confirmed` | INTEGER | 1 = within original date range from NY Fed; 0 = extrapolated forward |
| `lead_bank_rssd` | BIGINT | RSSD of the largest controlled subsidiary by total assets |
| `lead_bank_assets` | DOUBLE | Lead bank total assets in **thousands of USD** |
| `lead_bank_equity` | DOUBLE | Lead bank total equity in **thousands of USD** |

### `confirmed` flag

The source CSV contains entity-level date ranges (`dt_start` → `dt_end`). The latest `dt_end` is extended to the current quarter to populate forward. Rows within the original `dt_end` are `confirmed=1`; forward-extrapolated rows are `confirmed=0`.

### Lead bank methodology

1. Load NIC controlled relationships (`CTRL_IND=1`), active at the quarter-end
2. BFS from each BHC through the controlled-subsidiary tree → all transitive subsidiaries
3. Join each subsidiary with call report assets (CFLV for quarters ≤ 2000-Q4; FFIEC for 2001-Q1+)
4. Lead bank = subsidiary with maximum total assets in that quarter
5. Fallback: if the BHC itself files call reports and has no identified subsidiary, use BHC as lead bank

---

## Common Queries

```sql
-- Look up a bank by PERMCO
SELECT *
FROM crsp_frb_link
WHERE permco = 21
ORDER BY quarter_end;

-- Confirmed rows only for most recent quarter
SELECT *
FROM crsp_frb_link
WHERE quarter_end = (SELECT MAX(quarter_end) FROM crsp_frb_link)
  AND confirmed = 1
ORDER BY lead_bank_assets DESC NULLS LAST
LIMIT 20;

-- Link to HMDA: mortgage lending by BHC
-- (requires hmda/hmda.duckdb)
-- ATTACH 'C:\empirical-data-construction\hmda\hmda.duckdb' AS hmda (READ_ONLY);
-- SELECT l.permco, l.bhc_rssd, h.loan_amount
-- FROM crsp_frb_link l
-- JOIN hmda.lar h ON h.respondent_id = CAST(l.lead_bank_rssd AS VARCHAR)
--                 AND YEAR(h.as_of_year) = YEAR(l.quarter_end)
-- WHERE l.quarter_end = '2024-09-30';

-- Aggregate: total BHC assets by quarter (lead bank as proxy)
SELECT quarter_end,
       COUNT(*)                        AS n_bhcs,
       SUM(lead_bank_assets) / 1e6     AS total_assets_tn
FROM crsp_frb_link
WHERE confirmed = 1
GROUP BY quarter_end
ORDER BY quarter_end DESC
LIMIT 20;

-- Cross-join with FFIEC call reports
-- ATTACH 'C:\empirical-data-construction\call-reports-FFIEC\call-reports-ffiec.duckdb' AS ffiec (READ_ONLY);
-- SELECT l.permco, l.bhc_rssd, l.quarter_end,
--        bs.assets, bs.deposits, bs.ln_tot
-- FROM crsp_frb_link l
-- JOIN ffiec.bs_panel bs
--     ON bs.id_rssd = l.lead_bank_rssd
--    AND bs.date    = l.quarter_end
-- WHERE l.quarter_end = '2024-09-30'
-- ORDER BY bs.assets DESC NULLS LAST
-- LIMIT 20;
```

---

## Update Workflow

When NY Fed releases a new version of the CRSP-FRB link CSV:

1. Download the new CSV from https://www.newyorkfed.org/research/banking_research/crsp-frb
2. Place in `C:\empirical-data-construction\permco-rssd-link\raw\` (filename: `crsp_YYYYMMDD.csv`)
3. Register and rebuild:
   ```bash
   C:\envs\.basic_venv\Scripts\python.exe permco-rssd-link\download.py
   C:\envs\.basic_venv\Scripts\python.exe permco-rssd-link\construct.py --force
   ```

The `--force` flag is needed because a new CSV may update date ranges for existing entities.

---

## Files

| File | Purpose |
|------|---------|
| `construct.py` | ETL pipeline (CSV → quarterly panel → lead bank → Parquet → DuckDB) |
| `download.py` | Manifest tracking for raw CSV (no auto-download; manual placement) |
| `metadata.py` | Source URL, variable descriptions, panel_metadata DDL |
| `schema.py` | TypedDict `CrspFrbRecord` for the panel record |
| `NOTES.md` | Build log and operational notes |
