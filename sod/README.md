# FDIC Summary of Deposits (SOD)

Branch-level deposit data for all FDIC-insured institutions. Annual survey conducted each June 30. Covers 1994–present.

**Source**: FDIC BankFind API — `https://banks.data.fdic.gov/api/sod`  
**Scale**: ~75,000–85,000 branch records per year; ~2.6M rows total (1994–2026)  
**DuckDB**: `C:\empirical-data-construction\sod\sod.duckdb`  
**View**: `sod` (hive-partitioned by year)

---

## Quick Start

```python
import duckdb
conn = duckdb.connect(r"C:\empirical-data-construction\sod\sod.duckdb", read_only=True)

# Total deposits by year (in $billions)
conn.execute("""
    SELECT YEAR, COUNT(*) AS branches, SUM(DEPSUMBR) / 1e6 AS deposits_bil
    FROM sod
    GROUP BY YEAR ORDER BY YEAR
""").df()

# Branches for a specific institution by RSSD ID
conn.execute("SELECT * FROM sod WHERE RSSDID = 1119794 AND YEAR = 2023").df()

# County-level deposit concentration for 2023
conn.execute("""
    SELECT STCNTYBR, CNTYNAMB, STALP,
           COUNT(*) AS branches,
           SUM(DEPSUMBR) / 1e3 AS deposits_mil
    FROM sod WHERE YEAR = 2023
    GROUP BY STCNTYBR, CNTYNAMB, STALP
    ORDER BY deposits_mil DESC
""").df()
```

---

## Schema

All columns stored as-fetched from the FDIC API. Numeric columns cast to BIGINT; all others VARCHAR.

| Column | Type | Description |
|--------|------|-------------|
| `UNINUMBR` | BIGINT | Unique FDIC branch identifier. Stable across years — use for branch-level panel joins. |
| `YEAR` | BIGINT | Survey year (June 30 call date). |
| `CERT` | BIGINT | FDIC certificate number. Institution-level identifier. |
| `RSSDID` | BIGINT | Federal Reserve RSSD ID of the branch's institution. **Key join field** to NIC, HMDA, CRA, Call Reports. |
| `RSSDHCR` | BIGINT | RSSD ID of the top-tier holding company. |
| `NAMEFULL` | VARCHAR | Full legal name of the insured institution. |
| `NAMEBR` | VARCHAR | Branch office name. |
| `ADDRESBR` | VARCHAR | Branch street address. |
| `CITYBR` | VARCHAR | Branch city. |
| `STALP` | VARCHAR | Two-character state abbreviation. |
| `STNAME` | VARCHAR | Full state name. |
| `ZIPBR` | VARCHAR | Branch ZIP code. |
| `STCNTYBR` | VARCHAR | State+county FIPS — **missing leading zero** in API (e.g. `"6037"` not `"06037"`). Use `LPAD(STCNTYBR, 5, '0')` before joining HMDA/CRA. CT uses 9xxx planning region codes (no Census FIPS match). |
| `CNTYNAMB` | VARCHAR | County name. |
| `ASSET` | BIGINT | Total assets of the institution in **$thousands**. Multiply by 1000 for dollars. |
| `DEPSUMBR` | BIGINT | Deposits at this branch in **$thousands**. Multiply by 1000 for dollars. |
| `BRNUM` | BIGINT | Branch sequence number within institution. `0` = main office. |
| `BRSERTYP` | BIGINT | Branch service type code (see below). |
| `CHRTAGNT` | VARCHAR | Charter agent: `STATE`, `OCC`, `OTS`, `NCUA`. |
| `ESTYMD` | VARCHAR | Branch establishment date (`YYYY-MM-DD` string). |
| `NAMEHCR` | VARCHAR | Name of the top-tier holding company. |

### BRSERTYP codes
| Code | Description |
|------|-------------|
| 11 | Full service brick and mortar |
| 12 | Full service retail location |
| 13 | Full service cyber office |
| 21 | Limited service drive-through |
| 22 | Limited service mobile/seasonal |
| 23 | Limited service military |
| 24 | Limited service facility |
| 25 | Limited service loan production |
| 26 | Limited service consumer credit |
| 27 | Limited service internet |
| 28 | Limited service trust |
| 29 | Limited service administration |
| 30 | Limited service military (alternate) |

---

## Audit Table

```sql
SELECT * FROM panel_metadata ORDER BY year;
-- year | row_count | source_url | built_at | parquet_path
```

---

## Common Research Patterns

### Build institution-year deposit panel

```python
conn.execute("""
    SELECT YEAR, RSSDID, NAMEFULL,
           SUM(DEPSUMBR) AS total_deposits_k,    -- multiply by 1000 for $
           COUNT(*) AS branch_count,
           MAX(ASSET) AS total_assets_k
    FROM sod
    GROUP BY YEAR, RSSDID, NAMEFULL
    ORDER BY YEAR, total_deposits_k DESC
""").df()
```

### Join to NIC for holding company structure

```python
# SOD.RSSDID joins to NIC relationships.ID_RSSD_OFFSPRING
# SOD.RSSDHCR joins to NIC relationships.ID_RSSD_PARENT (top-tier HC)
conn.execute("""
    SELECT s.YEAR, s.RSSDID, s.NAMEFULL, s.RSSDHCR,
           SUM(s.DEPSUMBR) AS deposits_k
    FROM sod s
    WHERE s.YEAR = 2023
    GROUP BY s.YEAR, s.RSSDID, s.NAMEFULL, s.RSSDHCR
""").df()
```

### Join to HMDA by county

```python
# SOD.STCNTYBR = HMDA.county_fips (both are 5-char FIPS strings)
# Example: bank deposit market share in HMDA loan origination counties
conn.execute("""
    SELECT h.county_fips, h.year,
           COUNT(*) AS loan_count,
           SUM(s.DEPSUMBR) AS county_deposits_k
    FROM hmda_panel h
    JOIN sod s
      ON s.STCNTYBR = h.county_fips
     AND s.YEAR = h.year
    WHERE h.year = 2022
    GROUP BY h.county_fips, h.year
""").df()
```

### Market concentration (HHI) by county-year

```python
conn.execute("""
    WITH county_totals AS (
        SELECT YEAR, STCNTYBR, SUM(DEPSUMBR) AS county_dep
        FROM sod GROUP BY YEAR, STCNTYBR
    ),
    inst_shares AS (
        SELECT s.YEAR, s.STCNTYBR, s.RSSDID,
               SUM(s.DEPSUMBR) * 1.0 / ct.county_dep AS share
        FROM sod s
        JOIN county_totals ct USING (YEAR, STCNTYBR)
        GROUP BY s.YEAR, s.STCNTYBR, s.RSSDID, ct.county_dep
    )
    SELECT YEAR, STCNTYBR, SUM(share * share) AS HHI
    FROM inst_shares
    GROUP BY YEAR, STCNTYBR
    ORDER BY YEAR, HHI DESC
""").df()
```

---

## Pipeline

```bash
# Step 1: Download one year
C:\envs\.basic_venv\Scripts\python.exe -m sod.download --year 2024

# Download all years (1994-present)
C:\envs\.basic_venv\Scripts\python.exe -m sod.download --all

# Force re-download (ignores manifest)
C:\envs\.basic_venv\Scripts\python.exe -m sod.download --year 2024 --force

# Step 2: Construct Parquet + DuckDB view
C:\envs\.basic_venv\Scripts\python.exe -m sod.construct --year 2024
C:\envs\.basic_venv\Scripts\python.exe -m sod.construct --all
```

---

## Data Storage Layout

```
C:\empirical-data-construction\sod\
  sod.duckdb                        # DuckDB: sod view + panel_metadata table
  download_manifest.json            # Idempotency: total_rows per year
  raw\
    {year}\
      sod_{year}.csv                # Raw API data, all fields, UTF-8
  staging\
    year={year}\
      data.parquet                  # Snappy Parquet, typed columns
```

---

## Branch Openings

Use `sod/branch_openings.py` to identify when each branch first appears in the data (= branch opening event).

```python
import duckdb
from sod.branch_openings import get_branch_openings, summarize_openings

conn = duckdb.connect(r"C:\empirical-data-construction\sod\sod.duckdb", read_only=True)

# All branch opening events (OTS-adjusted by default)
openings = get_branch_openings(conn, start_year=2000, end_year=2024).df()

# Institution-year opening counts
summary = summarize_openings(conn).df()
```

Or from CLI:
```bash
# Branch-level list 2000-2024
python -m sod.branch_openings --start 2000 --end 2024

# Institution-year summary
python -m sod.branch_openings --summary

# Skip OTS adjustment (raw data)
python -m sod.branch_openings --no-ots-adjust
```

### OTS-to-OCC Regulatory Transfer Adjustment (2011)

**Problem**: The Office of Thrift Supervision (OTS) was abolished under Dodd-Frank (effective July 21, 2011). ~6,200 OTS-chartered thrift branches transferred to OCC supervision and appear in SOD for the first time in 2011 — not because they opened, but because OTS institutions were previously outside the SOD reporting universe. Without adjustment, 2011 shows 4,965 apparent "new branches" vs. a normal trend of ~2,000/year.

**Adjustment** (mirrors Dratnadiwakara et al.): Exclude any RSSDID that has new-UNINUMBR activity in 2011 but **zero** new UNINUMBRs in 2004–2010. These institutions had no organic branch expansion in the pre-transfer window — their 2011 "openings" are purely regulatory artifacts.

**Effect**:

| Year | Raw openings | Adjusted | Removed |
|------|-------------|----------|---------|
| 2010 | 2,011 | 2,011 | 0 |
| **2011** | **4,965** | **2,132** | **2,833** |
| 2012 | 1,548 | 1,463 | 85 |

516 RSSDs are excluded as OTS transfers. Post-adjustment, 2011 falls back in line with the surrounding trend.

**Note**: The excluded RSSDs are removed entirely from the openings dataset. If an OTS institution genuinely opened branches in 2012+, those are also excluded since the RSSDID filter is institution-level. For institution-level analysis after 2011, consider adding those RSSDs back manually if needed.

---

## Notes for Agents

- **Unit**: `DEPSUMBR` and `ASSET` are in **$thousands**. Always multiply by 1000 for dollar values.
- **Branch vs. institution**: SOD is branch-level. Aggregate on `RSSDID` (or `CERT`) for institution-level panel.
- **Main office**: `BRNUM = 0` identifies the main office branch.
- **RSSDID join key**: Use `RSSDID` to join to NIC (entity structure), HMDA (lender), and Call Reports (financials). `CERT` joins to FDIC Call Reports and other FDIC datasets.
- **STCNTYBR needs LPAD for joins**: The FDIC API omits leading zeros — California's LA County is stored as `"6037"` not `"06037"`. Always normalize before joining: `LPAD(STCNTYBR, 5, '0')`. Connecticut uses 9xxx codes (planning region based) which won't match Census FIPS.
- **Annual cadence**: Survey date is June 30 each year. Deposits represent balances at that point in time, not year-end.
- **Missing RSSDID**: Some branches (esp. credit unions and thrifts) may have NULL `RSSDID`. Use `CERT` for those.
- **Schema stable**: Single schema since 1994 — no era complexity, no field renaming across years.
