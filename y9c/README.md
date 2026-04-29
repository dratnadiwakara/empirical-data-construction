# Y-9C — Bank Holding Company Quarterly Financial Statements

Consolidated quarterly financial filings (FR Y-9C) by US Bank Holding Companies, harmonized into a CFLV/FFIEC-compatible panel. Use this to study top-of-house balance sheets, capital, and earnings.

**Source:** [FFIEC NIC FinancialDataDownload](https://www.ffiec.gov/npw/FinancialReport/FinancialDataDownload?selectedyear=2024) — one quarterly bulk ZIP per period (`BHCF{YYYYMMDD}.ZIP`), single caret-delimited TXT inside.

**Coverage:** 2000-Q1 → 2025-Q4 (~104 quarters, ~350-1,800 Y-9C filers per quarter).

---

## Quick Start

```python
import duckdb, sys
from pathlib import Path
sys.path.insert(0, r"C:\Users\dimut\OneDrive\github\empirical-data-construction")
from config import get_y9c_duckdb_path, DUCKDB_THREADS, DUCKDB_MEMORY_LIMIT

con = duckdb.connect(str(get_y9c_duckdb_path()), read_only=True)
con.execute(f"PRAGMA threads={DUCKDB_THREADS}")
con.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY_LIMIT}'")

# Top BHCs by assets, 2024 Q4
con.execute("""
    SELECT id_rssd, ROUND(assets/1e6, 1) AS assets_bn,
           ROUND(deposits/1e6, 1) AS deposits_bn
    FROM bs_panel_y9c
    WHERE activity_year = 2024 AND activity_quarter = 4
    ORDER BY assets DESC NULLS LAST LIMIT 10
""").df()
```

Hard-coded path: `C:\empirical-data-construction\y9c\y9c.duckdb`.

Always `read_only=True` unless running ETL.

---

## Tables

| Object | Kind | Description |
|--------|------|-------------|
| `y9c_raw` | view | Wide raw layer — every source column (BHCK*, BHDM*, BHFN*, BHCP*, BHSP*, RSSD9001…) as VARCHAR. Use when you need a code not in the harmonized layer. |
| `bs_panel_y9c` | view | Balance-sheet harmonized concepts (assets, deposits, equity, ln_tot, …) — Y-9C filers only |
| `is_panel_y9c` | view | Income-statement harmonized concepts (ytdnetinc, ytdint_inc, …) plus quarterly-flow `q_*` columns. YTD source. |
| `y9c_panel` | view | Convenience: `bs_panel_y9c JOIN is_panel_y9c` on identity columns |
| `harmonized_metadata_y9c` | table | Self-documenting concept catalog (variable_name, panel, desc, MDRM codes, formula) |
| `panel_metadata` | table | Per-quarter row count, n_columns, source_zip, parquet_path, built_at |

---

## Identity columns

| Column | Type | Description |
|--------|------|-------------|
| `id_rssd` | BIGINT | BHC RSSD ID (cast from `RSSD9001`) — join key for `permco-rssd-link.bhc_rssd`, `nic.relationships`, `call-reports-FFIEC.bs_panel.id_rssd` (when same RSSD also files call reports), `call-reports-CFLV.balance_sheets.id_rssd` |
| `activity_year` | INTEGER | 2000..2025 |
| `activity_quarter` | INTEGER | 1, 2, 3, 4 |
| `date` | DATE | Quarter-end date (e.g. 2024-12-31) — matches CFLV/FFIEC convention |
| `rssd9999_raw` | VARCHAR | Raw reporting period date string from source (YYYYMMDD) |

---

## Balance sheet variables (`bs_panel_y9c`)

All monetary values **thousands of USD**. Multiply by 1e3 for dollars; divide by 1e6 for billions, 1e9 for trillions.

| Variable | Description | MDRM code(s) |
|----------|-------------|--------------|
| `assets` | Total consolidated assets | BHCK2170 |
| `deposits` | Total deposits (NIB+IB across domestic+foreign offices) | BHDM6631 + BHDM6636 + BHFN6631 + BHFN6636 |
| `domestic_dep` | Domestic-office deposits (NIB+IB) | BHDM6631 + BHDM6636 |
| `foreign_dep` | Foreign-office deposits (NIB+IB) | BHFN6631 + BHFN6636 |
| `equity` | Total equity (BHCKG105 from 2009Q1, else BHCK3210) | BHCKG105, BHCK3210 |
| `ln_tot` | Total loans and leases, net of unearned income | BHCK2122 |
| `ln_tot_gross` | Total loans, gross | BHCK2122 + BHCK2123 |
| `ln_re` | Real estate loans (HC-C item 1) | BHCK1410 |
| `ln_ci` | Commercial and industrial loans (US + non-US addressees) | BHCK1763 + BHCK1764 |
| `ln_cc` | Credit card loans | BHCKB538 |
| `htmsec_ac` | Held-to-maturity securities (amortized cost) | BHCK1754 |
| `afssec_fv` | Available-for-sale securities (fair value) | BHCK1773 |
| `securities` | Total securities (HTM AC + AFS FV) | BHCK1754 + BHCK1773 |
| `trading_assets` | Trading assets | BHCK3545 |
| `premises` | Premises and fixed assets | BHCK2145 |
| `intangibles` | Intangible assets (incl. goodwill, MSAs) | BHCK2143 |
| `goodwill` | Goodwill | BHCK3163 |
| `other_assets` | Other assets | BHCK2160 |
| `borrowings` | Other borrowed money | BHCK3190 |
| `sub_debt` | Subordinated notes and debentures | BHCK4062 |
| `total_liab` | Total liabilities | BHCK2948 |
| `retained_earnings` | Retained earnings | BHCK3247 |
| `aoci` | Accumulated other comprehensive income | BHCKB530 |
| `ffs` | Federal funds sold | BHCKB987 |
| `reverse_repo` | Reverse repos (resell) | BHCKB989 |
| `ffp` | Federal funds purchased | BHCKB993 |
| `repo` | Repos (sold under repurchase) | BHCKB995 |
| `qtr_avg_assets` | Quarterly average total assets | BHCK3368 |
| `llres` | Allowance for loan and lease losses | BHCK3123 |

---

## Income statement variables (`is_panel_y9c`)

All YTD; quarterly flow derived as `q_<name> = ytd<name> - LAG(ytd<name>) OVER (PARTITION BY id_rssd, activity_year ORDER BY activity_quarter)`.

| YTD variable | Description | MDRM code(s) |
|--------------|-------------|--------------|
| `ytdint_inc` | Total interest income | BHCK4107 |
| `ytdint_exp` | Total interest expense | BHCK4073 |
| `ytdint_inc_net` | Net interest income | BHCK4074 |
| `ytdnonint_inc` | Total noninterest income | BHCK4079 |
| `ytdnonint_exp` | Total noninterest expense | BHCK4093 |
| `ytdllprov` | Provision for loan losses | BHCK4230 |
| `ytdnetinc` | Net income attributable to BHC | BHCK4340 (or BHBC4340 in newer files) |
| `ytdsalaries` | Salaries and benefits | BHCK4135 |
| `ytdinc_taxes` | Applicable income taxes | BHCK4302 |
| `ytdcommdividend` | Cash dividends on common stock | BHCK4460 |
| `ytdprefdividend` | Cash dividends on preferred stock | BHCK4470 |
| `ytdtradrev_inc` | Trading revenue | BHCKA220 |

Each YTD variable has matching `q_*` quarterly-flow column.

---

## Filing scope

The Y-9C bulk file (`BHCF{date}.zip`) **mixes three forms** — Y-9C (consolidated, BHCK prefix), Y-9LP (parent-only, BHCP prefix), Y-9SP (small/savings holding companies, BHSP prefix). The harmonized views filter to **Y-9C filers only** by requiring `BHCK2170 IS NOT NULL`. To query Y-9LP or Y-9SP filers, work directly with `y9c_raw` and filter on `BHCP*` or `BHSP*` column populating.

Y-9C filer count over time tracks regulatory threshold:
- 2000-2014: $500MM threshold → ~1,100-1,800 BHCs
- 2015-2017: $1B threshold → ~640-680 BHCs
- 2018+: $3B threshold → ~350-390 BHCs

---

## Key constraints for analysis

1. **Income-statement values are YTD**. Use `q_*` columns for single-quarter flow, or restrict to Q4 for annual totals.
2. **Annualize quarterly NIM/ROA** by multiplying single-quarter rates by 4.
3. **Y-9C is consolidated** — BHC's bank subsidiaries' balance sheets are *included*. To compare with FFIEC call reports (subsidiary-level), join via NIC (`y9c.id_rssd` = `nic.relationships.ID_RSSD_PARENT`).
4. **Prefix drift**: same MDRM 4-char code can appear under multiple BHC* prefixes across years. Harmonized layer uses BHCK by default; absent codes resolve to NULL via the column-existence guard in `harmonized/views.py`.
5. **Filter `assets > 0`** if you need only active filers — some quarter-rows are placeholder-only.

---

## Common queries

```sql
-- Top 20 BHCs by total assets, 2024 Q4
SELECT id_rssd, assets/1e6 AS assets_bn,
       deposits/1e6 AS deposits_bn,
       equity/1e6 AS equity_bn
FROM bs_panel_y9c
WHERE activity_year = 2024 AND activity_quarter = 4
ORDER BY assets_bn DESC NULLS LAST
LIMIT 20;

-- Single bank time series — JPMorgan Chase BHC (RSSD 1039502)
SELECT date, assets/1e6 AS assets_bn,
       deposits/1e6 AS deposits_bn,
       ln_tot/1e6 AS loans_bn,
       equity/1e6 AS equity_bn
FROM bs_panel_y9c
WHERE id_rssd = 1039502
ORDER BY date;

-- System-wide assets by year (Q4 snapshot)
SELECT activity_year,
       COUNT(*) AS n_bhcs,
       ROUND(SUM(assets)/1e9, 2) AS total_assets_tn
FROM bs_panel_y9c
WHERE activity_quarter = 4
GROUP BY 1 ORDER BY 1;

-- Quarterly net income (use q_netinc directly, no LAG needed)
SELECT id_rssd, activity_year, activity_quarter,
       ytdnetinc, q_netinc
FROM is_panel_y9c
WHERE id_rssd = 1039502 AND activity_year = 2023
ORDER BY activity_quarter;

-- Annualized NIM (quarterly-flow basis)
SELECT b.id_rssd, b.date,
       (i.q_int_inc_net * 4) / NULLIF(b.qtr_avg_assets, 0) AS nim_annualized
FROM bs_panel_y9c b
JOIN is_panel_y9c i USING (id_rssd, date, activity_year, activity_quarter)
WHERE b.qtr_avg_assets > 0
ORDER BY b.date DESC, nim_annualized DESC LIMIT 20;
```

---

## Cross-dataset joins

```sql
-- Public-market firm linkage via PERMCO
ATTACH 'C:\empirical-data-construction\permco-rssd-link\permco-rssd-link.duckdb' AS link (READ_ONLY);

SELECT l.permco, l.bhc_rssd, l.name,
       y.activity_year, y.activity_quarter,
       y.assets/1e6 AS assets_bn
FROM bs_panel_y9c y
JOIN link.crsp_frb_link l
    ON l.bhc_rssd = y.id_rssd
   AND l.quarter_end = y.date
WHERE y.activity_year = 2024 AND y.activity_quarter = 4
ORDER BY y.assets DESC NULLS LAST LIMIT 20;

-- Compare BHC-consolidated vs sum of subsidiary banks
ATTACH 'C:\empirical-data-construction\call-reports-FFIEC\call-reports-ffiec.duckdb' AS ffiec (READ_ONLY);
ATTACH 'C:\empirical-data-construction\nic\nic.duckdb' AS nic (READ_ONLY);

WITH subs AS (
    SELECT r.ID_RSSD_PARENT AS bhc_rssd,
           SUM(b.assets) AS subs_assets
    FROM nic.relationships r
    JOIN ffiec.bs_panel b ON b.id_rssd = r.ID_RSSD_OFFSPRING
    WHERE b.date = DATE '2024-12-31'
      AND r.CTRL_IND = 1
    GROUP BY 1
)
SELECT y.id_rssd, y.assets/1e6 AS bhc_assets_bn,
       s.subs_assets/1e6 AS subs_total_bn,
       (y.assets - s.subs_assets)/1e6 AS bhc_minus_subs_bn
FROM bs_panel_y9c y
LEFT JOIN subs s ON s.bhc_rssd = y.id_rssd
WHERE y.activity_year = 2024 AND y.activity_quarter = 4
ORDER BY y.assets DESC NULLS LAST LIMIT 20;

-- BHC structural changes (M&A, acquisitions)
SELECT y.id_rssd, y.date, y.assets/1e6 AS assets_bn,
       t.TRNSFM_DT, t.TRNSFM_TYP_DESC
FROM bs_panel_y9c y
JOIN nic.transformations t ON t.ID_RSSD_PREDECESSOR = y.id_rssd
WHERE y.activity_year >= 2020;
```

---

## Storage layout

```
C:\empirical-data-construction\y9c\
  y9c.duckdb                        # views + panel_metadata + harmonized_metadata_y9c
  download_manifest.json            # per-quarter file metadata + extract status
  raw\
    BHCF{YYYYMMDD}.ZIP              # user-placed bulk ZIPs
    {YYYY}Q{Q}\BHCF{YYYYMMDD}.txt   # extracted (caret-delimited TXT)
  staging\
    year={YYYY}\quarter={Q}\data.parquet  # Hive-partitioned snappy Parquet
```

---

## Pipeline

```bash
source C:/envs/.basic_venv/Scripts/activate

# Drop new ZIPs into raw/, then:
python -m y9c.download --scan        # register new quarters in manifest
python -m y9c.download --status      # inventory + coverage gaps

# Build:
python y9c/construct.py --quarter 2024Q4   # one quarter
python y9c/construct.py --year 2024        # one year
python y9c/construct.py --all              # everything in raw/
python y9c/construct.py --refresh-views    # only rebuild views (parquets already on disk)

# Optional flags:
#   --force         rebuild parquet even if present
#   --skip-views    skip view refresh during bulk load
#   --inspect       run validate.py at end of build

# Validation:
python y9c/validate.py
```

---

## Validation summary

Per-Q4 system aggregate, recent years:

| Year | Y-9C filers | Total assets ($T) | Y-9C / FFIEC subs ratio |
|------|-------------|-------------------|--------------------------|
| 2017 | 641 | 19.83 | 1.14 |
| 2018 | 373 | 19.93 | 1.11 |
| 2019 | 353 | 20.82 | 1.12 |
| 2020 | 348 | 24.08 | 1.10 |
| 2021 | 351 | 26.00 | 1.10 |
| 2022 | 364 | 25.89 | 1.10 |
| 2023 | 379 | 26.69 | 1.13 |
| 2024 | 382 | 23.17 | 0.96 |
| 2025 | 379 | 29.07 | 1.15 |

BHC consolidated total > FFIEC subsidiary total in every year (ratio 1.10-1.75) — consistent with BHC consolidation including non-bank subs and parent-company assets. 2018 threshold change ($1B → $3B) cuts filer count from ~640 to ~370.

### Known anomalies

- **2024Q4 total of $23.17T** is below 2024Q3 ($27.64T) and 2025Q1 ($28.18T). Y-9C/FFIEC ratio drops to 0.96 (only year < 1.0). Likely a few large BHCs are missing from the 2024Q4 source file or use a non-BHCK prefix that quarter — investigate before relying on 2024Q4 cross-sectional totals. Time-series queries for individual BHCs are unaffected.
- **2026Q1 partial data** (18 filers): preliminary release, will fill in over time.
- **Y-9SP/Y-9LP rows in raw**: the bulk source file contains Y-9SP semi-annual filers in Q2/Q4 (~3,400 rows) and Y-9LP parent-only filers in some quarters. Harmonized views filter these out via `BHCK2170 IS NOT NULL`. To query Y-9SP/Y-9LP, work with `y9c_raw` directly.

---

## Files

| File | Purpose |
|------|---------|
| `download.py` | Manifest scan + status of raw/ |
| `construct.py` | ETL: ZIP → extract → parquet → views |
| `metadata.py` | URL, filename regex, identity cols, panel_metadata DDL |
| `validate.py` | Row-count / null-rate / plausibility checks (renamed from `inspect.py` to avoid stdlib shadow) |
| `harmonized/concepts.py` | BS/IS concept SQL — single source of truth for harmonized variable definitions |
| `harmonized/views.py` | View builder with column-existence guard |
| `__init__.py` | Empty package marker |
