# RateWatch — Deposit Rate Panel

Weekly deposit-product interest rates set by US bank/credit-union ratesetting branches, sourced from S&P Global Market Intelligence (RateWatch / former MonitorBankRates).

**Source files** (local extract, not redistributable):
- 2001–2020: `D:\RateWatch_PS_full\RW_DepositDataFeedMASTER\depositRateData_clean_{YYYY}.txt`
- 2021:      `DepositRateData2021.txt` inside `D:\RateWatch_PS_full\RW_DepositDataFeedMASTER.zip`
- 2022–2024: `D:\RateWatch_LSU\DepositRateData{YYYY}.txt` (full-year coverage; supersedes PS_full 2022/2023 partial extracts)
- Support: `D:\RateWatch_PS_full\RW_DepositDataFeedMASTER\Deposit_InstitutionDetails.txt`, `Deposit_acct_join.txt`, `DepositCertChgHist.txt`, `DepositsNameChgHist.txt`

## Quick Start

```python
import duckdb
from config import get_ratewatch_duckdb_path

db = duckdb.connect(str(get_ratewatch_duckdb_path()), read_only=True)

# Average APY per product per week, 2021
db.execute("""
    SELECT week_date, prd_typ_join, AVG(apy) AS mean_apy, COUNT(*) AS n
    FROM ratewatch
    WHERE year = 2021
    GROUP BY 1, 2
    ORDER BY 1, 2
""").fetchdf()
```

## Grain

One row per `(week_date, account_number, prd_typ_join)` at the **dominant balance tier** for that product (e.g. 12-month $10K CD, $25K MM, $2,500 SAV).

`account_number` = ratesetting branch code (2-letter state + 5-digit). To get rates for branches *covered by* a ratesetter, use `ratewatch_branch_fanout` view.

## Schema

| Column | Type | Notes |
|--------|------|-------|
| year | INTEGER | survey year |
| week_date | DATE | from `DATESURVEYED`; surveys posted Mon–Thu, ~4 dates/week |
| account_number | VARCHAR | ratesetting branch code |
| prd_typ_join | VARCHAR | product code (CD, SAV, MM, INTCK, FIRA, VIRA, PREMMM, …) |
| productdescription | VARCHAR | tier code, e.g. `12MCD10K` |
| producttype | VARCHAR | broad category |
| prod_nm | VARCHAR | human-readable product name |
| promo | VARCHAR | promotional flag (Y/N) |
| amount | DOUBLE | balance threshold tier ($, e.g. 10000.0). Equals raw `MINTOEARN` (era A) or `AMOUNT` (era B). |
| maxtoearn | DOUBLE | upper tier ceiling — era A only; NULL for 2021+ |
| termlength | DOUBLE | term length |
| termtype | VARCHAR | term unit (`M` = months, NULL for non-term) |
| rate | DOUBLE | nominal rate, **percent** (5.0 = 5%) |
| apy | DOUBLE | Annual Percentage Yield, **percent** |
| cmt | VARCHAR | rate-index name |
| rssd_id | BIGINT | Fed RSSD identifier (joined from institution details) |
| uninumbr | BIGINT | RateWatch unique institution number |
| cert_nbr | BIGINT | FDIC certificate |
| inst_nm, inst_typ, inst_state, inst_zip, inst_cnty_fps, inst_state_fps, msa, cbsa | VARCHAR/DOUBLE | branch-level attributes |

**Units:** all rates and APYs are **percent** (matching raw RateWatch convention). The prior R-built reference at `C:\Users\dimut\OneDrive\research-data\RateWatch` uses the same convention.

## Source-era differences

Two source-file schemas:

| Era | Years | Key columns |
|-----|-------|-------------|
| A   | 2001–2020 | 14 cols incl. `MINTOEARN`, `MAXTOEARN` |
| B   | 2021–2023 | 13 cols with single `AMOUNT` (no MAXTOEARN) |

Pipeline harmonizes both into the unified `amount` column. `maxtoearn` is NULL for era B.

## Tier retention rule

Tier identity is the `(PRD_TYP_JOIN, PRODUCTDESCRIPTION)` pair. A pair is retained for a year if EITHER:

1. **Mandatory** — the `PRODUCTDESCRIPTION` appears in `MANDATORY_TIER_DESCRIPTIONS` for that product (kept regardless of coverage):

   ```python
   MANDATORY_TIER_DESCRIPTIONS = {
       "CD":    ["12MCD10K"],
       "MM":    ["MM10K", "MM25K"],
       "SAV":   ["SAV2.5K"],
       "INTCK": ["INTCK2.5K"],
   }
   ```

2. **Coverage** — distinct ratesetters offering the tier ≥ `MIN_TIER_SHARE` (default 10%) of all ratesetters in the year.

Multiple tiers per product allowed (e.g. CD often retains `12MCD10K`, `06MCD10K`, `12MCD100K`, `24MCD10K`, etc.). Mandatory tiers guarantee continuous time-series for canonical research products even when bank-specific tiers fall below threshold.

Selected pairs persist in `ratewatch/product_registry.json`. Rerun `python -m ratewatch.profile --year YYYY` to refresh.

Era A (2001–2020) raw files are ~98 GB total. Pipeline reads them directly from `D:\RateWatch_PS_full\RW_DepositDataFeedMASTER\` rather than copying to local staging — saves ~100 GB. If `raw/{year}/...` is missing, construct.py and profile.py automatically fall back to the source path.

### 2021 kept products (sample, multi-tier)

| Product | Tiers retained |
|---------|----------------|
| CD     | 12MCD10K, 12MCD100K, 06MCD10K, 24MCD10K, 06MCD100K, 24MCD100K, 36MCD10K, 36MCD100K, 60MCD10K, 60MCD100K, 48MCD10K, 48MCD100K, 18MCD10K, 18MCD100K, 03MCD10K, 03MCD100K, 30MCD10K, 30MCD100K, 09MCD100K, 09MCD10K, 01MCD100K, 01MCD10K |
| SAV    | SAV2.5K |
| MM     | MM10K, MM25K, MM100K, MM50K, MM250K, MM2.5K |
| INTCK  | INTCK2.5K, INTCK0K |
| FIRA   | 12MFIRA10K |
| VIRA   | VARIRA 10K, VARIRA0K |
| PREMMM | PREMMM10K, PREMMM25K, PREMMM50K, PREMMM100K, PREMMM250K |

For full per-year tier listing see `ratewatch/product_registry.json`.

## Pipeline

```bash
source C:/envs/.basic_venv/Scripts/activate

# 1. Stage raw text from D:\ -> C:\empirical-data-construction\ratewatch\raw\
python -m ratewatch.download --year 2021      # one year
python -m ratewatch.download --all            # all years

# 2. Profile products (writes product_registry.json)
python -m ratewatch.profile --year 2021

# 3. Construct: raw -> Parquet -> DuckDB views
python -m ratewatch.construct --year 2021

# 4. Validate
python -m ratewatch.inspect --year 2021
```

CLI flags: `--force` rebuilds even when manifest current.

## Storage layout

```
C:\empirical-data-construction\ratewatch\
  ratewatch.duckdb                              # views + panel_metadata
  download_manifest.json                        # idempotency tracking
  raw\
    {year}\depositRateData_clean_{year}.txt   # era A
    {year}\DepositRateData{year}.txt          # era B
    support\Deposit_InstitutionDetails.txt
    support\Deposit_acct_join.txt
    support\DepositCertChgHist.txt
    support\DepositsNameChgHist.txt
  staging\
    year={year}\data.parquet                  # one snappy Parquet per year
    support\institution_details.parquet
    support\acct_join.parquet
```

## DuckDB views

| View | Purpose |
|------|---------|
| `ratewatch` | Main panel — 1 row per (week, ratesetter, product) at dominant tier |
| `ratewatch_institutions` | Branch master from `Deposit_InstitutionDetails` (RSSD/UNINUMBR/address/MSA) |
| `ratewatch_acct_join` | Ratesetter→branch fanout map |
| `ratewatch_branch_fanout` | Convenience: rates broadcast to every covered branch (`branch_account` = covered branch, plus all rate columns) |
| `panel_metadata` | Per-year row count, branch count, product count, build timestamp |

## Example queries

```sql
-- 1. Rate dispersion across banks for 12-month CD
SELECT week_date,
       AVG(apy)    AS mean_apy,
       MEDIAN(apy) AS median_apy,
       MAX(apy) - MIN(apy) AS spread
FROM ratewatch
WHERE year = 2021 AND prd_typ_join = 'CD'
GROUP BY 1
ORDER BY 1;

-- 2. Bank-level (RSSD) average rate across all CDs in Q4 2021
SELECT rssd_id, inst_nm,
       AVG(apy) FILTER (WHERE prd_typ_join = 'CD')  AS cd_apy,
       AVG(apy) FILTER (WHERE prd_typ_join = 'MM')  AS mm_apy,
       AVG(apy) FILTER (WHERE prd_typ_join = 'SAV') AS sav_apy
FROM ratewatch
WHERE year = 2021
  AND week_date BETWEEN '2021-10-01' AND '2021-12-31'
  AND rssd_id IS NOT NULL
GROUP BY 1, 2
HAVING COUNT(*) > 5
ORDER BY cd_apy DESC NULLS LAST
LIMIT 20;

-- 3. Join to SOD: deposit-weighted average CD rate by state
SELECT s.stalpbr AS state,
       SUM(s.depsumbr * r.apy) / NULLIF(SUM(s.depsumbr), 0) AS dep_wgt_cd_apy
FROM read_parquet('C:/empirical-data-construction/sod/staging/year=2021/data.parquet') s
JOIN ratewatch r
  ON s.rssdid = r.rssd_id
WHERE r.year = 2021 AND r.prd_typ_join = 'CD'
GROUP BY 1
ORDER BY 2 DESC;

-- 4. Fanout: every branch with its applicable CD rate (Jan 2021)
SELECT branch_account, week_date, apy
FROM ratewatch_branch_fanout
WHERE year = 2021 AND prd_typ_join = 'CD'
  AND week_date BETWEEN '2021-01-01' AND '2021-01-31';
```

## Joins to other datasets

| Target | Key | Notes |
|--------|-----|-------|
| SOD    | `rssd_id` ↔ `rssdid` | bank-level deposit panel |
| HMDA   | `rssd_id` ↔ Avery RSSD | bank-level mortgage activity |
| NIC    | `rssd_id` ↔ `rssd_id` | parent/holding-company structure |
| CRA    | via crosswalk | requires bank crosswalks |
| Call Reports (FFIEC/CFLV) | `rssd_id` ↔ `rssd9001` | balance sheet |

## Validation summary

Full panel covers 2001–2024 (24 years). Key columns 0% NULL across every year. RSSD_ID join 99%+ in every year.

| Year | Rows | Branches | Products | Dates | Range |
|------|------|----------|----------|-------|-------|
| 2001 | 3,202,272 | 13,738 |  8 | 209 | 2001-01-01 → 2001-12-31 |
| 2002 | 2,919,561 | 13,396 |  8 | 219 | 2001-12-31 → 2002-12-31 |
| 2003 | 2,915,139 | 12,375 |  8 | 273 | 2002-12-30 → 2003-12-31 |
| 2004 | 2,066,620 | 12,117 |  8 | 265 | 2004-01-01 → 2004-12-31 |
| 2005 | 1,838,791 | 12,107 |  8 | 260 | 2005-01-03 → 2005-12-30 |
| 2006 | 1,684,607 | 11,706 |  8 | 260 | 2006-01-02 → 2006-12-29 |
| 2007 | 1,025,268 |  9,942 |  8 | 250 | 2007-01-01 → 2007-12-31 |
| 2008 |   747,865 |  9,780 |  8 | 267 | 2008-01-01 → 2008-12-31 |
| 2009 |   825,905 | 10,833 | 11 | 271 | 2009-01-01 → 2009-12-31 |
| 2010 |   867,651 | 10,771 | 11 | 278 | 2010-01-01 → 2010-12-31 |
| 2011 |   931,619 | 10,238 | 13 | 276 | 2011-01-03 → 2011-12-30 |
| 2012 |   916,936 |  9,926 | 13 | 284 | 2012-01-02 → 2012-12-31 |
| 2013 |   870,903 |  9,494 | 13 | 291 | 2012-12-31 → 2013-12-29 |
| 2014 |   810,982 |  8,946 | 13 | 270 | 2014-01-01 → 2014-12-26 |
| 2015 |   791,817 |  8,947 | 13 | 273 | 2014-12-29 → 2015-12-31 |
| 2016 |   746,667 |  8,753 | 13 | 273 | 2016-01-01 → 2016-12-31 |
| 2017 |   703,559 |  8,548 | 12 | 266 | 2017-01-02 → 2017-12-29 |
| 2018 |   708,918 |  8,515 | 12 | 272 | 2018-01-01 → 2018-12-31 |
| 2019 |   680,828 |  8,199 | 12 | 272 | 2019-01-01 → 2020-01-03 |
| **2020** |   **262,178** |  **7,663** | **12** | **109** | **2020-01-06 → 2020-05-29** (vendor cutoff) |
| 2021 | 2,379,242 |  9,326 |  7 | 196 | 2020-12-28 → 2021-12-02 |
| 2022 | 1,485,717 |  8,268 |  7 | 196 | 2021-11-29 → 2022-12-01 |
| 2023 | 2,478,383 |  8,003 |  7 | 212 | 2022-11-28 → 2023-11-30 |
| **2024** | **1,184,086** |  **7,983** |  **7** |  **92** | **2023-11-27 → 2024-05-02** (vendor cutoff) |

Parity vs prior reference RDS (2021–2024 only): median APY diff = 0.000 across all years; ≥99.2% of (qtr, branch, product) triples join. Differences arise because the prior reference averaged APY across **all** balance tiers while this build keeps only the dominant tier per product/year.

**Coverage anomalies:**
- 2007–2008: branch count drops sharply (~12K → ~10K) — RateWatch panel attrition during financial crisis
- 2008–2010: dominant CD tier shifts to `06MCD10K` (6-month) reflecting crisis-era short-duration CD focus
- 2020: source file truncates at 2020-05-29 (vendor extract cutoff); ~5 months of weekly data only
- 2024: source file truncates at 2024-05-02 (vendor cutoff)
- 2008–2010 picked up `BUSMM`, `PREMIC`, `BUSSAV` after coverage threshold met; 2011+ added `BSCD`, `BUSIC`; 2017+ dropped `BSCD`/`PREMIC` below threshold

The remaining 30% with larger differences arise because the prior R-built reference averaged APY across **all** balance tiers per (branch, product, quarter), while this build keeps only the dominant tier — different domains over which we average.

## Known anomalies / notes

- **Survey cadence is sub-weekly.** S&P RateWatch posts surveys on multiple weekdays per institution; expect 3–5 dates per week per branch, not exactly one.
- **Products vary by year.** Newer products (e.g. `E-INTCK`, `E-SAV`) appear later. Re-run `profile.py` per year — the registry stores year-specific selections.
- **RSSD coverage incomplete for credit unions and minor entities.** Approx 0.3% of rate observations lack an RSSD link (institution-detail row missing).
- **Rates are percent, not decimal.** A 1.05% APY is stored as `1.05`, not `0.0105`.
- **`amount` semantics:** for non-term products this is the minimum balance to earn the rate; for CDs it is the deposit amount tier.
