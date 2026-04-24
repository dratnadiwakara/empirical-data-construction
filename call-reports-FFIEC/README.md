# FFIEC Call Reports — Query Reference

Primary reference for retrieving data from this dataset (humans and AI agents). Gives everything needed without reading the source. For build log and operational notes see `NOTES.md`.

---

## What this dataset is

Quarterly FFIEC Call Reports (official regulator filings for every US commercial bank), 2001-Q1 through 2025-Q4, at **two query layers**:

1. **Raw MDRM layer** — 46 schedule views, every column is one MDRM item code (e.g. `RCFD2170` = Total Assets on form 031). Use when you know the MDRM code or need fine-grained line items not in the harmonized layer.
2. **Harmonized layer (v2)** — 4 views (`bs_panel`, `is_panel`, `filers_panel`, `call_reports_panel`) with CFLV-style variable names (`assets`, `deposits`, `ln_tot`, `ytdnetinc`) and unified RCFD/RCON resolution across forms 031/041/051. **Use this for most questions.**

~4,400–8,700 filers per quarter, 97 quarters, 46 raw schedules, **105 harmonized concepts** (+37 derived quarterly-flow vars).

**Sibling datasets in this repo** (cross-joinable):
- `call-reports-CFLV/` — same economic data, academic curated (CFLV authors), wider year range (1959+), pre-harmonized variable names. The FFIEC harmonized layer mirrors CFLV names so queries are portable. Join on `id_rssd` ↔ `bs_panel.id_rssd` (both BIGINT).
- `nic/` — FFIEC bank structure snapshots (parent/offspring relationships, transformations/mergers). Join on `ID_RSSD_OFFSPRING` ↔ `bs_panel.id_rssd`.

---

## Exact connection

```python
import duckdb, sys
from pathlib import Path
sys.path.insert(0, r"C:\Users\dimut\OneDrive\github\empirical-data-construction")
from config import get_ffiec_duckdb_path, DUCKDB_THREADS, DUCKDB_MEMORY_LIMIT

conn = duckdb.connect(str(get_ffiec_duckdb_path()), read_only=True)
conn.execute(f"PRAGMA threads={DUCKDB_THREADS}")
conn.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY_LIMIT}'")
```

Hard-coded path (if `config.py` unavailable):
```
C:\empirical-data-construction\call-reports-FFIEC\call-reports-ffiec.duckdb
```

Always `read_only=True` unless you are the ETL.

---

## What's inside the DuckDB

### Harmonized layer (preferred — use this first)

| Object | Kind | Purpose |
|--------|------|---------|
| `bs_panel` | view | Balance-sheet concepts (assets, deposits, equity, ln_tot, …) × quarter. CFLV-compatible. |
| `is_panel` | view | Income-statement concepts (ytdint_inc, ytdnetinc, ytdllprov, …) × quarter. All YTD. |
| `filers_panel` | view | Cleaned POR: id_rssd, date, form_type, nm_lgl, city, state_abbr_nm, zip_cd. |
| `call_reports_panel` | view | Convenience: `bs_panel JOIN is_panel` on identity columns. Wide one-stop panel. |
| `harmonized_metadata` | table | 35 rows documenting each harmonized variable (desc, unit, MDRM sources, formula). |

### Raw MDRM layer (when harmonized isn't enough)

| Object | Kind | Purpose |
|--------|------|---------|
| `call_filers` | view | POR with raw column names ("Financial Institution Name", etc.) — filers_panel is the cleaned alias. |
| `schedule_rc` | view | Schedule RC — Balance Sheet |
| `schedule_ri` | view | Schedule RI — Income Statement (all YTD cumulative) |
| `schedule_rca`, `_rcb`, `_rcci`, `_rccii`, `_rcd`, `_rce`, `_rcei`, `_rceii`, `_rcf`, `_rcg`, `_rch`, `_rci`, `_rck`, `_rcl`, `_rcm`, `_rcn`, `_rco`, `_rcp`, `_rcq`, `_rcr`, `_rcri`, `_rcria`, `_rcrib`, `_rcrii`, `_rcs`, `_rct`, `_rcv` | view | RC sub-schedules (securities, loans, deposits, derivatives, capital, etc.) |
| `schedule_ria`, `_ribi`, `_ribii`, `_ric`, `_rici`, `_ricii`, `_rid`, `_rie` | view | RI sub-schedules (equity changes, charge-offs, explanations) |
| `schedule_ci`, `_ent`, `_gci`, `_gi`, `_gl`, `_leo`, `_narr`, `_su` | view | Administrative / contact / narrative |
| `mdrm_dictionary` | table | **87,687 rows** — Federal Reserve's MDRM item catalog. Use to look up what any MDRM code means. |
| `panel_metadata` | table | One row per (schedule, year, quarter) with row_count, n_columns, source_zip, parquet_path, built_at |

---

## Harmonized variables (bs_panel, is_panel)

All monetary values are **thousands of USD** (multiply by 1e3 for dollars, divide by 1e6 for billions, 1e9 for trillions). All values are DOUBLE (no casting needed). Date convention mirrors CFLV: quarter-end DATE.

### `bs_panel` — balance sheet (44 concepts + identity)

**Core totals & securities (RC)**

| Variable | Description | MDRM source |
|----------|-------------|-------------|
| `assets` | Total assets | RCFD/RCON 2170 |
| `cash` | Cash and balances due from depository institutions | RCFD/RCON 0081 + 0071 |
| `htmsec_ac` | Held-to-maturity securities (amortized cost) | RCFD/RCON 1754 (RC-B) |
| `afssec_fv` | Available-for-sale securities (fair value) | RCFD/RCON 1773 (RC-B) |
| `securities` | Total securities (HTM + AFS) | sum of above |
| `trading_assets` | Trading assets | RCFD/RCON 3545 |
| `premises` | Premises and fixed assets (incl. right-of-use) | RCFD/RCON 2145 |
| `oreo` | Other real estate owned | RCFD/RCON 2150 |
| `intangibles` | Intangible assets (total; incl. goodwill + MSAs + other) | RCFD/RCON 2143 |
| `goodwill` | Goodwill component of intangibles | RCFD/RCON 3163 (RC-M) |
| `msa` | Mortgage servicing assets | RCFD/RCON 3164 (RC-M) |
| `other_assets` | Other assets | RCFD/RCON 2160 |

**Interbank / liquid (RC)**

| Variable | Description | MDRM source |
|----------|-------------|-------------|
| `ffs` | Federal funds sold | RCONB987 |
| `reverse_repo` | Securities purchased under resell | RCFD/RCON B989 |
| `ffp` | Federal funds purchased | RCONB993 |
| `repo` | Securities sold under repurchase | RCFD/RCON B995 |

**Loans (RC-C Part I)**

| Variable | Description | MDRM source |
|----------|-------------|-------------|
| `ln_tot` | Total loans and leases, net of unearned income | RCFD/RCON 2122 |
| `ln_tot_gross` | Total loans and leases, gross | 2122 + 2123 |
| `ln_re` | Loans secured by real estate (sum of 9 subcategories) | RC-C items 1.a–1.e |
| `ln_ci` | Commercial and industrial loans | RCON1766 / RCFD1763+1764 |
| `ln_cons` | Consumer loans (cards + other revolving + auto + other) | B538+B539+K137+K207 |
| `ln_cc` | Credit card loans | RCFD/RCON B538 |
| `ln_agr` | Agricultural production loans | RCFD/RCON 1590 |
| `llres` | Allowance for loan and lease losses | RCFD/RCON 3123 |

**Past-due and nonaccrual (RC-N)**

| Variable | Description | MDRM source |
|----------|-------------|-------------|
| `ppd_30_89` | Loans 30-89 days past due still accruing | RCFD/RCON 1406 |
| `npl_tot` | Non-performing loans (90+ still accruing + nonaccrual) | 1403 + 1407 |

**Deposits (RC / RC-E)**

| Variable | Description | MDRM source |
|----------|-------------|-------------|
| `deposits` | Total deposits (domestic + foreign) | RCON2200 + RCFN2200 |
| `domestic_dep` | Deposits in domestic offices | RCON2200 |
| `foreign_dep` | Deposits in foreign offices (031 only) | RCFN2200 |
| `mmda` | Money market deposit accounts | RCON6810 (RC-E) |
| `saving_dep` | Other savings deposits (excl. MMDAs) | RCON0352 (RC-E) |
| `td_small` | Time deposits < $100K | RCON6648 (RC-E) |
| `td_mid` | Time deposits $100K–$250K | RCONJ473 (RC-E, from 2010) |
| `td_large` | Time deposits > $250K | RCONJ474 (RC-E, from 2010) |
| `brokered_dep` | Total brokered deposits | RCON2365 (RC-E) |

**Liabilities (RC)**

| Variable | Description | MDRM source |
|----------|-------------|-------------|
| `borrowings` | Other borrowed money (incl. FHLB advances) | RCFD/RCON 3190 |
| `sub_debt` | Subordinated notes and debentures | RCFD/RCON 3200 |
| `other_liab` | Other liabilities | RCFD/RCON 2930 |
| `total_liab` | Total liabilities | RCFD/RCON 2948 |

**Equity (RC)**

| Variable | Description | MDRM source |
|----------|-------------|-------------|
| `equity` | Total equity capital | RCFD/RCON 3210 |
| `retained_earnings` | Retained earnings | RCFD/RCON 3632 |
| `aoci` | Accumulated other comprehensive income | RCFD/RCON B530 |

**Lease financing (RC-C)**

| Variable | Description | MDRM source |
|----------|-------------|-------------|
| `ln_lease` | Lease financing receivables (domestic) | RCON 2165 |

**Deposit structure (RC / RC-E)**

| Variable | Description | MDRM source |
|----------|-------------|-------------|
| `demand_deposits` | Total demand deposits (domestic) | RCON 2210 (RC-E) |
| `transaction_dep` | Total transaction accounts (domestic) | RCON 2215 (RC-E) |
| `nontransaction_dep` | Total nontransaction accounts incl. MMDAs (domestic) | RCON 2385 (RC-E) |
| `dom_deposit_ib` | Interest-bearing deposits in domestic offices | RCON 6636 (RC) |
| `dom_deposit_nib` | Noninterest-bearing deposits in domestic offices | RCON 6631 (RC) |

**Quarterly averages (RC-K)**

| Variable | Description | MDRM source |
|----------|-------------|-------------|
| `qtr_avg_assets` | Quarterly average total assets | RCFD/RCON 3368 |
| `qtr_avg_loans` | Quarterly avg total loans (domestic + foreign for 031) | RCON 3360 + RCFN 3360 |
| `qtr_avg_int_bearing_bal` | Quarterly avg interest-bearing balances due from dep inst | RCFD/RCON 3381 |
| `qtr_avg_ffs_reverse_repo` | Quarterly avg fed funds sold + securities purchased under resell | RCFD/RCON 3365 |
| `qtr_avg_ust_sec` | Quarterly avg US Treasury + agency (excl. MBS) | RCFD/RCON B558 |
| `qtr_avg_mbs` | Quarterly avg mortgage-backed securities | RCFD/RCON B559 |
| `qtr_avg_oth_sec` | Quarterly avg other debt + equity securities | RCFD/RCON B560 |
| `qtr_avg_ln_re` | Quarterly avg real estate loans (1-4 family + other RE) | RCON 3385 / 3465+3466 |
| `qtr_avg_ln_ci` | Quarterly avg C&I loans (domestic) | RCON 3387 |
| `qtr_avg_lease` | Quarterly avg lease financing | RCFD/RCON 3484 |
| `qtr_avg_trans_dep` | Quarterly avg IB transaction deposits (domestic) | RCON 3485 |
| `qtr_avg_savings_dep` | Quarterly avg savings deposits incl. MMDAs | RCON B563 |
| `qtr_avg_time_dep_le250k` | Quarterly avg time deposits ≤ $250K | RCON HK16 (2017+) |
| `qtr_avg_time_dep_gt250k` | Quarterly avg time deposits > $250K | RCON HK17 (2017+) |
| `qtr_avg_ffpurch_repo` | Quarterly avg fed funds purchased + securities sold under repurchase | RCFD/RCON 3353 |
| `qtr_avg_othbor` | Quarterly avg other borrowed money | RCFD/RCON 3355 |

### `is_panel` — income statement (38 YTD + 37 derived quarterly-flow)

Every YTD variable has a **matching current-quarter flow** column (`q_*` prefix) derived via window function:

```
q_<name> = ytd<name> - COALESCE(LAG(ytd<name>) OVER (PARTITION BY id_rssd, activity_year ORDER BY activity_quarter), 0)
```

Q1 flow = Q1 YTD. Q2–Q4 flows = YTD delta. Sum of `q_*` across all four quarters within a year equals the Q4 YTD (verified: 0.0 diff for JPM/BofA/Wells in 2023).

**Caveat:** when a quarter is missing (FFIEC did not publish e.g. 2021-Q3 and 2023-Q3), the next quarter's `q_*` absorbs both the missing and the current quarter's flow. The annual sum still reconciles.

| YTD variable | Quarterly-flow variable | Description | MDRM |
|--------------|-------------------------|-------------|------|
| `ytdint_inc` | `q_int_inc` | Total interest income | RIAD4107 |
| `ytdint_exp` | `q_int_exp` | Total interest expense | RIAD4073 |
| `ytdint_inc_net` | `q_int_inc_net` | Net interest income | RIAD4074 |
| `ytdnonint_inc` | `q_nonint_inc` | Total noninterest income | RIAD4079 |
| `ytdnonint_exp` | `q_nonint_exp` | Total noninterest expense | RIAD4093 |
| `ytdllprov` | `q_llprov` | Provision for loan losses | RIAD4230 (RI-B II) |
| `ytdtradrev_inc` | `q_tradrev_inc` | Trading revenue | RIADA220 |
| `ytdinc_before_disc_op` | `q_inc_before_disc_op` | Income before discontinued operations | RIAD4300 |
| `ytdinc_taxes` | `q_inc_taxes` | Taxes on income | RIAD4302 |
| `ytdnetinc` | `q_netinc` | Net income | RIAD4340 |
| `ytdcommdividend` | `q_commdividend` | Cash dividends on common stock | RIAD4460 (RI-A) |
| `ytdprefdividend` | `q_prefdividend` | Cash dividends on preferred stock | RIAD4470 (RI-A) |
| `ytdsalaries` | `q_salaries` | Salaries and employee benefits | RIAD4135 |
| `ytdprem_exp` | `q_prem_exp` | Expenses of premises and fixed assets | RIAD4217 |
| `ytdoth_nonint_exp` | `q_oth_nonint_exp` | Other noninterest expense | RIAD4092 |
| `ytdsvc_charges` | `q_svc_charges` | Service charges on deposit accounts | RIAD4080 |
| `ytdgain_afs` | `q_gain_afs` | Realized gain (loss) on AFS debt securities | RIAD3196 |
| `ytdchargeoffs` | `q_chargeoffs` | Total charge-offs on loans and leases | RIAD4635 (RI-B I) |
| `ytdrecoveries` | `q_recoveries` | Total recoveries on loans and leases | RIAD4605 (RI-B I) |
| `ytdint_inc_ln` | `q_int_inc_ln` | Total interest and fee income on loans | RIAD4010 |
| `ytdint_inc_ln_re` | `q_int_inc_ln_re` | Interest on real estate loans (1-4 family + other) | RIAD4435+4436 |
| `ytdint_inc_ln_ci` | `q_int_inc_ln_ci` | Interest on C&I loans | RIAD4012 |
| `ytdint_inc_ln_cc` | `q_int_inc_ln_cc` | Interest on credit cards | RIADB485 |
| `ytdint_inc_ln_othcons` | `q_int_inc_ln_othcons` | Interest on other consumer loans | RIADB486 |
| `ytdint_inc_sec_ust` | `q_int_inc_sec_ust` | Interest on UST + agency securities (excl. MBS) | RIADB488 |
| `ytdint_inc_sec_mbs` | `q_int_inc_sec_mbs` | Interest on MBS | RIADB489 |
| `ytdint_inc_sec_oth` | `q_int_inc_sec_oth` | Interest on all other securities | RIAD4060 |
| `ytdint_inc_ffrepo` | `q_int_inc_ffrepo` | Interest on fed funds sold + reverse repos | RIAD4020 |
| `ytdint_inc_lease` | `q_int_inc_lease` | Income from lease financing | RIAD4065 |
| `ytdint_inc_ibb` | `q_int_inc_ibb` | Interest on balances due from dep inst | RIAD4115 |
| `ytdint_exp_trans_dep` | `q_int_exp_trans_dep` | Interest expense on transaction accounts | RIAD4508 |
| `ytdint_exp_savings_dep` | `q_int_exp_savings_dep` | Interest expense on savings + MMDAs | RIAD0093 |
| `ytdint_exp_time_le250k` | `q_int_exp_time_le250k` | Interest expense on time deposits ≤ $250K | RIADHK03 (2017+) |
| `ytdint_exp_time_gt250k` | `q_int_exp_time_gt250k` | Interest expense on time deposits > $250K | RIADHK04 (2017+) |
| `ytdint_exp_ffrepo` | `q_int_exp_ffrepo` | Interest expense on fed funds purchased + repos | RIAD4180 |
| `ytdfiduc_inc` | `q_fiduc_inc` | Income from fiduciary activities | RIAD4070 |
| `ytdgain_htm` | `q_gain_htm` | Realized gain (loss) on HTM securities | RIAD3521 |
| `num_employees` | — | FTE employees (point-in-time, no flow counterpart) | RIAD4150 |

Use `ytd*` for CFLV-comparable queries and cumulative / annual metrics. Use `q_*` for single-quarter rates (NIM, ROA, etc. — annualize by multiplying by 4).

### Identity columns (on every panel)

`idrssd` (VARCHAR, raw), `id_rssd` (BIGINT, CFLV-joinable), `activity_year`, `activity_quarter`, `date` (quarter-end DATE), `form_type` ('031'/'041'/'051'), `nm_lgl`, `city`, `state_abbr_nm`, `zip_cd`.

### What's deliberately NOT in v2

- **Schedule RC-R (regulatory capital ratios)** — RCOA-prefix codes differ in column meaning across forms (CBLR vs. standardized vs. advanced approaches). Warrants its own pass.
- **Schedule RC-O (FDIC items)** — form-specific reporting triggers.
- **Schedule RC-L (derivatives / off-balance-sheet)** — wide column shape, most only relevant for large banks.

Use raw `schedule_rcr`, `schedule_rco`, `schedule_rcl` views directly for those.

### Validation: full CFLV cross-check (2024-Q4, 4,543 filers)

91 harmonized variables mapped to their CFLV counterparts and compared over all joined (`id_rssd`, `date`) pairs. Run `call-reports-FFIEC/compare_cflv.py` to reproduce.

| Metric | Value |
|--------|-------|
| Variables tested | 91 (58 BS + 33 IS) |
| Joined pairs with both sides non-NULL | 385,486 |
| **Exact match (diff < $1K)** | **99.90%** |
| **Near match (diff < $100K)** | **99.95%** |
| Variables ≥99% exact | 85 |
| Variables 95-99% exact | 1 (`cash` at 96.92% — mostly $1 rounding on 041 filers; 051=100%, 031=93%) |
| Variables <95% (non-gap) | 0 |
| Variables 0% (CFLV data gaps post-2001) | 5 |

Five variables show 0% because CFLV stopped populating them between 1983 and 2000 — our layer fills the 2001+ gap: `qtr_avg_savings_dep`, `ytdtradrev_inc`, `ytdint_inc_ln_cc`, `ytdint_inc_sec_oth`, `ytdint_exp_savings_dep`.

Rename mapping for CFLV users: `retained_earnings`→`retain_earn`, `trading_assets`→`trad_ass`, `borrowings`→`othbor_liab`, `sub_debt`→`subdebt`, `total_liab`→`liab_tot_unadj`, `ffs`→`ffsold`, `reverse_repo`→`repo_purch`, `ffp`→`ffpurch`, `repo`→`repo_sold`, `premises`→`fixed_ass`, `other_assets`→`oth_assets`, `td_small/mid/large`→`time_dep_lt100k/ge100k_le250k/gt250k`, `qtr_avg_loans`→`qtr_avg_ln_tot`, `ytdsalaries`→`ytdnonint_exp_comp`, `ytdsvc_charges`→`ytdnonint_inc_srv_chrg_dep`, plus `_dom` suffix on several IS expense vars. Full list in `NOTES.md`.

For discovery of every variable + its formula:
```sql
SELECT * FROM harmonized_metadata ORDER BY panel, variable_name;
```

---

## Retrieving variables not in the harmonized layer

Three patterns, ordered by how much you know about the variable.

### Pattern A: you know the MDRM code

Query the raw schedule view directly.

```sql
-- real estate loans (RCFD1410, on Schedule RC-C Part I)
SELECT IDRSSD, activity_year, activity_quarter,
       TRY_CAST(RCFD1410 AS DOUBLE) / 1e6 AS re_loans_bn
FROM schedule_rcci
WHERE activity_year = 2024 AND activity_quarter = 4
  AND RCFD1410 IS NOT NULL;
```

Raw-layer MDRM columns are VARCHAR — always `TRY_CAST(... AS DOUBLE)` for arithmetic.

### Pattern B: you know the concept name but not the MDRM code

Search `mdrm_dictionary`:

```sql
SELECT "Mnemonic", "Item Code", "Item Name", "Reporting Form", "Start Date", "End Date"
FROM mdrm_dictionary
WHERE LOWER("Item Name") LIKE '%real estate%loans%'
  AND "Reporting Form" LIKE '%FFIEC 0%'
  AND "End Date" LIKE '%9999%'          -- currently active codes
ORDER BY "Item Code";
```

Compose the prefix (`RCFD` / `RCON` / `RIAD` / …) with the 4-character `Item Code` to form the column name that exists on a raw schedule view. `Mnemonic` column gives the prefix.

### Pattern C: you don't know which schedule the column lives on

Probe across all schedule views. Small Python script:

```python
import duckdb
con = duckdb.connect(r'C:\empirical-data-construction\call-reports-FFIEC\call-reports-ffiec.duckdb', read_only=True)
code = 'RCFD1410'
for v in [r[0] for r in con.execute("SHOW TABLES").fetchall()]:
    cols = [c[0] for c in con.execute(f'DESCRIBE {v}').fetchall()]
    if code in cols:
        print(v)
```

Or from SQL-only:

```sql
SELECT table_name FROM information_schema.columns
WHERE column_name = 'RCFD1410';
```

### Mixing harmonized and raw in one query

Join the raw schedule onto `bs_panel` to keep identity columns + harmonized context:

```sql
SELECT bs.id_rssd, bs.nm_lgl, bs.date, bs.form_type,
       bs.assets                                           AS assets_thousands,
       TRY_CAST(rcci.RCFD1410 AS DOUBLE)                   AS re_loans_raw,
       TRY_CAST(rcci.RCFD1410 AS DOUBLE) / NULLIF(bs.ln_tot, 0) AS re_loan_share
FROM bs_panel bs
LEFT JOIN schedule_rcci rcci
    ON bs.idrssd = rcci.IDRSSD
   AND bs.activity_year = rcci.activity_year
   AND bs.activity_quarter = rcci.activity_quarter
WHERE bs.activity_year = 2024 AND bs.activity_quarter = 4
ORDER BY bs.assets DESC LIMIT 20;
```

---

## Extending the harmonized layer

Harmonized layer is additive and trivial to extend. Adding a new variable is a one-file, ~2-second operation — no re-ETL, no rebuild of raw parquet.

### Step 1: add a concept entry

Edit `call-reports-FFIEC/harmonized/concepts.py`. Append to `BS_CONCEPTS` or `IS_CONCEPTS`:

```python
"ln_re": {
    "sql": "TRY_CAST(rcci.RCFD1410 AS DOUBLE)",
    "desc": "Real estate loans",
    "unit": "thousands_usd",
    "source_schedule": "RC-C",
    "mdrm_codes": ["RCFD1410"],
    "available_from": "2001Q1",
},
```

Available table aliases:
- `bs_panel`: `rc`, `rcb`, `rcci`, `rcn`, `rck`, `cf`
- `is_panel`: `ri`, `ria`, `ribii`, `cf`

Use `_cx("code")` helper for the common `COALESCE(RCFD, RCON)` pattern. Use literal SQL for anything custom (sums, derived values, CASE expressions).

### Step 2: refresh views

```bash
C:\envs\.basic_venv\Scripts\python.exe call-reports-FFIEC\construct.py --refresh-views
```

New variable appears in `bs_panel` / `is_panel` and in `harmonized_metadata`.

### Step 3: if the MDRM code lives on a schedule not yet joined

If the code lives on a schedule not in the JOIN chain (e.g. `schedule_rcm` for Memoranda, `schedule_rco` for deposit-insurance data), add the JOIN in `harmonized/views.py::_bs_panel_sql` or `_is_panel_sql`:

```python
LEFT JOIN schedule_rcm rcm
    USING (IDRSSD, activity_year, activity_quarter)
```

Then reference `rcm.<MDRM>` in the concept's `sql`. Refresh views again.

### Era-branch formulas

Some CFLV concepts need year-branched formulas (e.g. `time_deposits` split changed at 2010Q1, `ytdint_exp_time_ge100k` threshold moved at 2017Q1). Write it as a CASE:

```python
"time_deposits": {
    "sql": """
        CASE
            WHEN rc.activity_year >= 2010
                THEN COALESCE(TRY_CAST(rc.RCONJ473 AS DOUBLE), 0)
                   + COALESCE(TRY_CAST(rc.RCONJ474 AS DOUBLE), 0)
                   + COALESCE(TRY_CAST(rc.RCON6648 AS DOUBLE), 0)
            ELSE COALESCE(TRY_CAST(rc.RCON2604 AS DOUBLE), 0)
               + COALESCE(TRY_CAST(rc.RCON6648 AS DOUBLE), 0)
        END
    """,
    ...
}
```

---

Every schedule view has these columns on top of its MDRM set:
- `IDRSSD` (VARCHAR) — filer identifier; join key everywhere.
- `activity_year` (INTEGER) — 2001..2025.
- `activity_quarter` (INTEGER) — 1, 2, 3, 4.

**`SHOW TABLES` to list everything. `DESCRIBE {view}` to see columns.**

---

## Schema invariants (memorize these)

1. **Every MDRM column is VARCHAR**. Always `TRY_CAST(col AS DOUBLE)` for arithmetic, comparisons, aggregations. (HMDA convention.)
2. **Amounts are in thousands of USD.** Multiply by 1,000 for dollars; divide by `1e6` for billions, `1e9` for trillions.
3. **Income statement items are YTD cumulative within a year**: Q1 = Jan-Mar, Q2 = Jan-Jun, Q4 = full year. To get a single quarter's flow, diff from the previous quarter's YTD in the same year (or use only Q4 for annual totals).
4. **Partition-prune: always filter `activity_year` first.** The views sit over Hive-partitioned Parquet; a year filter skips 96/97 quarters of scanning.
5. **Description rows are pre-filtered.** The CDR source TSVs have a second header row of free-text descriptions; the ETL drops it. You will never see it.

### MDRM code prefixes — what they mean

| Prefix | Scope | Which form populates it |
|--------|-------|-------------------------|
| `RCFD` | Consolidated: domestic + foreign | FFIEC 031 filers (large banks with foreign offices) |
| `RCON` | Domestic only | FFIEC 041 / 051 filers (no foreign offices) |
| `RCFN` | Foreign offices only | 031 filers, rarely used |
| `RIAD` | Income statement | all forms |
| `RCFA` / `RCOA` | Quarterly averages (consolidated / domestic) | 031 / 041-051 |
| `RCFW` / `RCOW` | Weekly averages | all forms |
| `TEXT*` | Free-text explanation columns | narrative schedules (RIE, NARR) |

**Canonical "best available" value** across 031/041/051 filers:
```sql
COALESCE(RCFD<code>, RCON<code>)
```
Use this for cross-form analysis. 051 filers have all RCFD NULL by design.

### POR column names (in `call_filers`) have spaces — quote them

```sql
SELECT "Financial Institution Name", "Financial Institution Filing Type", "Financial Institution City"
FROM call_filers
WHERE activity_year = 2024 AND activity_quarter = 4;
```

---

## Finding the right MDRM code

Option A — you already know the code (e.g. `RCFD2170` for total assets):
```sql
SELECT RCFD2170 FROM schedule_rc WHERE activity_year=2024 AND activity_quarter=4 AND IDRSSD='852218';
```

Option B — you need to discover it. Search the dictionary:
```sql
SELECT item_code, item_name, reporting_form, start_date, end_date
FROM mdrm_dictionary
WHERE LOWER(item_name) LIKE '%total assets%'
ORDER BY end_date DESC NULLS LAST
LIMIT 20;
```

Option C — you have a column name and want its description:
```sql
SELECT item_code, item_name, description
FROM mdrm_dictionary
WHERE item_code = '2170'          -- bare 4-char code, no prefix
ORDER BY end_date DESC NULLS LAST
LIMIT 5;
```

Note: `mdrm_dictionary.item_code` is the **bare numeric code** (e.g. `2170`), not the prefixed column name (e.g. `RCFD2170`). Strip the 4-letter prefix before joining.

---

## Canonical MDRM codes you will reach for

Balance sheet (`schedule_rc`):

| MDRM | Concept | Unit |
|------|---------|------|
| RCFD2170 / RCON2170 | **Total assets** | thousands |
| RCFD2200 / RCON2200 | Total deposits | thousands |
| RCFD3210 / RCON3210 | Total equity capital | thousands |
| RCFD2122 / RCON2122 | Total loans and leases, net | thousands |
| RCFD1400 / RCON1400 | Total loans and leases, gross | thousands |
| RCFD1410 / RCON1410 | Real estate loans | thousands |
| RCFD1763 / RCON1763 | Commercial & industrial loans (to US addressees) | thousands |
| RCFDB538 / RCONB538 | Credit card loans | thousands |
| RCFD0081 / RCON0081 | Cash — noninterest-bearing balances | thousands |
| RCFD1773 / RCON1773 | AFS securities | thousands |
| RCFD1754 / RCON1754 | HTM securities | thousands |

Income statement (`schedule_ri`, all YTD):

| MDRM | Concept | Unit |
|------|---------|------|
| RIAD4340 | Net income (YTD) | thousands |
| RIAD4107 | Total interest income (YTD) | thousands |
| RIAD4073 | Total interest expense (YTD) | thousands |
| RIAD4079 | Total noninterest income (YTD) | thousands |
| RIAD4093 | Total noninterest expense (YTD) | thousands |
| RIAD4230 | Provision for loan losses (YTD) | thousands |

Averages (`schedule_rck`):

| MDRM | Concept |
|------|---------|
| RCFD3368 / RCON3368 | Quarterly average total assets |

**Always verify with `mdrm_dictionary`** before relying on these — MDRM codes occasionally retire and get re-used.

---

## Query recipes — harmonized layer (preferred)

These use `bs_panel` / `is_panel` / `call_reports_panel`. Same shape as `call-reports-CFLV` queries.

### Top N banks by total assets for a quarter

```sql
SELECT nm_lgl, form_type, assets / 1e6 AS assets_bn
FROM bs_panel
WHERE activity_year = 2024 AND activity_quarter = 4
ORDER BY assets_bn DESC NULLS LAST
LIMIT 20;
```

### Single bank time series

```sql
SELECT date, assets / 1e6 AS assets_bn,
       deposits / 1e6 AS deposits_bn,
       equity / 1e6   AS equity_bn,
       ln_tot / 1e6   AS loans_bn
FROM bs_panel
WHERE id_rssd = 852218            -- JPMorgan Chase Bank NA
ORDER BY date;
```

### System-wide aggregates by year (Q4 snapshot)

```sql
SELECT activity_year,
       COUNT(*)                  AS n_banks,
       SUM(assets) / 1e9         AS total_assets_tn,
       SUM(deposits) / 1e9       AS total_deposits_tn,
       SUM(equity) / 1e9         AS total_equity_tn
FROM bs_panel
WHERE activity_quarter = 4
GROUP BY 1
ORDER BY 1;
```

### Net interest margin (annualized, single quarter)

Use quarterly flow columns to avoid the `× 4/quarter` annualization hack:

```sql
SELECT b.id_rssd, b.nm_lgl, b.date,
       (i.q_int_inc_net * 4) / NULLIF(b.qtr_avg_assets, 0) AS nim_annualized
FROM bs_panel b
JOIN is_panel i USING (id_rssd, date)
WHERE b.qtr_avg_assets > 0
ORDER BY b.date DESC, nim_annualized DESC
LIMIT 20;
```

### Net interest margin from YTD (Q4 snapshot = full year, no annualization)

```sql
SELECT b.id_rssd, b.nm_lgl, b.date,
       i.ytdint_inc_net / NULLIF(b.qtr_avg_assets, 0) AS nim_full_year
FROM bs_panel b
JOIN is_panel i USING (id_rssd, date)
WHERE b.activity_quarter = 4 AND b.qtr_avg_assets > 0
ORDER BY b.date DESC, nim_full_year DESC
LIMIT 20;
```

### Quarterly net income (no LAG needed — use q_netinc directly)

```sql
SELECT id_rssd, activity_year, activity_quarter,
       ytdnetinc     AS ytd_netinc,
       q_netinc      AS quarterly_netinc
FROM is_panel
WHERE id_rssd = 852218 AND activity_year = 2023
ORDER BY activity_quarter;
```

### Cross-validate against CFLV (identical semantics)

```sql
ATTACH 'C:\empirical-data-construction\call-reports-CFLV\call-reports-cflv.duckdb' AS cflv (READ_ONLY);

SELECT bs.id_rssd, bs.date,
       bs.assets  AS ffiec_assets, cb.assets  AS cflv_assets,
       bs.deposits AS ffiec_dep,   cb.deposits AS cflv_dep
FROM bs_panel bs
JOIN cflv.balance_sheets cb USING (id_rssd, date)
WHERE bs.activity_year = 2024 AND bs.activity_quarter = 4
  AND bs.assets != cb.assets
LIMIT 20;
```

---

## Query recipes — raw MDRM layer

Use when you need a variable not in the harmonized layer.

### Top N banks by total assets for a quarter

```sql
SELECT f."Financial Institution Name"                         AS name,
       f."Financial Institution Filing Type"                  AS form,
       TRY_CAST(COALESCE(rc.RCFD2170, rc.RCON2170) AS DOUBLE) / 1e6 AS assets_bn
FROM schedule_rc rc
JOIN call_filers f USING (IDRSSD, activity_year, activity_quarter)
WHERE rc.activity_year = 2024 AND rc.activity_quarter = 4
ORDER BY assets_bn DESC NULLS LAST
LIMIT 20;
```

### Single bank time series

```sql
SELECT activity_year, activity_quarter,
       TRY_CAST(COALESCE(RCFD2170, RCON2170) AS DOUBLE) / 1e6 AS assets_bn,
       TRY_CAST(COALESCE(RCFD2200, RCON2200) AS DOUBLE) / 1e6 AS deposits_bn,
       TRY_CAST(COALESCE(RCFD3210, RCON3210) AS DOUBLE) / 1e6 AS equity_bn
FROM schedule_rc
WHERE IDRSSD = '852218'          -- JPMorgan Chase Bank NA
ORDER BY activity_year, activity_quarter;
```

### Population of banks over time

```sql
SELECT activity_year, activity_quarter, COUNT(*) AS n_filers
FROM call_filers
GROUP BY 1, 2
ORDER BY 1, 2;
```

### Income statement: convert YTD → single-quarter flow

```sql
WITH ytd AS (
    SELECT IDRSSD, activity_year, activity_quarter,
           TRY_CAST(RIAD4340 AS DOUBLE) AS netinc_ytd
    FROM schedule_ri
    WHERE activity_year = 2024
)
SELECT IDRSSD, activity_year, activity_quarter,
       netinc_ytd - COALESCE(
           LAG(netinc_ytd) OVER (PARTITION BY IDRSSD, activity_year
                                  ORDER BY activity_quarter),
           0
       ) AS quarterly_netinc
FROM ytd
ORDER BY IDRSSD, activity_quarter;
```

### Aggregate system: US banking total assets by year (Q4 snapshot)

```sql
SELECT activity_year,
       COUNT(*)                                                          AS n_banks,
       SUM(TRY_CAST(COALESCE(RCFD2170, RCON2170) AS DOUBLE)) / 1e9      AS total_assets_tn
FROM schedule_rc
WHERE activity_quarter = 4
GROUP BY activity_year
ORDER BY activity_year;
```

### Bank name lookup by fragment

```sql
SELECT DISTINCT IDRSSD,
                "Financial Institution Name"     AS name,
                "Financial Institution City"     AS city,
                "Financial Institution State"    AS state
FROM call_filers
WHERE LOWER("Financial Institution Name") LIKE '%jpmorgan%'
  AND activity_year = 2024 AND activity_quarter = 4;
```

### Form-type breakdown (031 vs 041 vs 051)

```sql
SELECT "Financial Institution Filing Type" AS form, COUNT(*) AS n
FROM call_filers
WHERE activity_year = 2024 AND activity_quarter = 4
GROUP BY form;
```

### Multi-schedule join (balance sheet + income statement)

```sql
SELECT rc.IDRSSD,
       rc.activity_year, rc.activity_quarter,
       TRY_CAST(COALESCE(rc.RCFD2170, rc.RCON2170) AS DOUBLE) AS assets,
       TRY_CAST(ri.RIAD4340 AS DOUBLE)                        AS netinc_ytd
FROM schedule_rc rc
JOIN schedule_ri ri
    USING (IDRSSD, activity_year, activity_quarter)
WHERE rc.activity_year = 2024 AND rc.activity_quarter = 4
LIMIT 20;
```

### Cross-dataset join: bank structure (NIC)

```sql
ATTACH 'C:\empirical-data-construction\nic\nic.duckdb' AS nic (READ_ONLY);

SELECT f.IDRSSD,
       f."Financial Institution Name" AS name,
       r.ID_RSSD_PARENT               AS parent_rssd
FROM call_filers f
LEFT JOIN nic.relationships r
    ON TRY_CAST(f.IDRSSD AS BIGINT) = r.ID_RSSD_OFFSPRING
   AND (r.D_DT_END IS NULL OR r.D_DT_END = '')
WHERE f.activity_year = 2024 AND f.activity_quarter = 4
LIMIT 20;
```

### Cross-dataset join: CFLV pre-harmonized concepts

Use this when you want CFLV's clean variable names AND want to verify / enrich with raw FFIEC columns:
```sql
ATTACH 'C:\empirical-data-construction\call-reports-CFLV\call-reports-cflv.duckdb' AS cflv (READ_ONLY);

SELECT rc.IDRSSD,
       TRY_CAST(COALESCE(rc.RCFD2170, rc.RCON2170) AS DOUBLE) AS ffiec_assets,
       cb.assets                                              AS cflv_assets,
       cb.deposits, cb.equity, cb.ln_tot
FROM schedule_rc rc
JOIN cflv.balance_sheets cb
    ON TRY_CAST(rc.IDRSSD AS BIGINT) = cb.id_rssd
   AND cb.date = DATE '2024-12-31'
WHERE rc.activity_year = 2024 AND rc.activity_quarter = 4
LIMIT 20;
```

---

## Quirks you must know

| Quirk | What to do |
|-------|-----------|
| All MDRM columns are VARCHAR | Wrap in `TRY_CAST(col AS DOUBLE)` for arithmetic. |
| 051 filers have NULL RCFD columns | Use `COALESCE(RCFD, RCON)` for any cross-form query. |
| POR column names contain spaces | Always double-quote: `"Financial Institution Name"`. |
| Column set drifts across eras | The views use `union_by_name=true`; querying a recent-era column for 2001 data returns NULL (not an error). |
| Schedules added/removed/renamed across eras | e.g. `schedule_rcr` (pre-2014 regulatory capital) → `schedule_rcri` + `schedule_rcrii` from 2014 on; `schedule_leo` only pre-2003; `schedule_rcv` only 2014+. |
| 2021-Q3 and 2023-Q3 missing | FFIEC did not publish Q3 bulk ZIPs those years. Not a pipeline bug — don't retry. |
| Income statement columns are YTD | Use LAG-diff to get quarterly flow, or restrict to Q4 for annual totals. |
| RC-R Part II is a 4-part COLUMN split | Already joined in the view (751 columns for 2024). One row per bank. |
| MDRM codes same number, multiple rows in dictionary | Same code can appear across prefixes and validity windows. Add `ORDER BY end_date DESC NULLS LAST LIMIT 1` for the latest. |
| Narrative schedule (RIE) sometimes has fewer rows than RC | Rows with malformed embedded newlines are skipped at parse time (~300 rows out of 5,000). |
| `year` filter is critical for performance | The views are over Hive partitions. Without a `WHERE activity_year = ...` predicate, a query scans 97 quarters × 46 schedules. |

---

## Refresh / rebuild commands (ETL — only if user explicitly asks)

```bash
# index newly-placed raw ZIPs into the manifest
python call-reports-FFIEC\download.py --scan

# build or refresh one quarter
python call-reports-FFIEC\construct.py --quarter 2025Q4

# bulk load everything in raw/
python call-reports-FFIEC\construct.py --all --skip-views
python call-reports-FFIEC\construct.py --refresh-views

# download / update MDRM dictionary
python call-reports-FFIEC\download.py --mdrm --force
```

Always use the venv Python: `C:\envs\.basic_venv\Scripts\python.exe`.

---

## Behavior guide for agents

1. **Before answering a data question, verify via `panel_metadata` that the quarter(s) you need are loaded.** If absent, tell the user which quarters are missing — do not fabricate data.
2. **Look up MDRM codes through `mdrm_dictionary` rather than guessing.** A wrong code silently returns wrong numbers.
3. **When in doubt about totals, cross-check against `call-reports-CFLV`** — it has the same IDRSSD + quarter-end date convention and pre-harmonized names.
4. **For any user-facing number, include the unit explicitly** ("$3.46 trillion", "assets in thousands"), since the raw values are in thousands of dollars and this is not obvious.
5. **Always partition-prune by `activity_year`** — without it, a query scans the entire 97-quarter panel.
6. **Prefer reading through views, not raw Parquet** — views apply column-set harmonization (`union_by_name`) across eras. Direct parquet reads give era-dependent columns.
