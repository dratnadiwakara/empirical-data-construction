# Compustat Fundamentals Annual

Annual financial-statement panel for North American public firms, sourced
from a WRDS extract of `comp.funda`. Covers fiscal years **1979–2026**
(current snapshot, 48 fiscal years).

| Property | Value |
|---|---|
| Source | WRDS Compustat Fundamentals Annual (`comp.funda`) — local extract, no public URL |
| Scale | **501,847 firm-fiscal-year rows** after standard filter; 980 variables |
| Filter applied | `indfmt='INDL' AND datafmt='STD' AND consol='C'` (drops ~17k duplicates from FS/restated/non-consolidated rows) |
| Primary key | `(gvkey, datadate)` — unique after filter |
| DuckDB path | `C:\empirical-data-construction\compustat\compustat.duckdb` |
| Views | `compustat` (raw fundamentals), `compustat_equity_ratios` (computed ratios) |
| Tables | `panel_metadata` (audit), `variable_dictionary` (all 980 variable defs) |

---

## Quick Start (for AI agents)

```python
import duckdb

# ALWAYS open read-only unless rebuilding
conn = duckdb.connect(
    r"C:\empirical-data-construction\compustat\compustat.duckdb",
    read_only=True,
)

# Step 1: confirm coverage before querying
conn.execute("SELECT * FROM panel_metadata ORDER BY year").df()

# Step 2: pull a firm's annual panel
conn.execute("""
    SELECT fyear, datadate, conm, "at", sale, ni, ceq, lt
    FROM compustat
    WHERE gvkey = '001690'           -- Apple
    ORDER BY fyear
""").df()

# Step 3: look up any of the 980 variables
conn.execute("""
    SELECT variable_name, type, description
    FROM variable_dictionary
    WHERE description ILIKE '%research%development%'
       OR variable_name ILIKE '%xrd%'
""").df()
```

Path is hardcoded above for clarity. In code, prefer:

```python
from config import get_compustat_duckdb_path
conn = duckdb.connect(str(get_compustat_duckdb_path()), read_only=True)
```

---

## Two views you can query

### `compustat` — full annual fundamentals (980 columns)

Hive-partitioned by `fyear` for fast year-range scans. Every variable in
`compustat_vars.csv` is exposed; check `variable_dictionary` for the
definition of any code-named field.

```sql
-- Total assets aggregated by fyear ($billions)
SELECT fyear, SUM("at") / 1e3 AS total_at_bil
FROM compustat
GROUP BY fyear
ORDER BY fyear;
```

### `compustat_equity_ratios` — computed equity ratios (15 columns)

One row per `(gvkey, fyear)`, derived from `compustat`. Built into the same
duckdb so no extra setup.

Columns:

| Column | Meaning |
|---|---|
| `gvkey` | LPAD-6 string |
| `public_date` | `datadate + 90 days`, TIMESTAMP. Use as PIT (point-in-time) floor: `WHERE public_date <= '2024-09-30'` |
| `fyear` | Fiscal year |
| `pe_op_dil` | Diluted op-earnings PE = `prcc_f / eps_clean` |
| `evm` | EV / EBITDA |
| `divyield` | Annual cash-dividend yield = `dvc / csho / prcc_f` |
| `PEG_trailing` | `pe_op_dil / (ni_clean_3y_cagr * 100)` |
| `roa` | `ni_clean / at` |
| `debt_ebitda` | **Net** debt / EBITDA = `(dltt + dlc - che) / ebitda` |
| `Ticker` | `tic` |
| `_synth` | Always `TRUE` (marker — these are repo-built, not WRDS-shipped) |
| `ni_clean` | `ni - COALESCE(spi,0) - COALESCE(nopi,0) - COALESCE(xido,0)` |
| `ni_raw` | Raw `ni` |
| `ni_clean_pct` | `ni_clean / ni` when `ni > 0` |

**Earnings cleanup convention** (Buffett-style look-through):

```
ni_clean = ni - spi - nopi - xido       (treat NULL as 0)
eps_clean = ni_clean / csho             (only when csho > 0)
```

Strips one-off items (impairments, gains-on-sale, discontinued ops). Use
`ni_clean` everywhere downstream, never raw `ni`.

**Formulas** (each NaN-guarded):

| Ratio | Formula | NaN when |
|---|---|---|
| `pe_op_dil` | `prcc_f / eps_clean` | `eps_clean <= 0` (loss / impairment years) |
| `evm` | `(prcc_f*csho + dltt + dlc + mib - che) / ebitda` | `ebitda <= 0` |
| `divyield` | `dvc / csho / prcc_f` | `dvc IS NULL`; **returns 0.0** if `dvc=0` |
| `PEG_trailing` | `pe / (cagr_3y * 100)` | <4 yrs history OR start `ni_clean <= 0` OR negative growth |
| `roa` | `ni_clean / at` | `at <= 0` |
| `debt_ebitda` | `(dltt + dlc - che) / ebitda` | `ebitda <= 0`. **Can be negative** when cash exceeds debt — that's correct, NOT clipped to 0. |

CAGR-3y: `LAG(ni_clean, 3) OVER (PARTITION BY gvkey ORDER BY datadate)`,
requires exactly 4 records in the window.

**Validation vs WRDS ratios file** (see `temp/wrds_ratios.gz`):

| Ratio | Pearson corr | Notes |
|---|---|---|
| evm | 0.94 | tight |
| roa | 0.93 | corr tight but mine systematically lower (extra cleanup + period-end vs avg assets) |
| pe_op_dil | 0.72 | different operating-earnings def |
| debt_ebitda | 0.64 | mine is **net** debt, WRDS is gross |
| divyield | 0.62 | level matches (0.020 vs 0.021), scatter from DPS source diffs |
| PEG | 0.37 | compounds PE + growth errors |

The level biases (ROA, debt_ebitda) are **intentional** per the formula
spec. Don't "fix" by switching to WRDS conventions without checking with
the user.

---

## Units & conventions

| Topic | Rule |
|---|---|
| Dollar amounts | **All in $millions** (Compustat convention). `at=5000` means $5B. Multiply by `1e6` for raw $, `/1e3` for $B. |
| `gvkey` | VARCHAR, LPAD-6 (`'001690'` for Apple). Source extract may emit unpadded; the `compustat_equity_ratios` view forces LPAD-6. The base `compustat` view passes raw source string — pad before joining if uncertain: `LPAD(gvkey, 6, '0')`. |
| `datadate` | DATE, fiscal year-end. Apple's FY2024 → `2024-09-30`. |
| `fyear` | BIGINT, fiscal year (Apple FY2024 with datadate Sept 2024 → fyear=2024). |
| `cik` | VARCHAR, raw (not zero-padded to 10). For EDGAR joins LPAD to 10. |
| `cusip` | VARCHAR, 9-character (8 issuer + 1 check digit). |
| `at` | DuckDB **reserved keyword** (`AT TIME ZONE`). Always quote: `"at"`. Same caution for any 2-letter Compustat code that collides — quote when in doubt. |
| Empty strings & `'NA'` | Become NULL during construct. Use `IS NULL` filters, not `= ''`. |
| Date sentinels in raw | `TRY_CAST` is applied during construct; unparseable dates → NULL. |
| Financial firms | **Dropped** by `indfmt='INDL'`. Banks/insurers won't appear here — use FR Y-9C, Call Reports, or NIC for FS-format firms. |

---

## Headline columns

Documented inline. The other ~960 variables live in `variable_dictionary`.

| Column | Type | Meaning |
|---|---|---|
| `gvkey` | VARCHAR | Global Company Key — stable firm ID. **PK with `datadate`.** |
| `datadate` | DATE | Fiscal year-end date. |
| `fyear` | BIGINT | Fiscal year (partition column). |
| `tic` | VARCHAR | Ticker symbol. |
| `conm` | VARCHAR | Company name. |
| `cusip` | VARCHAR | 9-char CUSIP. |
| `cik` | VARCHAR | SEC CIK. |
| `exchg` | BIGINT | Exchange code: 11=NYSE, 12=ASE, 14=NASDAQ. |
| `sic` | VARCHAR | SIC industry code. |
| `naicsh` | BIGINT | NAICS historical. |
| `at` | DOUBLE | Total assets ($M). |
| `sale` | DOUBLE | Sales / revenue ($M). |
| `ni` | DOUBLE | Net income ($M). |
| `ceq` | DOUBLE | Common / ordinary equity ($M). |
| `lt` | DOUBLE | Total liabilities ($M). |
| `prcc_f` | DOUBLE | Stock price close, fiscal year-end ($). |
| `prcc_c` | DOUBLE | Stock price close, calendar year-end ($). |
| `csho` | DOUBLE | Common shares outstanding (millions). |
| `mkvalt` | DOUBLE | Market value of equity ($M) at fyear-end. |
| `dltt` | DOUBLE | Long-term debt ($M). |
| `dlc` | DOUBLE | Debt in current liabilities ($M). |
| `che` | DOUBLE | Cash + short-term investments ($M). |
| `ebitda` | DOUBLE | EBITDA ($M). |
| `oibdp` | DOUBLE | Operating income before D&A ($M). Fallback for `ebitda`. |
| `dvc` | DOUBLE | Common dividends ($M). |
| `spi` | DOUBLE | Special items ($M). |
| `nopi` | DOUBLE | Non-operating income ($M). |
| `xido` | DOUBLE | Extraordinary items + discontinued ops ($M). |
| `indfmt` | VARCHAR | Industry format. Always `'INDL'` here (filtered). |
| `datafmt` | VARCHAR | Data format. Always `'STD'` here. |
| `consol` | VARCHAR | Consolidation. Always `'C'` here. |
| `curcd` | VARCHAR | ISO currency. Mix of USD, CAD. |
| `costat` | VARCHAR | Company status: `A`=active, `I`=inactive. |

---

## Discovery patterns

```sql
-- Find variables matching a concept
SELECT variable_name, type, description
FROM variable_dictionary
WHERE description ILIKE '%capital expenditure%';

-- Find all variables of a given type
SELECT variable_name, description FROM variable_dictionary WHERE type = 'Date';

-- Confirm a column actually has data in your sample
SELECT
    AVG(CASE WHEN xrd IS NOT NULL THEN 1.0 ELSE 0.0 END) AS fill_rate,
    MIN(xrd), MEDIAN(xrd), MAX(xrd)
FROM compustat WHERE fyear = 2023;
```

---

## Common research patterns

### 1. Annual ROA / leverage / turnover panel

```sql
SELECT gvkey, conm, fyear,
       ni / NULLIF("at", 0)   AS roa_raw,
       lt / NULLIF("at", 0)   AS leverage,
       sale / NULLIF("at", 0) AS asset_turnover
FROM compustat
WHERE "at" IS NOT NULL AND "at" > 0
ORDER BY gvkey, fyear;
```

### 2. PIT-safe equity ratios as of a date

```sql
-- "Knowable as of 2024-09-30" — uses 90-day filing-lag floor
SELECT *
FROM compustat_equity_ratios
WHERE public_date <= '2024-09-30';
```

### 3. Industry aggregates by SIC2

```sql
SELECT fyear,
       SUBSTR(LPAD(sic, 4, '0'), 1, 2) AS sic2,
       COUNT(*)        AS n_firms,
       SUM(sale) / 1e3 AS sector_sales_bil,
       AVG(ni / NULLIF("at", 0)) AS avg_roa
FROM compustat
WHERE sic IS NOT NULL
GROUP BY fyear, sic2
ORDER BY fyear, sector_sales_bil DESC;
```

### 4. Join Compustat to other repo datasets

| Target | Bridge | Notes |
|---|---|---|
| **EDGAR / 10-Ks** | `cik` (LPAD to 10) | `compustat.cik` → `edgar.cik`. Some Compustat firms have NULL cik (non-SEC filers). |
| **NIC / Y-9C / Call Reports** | via `permco-rssd-link` then by `rssd_id` | Compustat has no rssd_id directly. Pipe through CIK -> RSSD (see `permco-rssd-link/README.md`). |
| **CRSP** | `gvkey` (via WRDS CCM linktable, not in this repo) | Repo doesn't ship CRSP; if you have a linktable use `gvkey -> permno`. |
| Direct ticker-based joins | `tic` | Unreliable across mergers; prefer `gvkey` or `cik`. |

Compustat is **not** the right source for banks/insurers (filtered out via
`indfmt='INDL'`). Use FR Y-9C for bank holding companies.

### 5. ESG / R&D / capex panel

```sql
SELECT gvkey, fyear, conm,
       capx           AS capex,
       xrd            AS rnd,
       xsga           AS sga,
       xrd / NULLIF(sale, 0) AS rnd_intensity
FROM compustat
WHERE xrd > 0 AND fyear BETWEEN 2015 AND 2024;
```

---

## Audit table

```sql
SELECT * FROM panel_metadata ORDER BY year;
-- year (= fyear) | row_count | source_url | built_at | parquet_path
```

Use this to:
- confirm a year has been built before querying
- detect stale builds (`built_at` timestamp)
- estimate scan cost (row_count per year)

---

## Pipeline commands

```bash
# 1. Snapshot from temp/ to HDD raw/
C:\envs\.basic_venv\Scripts\python.exe -m compustat.download

# 2. Build parquets + DuckDB views + variable_dictionary
C:\envs\.basic_venv\Scripts\python.exe -m compustat.construct

# Force rebuild of staging parquets
C:\envs\.basic_venv\Scripts\python.exe -m compustat.construct --force

# Refresh views only (skip parquet rebuild) — useful after editing the view SQL
C:\envs\.basic_venv\Scripts\python.exe -m compustat.construct --views-only

# 3. Validate
C:\envs\.basic_venv\Scripts\python.exe -m compustat.inspect

# Ad-hoc SQL against the duckdb
C:\envs\.basic_venv\Scripts\python.exe -m compustat.inspect --sql "SELECT COUNT(*) FROM compustat WHERE fyear=2020"
```

---

## Storage layout

```
C:\empirical-data-construction\compustat\
  compustat.duckdb                     # views: compustat, compustat_equity_ratios
                                       # tables: panel_metadata, variable_dictionary
  download_manifest.json               # sha256 of each ingested source file
  raw\
    compustat_fund.gz                  # WRDS Fundamentals Annual extract
    compustat_vars.csv                 # variable dictionary source (976 entries)
  staging\
    fyear=1979\ data_0.parquet         # snappy parquet, fyear partition column dropped from data
    fyear=2010\ data_0.parquet
    ...
    fyear=2025\ data_0.parquet
```

The partition column (`fyear`) lives only in the directory name. DuckDB's
`hive_partitioning=true` re-injects it as a column in the view. Queries
with `WHERE fyear = 2020` trigger partition pruning (read 1/17 files).

---

## Refreshing the data

WRDS has no public download endpoint. To refresh:

1. Pull a fresh `comp.funda` extract from WRDS (all years, all vars in
   `compustat_vars.csv`). Save as `compustat_fund.gz`.
2. If the variable set changed, also refresh `compustat_vars.csv`.
3. Replace files in `compustat/temp/`.
4. Run `python -m compustat.download` — sha256 change triggers re-copy.
5. Run `python -m compustat.construct --force` to rebuild parquets.
6. Run `python -m compustat.inspect` to verify.

`download_manifest.json` keeps the per-file sha256; idempotent reruns are
free if the source hasn't changed.

---

## Known gotchas (read before debugging)

1. **`at` is a reserved DuckDB keyword.** Always quote: `"at"`. Same for
   any short Compustat code that collides with SQL keywords. When in
   doubt, double-quote.

2. **gvkey may be unpadded in the raw view.** The `compustat` view exposes
   whatever the source file has. The `compustat_equity_ratios` view forces
   LPAD-6. For cross-source joins on gvkey, pad both sides:
   `LPAD(gvkey, 6, '0')`.

3. **Edge fyears 1979 (1,279 rows) and 2026 (249 rows) are partial.**
   Firms with non-December fiscal year-ends land in an fyear whose calendar
   window the extract only partly spans. The rows are valid, but year-level
   aggregates for 1979 and 2026 are not comparable to full years — drop both
   edges for panel work: `WHERE fyear BETWEEN 1980 AND 2025`.

4. **`at` fill-rate declines secularly, ~95% in the 1980s to 53% by 2025.**
   Not a bug and not degradation — shells, SPACs, trusts and pre-revenue
   listings, which file no total-assets figure, grew as a share of
   `indfmt='INDL'` over the period. Filter on `at IS NOT NULL` for
   balance-sheet analyses. `inspect.py` flags fill below 45%.

5. **Negative `sale` rows exist** (5–30 per year). Refunds / accounting
   corrections — real, not data errors. Filter `sale > 0` if you need
   positive revenue.

6. **`fyear` ≠ calendar year.** A firm with `datadate='2024-01-31'` has
   `fyear=2023` (FY23 closed January 2024). For calendar-year analyses,
   bucket by `YEAR(datadate)`, not `fyear`.

7. **Multiple currencies.** `curcd` is mostly `USD` but includes `CAD` (and
   a handful of others) for cross-listed Canadian firms. Filter
   `WHERE curcd='USD'` if dollar-amount aggregates matter.

8. **`compustat_equity_ratios` recomputes on view query.** Tens of ms for
   single-firm queries; sub-second for full-panel scans. If you need to
   join it heavily, materialize: `CREATE TABLE x AS SELECT * FROM compustat_equity_ratios`.

9. **`mkvalt` may be NULL even when `prcc_f * csho` is computable.** Use
   the latter for market cap if you need full coverage.

10. **`spi`, `nopi`, `xido` are commonly NULL.** The `ni_clean` formula
    `COALESCE`s them to 0 — don't `WHERE spi IS NOT NULL` or you'll lose
    most of the panel.

---

## Source files in `temp/`

| File | Used? | Purpose |
|---|---|---|
| `compustat_fund.gz` | YES | Main fundamentals extract — feeds `compustat` view |
| `compustat_vars.csv` | YES | Variable dictionary — feeds `variable_dictionary` table |
| `wrds_ratios.gz` | not yet | WRDS-shipped monthly ratio panel. Used for one-off validation (see ratios validation section above). Could become a second view if needed. |
| `compustat_ratios.csv` | not yet | Variable legend for `wrds_ratios.gz`. |

If you want ratios beyond the 7 in `compustat_equity_ratios`, ask before
building — many are already in `wrds_ratios.gz` and could be loaded as a
second view rather than recomputed.
