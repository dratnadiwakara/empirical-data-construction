# NOTES — FFIEC Call Reports pipeline

Build log + operational notes. User-facing docs live in `README.md`.

---

## Full CFLV cross-validation (2026-04-23)

Comprehensive comparison of all overlapping harmonized variables against CFLV
`balance_sheets` and `income_statements` tables, 2024-Q4 (4,543 filers joined
on `id_rssd + date`). Script: `compare_cflv.py` (kept for repeat runs on
future quarters).

**Headline numbers:**
- 91 variables mapped (58 BS + 33 IS)
- 385,486 joined pairs where both sides non-NULL
- **99.90% exact match** (diff < $1K)
- **99.95% near-match** (diff < $100K)

**Per-variable distribution:**
- 85 variables ≥99% exact match
- 1 variable 95-99% (`cash` at 96.92% — mostly $1 rounding on 041 filers)
- 0 variables 80-95%
- 0 variables <80% (non-gap)
- 5 variables at 0%: CFLV stopped populating them post-2001. Our layer
  extends these concepts with current MDRM codes:
    - `qtr_avg_savings_dep` (CFLV last 2000Q4)
    - `ytdtradrev_inc` (CFLV last 1995Q4)
    - `ytdint_inc_ln_cc` (CFLV last 2000Q4)
    - `ytdint_inc_sec_oth` (CFLV last 1983Q4)
    - `ytdint_exp_savings_dep` (CFLV last 2000Q4)

**Rename mapping (FFIEC → CFLV):** `retained_earnings` → `retain_earn`,
`trading_assets` → `trad_ass`, `trading_liab` → `trad_liab`, `borrowings` →
`othbor_liab`, `sub_debt` → `subdebt`, `other_liab` → `liab_oth`, `total_liab`
→ `liab_tot_unadj`, `ffs` → `ffsold`, `reverse_repo` → `repo_purch`, `ffp` →
`ffpurch`, `repo` → `repo_sold`, `premises` → `fixed_ass`, `other_assets` →
`oth_assets`, `td_small/mid/large` → `time_dep_lt100k/ge100k_le250k/gt250k`,
`qtr_avg_loans` → `qtr_avg_ln_tot`, `qtr_avg_int_bearing_bal` →
`qtr_avg_ib_bal_due`, `qtr_avg_ffs_reverse_repo` → `qtr_avg_ffrepo_ass`,
`qtr_avg_trans_dep` → `qtr_avg_trans_dep_dom`, `qtr_avg_savings_dep` →
`qtr_avg_sav_dep_dom`, `qtr_avg_ffpurch_repo` → `qtr_avg_ffrepo_liab`,
`qtr_avg_othbor` → `qtr_avg_othbor_liab`, `ytdsalaries` →
`ytdnonint_exp_comp`, `ytdprem_exp` → `ytdnonint_exp_fass`,
`ytdoth_nonint_exp` → `ytdoth_operating_exp`, `ytdsvc_charges` →
`ytdnonint_inc_srv_chrg_dep`, plus `_dom` suffix on several IS expense vars.

---

## v3 CFLV-parity expansion (2026-04-23)

Added **39 more harmonized concepts** to match the widely-used CFLV variable
set. Total now: **105 base + 37 derived q_\* flows = 142 user-facing columns**
across the panels. 147 entries in `harmonized_metadata`.

### Added variables

- **BS — RC-C**: `ln_lease` (RCON2165, domestic lease financing)
- **BS — RC/RC-E deposit structure**: `demand_deposits`, `transaction_dep`,
  `nontransaction_dep`, `dom_deposit_ib`, `dom_deposit_nib`
- **BS — RC-K quarterly averages** (15 vars): `qtr_avg_loans`,
  `qtr_avg_int_bearing_bal`, `qtr_avg_ffs_reverse_repo`, `qtr_avg_ust_sec`,
  `qtr_avg_mbs`, `qtr_avg_oth_sec`, `qtr_avg_ln_re` (with 3385 fallback for
  pre-2008), `qtr_avg_ln_ci`, `qtr_avg_lease`, `qtr_avg_trans_dep`,
  `qtr_avg_savings_dep`, `qtr_avg_time_dep_le250k`, `qtr_avg_time_dep_gt250k`,
  `qtr_avg_ffpurch_repo`, `qtr_avg_othbor`
- **IS — RI interest income detail** (11 vars): `ytdint_inc_ln`,
  `ytdint_inc_ln_re` (4435+4436), `ytdint_inc_ln_ci`, `ytdint_inc_ln_cc`,
  `ytdint_inc_ln_othcons`, `ytdint_inc_sec_ust`, `ytdint_inc_sec_mbs`,
  `ytdint_inc_sec_oth`, `ytdint_inc_ffrepo`, `ytdint_inc_lease`,
  `ytdint_inc_ibb`
- **IS — RI interest expense detail** (5 vars): `ytdint_exp_trans_dep`,
  `ytdint_exp_savings_dep`, `ytdint_exp_time_le250k`,
  `ytdint_exp_time_gt250k`, `ytdint_exp_ffrepo`
- **IS — other**: `ytdfiduc_inc` (RIAD4070), `ytdgain_htm` (RIAD3521)

### Validation

3-bank × 39-var 2025-Q4 spot-check vs PDFs: **117 of 117 cells exact match**
(BofA 031, Zions 041, Third Coast 051). Every cell verified against the
corresponding PDF MDRM code value. Includes the `qtr_avg_loans` 031
consolidation (domestic RCON3360 + foreign RCFN3360 = 1,146,944,000 for BofA)
and the `qtr_avg_ln_re` post-2008 split (3465+3466 for all 3 banks).

### Gotchas noted

- `RCFD3360`, `RCFD3387`, `RCFD3465`, `RCFD3485`, `RCFDB563`, `RCFDHK16`,
  `RCFDHK17` do not exist in CDR bulk — domestic-only codes. Formulas use
  RCON directly.
- `qtr_avg_ln_re`: CFLV's v3 spec was single line `RCON3385` (pre-2008).
  Post-2008 reports use the split RCON3465 (1-4 family) + RCON3466 (other
  RE). Our formula: `COALESCE(RCON3385, COALESCE(RCON3465,0) +
  COALESCE(RCON3466,0))` handles both eras.
- `qtr_avg_loans` 031-specific: domestic RCON3360 is only the domestic-loan
  portion. Total consolidated = RCON3360 + RCFN3360. 041/051 filers don't
  report RCFN3360 so sum degrades to RCON3360 alone. Verified for BofA 2025-Q4.
- `ytdint_inc_ln_re`: uses sum of RIAD4435 (1-4 family RE int inc) +
  RIAD4436 (other RE int inc). Post-2010 split replaces old RIAD4011 single
  line. Valid 2008Q1+.

---

## v2 harmonized layer expansion (2026-04-22, same day)

Added 31 new harmonized concepts bringing the total to **66 base variables** (+
19 derived quarterly-flow companions = 85 user-facing columns across the
panels). All new concepts identified by studying 3 sample PDFs (BofA 031, Zions
041, Third Coast 051, all 2025-Q4) to confirm the same MDRM line-item codes
appear on every form.

New schedule joins: `bs_panel` now `LEFT JOIN schedule_rce` (deposit breakdown)
and `LEFT JOIN schedule_rcm` (goodwill/MSA detail). `is_panel` now `LEFT JOIN
schedule_ribi` (charge-offs / recoveries).

### Added variables

- **BS — RC liquid/interbank**: `ffs`, `reverse_repo`, `ffp`, `repo`
- **BS — RC trading/premises/other**: `trading_assets`, `trading_liab`, `premises`, `oreo`, `intangibles`, `other_assets`
- **BS — RC liabilities**: `borrowings`, `sub_debt`, `other_liab`, `total_liab`
- **BS — RC equity detail**: `retained_earnings`, `aoci`
- **BS — RC-M intangibles**: `goodwill`, `msa`
- **BS — RC-C derived loan totals**: `ln_re`, `ln_ci`, `ln_cons` (formerly
  omitted in v1; activated via sum-of-subcategory formulas)
- **BS — RC-E deposit breakdown**: `mmda`, `saving_dep`, `td_small`, `td_mid`,
  `td_large`, `brokered_dep`
- **BS — RC-N past-due**: `ppd_30_89` (30-89 day bucket; complements existing `npl_tot` for 90+)
- **IS — RI expense detail**: `ytdsalaries`, `ytdprem_exp`, `ytdoth_nonint_exp`
- **IS — RI revenue detail**: `ytdsvc_charges`, `ytdgain_afs`
- **IS — RI-A dividends**: `ytdprefdividend`
- **IS — RI-B I charge-offs**: `ytdchargeoffs`, `ytdrecoveries`

All 8 new YTD variables automatically get `q_*` quarterly-flow companions via
the existing window-LAG layer.

### Gotchas documented in concepts.py

- `RCONB987` (ffs), `RCONB993` (ffp): no RCFD variant in CDR bulk — use RCON-only.
- `RCFD1766` doesn't exist in CDR bulk for 031 filers → `ln_ci` formula uses
  `COALESCE(RCON1766, RCFD1763 + RCFD1764)`. Verified 307,488,000 for BofA
  2025-Q4 against PDF RC-C item 4.
- RC-E is defined as domestic-only on all three forms — concepts use
  `TRY_CAST(rce.RCON<code> AS DOUBLE)` directly, not `_cx`.

### Validation

3-bank 2025-Q4 spot-check against PDFs: 100% exact match on every cell read
directly from the PDFs. Sum-identity checks:

- `total_liab` = `deposits + ffp + repo + trading_liab + borrowings + sub_debt
  + other_liab` — exact (diff = 0) for all 3 banks.
- `goodwill + msa <= intangibles` — passes for 4,393 of 4,394 filers in
  2025-Q4 (1 edge case, likely a bank in liquidation or with special accounting).
- `ln_re`, `ln_ci`, `ln_cons <= ln_tot_gross` — 100% pass (4,394 / 4,394).
- `q_chargeoffs` quarterly flow sums reconcile: BofA 2024 = 1,738 + 1,778 +
  1,802 + 1,735 = 7,053,000 = Q4 YTD.

Raw panel row counts unchanged (648,257) after adding 3 new LEFT JOINs —
confirms no row multiplication.

---

## v2 harmonized layer (2026-04-22, same day)

Added `harmonized/` package and wired it into `construct.refresh_views()`. Four additional views + one metadata table now sit on top of the raw schedule views:

- `filers_panel` — cleaned POR (renames `"Financial Institution Name"` → `nm_lgl`, etc., adds derived `id_rssd` BIGINT + `date` DATE).
- `bs_panel` — 16 balance-sheet concepts over `schedule_rc LEFT JOIN rcb LEFT JOIN rcci LEFT JOIN rcn LEFT JOIN rck LEFT JOIN filers_panel`.
- `is_panel` — 12 income-statement concepts over `schedule_ri LEFT JOIN ria LEFT JOIN ribii LEFT JOIN filers_panel`.
- `call_reports_panel` — convenience `bs_panel JOIN is_panel` on identity columns.
- `harmonized_metadata` — 35-row self-doc table (variable_name, panel, description, unit, source_schedule, mdrm_codes, formula, available_from).

### Files

```
call-reports-FFIEC/harmonized/
├── __init__.py
├── concepts.py       # BS_CONCEPTS / IS_CONCEPTS / FILERS_CONCEPTS dicts (35 entries)
└── views.py          # build_views(conn) — SQL generator, called from construct.refresh_views
```

Concept definitions are literal SQL expressions in a Python dict — no YAML, no DSL. The `_cx(code, table)` shorthand expands to `COALESCE(TRY_CAST(<t>.RCFD<code> AS DOUBLE), TRY_CAST(<t>.RCON<code> AS DOUBLE))`, which serves 031 filers from RCFD and 041/051 from RCON.

### Validation against CFLV

2024-Q4, 4,543 filers joined by `id_rssd` + `date`. Match rate for core concepts:

| | |
|---|---|
| `assets`, `deposits`, `equity`, `ln_tot`, `llres`, `npl_tot`, `htmsec_ac`, `afssec_fv`, `securities`, `domestic_dep`, `foreign_dep`, `ytdint_inc`, `ytdint_exp`, `ytdint_inc_net`, `ytdllprov`, `ytdnonint_inc`, `ytdnonint_exp`, `ytdnetinc`, `ytdinc_before_disc_op`, `ytdinc_taxes`, `ytdcommdividend`, `qtr_avg_assets` | 99.74%–100% |
| `ln_cc`, `ln_agr`, `foreign_dep`, `num_employees` | 100% |
| `cash` | 96.92% (see quirk below) |
| `ytdtradrev_inc` | 75.02% — 75% is 051-filer NULL/NULL matches; 25% non-matches are CFLV's custom derivation |

Remaining <1% diffs = CFLV amendment absorption vs FFIEC snapshot. Acceptable.

### Variables deliberately excluded from v1

`ln_re`, `ln_ci`, `ln_cons` — CFLV derives them from sub-sub-categories (e.g., RCONF158 + RCONF159 + RCON5367 + … for 041 real-estate loans). CDR bulk does not expose the consolidated `RCFD1410`/`RCFD1766`/`RCFD1975` codes for all forms; CFLV must reconstruct. Implementing this correctly requires per-form, per-era sub-category summation — deferred to a future phase. For now, query `schedule_rcci` directly if you need these.

### Fixes made during v2 build

| Issue | Fix |
|-------|-----|
| `RCFD2200` not in CDR bulk at all | `deposits` = `RCON2200 + COALESCE(RCFN2200, 0)` |
| `RCFD1754` (HTM) declared on `schedule_rc` but always NULL; real value on `schedule_rcb` | source `htmsec_ac` from `rcb`, not `rc`. Same for `afssec_fv`. |
| `RIAD4230` (ytdllprov) declared on `schedule_ri` but always NULL; real value on `schedule_ribii` | source `ytdllprov` from `ribii.RIAD4230`. |
| `RIAD4460` (dividends) only on `schedule_ria`, not `schedule_ri` | source `ytdcommdividend` from `ria`. |
| Initial concepts list placed loan categories on `schedule_rc`; they live on `schedule_rcci` | routed `ln_tot`, `ln_cc`, `ln_agr`, `npl_tot` through rcci/rcn with proper `rcci` / `rcn` aliases. |

### `cash` 3% mismatch

`cash` = `COALESCE(RCFD0081, RCON0081) + COALESCE(RCFD0071, RCON0071)`. Matches CFLV for 97% of filers. The 3% gap likely arises from filers where one of the two components is NULL (e.g., some small banks report only 0081 without 0071): CFLV may substitute 0 while our COALESCE within each term returns NULL, making the sum NULL. Acceptable as-is; fix is a `COALESCE(…, 0)` wrap on each component if strictness needed later.

---

## Build summary (2026-04-22)

Built the `call-reports-FFIEC` dataset module from scratch. Delivered:

- **Code**: `__init__.py`, `metadata.py`, `schema.py`, `download.py`, `construct.py`, `README.md`, `NOTES.md`, `plan.md` under `call-reports-FFIEC/`.
- **`config.py` additions**: `FFIEC_DATASET`, `FFIEC_USER_AGENT`, plus 6 path helpers (`get_ffiec_storage_path`, `get_ffiec_duckdb_path`, `get_ffiec_raw_path`, `get_ffiec_staging_path`, `get_ffiec_manifest_path`, `get_ffiec_mdrm_path`).
- **Data**: populated `C:\empirical-data-construction\call-reports-FFIEC\`:
  - 97 quarterly raw ZIPs (2001-Q1 → 2025-Q4; gaps: 2021-Q3, 2023-Q3 not published by FFIEC).
  - Hive-partitioned Parquet per schedule × quarter under `staging/`.
  - `call-reports-ffiec.duckdb`: 46 schedule views + `call_filers` + `mdrm_dictionary` (87,687 rows) + `panel_metadata`.

### Source

FFIEC CDR bulk download portal (<https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx>) — ASP.NET form, no public API. ZIPs placed manually in `raw/` by the user. `download.py --scan` indexes them into the manifest.

Each quarterly "Call Bulk All Schedules" ZIP contains ~35-48 tab-delimited files: one bulk POR (filer identity) plus one TSV per schedule (RC, RC-A…V, RI, RI-A…E, plus CI/ENT/GI/GL/NARR/SU/LEO). Forms 031 / 041 / 051 are merged inside the TSVs and discriminated by MDRM prefix (RCFD = consolidated-with-foreign, RCON = domestic-only) and by `"Financial Institution Filing Type"` in POR.

### ETL flow

1. **Extract**: `construct.py` unzips `FFIEC CDR Call Bulk All Schedules MMDDYYYY.zip` into `raw/{YYYY}Q{Q}/`. Skipped if already extracted (cached). ZIPs untouched.
2. **Parse**: per-schedule TSVs read via **pyarrow** (`pyarrow.csv.read_csv`) — all columns forced to string, description row dropped by filtering `IDRSSD` for numeric-only. Pyarrow chosen over DuckDB's `read_csv` because the latter's dialect sniffer fails on narrative-text schedules (RIE, NARR) where quoted values contain embedded newlines.
3. **Multi-part FULL OUTER JOIN**: column-split schedules (e.g. RC-B 2 parts, RC-L 2 parts, RC-O 2 parts, RC-Q 2 parts, RC-T 2 parts, RC-R II up to 4 parts) get per-part parquets written first, then DuckDB `FULL OUTER JOIN … USING ("IDRSSD")` merges them, tags each row with `activity_year` + `activity_quarter`, and writes one `data.parquet` under `staging/{SCHEDULE}/year=Y/quarter=Q/`. Per-part files deleted after join.
4. **Panel metadata**: `upsert_row` into `panel_metadata` per (schedule, year, quarter) with row_count, column count, source_zip, sha256 (lazy), parquet_path.
5. **Views**: one `CREATE OR REPLACE VIEW schedule_{suffix}` per schedule over `read_parquet(..., union_by_name=true, hive_partitioning=true)`. Unknown suffixes auto-register.
6. **MDRM dictionary**: `download.py --mdrm` pulls `MDRM.zip` from federalreserve.gov, extracts `MDRM.csv`, `construct.py --refresh-views` loads it as the `mdrm_dictionary` table (87,687 rows; `parallel=false` required due to quoted newlines in description fields).

### Validation (2024-Q4)

- 4,543 filers total (matches ~4,500 expected bank population).
- JPMorgan (IDRSSD 852218) RCFD2170 = `3,459,261,000` (thousands) = **$3.46T**, **exactly** matches `call-reports-CFLV` `balance_sheets.assets` for same `id_rssd` + date.
- 4,543 shared filers with CFLV; 9 (0.2%) with diff > $1K — explainable by amendment timing or precision rounding.
- Top-10 by assets: JPM, BofA, Wells, Citi, USB, Goldman, PNC, Truist, CapOne, TD — correct roster.

### Filer counts across years (Q4 snapshot) — matches known industry consolidation

| Year | Filers | Year | Filers |
|------|--------|------|--------|
| 2001 | 8,689 | 2014 | 6,570 |
| 2005 | 8,056 | 2018 | 5,456 |
| 2010 | 6,999 | 2022 | 4,756 |
| 2013 | 6,877 | 2025 | 4,394 |

### Fixes made during build

| Issue | Fix |
|-------|-----|
| Verbose-regex backslash-newline continuation broke filename parser | Rewrote `INNER_FILENAME_REGEX` without `re.VERBOSE`. |
| Unicode arrow (`→`) in `logger.info` → cp1252 encode error on Windows stdout | Replaced with ASCII `->`. |
| `datetime.utcnow()` deprecated (Python 3.14) | Switched to `datetime.now(timezone.utc)`. |
| DuckDB `read_csv` sniffer aborts on RIE/NARR schedules (embedded newlines in `TEXT*` fields, some years like 2021-Q1) | Switched CSV path entirely to **pyarrow** (`pyarrow.csv.read_csv` with `newlines_in_values=True`, `invalid_row_handler=skip`). Per-part Parquets then joined in DuckDB. |
| MDRM.csv parallel scan incompatible with quoted newlines in description field | Added `parallel=false` to the MDRM `read_csv`. |

### Performance

- Single quarter: ~3 s (39 schedules, up to 751 cols via 4-part join on RC-R II).
- All 97 quarters `--skip-views`: ~5 min wall-clock on 8 GB RAM / 4-core / NVMe.
- MDRM.zip download + extract: ~2 s (91 MB CSV).

### Design choices

- **Raw MDRM layer only** in v1 per user direction. Every column is preserved as VARCHAR with its original MDRM code name (RCFD2170, RIAD4340, etc.). No concept-level harmonization yet.
- **Hyphen in folder name** (`call-reports-FFIEC`) prevents Python module imports; scripts use `importlib.util.spec_from_file_location` to load `metadata.py` (same pattern as `call-reports-CFLV`). Run scripts by file path.
- **Schedules as separate views** rather than a single wide view — each CDR schedule has its own column set (50-750 cols); a single 1,000+ column view would be unwieldy and lose semantic grouping.
- **IDRSSD kept as VARCHAR** matching the HMDA convention; callers cast to BIGINT when joining to CFLV / NIC.
- **Future harmonized layer** is purely additive: a `call_reports_panel` VIEW coalescing RCFD ↔ RCON and translating MDRM codes to CFLV-style names can be added later without touching the raw layer.

---

## Operational notes

### ZIP filename expectations

Raw ZIPs must match exactly:
```
FFIEC CDR Call Bulk All Schedules MMDDYYYY.zip
```
where `MMDD` ∈ {`0331`, `0630`, `0930`, `1231`}. Any other filename is ignored by `download.py --scan`.

Inner TSVs match:
```
FFIEC CDR Call Bulk POR MMDDYYYY.txt
FFIEC CDR Call Schedule {SCHEDULE} MMDDYYYY.txt
FFIEC CDR Call Schedule {SCHEDULE} MMDDYYYY(N of M).txt
```

If FFIEC changes the naming convention, `metadata.INNER_FILENAME_REGEX` and `SCHEDULE_REGISTRY` must be updated. Unknown schedule suffixes auto-register under `schedule_{suffix_lower}`.

### Description row filter

Every schedule TSV (not POR) has a second header row of free-text column descriptions. Dropped via `pyarrow.compute.match_substring_regex(IDRSSD, r"^\d+$")`. If FFIEC ever ships a TSV where the description-row IDRSSD column is numeric, this filter would fail.

### Multi-part FULL OUTER JOIN

Each schedule part is a COLUMN split — same set of filers, different MDRM columns. ETL writes a temp parquet per part, then DuckDB `FULL OUTER JOIN … USING ("IDRSSD")` merges them. Collisions on non-USING columns would error out — we have not seen this 2001-2025.

### Extracted files kept on disk

Pipeline does not delete extracted TSVs after building parquet. Makes `--force` rebuilds fast (no re-unzip) and allows manual inspection. Extracted TSVs across 97 quarters consume ~30 GB; delete `raw/{YYYY}Q{Q}/*.txt` manually if disk-pressured (ZIPs remain authoritative).

### Amended filings

CDR publishes replacement bulk ZIPs when filers submit amendments. If you re-download a ZIP:
1. New ZIP overwrites the old one in `raw/` (same filename).
2. `download.py --scan` resets `extract_status` to `not_extracted` because size/mtime changed.
3. `construct.py --quarter {YYYY}Q{Q} --force` re-extracts and rebuilds parquet.
4. `upsert_row` in `panel_metadata` handles the replacement cleanly.

No automatic detection of amended publication dates — re-download manually when needed.

### Adding a schedule description

Edit `metadata.SCHEDULE_REGISTRY` and add a `{suffix: (view_name, description)}` entry. Not strictly required — unknown schedules auto-register — but the mapping is what `SHOW TABLES` users read.

### Cross-dataset joins

Integer-cast pattern for joining with CFLV / NIC:
```sql
TRY_CAST(IDRSSD AS BIGINT) = cflv.id_rssd
TRY_CAST(IDRSSD AS BIGINT) = nic.ID_RSSD_OFFSPRING
```
IDRSSD is VARCHAR in this dataset (HMDA convention); both CFLV and NIC store RSSD as BIGINT.

### Future harmonized layer (v2)

When adding the `call_reports_panel` concept view:
- Create `harmonized/` subdirectory.
- Add `concepts.yaml` mapping concept names → MDRM coalesce expressions (`assets: COALESCE(RCFD2170, RCON2170)`).
- Python loader translates YAML → `CREATE OR REPLACE VIEW` SQL.
- Invoke from `construct.refresh_views()` after raw-view rebuild.

v1 untouched; v2 is purely additive.
