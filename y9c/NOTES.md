# Y-9C — Build Notes

Operational log + agent memory. Reference material in `README.md`.

---

## Build provenance

- **Source**: FFIEC NIC FinancialDataDownload (`?selectedyear={YYYY}`). Manual download — page is interactive ASP.NET, no scraper.
- **Coverage in this build**: 2000-Q1 → 2025-Q4 (104 quarters), plus partial 2026-Q1 (18 filers, preliminary).
- **Earlier years (1986-Q3 → 1999-Q4)**: skipped per user direction. FFIEC NIC `selectedyear` floor is 2000. Pre-2000 data lives in Chicago Fed archive (different filename convention `bhcfYYQQ.zip`); not loaded here.
- **Filename observed**: `BHCF{YYYYMMDD}.ZIP` (uppercase ext on disk). Inner: single TXT, caret-delimited.

## File format

- Delimiter: `^` (caret) for **all** observed years 2000-2026. Pipeline still sniffs (`,` vs `^`) and stores choice on `panel_metadata.delimiter`.
- All columns ingested as VARCHAR (`all_varchar=true`). MDRM convention; arithmetic via `TRY_CAST(... AS DOUBLE)`.
- 2024Q4 file has 2229 columns; 2026Q1 has 2252 (column drift across quarters). `union_by_name=true` on `y9c_raw` view absorbs drift.

## Mixed-form source file

The bulk file mixes three forms in one CSV:

| Form | Prefix | Filers (Q4 2024) |
|------|--------|------------------|
| Y-9C consolidated | `BHCK*` | ~382 |
| Y-9LP parent-only | `BHCP*` | ~70 (overlap with Y-9C) |
| Y-9SP semi-annual small | `BHSP*` | ~3300 (Q2/Q4 only) |

Q1/Q3 raw row count ~450 (Y-9C only). Q2/Q4 raw ~3800 (Y-9C + Y-9SP). Harmonized views filter via `BHCK2170 IS NOT NULL` to retain Y-9C consolidated only. Total `y9c_raw` rows: 325,114; `bs_panel_y9c` rows: 107,509.

## Prefix conventions discovered

Code 4-char numeric (e.g. 2170 = total assets) appears under multiple BHC* prefixes per filer per quarter, depending on year and what cell type it represents:

- `BHCK` — consolidated, the canonical Y-9C prefix (always present for key BS/IS items 2000-2025)
- `BHDM` — domestic offices only
- `BHFN` — foreign offices only
- `BHCP` — parent-only (Y-9LP filers)
- `BHSP` — small/savings (Y-9SP filers)
- `BHBC`, `BHCT`, `BHCA`, `BHC0`, `BHC2`, `BHC5`, `BHC9`, `BHCE`, `BHCX`, `BHPA` — additional consolidated/segment variants in some quarters

Y-9C-consolidated **total deposits** has no `BHCK2200` (parent-only `BHCP2200` exists). Computed in harmonized layer as `BHDM6631+BHDM6636+BHFN6631+BHFN6636` (NIB + IB across domestic + foreign).

Equity: `BHCK3210` pre-2009Q1, `BHCKG105` from 2009Q1 onward (FAS 160 / non-controlling interests). Concept handles via COALESCE.

Net income: most quarters `BHCK4340`; some recent quarters also populate `BHBC4340`. Concept COALESCEs both.

## Column-existence guard

`harmonized/views.py::_strip_missing_cols` rewrites concept SQL — replaces any `y.<COL>` reference with `NULL` literal when COL not in `y9c_raw` schema. Lets you add a new concept tentatively without crashing the view if you guessed the prefix wrong. Surface any all-NULL columns via `harmonized_metadata_y9c` audit.

## Filer-count step changes (validation cross-check)

Drop in filer count tracks Y-9C threshold history:

| Period | Threshold | Quarterly Y-9C filers |
|--------|-----------|------------------------|
| 2000-2005 | $150MM | 1700-2300 |
| 2006-2014 | $500MM | 970-1180 |
| 2015-2017 | $1B | 640-680 |
| 2018Q3+ | $3B | 350-390 |

2018Q3 jump (655 → 371) confirms $1B→$3B threshold raise on schedule.

## Anomalies (do not retry)

- **2024Q4 system total = $23.17T**, below 2024Q3 ($27.64T) and 2025Q1 ($28.18T). Y-9C/FFIEC subsidiary ratio drops to 0.96 (only year < 1.0). Source-file issue, not pipeline. Time-series queries for individual BHCs unaffected.
- **2026Q1**: 18 filers, $0.09T. Preliminary release. Will fill in over time.
- **Pre-2026 file**: shows `BHCF20260331.ZIP` present in raw/ — user pre-placed beyond 2025Q4. Loaded as-is.

## Validation

`validate.py` runs `check_row_counts`, `check_null_rates`, `check_plausibility`. Last run: 3 warnings (2024Q4 ratio anomaly + 2004-2005 ratio 1.73-1.75 above [0.5, 1.6] band — explained by pre-2006 lower threshold). All other quarters pass.

## Naming gotcha

Module file originally named `inspect.py` shadows stdlib `inspect` and breaks `duckdb` import (`importlib.metadata` → `inspect.getmro`). Renamed to `validate.py`. Don't rename back.

## Future extensions

- **Pre-2000 backfill**: Chicago Fed archive (`bhcfYYQQ.zip`, comma-delimited). Would need second filename regex + delimiter branch in `download.py`/`construct.py`. User opted out; revisit only if research needs pre-2000.
- **More BS/IS concepts**: extend `BS_CONCEPTS_Y9C`/`IS_CONCEPTS_Y9C` in `harmonized/concepts.py`, then `python y9c/construct.py --refresh-views`. ~2-second rebuild.
- **MDRM dictionary attach**: `call-reports-FFIEC/mdrm/MDRM.csv` covers BHCK/BHCP/BHSP. Could ATTACH FFIEC DuckDB and reference `mdrm_dictionary` table directly rather than duplicating.
- **Y-9LP / Y-9SP harmonized views**: same source file already loaded. Mirror `bs_panel_y9c` with `BHCP*`/`BHSP*` prefix. Not needed for current research.

## Build commands

```bash
source C:/envs/.basic_venv/Scripts/activate
python y9c/download.py --scan
python y9c/construct.py --all
python y9c/validate.py
```

Total build time: ~3 minutes for 105 quarters on local machine.
