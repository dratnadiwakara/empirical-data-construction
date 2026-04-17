# CFLV Call Reports — Variable Documentation

**Source:** Correia, Fermin, Luck, Verner (2025). *A Long-Run History of Bank Balance Sheets and Income Statements.*
Federal Reserve Bank of New York Liberty Street Economics, December 22, 2025.
[Article and data download](https://libertystreeteconomics.newyorkfed.org/2025/12/a-long-run-history-of-bank-balance-sheets-and-income-statements/)

**Coverage:** 1959-Q4 through 2025-Q1 (approx.), ~2.5M quarterly observations, ~24K unique banks.

**Monetary unit:** All financial variables in **thousands of USD** unless noted.

**Date convention:** `date` column stores the **last day of the quarter** as a DATE type:
Q1 → March 31, Q2 → June 30, Q3 → September 30, Q4 → December 31.
(Source Stata files encode dates as first-day-of-quarter; `construct.py` converts on ingest.)

**Income statement convention:** All `ytd*` variables are **year-to-date cumulative**:
Q1 = Jan–Mar total, Q2 = Jan–Jun total, Q3 = Jan–Sep total, Q4 = Jan–Dec total (full year).
`num_employees` is the **only** non-YTD income statement variable (point-in-time headcount).

**MDRM series prefixes:**
| Prefix | Meaning |
|--------|---------|
| RCFD | Foreign and Domestic offices combined |
| RCON | Domestic offices only |
| RCFN | Foreign offices only (FFIEC 031 large-bank filers only) |
| RIAD | Income/expense (1969–present) |
| IADX | Historical income/expense (1960–1968) |

**FFIEC form types:**
- **031** — Large banks with foreign offices (RCFN* fields populated)
- **041/051** — Domestic-only banks (RCFN* fields are NULL)

MDRM definitions: [FFIEC MDRM Viewer](https://www.ffiec.gov/nicpubweb/content/FFIEC_PUBLIC.aspx)

---

## Identifiers and Metadata Variables

| Variable | Description | Unit | Notes |
|----------|-------------|------|-------|
| `id_rssd` | Federal Reserve RSSD ID (primary key) | identifier | Unique bank identifier, persistent over time |
| `date` | Quarter-end date | DATE | Mar 31 / Jun 30 / Sep 30 / Dec 31 |
| `id_rssd_hd_off` | RSSD ID of Head Office | identifier | |
| `id_fdic_cert` | FDIC Certificate ID | identifier | |
| `id_occ` | OCC Charter ID | identifier | |
| `id_cusip` | CUSIP ID (6-character) | identifier | |
| `id_thrift` | OTS docket number | identifier | Thrift institutions only |
| `id_aba_prim` | Primary ABA routing number | identifier | |
| `id_tax` | Tax ID | identifier | |
| `id_lei` | Legal Entity Identifier | identifier | |
| `nm_lgl` | Legal name | text | |
| `nm_short` | Short name | text | |
| `city` | City/town | text | |
| `state_abbr_nm` | State abbreviation | text | |
| `state_cd` | Physical state FIPS code | code | |
| `county_cd` | County FIPS code | code | |
| `zip_cd` | Zip code | code | |
| `street_line1` | Physical street address | text | |
| `cntry_nm` | Country name | text | |
| `cntry_cd` | Country code (U.S. Treasury classification) | code | |
| `dist_frs` | Federal Reserve District code | code | 1–12 |
| `ent_type_cd` | Entity type code | code | 60+ categories |
| `entity_type` | Entity type (human-readable) | text | |
| `chtr_type_cd` | Charter type code | code | |
| `act_prim_cd` | Primary activity code (NAICS) | code | |
| `dt_open` | Date of opening | date | YYYYMMDD in source |
| `reason_term_cd` | Reason for termination | code | NULL if still active |
| `reg_hh_1_id` | Regulatory High Holder RSSD ID | identifier | |
| `fin_hh_id` | Financial High Holder RSSD ID | identifier | |
| `reg_dh_1_id` | Regulatory Direct Holder RSSD ID | identifier | |
| `fgn_call_fam_id` | Foreign Call Family RSSD ID | identifier | Foreign bank branches |
| `fgn_call_cntry_cd` | Foreign Call Family Country Code | code | |

---

## Balance Sheet Variables

### Core Assets

| Variable | Description | MDRM Code(s) | Valid Period | Notes |
|----------|-------------|--------------|-------------|-------|
| `assets` | Total Assets | RCFD 2170 | 1959-Q4 onward | |
| `cash` | Cash and balances due from depository institutions | RCFD 0010; RCFD 0081+0071 | 0010: pre-1984Q1; 0081+0071: 1984Q1+ | Imputed missing in Q1/Q3 1973–1975 for foreign holdings |
| `securities` | Total Securities | RCON/RCFD 0400+0600+0900+0950; RCFD 1754+1773 | Sum of components pre-1994; HTM+AFS from 1994Q1+ | Post-1994 = `htmsec_ac + afssec_fv` |
| `htmsec_ac` | HTM Securities at Amortized Cost | RCFD 1754 | 1994Q1 onward | |
| `afssec_fv` | AFS Securities at Fair Value | RCFD 1773 | 1994Q1 onward | |
| `securities_ac` | Securities at Amortized Cost (HTM+AFS) | RCFD 1754+1772 | 1994Q1 onward | |
| `ln_tot_gross` | Total Loans, gross (before allowance) | RCFD 1400; RCFD 2122+2123 | 1400: pre-1976Q1; 2122+2123: 1976Q1+ | |
| `ln_tot` | Total Loans and Leases Net of Unearned Income | RCFD 2122 | 1976Q1 onward | After unearned income but before allowance |
| `llres` | Loan Loss Reserve (Allowance for Credit Losses) | RCFD 3120; RCFD 3123 | 3120: pre-1976Q1; 3123: 1976Q1+ | |
| `trad_ass` | Trading Assets | RCFD 2146; RCFD 3545 | 2146: 1984Q1–1994Q1; 3545: 1994Q1+ | |
| `ffrepo_ass` | Fed Funds Sold + Securities Purchased Under Agreements to Resell | RCFD 1350; RCONB987+RCFDB989 | 1350: pre-2002Q1; components: 2002Q1+ | |
| `ffsold` | Federal Funds Sold | RCFD 0276; RCONB987 | 0276: 1988Q1–1997Q1; B987: 2002Q1+ | |
| `repo_purch` | Securities Purchased Under Agreement to Resell | RCFD 0277; RCFDB989 | 0277: 1988Q1–1997Q1; B989: 2002Q1+ | |
| `fixed_ass` | Total Fixed Assets | RCFD 2145 | 1959-Q4 onward | |
| `oreo` | Other Real Estate Owned | RCFD 2150 | 1959-Q4 onward | |
| `oth_assets` | Other Assets (line item, unadjusted) | RCFD 2160 | 1959-Q4 onward | |
| `oth_assets_alt` | Other Assets (adjusted for form changes) | RCON/RCFD 2160+2155; RCFD 2160+2155+2130 | See metadata.py | |

### Loans by Category

| Variable | Description | MDRM Code(s) | Valid Period | Notes |
|----------|-------------|--------------|-------------|-------|
| `ln_re` | Real Estate Loans | RCFD 1410 | 1959-Q4 onward | |
| `ln_ci` | Commercial and Industrial Loans | RCFD 1600; RCFD 1766 | 1600: pre-1984Q1; 1766: 1984Q1+ | If 1766 missing: RCFD 1763+1764 |
| `ln_cons` | Consumer Loans | RCFD 1975 | 1959-Q4 onward | |
| `ln_cc` | Credit Card Loans | RCFD 2008; RCFDB538 | 2008: 1967-Q4–2001Q1; B538: 2001Q1+ | |
| `ln_agr` | Loans to Finance Agricultural Production | RCFD 1590 | 1959-Q4 onward | |
| `ln_lease` | Lease Financing Receivables | RCFD 2165 | 1969-Q2 onward | |
| `ln_fi` | Loans to Financial Institutions | RCFD 1495 | 1959-Q4–1984Q1 | |
| `ln_dep_inst` | Loans to Depository Institutions and Acceptances | RCFD composite | 1976Q1 onward | See metadata.py for era codes; includes foreign holdings from 1978Q4 |
| `ln_dep_inst_dom` | Loans to Depository Institutions (domestic only) | RCON composite | 1976Q1 onward | Domestic offices only; avoids 1978Q4 jump |
| `ln_oth` | Other Loans (non-dep fin inst + carrying securities) | RCFD 1545+2080; RCFD 1563 | 1563: 1984Q1+ | Multiple fallbacks; see metadata.py |
| `npl_tot` | Non-Performing Loans (nonaccrual + 90+ days past due) | RCFD 1403+1407 | 1982-Q4 onward | |

### Core Liabilities and Equity

| Variable | Description | MDRM Code(s) | Valid Period | Notes |
|----------|-------------|--------------|-------------|-------|
| `deposits` | Total Deposits | RCFD 2200 | 1959-Q4 onward | |
| `domestic_dep` | Deposits in Domestic Offices | RCON 2200 | 1959-Q4 onward | |
| `foreign_dep` | Deposits in Foreign Offices | RCFN 2200 | 1969-Q2 onward | **FFIEC 031 only** (NULL for 041/051 filers) |
| `demand_deposits` | Total Demand Deposits | RCFD 2210 | 1959-Q4 onward | |
| `time_deposits` | Total Time Deposits | RCON composite | See metadata.py | Era-adjusted series |
| `ffrepo_liab` | Fed Funds Purchased + Securities Sold Under Agreements to Repurchase | RCFD 2800; RCONB993+RCFDB995 | 2800: pre-2002Q1; components: 2002Q1+ | |
| `ffpurch` | Federal Funds Purchased | RCFD 0278; RCONB993 | 0278: 1988Q1–1997Q1; B993: 2002Q1+ | |
| `repo_sold` | Securities Sold Under Agreements to Repurchase | RCFD 0279; RCFDB995 | 0279: 1988Q1–1997Q1; B995: 2002Q1+ | |
| `trad_liab` | Total Trading Liabilities | RCFD 3548 | 1994Q1 onward | |
| `othbor_liab` | Other Borrowed Money | Multiple RCFD codes | 1959-Q4 onward | Complex era series; see metadata.py |
| `subdebt` | Subordinated Notes and Debentures | RCFD 3200 | 1959-Q4 onward | |
| `liab_oth` | Other Liabilities | RCFD composite | 1959-Q4 onward | See metadata.py |
| `liab_tot` | Total Liabilities (minority-interest adjusted) | RCFD composite | 1959-Q4 onward | See metadata.py for era adjustments |
| `liab_tot_unadj` | Total Liabilities (unadjusted) | RCFD 2950; RCFD 2948 | | |
| `minorint` | Minority Interest in Consolidated Subsidiaries | RCFD/RCON 3000 | 1969-Q2 onward | |
| `equity` | Total Equity | RCFD 3210 | 1959-Q4 onward | |
| `pref_stock` | Preferred Stock | RCFD 3220; RCFD 3838 | 3220: pre-1997Q1; 3838: 1997Q1+ | |
| `comm_stock` | Common Stock | RCFD 3230 | 1959-Q4 onward | |
| `surplus` | Surplus | RCFD 3240; RCFD 3839 | 3240: pre-1990Q1; 3839: 1990Q1+ | |
| `retain_earn` | Retained Earnings / Undivided Profits | RCFD composite; RCFD 3632 | 3632: 1989Q1+ | |

### Deposit Detail

| Variable | Description | MDRM Code(s) | Valid Period | Notes |
|----------|-------------|--------------|-------------|-------|
| `dom_deposit_ib` | Interest-Bearing Domestic Deposits | RCON 6636 | 1978-Q4 onward | |
| `dom_deposit_nib` | Noninterest-Bearing Domestic Deposits | RCON 6631 | 1984Q1 onward | |
| `for_deposit_ib` | Interest-Bearing Foreign Deposits | RCFN 6636 | 1978-Q4 onward | **031 only** |
| `for_deposit_nib` | Noninterest-Bearing Foreign Deposits | RCFN 6631 | 1984Q1 onward | **031 only** |
| `brokered_dep` | Brokered Deposits | RCON 2365 | 1983-Q3 onward | |
| `brokered_dep_lt100k` | Brokered Deposits < $100K denomination | RCON 2343 | 1984Q1–2017Q1 | |
| `brokered_dep_ge100k` | Brokered Deposits ≥ $100K denomination | RCON 2344 | 1984Q1–2017Q1 | |
| `insured_deposits` | Insured Deposits (line item) | RCON 2702; RCONF049+F045 | 2702: 1983–2006Q2; F codes: 2006Q2+ | Q2 only for all banks 1983–1990; quarterly from 1991 |
| `insured_deposits_alt` | Insured Deposits (alternate, account-based) | Derived | 1983-Q2 onward | Corrects undercounting; multiplier changed from $100K to $250K at 2009-Q3 |
| `transaction_dep` | Total Transaction Accounts (Domestic) | RCON 2215 | 1984Q1 onward | |
| `nontransaction_dep` | Total Nontransaction Accounts (incl. MMDAs) | RCON 2385 | 1984-Q3 onward | |
| `nontransaction_sav_dep` | Nontransaction Savings Deposits | RCON 2389 | 1984Q1 onward | |
| `time_sav_dep` | Total Time and Savings Deposits | RCFD 2350 | 1959-Q4 onward | |
| `tot_sav_dep` | Total Savings Deposits (derived) | `time_sav_dep - time_deposits` | 1961-Q1 onward | |
| `uninsured_time_dep` | Uninsured Time Deposits | RCON 2604; RCONJ474 | 2604: 1974–2010Q1; J474: 2010Q1+ | |
| `time_dep_ge100k` | Time Deposits ≥ $100K | RCON 2604 | 1974-Q2 onward | |
| `time_dep_lt100k` | Time Deposits < $100K | RCON 6648 | 1984Q1 onward | |
| `time_cd_ge100k` | Time CDs ≥ $100K (domestic) | RCON 6645 | 1976Q1–1997Q2 | |
| `time_dep_gt250k` | Time Deposits > $250K | RCONJ474 | 2010Q1 onward | |
| `time_ge100k_le250k` | Time Deposits $100K–$250K | RCONJ473 | 2010Q1 onward | |
| `time_dep_le250k` | Time Deposits ≤ $250K | RCONJ474+RCON6648 | 2010Q1 onward | |

### Quarterly Averages

| Variable | Description | MDRM Code(s) | Valid Period |
|----------|-------------|--------------|-------------|
| `qtr_avg_assets` | Quarterly Average Total Assets | RCFD 3368 | 1978-Q4 onward |
| `qtr_avg_ln_tot` | Quarterly Average Total Loans (Domestic) | RCON 3360 | 1969-Q2 onward |
| `qtr_avg_ln_re` | Quarterly Average Real Estate Loans | RCON 3385 | 1984Q1 onward |
| `qtr_avg_ln_rre` | Quarterly Average 1–4 Family Residential Loans | RCON 3465 | 1986Q1–1989Q1, 2008Q1+ |
| `qtr_avg_ln_othre` | Quarterly Average Other RE Loans | RCON 3466 | 1986Q1–1989Q1, 2008Q1+ |
| `qtr_avg_ln_ci` | Quarterly Average C&I Loans | RCON 3387 | 1984Q1 onward |
| `qtr_avg_ln_agr` | Quarterly Average Agricultural Loans | RCON 3386 | 1984Q1 onward |
| `qtr_avg_ln_cc` | Quarterly Average Credit Cards | RCONB561 | 2001Q1 onward |
| `qtr_avg_ln_cons` | Quarterly Average Consumer Loans | RCONB561+B562 | 2001Q1 onward |
| `qtr_avg_ln_othcons` | Quarterly Average Other Individual Loans | RCONB562 | 2001Q1 onward |
| `qtr_avg_ln_fgn` | Quarterly Average Loans in Foreign Offices | RCFN 3360 | 1978-Q4 onward |
| `qtr_avg_lease` | Quarterly Average Lease Financing | RCFD 3484 | 1987Q1 onward |
| `qtr_avg_securities` | Quarterly Average Securities | RCFD composite | 1984Q1 onward |
| `qtr_avg_ust_sec` | Quarterly Average U.S. Treasury/Agency Securities | RCFDB558 | 2001Q1 onward |
| `qtr_avg_mbs` | Quarterly Average Mortgage-Backed Securities | RCFDB559 | 2001Q1 onward |
| `qtr_avg_oth_sec` | Quarterly Average Other Securities | RCFDB560 | 2001Q1 onward |
| `qtr_avg_trad_ass` | Quarterly Average Trading Assets | RCFD 3401 | 1984Q1 onward |
| `qtr_avg_ffrepo_ass` | Quarterly Average Fed Funds Sold + Reverse Repos | RCFD 3365 | 1976Q1 onward |
| `qtr_avg_ib_bal_due` | Quarterly Average IB Balances Due from Dep. Inst. | RCFD 3381 | 1984Q1 onward |
| `qtr_avg_trans_dep_dom` | Quarterly Average IB Transaction Accounts | RCON 3485 | 1987Q1 onward |
| `qtr_avg_sav_dep_dom` | Quarterly Average Savings Deposits (incl. MMDAs) | RCON 3486+3487; RCONB563 | 1987Q1 onward |
| `qtr_avg_time_dep_ge100k` | Quarterly Average Time Deposits ≥ $100K | RCONA514 | 1997Q1–2017Q1 |
| `qtr_avg_time_dep_gt250k` | Quarterly Average Time Deposits > $250K | RCONHK17 | 2017Q1 onward |
| `qtr_avg_time_dep_lt100k` | Quarterly Average Time Deposits < $100K | RCONA529 | 1997Q1–2017Q1 |
| `qtr_avg_time_dep_le250k` | Quarterly Average Time Deposits ≤ $250K | RCONHK16 | 2017Q1 onward |
| `qtr_avg_time_cd_ge100k` | Quarterly Average Time CDs ≥ $100K | RCON 3345 | 1978-Q4–1997Q1 |
| `qtr_avg_fgn_dep` | Quarterly Average IB Foreign Deposits | RCFN 3404 | 1984Q1 onward |
| `qtr_avg_ffrepo_liab` | Quarterly Average Fed Funds Purchased + Repos | RCFD 3353 | 1976Q1 onward |
| `qtr_avg_othbor_liab` | Quarterly Average Other Borrowed Money | RCFD 3355 | 1976Q1 onward |

### Interest Rate Sensitivity (Maturity/Repricing Buckets)

> All available from 1997-Q2 onward (RCFD/RCON codes: A549–A575, A555–A569).

| Variable | Description | Bucket |
|----------|-------------|--------|
| `ust_sec_3mo_less` | U.S. Treasury/Gov/Other Debt Securities | ≤ 3 months |
| `ust_sec_3mo_12mo` | U.S. Treasury/Gov/Other Debt Securities | 3–12 months |
| `ust_sec_1y_3y` | U.S. Treasury/Gov/Other Debt Securities | 1–3 years |
| `ust_sec_3y_5y` | U.S. Treasury/Gov/Other Debt Securities | 3–5 years |
| `ust_sec_5y_15y` | U.S. Treasury/Gov/Other Debt Securities | 5–15 years |
| `ust_sec_over_15y` | U.S. Treasury/Gov/Other Debt Securities | > 15 years |
| `res_mbs_*` | Residential MBS (6 buckets, same structure) | |
| `other_mbs_3y_less` | Other MBS | ≤ 3 years |
| `other_mbs_3y_more` | Other MBS | > 3 years |
| `securities_*` | All securities combined (6 buckets) | |
| `securities_mat_1y_less` | Debt securities with remaining maturity ≤ 1 year | RCFDA248, 1996Q1+ |
| `res_loans_*` | Residential loans (6 buckets) | RCON A564–A569 |
| `ln_lease_*` | Loans and leases (6 buckets + `_mat_1y_less`) | RCON A570–A575+A247 |
| `time_dep_lt100k_*` | Time deposits < $100K (4 repricing + 1 maturity buckets) | 1997Q2–2017Q1 |
| `time_dep_ge100k_*` | Time deposits ≥ $100K (4 repricing buckets) | 1996Q1–2017Q1 |
| `time_dep_le250k_*` | Time deposits ≤ $250K (4 repricing + 1 maturity buckets) | 2017Q1 onward |
| `time_dep_gt250k_*` | Time deposits > $250K (4 repricing buckets) | 2017Q1 onward |

### Derivatives

| Variable | Description | MDRM Code(s) | Valid Period |
|----------|-------------|--------------|-------------|
| `tot_gna_deriv_ir` | Gross notional: interest rate derivatives (hedging, not trading) | RCFD 8725 | 1995Q1 onward |
| `tot_gna_deriv_ir_par` | Gross notional: IR derivatives (not MTM) | RCFD 8729 | 1995Q1–2001Q1 |
| `gross_hedging` | Gross Hedging = `tot_gna_deriv_ir` + `tot_gna_deriv_ir_par` | Derived | 1995Q1 onward |
| `tot_gna_ir_fixed_rate_swap` | Gross notional: fixed-rate IR swaps | RCFDA589 | 1997Q2 onward |
| `tot_swaps_int_rate` | Gross notional: all IR swap contracts | RCFD 3450 | 1985-Q2 onward |
| `tot_gna_ir_float_rate_swap` | Gross notional: floating-rate IR swaps (derived) | RCFD 3450 − RCFDA589 | 1997Q2 onward |
| `net_hedging` | Net Hedging position (fixed − floating notionals) | Derived | 1997Q2 onward |
| `tot_gna_deriv_trad_ir` | Gross notional: IR derivatives held for trading | RCFDA126 | 1995Q1 onward |

---

## Income Statement Variables

> **All variables are year-to-date (YTD) cumulative** unless noted.
> Q1 value = Jan–Mar; Q2 value = Jan–Jun; Q3 value = Jan–Sep; Q4 value = full-year Jan–Dec.
> To compute a single-quarter increment: `Q2_inc = Q2_ytd − Q1_ytd` etc.
>
> **Historical codes:** IADX series (1960–1968) preceded the RIAD series (1969–present).

### Total Revenue and Expense Lines

| Variable | Description | MDRM Code(s) | Valid Period | Notes |
|----------|-------------|--------------|-------------|-------|
| `ytdint_inc` | Total Interest Income | IADX composite; RIAD4107 | RIAD4107: 1984Q1+ | |
| `ytdint_exp` | Total Interest Expense | IADX composite; RIAD4073 | RIAD4073: 1984Q1+ | |
| `ytdint_inc_net` | Net Interest Income | RIAD 4074 | 1984Q1 onward | |
| `ytdnonint_inc` | Total Noninterest Income | IADX composite; RIAD4079 | RIAD4079: 1984Q1+ | |
| `ytdnonint_exp` | Total Noninterest Expense | IADX composite; RIAD4093 | RIAD4093: 1984Q1+ | |
| `ytdoperating_inc_tot` | Total Operating Income | IADX5000; RIAD4000 | 1960-Q4 onward | |
| `ytdoperating_exp_tot` | Total Operating Expense | IADX5018; RIAD4130 | 1960-Q4 onward | |
| `ytdoperating_exp_tot_adj` | Total Operating Expense (minus minority interest and LLP) | Derived | 1960-Q4 onward | |

### Interest Income Detail

| Variable | Description | MDRM Code(s) | Valid Period |
|----------|-------------|--------------|-------------|
| `ytdint_inc_sec` | Interest and Dividend Income on Securities | IADX 5002+5004; RIAD composite | 1960-Q4 onward |
| `ytdint_inc_sec_ust` | Interest on U.S. Treasury and Agency Securities | RIADB488 | 2001Q1 onward |
| `ytdint_inc_sec_mbs` | Interest on Mortgage-Backed Securities | RIADB489 | 2001Q1 onward |
| `ytdint_inc_sec_oth` | Interest on All Other Securities | RIAD composite | 1969-Q4 onward |
| `ytdint_inc_ln` | Interest Income on Loans | IADX5006; RIAD4010 | 1960-Q4 onward |
| `ytdint_inc_ln_re` | Interest on Real Estate Loans | RIAD4011 | 1984Q1 onward |
| `ytdint_inc_ln_rre` | Interest on 1–4 Family Residential Loans | RIAD4435 | 1986Q1–1989Q1, 2008Q1+ |
| `ytdint_inc_ln_othre` | Interest on Other RE Loans | RIAD4436 | 1986Q1–1989Q1, 2008Q1+ |
| `ytdint_inc_ln_ci` | Interest on C&I Loans | RIAD4012 | 1984Q1 onward |
| `ytdint_inc_ln_agr` | Interest on Agricultural Loans | RIAD4024 | 1984Q1 onward |
| `ytdint_inc_ln_cc` | Interest on Credit Cards | RIAD4054; RIADB485 | 1984Q1 onward |
| `ytdint_inc_ln_othcons` | Interest on Other Consumer Loans | RIADB486 | 2001Q1 onward |
| `ytdint_inc_ln_indiv` | Interest on All Individual Loans | RIAD composite | 1984Q1 onward |
| `ytdint_inc_ln_fgn` | Interest on Foreign Office Loans | RIAD4059 | 1984Q1 onward |
| `ytdint_inc_ffrepo` | Interest on Fed Funds Sold and Reverse Repos | RIAD4020 | 1969-Q4 onward |
| `ytdint_inc_lease` | Interest on Lease Financing | RIAD4065 | 1976Q1 onward |
| `ytdint_inc_ibb` | Interest on Balances Due from Dep. Institutions | RIAD4115 | 1976Q1 onward |

### Interest Expense Detail

| Variable | Description | MDRM Code(s) | Valid Period |
|----------|-------------|--------------|-------------|
| `ytdint_exp_dep` | Interest on Deposits | IADX5032; RIAD4170; composite | 1960-Q4 onward |
| `ytdint_exp_ffrepo` | Interest on Fed Funds Purchased and Repos | RIAD4180 | 1969-Q4 onward |
| `ytdint_exp_othbor` | Interest on Other Borrowed Money | IADX5034; RIAD4190 | 1960-Q4–1984Q1 |
| `ytdint_exp_trad_othbor` | Interest on Trading Liabilities + Other Borrowed Money | RIAD4185 | 1978-Q4 onward |
| `ytdint_exp_subdebt` | Interest on Subordinated Notes | RIAD4200 | 1969-Q4 onward |
| `ytdint_exp_time_dep` | Interest on Time Deposits | RIAD composite | 1987Q1 onward |
| `ytdint_exp_time_ge100k_dom` | Interest on Time Deposits ≥ $100K (domestic) | RIAD4174; RIADA517 | 1976Q1–2017Q1 |
| `ytdint_exp_time_lt100k_dom` | Interest on Time Deposits < $100K (domestic) | RIADA518 | 1997Q1–2017Q1 |
| `ytdint_exp_time_le250k_dom` | Interest on Time Deposits ≤ $250K (domestic) | RIADHK03 | 2017Q1 onward |
| `ytdint_exp_time_gt250k_dom` | Interest on Time Deposits > $250K (domestic) | RIADHK04 | 2017Q1 onward |
| `ytdint_exp_trans_dep_dom` | Interest on Transaction Account Deposits | RIAD4508 | 1987Q1 onward |
| `ytdint_exp_savings_dep_dom` | Interest on Savings Deposits incl. MMDAs | RIAD4509+4511; RIAD0093 | 1987Q1 onward |
| `ytdint_exp_fgn` | Interest on Foreign Deposits | RIAD4172 | 1976Q1 onward |

### Noninterest Income and Expense Detail

| Variable | Description | MDRM Code(s) | Valid Period |
|----------|-------------|--------------|-------------|
| `ytdfiduc_inc` | Fiduciary Activities Income | IADX5014; RIAD4070 | 1960-Q4 onward |
| `ytdnonint_inc_srv_chrg_dep` | Service Charges on Deposits | IADX5010; RIAD4080 | 1960-Q4 onward |
| `ytdoth_srv_chrg` | Other Service Charges, Commissions, and Fees | IADX5012; RIAD4090 | 1960-Q4–1984Q1 |
| `ytdoth_operating_inc` | Other Operating Income | IADX5016; RIAD composite | 1960-Q4 onward |
| `ytdoth_operating_inc_adj` | Other Operating Income (time-consistent adjusted) | Derived | 1960-Q4 onward |
| `ytdtradrev_inc` | Trading Revenue | RIAD4077+4075; RIADA220 | 1984Q1 onward |
| `ytdnonint_exp_comp` | Salaries and Employee Benefits | IADX composite; RIAD4135 | 1960-Q4 onward |
| `ytdnonint_exp_fass` | Premises and Fixed Asset Expenses | IADX composite; RIAD4217 | 1960-Q4 onward |
| `ytdoth_operating_exp` | Other Operating Expenses | IADX5066; RIAD4092 | 1960-Q4 onward |

### Profitability and Provisions

| Variable | Description | MDRM Code(s) | Valid Period | Notes |
|----------|-------------|--------------|-------------|-------|
| `ytdllprov` | Loan Loss Provisions | RIAD4230 | 1969-Q4 onward | |
| `ytdnetinc` | Net Income | IADX5106; RIAD4340 | 1960-Q4 onward | **Key research variable** |
| `ytdnet_operating_earn` | Net Operating Earnings (before taxes and securities gains) | IADX5068; RIAD4250; RIAD4301 | 1960-Q4 onward | |
| `ytdop_inc_tot_1960` | Total Operating Income (1960 time-consistent) | Derived | 1960-Q4 onward | Holds 1960 income statement structure constant |
| `ytdop_exp_tot_1960` | Total Operating Expense (1960 time-consistent) | Derived | 1960-Q4 onward | |
| `ytdnet_op_earn_1960` | Net Operating Earnings (1960 time-consistent) | `ytdop_inc_tot_1960 - ytdop_exp_tot_1960` | 1960-Q4 onward | |
| `ytdinc_before_sec_gain` | Income Before Securities Gains/Losses (after taxes) | IADX composite; RIAD composite | 1960-Q4 onward | |
| `ytdinc_before_disc_op` | Income Before Discontinued Operations | RIAD4300 | 1969-Q4 onward | |
| `ytdsecur_inc` | Gains and Losses on Securities | RIAD4280; RIAD4091 | 1969-Q4 onward | |
| `ytdsec_net` | Securities Gains (Losses), net | RIAD4290; RIAD4091-4219 | 1969-Q4 onward | |
| `ytdrecov_tot` | Recoveries, transfers from valuation reserves, and profits | IADX5070 | 1960-Q4–1968 | Reported instead of LLP prior to 1969 |
| `ytdxoff_tot` | Losses, charge-offs, and transfers to valuation reserves | IADX5084 | 1960-Q4–1968 | Reported instead of LLP prior to 1969 |
| `ytdextra_inc_gross` | Extraordinary Items, gross of taxes | RIAD4310 | 1969-Q4–1997Q1 | |
| `ytdextra_inc` | Extraordinary Items, net of taxes | RIAD4320 | 1969-Q4–2016-Q3 | |
| `ytddisc_op` | Discontinued Operations, net of taxes | RIADFT28 | 2016-Q3 onward | |
| `ytdminor_int` | Minority Interest | RIAD4330; RIAD4484; RIADG103 | 1969-Q4 onward | |

### Taxes and Dividends

| Variable | Description | MDRM Code(s) | Valid Period |
|----------|-------------|--------------|-------------|
| `ytdinc_taxes` | Income Taxes (before extraordinary items) | RIAD4302 | 1984Q1 onward |
| `ytdinc_taxes_net` | Taxes on Net Income after losses/gains | IADX5100 | 1960-Q4–1968 |
| `ytdinc_taxes_netsec` | Income Taxes excluding taxes on securities gains | RIAD4260 | 1969-Q4–1984Q1 |
| `ytdinc_taxes_sec` | Income Taxes on Securities Gains | RIAD4285 | 1976Q1–1984Q1 |
| `ytdcommdividend` | Cash Dividends Declared on Common Stock | RIAD4460 | 1969-Q4 onward |

### Headcount

| Variable | Description | MDRM Code(s) | Valid Period | Notes |
|----------|-------------|--------------|-------------|-------|
| `num_employees` | Full-time equivalent employees | IADX5022+5026; RIAD4150 | 1960-Q4 onward | **Not YTD** — point-in-time at report date. OCR outliers in 1967/1968 adjusted. |

---

## Sample Queries

```sql
-- Large bank balance sheet trend (Bank of America, RSSD 480228)
SELECT date, assets, deposits, equity, ln_tot
FROM balance_sheets
WHERE id_rssd = 480228
ORDER BY date DESC
LIMIT 20;

-- Banking system total assets over time (annual Q4 snapshots)
SELECT YEAR(date) AS year, SUM(assets) / 1e9 AS total_assets_bn
FROM balance_sheets
WHERE MONTH(date) = 12
GROUP BY 1 ORDER BY 1;

-- Net interest margin proxy (annual)
SELECT b.date, b.id_rssd,
       (i.ytdint_inc - i.ytdint_exp) / b.qtr_avg_assets AS nim_approx
FROM balance_sheets b
JOIN income_statements i USING (id_rssd, date)
WHERE MONTH(b.date) = 12   -- Q4 = full-year YTD income
  AND b.qtr_avg_assets > 0;

-- Variable documentation lookup
SELECT variable_name, description, mdrm_codes, unit, notes
FROM panel_metadata
WHERE source_table = 'balance_sheets'
  AND variable_name LIKE 'ln_%';
```
