"""
CFLV Call Reports metadata: variable definitions, MDRM mappings, era notes.

Source: historical_call_data_dictionary.xlsx (Jan 2026 release)
        Correia, Fermin, Luck, Verner — FRBNY Liberty Street, Dec 2025.

All financial variables are in thousands of USD unless noted.
Income statement variables are year-to-date (YTD) cumulative:
  Q1 = Jan-Mar total, Q2 = Jan-Jun total, Q3 = Jan-Sep total, Q4 = Jan-Dec total.

Date convention in DuckDB: quarter-end DATE (Mar 31, Jun 30, Sep 30, Dec 31).
Source files store dates as first-day-of-quarter; construct.py converts on ingest.
"""

# Each variable entry:
#   description : str
#   unit        : str
#   mdrm        : list of {"code": str, "period": str, "notes": str|None}
#   notes       : str  (data quality, derivation, or coverage notes)

BALANCE_SHEET_VARS: dict[str, dict] = {
    "cash": {
        "description": "Cash and balances due from depository institutions",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD0010", "period": "dt >= 19591231 & dt < 19840331", "notes": "Imputed as missing in Q1 and Q3 for 1973-1975 to account for foreign holdings."},
            {"code": "RCFD0081+RCFD0071", "period": "dt >= 19840331", "notes": None},
        ],
        "notes": "Granular loan variables reported semiannually between 1973-1975.",
    },
    "securities": {
        "description": "Total Securities",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCON0400+RCON0600+RCON0900+RCON0950", "period": "dt >= 19591231 & dt < 19690630", "notes": None},
            {"code": "RCFD0400+RCFD0600+RCFD0900+RCFD0950", "period": "dt >= 19690630 & dt < 19940331", "notes": None},
            {"code": "RCFD0390", "period": "dt >= 19830930 & dt < 19940331", "notes": "Alternate aggregate: 0390 = 0400+0600+0900+0950; used when components missing."},
            {"code": "RCFD1754+RCFD1773", "period": "dt >= 19940331", "notes": None},
        ],
        "notes": "Post-1994: sum of HTM (amortized cost) + AFS (fair value).",
    },
    "ln_tot_gross": {
        "description": "Total Loans, gross",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD1400", "period": "dt >= 19591231 & dt < 19760331", "notes": None},
            {"code": "RCFD2122+RCFD2123", "period": "dt >= 19760331", "notes": None},
        ],
        "notes": None,
    },
    "ln_tot": {
        "description": "Total Loans and Leases Net of Unearned Income",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD2122", "period": "dt >= 19760331", "notes": None},
        ],
        "notes": None,
    },
    "llres": {
        "description": "Loan Loss Reserve (Allowance for Credit Losses)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD3120", "period": "dt >= 19591231 & dt < 19760331", "notes": None},
            {"code": "RCFD3123", "period": "dt >= 19760331", "notes": None},
        ],
        "notes": None,
    },
    "trad_ass": {
        "description": "Trading Assets",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD2146", "period": "dt >= 19840331 & dt < 19940331", "notes": None},
            {"code": "RCFD3545", "period": "dt >= 19940331", "notes": None},
        ],
        "notes": None,
    },
    "ffrepo_ass": {
        "description": "Federal Funds Sold, Securities Purchased under Agreements to Resell",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD1350", "period": "dt >= 19651231 & dt < 20020331", "notes": None},
            {"code": "RCONB987+RCFDB989", "period": "dt >= 20020331", "notes": None},
        ],
        "notes": None,
    },
    "ffsold": {
        "description": "Federal Funds Sold",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD0276", "period": "dt >= 19880331 & dt < 19970331", "notes": None},
            {"code": "RCONB987", "period": "dt >= 20020331", "notes": None},
        ],
        "notes": None,
    },
    "repo_purch": {
        "description": "Securities Purchased Under Agreement to Resell",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD0277", "period": "dt >= 19880331 & dt < 19970331", "notes": None},
            {"code": "RCFDB989", "period": "dt >= 20020331", "notes": None},
        ],
        "notes": None,
    },
    "fixed_ass": {
        "description": "Total Fixed Assets",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD2145", "period": "dt >= 19591231", "notes": None},
        ],
        "notes": None,
    },
    "oreo": {
        "description": "Other Real Estate Owned",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD2150", "period": "dt >= 19591231", "notes": None},
        ],
        "notes": None,
    },
    "oth_assets": {
        "description": "Other assets (line item, without adjustments)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD2160", "period": "dt >= 19591231", "notes": None},
        ],
        "notes": None,
    },
    "oth_assets_alt": {
        "description": "Other assets (adjusted to include additional other-asset components across form changes)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCON2160+RCON2155", "period": "dt >= 19591231 & dt < 19651231", "notes": None},
            {"code": "RCFD2160+RCFD2155+RCFD2130", "period": "dt >= 19651231", "notes": None},
        ],
        "notes": None,
    },
    "assets": {
        "description": "Total Assets",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD2170", "period": "dt >= 19591231", "notes": None},
        ],
        "notes": None,
    },
    "demand_deposits": {
        "description": "Total Demand Deposits",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD2210", "period": "dt >= 19591231", "notes": None},
        ],
        "notes": None,
    },
    "time_deposits": {
        "description": "Total Time Deposits",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCON2514", "period": "dt >= 19610331 & dt < 19840331", "notes": None},
            {"code": "RCON2604+RCON6648", "period": "dt >= 19840331 & dt < 20100331", "notes": None},
            {"code": "RCONJ473+RCONJ474+RCON6648", "period": "dt >= 20100331", "notes": None},
        ],
        "notes": None,
    },
    "deposits": {
        "description": "Total Deposits",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD2200", "period": "dt >= 19591231", "notes": None},
        ],
        "notes": None,
    },
    "ffrepo_liab": {
        "description": "Federal Funds Purchased, Securities Sold Under Agreements to Repurchase",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD2800", "period": "dt >= 19651231 & dt < 20020331", "notes": None},
            {"code": "RCONB993+RCFDB995", "period": "dt >= 20020331", "notes": None},
        ],
        "notes": None,
    },
    "ffpurch": {
        "description": "Federal Funds Purchased",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD0278", "period": "dt >= 19880331 & dt < 19970331", "notes": None},
            {"code": "RCONB993", "period": "dt >= 20020331", "notes": None},
        ],
        "notes": None,
    },
    "repo_sold": {
        "description": "Securities Sold Under Agreements to Repurchase",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD0279", "period": "dt >= 19880331 & dt < 19970331", "notes": None},
            {"code": "RCFDB995", "period": "dt >= 20020331", "notes": None},
        ],
        "notes": None,
    },
    "trad_liab": {
        "description": "Total Trading Liabilities",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD3548", "period": "dt >= 19940331", "notes": None},
        ],
        "notes": None,
    },
    "othbor_liab": {
        "description": "Other Borrowed Money",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCON2850+RCON2910", "period": "dt >= 19591231 & dt < 19690630", "notes": None},
            {"code": "RCFD2850+RCFD2910", "period": "dt >= 19690630 & dt < 19940331", "notes": None},
            {"code": "RCFD2332+RCFD2333+RCFD2910", "period": "dt >= 19940331 & dt < 19970331", "notes": None},
            {"code": "RCFD2332+RCFD2333", "period": "dt >= 19970331 & dt < 19970630", "notes": None},
            {"code": "RCFD2332+RCFDA547+RCFDA548", "period": "dt >= 19970630 & dt < 20010331", "notes": None},
            {"code": "RCFD3190", "period": "dt >= 20010331", "notes": None},
        ],
        "notes": None,
    },
    "subdebt": {
        "description": "Subordinated notes and debentures",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD3200", "period": "dt >= 19591231", "notes": None},
        ],
        "notes": None,
    },
    "liab_oth": {
        "description": "Other Liabilities",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD2930+RCFD2920-RCFD3000", "period": "dt >= 19591231 & dt < 20010331", "notes": "Some banks report aggregate RCFD2915 = 2930+2920."},
            {"code": "RCFD2930+RCFD2920", "period": "dt >= 20010331 & dt < 20060331", "notes": None},
            {"code": "RCFD2930", "period": "dt >= 20060331", "notes": None},
        ],
        "notes": None,
    },
    "liab_tot": {
        "description": "Total Liabilities",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD2950+RCFD3200+RCFD3282-RCFD3000", "period": "dt >= 19591231 & dt < 19840331", "notes": None},
            {"code": "RCFD2948+RCFD3282-RCFD3000", "period": "dt >= 19840331 & dt < 19970331", "notes": None},
            {"code": "RCFD2948-RCFD3000", "period": "dt >= 19970331 & dt < 20010331", "notes": None},
            {"code": "RCFD2948", "period": "dt >= 20010331", "notes": None},
        ],
        "notes": None,
    },
    "liab_tot_unadj": {
        "description": "Total Liabilities (unadjusted, without minority interest subtraction)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD2950", "period": "dt >= 19591231 & dt < 19840331", "notes": None},
            {"code": "RCFD2948", "period": "dt >= 19840331", "notes": None},
        ],
        "notes": None,
    },
    "minorint": {
        "description": "Minority interest in consolidated subsidiaries",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD3000", "period": "dt >= 19690630 & dt < 19760331", "notes": None},
            {"code": "RCON3000", "period": "dt >= 19760331 & dt < 19781231", "notes": None},
            {"code": "RCFD3000", "period": "dt >= 19840331", "notes": None},
        ],
        "notes": None,
    },
    "pref_stock": {
        "description": "Preferred Stock, total",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD3220", "period": "dt >= 19591231 & dt < 19970331", "notes": "Some banks report RCFD3838+RCFD3282 when 3220 is missing."},
            {"code": "RCFD3838", "period": "dt >= 19970331", "notes": None},
        ],
        "notes": None,
    },
    "comm_stock": {
        "description": "Common Stock",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD3230", "period": "dt >= 19591231", "notes": None},
        ],
        "notes": None,
    },
    "surplus": {
        "description": "Surplus",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD3240", "period": "dt >= 19591231 & dt < 19900331", "notes": None},
            {"code": "RCFD3839", "period": "dt >= 19900331", "notes": None},
        ],
        "notes": None,
    },
    "retain_earn": {
        "description": "Retained Earnings / Undivided profits and capital reserves",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD3250+RCFD3260", "period": "dt >= 19591231 & dt < 19840331", "notes": "Some banks report aggregate RCFD3247 = 3250+3260."},
            {"code": "RCFD3632", "period": "dt >= 19890331", "notes": None},
        ],
        "notes": None,
    },
    "equity": {
        "description": "Total Equity",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD3210", "period": "dt >= 19591231", "notes": None},
        ],
        "notes": None,
    },
    "ln_re": {
        "description": "Real Estate Loans",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD1410", "period": "dt >= 19591231", "notes": None},
        ],
        "notes": None,
    },
    "ln_fi": {
        "description": "Loans to Financial Institutions",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD1495", "period": "dt >= 19591231 & dt < 19840331", "notes": None},
        ],
        "notes": None,
    },
    "ln_dep_inst": {
        "description": "Loans to Depository Institutions and Acceptances of Other Banks",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD1505+RCFD1510+RCON1538", "period": "dt >= 19760331 & dt < 19840331", "notes": "Jumps in 1978q4 due to addition of foreign holdings."},
            {"code": "RCFD1505+RCFD1517+RCFD1510+RCFD1756+RCFD1757", "period": "dt >= 19840331 & dt < 20010331", "notes": "Smaller banks report RCFD1489."},
            {"code": "RCFDB531+RCFDB534+RCFDB535", "period": "dt >= 20010331", "notes": "Smaller banks report RCFD1288."},
        ],
        "notes": "See ln_dep_inst_dom for domestic-only version.",
    },
    "ln_dep_inst_dom": {
        "description": "Loans to Depository Institutions and Acceptances of Other Banks (domestic only)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCON1505+RCON1510+RCON1538", "period": "dt >= 19760331 & dt < 19840331", "notes": None},
            {"code": "RCON1505+RCON1517+RCON1510+RCON1756+RCON1757", "period": "dt >= 19840331 & dt < 20010331", "notes": "Smaller banks: RCON1489."},
            {"code": "RCONB531+RCONB534+RCONB535", "period": "dt >= 20010331", "notes": "Smaller banks: RCON1288."},
        ],
        "notes": None,
    },
    "ln_oth": {
        "description": "Other loans (incl. loans to non-dep fin inst & for purchasing/carrying securities)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD1545+RCFD2080", "period": "dt >= 19591231 & dt < 19840331", "notes": None},
            {"code": "RCFD1563", "period": "dt >= 19840331", "notes": "If missing: RCFD1545+RCON1564 (pre-2010) or RCFD1545+RCONJ454+RCONJ451 (2010+); fallback RCFD2080."},
        ],
        "notes": None,
    },
    "ln_cc": {
        "description": "Credit Card Loans",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD2008", "period": "dt >= 19671231 & dt < 20010331", "notes": None},
            {"code": "RCFDB538", "period": "dt >= 20010331", "notes": None},
        ],
        "notes": None,
    },
    "ln_ci": {
        "description": "Commercial and Industrial Loans",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD1600", "period": "dt >= 19591231 & dt < 19840331", "notes": None},
            {"code": "RCFD1766", "period": "dt >= 19840331", "notes": "If missing: RCFD1763+RCFD1764."},
        ],
        "notes": None,
    },
    "ln_cons": {
        "description": "Consumer Loans",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD1975", "period": "dt >= 19591231", "notes": None},
        ],
        "notes": None,
    },
    "npl_tot": {
        "description": "Non-performing Loans, Total (nonaccrual + past due 90+ days)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD1403+RCFD1407", "period": "dt >= 19821231", "notes": None},
        ],
        "notes": None,
    },
    "brokered_dep": {
        "description": "Brokered Deposits",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCON2365", "period": "dt >= 19830930", "notes": None},
        ],
        "notes": None,
    },
    "insured_deposits": {
        "description": "Insured Deposits (line item)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCON2702", "period": "dt >= 19830630 & dt < 20060630", "notes": "Reported in Q2 only for all banks 1983-1990; quarterly from 1991."},
            {"code": "RCONF049+RCONF045", "period": "dt >= 20060630", "notes": None},
        ],
        "notes": None,
    },
    "insured_deposits_alt": {
        "description": "Insured Deposits (alternate construction, adjusts for deposit account characteristics)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCON2702+RCON2722*100", "period": "dt >= 19830630 & dt < 20060630", "notes": None},
            {"code": "RCONF049+RCONF052*100+RCONF045+RCONF048*250", "period": "dt >= 20060630 & dt < 20090930", "notes": None},
            {"code": "RCONF049+RCONF052*250+RCONF045+RCONF048*250", "period": "dt >= 20090930", "notes": "Multiplied by 250 to reflect FDIC insurance increase from $100K to $250K."},
        ],
        "notes": None,
    },
    "ln_agr": {
        "description": "Loans to Finance Agricultural Production",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD1590", "period": "dt >= 19591231", "notes": None},
        ],
        "notes": None,
    },
    "ln_lease": {
        "description": "Lease financing receivables",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD2165", "period": "dt >= 19690630", "notes": None},
        ],
        "notes": None,
    },
    "transaction_dep": {
        "description": "Total Transaction Accounts (Domestic Offices)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCON2215", "period": "dt >= 19840331", "notes": None},
        ],
        "notes": None,
    },
    "time_dep_ge100k": {
        "description": "Total Time Deposits of $100K or more",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCON2604", "period": "dt >= 19740630", "notes": None},
        ],
        "notes": None,
    },
    "time_cd_ge100k": {
        "description": "Time CDs of $100K or more (domestic offices)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCON6645", "period": "dt >= 19760331 & dt < 19970630", "notes": None},
        ],
        "notes": None,
    },
    "time_dep_lt100k": {
        "description": "Total Time Deposits of less than $100K",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCON6648", "period": "dt >= 19840331", "notes": None},
        ],
        "notes": None,
    },
    "time_dep_gt250k": {
        "description": "Time Deposits of more than $250K",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCONJ474", "period": "dt >= 20100331", "notes": None},
        ],
        "notes": None,
    },
    "time_ge100k_le250k": {
        "description": "Time Deposits of $100K through $250K",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCONJ473", "period": "dt >= 20100331", "notes": None},
        ],
        "notes": None,
    },
    "time_dep_le250k": {
        "description": "Time Deposits of $250K or less",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCONJ474+RCON6648", "period": "dt >= 20100331", "notes": None},
        ],
        "notes": None,
    },
    "securities_ac": {
        "description": "Securities at Amortized Cost (HTM + AFS)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD1754+RCFD1772", "period": "dt >= 19940331", "notes": None},
        ],
        "notes": None,
    },
    "htmsec_ac": {
        "description": "Total HTM (Held-to-Maturity) Securities at Amortized Cost",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD1754", "period": "dt >= 19940331", "notes": None},
        ],
        "notes": None,
    },
    "afssec_fv": {
        "description": "Total AFS (Available-for-Sale) Securities at Fair Value",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD1773", "period": "dt >= 19940331", "notes": None},
        ],
        "notes": None,
    },
    "domestic_dep": {
        "description": "Deposits in Domestic Offices",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCON2200", "period": "dt >= 19591231", "notes": None},
        ],
        "notes": None,
    },
    "foreign_dep": {
        "description": "Deposits in Foreign Offices (FFIEC 031 filers only; NULL for 041/051)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFN2200", "period": "dt >= 19690630", "notes": None},
        ],
        "notes": "Reported only by large banks with foreign offices (FFIEC 031).",
    },
    "dom_deposit_ib": {
        "description": "Interest-Bearing Domestic Deposits",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCON6636", "period": "dt >= 19781231", "notes": None},
        ],
        "notes": None,
    },
    "dom_deposit_nib": {
        "description": "Noninterest-Bearing Domestic Deposits",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCON6631", "period": "dt >= 19840331", "notes": None},
        ],
        "notes": None,
    },
    "for_deposit_ib": {
        "description": "Interest-Bearing Foreign Deposits (FFIEC 031 filers only)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFN6636", "period": "dt >= 19781231", "notes": None},
        ],
        "notes": None,
    },
    "for_deposit_nib": {
        "description": "Noninterest-Bearing Foreign Deposits (FFIEC 031 filers only)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFN6631", "period": "dt >= 19840331", "notes": None},
        ],
        "notes": None,
    },
    "time_sav_dep": {
        "description": "Total Time and Savings Deposits",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD2350", "period": "dt >= 19591231", "notes": "If missing (post-1984): RCON2215+RCON2385-RCON2210."},
        ],
        "notes": None,
    },
    "nontransaction_dep": {
        "description": "Total Nontransaction Accounts (including MMDAs)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCON2385", "period": "dt >= 19840930", "notes": None},
        ],
        "notes": None,
    },
    "uninsured_time_dep": {
        "description": "Uninsured Time Deposits",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCON2604", "period": "dt >= 19740630 & dt < 20100331", "notes": None},
            {"code": "RCONJ474", "period": "dt >= 20100331", "notes": None},
        ],
        "notes": None,
    },
    "nontransaction_sav_dep": {
        "description": "Nontransaction Savings Deposits",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCON2389", "period": "dt >= 19840331", "notes": "If missing: RCON6810+RCON0352."},
        ],
        "notes": None,
    },
    "tot_sav_dep": {
        "description": "Total Savings Deposits (derived: time_sav_dep - time_deposits)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "time_sav_dep - time_deposits", "period": "dt >= 19610331", "notes": "Derived variable."},
        ],
        "notes": None,
    },
    # Quarterly averages
    "qtr_avg_ib_bal_due": {
        "description": "Quarterly Average of Interest-bearing balances due from depository institutions",
        "unit": "thousands_usd",
        "mdrm": [{"code": "RCFD3381", "period": "dt >= 19840331", "notes": None}],
        "notes": None,
    },
    "qtr_avg_securities": {
        "description": "Quarterly Average of Securities",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCFD3382+RCFD3383+RCFD3647+RCFD3648", "period": "dt >= 19840331 & dt < 20010331", "notes": None},
            {"code": "RCFDB558+RCFDB559+RCFDB560", "period": "dt >= 20010331", "notes": None},
        ],
        "notes": None,
    },
    "qtr_avg_ust_sec": {"description": "Quarterly Average of U.S. Treasury and Agency Securities (excl. MBS)", "unit": "thousands_usd", "mdrm": [{"code": "RCFDB558", "period": "dt >= 20010331", "notes": None}], "notes": None},
    "qtr_avg_mbs": {"description": "Quarterly Average of Mortgage-backed securities", "unit": "thousands_usd", "mdrm": [{"code": "RCFDB559", "period": "dt >= 20010331", "notes": None}], "notes": None},
    "qtr_avg_oth_sec": {"description": "Quarterly Average of all other securities", "unit": "thousands_usd", "mdrm": [{"code": "RCFDB560", "period": "dt >= 20010331", "notes": None}], "notes": None},
    "qtr_avg_trad_ass": {"description": "Quarterly Average of trading assets", "unit": "thousands_usd", "mdrm": [{"code": "RCFD3401", "period": "dt >= 19840331", "notes": None}], "notes": None},
    "qtr_avg_ffrepo_ass": {"description": "Quarterly Average of fed funds sold and securities purchased under agreements to resell", "unit": "thousands_usd", "mdrm": [{"code": "RCFD3365", "period": "dt >= 19760331", "notes": None}], "notes": None},
    "qtr_avg_ln_tot": {"description": "Quarterly Average of Total Loans (Domestic Offices)", "unit": "thousands_usd", "mdrm": [{"code": "RCON3360", "period": "dt >= 19690630", "notes": None}], "notes": None},
    "qtr_avg_ln_re": {"description": "Quarterly Average of Loans secured by real estate", "unit": "thousands_usd", "mdrm": [{"code": "RCON3385", "period": "dt >= 19840331", "notes": "If missing (post-2008): RCON3465+RCON3466."}], "notes": None},
    "qtr_avg_ln_rre": {"description": "Quarterly Average of Loans secured by 1-4 family residential properties", "unit": "thousands_usd", "mdrm": [{"code": "RCON3465", "period": "dt >= 19860331 & dt < 19890331, dt >= 20080331", "notes": "Reported by savings banks 1986q1-1988q4."}], "notes": None},
    "qtr_avg_ln_othre": {"description": "Quarterly Average of All Other Loans secured by Real Estate", "unit": "thousands_usd", "mdrm": [{"code": "RCON3466", "period": "dt >= 19860331 & dt < 19890331, dt >= 20080331", "notes": None}], "notes": None},
    "qtr_avg_ln_agr": {"description": "Quarterly Average of Loans to Finance Agricultural Production", "unit": "thousands_usd", "mdrm": [{"code": "RCON3386", "period": "dt >= 19840331", "notes": None}], "notes": None},
    "qtr_avg_ln_ci": {"description": "Quarterly Average of C&I Loans", "unit": "thousands_usd", "mdrm": [{"code": "RCON3387", "period": "dt >= 19840331", "notes": None}], "notes": None},
    "qtr_avg_ln_cons": {"description": "Quarterly Average of Loans to individuals for personal expenditures", "unit": "thousands_usd", "mdrm": [{"code": "RCONB561+RCONB562", "period": "dt >= 20010331", "notes": None}], "notes": None},
    "qtr_avg_ln_cc": {"description": "Quarterly Average of Credit cards", "unit": "thousands_usd", "mdrm": [{"code": "RCONB561", "period": "dt >= 20010331", "notes": None}], "notes": None},
    "qtr_avg_ln_othcons": {"description": "Quarterly Average of Other Loans to individuals for personal expenditures", "unit": "thousands_usd", "mdrm": [{"code": "RCONB562", "period": "dt >= 20010331", "notes": None}], "notes": None},
    "qtr_avg_ln_fgn": {"description": "Quarterly Average of total loans in foreign offices", "unit": "thousands_usd", "mdrm": [{"code": "RCFN3360", "period": "dt >= 19781231", "notes": None}], "notes": None},
    "qtr_avg_lease": {"description": "Quarterly Average of lease financing receivables", "unit": "thousands_usd", "mdrm": [{"code": "RCFD3484", "period": "dt >= 19870331", "notes": None}], "notes": None},
    "qtr_avg_assets": {"description": "Quarterly Average of Total Assets", "unit": "thousands_usd", "mdrm": [{"code": "RCFD3368", "period": "dt >= 19781231", "notes": None}], "notes": None},
    "qtr_avg_trans_dep_dom": {"description": "Quarterly Average of Interest-bearing transaction accounts", "unit": "thousands_usd", "mdrm": [{"code": "RCON3485", "period": "dt >= 19870331", "notes": None}], "notes": None},
    "qtr_avg_sav_dep_dom": {
        "description": "Quarterly Average of Savings Deposits (includes MMDAs)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RCON3486+RCON3487", "period": "dt >= 19870331 & dt < 20010331", "notes": None},
            {"code": "RCONB563", "period": "dt >= 20010331", "notes": None},
        ],
        "notes": None,
    },
    "qtr_avg_time_dep_ge100k": {"description": "Quarterly Average of Time Deposits of $100K or more", "unit": "thousands_usd", "mdrm": [{"code": "RCONA514", "period": "dt >= 19970331 & dt < 20170331", "notes": None}], "notes": None},
    "qtr_avg_time_dep_gt250k": {"description": "Quarterly Average of Time Deposits of More than $250K", "unit": "thousands_usd", "mdrm": [{"code": "RCONHK17", "period": "dt >= 20170331", "notes": None}], "notes": None},
    "qtr_avg_time_dep_lt100k": {"description": "Quarterly Average of Time Deposits of less than $100K", "unit": "thousands_usd", "mdrm": [{"code": "RCONA529", "period": "dt >= 19970331 & dt < 20170331", "notes": None}], "notes": None},
    "qtr_avg_time_dep_le250k": {"description": "Quarterly Average of Time Deposits of $250K or less", "unit": "thousands_usd", "mdrm": [{"code": "RCONHK16", "period": "dt >= 20170331", "notes": None}], "notes": None},
    "qtr_avg_time_cd_ge100k": {"description": "Quarterly Average of Time CDs of >= $100K", "unit": "thousands_usd", "mdrm": [{"code": "RCON3345", "period": "dt >= 19781231 & dt < 19970331", "notes": None}], "notes": None},
    "qtr_avg_fgn_dep": {"description": "Quarterly Average of interest-bearing deposits in foreign offices", "unit": "thousands_usd", "mdrm": [{"code": "RCFN3404", "period": "dt >= 19840331", "notes": None}], "notes": None},
    "qtr_avg_ffrepo_liab": {"description": "Quarterly Average of fed funds purchased and securities sold under agreements to repurchase", "unit": "thousands_usd", "mdrm": [{"code": "RCFD3353", "period": "dt >= 19760331", "notes": None}], "notes": None},
    "qtr_avg_othbor_liab": {"description": "Quarterly Average of Other borrowed money", "unit": "thousands_usd", "mdrm": [{"code": "RCFD3355", "period": "dt >= 19760331", "notes": None}], "notes": None},
    # Interest rate sensitivity by maturity bucket
    "ust_sec_3mo_less": {"description": "U.S. Treasury/Gov/Other Debt securities: remaining maturity/repricing <= 3 months", "unit": "thousands_usd", "mdrm": [{"code": "RCFDA549", "period": "dt >= 19970630", "notes": None}], "notes": None},
    "ust_sec_3mo_12mo": {"description": "U.S. Treasury/Gov/Other Debt securities: remaining maturity/repricing 3-12 months", "unit": "thousands_usd", "mdrm": [{"code": "RCFDA550", "period": "dt >= 19970630", "notes": None}], "notes": None},
    "ust_sec_1y_3y": {"description": "U.S. Treasury/Gov/Other Debt securities: remaining maturity/repricing 1-3 years", "unit": "thousands_usd", "mdrm": [{"code": "RCFDA551", "period": "dt >= 19970630", "notes": None}], "notes": None},
    "ust_sec_3y_5y": {"description": "U.S. Treasury/Gov/Other Debt securities: remaining maturity/repricing 3-5 years", "unit": "thousands_usd", "mdrm": [{"code": "RCFDA552", "period": "dt >= 19970630", "notes": None}], "notes": None},
    "ust_sec_5y_15y": {"description": "U.S. Treasury/Gov/Other Debt securities: remaining maturity/repricing 5-15 years", "unit": "thousands_usd", "mdrm": [{"code": "RCFDA553", "period": "dt >= 19970630", "notes": None}], "notes": None},
    "ust_sec_over_15y": {"description": "U.S. Treasury/Gov/Other Debt securities: remaining maturity/repricing > 15 years", "unit": "thousands_usd", "mdrm": [{"code": "RCFDA554", "period": "dt >= 19970630", "notes": None}], "notes": None},
    "res_mbs_3mo_less": {"description": "Residential MBS: remaining maturity or next repricing <= 3 months", "unit": "thousands_usd", "mdrm": [{"code": "RCFDA555", "period": "dt >= 19970630", "notes": None}], "notes": None},
    "res_mbs_3mo_12mo": {"description": "Residential MBS: remaining maturity or next repricing 3-12 months", "unit": "thousands_usd", "mdrm": [{"code": "RCFDA556", "period": "dt >= 19970630", "notes": None}], "notes": None},
    "res_mbs_1y_3y": {"description": "Residential MBS: remaining maturity or next repricing 1-3 years", "unit": "thousands_usd", "mdrm": [{"code": "RCFDA557", "period": "dt >= 19970630", "notes": None}], "notes": None},
    "res_mbs_3y_5y": {"description": "Residential MBS: remaining maturity or next repricing 3-5 years", "unit": "thousands_usd", "mdrm": [{"code": "RCFDA558", "period": "dt >= 19970630", "notes": None}], "notes": None},
    "res_mbs_5y_15y": {"description": "Residential MBS: remaining maturity or next repricing 5-15 years", "unit": "thousands_usd", "mdrm": [{"code": "RCFDA559", "period": "dt >= 19970630", "notes": None}], "notes": None},
    "res_mbs_over_15y": {"description": "Residential MBS: remaining maturity or next repricing > 15 years", "unit": "thousands_usd", "mdrm": [{"code": "RCFDA560", "period": "dt >= 19970630", "notes": None}], "notes": None},
    "other_mbs_3y_less": {"description": "Other MBS: remaining maturity or next repricing <= 3 years", "unit": "thousands_usd", "mdrm": [{"code": "RCFDA561", "period": "dt >= 19970630", "notes": None}], "notes": None},
    "other_mbs_3y_more": {"description": "Other MBS: remaining maturity or next repricing > 3 years", "unit": "thousands_usd", "mdrm": [{"code": "RCFDA562", "period": "dt >= 19970630", "notes": None}], "notes": None},
    "securities_3mo_less": {"description": "All securities with repricing maturity <= 3 months", "unit": "thousands_usd", "mdrm": [{"code": "RCFDA549+RCFDA555", "period": "dt >= 19970630", "notes": None}], "notes": None},
    "securities_3mo_12mo": {"description": "All securities with repricing maturity 3-12 months", "unit": "thousands_usd", "mdrm": [{"code": "RCFDA550+RCFDA556", "period": "dt >= 19970630", "notes": None}], "notes": None},
    "securities_1y_3y": {"description": "All securities with repricing maturity 1-3 years", "unit": "thousands_usd", "mdrm": [{"code": "RCFDA551+RCFDA557", "period": "dt >= 19970630", "notes": None}], "notes": None},
    "securities_3y_5y": {"description": "All securities with repricing maturity 3-5 years", "unit": "thousands_usd", "mdrm": [{"code": "RCFDA552+RCFDA558", "period": "dt >= 19970630", "notes": None}], "notes": None},
    "securities_5y_15y": {"description": "All securities with repricing maturity 5-15 years", "unit": "thousands_usd", "mdrm": [{"code": "RCFDA553+RCFDA559", "period": "dt >= 19970630", "notes": None}], "notes": None},
    "securities_over_15y": {"description": "All securities with repricing maturity > 15 years", "unit": "thousands_usd", "mdrm": [{"code": "RCFDA554+RCFDA560", "period": "dt >= 19970630", "notes": None}], "notes": None},
    "securities_mat_1y_less": {"description": "Debt securities with remaining maturity of one year or less", "unit": "thousands_usd", "mdrm": [{"code": "RCFDA248", "period": "dt >= 19960331", "notes": None}], "notes": None},
    "res_loans_3mo_less": {"description": "Residential loans: remaining maturity or next repricing <= 3 months", "unit": "thousands_usd", "mdrm": [{"code": "RCONA564", "period": "dt >= 19970630", "notes": None}], "notes": None},
    "res_loans_3mo_12mo": {"description": "Residential loans: remaining maturity or next repricing 3-12 months", "unit": "thousands_usd", "mdrm": [{"code": "RCONA565", "period": "dt >= 19970630", "notes": None}], "notes": None},
    "res_loans_1y_3y": {"description": "Residential loans: remaining maturity or next repricing 1-3 years", "unit": "thousands_usd", "mdrm": [{"code": "RCONA566", "period": "dt >= 19970630", "notes": None}], "notes": None},
    "res_loans_3y_5y": {"description": "Residential loans: remaining maturity or next repricing 3-5 years", "unit": "thousands_usd", "mdrm": [{"code": "RCONA567", "period": "dt >= 19970630", "notes": None}], "notes": None},
    "res_loans_5y_15y": {"description": "Residential loans: remaining maturity or next repricing 5-15 years", "unit": "thousands_usd", "mdrm": [{"code": "RCONA568", "period": "dt >= 19970630", "notes": None}], "notes": None},
    "res_loans_over_15y": {"description": "Residential loans: remaining maturity or next repricing > 15 years", "unit": "thousands_usd", "mdrm": [{"code": "RCONA569", "period": "dt >= 19970630", "notes": None}], "notes": None},
    "ln_lease_3mo_less": {"description": "Loans and Leases: remaining maturity or next repricing <= 3 months", "unit": "thousands_usd", "mdrm": [{"code": "RCONA570+RCONA564", "period": "dt >= 19970630", "notes": None}], "notes": None},
    "ln_lease_3mo_12mo": {"description": "Loans and Leases: remaining maturity or next repricing 3-12 months", "unit": "thousands_usd", "mdrm": [{"code": "RCONA571+RCONA565", "period": "dt >= 19970630", "notes": None}], "notes": None},
    "ln_lease_1y_3y": {"description": "Loans and Leases: remaining maturity or next repricing 1-3 years", "unit": "thousands_usd", "mdrm": [{"code": "RCONA572+RCONA566", "period": "dt >= 19970630", "notes": None}], "notes": None},
    "ln_lease_3y_5y": {"description": "Loans and Leases: remaining maturity or next repricing 3-5 years", "unit": "thousands_usd", "mdrm": [{"code": "RCONA573+RCONA567", "period": "dt >= 19970630", "notes": None}], "notes": None},
    "ln_lease_5y_15y": {"description": "Loans and Leases: remaining maturity or next repricing 5-15 years", "unit": "thousands_usd", "mdrm": [{"code": "RCONA574+RCONA568", "period": "dt >= 19970630", "notes": None}], "notes": None},
    "ln_lease_over_15y": {"description": "Loans and Leases: remaining maturity or next repricing > 15 years", "unit": "thousands_usd", "mdrm": [{"code": "RCONA575+RCONA569", "period": "dt >= 19970630", "notes": None}], "notes": None},
    "ln_lease_mat_1y_less": {"description": "Loans and Leases with remaining maturity of one year or less", "unit": "thousands_usd", "mdrm": [{"code": "RCONA247", "period": "dt >= 19960331", "notes": None}], "notes": None},
    "brokered_dep_lt100k": {"description": "Brokered deposits issued in denominations < $100K", "unit": "thousands_usd", "mdrm": [{"code": "RCON2343", "period": "dt >= 19840331 & dt < 20170331", "notes": None}], "notes": None},
    "brokered_dep_ge100k": {"description": "Brokered deposits issued in denominations >= $100K", "unit": "thousands_usd", "mdrm": [{"code": "RCON2344", "period": "dt >= 19840331 & dt < 20170331", "notes": None}], "notes": None},
    "time_dep_lt100k_3mo_less": {"description": "Time deposits < $100K: remaining maturity/repricing <= 3 months", "unit": "thousands_usd", "mdrm": [{"code": "RCONA579", "period": "dt >= 19970630 & dt < 20170331", "notes": None}], "notes": None},
    "time_dep_lt100k_3mo_12mo": {"description": "Time deposits < $100K: remaining maturity/repricing 3-12 months", "unit": "thousands_usd", "mdrm": [{"code": "RCONA580", "period": "dt >= 19970630 & dt < 20170331", "notes": None}], "notes": None},
    "time_dep_lt100k_1y_3y": {"description": "Time deposits < $100K: remaining maturity/repricing 1-3 years", "unit": "thousands_usd", "mdrm": [{"code": "RCONA581", "period": "dt >= 19970630 & dt < 20170331", "notes": None}], "notes": None},
    "time_dep_lt100k_over_3yr": {"description": "Time deposits < $100K: remaining maturity/repricing > 3 years", "unit": "thousands_usd", "mdrm": [{"code": "RCONA582", "period": "dt >= 19970630 & dt < 20170331", "notes": None}], "notes": None},
    "time_dep_lt100k_mat_1y_less": {"description": "Time deposits < $100K: remaining maturity <= 1 year (fixed + floating rate)", "unit": "thousands_usd", "mdrm": [{"code": "RCONA241", "period": "dt >= 19960331 & dt < 20170331", "notes": None}], "notes": None},
    "time_dep_ge100k_3mo_less": {"description": "Time deposits >= $100K: remaining maturity/repricing <= 3 months", "unit": "thousands_usd", "mdrm": [{"code": "RCONA232+RCONA236", "period": "dt >= 19960331 & dt < 19970630", "notes": None}, {"code": "RCONA584", "period": "dt >= 19970630 & dt < 20170331", "notes": None}], "notes": None},
    "time_dep_ge100k_3mo_12mo": {"description": "Time deposits >= $100K: remaining maturity/repricing 3-12 months", "unit": "thousands_usd", "mdrm": [{"code": "RCONA233+RCONA237", "period": "dt >= 19960331 & dt < 19970630", "notes": None}, {"code": "RCONA585", "period": "dt >= 19970630 & dt < 20170331", "notes": None}], "notes": None},
    "time_dep_ge100k_1y_3y": {"description": "Time deposits >= $100K: remaining maturity/repricing 1-3 years", "unit": "thousands_usd", "mdrm": [{"code": "RCONA586", "period": "dt >= 19970630 & dt < 20170331", "notes": None}], "notes": None},
    "time_dep_ge100k_over_3yr": {"description": "Time deposits >= $100K: remaining maturity/repricing > 3 years", "unit": "thousands_usd", "mdrm": [{"code": "RCONA587", "period": "dt >= 19970630 & dt < 20170331", "notes": None}], "notes": None},
    "time_dep_le250k_3mo_less": {"description": "Time deposits <= $250K: remaining maturity/repricing <= 3 months", "unit": "thousands_usd", "mdrm": [{"code": "RCONHK07", "period": "dt >= 20170331", "notes": None}], "notes": None},
    "time_dep_le250k_3mo_12mo": {"description": "Time deposits <= $250K: remaining maturity/repricing 3-12 months", "unit": "thousands_usd", "mdrm": [{"code": "RCONHK08", "period": "dt >= 20170331", "notes": None}], "notes": None},
    "time_dep_le250k_1y_3y": {"description": "Time deposits <= $250K: remaining maturity/repricing 1-3 years", "unit": "thousands_usd", "mdrm": [{"code": "RCONHK09", "period": "dt >= 20170331", "notes": None}], "notes": None},
    "time_dep_le250k_over_3yr": {"description": "Time deposits <= $250K: remaining maturity/repricing > 3 years", "unit": "thousands_usd", "mdrm": [{"code": "RCONHK10", "period": "dt >= 20170331", "notes": None}], "notes": None},
    "time_dep_le250k_mat_1y_less": {"description": "Time deposits <= $250K: remaining maturity <= 1 year", "unit": "thousands_usd", "mdrm": [{"code": "RCONHK11", "period": "dt >= 20170331", "notes": None}], "notes": None},
    "time_dep_gt250k_3mo_less": {"description": "Time deposits > $250K: remaining maturity/repricing <= 3 months", "unit": "thousands_usd", "mdrm": [{"code": "RCONHK12", "period": "dt >= 20170331", "notes": None}], "notes": None},
    "time_dep_gt250k_3mo_12mo": {"description": "Time deposits > $250K: remaining maturity/repricing 3-12 months", "unit": "thousands_usd", "mdrm": [{"code": "RCONHK13", "period": "dt >= 20170331", "notes": None}], "notes": None},
    "time_dep_gt250k_1y_3y": {"description": "Time deposits > $250K: remaining maturity/repricing 1-3 years", "unit": "thousands_usd", "mdrm": [{"code": "RCONHK14", "period": "dt >= 20170331", "notes": None}], "notes": None},
    "time_dep_gt250k_over_3yr": {"description": "Time deposits > $250K: remaining maturity/repricing > 3 years", "unit": "thousands_usd", "mdrm": [{"code": "RCONHK15", "period": "dt >= 20170331", "notes": None}], "notes": None},
    # Derivatives
    "tot_gna_deriv_ir": {"description": "Total gross notional amount of interest rate derivative contracts (not trading)", "unit": "thousands_usd", "mdrm": [{"code": "RCFD8725", "period": "dt >= 19950331", "notes": None}], "notes": None},
    "tot_gna_deriv_ir_par": {"description": "Total gross notional amount of interest rate derivative contracts (not marked to market)", "unit": "thousands_usd", "mdrm": [{"code": "RCFD8729", "period": "dt >= 19950331 & dt < 20010331", "notes": None}], "notes": None},
    "gross_hedging": {"description": "Gross Hedging (interest rate derivatives, not trading + not MTM)", "unit": "thousands_usd", "mdrm": [{"code": "RCFD8729+RCFD8725", "period": "dt >= 19950331", "notes": None}], "notes": None},
    "tot_gna_ir_fixed_rate_swap": {"description": "Total gross notional amount of fixed-rate interest rate swaps", "unit": "thousands_usd", "mdrm": [{"code": "RCFDA589", "period": "dt >= 19970630", "notes": None}], "notes": None},
    "tot_swaps_int_rate": {"description": "Gross amounts of interest rate swap contracts", "unit": "thousands_usd", "mdrm": [{"code": "RCFD3450", "period": "dt >= 19850630", "notes": None}], "notes": None},
    "tot_gna_ir_float_rate_swap": {"description": "Total gross notional amount of floating-rate interest rate swaps", "unit": "thousands_usd", "mdrm": [{"code": "RCFD3450-RCFDA589", "period": "dt >= 19970630", "notes": "Derived: total swaps minus fixed-rate swaps."}], "notes": None},
    "net_hedging": {"description": "Net Hedging position (fixed - floating swap notionals)", "unit": "thousands_usd", "mdrm": [{"code": "RCFDA589-(RCFD3450-RCFDA589)", "period": "dt >= 19970630", "notes": "Derived variable."}], "notes": None},
    "tot_gna_deriv_trad_ir": {"description": "Total gross notional amount of interest rate derivative contracts held for trading", "unit": "thousands_usd", "mdrm": [{"code": "RCFDA126", "period": "dt >= 19950331", "notes": None}], "notes": None},
}


INCOME_STATEMENT_VARS: dict[str, dict] = {
    "ytdint_inc_sec": {
        "description": "Interest and Dividend Income on Securities (YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "IADX5002+IADX5004", "period": "dt >= 19601231 & dt < 19691231", "notes": "Historical IADX codes (1960-1968)."},
            {"code": "RIAD4027+RIAD4050+RIAD4060", "period": "dt >= 19691231 & dt < 19840331", "notes": None},
            {"code": "RIAD4218", "period": "dt >= 19840331 & dt < 20010331", "notes": "Smaller banks."},
            {"code": "RIAD4027+RIAD4066+RIAD4067+RIAD4068", "period": "dt >= 19840331 & dt < 19890331", "notes": "Larger banks."},
            {"code": "RIAD4027+RIAD3657+RIAD4506+RIAD4507+RIAD3658+RIAD3659", "period": "dt >= 19890331 & dt < 20010331", "notes": "Larger banks."},
            {"code": "RIADB488+RIADB489+RIAD4060", "period": "dt >= 20010331", "notes": None},
        ],
        "notes": "Derived item constructed from component variables.",
    },
    "ytdint_inc_ln": {
        "description": "Interest Income on Loans (YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "IADX5006", "period": "dt >= 19601231 & dt < 19691231", "notes": None},
            {"code": "RIAD4010", "period": "dt >= 19691231", "notes": None},
        ],
        "notes": None,
    },
    "ytdint_inc_ffrepo": {
        "description": "Interest Income on Federal Funds Sold and Repo Purchased (YTD)",
        "unit": "thousands_usd",
        "mdrm": [{"code": "RIAD4020", "period": "dt >= 19691231", "notes": None}],
        "notes": None,
    },
    "ytdint_inc_lease": {
        "description": "Income from Lease Financing (YTD)",
        "unit": "thousands_usd",
        "mdrm": [{"code": "RIAD4065", "period": "dt >= 19760331", "notes": None}],
        "notes": None,
    },
    "ytdint_inc_ibb": {
        "description": "Interest Income on balances due from depository institutions (YTD)",
        "unit": "thousands_usd",
        "mdrm": [{"code": "RIAD4115", "period": "dt >= 19760331", "notes": None}],
        "notes": None,
    },
    "ytdfiduc_inc": {
        "description": "Income from Fiduciary Activities (YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "IADX5014", "period": "dt >= 19601231 & dt < 19691231", "notes": None},
            {"code": "RIAD4070", "period": "dt >= 19691231", "notes": None},
        ],
        "notes": None,
    },
    "ytdnonint_inc_srv_chrg_dep": {
        "description": "Service Charges on Deposits (YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "IADX5010", "period": "dt >= 19601231 & dt < 19691231", "notes": None},
            {"code": "RIAD4080", "period": "dt >= 19691231", "notes": None},
        ],
        "notes": None,
    },
    "ytdoth_srv_chrg": {
        "description": "Other Service Charges, Commissions, and Fees (YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "IADX5012", "period": "dt >= 19601231 & dt < 19691231", "notes": None},
            {"code": "RIAD4090", "period": "dt >= 19691231 & dt < 19840331", "notes": None},
        ],
        "notes": None,
    },
    "ytdoth_operating_inc": {
        "description": "Other Operating Income (YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "IADX5016", "period": "dt >= 19601231 & dt < 19691231", "notes": None},
            {"code": "RIAD4100", "period": "dt >= 19691231 & dt < 19840331", "notes": None},
            {"code": "RIAD4078", "period": "dt >= 19840331 & dt < 19910331", "notes": None},
            {"code": "RIAD5407+RIAD5408", "period": "dt >= 19910331 & dt < 20010331", "notes": None},
            {"code": "RIAD4518+RIADB497", "period": "dt >= 20010331", "notes": None},
        ],
        "notes": None,
    },
    "ytdoth_operating_inc_adj": {
        "description": "Other Operating Income, adjusted for time-consistency (YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "IADX5016", "period": "dt >= 19601231 & dt < 19691231", "notes": None},
            {"code": "RIAD4100", "period": "dt >= 19691231 & dt < 19761231", "notes": None},
            {"code": "RIAD4100+RIAD4115+RIAD4065", "period": "dt >= 19761231 & dt < 19840331", "notes": None},
            {"code": "RIAD4115+RIAD4065+RIAD4069+RIAD4075+RIAD4076+RIAD4077+RIAD4078", "period": "dt >= 19840331 & dt < 19910331", "notes": None},
            {"code": "RIAD4115+RIAD4065+RIAD4069+RIAD4075+RIAD4076+RIAD4077+RIAD5407+RIAD5408+RIADA220", "period": "dt >= 19910331 & dt < 20010331", "notes": None},
            {"code": "RIAD4115+RIAD4065+RIAD4069+RIAD4518+RIADB497+RIADA220", "period": "dt >= 20010331", "notes": None},
        ],
        "notes": None,
    },
    "ytdoperating_inc_tot": {
        "description": "Total Operating Income (YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "IADX5000", "period": "dt >= 19601231 & dt < 19691231", "notes": None},
            {"code": "RIAD4000", "period": "dt >= 19691231", "notes": None},
        ],
        "notes": None,
    },
    "ytdop_inc_tot_1960": {
        "description": "Total Operating Income, 1960 time-consistent (YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "IADX5000", "period": "dt >= 19601231 & dt < 19691231", "notes": None},
            {"code": "ytdint_inc_sec+ytdint_inc_ln+ytdfiduc_inc+ytdnonint_inc_srv_chrg_dep+ytdoth_srv_chrg+ytdoth_operating_inc_adj", "period": "dt >= 19691231", "notes": "Derived from time-consistent components."},
        ],
        "notes": "Uses 1960 income statement line items as baseline for comparability.",
    },
    "num_employees": {
        "description": "Number of full-time equivalent employees",
        "unit": "count",
        "mdrm": [
            {"code": "IADX5022+IADX5026", "period": "dt >= 19601231 & dt < 19691231", "notes": "Adjusted for trailing-zero outliers in 1967/1968 OCR data."},
            {"code": "RIAD4150", "period": "dt >= 19691231", "notes": None},
        ],
        "notes": "Reported on income statement but NOT year-to-date; represents point-in-time headcount.",
    },
    "ytdnonint_exp_comp": {
        "description": "Salaries and Employee Benefits (compensation, YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "IADX5020+IADX5024+IADX5028", "period": "dt >= 19601231 & dt < 19691231", "notes": None},
            {"code": "RIAD4135", "period": "dt >= 19691231", "notes": None},
        ],
        "notes": None,
    },
    "ytdint_exp_dep": {
        "description": "Interest expense on deposits (YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "IADX5032", "period": "dt >= 19601231 & dt < 19691231", "notes": None},
            {"code": "RIAD4170", "period": "dt >= 19691231 & dt < 20170331", "notes": "If missing (1976-1997): RIAD4174+RIAD4176+RIAD4172."},
            {"code": "RIAD4508+RIAD0093+RIADHK03+RIADHK04+RIAD4172", "period": "dt >= 20170331", "notes": None},
        ],
        "notes": None,
    },
    "ytdint_exp_ffrepo": {
        "description": "Interest Expense on Fed Funds Purchased and Securities Sold Under Agreements to Repurchase (YTD)",
        "unit": "thousands_usd",
        "mdrm": [{"code": "RIAD4180", "period": "dt >= 19691231", "notes": None}],
        "notes": None,
    },
    "ytdint_exp_othbor": {
        "description": "Interest on Other Borrowed Money (YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "IADX5034", "period": "dt >= 19601231 & dt < 19691231", "notes": None},
            {"code": "RIAD4190", "period": "dt >= 19691231 & dt < 19840331", "notes": None},
        ],
        "notes": None,
    },
    "ytdint_exp_trad_othbor": {
        "description": "Interest on Trading Liabilities and Other Borrowed Money (YTD)",
        "unit": "thousands_usd",
        "mdrm": [{"code": "RIAD4185", "period": "dt >= 19781231", "notes": None}],
        "notes": None,
    },
    "ytdint_exp_subdebt": {
        "description": "Interest on Subordinated Notes and Debentures (YTD)",
        "unit": "thousands_usd",
        "mdrm": [{"code": "RIAD4200", "period": "dt >= 19691231", "notes": None}],
        "notes": None,
    },
    "ytdnonint_exp_fass": {
        "description": "Noninterest Expense of Premises and Fixed Assets (YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "IADX5036+IADX5064", "period": "dt >= 19601231 & dt < 19691231", "notes": None},
            {"code": "RIAD4217", "period": "dt >= 19691231", "notes": None},
        ],
        "notes": None,
    },
    "ytdllprov": {
        "description": "Loan Loss Provisions (YTD)",
        "unit": "thousands_usd",
        "mdrm": [{"code": "RIAD4230", "period": "dt >= 19691231", "notes": None}],
        "notes": None,
    },
    "ytdoth_operating_exp": {
        "description": "Other Operating Expenses (YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "IADX5066", "period": "dt >= 19601231 & dt < 19691231", "notes": None},
            {"code": "RIAD4240-RIAD4330", "period": "dt >= 19691231 & dt < 19840331", "notes": "Subtracts minority interests (4330)."},
            {"code": "RIAD4092", "period": "dt >= 19840331", "notes": None},
        ],
        "notes": None,
    },
    "ytdoperating_exp_tot": {
        "description": "Total Operating Expense (YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "IADX5018", "period": "dt >= 19601231 & dt < 19691231", "notes": None},
            {"code": "RIAD4130", "period": "dt >= 19691231", "notes": None},
        ],
        "notes": None,
    },
    "ytdoperating_exp_tot_adj": {
        "description": "Total Operating Expense, adjusted (subtracts minority interest and loan loss provisions, YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "IADX5018", "period": "dt >= 19601231 & dt < 19691231", "notes": None},
            {"code": "RIAD4130-RIAD4330-RIAD4230", "period": "dt >= 19691231", "notes": None},
        ],
        "notes": None,
    },
    "ytdop_exp_tot_1960": {
        "description": "Total Operating Expense, 1960 time-consistent (YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "IADX5018", "period": "dt >= 19601231 & dt < 19691231", "notes": None},
            {"code": "ytdnonint_exp_comp+ytdint_exp_dep+ytdint_exp_othbor+ytdnonint_exp_fass+ytdoth_operating_exp", "period": "dt >= 19691231 & dt < 19781231", "notes": "Derived."},
            {"code": "ytdnonint_exp_comp+ytdint_exp_dep+ytdint_exp_trad_othbor+ytdnonint_exp_fass+ytdoth_operating_exp", "period": "dt >= 19781231", "notes": "Derived."},
        ],
        "notes": None,
    },
    "ytdnet_op_earn_1960": {
        "description": "Net Operating Earnings, 1960 time-consistent (YTD)",
        "unit": "thousands_usd",
        "mdrm": [{"code": "ytdop_inc_tot_1960-ytdop_exp_tot_1960", "period": "dt >= 19601231", "notes": "Derived."}],
        "notes": None,
    },
    "ytdnet_operating_earn": {
        "description": "Net Operating Earnings / Income before taxes and securities gains (YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "IADX5068", "period": "dt >= 19601231 & dt < 19691231", "notes": None},
            {"code": "RIAD4250", "period": "dt >= 19691231 & dt < 19840331", "notes": None},
            {"code": "RIAD4301", "period": "dt >= 19840331", "notes": None},
        ],
        "notes": None,
    },
    "ytdrecov_tot": {
        "description": "Total recoveries, transfers from valuation reserves, and profits (YTD)",
        "unit": "thousands_usd",
        "mdrm": [{"code": "IADX5070", "period": "dt >= 19601231 & dt < 19691231", "notes": "Reported on income statement prior to 1969 instead of loan loss provisions."}],
        "notes": None,
    },
    "ytdxoff_tot": {
        "description": "Total losses, charge-offs, and transfers to valuation reserves (YTD)",
        "unit": "thousands_usd",
        "mdrm": [{"code": "IADX5084", "period": "dt >= 19601231 & dt < 19691231", "notes": "Reported on income statement prior to 1969 instead of loan loss provisions."}],
        "notes": None,
    },
    "ytdinc_taxes_net": {
        "description": "Taxes on Net Income after losses/gains (1960-1969, YTD)",
        "unit": "thousands_usd",
        "mdrm": [{"code": "IADX5100", "period": "dt >= 19601231 & dt < 19691231", "notes": None}],
        "notes": None,
    },
    "ytdinc_taxes_netsec": {
        "description": "Income taxes, excluding taxes on securities gains or losses (YTD)",
        "unit": "thousands_usd",
        "mdrm": [{"code": "RIAD4260", "period": "dt >= 19691231 & dt < 19840331", "notes": None}],
        "notes": None,
    },
    "ytdinc_taxes_sec": {
        "description": "Income taxes on securities gains (losses, YTD)",
        "unit": "thousands_usd",
        "mdrm": [{"code": "RIAD4285", "period": "dt >= 19760331 & dt < 19840331", "notes": None}],
        "notes": None,
    },
    "ytdinc_taxes": {
        "description": "Taxes on income before extraordinary items/adjustments (YTD)",
        "unit": "thousands_usd",
        "mdrm": [{"code": "RIAD4302", "period": "dt >= 19840331", "notes": None}],
        "notes": None,
    },
    "ytdinc_before_sec_gain": {
        "description": "Income Before Securities Gains or Losses, after taxes (approximate, YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "IADX5068-IADX5100", "period": "dt >= 19601231 & dt < 19691231", "notes": None},
            {"code": "RIAD4270", "period": "dt >= 19691231 & dt < 19840331", "notes": None},
            {"code": "RIAD4300-RIAD4091+RIAD4219", "period": "dt >= 19840331", "notes": None},
        ],
        "notes": None,
    },
    "ytdsecur_inc": {
        "description": "Gains and losses on securities (YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RIAD4280", "period": "dt >= 19691231 & dt < 19840331", "notes": None},
            {"code": "RIAD4091", "period": "dt >= 19840331", "notes": None},
        ],
        "notes": None,
    },
    "ytdsec_net": {
        "description": "Securities Gains (Losses), net (YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RIAD4290", "period": "dt >= 19691231 & dt < 19840331", "notes": None},
            {"code": "RIAD4091-RIAD4219", "period": "dt >= 19840331 & dt < 19860331", "notes": None},
        ],
        "notes": None,
    },
    "ytdinc_before_disc_op": {
        "description": "Income (loss) before discontinued operations (YTD)",
        "unit": "thousands_usd",
        "mdrm": [{"code": "RIAD4300", "period": "dt >= 19691231", "notes": None}],
        "notes": None,
    },
    "ytdextra_inc_gross": {
        "description": "Extraordinary Items, gross of income taxes (YTD)",
        "unit": "thousands_usd",
        "mdrm": [{"code": "RIAD4310", "period": "dt >= 19691231 & dt < 19970331", "notes": None}],
        "notes": None,
    },
    "ytdextra_inc": {
        "description": "Extraordinary Items, net of income taxes (YTD)",
        "unit": "thousands_usd",
        "mdrm": [{"code": "RIAD4320", "period": "dt >= 19691231 & dt < 20160930", "notes": None}],
        "notes": None,
    },
    "ytddisc_op": {
        "description": "Discontinued Operations, net of applicable income taxes (YTD)",
        "unit": "thousands_usd",
        "mdrm": [{"code": "RIADFT28", "period": "dt >= 20160930", "notes": None}],
        "notes": None,
    },
    "ytdminor_int": {
        "description": "Minority interest (YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RIAD4330", "period": "dt >= 19691231 & dt < 19840331", "notes": None},
            {"code": "RIAD4484", "period": "dt >= 19840331 & dt < 20090331", "notes": None},
            {"code": "RIADG103", "period": "dt >= 20090331", "notes": None},
        ],
        "notes": None,
    },
    "ytdnetinc": {
        "description": "Net Income (YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "IADX5106", "period": "dt >= 19601231 & dt < 19691231", "notes": None},
            {"code": "RIAD4340", "period": "dt >= 19691231", "notes": None},
        ],
        "notes": None,
    },
    "ytdint_inc": {
        "description": "Total Interest Income (YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "IADX5002+IADX5004+IADX5006", "period": "dt >= 19601231 & dt < 19691231", "notes": None},
            {"code": "RIAD4010+RIAD4020+RIAD4027+RIAD4050+RIAD4060", "period": "dt >= 19691231 & dt < 19760331", "notes": None},
            {"code": "RIAD4010+RIAD4115+RIAD4020+RIAD4027+RIAD4050+RIAD4060+RIAD4065", "period": "dt >= 19760331 & dt < 19840331", "notes": None},
            {"code": "RIAD4107", "period": "dt >= 19840331", "notes": None},
        ],
        "notes": None,
    },
    "ytdnonint_inc": {
        "description": "Total Noninterest Income (YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "IADX5010+IADX5012+IADX5014", "period": "dt >= 19601231 & dt < 19691231", "notes": None},
            {"code": "RIAD4070+RIAD4080+RIAD4090", "period": "dt >= 19691231 & dt < 19840331", "notes": None},
            {"code": "RIAD4079", "period": "dt >= 19840331", "notes": None},
        ],
        "notes": None,
    },
    "ytdint_exp": {
        "description": "Total Interest Expense (YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "IADX5032+IADX5034", "period": "dt >= 19601231 & dt < 19691231", "notes": None},
            {"code": "RIAD4170+RIAD4180+RIAD4190+RIAD4200", "period": "dt >= 19691231 & dt < 19781231", "notes": None},
            {"code": "RIAD4170+RIAD4180+RIAD4185+RIAD4200", "period": "dt >= 19781231 & dt < 19840331", "notes": None},
            {"code": "RIAD4073", "period": "dt >= 19840331", "notes": None},
        ],
        "notes": None,
    },
    "ytdnonint_exp": {
        "description": "Total Noninterest Expense (YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "IADX5020+IADX5024+IADX5028+IADX5030+IADX5036+IADX5064", "period": "dt >= 19601231 & dt < 19691231", "notes": None},
            {"code": "RIAD4135+RIAD4217", "period": "dt >= 19691231 & dt < 19840331", "notes": None},
            {"code": "RIAD4093", "period": "dt >= 19840331", "notes": None},
        ],
        "notes": None,
    },
    "ytdint_inc_net": {
        "description": "Net Interest Income (YTD)",
        "unit": "thousands_usd",
        "mdrm": [{"code": "RIAD4074", "period": "dt >= 19840331", "notes": None}],
        "notes": None,
    },
    "ytdint_exp_time_ge100k_dom": {
        "description": "Interest Expense on Time Deposits >= $100K (domestic, YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RIAD4174", "period": "dt >= 19760331 & dt < 19970331", "notes": None},
            {"code": "RIADA517", "period": "dt >= 19970331 & dt < 20170331", "notes": None},
        ],
        "notes": None,
    },
    "ytdint_exp_time_lt100k_dom": {
        "description": "Interest Expense on Time Deposits < $100K (domestic, YTD)",
        "unit": "thousands_usd",
        "mdrm": [{"code": "RIADA518", "period": "dt >= 19970331 & dt < 20170331", "notes": None}],
        "notes": None,
    },
    "ytdint_exp_time_le250k_dom": {
        "description": "Interest Expense on Time Deposits <= $250K (domestic, YTD)",
        "unit": "thousands_usd",
        "mdrm": [{"code": "RIADHK03", "period": "dt >= 20170331", "notes": None}],
        "notes": None,
    },
    "ytdint_exp_time_gt250k_dom": {
        "description": "Interest Expense on Time Deposits > $250K (domestic, YTD)",
        "unit": "thousands_usd",
        "mdrm": [{"code": "RIADHK04", "period": "dt >= 20170331", "notes": None}],
        "notes": None,
    },
    "ytdint_exp_time_dep": {
        "description": "Interest Expense on Time Deposits (YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RIAD4174+RIAD4512", "period": "dt >= 19870331 & dt < 19970331", "notes": None},
            {"code": "RIADA517+RIADA518", "period": "dt >= 19970331 & dt < 20170331", "notes": None},
            {"code": "RIADHK03+RIADHK04", "period": "dt >= 20170331", "notes": None},
        ],
        "notes": None,
    },
    "ytdtradrev_inc": {
        "description": "Trading Revenue (YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RIAD4077+RIAD4075", "period": "dt >= 19840331 & dt < 19960331", "notes": None},
            {"code": "RIADA220", "period": "dt >= 19960331", "notes": None},
        ],
        "notes": None,
    },
    "ytdcommdividend": {
        "description": "Cash dividends declared on common stock (YTD)",
        "unit": "thousands_usd",
        "mdrm": [{"code": "RIAD4460", "period": "dt >= 19691231", "notes": None}],
        "notes": None,
    },
    "ytdint_inc_sec_ust": {
        "description": "Interest Income on U.S. Treasury and Agency Securities (YTD)",
        "unit": "thousands_usd",
        "mdrm": [{"code": "RIADB488", "period": "dt >= 20010331", "notes": None}],
        "notes": None,
    },
    "ytdint_inc_sec_mbs": {
        "description": "Interest Income on Mortgage-Backed Securities (YTD)",
        "unit": "thousands_usd",
        "mdrm": [{"code": "RIADB489", "period": "dt >= 20010331", "notes": None}],
        "notes": None,
    },
    "ytdint_inc_sec_oth": {
        "description": "Interest Income on All Other Securities (YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RIAD4060", "period": "dt >= 19691231 & dt < 19840331", "notes": None},
            {"code": "RIAD4066+RIAD4067+RIAD4068", "period": "dt >= 19840331 & dt < 19890331", "notes": None},
            {"code": "RIAD4066+RIAD3657+RIAD3658+RIAD3659", "period": "dt >= 19890331 & dt < 20010331", "notes": None},
            {"code": "RIAD4060", "period": "dt >= 20010331", "notes": None},
        ],
        "notes": None,
    },
    "ytdint_inc_ln_re": {
        "description": "Interest and fee income on loans secured by real estate (YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RIAD4011", "period": "dt >= 19840331", "notes": "Smaller banks (FFIEC 033/034): RIAD4246 (pre-2001). If missing (post-2008): RIAD4435+RIAD4436."},
        ],
        "notes": None,
    },
    "ytdint_inc_ln_rre": {
        "description": "Interest & Fee Income on Loans Secured by 1-4 Family Residential Properties (YTD)",
        "unit": "thousands_usd",
        "mdrm": [{"code": "RIAD4435", "period": "dt >= 19860331 & dt < 19890331, dt >= 20080331", "notes": "Reported by savings banks 1986q1-1988q4."}],
        "notes": None,
    },
    "ytdint_inc_ln_othre": {
        "description": "Interest & Fee Income on All Other Loans Secured by Real Estate (YTD)",
        "unit": "thousands_usd",
        "mdrm": [{"code": "RIAD4436", "period": "dt >= 19860331 & dt < 19890331, dt >= 20080331", "notes": None}],
        "notes": None,
    },
    "ytdint_inc_ln_agr": {
        "description": "Interest Income on Loans to finance agricultural production (YTD)",
        "unit": "thousands_usd",
        "mdrm": [{"code": "RIAD4024", "period": "dt >= 19840331", "notes": None}],
        "notes": None,
    },
    "ytdint_inc_ln_ci": {
        "description": "Interest Income on C&I Loans (YTD)",
        "unit": "thousands_usd",
        "mdrm": [{"code": "RIAD4012", "period": "dt >= 19840331", "notes": None}],
        "notes": None,
    },
    "ytdint_inc_ln_cc": {
        "description": "Interest and fee income on credit cards (YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RIAD4054", "period": "dt >= 19840331 & dt < 20010331", "notes": "Smaller banks: RIAD4248."},
            {"code": "RIADB485", "period": "dt >= 20010331", "notes": None},
        ],
        "notes": None,
    },
    "ytdint_inc_ln_othcons": {
        "description": "Interest income on other loans to individuals for personal expenditures (YTD)",
        "unit": "thousands_usd",
        "mdrm": [{"code": "RIADB486", "period": "dt >= 20010331", "notes": None}],
        "notes": None,
    },
    "ytdint_inc_ln_fgn": {
        "description": "Interest & Fee Income on Loans in Foreign Offices (YTD)",
        "unit": "thousands_usd",
        "mdrm": [{"code": "RIAD4059", "period": "dt >= 19840331", "notes": None}],
        "notes": None,
    },
    "ytdint_inc_ln_indiv": {
        "description": "Interest Income on Individual Loans (YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RIAD4054+RIAD4055", "period": "dt >= 19840331 & dt < 20010331", "notes": None},
            {"code": "RIADB485+RIADB486", "period": "dt >= 20010331", "notes": None},
        ],
        "notes": None,
    },
    "ytdint_exp_trans_dep_dom": {
        "description": "Interest Expense on Transaction Account Deposits (YTD)",
        "unit": "thousands_usd",
        "mdrm": [{"code": "RIAD4508", "period": "dt >= 19870331", "notes": None}],
        "notes": None,
    },
    "ytdint_exp_savings_dep_dom": {
        "description": "Interest Expense on Savings Deposits including MMDAs (YTD)",
        "unit": "thousands_usd",
        "mdrm": [
            {"code": "RIAD4509+RIAD4511", "period": "dt >= 19870331 & dt < 20010331", "notes": None},
            {"code": "RIAD0093", "period": "dt >= 20010331", "notes": None},
        ],
        "notes": None,
    },
    "ytdint_exp_fgn": {
        "description": "Interest Expense on Foreign Deposits (YTD)",
        "unit": "thousands_usd",
        "mdrm": [{"code": "RIAD4172", "period": "dt >= 19760331", "notes": None}],
        "notes": None,
    },
}


# Metadata/identifier variables (from xlsx Sheet 4)
METADATA_VARS: dict[str, dict] = {
    "id_rssd": {"description": "RSSD ID — unique Federal Reserve bank identifier", "unit": "identifier"},
    "id_rssd_hd_off": {"description": "RSSD ID of Head Office", "unit": "identifier"},
    "id_cusip": {"description": "CUSIP ID (6-character security identifier)", "unit": "identifier"},
    "id_thrift": {"description": "OTS docket number (thrift institutions)", "unit": "identifier"},
    "id_aba_prim": {"description": "Primary ABA routing number", "unit": "identifier"},
    "id_fdic_cert": {"description": "FDIC Certificate ID", "unit": "identifier"},
    "id_occ": {"description": "OCC Charter ID", "unit": "identifier"},
    "id_tax": {"description": "Tax ID", "unit": "identifier"},
    "id_lei": {"description": "Legal Entity Identifier (LEI)", "unit": "identifier"},
    "nm_lgl": {"description": "Legal name of institution", "unit": "text"},
    "nm_short": {"description": "Short name of institution", "unit": "text"},
    "city": {"description": "City/town name", "unit": "text"},
    "cntry_nm": {"description": "Country name", "unit": "text"},
    "state_abbr_nm": {"description": "State abbreviation", "unit": "text"},
    "state_cd": {"description": "Physical state FIPS code", "unit": "code"},
    "street_line1": {"description": "Physical street address line 1", "unit": "text"},
    "zip_cd": {"description": "Zip code", "unit": "code"},
    "county_cd": {"description": "County FIPS code", "unit": "code"},
    "cntry_cd": {"description": "Country code (U.S. Treasury classification)", "unit": "code"},
    "dist_frs": {"description": "Federal Reserve District code", "unit": "code"},
    "ent_type_cd": {"description": "Entity type code (60+ categories: commercial banks, savings banks, BHCs, foreign banks, etc.)", "unit": "code"},
    "entity_type": {"description": "Entity type (human-readable label)", "unit": "text"},
    "chtr_type_cd": {"description": "Charter type code", "unit": "code"},
    "act_prim_cd": {"description": "Primary activity code (NAICS)", "unit": "code"},
    "reason_term_cd": {"description": "Reason for termination code (if institution closed)", "unit": "code"},
    "dt_open": {"description": "Date of opening (YYYYMMDD format in source)", "unit": "date"},
    "reg_hh_1_id": {"description": "Regulatory High Holder RSSD ID", "unit": "identifier"},
    "fin_hh_id": {"description": "Financial High Holder RSSD ID", "unit": "identifier"},
    "reg_dh_1_id": {"description": "Regulatory Direct Holder RSSD ID", "unit": "identifier"},
    "fgn_call_fam_id": {"description": "Foreign Call Family RSSD ID (foreign bank branches)", "unit": "identifier"},
    "fgn_call_cntry_cd": {"description": "Foreign Call Family Country Code", "unit": "code"},
}


# Source file registry
SOURCE_FILES = {
    "balance_sheets": {
        "zip": "call-reports-balance-sheets-Jan2026.zip",
        "dta": "call-reports-balance-sheets-Jan2026.dta",
        "description": "Balance sheet panel: assets, liabilities, equity, loan categories, securities, deposits",
        "approx_rows": 2_658_000,
        "uncompressed_bytes": 2_658_391_856,
    },
    "income_statements": {
        "zip": "call-reports-income-statements-Jan2026.zip",
        "dta": "call-reports-income-statements-Jan2026.dta",
        "description": "Income statement panel: interest income/expense, noninterest items, net income (YTD)",
        "approx_rows": 2_500_000,
        "uncompressed_bytes": 718_774_021,
    },
}

RELEASE = "Jan2026"
COVERAGE_START = "1959-12-31"
COVERAGE_END = "2025-03-31"   # approximate; update when newer release ingested
SOURCE_URL = "https://libertystreeteconomics.newyorkfed.org/2025/12/a-long-run-history-of-bank-balance-sheets-and-income-statements/"
