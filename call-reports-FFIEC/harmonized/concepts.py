"""
Canonical variable → formula mapping for the harmonized layer.

Each concept entry has:
  - ``sql``: literal SQL expression that produces the value. Available table
    aliases in the bs_panel view:
       rc    = schedule_rc            (Balance Sheet)
       rcb   = schedule_rcb           (RC-B — Securities)
       rcci  = schedule_rcci          (RC-C Part I — Loans and Lease Financing Receivables)
       rce   = schedule_rce           (RC-E — Deposit Liabilities)
       rck   = schedule_rck           (RC-K — Quarterly Averages)
       rcm   = schedule_rcm           (RC-M — Memoranda; goodwill/MSA detail)
       rcn   = schedule_rcn           (RC-N — Past Due and Nonaccrual)
       cf    = call_filers            (POR, joined via filers_panel)

    In the is_panel view:
       ri    = schedule_ri            (Income Statement)
       ria   = schedule_ria           (RI-A — Changes in Bank Equity Capital)
       ribi  = schedule_ribi          (RI-B Part I — Charge-offs and Recoveries)
       ribii = schedule_ribii         (RI-B Part II — Changes in Allowances)
       cf    = call_filers

  - ``desc``: short human-readable description.
  - ``unit``: ``thousands_usd`` | ``count`` | ``date`` | ``identifier`` | ``text``.
  - ``source_schedule``: e.g. RC / RC-B / RC-C / RC-E / RC-K / RC-M / RC-N / RI / RI-A / RI-B I / RI-B II / POR.
  - ``mdrm_codes``: list of raw MDRM codes the formula touches (for discovery).
  - ``available_from``: first quarter the value is expected to be non-NULL in
    post-2001 coverage.

Semantics mirror ``call-reports-CFLV`` wherever CFLV exposes the same MDRM
codes. `ln_re` / `ln_ci` / `ln_cons` are computed as sum-of-subcategories
(standard BankRegData/UBPR convention) — not left out as in v1.

COALESCE convention: ``_cx(code, table)`` returns
``COALESCE(TRY_CAST(<t>.RCFD<code> AS DOUBLE), TRY_CAST(<t>.RCON<code> AS DOUBLE))``.
This picks RCFD for 031 filers (where populated) and RCON for 041/051. A
handful of codes have no RCFD variant in CDR bulk (e.g. RCONB987 ffs,
RCONB993 ffp) — those use RCON-only TRY_CAST. The C&I total `ln_ci` uses a
special fallback: RCON1766 for 041/051, RCFD1763+RCFD1764 for 031 (no
RCFD1766 in bulk data).
"""
from __future__ import annotations


def _cx(code: str, table: str = "rc") -> str:
    return (
        f"COALESCE("
        f"TRY_CAST({table}.RCFD{code} AS DOUBLE), "
        f"TRY_CAST({table}.RCON{code} AS DOUBLE)"
        f")"
    )


# ── Balance sheet ─────────────────────────────────────────────────────────────

BS_CONCEPTS: dict[str, dict] = {
    "assets": {
        "sql": _cx("2170"),
        "desc": "Total assets",
        "unit": "thousands_usd",
        "source_schedule": "RC",
        "mdrm_codes": ["RCFD2170", "RCON2170"],
        "available_from": "2001Q1",
    },
    "cash": {
        "sql": f"{_cx('0081')} + {_cx('0071')}",
        "desc": "Cash and balances due from depository institutions",
        "unit": "thousands_usd",
        "source_schedule": "RC",
        "mdrm_codes": ["RCFD0081", "RCON0081", "RCFD0071", "RCON0071"],
        "available_from": "2001Q1",
    },
    "htmsec_ac": {
        "sql": _cx("1754", table="rcb"),
        "desc": "Held-to-maturity securities (amortized cost)",
        "unit": "thousands_usd",
        "source_schedule": "RC-B",
        "mdrm_codes": ["RCFD1754", "RCON1754"],
        "available_from": "2001Q1",
    },
    "afssec_fv": {
        "sql": _cx("1773", table="rcb"),
        "desc": "Available-for-sale securities (fair value)",
        "unit": "thousands_usd",
        "source_schedule": "RC-B",
        "mdrm_codes": ["RCFD1773", "RCON1773"],
        "available_from": "2001Q1",
    },
    "securities": {
        "sql": (
            f"COALESCE({_cx('1754', table='rcb')}, 0) "
            f"+ COALESCE({_cx('1773', table='rcb')}, 0)"
        ),
        "desc": "Total securities (HTM + AFS)",
        "unit": "thousands_usd",
        "source_schedule": "RC-B",
        "mdrm_codes": ["RCFD1754", "RCON1754", "RCFD1773", "RCON1773"],
        "available_from": "2001Q1",
    },
    "ln_tot": {
        "sql": _cx("2122", table="rcci"),
        "desc": "Total loans and leases, net of unearned income",
        "unit": "thousands_usd",
        "source_schedule": "RC-C",
        "mdrm_codes": ["RCFD2122", "RCON2122"],
        "available_from": "2001Q1",
    },
    "ln_tot_gross": {
        "sql": (
            f"COALESCE({_cx('2122', table='rcci')}, 0) "
            f"+ COALESCE({_cx('2123', table='rcci')}, 0)"
        ),
        "desc": "Total loans and leases, gross",
        "unit": "thousands_usd",
        "source_schedule": "RC-C",
        "mdrm_codes": ["RCFD2122", "RCON2122", "RCFD2123", "RCON2123"],
        "available_from": "2001Q1",
    },
    "llres": {
        "sql": _cx("3123"),
        "desc": "Allowance for loan and lease losses",
        "unit": "thousands_usd",
        "source_schedule": "RC",
        "mdrm_codes": ["RCFD3123", "RCON3123"],
        "available_from": "2001Q1",
    },
    "ln_cc": {
        "sql": _cx("B538", table="rcci"),
        "desc": "Credit card loans",
        "unit": "thousands_usd",
        "source_schedule": "RC-C",
        "mdrm_codes": ["RCFDB538", "RCONB538"],
        "available_from": "2001Q1",
    },
    "ln_agr": {
        "sql": _cx("1590", table="rcci"),
        "desc": "Agricultural production loans",
        "unit": "thousands_usd",
        "source_schedule": "RC-C",
        "mdrm_codes": ["RCFD1590", "RCON1590"],
        "available_from": "2001Q1",
    },
    "npl_tot": {
        "sql": (
            f"COALESCE({_cx('1403', table='rcn')}, 0) "
            f"+ COALESCE({_cx('1407', table='rcn')}, 0)"
        ),
        "desc": "Non-performing loans (nonaccrual + 90+ days past due)",
        "unit": "thousands_usd",
        "source_schedule": "RC-N",
        "mdrm_codes": ["RCFD1403", "RCON1403", "RCFD1407", "RCON1407"],
        "available_from": "2001Q1",
    },
    "deposits": {
        # Consolidated = domestic (RCON2200) + foreign (RCFN2200).
        # RCFD2200 does NOT exist in CDR bulk TSV; CFLV derives identically.
        "sql": (
            "TRY_CAST(rc.RCON2200 AS DOUBLE) "
            "+ COALESCE(TRY_CAST(rc.RCFN2200 AS DOUBLE), 0)"
        ),
        "desc": "Total deposits (domestic + foreign offices)",
        "unit": "thousands_usd",
        "source_schedule": "RC",
        "mdrm_codes": ["RCON2200", "RCFN2200"],
        "available_from": "2001Q1",
    },
    "domestic_dep": {
        "sql": "TRY_CAST(rc.RCON2200 AS DOUBLE)",
        "desc": "Deposits in domestic offices",
        "unit": "thousands_usd",
        "source_schedule": "RC",
        "mdrm_codes": ["RCON2200"],
        "available_from": "2001Q1",
    },
    "foreign_dep": {
        "sql": "TRY_CAST(rc.RCFN2200 AS DOUBLE)",
        "desc": "Deposits in foreign offices (031 only; NULL for 041/051)",
        "unit": "thousands_usd",
        "source_schedule": "RC",
        "mdrm_codes": ["RCFN2200"],
        "available_from": "2001Q1",
    },
    "equity": {
        "sql": _cx("3210"),
        "desc": "Total equity capital",
        "unit": "thousands_usd",
        "source_schedule": "RC",
        "mdrm_codes": ["RCFD3210", "RCON3210"],
        "available_from": "2001Q1",
    },
    "qtr_avg_assets": {
        "sql": _cx("3368", table="rck"),
        "desc": "Quarterly average total assets",
        "unit": "thousands_usd",
        "source_schedule": "RC-K",
        "mdrm_codes": ["RCFD3368", "RCON3368"],
        "available_from": "2001Q1",
    },
    # ── Derived loan categories (Schedule RC-C Part I) ───────────────────
    "ln_re": {
        "sql": (
            f"COALESCE({_cx('F158', table='rcci')}, 0) "
            f"+ COALESCE({_cx('F159', table='rcci')}, 0) "
            f"+ COALESCE({_cx('1420', table='rcci')}, 0) "
            f"+ COALESCE({_cx('1797', table='rcci')}, 0) "
            f"+ COALESCE({_cx('5367', table='rcci')}, 0) "
            f"+ COALESCE({_cx('5368', table='rcci')}, 0) "
            f"+ COALESCE({_cx('1460', table='rcci')}, 0) "
            f"+ COALESCE({_cx('F160', table='rcci')}, 0) "
            f"+ COALESCE({_cx('F161', table='rcci')}, 0)"
        ),
        "desc": "Loans secured by real estate (sum of RC-C item 1.a-1.e subcategories)",
        "unit": "thousands_usd",
        "source_schedule": "RC-C",
        "mdrm_codes": [
            "RCFDF158","RCONF158","RCFDF159","RCONF159","RCFD1420","RCON1420",
            "RCFD1797","RCON1797","RCFD5367","RCON5367","RCFD5368","RCON5368",
            "RCFD1460","RCON1460","RCFDF160","RCONF160","RCFDF161","RCONF161",
        ],
        "available_from": "2001Q1",
    },
    "ln_ci": {
        # 041/051: RCON1766 is the total. 031: RCON1766 is NULL, sum the
        # domicile-split RCFDs (US + non-US addressees).
        "sql": (
            "COALESCE("
            "TRY_CAST(rcci.RCON1766 AS DOUBLE), "
            "COALESCE(TRY_CAST(rcci.RCFD1763 AS DOUBLE), 0) "
            "+ COALESCE(TRY_CAST(rcci.RCFD1764 AS DOUBLE), 0)"
            ")"
        ),
        "desc": "Commercial and industrial loans (consolidated)",
        "unit": "thousands_usd",
        "source_schedule": "RC-C",
        "mdrm_codes": ["RCON1766", "RCFD1763", "RCFD1764"],
        "available_from": "2001Q1",
    },
    "ln_cons": {
        "sql": (
            f"COALESCE({_cx('B538', table='rcci')}, 0) "
            f"+ COALESCE({_cx('B539', table='rcci')}, 0) "
            f"+ COALESCE({_cx('K137', table='rcci')}, 0) "
            f"+ COALESCE({_cx('K207', table='rcci')}, 0)"
        ),
        "desc": "Consumer loans (credit cards + other revolving + auto + other consumer)",
        "unit": "thousands_usd",
        "source_schedule": "RC-C",
        "mdrm_codes": [
            "RCFDB538","RCONB538","RCFDB539","RCONB539",
            "RCFDK137","RCONK137","RCFDK207","RCONK207",
        ],
        "available_from": "2001Q1",
    },
    # ── Balance sheet — liquid / interbank (Schedule RC) ─────────────────
    "ffs": {
        "sql": "TRY_CAST(rc.RCONB987 AS DOUBLE)",
        "desc": "Federal funds sold",
        "unit": "thousands_usd",
        "source_schedule": "RC",
        "mdrm_codes": ["RCONB987"],
        "available_from": "2002Q1",
    },
    "reverse_repo": {
        "sql": _cx("B989"),
        "desc": "Securities purchased under agreements to resell",
        "unit": "thousands_usd",
        "source_schedule": "RC",
        "mdrm_codes": ["RCFDB989", "RCONB989"],
        "available_from": "2002Q1",
    },
    "ffp": {
        "sql": "TRY_CAST(rc.RCONB993 AS DOUBLE)",
        "desc": "Federal funds purchased",
        "unit": "thousands_usd",
        "source_schedule": "RC",
        "mdrm_codes": ["RCONB993"],
        "available_from": "2002Q1",
    },
    "repo": {
        "sql": _cx("B995"),
        "desc": "Securities sold under agreements to repurchase",
        "unit": "thousands_usd",
        "source_schedule": "RC",
        "mdrm_codes": ["RCFDB995", "RCONB995"],
        "available_from": "2002Q1",
    },
    # ── Balance sheet — trading / premises / other (Schedule RC) ─────────
    "trading_assets": {
        "sql": _cx("3545"),
        "desc": "Trading assets",
        "unit": "thousands_usd",
        "source_schedule": "RC",
        "mdrm_codes": ["RCFD3545", "RCON3545"],
        "available_from": "2001Q1",
    },
    "trading_liab": {
        "sql": _cx("3548"),
        "desc": "Trading liabilities",
        "unit": "thousands_usd",
        "source_schedule": "RC",
        "mdrm_codes": ["RCFD3548", "RCON3548"],
        "available_from": "2001Q1",
    },
    "premises": {
        "sql": _cx("2145"),
        "desc": "Premises and fixed assets (incl. right-of-use)",
        "unit": "thousands_usd",
        "source_schedule": "RC",
        "mdrm_codes": ["RCFD2145", "RCON2145"],
        "available_from": "2001Q1",
    },
    "oreo": {
        "sql": _cx("2150"),
        "desc": "Other real estate owned",
        "unit": "thousands_usd",
        "source_schedule": "RC",
        "mdrm_codes": ["RCFD2150", "RCON2150"],
        "available_from": "2001Q1",
    },
    "intangibles": {
        "sql": _cx("2143"),
        "desc": "Intangible assets (total; includes goodwill, MSAs, other)",
        "unit": "thousands_usd",
        "source_schedule": "RC",
        "mdrm_codes": ["RCFD2143", "RCON2143"],
        "available_from": "2001Q1",
    },
    "other_assets": {
        "sql": _cx("2160"),
        "desc": "Other assets (balance-sheet residual)",
        "unit": "thousands_usd",
        "source_schedule": "RC",
        "mdrm_codes": ["RCFD2160", "RCON2160"],
        "available_from": "2001Q1",
    },
    # ── Balance sheet — liabilities (Schedule RC) ────────────────────────
    "borrowings": {
        "sql": _cx("3190"),
        "desc": "Other borrowed money (includes FHLB advances, mortgage indebtedness)",
        "unit": "thousands_usd",
        "source_schedule": "RC",
        "mdrm_codes": ["RCFD3190", "RCON3190"],
        "available_from": "2001Q1",
    },
    "sub_debt": {
        "sql": _cx("3200"),
        "desc": "Subordinated notes and debentures",
        "unit": "thousands_usd",
        "source_schedule": "RC",
        "mdrm_codes": ["RCFD3200", "RCON3200"],
        "available_from": "2001Q1",
    },
    "other_liab": {
        "sql": _cx("2930"),
        "desc": "Other liabilities",
        "unit": "thousands_usd",
        "source_schedule": "RC",
        "mdrm_codes": ["RCFD2930", "RCON2930"],
        "available_from": "2001Q1",
    },
    "total_liab": {
        "sql": _cx("2948"),
        "desc": "Total liabilities",
        "unit": "thousands_usd",
        "source_schedule": "RC",
        "mdrm_codes": ["RCFD2948", "RCON2948"],
        "available_from": "2001Q1",
    },
    # ── Balance sheet — equity detail (Schedule RC) ──────────────────────
    "retained_earnings": {
        "sql": _cx("3632"),
        "desc": "Retained earnings",
        "unit": "thousands_usd",
        "source_schedule": "RC",
        "mdrm_codes": ["RCFD3632", "RCON3632"],
        "available_from": "2001Q1",
    },
    "aoci": {
        "sql": _cx("B530"),
        "desc": "Accumulated other comprehensive income",
        "unit": "thousands_usd",
        "source_schedule": "RC",
        "mdrm_codes": ["RCFDB530", "RCONB530"],
        "available_from": "2009Q1",
    },
    # ── Intangibles detail (Schedule RC-M) ───────────────────────────────
    "goodwill": {
        "sql": _cx("3163", table="rcm"),
        "desc": "Goodwill (component of intangibles)",
        "unit": "thousands_usd",
        "source_schedule": "RC-M",
        "mdrm_codes": ["RCFD3163", "RCON3163"],
        "available_from": "2001Q1",
    },
    "msa": {
        "sql": _cx("3164", table="rcm"),
        "desc": "Mortgage servicing assets (component of intangibles)",
        "unit": "thousands_usd",
        "source_schedule": "RC-M",
        "mdrm_codes": ["RCFD3164", "RCON3164"],
        "available_from": "2001Q1",
    },
    # ── Deposit breakdown (Schedule RC-E, domestic-scoped) ───────────────
    "mmda": {
        "sql": "TRY_CAST(rce.RCON6810 AS DOUBLE)",
        "desc": "Money market deposit accounts (domestic offices)",
        "unit": "thousands_usd",
        "source_schedule": "RC-E",
        "mdrm_codes": ["RCON6810"],
        "available_from": "2001Q1",
    },
    "saving_dep": {
        "sql": "TRY_CAST(rce.RCON0352 AS DOUBLE)",
        "desc": "Other savings deposits (excl. MMDAs, domestic offices)",
        "unit": "thousands_usd",
        "source_schedule": "RC-E",
        "mdrm_codes": ["RCON0352"],
        "available_from": "2001Q1",
    },
    "td_small": {
        "sql": "TRY_CAST(rce.RCON6648 AS DOUBLE)",
        "desc": "Time deposits of less than $100,000 (domestic)",
        "unit": "thousands_usd",
        "source_schedule": "RC-E",
        "mdrm_codes": ["RCON6648"],
        "available_from": "2001Q1",
    },
    "td_mid": {
        "sql": "TRY_CAST(rce.RCONJ473 AS DOUBLE)",
        "desc": "Time deposits of $100,000 through $250,000 (domestic)",
        "unit": "thousands_usd",
        "source_schedule": "RC-E",
        "mdrm_codes": ["RCONJ473"],
        "available_from": "2010Q1",
    },
    "td_large": {
        "sql": "TRY_CAST(rce.RCONJ474 AS DOUBLE)",
        "desc": "Time deposits of more than $250,000 (domestic)",
        "unit": "thousands_usd",
        "source_schedule": "RC-E",
        "mdrm_codes": ["RCONJ474"],
        "available_from": "2010Q1",
    },
    "brokered_dep": {
        "sql": "TRY_CAST(rce.RCON2365 AS DOUBLE)",
        "desc": "Total brokered deposits (domestic)",
        "unit": "thousands_usd",
        "source_schedule": "RC-E",
        "mdrm_codes": ["RCON2365"],
        "available_from": "2001Q1",
    },
    # ── Past-due detail (Schedule RC-N) ──────────────────────────────────
    "ppd_30_89": {
        "sql": _cx("1406", table="rcn"),
        "desc": "Loans 30-89 days past due and still accruing (early-stage credit stress)",
        "unit": "thousands_usd",
        "source_schedule": "RC-N",
        "mdrm_codes": ["RCFD1406", "RCON1406"],
        "available_from": "2001Q1",
    },
    # ── Lease financing (Schedule RC-C Part I, domestic) ─────────────────
    "ln_lease": {
        "sql": "TRY_CAST(rcci.RCON2165 AS DOUBLE)",
        "desc": "Lease financing receivables, net of unearned income (domestic)",
        "unit": "thousands_usd",
        "source_schedule": "RC-C",
        "mdrm_codes": ["RCON2165"],
        "available_from": "2001Q1",
    },
    # ── Deposit structure (Schedule RC / RC-E, all domestic) ─────────────
    "demand_deposits": {
        "sql": "TRY_CAST(rce.RCON2210 AS DOUBLE)",
        "desc": "Total demand deposits (domestic offices)",
        "unit": "thousands_usd",
        "source_schedule": "RC-E",
        "mdrm_codes": ["RCON2210"],
        "available_from": "2001Q1",
    },
    "transaction_dep": {
        "sql": "TRY_CAST(rce.RCON2215 AS DOUBLE)",
        "desc": "Total transaction accounts (domestic)",
        "unit": "thousands_usd",
        "source_schedule": "RC-E",
        "mdrm_codes": ["RCON2215"],
        "available_from": "2001Q1",
    },
    "nontransaction_dep": {
        "sql": "TRY_CAST(rce.RCON2385 AS DOUBLE)",
        "desc": "Total nontransaction accounts (domestic, includes MMDAs)",
        "unit": "thousands_usd",
        "source_schedule": "RC-E",
        "mdrm_codes": ["RCON2385"],
        "available_from": "2001Q1",
    },
    "dom_deposit_ib": {
        "sql": "TRY_CAST(rc.RCON6636 AS DOUBLE)",
        "desc": "Interest-bearing deposits in domestic offices (BS line 13.a.2)",
        "unit": "thousands_usd",
        "source_schedule": "RC",
        "mdrm_codes": ["RCON6636"],
        "available_from": "2001Q1",
    },
    "dom_deposit_nib": {
        "sql": "TRY_CAST(rc.RCON6631 AS DOUBLE)",
        "desc": "Noninterest-bearing deposits in domestic offices (BS line 13.a.1)",
        "unit": "thousands_usd",
        "source_schedule": "RC",
        "mdrm_codes": ["RCON6631"],
        "available_from": "2001Q1",
    },
    # ── Quarterly averages — assets side (Schedule RC-K) ─────────────────
    "qtr_avg_loans": {
        # 031 consolidated = domestic (RCON3360) + foreign (RCFN3360); 041/051 = RCON3360 only.
        "sql": (
            "COALESCE(TRY_CAST(rck.RCON3360 AS DOUBLE), 0) "
            "+ COALESCE(TRY_CAST(rck.RCFN3360 AS DOUBLE), 0)"
        ),
        "desc": "Quarterly average total loans (consolidated = domestic + foreign for 031)",
        "unit": "thousands_usd",
        "source_schedule": "RC-K",
        "mdrm_codes": ["RCON3360", "RCFN3360"],
        "available_from": "2001Q1",
    },
    "qtr_avg_int_bearing_bal": {
        "sql": _cx("3381", table="rck"),
        "desc": "Quarterly average interest-bearing balances due from depository institutions",
        "unit": "thousands_usd",
        "source_schedule": "RC-K",
        "mdrm_codes": ["RCFD3381", "RCON3381"],
        "available_from": "2001Q1",
    },
    "qtr_avg_ffs_reverse_repo": {
        "sql": _cx("3365", table="rck"),
        "desc": "Quarterly average federal funds sold + securities purchased under resell",
        "unit": "thousands_usd",
        "source_schedule": "RC-K",
        "mdrm_codes": ["RCFD3365", "RCON3365"],
        "available_from": "2001Q1",
    },
    "qtr_avg_ust_sec": {
        "sql": _cx("B558", table="rck"),
        "desc": "Quarterly average U.S. Treasury + agency securities (excl. MBS)",
        "unit": "thousands_usd",
        "source_schedule": "RC-K",
        "mdrm_codes": ["RCFDB558", "RCONB558"],
        "available_from": "2001Q1",
    },
    "qtr_avg_mbs": {
        "sql": _cx("B559", table="rck"),
        "desc": "Quarterly average mortgage-backed securities",
        "unit": "thousands_usd",
        "source_schedule": "RC-K",
        "mdrm_codes": ["RCFDB559", "RCONB559"],
        "available_from": "2001Q1",
    },
    "qtr_avg_oth_sec": {
        "sql": _cx("B560", table="rck"),
        "desc": "Quarterly average all other debt + equity securities (excl. trading)",
        "unit": "thousands_usd",
        "source_schedule": "RC-K",
        "mdrm_codes": ["RCFDB560", "RCONB560"],
        "available_from": "2001Q1",
    },
    "qtr_avg_ln_re": {
        # Post-2008 uses 3465+3466 split; pre-2008 uses single 3385 line.
        "sql": (
            "COALESCE("
            "TRY_CAST(rck.RCON3385 AS DOUBLE), "
            "COALESCE(TRY_CAST(rck.RCON3465 AS DOUBLE), 0) "
            "+ COALESCE(TRY_CAST(rck.RCON3466 AS DOUBLE), 0)"
            ")"
        ),
        "desc": "Quarterly average real estate loans (1-4 family + other RE; domestic)",
        "unit": "thousands_usd",
        "source_schedule": "RC-K",
        "mdrm_codes": ["RCON3385", "RCON3465", "RCON3466"],
        "available_from": "2001Q1",
    },
    "qtr_avg_ln_ci": {
        "sql": "TRY_CAST(rck.RCON3387 AS DOUBLE)",
        "desc": "Quarterly average commercial and industrial loans (domestic)",
        "unit": "thousands_usd",
        "source_schedule": "RC-K",
        "mdrm_codes": ["RCON3387"],
        "available_from": "2001Q1",
    },
    "qtr_avg_lease": {
        "sql": _cx("3484", table="rck"),
        "desc": "Quarterly average lease financing receivables",
        "unit": "thousands_usd",
        "source_schedule": "RC-K",
        "mdrm_codes": ["RCFD3484", "RCON3484"],
        "available_from": "2001Q1",
    },
    "qtr_avg_trans_dep": {
        "sql": "TRY_CAST(rck.RCON3485 AS DOUBLE)",
        "desc": "Quarterly average interest-bearing transaction accounts (domestic)",
        "unit": "thousands_usd",
        "source_schedule": "RC-K",
        "mdrm_codes": ["RCON3485"],
        "available_from": "2001Q1",
    },
    "qtr_avg_savings_dep": {
        "sql": "TRY_CAST(rck.RCONB563 AS DOUBLE)",
        "desc": "Quarterly average savings deposits (incl. MMDAs; domestic)",
        "unit": "thousands_usd",
        "source_schedule": "RC-K",
        "mdrm_codes": ["RCONB563"],
        "available_from": "2001Q1",
    },
    "qtr_avg_time_dep_le250k": {
        "sql": "TRY_CAST(rck.RCONHK16 AS DOUBLE)",
        "desc": "Quarterly average time deposits of $250K or less (domestic)",
        "unit": "thousands_usd",
        "source_schedule": "RC-K",
        "mdrm_codes": ["RCONHK16"],
        "available_from": "2017Q1",
    },
    "qtr_avg_time_dep_gt250k": {
        "sql": "TRY_CAST(rck.RCONHK17 AS DOUBLE)",
        "desc": "Quarterly average time deposits of more than $250K (domestic)",
        "unit": "thousands_usd",
        "source_schedule": "RC-K",
        "mdrm_codes": ["RCONHK17"],
        "available_from": "2017Q1",
    },
    "qtr_avg_ffpurch_repo": {
        "sql": _cx("3353", table="rck"),
        "desc": "Quarterly average federal funds purchased + securities sold under repurchase",
        "unit": "thousands_usd",
        "source_schedule": "RC-K",
        "mdrm_codes": ["RCFD3353", "RCON3353"],
        "available_from": "2001Q1",
    },
    "qtr_avg_othbor": {
        "sql": _cx("3355", table="rck"),
        "desc": "Quarterly average other borrowed money",
        "unit": "thousands_usd",
        "source_schedule": "RC-K",
        "mdrm_codes": ["RCFD3355", "RCON3355"],
        "available_from": "2001Q1",
    },
}

# ── Income statement (all YTD except num_employees) ──────────────────────────

IS_CONCEPTS: dict[str, dict] = {
    "ytdint_inc": {
        "sql": "TRY_CAST(ri.RIAD4107 AS DOUBLE)",
        "desc": "Total interest income (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "RI",
        "mdrm_codes": ["RIAD4107"],
        "available_from": "2001Q1",
    },
    "ytdint_exp": {
        "sql": "TRY_CAST(ri.RIAD4073 AS DOUBLE)",
        "desc": "Total interest expense (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "RI",
        "mdrm_codes": ["RIAD4073"],
        "available_from": "2001Q1",
    },
    "ytdint_inc_net": {
        "sql": "TRY_CAST(ri.RIAD4074 AS DOUBLE)",
        "desc": "Net interest income (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "RI",
        "mdrm_codes": ["RIAD4074"],
        "available_from": "2001Q1",
    },
    "ytdnonint_inc": {
        "sql": "TRY_CAST(ri.RIAD4079 AS DOUBLE)",
        "desc": "Total noninterest income (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "RI",
        "mdrm_codes": ["RIAD4079"],
        "available_from": "2001Q1",
    },
    "ytdnonint_exp": {
        "sql": "TRY_CAST(ri.RIAD4093 AS DOUBLE)",
        "desc": "Total noninterest expense (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "RI",
        "mdrm_codes": ["RIAD4093"],
        "available_from": "2001Q1",
    },
    "ytdllprov": {
        "sql": "TRY_CAST(ribii.RIAD4230 AS DOUBLE)",
        "desc": "Provision for loan and lease losses (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "RI-B II",
        "mdrm_codes": ["RIAD4230"],
        "available_from": "2001Q1",
    },
    "ytdtradrev_inc": {
        "sql": "TRY_CAST(ri.RIADA220 AS DOUBLE)",
        "desc": "Trading revenue (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "RI",
        "mdrm_codes": ["RIADA220"],
        "available_from": "2001Q1",
    },
    "ytdinc_before_disc_op": {
        "sql": "TRY_CAST(ri.RIAD4300 AS DOUBLE)",
        "desc": "Income before discontinued operations (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "RI",
        "mdrm_codes": ["RIAD4300"],
        "available_from": "2001Q1",
    },
    "ytdinc_taxes": {
        "sql": "TRY_CAST(ri.RIAD4302 AS DOUBLE)",
        "desc": "Taxes on income before extraordinary items (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "RI",
        "mdrm_codes": ["RIAD4302"],
        "available_from": "2001Q1",
    },
    "ytdnetinc": {
        "sql": "TRY_CAST(ri.RIAD4340 AS DOUBLE)",
        "desc": "Net income (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "RI",
        "mdrm_codes": ["RIAD4340"],
        "available_from": "2001Q1",
    },
    "ytdcommdividend": {
        "sql": "TRY_CAST(ria.RIAD4460 AS DOUBLE)",
        "desc": "Cash dividends declared on common stock (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "RI-A",
        "mdrm_codes": ["RIAD4460"],
        "available_from": "2001Q1",
    },
    "num_employees": {
        "sql": "TRY_CAST(ri.RIAD4150 AS INTEGER)",
        "desc": "Full-time equivalent employees (point-in-time, NOT YTD)",
        "unit": "count",
        "source_schedule": "RI",
        "mdrm_codes": ["RIAD4150"],
        "available_from": "2001Q1",
    },
    # ── Income statement — expense detail (Schedule RI) ──────────────────
    "ytdsalaries": {
        "sql": "TRY_CAST(ri.RIAD4135 AS DOUBLE)",
        "desc": "Salaries and employee benefits (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "RI",
        "mdrm_codes": ["RIAD4135"],
        "available_from": "2001Q1",
    },
    "ytdprem_exp": {
        "sql": "TRY_CAST(ri.RIAD4217 AS DOUBLE)",
        "desc": "Expenses of premises and fixed assets (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "RI",
        "mdrm_codes": ["RIAD4217"],
        "available_from": "2001Q1",
    },
    "ytdoth_nonint_exp": {
        "sql": "TRY_CAST(ri.RIAD4092 AS DOUBLE)",
        "desc": "Other noninterest expense (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "RI",
        "mdrm_codes": ["RIAD4092"],
        "available_from": "2001Q1",
    },
    # ── Income statement — revenue detail (Schedule RI) ──────────────────
    "ytdsvc_charges": {
        "sql": "TRY_CAST(ri.RIAD4080 AS DOUBLE)",
        "desc": "Service charges on deposit accounts (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "RI",
        "mdrm_codes": ["RIAD4080"],
        "available_from": "2001Q1",
    },
    "ytdgain_afs": {
        "sql": "TRY_CAST(ri.RIAD3196 AS DOUBLE)",
        "desc": "Realized gain (loss) on available-for-sale debt securities (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "RI",
        "mdrm_codes": ["RIAD3196"],
        "available_from": "2001Q1",
    },
    # ── Dividends detail (Schedule RI-A) ─────────────────────────────────
    "ytdprefdividend": {
        "sql": "TRY_CAST(ria.RIAD4470 AS DOUBLE)",
        "desc": "Cash dividends declared on preferred stock (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "RI-A",
        "mdrm_codes": ["RIAD4470"],
        "available_from": "2001Q1",
    },
    # ── Charge-offs / recoveries (Schedule RI-B Part I) ──────────────────
    "ytdchargeoffs": {
        "sql": "TRY_CAST(ribi.RIAD4635 AS DOUBLE)",
        "desc": "Total charge-offs on loans and leases (YTD, col A)",
        "unit": "thousands_usd",
        "source_schedule": "RI-B I",
        "mdrm_codes": ["RIAD4635"],
        "available_from": "2001Q1",
    },
    "ytdrecoveries": {
        "sql": "TRY_CAST(ribi.RIAD4605 AS DOUBLE)",
        "desc": "Total recoveries on loans and leases (YTD, col B)",
        "unit": "thousands_usd",
        "source_schedule": "RI-B I",
        "mdrm_codes": ["RIAD4605"],
        "available_from": "2001Q1",
    },
    # ── Interest income detail (Schedule RI) ─────────────────────────────
    "ytdint_inc_ln": {
        "sql": "TRY_CAST(ri.RIAD4010 AS DOUBLE)",
        "desc": "Total interest and fee income on loans (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "RI",
        "mdrm_codes": ["RIAD4010"],
        "available_from": "2001Q1",
    },
    "ytdint_inc_ln_re": {
        "sql": (
            "COALESCE(TRY_CAST(ri.RIAD4435 AS DOUBLE), 0) "
            "+ COALESCE(TRY_CAST(ri.RIAD4436 AS DOUBLE), 0)"
        ),
        "desc": "Interest and fee income on real estate loans (1-4 family + other RE; YTD)",
        "unit": "thousands_usd",
        "source_schedule": "RI",
        "mdrm_codes": ["RIAD4435", "RIAD4436"],
        "available_from": "2008Q1",
    },
    "ytdint_inc_ln_ci": {
        "sql": "TRY_CAST(ri.RIAD4012 AS DOUBLE)",
        "desc": "Interest and fee income on C&I loans (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "RI",
        "mdrm_codes": ["RIAD4012"],
        "available_from": "2001Q1",
    },
    "ytdint_inc_ln_cc": {
        "sql": "TRY_CAST(ri.RIADB485 AS DOUBLE)",
        "desc": "Interest and fee income on credit card loans (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "RI",
        "mdrm_codes": ["RIADB485"],
        "available_from": "2001Q1",
    },
    "ytdint_inc_ln_othcons": {
        "sql": "TRY_CAST(ri.RIADB486 AS DOUBLE)",
        "desc": "Interest and fee income on other consumer loans (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "RI",
        "mdrm_codes": ["RIADB486"],
        "available_from": "2001Q1",
    },
    "ytdint_inc_sec_ust": {
        "sql": "TRY_CAST(ri.RIADB488 AS DOUBLE)",
        "desc": "Interest/dividend income on U.S. Treasury + agency securities excl. MBS (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "RI",
        "mdrm_codes": ["RIADB488"],
        "available_from": "2001Q1",
    },
    "ytdint_inc_sec_mbs": {
        "sql": "TRY_CAST(ri.RIADB489 AS DOUBLE)",
        "desc": "Interest/dividend income on mortgage-backed securities (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "RI",
        "mdrm_codes": ["RIADB489"],
        "available_from": "2001Q1",
    },
    "ytdint_inc_sec_oth": {
        "sql": "TRY_CAST(ri.RIAD4060 AS DOUBLE)",
        "desc": "Interest/dividend income on all other securities (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "RI",
        "mdrm_codes": ["RIAD4060"],
        "available_from": "2001Q1",
    },
    "ytdint_inc_ffrepo": {
        "sql": "TRY_CAST(ri.RIAD4020 AS DOUBLE)",
        "desc": "Interest income on fed funds sold + securities purchased under resell (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "RI",
        "mdrm_codes": ["RIAD4020"],
        "available_from": "2001Q1",
    },
    "ytdint_inc_lease": {
        "sql": "TRY_CAST(ri.RIAD4065 AS DOUBLE)",
        "desc": "Income from lease financing receivables (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "RI",
        "mdrm_codes": ["RIAD4065"],
        "available_from": "2001Q1",
    },
    "ytdint_inc_ibb": {
        "sql": "TRY_CAST(ri.RIAD4115 AS DOUBLE)",
        "desc": "Interest income on balances due from depository institutions (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "RI",
        "mdrm_codes": ["RIAD4115"],
        "available_from": "2001Q1",
    },
    # ── Interest expense detail (Schedule RI) ────────────────────────────
    "ytdint_exp_trans_dep": {
        "sql": "TRY_CAST(ri.RIAD4508 AS DOUBLE)",
        "desc": "Interest expense on transaction accounts (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "RI",
        "mdrm_codes": ["RIAD4508"],
        "available_from": "2001Q1",
    },
    "ytdint_exp_savings_dep": {
        "sql": "TRY_CAST(ri.RIAD0093 AS DOUBLE)",
        "desc": "Interest expense on savings deposits incl. MMDAs (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "RI",
        "mdrm_codes": ["RIAD0093"],
        "available_from": "2001Q1",
    },
    "ytdint_exp_time_le250k": {
        "sql": "TRY_CAST(ri.RIADHK03 AS DOUBLE)",
        "desc": "Interest expense on time deposits of $250K or less (YTD, domestic)",
        "unit": "thousands_usd",
        "source_schedule": "RI",
        "mdrm_codes": ["RIADHK03"],
        "available_from": "2017Q1",
    },
    "ytdint_exp_time_gt250k": {
        "sql": "TRY_CAST(ri.RIADHK04 AS DOUBLE)",
        "desc": "Interest expense on time deposits of more than $250K (YTD, domestic)",
        "unit": "thousands_usd",
        "source_schedule": "RI",
        "mdrm_codes": ["RIADHK04"],
        "available_from": "2017Q1",
    },
    "ytdint_exp_ffrepo": {
        "sql": "TRY_CAST(ri.RIAD4180 AS DOUBLE)",
        "desc": "Interest expense on fed funds purchased + securities sold under repurchase (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "RI",
        "mdrm_codes": ["RIAD4180"],
        "available_from": "2001Q1",
    },
    # ── Other income / gains (Schedule RI) ───────────────────────────────
    "ytdfiduc_inc": {
        "sql": "TRY_CAST(ri.RIAD4070 AS DOUBLE)",
        "desc": "Income from fiduciary activities (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "RI",
        "mdrm_codes": ["RIAD4070"],
        "available_from": "2001Q1",
    },
    "ytdgain_htm": {
        "sql": "TRY_CAST(ri.RIAD3521 AS DOUBLE)",
        "desc": "Realized gain (loss) on held-to-maturity securities (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "RI",
        "mdrm_codes": ["RIAD3521"],
        "available_from": "2001Q1",
    },
}


# ── Identity / metadata (filers_panel) ───────────────────────────────────────

FILERS_CONCEPTS: dict[str, dict] = {
    "id_rssd": {
        "sql": "TRY_CAST(cf.IDRSSD AS BIGINT)",
        "desc": "RSSD ID (unique Federal Reserve identifier, BIGINT for CFLV/NIC joins)",
        "unit": "identifier",
        "source_schedule": "POR",
        "mdrm_codes": ["IDRSSD"],
        "available_from": "2001Q1",
    },
    "date": {
        "sql": (
            "make_date(cf.activity_year, "
            "CASE cf.activity_quarter "
            "WHEN 1 THEN 3 WHEN 2 THEN 6 WHEN 3 THEN 9 WHEN 4 THEN 12 END, "
            "CASE cf.activity_quarter "
            "WHEN 1 THEN 31 WHEN 2 THEN 30 WHEN 3 THEN 30 WHEN 4 THEN 31 END)"
        ),
        "desc": "Quarter-end date",
        "unit": "date",
        "source_schedule": "POR",
        "mdrm_codes": [],
        "available_from": "2001Q1",
    },
    "form_type": {
        "sql": 'cf."Financial Institution Filing Type"',
        "desc": "FFIEC reporting form: 031 / 041 / 051",
        "unit": "text",
        "source_schedule": "POR",
        "mdrm_codes": [],
        "available_from": "2001Q1",
    },
    "nm_lgl": {
        "sql": 'cf."Financial Institution Name"',
        "desc": "Legal name of institution",
        "unit": "text",
        "source_schedule": "POR",
        "mdrm_codes": [],
        "available_from": "2001Q1",
    },
    "city": {
        "sql": 'cf."Financial Institution City"',
        "desc": "City of institution",
        "unit": "text",
        "source_schedule": "POR",
        "mdrm_codes": [],
        "available_from": "2001Q1",
    },
    "state_abbr_nm": {
        "sql": 'cf."Financial Institution State"',
        "desc": "State abbreviation",
        "unit": "text",
        "source_schedule": "POR",
        "mdrm_codes": [],
        "available_from": "2001Q1",
    },
    "zip_cd": {
        "sql": 'cf."Financial Institution Zip Code"',
        "desc": "ZIP code",
        "unit": "text",
        "source_schedule": "POR",
        "mdrm_codes": [],
        "available_from": "2001Q1",
    },
}


# ── Schema for harmonized_metadata reference table ───────────────────────────

HARMONIZED_METADATA_DDL = """
CREATE TABLE IF NOT EXISTS harmonized_metadata (
    variable_name VARCHAR,
    panel VARCHAR,
    description VARCHAR,
    unit VARCHAR,
    source_schedule VARCHAR,
    mdrm_codes VARCHAR,
    formula VARCHAR,
    available_from VARCHAR,
    PRIMARY KEY (variable_name)
)
"""
