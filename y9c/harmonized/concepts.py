"""
Y-9C harmonized variable definitions.

Mirrors call-reports-FFIEC and CFLV variable names where possible so cross-
dataset queries are portable. All MDRM 4-character codes are accessed via the
BHCK prefix (Y-9C consolidated). Sniff confirmed BHCK exists for every key
balance-sheet/income-statement code across 2000-2025 in the FFIEC NIC bulk
files. For codes that occasionally drop BHCK in favor of an alternate prefix
in some quarters (rare; observed for a few RI items), use ``_bhc(code, alts=...)``.

All monetary values are in **thousands of USD** (Y-9C convention, identical
to call reports).
"""
from __future__ import annotations


def _bhc(code: str, alts: tuple[str, ...] = ()) -> str:
    """COALESCE TRY_CAST across BHCK + optional alternate prefixes.

    Always uses the y9c_raw view (alias `y`).
    """
    parts = [f"TRY_CAST(y.BHCK{code} AS DOUBLE)"]
    for p in alts:
        parts.append(f"TRY_CAST(y.{p}{code} AS DOUBLE)")
    if len(parts) == 1:
        return parts[0]
    return f"COALESCE({', '.join(parts)})"


# ── Balance sheet ─────────────────────────────────────────────────────────────

BS_CONCEPTS_Y9C: dict[str, dict] = {
    "assets": {
        "sql": _bhc("2170"),
        "desc": "Total consolidated assets",
        "unit": "thousands_usd",
        "source_schedule": "HC",
        "mdrm_codes": ["BHCK2170"],
        "available_from": "2000Q1",
    },
    "deposits": {
        "sql": (
            "COALESCE(TRY_CAST(y.BHDM6631 AS DOUBLE), 0) + "
            "COALESCE(TRY_CAST(y.BHDM6636 AS DOUBLE), 0) + "
            "COALESCE(TRY_CAST(y.BHFN6631 AS DOUBLE), 0) + "
            "COALESCE(TRY_CAST(y.BHFN6636 AS DOUBLE), 0)"
        ),
        "desc": "Total deposits (consolidated, NIB+IB across domestic+foreign offices)",
        "unit": "thousands_usd",
        "source_schedule": "HC",
        "mdrm_codes": ["BHDM6631", "BHDM6636", "BHFN6631", "BHFN6636"],
        "available_from": "2000Q1",
    },
    "domestic_dep": {
        "sql": (
            "COALESCE(TRY_CAST(y.BHDM6631 AS DOUBLE), 0) + "
            "COALESCE(TRY_CAST(y.BHDM6636 AS DOUBLE), 0)"
        ),
        "desc": "Deposits in domestic offices (NIB + IB)",
        "unit": "thousands_usd",
        "source_schedule": "HC",
        "mdrm_codes": ["BHDM6631", "BHDM6636"],
        "available_from": "2000Q1",
    },
    "foreign_dep": {
        "sql": (
            "COALESCE(TRY_CAST(y.BHFN6631 AS DOUBLE), 0) + "
            "COALESCE(TRY_CAST(y.BHFN6636 AS DOUBLE), 0)"
        ),
        "desc": "Deposits in foreign offices (NIB + IB)",
        "unit": "thousands_usd",
        "source_schedule": "HC",
        "mdrm_codes": ["BHFN6631", "BHFN6636"],
        "available_from": "2000Q1",
    },
    "equity": {
        "sql": (
            f"COALESCE({_bhc('G105')}, {_bhc('3210')})"
        ),
        "desc": "Total equity capital (BHCKG105 from 2009Q1, BHCK3210 prior)",
        "unit": "thousands_usd",
        "source_schedule": "HC",
        "mdrm_codes": ["BHCKG105", "BHCK3210"],
        "available_from": "2000Q1",
    },
    "ln_tot": {
        "sql": _bhc("2122"),
        "desc": "Total loans and leases, net of unearned income (consolidated)",
        "unit": "thousands_usd",
        "source_schedule": "HC-C",
        "mdrm_codes": ["BHCK2122"],
        "available_from": "2000Q1",
    },
    "ln_tot_gross": {
        "sql": (
            f"COALESCE({_bhc('2122')}, 0) + COALESCE({_bhc('2123')}, 0)"
        ),
        "desc": "Total loans and leases, gross (BHCK2122 + BHCK2123 unearned income)",
        "unit": "thousands_usd",
        "source_schedule": "HC-C",
        "mdrm_codes": ["BHCK2122", "BHCK2123"],
        "available_from": "2000Q1",
    },
    "ln_re": {
        "sql": _bhc("1410"),
        "desc": "Loans secured by real estate (HC-C item 1)",
        "unit": "thousands_usd",
        "source_schedule": "HC-C",
        "mdrm_codes": ["BHCK1410"],
        "available_from": "2000Q1",
    },
    "ln_ci": {
        "sql": (
            f"COALESCE({_bhc('1763')}, 0) + COALESCE({_bhc('1764')}, 0)"
        ),
        "desc": "Commercial and industrial loans (US + non-US addressees, BHCK1763+1764)",
        "unit": "thousands_usd",
        "source_schedule": "HC-C",
        "mdrm_codes": ["BHCK1763", "BHCK1764"],
        "available_from": "2000Q1",
    },
    "ln_cc": {
        "sql": _bhc("B538"),
        "desc": "Credit card loans",
        "unit": "thousands_usd",
        "source_schedule": "HC-C",
        "mdrm_codes": ["BHCKB538"],
        "available_from": "2001Q1",
    },
    "htmsec_ac": {
        "sql": _bhc("1754"),
        "desc": "Held-to-maturity securities (amortized cost)",
        "unit": "thousands_usd",
        "source_schedule": "HC-B",
        "mdrm_codes": ["BHCK1754"],
        "available_from": "2000Q1",
    },
    "afssec_fv": {
        "sql": _bhc("1773"),
        "desc": "Available-for-sale securities (fair value)",
        "unit": "thousands_usd",
        "source_schedule": "HC-B",
        "mdrm_codes": ["BHCK1773"],
        "available_from": "2000Q1",
    },
    "securities": {
        "sql": (
            f"COALESCE({_bhc('1754')}, 0) + COALESCE({_bhc('1773')}, 0)"
        ),
        "desc": "Total securities (HTM amortized cost + AFS fair value)",
        "unit": "thousands_usd",
        "source_schedule": "HC-B",
        "mdrm_codes": ["BHCK1754", "BHCK1773"],
        "available_from": "2000Q1",
    },
    "trading_assets": {
        "sql": _bhc("3545"),
        "desc": "Trading assets",
        "unit": "thousands_usd",
        "source_schedule": "HC",
        "mdrm_codes": ["BHCK3545"],
        "available_from": "2000Q1",
    },
    "premises": {
        "sql": _bhc("2145"),
        "desc": "Premises and fixed assets",
        "unit": "thousands_usd",
        "source_schedule": "HC",
        "mdrm_codes": ["BHCK2145"],
        "available_from": "2000Q1",
    },
    "intangibles": {
        "sql": _bhc("2143"),
        "desc": "Intangible assets (incl. goodwill, MSAs, other)",
        "unit": "thousands_usd",
        "source_schedule": "HC",
        "mdrm_codes": ["BHCK2143"],
        "available_from": "2000Q1",
    },
    "goodwill": {
        "sql": _bhc("3163"),
        "desc": "Goodwill",
        "unit": "thousands_usd",
        "source_schedule": "HC-M",
        "mdrm_codes": ["BHCK3163"],
        "available_from": "2001Q1",
    },
    "other_assets": {
        "sql": _bhc("2160"),
        "desc": "Other assets",
        "unit": "thousands_usd",
        "source_schedule": "HC",
        "mdrm_codes": ["BHCK2160"],
        "available_from": "2000Q1",
    },
    "borrowings": {
        "sql": _bhc("3190"),
        "desc": "Other borrowed money",
        "unit": "thousands_usd",
        "source_schedule": "HC",
        "mdrm_codes": ["BHCK3190"],
        "available_from": "2000Q1",
    },
    "sub_debt": {
        "sql": _bhc("4062"),
        "desc": "Subordinated notes and debentures",
        "unit": "thousands_usd",
        "source_schedule": "HC",
        "mdrm_codes": ["BHCK4062"],
        "available_from": "2000Q1",
    },
    "total_liab": {
        "sql": _bhc("2948"),
        "desc": "Total liabilities",
        "unit": "thousands_usd",
        "source_schedule": "HC",
        "mdrm_codes": ["BHCK2948"],
        "available_from": "2000Q1",
    },
    "retained_earnings": {
        "sql": _bhc("3247"),
        "desc": "Retained earnings",
        "unit": "thousands_usd",
        "source_schedule": "HC",
        "mdrm_codes": ["BHCK3247"],
        "available_from": "2000Q1",
    },
    "aoci": {
        "sql": _bhc("B530"),
        "desc": "Accumulated other comprehensive income",
        "unit": "thousands_usd",
        "source_schedule": "HC",
        "mdrm_codes": ["BHCKB530"],
        "available_from": "2009Q1",
    },
    "ffs": {
        "sql": _bhc("B987"),
        "desc": "Federal funds sold",
        "unit": "thousands_usd",
        "source_schedule": "HC",
        "mdrm_codes": ["BHCKB987"],
        "available_from": "2002Q1",
    },
    "reverse_repo": {
        "sql": _bhc("B989"),
        "desc": "Securities purchased under agreements to resell",
        "unit": "thousands_usd",
        "source_schedule": "HC",
        "mdrm_codes": ["BHCKB989"],
        "available_from": "2002Q1",
    },
    "ffp": {
        "sql": _bhc("B993"),
        "desc": "Federal funds purchased",
        "unit": "thousands_usd",
        "source_schedule": "HC",
        "mdrm_codes": ["BHCKB993"],
        "available_from": "2002Q1",
    },
    "repo": {
        "sql": _bhc("B995"),
        "desc": "Securities sold under agreements to repurchase",
        "unit": "thousands_usd",
        "source_schedule": "HC",
        "mdrm_codes": ["BHCKB995"],
        "available_from": "2002Q1",
    },
    "qtr_avg_assets": {
        "sql": _bhc("3368"),
        "desc": "Quarterly average total consolidated assets",
        "unit": "thousands_usd",
        "source_schedule": "HC-K",
        "mdrm_codes": ["BHCK3368"],
        "available_from": "2000Q1",
    },
    "llres": {
        "sql": _bhc("3123"),
        "desc": "Allowance for loan and lease losses",
        "unit": "thousands_usd",
        "source_schedule": "HC",
        "mdrm_codes": ["BHCK3123"],
        "available_from": "2000Q1",
    },
}


# ── Income statement (all YTD; quarterly flow derived via LAG) ───────────────

IS_CONCEPTS_Y9C: dict[str, dict] = {
    "ytdint_inc": {
        "sql": _bhc("4107"),
        "desc": "Total interest income (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "HI",
        "mdrm_codes": ["BHCK4107"],
        "available_from": "2000Q1",
    },
    "ytdint_exp": {
        "sql": _bhc("4073"),
        "desc": "Total interest expense (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "HI",
        "mdrm_codes": ["BHCK4073"],
        "available_from": "2000Q1",
    },
    "ytdint_inc_net": {
        "sql": _bhc("4074"),
        "desc": "Net interest income (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "HI",
        "mdrm_codes": ["BHCK4074"],
        "available_from": "2000Q1",
    },
    "ytdnonint_inc": {
        "sql": _bhc("4079"),
        "desc": "Total noninterest income (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "HI",
        "mdrm_codes": ["BHCK4079"],
        "available_from": "2000Q1",
    },
    "ytdnonint_exp": {
        "sql": _bhc("4093"),
        "desc": "Total noninterest expense (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "HI",
        "mdrm_codes": ["BHCK4093"],
        "available_from": "2000Q1",
    },
    "ytdllprov": {
        "sql": _bhc("4230"),
        "desc": "Provision for loan and lease losses (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "HI",
        "mdrm_codes": ["BHCK4230"],
        "available_from": "2000Q1",
    },
    "ytdnetinc": {
        "sql": _bhc("4340", alts=("BHBC",)),
        "desc": "Net income attributable to BHC (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "HI",
        "mdrm_codes": ["BHCK4340", "BHBC4340"],
        "available_from": "2000Q1",
    },
    "ytdsalaries": {
        "sql": _bhc("4135"),
        "desc": "Salaries and employee benefits (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "HI",
        "mdrm_codes": ["BHCK4135"],
        "available_from": "2000Q1",
    },
    "ytdinc_taxes": {
        "sql": _bhc("4302"),
        "desc": "Applicable income taxes (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "HI",
        "mdrm_codes": ["BHCK4302"],
        "available_from": "2000Q1",
    },
    "ytdcommdividend": {
        "sql": _bhc("4460"),
        "desc": "Cash dividends declared on common stock (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "HI-A",
        "mdrm_codes": ["BHCK4460"],
        "available_from": "2000Q1",
    },
    "ytdprefdividend": {
        "sql": _bhc("4470"),
        "desc": "Cash dividends declared on preferred stock (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "HI-A",
        "mdrm_codes": ["BHCK4470"],
        "available_from": "2000Q1",
    },
    "ytdtradrev_inc": {
        "sql": _bhc("A220"),
        "desc": "Trading revenue (YTD)",
        "unit": "thousands_usd",
        "source_schedule": "HI",
        "mdrm_codes": ["BHCKA220"],
        "available_from": "2001Q1",
    },
}


# ── Identity / metadata fields ───────────────────────────────────────────────

IDENTITY_CONCEPTS: dict[str, dict] = {
    "id_rssd": {
        "sql": "TRY_CAST(y.RSSD9001 AS BIGINT)",
        "desc": "RSSD ID of the BHC (Federal Reserve identifier)",
        "unit": "identifier",
        "source_schedule": "POR",
        "mdrm_codes": ["RSSD9001"],
        "available_from": "2000Q1",
    },
    "rssd9999_raw": {
        "sql": "y.RSSD9999",
        "desc": "Raw reporting period date string (YYYYMMDD)",
        "unit": "text",
        "source_schedule": "POR",
        "mdrm_codes": ["RSSD9999"],
        "available_from": "2000Q1",
    },
}


HARMONIZED_METADATA_DDL_Y9C = """
CREATE TABLE IF NOT EXISTS harmonized_metadata_y9c (
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
