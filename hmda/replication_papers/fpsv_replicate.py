#!/usr/bin/env python3
"""
Replication of Fuster, Plosser, Schnabl, Vickery (2019, RFS)
"The Role of Technology in Mortgage Lending"

Replicates Table 1 (Top-20 originators in 2016) and
Table 2 (Descriptive stats by lender type, 2010-mid 2016).

NOTES:
- Paper sample: Jan 2010 to June 2016; we use full year 2016 (inflates obs ~8M)
- Processing time rows CANNOT be replicated: requires restricted HMDA data
  with exact application/action dates (public HMDA has year only)
- Sample filter: loan_purpose IN ('1','3') -- purchase + refinance only
  (purchase + refi shares sum to exactly 1.00 in paper, confirming this filter)
- Jumbo flag: loan_amount > 417,000 (national CLL baseline 2010-2016)
- FinTech lenders identified by name-matching in Avery crosswalk
- Bank vs nonbank: agency_code in (1,2,3,5,9) = bank; agency_code=7 = nonbank

Run from repo root: C:/OneDrive/github/empirical-data-construction/
"""

import sys
from pathlib import Path
import duckdb
import pandas as pd

REPO_ROOT = Path("C:/OneDrive/github/empirical-data-construction")
sys.path.insert(0, str(REPO_ROOT))
from config import get_duckdb_path, DUCKDB_THREADS, DUCKDB_MEMORY_LIMIT  # noqa: E402

OUT_DIR = REPO_ROOT / "hmda" / "replication_papers"

# ----------------------------------------------------------------
# Connect
# ----------------------------------------------------------------
conn = duckdb.connect(str(get_duckdb_path()), read_only=True)
conn.execute(f"PRAGMA threads={DUCKDB_THREADS}")
conn.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY_LIMIT}'")
print("Connected to DuckDB:", get_duckdb_path())

# ================================================================
# STEP 1 -- Find FinTech lender (respondent_id, agency_code) pairs
# ================================================================
# Paper's FinTech lenders at end of 2016 (Table 1):
#   Quicken Loans, LoanDepot.com, Guaranteed Rate, Movement Mortgage,
#   Everett Financial (Supreme Lending), Avex Funding (Better.com)
# Earlier FinTech lenders (active during 2010-2016):
#   same set; paper says there were 2 in 2010 growing to 18 by 2017
#   but the 12 that adopted in first half of 2017 are NOT in our sample

FT_KEYWORDS = [
    "quicken",
    "loandepot",
    "loan depot",
    "guaranteed rate",
    "movement mortgage",
    "everett financial",
    "supreme lending",
    "avex fund",
    "better.com",
    "better mortgage",
]

like_clauses = " OR ".join([
    f"LOWER(respondent_name) LIKE '%{kw}%'" for kw in FT_KEYWORDS
])

ft_search_sql = f"""
SELECT DISTINCT
    respondent_id,
    CAST(agency_code AS VARCHAR)  AS agency_code,
    respondent_name,
    MIN(activity_year)            AS first_hmda_year
FROM avery_crosswalk
WHERE activity_year BETWEEN 2010 AND 2016
  AND ({like_clauses})
GROUP BY respondent_id, agency_code, respondent_name
ORDER BY respondent_name, respondent_id
"""

df_ft = conn.execute(ft_search_sql).df()
print("\n=== FinTech Lender Candidates (from Avery crosswalk) ===")
print(df_ft.to_string(index=False))

# Year-conditional FinTech adoption dates (from paper Table 1)
# Key: (respondent_id, agency_code)  Value: first year classified as FinTech
FINTECH_ADOPTION = {
    ("7197000003", "7"): 2010,   # Quicken Loans
    ("36-4327855", "7"): 2010,   # Guaranteed Rate Inc
    ("26-0595342", "7"): 2014,   # Movement Mortgage LLC
    ("26-4599244", "7"): 2016,   # LoanDepot.com LLC
    ("75-2695327", "7"): 2016,   # Everett Financial (Supreme) -- newer ID
    ("1722400006", "7"): 2016,   # Everett Financial older ID
    ("87-0691650", "7"): 2016,   # Avex Funding (Better.com)
}

# Build Python set for row-level classification (for Table 1, year=2016 → all qualify)
ft_set = set(FINTECH_ADOPTION.keys())

# Build year-conditional SQL CASE expression
# Each FinTech lender is FinTech only from their adoption year onwards
ft_when_clauses = "\n        ".join([
    f"WHEN l.respondent_id = '{rid}' AND l.agency_code = '{ac}' AND l.year >= {yr} THEN 'FinTech'"
    for (rid, ac), yr in FINTECH_ADOPTION.items()
])

lender_type_case = f"""
    CASE
        {ft_when_clauses}
        WHEN l.agency_code IN ('1','2','3','5','9') THEN 'Bank'
        WHEN l.agency_code = '7'                    THEN 'Nonbank_NonFT'
        ELSE 'Other'
    END"""

# ================================================================
# TABLE 1 -- Top-20 originators in 2016 by HMDA volume
# ================================================================
print("\n=== Computing Table 1 (Top-20 originators, 2016) ===")

t1_sql = """
WITH total_vol AS (
    SELECT SUM(TRY_CAST(loan_amount AS DOUBLE)) AS tv
    FROM lar_panel
    WHERE year = 2016
      AND action_taken = '1'
),
lender_vol AS (
    SELECT
        l.respondent_id,
        l.agency_code,
        COALESCE(av.respondent_name, l.respondent_id) AS lender_name,
        SUM(TRY_CAST(l.loan_amount AS DOUBLE))        AS vol
    FROM lar_panel l
    LEFT JOIN avery_crosswalk av
           ON l.respondent_id                    = av.respondent_id
          AND TRY_CAST(l.agency_code AS INTEGER) = av.agency_code
          AND av.activity_year                   = 2016
    WHERE l.year = 2016
      AND l.action_taken = '1'
    GROUP BY l.respondent_id, l.agency_code,
             COALESCE(av.respondent_name, l.respondent_id)
)
SELECT
    ROW_NUMBER() OVER (ORDER BY lv.vol DESC) AS rank,
    lv.lender_name,
    lv.agency_code,
    lv.respondent_id,
    ROUND(lv.vol / 1e9,                    2) AS volume_bn,
    ROUND(100.0 * lv.vol / tv.tv,          2) AS mkt_share_pct
FROM lender_vol lv
CROSS JOIN total_vol tv
ORDER BY lv.vol DESC
LIMIT 60
"""

df_t1_raw = conn.execute(t1_sql).df()


def classify_type(row):
    rid = str(row["respondent_id"])
    ac  = str(row["agency_code"])
    # For Table 1 (year=2016): all FinTechs with adoption <= 2016 qualify
    if (rid, ac) in FINTECH_ADOPTION:
        return "FinTech"
    if ac in ("1", "2", "3", "5", "9"):
        return "Bank"
    return "Mtg"


df_t1_raw["type"] = df_t1_raw.apply(classify_type, axis=1)

# Show top-20 plus any FinTech lenders ranked below 20
mask = (df_t1_raw["rank"] <= 20) | (df_t1_raw["type"] == "FinTech")
df_t1_out = df_t1_raw[mask].copy()

cols_t1 = ["rank", "type", "lender_name", "volume_bn", "mkt_share_pct"]
print(df_t1_out[cols_t1].to_string(index=False))

df_t1_out[cols_t1].to_csv(OUT_DIR / "table1_replication.csv", index=False)
print("Saved table1_replication.csv")

# ================================================================
# TABLE 2 PART A -- Originated mortgages (action_taken = '1')
#                   loan_purpose IN ('1','3') only
# ================================================================
print("\n=== Computing Table 2A (Originated mortgages, 2010-2016) ===")
print("Note: processing time cannot be replicated (restricted data).")

# First compute LTI winsorization bounds globally (all lender types combined)
lti_bounds_sql = """
SELECT
    APPROX_QUANTILE(
        TRY_CAST(loan_amount AS DOUBLE) / NULLIF(TRY_CAST(income AS DOUBLE) * 1000, 0),
        0.005) AS lti_lo,
    APPROX_QUANTILE(
        TRY_CAST(loan_amount AS DOUBLE) / NULLIF(TRY_CAST(income AS DOUBLE) * 1000, 0),
        0.995) AS lti_hi
FROM lar_panel
WHERE year BETWEEN 2010 AND 2016
  AND action_taken = '1'
  AND loan_purpose IN ('1','3')
  AND TRY_CAST(income      AS DOUBLE) > 0
  AND TRY_CAST(loan_amount AS DOUBLE) > 0
"""
lti_row  = conn.execute(lti_bounds_sql).fetchone()
lti_lo   = float(lti_row[0])
lti_hi   = float(lti_row[1])
print(f"LTI winsorization bounds: [{lti_lo:.4f}, {lti_hi:.4f}]")

orig_sql = f"""
WITH base AS (
    SELECT
        {lender_type_case} AS lender_type,
        TRY_CAST(l.income       AS DOUBLE)  AS inc,
        TRY_CAST(l.loan_amount  AS DOUBLE)  AS lamt,
        CASE WHEN l.loan_purpose            = '1'  THEN 1.0 ELSE 0.0 END  AS is_purchase,
        CASE WHEN l.loan_purpose_harmonized = '3'  THEN 1.0 ELSE 0.0 END  AS is_refi,
        CASE WHEN TRY_CAST(l.loan_amount AS DOUBLE) > 417000 THEN 1.0 ELSE 0.0 END AS is_jumbo,
        CASE WHEN l.loan_type     = '1'              THEN 1.0 ELSE 0.0 END  AS is_conv,
        CASE WHEN l.loan_type     = '2'              THEN 1.0 ELSE 0.0 END  AS is_fha,
        CASE WHEN l.loan_type     = '3'              THEN 1.0 ELSE 0.0 END  AS is_va,
        CASE WHEN l.occupancy_type = '1'             THEN 1.0 ELSE 0.0 END  AS is_owner,
        CASE WHEN l.applicant_sex  = '1'             THEN 1.0 ELSE 0.0 END  AS is_male,
        CASE WHEN l.applicant_sex  = '2'             THEN 1.0 ELSE 0.0 END  AS is_female,
        -- pre-2018: co_applicant_sex = '4' means "Not applicable" (no co-applicant)
        CASE WHEN l.co_applicant_sex IN ('4','5')    THEN 1.0 ELSE 0.0 END  AS no_coappl,
        CASE WHEN l.applicant_race_1 = '5'           THEN 1.0 ELSE 0.0 END  AS is_white,
        CASE WHEN l.applicant_race_1 = '3'           THEN 1.0 ELSE 0.0 END  AS is_black,
        CASE WHEN l.applicant_race_1 = '2'           THEN 1.0 ELSE 0.0 END  AS is_asian,
        CASE WHEN l.applicant_race_1 IN ('1','4')    THEN 1.0 ELSE 0.0 END  AS is_other_race,
        CASE WHEN l.applicant_race_1 IN ('6','7')    THEN 1.0 ELSE 0.0 END  AS is_unknown_race,
        -- winsorized LTI (NULL when income missing/zero)
        CASE
            WHEN TRY_CAST(l.income      AS DOUBLE) > 0
             AND TRY_CAST(l.loan_amount AS DOUBLE) > 0
            THEN GREATEST({lti_lo},
                     LEAST({lti_hi},
                           TRY_CAST(l.loan_amount AS DOUBLE)
                           / (TRY_CAST(l.income AS DOUBLE) * 1000)))
            ELSE NULL
        END AS lti_wins
    FROM lar_panel l
    WHERE l.year          BETWEEN 2010 AND 2016
      AND l.action_taken  = '1'
      AND l.loan_purpose  IN ('1','3')
)
SELECT
    lender_type,
    COUNT(*)                                               AS obs,
    ROUND(AVG(inc),                                     0) AS income_mean,
    ROUND(APPROX_QUANTILE(inc, 0.5),                    0) AS income_p50,
    ROUND(AVG(lti_wins),                                2) AS lti_mean,
    ROUND(APPROX_QUANTILE(lti_wins, 0.5),               2) AS lti_p50,
    ROUND(AVG(is_purchase),                             2) AS purchase_share,
    ROUND(AVG(is_refi),                                 2) AS refi_share,
    ROUND(AVG(is_jumbo),                                2) AS jumbo_share,
    ROUND(AVG(is_conv),                                 2) AS conv_share,
    ROUND(AVG(is_fha),                                  2) AS fha_share,
    ROUND(AVG(is_va),                                   2) AS va_share,
    ROUND(AVG(is_owner),                                2) AS owner_occ_share,
    ROUND(AVG(is_male),                                 2) AS male_share,
    ROUND(AVG(is_female),                               2) AS female_share,
    ROUND(AVG(no_coappl),                               2) AS no_coappl_share,
    ROUND(AVG(is_white),                                2) AS white_share,
    ROUND(AVG(is_black),                                2) AS black_share,
    ROUND(AVG(is_asian),                                2) AS asian_share,
    ROUND(AVG(is_other_race),                           2) AS other_race_share,
    ROUND(AVG(is_unknown_race),                         2) AS unknown_race_share
FROM base
GROUP BY lender_type
ORDER BY lender_type
"""

df_orig = conn.execute(orig_sql).df()
# Add All-lenders row
orig_all_sql = orig_sql.replace("GROUP BY lender_type\nORDER BY lender_type", "")
# Rebuild without grouping -- easier to just UNION
orig_all_sql2 = f"""
WITH base AS (
    SELECT
        {lender_type_case} AS lender_type,
        TRY_CAST(l.income       AS DOUBLE)  AS inc,
        TRY_CAST(l.loan_amount  AS DOUBLE)  AS lamt,
        CASE WHEN l.loan_purpose            = '1'  THEN 1.0 ELSE 0.0 END  AS is_purchase,
        CASE WHEN l.loan_purpose_harmonized = '3'  THEN 1.0 ELSE 0.0 END  AS is_refi,
        CASE WHEN TRY_CAST(l.loan_amount AS DOUBLE) > 417000 THEN 1.0 ELSE 0.0 END AS is_jumbo,
        CASE WHEN l.loan_type     = '1'              THEN 1.0 ELSE 0.0 END  AS is_conv,
        CASE WHEN l.loan_type     = '2'              THEN 1.0 ELSE 0.0 END  AS is_fha,
        CASE WHEN l.loan_type     = '3'              THEN 1.0 ELSE 0.0 END  AS is_va,
        CASE WHEN l.occupancy_type = '1'             THEN 1.0 ELSE 0.0 END  AS is_owner,
        CASE WHEN l.applicant_sex  = '1'             THEN 1.0 ELSE 0.0 END  AS is_male,
        CASE WHEN l.applicant_sex  = '2'             THEN 1.0 ELSE 0.0 END  AS is_female,
        CASE WHEN l.co_applicant_sex IN ('4','5')    THEN 1.0 ELSE 0.0 END  AS no_coappl,
        CASE WHEN l.applicant_race_1 = '5'           THEN 1.0 ELSE 0.0 END  AS is_white,
        CASE WHEN l.applicant_race_1 = '3'           THEN 1.0 ELSE 0.0 END  AS is_black,
        CASE WHEN l.applicant_race_1 = '2'           THEN 1.0 ELSE 0.0 END  AS is_asian,
        CASE WHEN l.applicant_race_1 IN ('1','4')    THEN 1.0 ELSE 0.0 END  AS is_other_race,
        CASE WHEN l.applicant_race_1 IN ('6','7')    THEN 1.0 ELSE 0.0 END  AS is_unknown_race,
        CASE
            WHEN TRY_CAST(l.income      AS DOUBLE) > 0
             AND TRY_CAST(l.loan_amount AS DOUBLE) > 0
            THEN GREATEST({lti_lo},
                     LEAST({lti_hi},
                           TRY_CAST(l.loan_amount AS DOUBLE)
                           / (TRY_CAST(l.income AS DOUBLE) * 1000)))
            ELSE NULL
        END AS lti_wins
    FROM lar_panel l
    WHERE l.year         BETWEEN 2010 AND 2016
      AND l.action_taken = '1'
      AND l.loan_purpose IN ('1','3')
)
SELECT lender_type, COUNT(*) AS obs,
    ROUND(AVG(inc),0)                          AS income_mean,
    ROUND(APPROX_QUANTILE(inc,0.5),0)          AS income_p50,
    ROUND(AVG(lti_wins),2)                     AS lti_mean,
    ROUND(APPROX_QUANTILE(lti_wins,0.5),2)     AS lti_p50,
    ROUND(AVG(is_purchase),2)                  AS purchase_share,
    ROUND(AVG(is_refi),2)                      AS refi_share,
    ROUND(AVG(is_jumbo),2)                     AS jumbo_share,
    ROUND(AVG(is_conv),2)                      AS conv_share,
    ROUND(AVG(is_fha),2)                       AS fha_share,
    ROUND(AVG(is_va),2)                        AS va_share,
    ROUND(AVG(is_owner),2)                     AS owner_occ_share,
    ROUND(AVG(is_male),2)                      AS male_share,
    ROUND(AVG(is_female),2)                    AS female_share,
    ROUND(AVG(no_coappl),2)                    AS no_coappl_share,
    ROUND(AVG(is_white),2)                     AS white_share,
    ROUND(AVG(is_black),2)                     AS black_share,
    ROUND(AVG(is_asian),2)                     AS asian_share,
    ROUND(AVG(is_other_race),2)                AS other_race_share,
    ROUND(AVG(is_unknown_race),2)              AS unknown_race_share
FROM base
GROUP BY lender_type

UNION ALL

SELECT 'All' AS lender_type, COUNT(*) AS obs,
    ROUND(AVG(inc),0),
    ROUND(APPROX_QUANTILE(inc,0.5),0),
    ROUND(AVG(lti_wins),2),
    ROUND(APPROX_QUANTILE(lti_wins,0.5),2),
    ROUND(AVG(is_purchase),2),
    ROUND(AVG(is_refi),2),
    ROUND(AVG(is_jumbo),2),
    ROUND(AVG(is_conv),2),
    ROUND(AVG(is_fha),2),
    ROUND(AVG(is_va),2),
    ROUND(AVG(is_owner),2),
    ROUND(AVG(is_male),2),
    ROUND(AVG(is_female),2),
    ROUND(AVG(no_coappl),2),
    ROUND(AVG(is_white),2),
    ROUND(AVG(is_black),2),
    ROUND(AVG(is_asian),2),
    ROUND(AVG(is_other_race),2),
    ROUND(AVG(is_unknown_race),2)
FROM base

ORDER BY lender_type
"""

df_orig = conn.execute(orig_all_sql2).df()
print(df_orig.to_string(index=False))
df_orig.to_csv(OUT_DIR / "table2_originated_replication.csv", index=False)
print("Saved table2_originated_replication.csv")

# ================================================================
# TABLE 2 PART B -- All applications
#                   action_taken IN ('1','2','3','4','5')
#                   loan_purpose IN ('1','3')
# ================================================================
print("\n=== Computing Table 2B (All applications, 2010-2016) ===")

allapps_sql = f"""
SELECT
    {lender_type_case} AS lender_type,
    COUNT(*) AS obs,
    ROUND(AVG(CASE WHEN l.action_taken = '1' THEN 1.0 ELSE 0.0 END), 2) AS originated_share,
    ROUND(AVG(CASE WHEN l.action_taken = '2' THEN 1.0 ELSE 0.0 END), 2) AS approved_not_acc,
    ROUND(AVG(CASE WHEN l.action_taken = '3' THEN 1.0 ELSE 0.0 END), 2) AS denied_share,
    ROUND(AVG(CASE WHEN l.action_taken = '4' THEN 1.0 ELSE 0.0 END), 2) AS withdrawn_share,
    ROUND(AVG(CASE WHEN l.action_taken = '5' THEN 1.0 ELSE 0.0 END), 2) AS incomplete_share
FROM lar_panel l
WHERE l.year         BETWEEN 2010 AND 2016
  AND l.action_taken IN ('1','2','3','4','5')
  AND l.loan_purpose IN ('1','3')
GROUP BY {lender_type_case}

UNION ALL

SELECT
    'All',
    COUNT(*),
    ROUND(AVG(CASE WHEN l.action_taken = '1' THEN 1.0 ELSE 0.0 END), 2),
    ROUND(AVG(CASE WHEN l.action_taken = '2' THEN 1.0 ELSE 0.0 END), 2),
    ROUND(AVG(CASE WHEN l.action_taken = '3' THEN 1.0 ELSE 0.0 END), 2),
    ROUND(AVG(CASE WHEN l.action_taken = '4' THEN 1.0 ELSE 0.0 END), 2),
    ROUND(AVG(CASE WHEN l.action_taken = '5' THEN 1.0 ELSE 0.0 END), 2)
FROM lar_panel l
WHERE l.year         BETWEEN 2010 AND 2016
  AND l.action_taken IN ('1','2','3','4','5')
  AND l.loan_purpose IN ('1','3')

ORDER BY lender_type
"""

df_apps = conn.execute(allapps_sql).df()
print(df_apps.to_string(index=False))
df_apps.to_csv(OUT_DIR / "table2_allapps_replication.csv", index=False)
print("Saved table2_allapps_replication.csv")

# ================================================================
# COMPARISON SUMMARY
# ================================================================
print("\n" + "=" * 70)
print("COMPARISON vs PAPER (Table 2, Originated Mortgages)")
print("=" * 70)

paper_orig = {
    "Bank":           {"income_mean": 121, "income_p50": 86,  "lti_mean": 1.96, "lti_p50": 1.80,
                       "purchase_share": 0.34, "refi_share": 0.66, "jumbo_share": 0.05,
                       "conv_share": 0.86, "fha_share": 0.09, "va_share": 0.05,
                       "owner_occ_share": 0.88, "male_share": 0.67, "female_share": 0.25,
                       "no_coappl_share": 0.45, "white_share": 0.79, "black_share": 0.04,
                       "asian_share": 0.05, "other_race_share": 0.01, "unknown_race_share": 0.11,
                       "obs": 32_751_662},
    "Nonbank_NonFT":  {"income_mean": 102, "income_p50": 82,  "lti_mean": 2.46, "lti_p50": 2.40,
                       "purchase_share": 0.52, "refi_share": 0.48, "jumbo_share": 0.02,
                       "conv_share": 0.61, "fha_share": 0.28, "va_share": 0.11,
                       "owner_occ_share": 0.92, "male_share": 0.69, "female_share": 0.27,
                       "no_coappl_share": 0.52, "white_share": 0.78, "black_share": 0.06,
                       "asian_share": 0.07, "other_race_share": 0.01, "unknown_race_share": 0.09,
                       "obs": 14_742_227},
    "FinTech":        {"income_mean": 102, "income_p50": 84,  "lti_mean": 2.34, "lti_p50": 2.19,
                       "purchase_share": 0.22, "refi_share": 0.78, "jumbo_share": 0.02,
                       "conv_share": 0.71, "fha_share": 0.20, "va_share": 0.09,
                       "owner_occ_share": 0.92, "male_share": 0.59, "female_share": 0.26,
                       "no_coappl_share": 0.50, "white_share": 0.68, "black_share": 0.05,
                       "asian_share": 0.04, "other_race_share": 0.01, "unknown_race_share": 0.22,
                       "obs": 2_306_237},
    "All":            {"income_mean": 115, "income_p50": 84,  "lti_mean": 2.13, "lti_p50": 2.00,
                       "purchase_share": 0.38, "refi_share": 0.62, "jumbo_share": 0.04,
                       "conv_share": 0.78, "fha_share": 0.15, "va_share": 0.07,
                       "owner_occ_share": 0.89, "male_share": 0.68, "female_share": 0.26,
                       "no_coappl_share": 0.48, "white_share": 0.78, "black_share": 0.05,
                       "asian_share": 0.06, "other_race_share": 0.01, "unknown_race_share": 0.11,
                       "obs": 49_800_126},
}

comparison_rows = []
for _, row in df_orig.iterrows():
    lt = row["lender_type"]
    if lt not in paper_orig:
        continue
    paper = paper_orig[lt]
    for col in paper:
        our_val   = row.get(col, None)
        paper_val = paper[col]
        if our_val is not None:
            diff = float(our_val) - float(paper_val)
        else:
            diff = None
        comparison_rows.append({
            "lender_type": lt,
            "statistic":   col,
            "paper":       paper_val,
            "ours":        our_val,
            "diff":        round(diff, 4) if diff is not None else None,
        })

df_cmp = pd.DataFrame(comparison_rows)
print(df_cmp.to_string(index=False))
df_cmp.to_csv(OUT_DIR / "table2_comparison.csv", index=False)
print("\nSaved table2_comparison.csv")

print("\n" + "=" * 70)
print("COMPARISON vs PAPER (Table 2, All Applications)")
print("=" * 70)
paper_apps = {
    "Bank":          {"originated_share": 0.64, "approved_not_acc": 0.04, "denied_share": 0.20,
                      "withdrawn_share": 0.09, "incomplete_share": 0.03, "obs": 51_448_444},
    "Nonbank_NonFT": {"originated_share": 0.58, "approved_not_acc": 0.05, "denied_share": 0.16,
                      "withdrawn_share": 0.15, "incomplete_share": 0.06, "obs": 25_604_501},
    "FinTech":       {"originated_share": 0.66, "approved_not_acc": 0.03, "denied_share": 0.27,
                      "withdrawn_share": 0.03, "incomplete_share": 0.01, "obs":  3_473_506},
    "All":           {"originated_share": 0.62, "approved_not_acc": 0.04, "denied_share": 0.19,
                      "withdrawn_share": 0.11, "incomplete_share": 0.04, "obs": 80_526_451},
}
cmp_apps = []
for _, row in df_apps.iterrows():
    lt = row["lender_type"]
    if lt not in paper_apps:
        continue
    paper = paper_apps[lt]
    for col in paper:
        our_val   = row.get(col, None)
        paper_val = paper[col]
        diff = round(float(our_val) - float(paper_val), 4) if our_val is not None else None
        cmp_apps.append({"lender_type": lt, "statistic": col,
                         "paper": paper_val, "ours": our_val, "diff": diff})
df_cmp_apps = pd.DataFrame(cmp_apps)
print(df_cmp_apps.to_string(index=False))
df_cmp_apps.to_csv(OUT_DIR / "table2apps_comparison.csv", index=False)
print("Saved table2apps_comparison.csv")

print("\nAll outputs written to:", OUT_DIR)
print("Done.")
