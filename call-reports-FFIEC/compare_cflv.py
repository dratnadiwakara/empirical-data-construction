"""Compare FFIEC harmonized panel vs CFLV — all overlapping variables, 2024-Q4."""
import duckdb
import sys
sys.stdout.reconfigure(encoding="utf-8")

conn = duckdb.connect(r"C:\empirical-data-construction\call-reports-FFIEC\call-reports-ffiec.duckdb", read_only=True)
conn.execute("ATTACH 'C:/empirical-data-construction/call-reports-CFLV/call-reports-cflv.duckdb' AS cflv (READ_ONLY)")

bs_mapping = {
    "assets": "assets", "cash": "cash",
    "htmsec_ac": "htmsec_ac", "afssec_fv": "afssec_fv", "securities": "securities",
    "ln_tot": "ln_tot", "ln_tot_gross": "ln_tot_gross", "llres": "llres",
    "ln_cc": "ln_cc", "ln_agr": "ln_agr", "ln_re": "ln_re", "ln_ci": "ln_ci", "ln_cons": "ln_cons",
    "npl_tot": "npl_tot",
    "deposits": "deposits", "domestic_dep": "domestic_dep", "foreign_dep": "foreign_dep",
    "equity": "equity", "qtr_avg_assets": "qtr_avg_assets",
    "ln_lease": "ln_lease",
    "demand_deposits": "demand_deposits",
    "transaction_dep": "transaction_dep", "nontransaction_dep": "nontransaction_dep",
    "dom_deposit_ib": "dom_deposit_ib", "dom_deposit_nib": "dom_deposit_nib",
    "brokered_dep": "brokered_dep", "oreo": "oreo",
    "retained_earnings": "retain_earn",
    "trading_assets": "trad_ass", "trading_liab": "trad_liab",
    "borrowings": "othbor_liab", "sub_debt": "subdebt",
    "other_liab": "liab_oth", "total_liab": "liab_tot_unadj",
    "ffs": "ffsold", "reverse_repo": "repo_purch",
    "ffp": "ffpurch", "repo": "repo_sold",
    "premises": "fixed_ass", "other_assets": "oth_assets",
    "td_small": "time_dep_lt100k", "td_mid": "time_ge100k_le250k", "td_large": "time_dep_gt250k",
    "qtr_avg_loans": "qtr_avg_ln_tot",
    "qtr_avg_int_bearing_bal": "qtr_avg_ib_bal_due",
    "qtr_avg_ffs_reverse_repo": "qtr_avg_ffrepo_ass",
    "qtr_avg_ust_sec": "qtr_avg_ust_sec", "qtr_avg_mbs": "qtr_avg_mbs", "qtr_avg_oth_sec": "qtr_avg_oth_sec",
    "qtr_avg_ln_re": "qtr_avg_ln_re", "qtr_avg_ln_ci": "qtr_avg_ln_ci", "qtr_avg_lease": "qtr_avg_lease",
    "qtr_avg_trans_dep": "qtr_avg_trans_dep_dom",
    "qtr_avg_savings_dep": "qtr_avg_sav_dep_dom",
    "qtr_avg_time_dep_le250k": "qtr_avg_time_dep_le250k",
    "qtr_avg_time_dep_gt250k": "qtr_avg_time_dep_gt250k",
    "qtr_avg_ffpurch_repo": "qtr_avg_ffrepo_liab",
    "qtr_avg_othbor": "qtr_avg_othbor_liab",
}

is_mapping = {
    "ytdint_inc": "ytdint_inc", "ytdint_exp": "ytdint_exp", "ytdint_inc_net": "ytdint_inc_net",
    "ytdnonint_inc": "ytdnonint_inc", "ytdnonint_exp": "ytdnonint_exp",
    "ytdllprov": "ytdllprov", "ytdtradrev_inc": "ytdtradrev_inc",
    "ytdinc_before_disc_op": "ytdinc_before_disc_op",
    "ytdinc_taxes": "ytdinc_taxes", "ytdnetinc": "ytdnetinc",
    "ytdcommdividend": "ytdcommdividend", "num_employees": "num_employees",
    "ytdsalaries": "ytdnonint_exp_comp",
    "ytdprem_exp": "ytdnonint_exp_fass",
    "ytdoth_nonint_exp": "ytdoth_operating_exp",
    "ytdsvc_charges": "ytdnonint_inc_srv_chrg_dep",
    "ytdfiduc_inc": "ytdfiduc_inc",
    "ytdint_inc_ln": "ytdint_inc_ln",
    "ytdint_inc_ln_re": "ytdint_inc_ln_re",
    "ytdint_inc_ln_ci": "ytdint_inc_ln_ci",
    "ytdint_inc_ln_cc": "ytdint_inc_ln_cc",
    "ytdint_inc_ln_othcons": "ytdint_inc_ln_othcons",
    "ytdint_inc_sec_ust": "ytdint_inc_sec_ust",
    "ytdint_inc_sec_mbs": "ytdint_inc_sec_mbs",
    "ytdint_inc_sec_oth": "ytdint_inc_sec_oth",
    "ytdint_inc_ffrepo": "ytdint_inc_ffrepo",
    "ytdint_inc_lease": "ytdint_inc_lease",
    "ytdint_inc_ibb": "ytdint_inc_ibb",
    "ytdint_exp_trans_dep": "ytdint_exp_trans_dep_dom",
    "ytdint_exp_savings_dep": "ytdint_exp_savings_dep_dom",
    "ytdint_exp_time_le250k": "ytdint_exp_time_le250k_dom",
    "ytdint_exp_time_gt250k": "ytdint_exp_time_gt250k_dom",
    "ytdint_exp_ffrepo": "ytdint_exp_ffrepo",
}


def compare(panel, cflv_table, pairs, year=2024, quarter=4):
    results = []
    q_date = {1: "03-31", 2: "06-30", 3: "09-30", 4: "12-31"}[quarter]
    date_str = f"{year}-{q_date}"
    for ours, theirs in pairs.items():
        sql = f"""
        WITH o AS (
            SELECT id_rssd, {ours} AS vo FROM {panel}
            WHERE activity_year={year} AND activity_quarter={quarter}
        ),
        t AS (
            SELECT id_rssd, {theirs} AS vt FROM cflv.{cflv_table}
            WHERE date = DATE '{date_str}'
        )
        SELECT
            COUNT(*) joined_n,
            SUM(CASE WHEN vo IS NOT NULL AND vt IS NOT NULL THEN 1 ELSE 0 END) both_n,
            SUM(CASE WHEN vo IS NOT NULL AND vt IS NOT NULL AND ABS(vo - vt) < 1 THEN 1 ELSE 0 END) exact_n,
            SUM(CASE WHEN vo IS NOT NULL AND vt IS NOT NULL AND ABS(vo - vt) < 100 THEN 1 ELSE 0 END) near100_n,
            AVG(CASE WHEN vo IS NOT NULL AND vt IS NOT NULL THEN ABS(vo - vt) END) mean_abs_diff,
            AVG(CASE WHEN vo IS NOT NULL AND vt IS NOT NULL AND vt != 0 THEN ABS(vo - vt) / ABS(vt) END) mean_pct_diff
        FROM o JOIN t USING (id_rssd)
        """
        try:
            r = conn.execute(sql).fetchone()
            j, b, e, n, mad, mpd = r
            pct = 100.0 * e / b if b else 0.0
            npct = 100.0 * n / b if b else 0.0
            results.append((ours, theirs, j, b, e, pct, n, npct, mad or 0, (mpd or 0) * 100))
        except Exception as ex:
            results.append((ours, theirs, None, None, None, None, None, None, None, str(ex)[:60]))
    return results


def print_table(title, results):
    print("=" * 115)
    print(title)
    print("=" * 115)
    print(f'{"Harmonized":<26} {"CFLV":<28} {"joined":>7} {"both":>7} {"exact":>7} {"pct":>7} {"near100":>8} {"pct":>7}')
    print("-" * 115)
    for r in results:
        if r[2] is None:
            print(f"{r[0]:<26} {r[1]:<28} ERROR {r[9]}")
        else:
            print(f"{r[0]:<26} {r[1]:<28} {r[2]:>7,} {r[3]:>7,} {r[4]:>7,} {r[5]:>6.2f}% {r[6]:>8,} {r[7]:>6.2f}%")


bs_res = compare("bs_panel", "balance_sheets", bs_mapping)
print_table("BS panel vs CFLV balance_sheets (2024-Q4)", bs_res)

is_res = compare("is_panel", "income_statements", is_mapping)
print()
print_table("IS panel vs CFLV income_statements (2024-Q4)", is_res)

valid = [r for r in (bs_res + is_res) if r[3] and r[3] > 0]
tot_pairs = sum(r[3] for r in valid)
exact = sum(r[4] for r in valid)
near = sum(r[6] for r in valid)
print()
print("=" * 60)
print("Aggregate summary")
print("=" * 60)
print(f"Variables tested: {len(bs_res) + len(is_res)} ({len(bs_res)} BS + {len(is_res)} IS)")
print(f"Joined pairs (both non-NULL): {tot_pairs:,}")
print(f"Exact matches (diff < $1K):   {exact:,} ({100 * exact / tot_pairs:.2f}%)")
print(f"Near matches (diff < $100K):  {near:,} ({100 * near / tot_pairs:.2f}%)")
print()
print(f'Vars with >=99% exact: {sum(1 for r in valid if r[5] >= 99)}')
print(f'Vars with 95-99% exact: {sum(1 for r in valid if 95 <= r[5] < 99)}')
print(f'Vars with 80-95% exact: {sum(1 for r in valid if 80 <= r[5] < 95)}')
print(f'Vars with <80% exact: {sum(1 for r in valid if r[5] < 80)}')
print()
print("Low-match variables (<95%, ranked):")
lows = sorted([r for r in valid if r[5] < 95], key=lambda r: r[5])
for r in lows:
    print(f"  {r[0]:<26} vs {r[1]:<28}: {r[5]:>5.2f}% exact, {r[7]:>5.2f}% near-$100K, mean abs diff ${r[8]:,.0f}")
