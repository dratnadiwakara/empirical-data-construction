"""
plot_system.py — Banking system time series plots from CFLV DuckDB.

Produces two figures:
  1. Number of banks reporting each quarter (1959-Q4 to 2025-Q3)
  2. Total banking system assets each quarter (trillions USD)

Usage:
    C:\envs\.basic_venv\Scripts\python.exe call-reports-CFLV/plot_system.py
"""
import sys
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
sys.path.insert(0, str(_ROOT))

from config import DUCKDB_MEMORY_LIMIT, DUCKDB_THREADS, get_cflv_duckdb_path

DB_PATH = get_cflv_duckdb_path()

conn = duckdb.connect(str(DB_PATH), read_only=True)
conn.execute(f"PRAGMA threads={DUCKDB_THREADS}")
conn.execute(f"PRAGMA memory_limit='{DUCKDB_MEMORY_LIMIT}'")

df = conn.execute("""
    SELECT
        date,
        COUNT(*)            AS n_banks,
        SUM(assets) / 1e9   AS total_assets_tn
    FROM balance_sheets
    GROUP BY date
    HAVING COUNT(*) >= 100   -- drop spurious partial quarters
    ORDER BY date
""").df()
conn.close()

df["date"] = pd.to_datetime(df["date"])

# ── Recession shading (NBER peaks/troughs) ───────────────────────────────────
RECESSIONS = [
    ("1960-04-01", "1961-02-28"),
    ("1969-12-01", "1970-11-30"),
    ("1973-11-01", "1975-03-31"),
    ("1980-01-01", "1980-07-31"),
    ("1981-07-01", "1982-11-30"),
    ("1990-07-01", "1991-03-31"),
    ("2001-03-01", "2001-11-30"),
    ("2007-12-01", "2009-06-30"),
    ("2020-02-01", "2020-04-30"),
]

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(13, 9), sharex=True)
fig.subplots_adjust(hspace=0.08)

for ax in (ax1, ax2):
    for start, end in RECESSIONS:
        ax.axvspan(pd.Timestamp(start), pd.Timestamp(end),
                   color="#d0d0d0", alpha=0.5, linewidth=0, zorder=0)

# ── Panel 1: Number of banks ──────────────────────────────────────────────────
ax1.plot(df["date"], df["n_banks"], color="#1a5276", linewidth=1.1, zorder=2)
ax1.fill_between(df["date"], df["n_banks"], alpha=0.12, color="#1a5276", zorder=1)
ax1.set_ylabel("Banks reporting", fontsize=11)
ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
ax1.set_title("U.S. Banking System — CFLV Historical Call Reports", fontsize=13, fontweight="bold", pad=10)
ax1.grid(axis="y", linestyle="--", linewidth=0.5, alpha=0.6)
ax1.spines[["top", "right"]].set_visible(False)

# Peak annotation
peak_row = df.loc[df["n_banks"].idxmax()]
ax1.annotate(
    f"Peak: {int(peak_row['n_banks']):,}\n({peak_row['date'].strftime('%Y-%m')})",
    xy=(peak_row["date"], peak_row["n_banks"]),
    xytext=(peak_row["date"] - pd.DateOffset(years=10), peak_row["n_banks"] * 0.97),
    arrowprops=dict(arrowstyle="->", color="black", lw=0.8),
    fontsize=8.5,
)

# ── Panel 2: Total assets ─────────────────────────────────────────────────────
ax2.plot(df["date"], df["total_assets_tn"], color="#922b21", linewidth=1.1, zorder=2)
ax2.fill_between(df["date"], df["total_assets_tn"], alpha=0.12, color="#922b21", zorder=1)
ax2.set_ylabel("Total assets ($ trillions)", fontsize=11)
ax2.set_xlabel("Quarter", fontsize=11)
ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:.0f}T"))
ax2.grid(axis="y", linestyle="--", linewidth=0.5, alpha=0.6)
ax2.spines[["top", "right"]].set_visible(False)

# Latest value annotation
last_row = df.iloc[-1]
ax2.annotate(
    f"{last_row['total_assets_tn']:.1f}T\n({last_row['date'].strftime('%b %Y')})",
    xy=(last_row["date"], last_row["total_assets_tn"]),
    xytext=(last_row["date"] - pd.DateOffset(years=8), last_row["total_assets_tn"] * 0.90),
    arrowprops=dict(arrowstyle="->", color="black", lw=0.8),
    fontsize=8.5,
)

fig.text(
    0.99, 0.01,
    "Source: Correia, Fermin, Luck, Verner (2025). FRBNY Liberty Street Economics. Shaded = NBER recessions.",
    ha="right", va="bottom", fontsize=7.5, color="#555555",
)

out_path = _HERE / "banking_system_trends.png"
fig.savefig(out_path, dpi=150, bbox_inches="tight")
print(f"Saved: {out_path}")
plt.show()
