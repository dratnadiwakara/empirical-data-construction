"""
Central configuration for empirical-data-construction pipeline.
Set the FIN_DATA_ROOT environment variable to override the data root path.
Default: C:\\empirical-data-construction
"""
import os
from pathlib import Path

# ── Storage ───────────────────────────────────────────────────────────────────
# Set FIN_DATA_ROOT in your .env or shell profile, e.g.:
#   Windows: setx FIN_DATA_ROOT "C:\empirical-data-construction"
HDD_PATH = Path(os.getenv("FIN_DATA_ROOT", r"C:\empirical-data-construction"))

DATASET = "hmda"

# ── HTTP ──────────────────────────────────────────────────────────────────────
USER_AGENT = (
    "HMDA-Research-Pipeline/1.0 "
    "(academic; empirical-data-construction; "
    "https://github.com/dratnadiwakara/empirical-data-construction)"
)
HTTP_TIMEOUT = 120          # seconds — large files need generous timeout
HTTP_RETRIES = 5
HTTP_BACKOFF_BASE = 2.0     # exponential base; sleep = min(base^attempt, 120)
HTTP_CHUNK_SIZE = 8 * 1024 * 1024   # 8 MB streaming chunks

# ── Parquet ───────────────────────────────────────────────────────────────────
PARQUET_COMPRESSION = "snappy"
# Target row-group size in rows; ~500 bytes/row compressed → 128 MB ≈ 260k rows
PARQUET_ROW_GROUP_SIZE = 260_000

# ── DuckDB ────────────────────────────────────────────────────────────────────
DUCKDB_THREADS = 4
DUCKDB_MEMORY_LIMIT = "6GB"   # conservative for 8 GB machine


# ── Path helpers ──────────────────────────────────────────────────────────────

def get_storage_path(subdataset: str = "") -> Path:
    """Returns the path to the dataset folder on the external HDD."""
    path = HDD_PATH / DATASET / subdataset if subdataset else HDD_PATH / DATASET
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_duckdb_path() -> Path:
    """Path to the master HMDA DuckDB database."""
    return get_storage_path() / "hmda.duckdb"


def get_raw_path(year: int) -> Path:
    """Raw download directory for a given year."""
    p = get_storage_path("raw") / str(year)
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_staging_path(year: int) -> Path:
    """Hive-partitioned Parquet staging directory for a given year."""
    p = get_storage_path("staging") / f"year={year}"
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_manifest_path() -> Path:
    """Path to the download idempotency manifest JSON."""
    return get_storage_path() / "download_manifest.json"


def get_avery_path() -> Path:
    """Directory for Avery HMDA lender crosswalk files."""
    p = get_storage_path("avery")
    p.mkdir(parents=True, exist_ok=True)
    return p


# ── CRA path helpers ─────────────────────────────────────────────────────────

CRA_DATASET = "cra"

CRA_USER_AGENT = (
    "CRA-Research-Pipeline/1.0 "
    "(academic; empirical-data-construction; "
    "https://github.com/dratnadiwakara/empirical-data-construction)"
)


def get_cra_storage_path(subdataset: str = "") -> Path:
    """Returns the path to the CRA dataset folder on the external HDD."""
    path = HDD_PATH / CRA_DATASET / subdataset if subdataset else HDD_PATH / CRA_DATASET
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_cra_duckdb_path() -> Path:
    """Path to the master CRA DuckDB database."""
    return get_cra_storage_path() / "cra.duckdb"


def get_cra_raw_path(year: int) -> Path:
    """Raw download directory for a given CRA year."""
    p = get_cra_storage_path("raw") / str(year)
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_cra_staging_path(table_type: str, year: int) -> Path:
    """Hive-partitioned Parquet staging directory for a CRA year + table type."""
    p = get_cra_storage_path("staging") / table_type / f"year={year}"
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_cra_manifest_path() -> Path:
    """Path to the CRA download idempotency manifest JSON."""
    return get_cra_storage_path() / "download_manifest.json"


# ── NIC path helpers ─────────────────────────────────────────────────────────

NIC_DATASET = "nic"

NIC_USER_AGENT = (
    "NIC-Research-Pipeline/1.0 "
    "(academic; empirical-data-construction; "
    "https://github.com/dratnadiwakara/empirical-data-construction)"
)


def get_nic_storage_path(subdataset: str = "") -> Path:
    """Returns the path to the NIC dataset folder on the external HDD."""
    path = HDD_PATH / NIC_DATASET / subdataset if subdataset else HDD_PATH / NIC_DATASET
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_nic_duckdb_path() -> Path:
    """Path to the master NIC DuckDB database."""
    return get_nic_storage_path() / "nic.duckdb"


def get_nic_raw_path() -> Path:
    """Raw download directory for NIC snapshots."""
    p = get_nic_storage_path("raw")
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_nic_staging_path(dataset: str) -> Path:
    """Staging Parquet directory for a NIC dataset."""
    p = get_nic_storage_path("staging") / dataset
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_nic_manifest_path() -> Path:
    """Path to the NIC download idempotency manifest JSON."""
    return get_nic_storage_path() / "download_manifest.json"


# ── Call Reports CFLV path helpers ───────────────────────────────────────────

CFLV_DATASET = "call-reports-CFLV"


def get_cflv_storage_path(subdataset: str = "") -> Path:
    """Returns the path to the CFLV dataset folder on the HDD."""
    path = HDD_PATH / CFLV_DATASET / subdataset if subdataset else HDD_PATH / CFLV_DATASET
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_cflv_duckdb_path() -> Path:
    """Path to the CFLV DuckDB database."""
    return get_cflv_storage_path() / "call-reports-cflv.duckdb"


def get_cflv_raw_path() -> Path:
    """Directory for extracted raw .dta files."""
    p = get_cflv_storage_path("raw")
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_cflv_staging_path(table: str) -> Path:
    """Hive-partitioned Parquet staging directory for a given table."""
    p = get_cflv_storage_path("staging") / table
    p.mkdir(parents=True, exist_ok=True)
    return p


# ── Call Reports FFIEC path helpers ──────────────────────────────────────────

FFIEC_DATASET = "call-reports-FFIEC"

FFIEC_USER_AGENT = (
    "FFIEC-Call-Reports-Pipeline/1.0 "
    "(academic; empirical-data-construction; "
    "https://github.com/dratnadiwakara/empirical-data-construction)"
)


def get_ffiec_storage_path(subdataset: str = "") -> Path:
    """Returns the path to the FFIEC Call Reports dataset folder on the HDD."""
    path = HDD_PATH / FFIEC_DATASET / subdataset if subdataset else HDD_PATH / FFIEC_DATASET
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_ffiec_duckdb_path() -> Path:
    """Path to the master FFIEC Call Reports DuckDB database."""
    return get_ffiec_storage_path() / "call-reports-ffiec.duckdb"


def get_ffiec_raw_path(year: int, quarter: int) -> Path:
    """Raw extracted directory for one (year, quarter)."""
    p = get_ffiec_storage_path("raw") / f"{year}Q{quarter}"
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_ffiec_staging_path(schedule: str, year: int, quarter: int) -> Path:
    """Hive-partitioned Parquet staging directory for (schedule, year, quarter)."""
    p = (
        get_ffiec_storage_path("staging")
        / schedule
        / f"year={year}"
        / f"quarter={quarter}"
    )
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_ffiec_manifest_path() -> Path:
    """Path to the FFIEC Call Reports download/extract manifest JSON."""
    return get_ffiec_storage_path() / "download_manifest.json"


def get_ffiec_mdrm_path() -> Path:
    """Directory holding the MDRM dictionary (MDRM.zip + MDRM.csv)."""
    p = get_ffiec_storage_path("mdrm")
    p.mkdir(parents=True, exist_ok=True)
    return p


# ── PERMCO-RSSD link path helpers ─────────────────────────────────────────────

PERMCO_RSSD_DATASET = "permco-rssd-link"


def get_permco_rssd_storage_path(subdataset: str = "") -> Path:
    """Returns the path to the permco-rssd-link dataset folder on the HDD."""
    path = HDD_PATH / PERMCO_RSSD_DATASET / subdataset if subdataset else HDD_PATH / PERMCO_RSSD_DATASET
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_permco_rssd_duckdb_path() -> Path:
    """Path to the permco-rssd-link DuckDB database."""
    return get_permco_rssd_storage_path() / "permco-rssd-link.duckdb"


def get_permco_rssd_raw_path() -> Path:
    """Raw directory for the source CRSP-FRB link CSV."""
    p = get_permco_rssd_storage_path("raw")
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_permco_rssd_staging_path() -> Path:
    """Staging Parquet directory for the crsp_frb_link table."""
    p = get_permco_rssd_storage_path("staging") / "crsp_frb_link"
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_permco_rssd_manifest_path() -> Path:
    """Path to the permco-rssd-link download manifest JSON."""
    return get_permco_rssd_storage_path() / "download_manifest.json"


# ── SOD (FDIC Summary of Deposits) path helpers ───────────────────────────────

SOD_DATASET = "sod"

SOD_USER_AGENT = (
    "SOD-Research-Pipeline/1.0 "
    "(academic; empirical-data-construction; "
    "https://github.com/dratnadiwakara/empirical-data-construction)"
)


def get_sod_storage_path(subdataset: str = "") -> Path:
    """Returns the path to the SOD dataset folder on the HDD."""
    path = HDD_PATH / SOD_DATASET / subdataset if subdataset else HDD_PATH / SOD_DATASET
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_sod_duckdb_path() -> Path:
    """Path to the SOD DuckDB database."""
    return get_sod_storage_path() / "sod.duckdb"


def get_sod_raw_path(year: int) -> Path:
    """Raw download directory for one SOD survey year."""
    p = get_sod_storage_path("raw") / str(year)
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_sod_staging_path(year: int) -> Path:
    """Hive-partitioned Parquet staging directory for one SOD survey year."""
    p = get_sod_storage_path("staging") / f"year={year}"
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_sod_manifest_path() -> Path:
    """Path to the SOD download idempotency manifest JSON."""
    return get_sod_storage_path() / "download_manifest.json"


# ── IRS SOI (Individual Income Tax ZIP Code Data) path helpers ────────────────

IRS_DATASET = "irs"

IRS_USER_AGENT = (
    "IRS-SOI-Research-Pipeline/1.0 "
    "(academic; empirical-data-construction; "
    "https://github.com/dratnadiwakara/empirical-data-construction)"
)


def get_irs_storage_path(subdataset: str = "") -> Path:
    """Returns the path to the IRS SOI dataset folder on the HDD."""
    path = HDD_PATH / IRS_DATASET / subdataset if subdataset else HDD_PATH / IRS_DATASET
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_irs_duckdb_path() -> Path:
    """Path to the IRS SOI DuckDB database."""
    return get_irs_storage_path() / "irs.duckdb"


def get_irs_raw_path(year: int) -> Path:
    """Raw download directory for one IRS SOI tax year."""
    p = get_irs_storage_path("raw") / str(year)
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_irs_staging_path(year: int) -> Path:
    """Hive-partitioned Parquet staging directory for one IRS SOI tax year."""
    p = get_irs_storage_path("staging") / f"year={year}"
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_irs_manifest_path() -> Path:
    """Path to the IRS SOI download idempotency manifest JSON."""
    return get_irs_storage_path() / "download_manifest.json"


# ── RateWatch (S&P Global deposit rate panel) path helpers ───────────────────

RATEWATCH_DATASET = "ratewatch"

# Source: local S&P Global RateWatch extract (no HTTP — files come on disk).
RATEWATCH_SOURCE_ROOT = Path(r"D:\RateWatch_PS_full")


def get_ratewatch_storage_path(subdataset: str = "") -> Path:
    """Returns the path to the RateWatch dataset folder on the HDD."""
    path = HDD_PATH / RATEWATCH_DATASET / subdataset if subdataset else HDD_PATH / RATEWATCH_DATASET
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_ratewatch_duckdb_path() -> Path:
    """Path to the RateWatch DuckDB database."""
    return get_ratewatch_storage_path() / "ratewatch.duckdb"


def get_ratewatch_raw_path(year: int) -> Path:
    """Raw extracted rate-data directory for one year."""
    p = get_ratewatch_storage_path("raw") / str(year)
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_ratewatch_support_path() -> Path:
    """Directory holding shared lookup files (institution details, acct join, change history)."""
    p = get_ratewatch_storage_path("raw") / "support"
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_ratewatch_staging_path(year: int) -> Path:
    """Hive-partitioned Parquet staging directory for one RateWatch year."""
    p = get_ratewatch_storage_path("staging") / f"year={year}"
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_ratewatch_support_staging_path() -> Path:
    """Staging directory for support-table Parquet (institution details, acct join)."""
    p = get_ratewatch_storage_path("staging") / "support"
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_ratewatch_manifest_path() -> Path:
    """Path to the RateWatch download/extract manifest JSON."""
    return get_ratewatch_storage_path() / "download_manifest.json"


# ── Y-9C (FR Y-9C Bank Holding Company quarterly filings) path helpers ───────

Y9C_DATASET = "y9c"

Y9C_USER_AGENT = (
    "Y9C-Research-Pipeline/1.0 "
    "(academic; empirical-data-construction; "
    "https://github.com/dratnadiwakara/empirical-data-construction)"
)


def get_y9c_storage_path(subdataset: str = "") -> Path:
    """Returns the path to the Y-9C dataset folder on the HDD."""
    path = HDD_PATH / Y9C_DATASET / subdataset if subdataset else HDD_PATH / Y9C_DATASET
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_y9c_duckdb_path() -> Path:
    """Path to the Y-9C DuckDB database."""
    return get_y9c_storage_path() / "y9c.duckdb"


def get_y9c_raw_path(year: int, quarter: int) -> Path:
    """Raw extracted directory for one (year, quarter)."""
    p = get_y9c_storage_path("raw") / f"{year}Q{quarter}"
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_y9c_staging_path(year: int, quarter: int) -> Path:
    """Hive-partitioned Parquet staging directory for one (year, quarter)."""
    p = get_y9c_storage_path("staging") / f"year={year}" / f"quarter={quarter}"
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_y9c_manifest_path() -> Path:
    """Path to the Y-9C download/extract manifest JSON."""
    return get_y9c_storage_path() / "download_manifest.json"
