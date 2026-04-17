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
