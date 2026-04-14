# CLAUDE.md

## Project Overview
A modular, high-performance Python framework for constructing research-ready financial panels (e.g., HMDA, Call Reports, FFIEC). This project automates the ETL process, transforming messy, heterogeneous regulatory data into unified, harmonized Parquet files and DuckDB databases.

## Architecture & Directory Structure
The project is organized by dataset. Each folder is self-contained:
- `[dataset-name]/`: e.g., `hmda/`, `call_reports/`
  - `download.py`: Logic for fetching raw files with resume capability and `User-Agent` headers.
  - `construct.py`: Main ETL script utilizing Polars/DuckDB for harmonization.
  - `metadata.py`: Definitions for column mappings, crosswalks, and variable descriptions.
  - `schema.py`: Pydantic or TypedDict definitions for schema enforcement.
- `utils/`: Shared utilities for logging, HDD path resolution, and DuckDB connection management.
- `config.py`: Central configuration for environment variables and external drive paths.

## Data Storage Strategy (Mandatory)
- **Codebase**: Resides on the local machine (GitHub repo).
- **Data (Local)**: All "heavy" files (Raw CSV/ZIP, Staging Parquet, Final `.duckdb`) **MUST** be stored under `C:\empirical-data-construction`.
- **Path Resolution**: Never hardcode absolute local paths. Use a central `FIN_DATA_ROOT` environment variable (default: `C:\empirical-data-construction`) or the `config.py` utility to resolve paths.

## Technical Stack & Constraints
- **Primary Languages**: Python 3.10+, SQL.
- **Processing**:
  - **Polars**: Primary tool for in-memory wrangling and LazyFrame operations.
  - **DuckDB**: Primary tool for persistent storage, out-of-core SQL joins, and managing 100M+ row datasets.
- **Download**: `httpx` or `requests` with stream processing.
- **Memory Constraint**: **Strictly avoid `pandas`** for large-scale data. Always assume the dataset exceeds available RAM; use `polars.scan_parquet()` or DuckDB's native Parquet readers.

## Construction & Update Guiding Principles
1. **Raw-to-Staging**: Convert raw CSV/ZIP files to compressed Parquet immediately after download to save HDD space and improve read speeds.
2. **Idempotent Updates**:
   - Scripts must check if data for a specific period (e.g., Year/Quarter) already exists on the HDD before downloading/processing.
   - Use an `--update` flag to trigger a check for new data releases without rebuilding the entire history.
3. **Schema Harmonization**: Create explicit mappings to handle regulatory regime shifts (e.g., HMDA pre-2018 vs. post-2018). Ensure types are consistent (e.g., mapping numeric codes to unified string labels).
4. **Self-Documenting Metadata**: Every DuckDB instance must contain a `metadata` table documenting:
   - Harmonized variable names and their original source names.
   - Data types and valid value ranges.
   - Year/Quarter availability.

## Agent Instructions (Claude Code / Cursor)
- **Context Awareness**: Before proposing a query, check the `metadata` table in the database to understand the harmonized schema.
- **Safe Writing**: When updating the master panel, use atomic writes (write to a temp file and rename) to prevent database corruption on the external HDD.
- **Efficiency**: Use DuckDB's `COPY` or `INSERT INTO` commands for efficient ingestion.
- **Validation**: Include a basic validation step (e.g., row counts, null checks) after each construction run to ensure data integrity.

## Suggested `config.py`

```python
import os
from pathlib import Path

# Set the environment variable FIN_DATA_ROOT in your .env or shell
# Example (Windows): setx FIN_DATA_ROOT "C:\empirical-data-construction"
HDD_PATH = Path(os.getenv("FIN_DATA_ROOT", r"C:\empirical-data-construction"))

def get_storage_path(dataset: str) -> Path:
    """Returns the path to the dataset folder under FIN_DATA_ROOT."""
    path = HDD_PATH / dataset
    path.mkdir(parents=True, exist_ok=True)
    return path
```
