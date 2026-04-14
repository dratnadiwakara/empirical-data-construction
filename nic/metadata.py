"""
Metadata and schema definitions for the FFIEC NIC pipeline.
"""
from __future__ import annotations

BASE_URL = "https://www.ffiec.gov"

DATASETS = {
    "relationships": {
        "filename": "CSV_RELATIONSHIPS.ZIP",
        "download_path": "/npw/FinancialReport/ReturnRelationshipsZipFileCSV",
        "description": "Ownership relationships between entities",
        "view_name": "relationships",
    },
    "transformations": {
        "filename": "CSV_TRANSFORMATIONS.ZIP",
        "download_path": "/npw/FinancialReport/ReturnTransformationZipFileCSV",
        "description": "Mergers, acquisitions, and structural changes",
        "view_name": "transformations",
    },
}

DATE_COLUMNS = [
    "D_DT_START",
    "D_DT_END",
    "D_DT_TRANS",
    "DT_START",
    "DT_END",
    "DT_TRANS",
    "DT_EXIST",
]

ID_COLUMNS = {
    "relationships": [
        "ID_RSSD_PARENT",
        "ID_RSSD_OFFSPRING",
    ],
    "transformations": [
        "ID_RSSD_PREDECESSOR",
        "ID_RSSD_SUCCESSOR",
    ],
}

NUMERIC_ID_COLUMNS = sorted(
    {
        *ID_COLUMNS["relationships"],
        *ID_COLUMNS["transformations"],
        "ID_RSSD",
        "ID_RSSD_HD_OFF",
    }
)

# Minimal curated descriptions for key columns frequently used in joins/filters.
VARIABLE_DESCRIPTIONS = {
    "ID_RSSD_PARENT": "RSSD identifier for the parent entity in a relationship.",
    "ID_RSSD_OFFSPRING": "RSSD identifier for the offspring/subordinate entity.",
    "D_DT_START": "Relationship start date.",
    "D_DT_END": "Relationship end date.",
    "ID_RSSD_PREDECESSOR": "RSSD identifier of the predecessor institution.",
    "ID_RSSD_SUCCESSOR": "RSSD identifier of the successor institution.",
    "D_DT_TRANS": "Transformation event date.",
}

PANEL_METADATA_DDL = """
CREATE TABLE IF NOT EXISTS panel_metadata (
    dataset VARCHAR,
    row_count BIGINT,
    file_sha256 VARCHAR,
    source_url VARCHAR,
    built_at VARCHAR,
    parquet_path VARCHAR,
    PRIMARY KEY (dataset)
)
"""
