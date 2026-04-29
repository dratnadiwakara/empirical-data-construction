"""
SQL generator for the Y-9C harmonized view layer.

Reads concept dicts from concepts.py and materializes:
  - view  bs_panel_y9c              (balance-sheet concepts + identity)
  - view  is_panel_y9c              (income-statement concepts + identity + q_* flows)
  - view  y9c_panel                 (bs_panel_y9c JOIN is_panel_y9c)
  - table harmonized_metadata_y9c   (one row per variable)

Called from construct.py::refresh_views.

Resilience: concept SQL references columns like y.BHCK2200. Some codes don't
exist in any quarter (e.g. BHC-consolidated deposits use BHDM/BHFN, not BHCK).
Before binding the views, we read y9c_raw's actual column set and rewrite any
``y.<COLNAME>`` references to ``NULL`` where the column is absent. This way
adding a concept that turns out to use the wrong prefix is a NULL column at
worst, not a Binder Error that nukes the whole view.
"""
from __future__ import annotations

import importlib.util
import re
from pathlib import Path

import duckdb

_HERE = Path(__file__).resolve().parent

_COL_REF_RE = re.compile(r"\by\.([A-Za-z][A-Za-z0-9_]*)")


def _y9c_raw_columns(conn: duckdb.DuckDBPyConnection) -> set[str]:
    rows = conn.execute("DESCRIBE y9c_raw").fetchall()
    return {r[0].upper() for r in rows}


def _strip_missing_cols(sql: str, available: set[str]) -> str:
    """Replace ``y.<COL>`` with ``NULL`` when COL not in available set."""
    def repl(m: re.Match) -> str:
        col = m.group(1).upper()
        if col in available:
            return f"y.{m.group(1)}"
        return "NULL"
    return _COL_REF_RE.sub(repl, sql)


def _load_concepts():
    spec = importlib.util.spec_from_file_location(
        "y9c_harmonized_concepts", _HERE / "concepts.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[attr-defined]
    return mod


def qtr_name(ytd_name: str) -> str:
    if ytd_name.startswith("ytd"):
        return "q_" + ytd_name[3:]
    return "q_" + ytd_name


# ── SQL builders ──────────────────────────────────────────────────────────────

def _bs_panel_sql(concepts, avail: set[str]) -> str:
    bs_exprs = [f"{_strip_missing_cols(spec['sql'], avail)} AS {name}"
                for name, spec in concepts.BS_CONCEPTS_Y9C.items()]
    id_exprs = [f"{_strip_missing_cols(spec['sql'], avail)} AS {name}"
                for name, spec in concepts.IDENTITY_CONCEPTS.items()]
    body = ",\n            ".join(id_exprs + bs_exprs)
    # Filter to Y-9C consolidated filers only: rows where BHCK2170 (total
    # assets) is populated. Excludes Y-9SP/Y-9LP filers that share the same
    # bulk source file but populate BHSP*/BHCP* prefixes instead.
    bhck_filter = "TRY_CAST(y.BHCK2170 AS DOUBLE) IS NOT NULL" \
        if "BHCK2170" in avail else "1=1"
    return f"""
        CREATE OR REPLACE VIEW bs_panel_y9c AS
        SELECT
            y.activity_year,
            y.activity_quarter,
            y.date,
            {body}
        FROM y9c_raw y
        WHERE {bhck_filter}
    """


def _is_panel_sql(concepts, avail: set[str]) -> str:
    is_exprs = [f"{_strip_missing_cols(spec['sql'], avail)} AS {name}"
                for name, spec in concepts.IS_CONCEPTS_Y9C.items()]
    id_exprs = [f"{_strip_missing_cols(spec['sql'], avail)} AS {name}"
                for name, spec in concepts.IDENTITY_CONCEPTS.items()]
    body = ",\n                ".join(id_exprs + is_exprs)

    ytd_cols = list(concepts.IS_CONCEPTS_Y9C.keys())
    qtr_exprs = [
        f"{n} - COALESCE(LAG({n}) OVER w, 0) AS {qtr_name(n)}"
        for n in ytd_cols
    ]
    qtr_select = ",\n            ".join(qtr_exprs)

    bhck_filter = "TRY_CAST(y.BHCK2170 AS DOUBLE) IS NOT NULL" \
        if "BHCK2170" in avail else "1=1"
    return f"""
        CREATE OR REPLACE VIEW is_panel_y9c AS
        WITH ytd AS (
            SELECT
                y.activity_year,
                y.activity_quarter,
                y.date,
                {body}
            FROM y9c_raw y
            WHERE {bhck_filter}
        )
        SELECT *,
            {qtr_select}
        FROM ytd
        WINDOW w AS (
            PARTITION BY id_rssd, activity_year
            ORDER BY activity_quarter
        )
    """


def _y9c_panel_sql(concepts) -> str:
    """Convenience: bs_panel_y9c LEFT JOIN is_panel_y9c on identity columns."""
    is_value_cols = list(concepts.IS_CONCEPTS_Y9C.keys())
    is_value_cols += [qtr_name(n) for n in concepts.IS_CONCEPTS_Y9C.keys()]
    is_select = ",\n            ".join(f"i.{c}" for c in is_value_cols)

    return f"""
        CREATE OR REPLACE VIEW y9c_panel AS
        SELECT bs.*,
            {is_select}
        FROM bs_panel_y9c bs
        LEFT JOIN is_panel_y9c i
            USING (id_rssd, activity_year, activity_quarter, date, rssd9999_raw)
    """


def _harmonized_metadata_rows(concepts) -> list[tuple]:
    rows: list[tuple] = []
    for panel_name, cdict in (
        ("bs_panel_y9c", concepts.BS_CONCEPTS_Y9C),
        ("is_panel_y9c", concepts.IS_CONCEPTS_Y9C),
        ("identity", concepts.IDENTITY_CONCEPTS),
    ):
        for name, spec in cdict.items():
            rows.append((
                name,
                panel_name,
                spec["desc"],
                spec["unit"],
                spec["source_schedule"],
                ",".join(spec.get("mdrm_codes", [])),
                spec["sql"],
                spec.get("available_from", ""),
            ))

    for name, spec in concepts.IS_CONCEPTS_Y9C.items():
        qn = qtr_name(name)
        desc = spec["desc"].replace(" (YTD)", "").rstrip() + " (current quarter, derived)"
        formula = (
            f"{name} - COALESCE(LAG({name}) "
            f"OVER (PARTITION BY id_rssd, activity_year ORDER BY activity_quarter), 0)"
        )
        rows.append((
            qn,
            "is_panel_y9c",
            desc,
            spec["unit"],
            spec["source_schedule"],
            ",".join(spec.get("mdrm_codes", [])),
            formula,
            spec.get("available_from", ""),
        ))
    return rows


# ── Public API ────────────────────────────────────────────────────────────────

def build_views(conn: duckdb.DuckDBPyConnection) -> list[str]:
    concepts = _load_concepts()
    avail = _y9c_raw_columns(conn)

    conn.execute(_bs_panel_sql(concepts, avail))
    conn.execute(_is_panel_sql(concepts, avail))
    conn.execute(_y9c_panel_sql(concepts))

    conn.execute(concepts.HARMONIZED_METADATA_DDL_Y9C)
    conn.execute("DELETE FROM harmonized_metadata_y9c")
    for row in _harmonized_metadata_rows(concepts):
        conn.execute(
            "INSERT INTO harmonized_metadata_y9c "
            "(variable_name, panel, description, unit, source_schedule, "
            "mdrm_codes, formula, available_from) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            list(row),
        )

    return ["bs_panel_y9c", "is_panel_y9c", "y9c_panel"]
