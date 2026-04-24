"""
SQL generator for the harmonized view layer.

Reads concept dicts from ``concepts.py`` and materializes:

  - view  ``filers_panel``          (cleaned POR: id_rssd, date, form_type, name, city, state, zip)
  - view  ``bs_panel``              (balance sheet concepts + identity cols)
  - view  ``is_panel``              (income statement concepts + identity cols)
  - view  ``call_reports_panel``    (bs_panel JOIN is_panel; convenience)
  - table ``harmonized_metadata``   (one row per variable — self-documenting)

Called from ``construct.py::refresh_views``.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import duckdb

_HERE = Path(__file__).resolve().parent


def _load_concepts():
    spec = importlib.util.spec_from_file_location(
        "ffiec_harmonized_concepts", _HERE / "concepts.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[attr-defined]
    return mod


# ── SQL builders ──────────────────────────────────────────────────────────────

def _filers_panel_sql(concepts) -> str:
    """
    Build ``filers_panel`` from ``call_filers``. One row per filer × quarter
    with renamed CFLV-style columns.
    """
    exprs = [f"{spec['sql']} AS {name}" for name, spec in concepts.FILERS_CONCEPTS.items()]
    return f"""
        CREATE OR REPLACE VIEW filers_panel AS
        SELECT
            cf.IDRSSD,
            cf.activity_year,
            cf.activity_quarter,
            {','.join(chr(10) + '            ' + e for e in exprs)}
        FROM call_filers cf
    """


def _bs_panel_sql(concepts) -> str:
    """Build ``bs_panel`` — schedule_rc LEFT JOIN rcci, rcn, rck, filers_panel."""
    bs_exprs = [f"{spec['sql']} AS {name}" for name, spec in concepts.BS_CONCEPTS.items()]
    filer_cols = [name for name in concepts.FILERS_CONCEPTS.keys()]
    filer_select = ",\n            ".join(f"f.{c}" for c in filer_cols)
    bs_select = ",\n            ".join(bs_exprs)

    return f"""
        CREATE OR REPLACE VIEW bs_panel AS
        SELECT
            rc.IDRSSD                                    AS idrssd,
            rc.activity_year,
            rc.activity_quarter,
            {filer_select},
            {bs_select}
        FROM schedule_rc rc
        LEFT JOIN schedule_rcb rcb
            USING (IDRSSD, activity_year, activity_quarter)
        LEFT JOIN schedule_rcci rcci
            USING (IDRSSD, activity_year, activity_quarter)
        LEFT JOIN schedule_rce rce
            USING (IDRSSD, activity_year, activity_quarter)
        LEFT JOIN schedule_rcm rcm
            USING (IDRSSD, activity_year, activity_quarter)
        LEFT JOIN schedule_rcn rcn
            USING (IDRSSD, activity_year, activity_quarter)
        LEFT JOIN schedule_rck rck
            USING (IDRSSD, activity_year, activity_quarter)
        LEFT JOIN filers_panel f
            USING (IDRSSD, activity_year, activity_quarter)
    """


def qtr_name(ytd_name: str) -> str:
    """Convert a ytd concept name to its quarterly-flow counterpart.

    ytdnetinc -> q_netinc ; ytdint_inc -> q_int_inc ; ytdcommdividend -> q_commdividend.
    Anything not starting with 'ytd' gets 'q_' prefixed as-is.
    """
    if ytd_name.startswith("ytd"):
        return "q_" + ytd_name[3:]
    return "q_" + ytd_name


def _is_panel_sql(concepts) -> str:
    """Build ``is_panel`` — schedule_ri LEFT JOIN ria LEFT JOIN ribii LEFT JOIN
    filers_panel. Wraps with a window-function layer that adds a quarterly-flow
    column (``q_<name>``) for every YTD concept.

    Q1 flow = Q1 YTD (LAG returns NULL -> COALESCE 0).
    Q2 flow = Q2 YTD - Q1 YTD.  Q3 flow = Q3 YTD - Q2 YTD.  Q4 flow = Q4 YTD - Q3 YTD.
    """
    is_exprs = [f"{spec['sql']} AS {name}" for name, spec in concepts.IS_CONCEPTS.items()]
    filer_cols = [name for name in concepts.FILERS_CONCEPTS.keys()]
    filer_select = ",\n                ".join(f"f.{c}" for c in filer_cols)
    is_select = ",\n                ".join(is_exprs)

    # Quarterly-flow derivations via LAG within (id_rssd, activity_year).
    # Skip num_employees — it's point-in-time, not YTD.
    ytd_cols = [
        n for n, spec in concepts.IS_CONCEPTS.items()
        if n != "num_employees"
    ]
    qtr_exprs = []
    for n in ytd_cols:
        qn = qtr_name(n)
        qtr_exprs.append(
            f"{n} - COALESCE(LAG({n}) OVER w, 0) AS {qn}"
        )
    qtr_select = ",\n            ".join(qtr_exprs)

    return f"""
        CREATE OR REPLACE VIEW is_panel AS
        WITH ytd AS (
            SELECT
                ri.IDRSSD                                    AS idrssd,
                ri.activity_year,
                ri.activity_quarter,
                {filer_select},
                {is_select}
            FROM schedule_ri ri
            LEFT JOIN schedule_ria ria
                USING (IDRSSD, activity_year, activity_quarter)
            LEFT JOIN schedule_ribi ribi
                USING (IDRSSD, activity_year, activity_quarter)
            LEFT JOIN schedule_ribii ribii
                USING (IDRSSD, activity_year, activity_quarter)
            LEFT JOIN filers_panel f
                USING (IDRSSD, activity_year, activity_quarter)
        )
        SELECT *,
            {qtr_select}
        FROM ytd
        WINDOW w AS (
            PARTITION BY id_rssd, activity_year
            ORDER BY activity_quarter
        )
    """


def _call_reports_panel_sql(concepts) -> str:
    """Convenience: bs_panel LEFT JOIN is_panel on identity columns."""
    # Identity columns to USING-join (so they deduplicate cleanly)
    identity_cols = ["idrssd", "activity_year", "activity_quarter"] + list(
        concepts.FILERS_CONCEPTS.keys()
    )
    using_clause = "(" + ", ".join(identity_cols) + ")"

    is_value_cols = list(concepts.IS_CONCEPTS.keys())
    # Also include the qtr_* derived flows
    is_value_cols += [qtr_name(n) for n in concepts.IS_CONCEPTS.keys() if n != "num_employees"]
    is_select = ",\n            ".join(f"i.{c}" for c in is_value_cols)

    return f"""
        CREATE OR REPLACE VIEW call_reports_panel AS
        SELECT bs.*,
            {is_select}
        FROM bs_panel bs
        LEFT JOIN is_panel i
            USING {using_clause}
    """


def _harmonized_metadata_rows(concepts) -> list[tuple]:
    """Flatten concept dicts into (variable_name, panel, description, unit,
    source_schedule, mdrm_codes_csv, formula, available_from) tuples.

    Also generates derived rows for each qtr_* flow column on is_panel.
    """
    rows: list[tuple] = []
    for panel_name, cdict in (
        ("bs_panel", concepts.BS_CONCEPTS),
        ("is_panel", concepts.IS_CONCEPTS),
        ("filers_panel", concepts.FILERS_CONCEPTS),
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

    # Derived quarterly-flow variables on is_panel
    for name, spec in concepts.IS_CONCEPTS.items():
        if name == "num_employees":
            continue
        qn = qtr_name(name)
        desc = spec["desc"]
        # strip the "(YTD)" annotation and add "(current quarter)"
        desc = desc.replace(" (YTD)", "").rstrip() + " (current quarter, derived)"
        formula = (
            f"{name} - COALESCE(LAG({name}) "
            f"OVER (PARTITION BY id_rssd, activity_year ORDER BY activity_quarter), 0)"
        )
        rows.append((
            qn,
            "is_panel",
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
    """Create/replace harmonized views + metadata table. Returns the list of
    view names created."""
    concepts = _load_concepts()

    # Order matters: filers_panel first (bs_panel / is_panel join into it).
    conn.execute(_filers_panel_sql(concepts))
    conn.execute(_bs_panel_sql(concepts))
    conn.execute(_is_panel_sql(concepts))
    conn.execute(_call_reports_panel_sql(concepts))

    # Harmonized metadata table
    conn.execute(concepts.HARMONIZED_METADATA_DDL)
    conn.execute("DELETE FROM harmonized_metadata")
    for row in _harmonized_metadata_rows(concepts):
        conn.execute(
            "INSERT INTO harmonized_metadata "
            "(variable_name, panel, description, unit, source_schedule, "
            "mdrm_codes, formula, available_from) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            list(row),
        )

    return ["filers_panel", "bs_panel", "is_panel", "call_reports_panel"]
