"""Overview dashboard for alignment curation workflow."""

from __future__ import annotations

from pathlib import Path

import streamlit as st

from curation_app.config import (
    DEFAULT_CANDIDATES_FILE,
    DEFAULT_CURATED_FILE,
    DEFAULT_GROUPS_FILE,
    DEFAULT_RECONCILED_FILE,
    DEFAULT_SQLITE_DB,
)
from curation_app.helpers import read_tsv, to_relpath


def _artifact_metric(path: Path) -> tuple[str, str]:
    if path.suffix == ".tsv":
        df = read_tsv(path)
        if df.empty and not path.is_file():
            return "missing", "-"
        return "available", str(len(df))
    if path.is_file():
        return "available", f"{path.stat().st_size:,} bytes"
    return "missing", "-"


def render() -> None:
    st.title("Ontology Alignment Curation")
    st.write(
        "Run the complete pairwise + SQLite workflow with guided modules. "
        "Each module supports file preview and direct export of intermediate outputs."
    )

    artifacts = {
        "Candidate pairs": DEFAULT_CANDIDATES_FILE,
        "Curated pairs": DEFAULT_CURATED_FILE,
        "Reconciled mappings": DEFAULT_RECONCILED_FILE,
        "Canonical groups": DEFAULT_GROUPS_FILE,
        "SQLite DB": DEFAULT_SQLITE_DB,
    }

    cols = st.columns(len(artifacts))
    for col, (label, path) in zip(cols, artifacts.items()):
        status, value = _artifact_metric(path)
        icon = "OK" if status == "available" else "MISSING"
        col.metric(label, value, delta=icon)

    st.subheader("Current Artifacts")
    for label, path in artifacts.items():
        exists = "yes" if path.is_file() else "no"
        st.write(f"- {label}: `{to_relpath(path)}` (exists: {exists})")

    st.subheader("Modules")
    st.write("1. Download external ontology sources")
    st.write("2. Extract local terms from TTL")
    st.write("3. Generate pairwise candidates")
    st.write("4. Curate candidate decisions")
    st.write("5. Finalize + validate pair alignments")
    st.write("6. Sync to SQLite + export reconciled tables")
    st.write("7. Inspect SQLite canonical groups and custom queries")
