"""OLS ontology catalog fetch and preview page."""

from __future__ import annotations

import streamlit as st

from curation_app.config import DEFAULT_OLS_ONTOLOGIES_FILE
from curation_app.helpers import (
    read_tsv,
    render_clickable_dataframe,
    run_python_script,
    show_command_result,
    to_relpath,
)


def render() -> None:
    st.title("OLS Ontology Catalog")
    st.write(
        "Fetch ontology IDs, short descriptions, and links (homepage/OLS page) from OLS4 "
        "for use in candidate generation."
    )

    timeout = st.number_input("Request timeout (seconds)", min_value=1.0, value=10.0, step=1.0)
    page_size = st.number_input("API page size", min_value=10, value=200, step=10)
    fetch_details = st.checkbox("Fetch detailed ontology metadata (slower)", value=True)

    if st.button("Fetch ontologies from OLS", type="primary"):
        args = [
            "--output",
            to_relpath(DEFAULT_OLS_ONTOLOGIES_FILE),
            "--timeout",
            str(timeout),
            "--page-size",
            str(page_size),
        ]
        if fetch_details:
            args.append("--fetch-details")
        result = run_python_script("scripts/fetch_ols_ontologies.py", args)
        show_command_result(result)

    catalog_df = read_tsv(DEFAULT_OLS_ONTOLOGIES_FILE)
    st.subheader("Catalog Preview")
    if catalog_df.empty and not DEFAULT_OLS_ONTOLOGIES_FILE.is_file():
        st.info("No local OLS catalog file yet. Click 'Fetch ontologies from OLS'.")
        return
    st.caption(f"Rows: {len(catalog_df)}")
    render_clickable_dataframe(catalog_df.head(500), use_container_width=True, hide_index=True)
