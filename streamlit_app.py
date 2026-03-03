"""Main Streamlit entrypoint for ontology curation workflow."""

from __future__ import annotations

import streamlit as st

from curation_app.context import enabled_source_ids, load_manifest, source_context, source_ids
from curation_app.pages import (
    curate_candidates,
    download_sources,
    extract_terms,
    finalize_validate,
    generate_candidates,
    ols_ontologies,
    overview,
    sqlite_inspect,
    sync_export,
)

STATE_SOURCE_ID = "active_source_id"


PAGES = {
    "Overview": overview.render,
    "Step 0 - Download": download_sources.render,
    "Step 1 - Extract": extract_terms.render,
    "Step 1.5 - OLS Ontologies": ols_ontologies.render,
    "Step 2 - Generate": generate_candidates.render,
    "Step 3 - Curate": curate_candidates.render,
    "Step 4-5 - Review + Export": finalize_validate.render,
    "Step 6 - Sync + Export": sync_export.render,
    "Step 7 - SQLite Inspect": sqlite_inspect.render,
}


def main() -> None:
    st.set_page_config(
        page_title="Ontology Alignment Curation",
        layout="wide",
    )

    manifest_df = load_manifest()
    all_source_ids = source_ids(manifest_df)
    enabled_ids = enabled_source_ids(manifest_df)
    available_ids = enabled_ids or all_source_ids

    st.sidebar.title("Workflow")
    if available_ids:
        default_source = st.session_state.get(STATE_SOURCE_ID, available_ids[0])
        default_idx = available_ids.index(default_source) if default_source in available_ids else 0
        selected_source_id = st.sidebar.selectbox(
            "Source ID",
            options=available_ids,
            index=default_idx,
            help="Single source slug reused automatically across workflow steps.",
        )
        st.session_state[STATE_SOURCE_ID] = selected_source_id
        ctx = source_context(selected_source_id, manifest_df)
        st.sidebar.caption(f"TTL: `{ctx.download_ttl.name}`")
        st.sidebar.caption(f"Terms: `{ctx.terms_tsv.name}`")
        st.sidebar.caption(f"Candidates: `{ctx.candidates_tsv.name}`")
    else:
        st.sidebar.error("No source_id found in registry/external_sources.tsv")

    st.sidebar.title("Workflow Modules")
    selected_page = st.sidebar.radio("Navigate", options=list(PAGES.keys()))

    PAGES[selected_page]()


if __name__ == "__main__":
    main()
