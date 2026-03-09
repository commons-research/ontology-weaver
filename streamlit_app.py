"""Main Streamlit entrypoint for ontology curation workflow."""

from __future__ import annotations

import streamlit as st

from curation_app.auto_sync import STATE_SYNC_LAST_ERROR, auto_sync_sqlite
from curation_app.context import enabled_source_ids, load_manifest, source_context, source_ids
from curation_app.helpers import fetch_orcid_display_name, is_valid_orcid, normalize_orcid
from curation_app.pages import (
    add_terms,
    curate_candidates,
    download_sources,
    extract_terms,
    finalize_validate,
    generate_candidates,
    ols_ontologies,
    overview,
    sqlite_inspect,
    view_schema,
)

STATE_SOURCE_ID = "active_source_id"
STATE_CURATOR = "active_curator"
STATE_CURATOR_INPUT = "active_curator_input"
STATE_CURATOR_NAME = "active_curator_name"
STATE_PAGE = "active_page"


PAGES = {
    "Overview": overview.render,
    "Fetch schemas": download_sources.render,
    "OLS catalog": ols_ontologies.render,
    "Extract terms": extract_terms.render,
    "Generate candidates": generate_candidates.render,
    "Add terms": add_terms.render,
    "Curate candidates": curate_candidates.render,
    "Review and export": finalize_validate.render,
    "View schema": view_schema.render,
    "Inspect SQLite": sqlite_inspect.render,
}

PAGE_GROUPS = {
    "Overview": ["Overview"],
    "Fetch schemas and ontologies": [
        "Fetch schemas",
        "OLS catalog",
    ],
    "Alignment workflow": [
        "Extract terms",
        "Generate candidates",
        "Add terms",
        "Curate candidates",
        "Review and export",
    ],
    "Data inspection": ["Inspect SQLite", "View schema"],
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
        curator_value = str(
            st.session_state.get(STATE_CURATOR_INPUT, st.session_state.get(STATE_CURATOR, "")) or ""
        ).strip()
        curator_input = st.sidebar.text_input(
            "Curator ORCID",
            value=curator_value,
            help="Enter a valid ORCID iD. The public name is fetched automatically from ORCID.",
        )
        st.session_state[STATE_CURATOR_INPUT] = curator_input.strip()
        normalized_orcid = normalize_orcid(curator_input)
        if curator_input.strip():
            if is_valid_orcid(normalized_orcid):
                st.session_state[STATE_CURATOR] = normalized_orcid
                display_name, lookup_error = fetch_orcid_display_name(normalized_orcid)
                st.sidebar.caption(f"Normalized ORCID: `{normalized_orcid}`")
                if display_name:
                    st.session_state[STATE_CURATOR_NAME] = display_name
                    st.sidebar.caption(f"Resolved name: `{display_name}`")
                elif lookup_error:
                    st.session_state[STATE_CURATOR] = ""
                    st.session_state[STATE_CURATOR_NAME] = ""
                    st.sidebar.warning(lookup_error)
            else:
                st.session_state[STATE_CURATOR] = ""
                st.session_state[STATE_CURATOR_NAME] = ""
                st.sidebar.error("Enter a valid ORCID iD, for example `0000-0002-1825-0097`.")
        else:
            st.session_state[STATE_CURATOR] = ""
            st.session_state[STATE_CURATOR_NAME] = ""
        ctx = source_context(selected_source_id, manifest_df)
        st.sidebar.caption(f"TTL: `{ctx.download_ttl.name}`")
        st.sidebar.caption(f"Terms: `{ctx.terms_tsv.name}`")
        st.sidebar.caption(f"Review ledger: `{ctx.review_tsv.name}`")
        st.sidebar.caption(f"Local queue: `{ctx.queue_tsv.name}`")
    else:
        st.sidebar.error("No source_id found in registry/external_sources.tsv")

    sync_ok, sync_msg = auto_sync_sqlite(manifest_df)
    if not sync_ok:
        st.sidebar.warning(sync_msg)
        detail = str(st.session_state.get(STATE_SYNC_LAST_ERROR, "")).strip()
        if detail:
            st.sidebar.caption(detail[:300])

    if STATE_PAGE not in st.session_state or st.session_state[STATE_PAGE] not in PAGES:
        st.session_state[STATE_PAGE] = "Overview"

    st.sidebar.title("Workflow Modules")
    for section_name, page_names in PAGE_GROUPS.items():
        with st.sidebar.expander(section_name, expanded=(section_name == "Overview")):
            for page_name in page_names:
                is_active = st.session_state[STATE_PAGE] == page_name
                label = f"• {page_name}" if is_active else page_name
                if st.button(label, key=f"nav_{page_name}", use_container_width=True):
                    st.session_state[STATE_PAGE] = page_name
                    st.rerun()
    selected_page = st.session_state[STATE_PAGE]
    st.sidebar.caption(f"Current page: `{selected_page}`")
    PAGES[selected_page]()


if __name__ == "__main__":
    main()
