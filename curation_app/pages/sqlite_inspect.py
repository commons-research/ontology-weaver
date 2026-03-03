"""SQLite inspection and query page."""

from __future__ import annotations

import streamlit as st

from curation_app.config import DEFAULT_SQLITE_DB
from curation_app.helpers import (
    dataframe_to_tsv_bytes,
    render_clickable_dataframe,
    sqlite_query,
    sqlite_tables,
    to_relpath,
)

QUERY_EXAMPLES = {
    "Top canonical groups by mapped count": (
        "SELECT canonical_term_iri, canonical_term_label, canonical_term_source, mapped_term_count "
        "FROM reconciled_canonical_groups "
        "ORDER BY CAST(mapped_term_count AS INTEGER) DESC "
        "LIMIT 200"
    ),
    "Approved mappings by canonical source": (
        "SELECT canonical_term_source, COUNT(*) AS mapping_count "
        "FROM reconciled_mappings "
        "GROUP BY canonical_term_source "
        "ORDER BY mapping_count DESC"
    ),
    "Mappings with missing reviewer": (
        "SELECT alignment_id, source_term_source, source_term_label, canonical_term_label "
        "FROM reconciled_mappings "
        "WHERE COALESCE(TRIM(reviewer), '') = '' "
        "LIMIT 200"
    ),
    "Top left sources in candidate table": (
        "SELECT left_source, status, COUNT(*) AS n "
        "FROM pair_alignment_candidates "
        "GROUP BY left_source, status "
        "ORDER BY left_source, status"
    ),
    "Latest reviewed curated alignments": (
        "SELECT alignment_id, left_source, left_label, canonical_term_label, date_reviewed "
        "FROM pair_alignments "
        "WHERE COALESCE(TRIM(date_reviewed), '') <> '' "
        "ORDER BY date_reviewed DESC "
        "LIMIT 200"
    ),
}


def render() -> None:
    st.title("Inspect SQLite")
    db_path = to_relpath(DEFAULT_SQLITE_DB)
    if not DEFAULT_SQLITE_DB.is_file():
        st.warning(f"Database file not found: `{db_path}`")
        return

    st.caption(f"Using SQLite DB: `{db_path}`")
    table_names = sqlite_tables(db_path)
    st.write(f"Tables: {', '.join(table_names) if table_names else '-'}")

    st.subheader("Quick table preview")
    preview_table = st.selectbox("Table", options=table_names or ["(none)"])
    preview_limit = st.number_input("Limit", min_value=1, value=100, step=10)

    if table_names and st.button("Preview table"):
        query = f"SELECT * FROM {preview_table} LIMIT {int(preview_limit)}"
        try:
            df = sqlite_query(db_path, query)
        except Exception as exc:
            st.error(f"Query failed: {exc}")
        else:
            render_clickable_dataframe(df, use_container_width=True, hide_index=True)
            st.download_button(
                label="Download preview TSV",
                data=dataframe_to_tsv_bytes(df),
                file_name=f"{preview_table}_preview.tsv",
                mime="text/tab-separated-values",
                key="download_sqlite_preview",
            )

    st.subheader("Custom SQL")
    example_name = st.selectbox(
        "SQL examples",
        options=list(QUERY_EXAMPLES.keys()),
        index=0,
        help="Select a useful query template and click 'Use selected example'.",
    )
    if st.button("Use selected example"):
        st.session_state["sqlite_query_text"] = QUERY_EXAMPLES[example_name]

    if "sqlite_query_text" not in st.session_state:
        st.session_state["sqlite_query_text"] = QUERY_EXAMPLES["Top canonical groups by mapped count"]
    query_text = st.text_area("SQL query", height=160, key="sqlite_query_text")

    if st.button("Run query", type="primary"):
        try:
            df = sqlite_query(db_path, query_text)
        except Exception as exc:
            st.error(f"Query failed: {exc}")
        else:
            st.write(f"Rows: {len(df)}")
            render_clickable_dataframe(df, use_container_width=True, hide_index=True)
            st.download_button(
                label="Download query TSV",
                data=dataframe_to_tsv_bytes(df),
                file_name="sqlite_query_results.tsv",
                mime="text/tab-separated-values",
                key="download_sqlite_query",
            )
