"""Step 7: SQLite inspection and query page."""

from __future__ import annotations

import streamlit as st

from curation_app.config import DEFAULT_SQLITE_DB
from curation_app.helpers import (
    dataframe_to_tsv_bytes,
    render_clickable_dataframe,
    sqlite_query,
    sqlite_tables,
    to_path,
    to_relpath,
)


def render() -> None:
    st.title("Step 7: Inspect SQLite")

    db_path = st.text_input("SQLite DB", value=to_relpath(DEFAULT_SQLITE_DB))
    resolved_db = to_path(db_path)

    if not resolved_db.is_file():
        st.warning(f"Database file not found: `{db_path}`")
        return

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
    default_query = (
        "SELECT canonical_term_iri, canonical_term_label, mapped_term_count "
        "FROM reconciled_canonical_groups ORDER BY mapped_term_count DESC LIMIT 200"
    )
    query_text = st.text_area("SQL query", value=default_query, height=160)

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
