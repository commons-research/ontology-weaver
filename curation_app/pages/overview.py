"""Overview dashboard for alignment curation workflow."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from curation_app.context import enabled_source_ids, load_manifest, source_context, source_ids
from curation_app.helpers import read_tsv, render_clickable_dataframe, to_relpath

STATE_PAGE = "active_page"


def _nav_button(label: str, page_name: str, help_text: str) -> None:
    if st.button(label, help=help_text, use_container_width=True):
        st.session_state[STATE_PAGE] = page_name
        st.rerun()


def _source_metrics_df() -> pd.DataFrame:
    manifest_df = load_manifest()
    ids = enabled_source_ids(manifest_df) or source_ids(manifest_df)
    rows: list[dict[str, object]] = []
    for source_id in ids:
        ctx = source_context(source_id, manifest_df)

        terms_df = read_tsv(ctx.terms_tsv)
        terms_loaded = len(terms_df) if ctx.terms_tsv.is_file() else 0

        cand_df = read_tsv(ctx.candidates_tsv)
        candidate_rows = len(cand_df) if ctx.candidates_tsv.is_file() else 0

        terms_in_scope = 0
        terms_needs_review = 0
        terms_curated = 0
        terms_approved = 0
        progress_pct = 0.0

        if candidate_rows > 0 and {"left_source", "left_term_iri", "status"}.issubset(cand_df.columns):
            grouped = (
                cand_df.groupby(["left_source", "left_term_iri"], dropna=False)["status"]
                .agg(
                    has_needs_review=lambda series: any(str(v) == "needs_review" for v in series),
                    has_approved=lambda series: any(str(v) == "approved" for v in series),
                )
                .reset_index()
            )
            terms_in_scope = len(grouped)
            terms_needs_review = int(grouped["has_needs_review"].sum()) if not grouped.empty else 0
            terms_curated = max(0, terms_in_scope - terms_needs_review)
            terms_approved = int(grouped["has_approved"].sum()) if not grouped.empty else 0
            progress_pct = (100.0 * terms_curated / terms_in_scope) if terms_in_scope else 0.0

        ttl_status = "yes" if ctx.download_ttl.is_file() else "no"
        rows.append(
            {
                "Source": ctx.source_label,
                "TTL downloaded": ttl_status,
                "Terms loaded": terms_loaded,
                "Candidate rows": candidate_rows,
                "Terms in curation": terms_in_scope,
                "Curated terms": terms_curated,
                "Terms needs review": terms_needs_review,
                "Approved terms": terms_approved,
                "Progress %": f"{progress_pct:.1f}",
                "TTL file": to_relpath(ctx.download_ttl),
                "Terms file": to_relpath(ctx.terms_tsv),
                "Candidates file": to_relpath(ctx.candidates_tsv),
            }
        )
    return pd.DataFrame(rows)


def render() -> None:
    st.title("Schema Alignment")
    st.write(
        "Use this dashboard to curate and align local schemas with available ontologies."
    )

    st.subheader("Schema metrics")
    metrics_df = _source_metrics_df()
    if metrics_df.empty:
        st.info("No sources found in manifest yet.")
    else:
        render_clickable_dataframe(metrics_df, use_container_width=True, hide_index=True)
        st.subheader("Curation progress by schema")
        for _, row in metrics_df.iterrows():
            source = str(row.get("Source", "") or "-")
            curated = int(row.get("Curated terms", 0) or 0)
            total = int(row.get("Terms in curation", 0) or 0)
            pct = (curated / total) if total else 0.0
            st.write(f"**{source}**")
            st.progress(pct, text=f"{curated}/{total} terms curated ({pct * 100:.1f}%)")

    st.subheader("Recommended flow")
    st.write("1. **Fetch schemas and ontologies**: maintain source manifest, download TTLs, and browse OLS catalog.")
    st.write("2. **Extract terms**: parse local TTL into term TSV with labels and metadata.")
    st.write("3. **Generate candidates**: build left-vs-right or left-vs-OLS candidate matches.")
    st.write("4. **Add terms**: create missing source classes/properties and seed mapping candidates.")
    st.write("5. **Curate candidates**: validate one match (or keep left term) for each left concept.")
    st.write("6. **Review and export**: filter curated dataset and export updated source TTL.")
    st.write("7. **View schema**: inspect ontology documentation before/after curation with pyLODE.")
    st.write("8. **Inspect SQLite**: run table previews and SQL checks on auto-synced reconciliation tables.")

    st.subheader("Open modules")
    c1, c2 = st.columns(2)
    with c1:
        _nav_button(
            "Fetch schemas",
            "Fetch schemas",
            "Manage manifest and download source TTL files.",
        )
        _nav_button(
            "Extract terms",
            "Extract terms",
            "Extract terms from current source TTL.",
        )
        _nav_button(
            "Curate candidates",
            "Curate candidates",
            "Review and validate candidate matches.",
        )
        _nav_button(
            "Add terms",
            "Add terms",
            "Add missing source terms and create candidate mappings.",
        )
        _nav_button(
            "Inspect SQLite",
            "Inspect SQLite",
            "Query auto-synced reconciliation tables.",
        )
    with c2:
        _nav_button(
            "OLS catalog",
            "OLS catalog",
            "Browse/search available OLS ontologies and metadata.",
        )
        _nav_button(
            "Generate candidates",
            "Generate candidates",
            "Generate candidate mappings from extracted terms.",
        )
        _nav_button(
            "Review and export",
            "Review and export",
            "Review curated rows and export updated source TTL.",
        )
        _nav_button(
            "View schema",
            "View schema",
            "Generate and view ontology documentation with pyLODE.",
        )
