"""Steps 4 and 5: review curated dataset and export TTL."""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import streamlit as st

from curation_app.context import active_source_context
from curation_app.helpers import (
    file_to_bytes,
    read_tsv,
    render_clickable_dataframe,
    to_relpath,
)

REL_PRED = {
    "exact": "skos:exactMatch",
    "close": "skos:closeMatch",
    "related": "skos:relatedMatch",
    "broad": "skos:broadMatch",
    "narrow": "skos:narrowMatch",
}

VIEW_OPTIONS = [
    "All rows",
    "Reviewed rows (not needs_review)",
    "Automatically matched and manually validated",
    "Original terms kept",
    "Manually added terms",
]


def _safe_text(value: object) -> str:
    return str(value or "").strip()


def _escape_literal(text: str) -> str:
    return text.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _safe_id(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_\\-]", "_", value.strip())
    return cleaned or "row"


def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in [
        "alignment_id",
        "left_source",
        "left_term_iri",
        "left_label",
        "right_source",
        "right_term_iri",
        "right_label",
        "relation",
        "status",
        "match_method",
        "suggestion_source",
        "notes",
    ]:
        if col not in out.columns:
            out[col] = ""
    return out


def _apply_view(df: pd.DataFrame, view: str) -> pd.DataFrame:
    if df.empty:
        return df
    if view == "Reviewed rows (not needs_review)":
        return df[df["status"] != "needs_review"]
    if view == "Automatically matched and manually validated":
        return df[
            (df["status"] == "approved")
            & (df["suggestion_source"] == "manual_curated")
            & (~df["match_method"].str.startswith("manual_", na=False))
        ]
    if view == "Original terms kept":
        # Left terms where all rows are rejected and notes indicate keep-left decision.
        grouped = (
            df.groupby(["left_source", "left_term_iri"], dropna=False)
            .agg(
                all_rejected=("status", lambda s: all(_safe_text(v) == "rejected" for v in s)),
                notes=("notes", lambda s: " | ".join(_safe_text(v) for v in s)),
            )
            .reset_index()
        )
        keep_groups = grouped[
            grouped["all_rejected"]
            & grouped["notes"].str.lower().str.contains("kept current left term", na=False)
        ][["left_source", "left_term_iri"]]
        if keep_groups.empty:
            return df.iloc[0:0]
        return df.merge(keep_groups, on=["left_source", "left_term_iri"], how="inner")
    if view == "Manually added terms":
        return df[
            df["match_method"].str.startswith("manual_", na=False)
            | (df["suggestion_source"] == "manual_search")
        ]
    return df


def _build_ttl(df: pd.DataFrame, source_slug: str) -> str:
    lines: list[str] = [
        "@prefix skos: <http://www.w3.org/2004/02/skos/core#> .",
        "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .",
        "@prefix pf: <https://w3id.org/potential-funicular/schema#> .",
        f"@prefix pfa: <https://w3id.org/potential-funicular/alignment/{_safe_id(source_slug)}/> .",
        "",
    ]

    for _, row in df.iterrows():
        left_iri = _safe_text(row.get("left_term_iri"))
        right_iri = _safe_text(row.get("right_term_iri"))
        if not left_iri or not right_iri:
            continue
        rel = _safe_text(row.get("relation")).lower()
        pred = REL_PRED.get(rel, "skos:relatedMatch")
        align_id = _safe_id(_safe_text(row.get("alignment_id")) or f"{_safe_id(source_slug)}_{_}")
        left_label = _escape_literal(_safe_text(row.get("left_label")))
        right_label = _escape_literal(_safe_text(row.get("right_label")))
        method = _escape_literal(_safe_text(row.get("match_method")))
        notes = _escape_literal(_safe_text(row.get("notes")))
        score = _safe_text(row.get("match_score"))

        lines.append(f"<{left_iri}> {pred} <{right_iri}> .")
        lines.append(
            f"pfa:{align_id} a pf:CuratedAlignment ; "
            f'pf:sourceSlug "{_escape_literal(source_slug)}" ; '
            f"pf:leftTerm <{left_iri}> ; "
            f"pf:rightTerm <{right_iri}> ; "
            f'pf:relation "{_escape_literal(rel or "related")}" ; '
            f'pf:leftLabel "{left_label}" ; '
            f'pf:rightLabel "{right_label}" ; '
            f'pf:matchMethod "{method}" ; '
            f'pf:notes "{notes}"'
            + (f" ; pf:matchScore \"{_escape_literal(score)}\"^^xsd:decimal ." if score else " .")
        )
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def render() -> None:
    st.title("Steps 4-5: Review and Export")

    ctx = active_source_context()
    if ctx is None:
        st.warning("No source slug available. Configure Step 0 first.")
        return

    candidates_path = ctx.candidates_tsv
    df = _ensure_columns(read_tsv(candidates_path))
    if df.empty and not candidates_path.is_file():
        st.warning("No curated candidate file found yet. Please run Step 2 and Step 3 first.")
        return

    st.caption(f"Dataset: `{to_relpath(candidates_path)}`")

    view = st.selectbox("View", options=VIEW_OPTIONS, index=1)
    token = st.text_input("Filter text (labels, notes, IRIs)", value="")
    shown = _apply_view(df, view)
    if token.strip():
        t = token.strip().lower()
        hay = (
            shown["left_label"].str.lower()
            + " "
            + shown["right_label"].str.lower()
            + " "
            + shown["left_term_iri"].str.lower()
            + " "
            + shown["right_term_iri"].str.lower()
            + " "
            + shown["notes"].str.lower()
        )
        shown = shown[hay.str.contains(t, na=False)]

    st.caption(f"Rows shown: {len(shown)}")
    render_clickable_dataframe(shown, use_container_width=True, hide_index=True)

    st.subheader("Export TTL")
    status_options = sorted(df["status"].dropna().unique().tolist())
    default_statuses = ["approved"] if "approved" in status_options else status_options
    export_statuses = st.multiselect(
        "Statuses to export",
        options=status_options,
        default=default_statuses,
        help="TTL export usually uses only approved rows.",
    )
    export_df = shown[shown["status"].isin(export_statuses)] if export_statuses else shown.iloc[0:0]
    ttl_text = _build_ttl(export_df, ctx.source_id)
    st.caption(f"TTL triples from {len(export_df)} row(s)")
    st.code(ttl_text[:12000], language="turtle")

    export_dir = Path("registry/exports")
    export_dir.mkdir(parents=True, exist_ok=True)
    ttl_path = export_dir / f"{ctx.source_id}_curated_alignments.ttl"

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Write TTL file", type="primary"):
            ttl_path.write_text(ttl_text, encoding="utf-8")
            st.success(f"Wrote `{to_relpath(ttl_path)}`")
    with col2:
        st.download_button(
            label="Download TTL",
            data=ttl_text.encode("utf-8"),
            file_name=ttl_path.name,
            mime="text/turtle",
            key=f"download_ttl_{ctx.source_id}",
        )

    if ttl_path.is_file():
        st.caption(f"Latest TTL export: `{to_relpath(ttl_path)}`")
        st.download_button(
            label="Download latest written TTL",
            data=file_to_bytes(ttl_path),
            file_name=ttl_path.name,
            mime="text/turtle",
            key=f"download_ttl_written_{ctx.source_id}",
        )
