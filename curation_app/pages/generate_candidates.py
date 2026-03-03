"""Step 2: candidate generation page."""

from __future__ import annotations

import streamlit as st

from curation_app.config import DEFAULT_CURATED_FILE, DEFAULT_OLS_ONTOLOGIES_FILE
from curation_app.context import (
    STATE_SOURCE_ID,
    enabled_source_ids,
    load_manifest,
    source_context,
    source_ids,
)
from curation_app.helpers import (
    read_tsv,
    render_clickable_dataframe,
    run_python_script,
    show_command_result,
    to_relpath,
)

DEFAULT_OLS_ONTOLOGIES = ["chebi", "obi", "ms", "chmo", "edam"]


def _ontology_display(ontology: str, label_map: dict[str, str], desc_map: dict[str, str]) -> str:
    label = label_map.get(ontology, "").strip()
    description = desc_map.get(ontology, "").strip()
    if description and len(description) > 100:
        description = description[:97].rstrip() + "..."
    if label and description:
        return f"{ontology} - {label}: {description}"
    if label:
        return f"{ontology} - {label}"
    return ontology


def _ols_catalog() -> tuple[list[str], dict[str, str], dict[str, str]]:
    df = read_tsv(DEFAULT_OLS_ONTOLOGIES_FILE)
    if df.empty or "ontology" not in df.columns:
        return DEFAULT_OLS_ONTOLOGIES.copy(), {}, {}

    options: list[str] = []
    label_map: dict[str, str] = {}
    desc_map: dict[str, str] = {}
    seen: set[str] = set()

    for _, row in df.iterrows():
        ontology = str(row.get("ontology", "") or "").strip().lower()
        if not ontology or ontology in seen:
            continue
        seen.add(ontology)
        options.append(ontology)
        label_map[ontology] = str(row.get("label", "") or "").strip()
        desc_map[ontology] = str(row.get("description", "") or "").strip()

    if not options:
        return DEFAULT_OLS_ONTOLOGIES.copy(), {}, {}
    return options, label_map, desc_map


def render() -> None:
    st.title("Step 2: Generate Pairwise Candidates")

    manifest_df = load_manifest()
    source_options = enabled_source_ids(manifest_df) or source_ids(manifest_df)
    if not source_options:
        st.warning("No source slug found in manifest.")
        return

    current_source = str(st.session_state.get(STATE_SOURCE_ID, source_options[0])).strip().lower()
    if current_source not in source_options:
        current_source = source_options[0]

    st.subheader("Comparison Setup")
    left_col, right_col = st.columns(2)
    with left_col:
        left_slug = st.selectbox(
            "Left source slug",
            options=source_options,
            index=source_options.index(current_source),
            help="Local ontology slug used as the left side of matching.",
        )
    st.session_state[STATE_SOURCE_ID] = left_slug
    left_ctx = source_context(left_slug, manifest_df)

    mode = st.radio(
        "Mode",
        options=["Local vs OLS", "Local vs Local"],
        horizontal=True,
        help=(
            "Local vs OLS: match local terms against OLS search results. "
            "Local vs Local: match terms between two local ontology exports."
        ),
    )

    right_terms_path = None
    selected_ontologies: list[str] = []
    if mode == "Local vs Local":
        right_options = [slug for slug in source_options if slug != left_slug] or source_options
        with right_col:
            right_slug = st.selectbox(
                "Right source slug",
                options=right_options,
                index=0,
                help="Second local ontology slug used as the right side in Local vs Local mode.",
            )
        right_ctx = source_context(right_slug, manifest_df)
        right_terms_path = right_ctx.terms_tsv
    else:
        ols_options, label_map, desc_map = _ols_catalog()
        default_ontologies = [o for o in DEFAULT_OLS_ONTOLOGIES if o in ols_options]
        if not default_ontologies:
            default_ontologies = ols_options[: min(5, len(ols_options))]
        with right_col:
            selected_ontologies = st.multiselect(
                "OLS ontologies",
                options=ols_options,
                default=default_ontologies,
                format_func=lambda ontology: _ontology_display(ontology, label_map, desc_map),
                help=(
                    "Target OLS ontologies queried in Local vs OLS mode. "
                    "Use Step 1.5 to refresh catalog labels/descriptions."
                ),
            )

    include_existing_curated = st.checkbox(
        "Include pairs already curated",
        value=False,
        help="If off, candidate pairs already present in curated alignments are excluded.",
    )

    st.caption("Curator is fixed to `auto` at generation time.")
    max_left_terms = st.number_input(
        "Max left terms (0 = all)",
        min_value=0,
        value=0,
        step=1,
        help="Limits how many left-side terms are processed. Use 0 to process all terms.",
    )
    focus = st.text_input(
        "Focus filter (normalized label contains)",
        value="",
        help=(
            "Optional substring filter on normalized labels (lowercased, punctuation/formatting removed). "
            "Example: 'chemical entity'."
        ),
    )

    args = [
        "--left-terms",
        to_relpath(left_ctx.terms_tsv),
        "--left-source",
        left_slug.upper(),
        "--curated-alignments",
        to_relpath(DEFAULT_CURATED_FILE),
        "--output",
        to_relpath(left_ctx.candidates_tsv),
        "--max-left-terms",
        str(max_left_terms),
        "--curator",
        "auto",
    ]
    if focus.strip():
        args.extend(["--focus", focus.strip()])
    if include_existing_curated:
        args.append("--include-existing-curated")

    if mode == "Local vs OLS":
        ontologies = ",".join(selected_ontologies)
        top_n_ols = st.number_input(
            "Top N output hits per left term",
            min_value=1,
            value=3,
            step=1,
            help=(
                "How many best OLS matches to keep per left term in output candidates."
            ),
        )
        ols_rows = max(int(top_n_ols), 5)
        st.caption(f"OLS fetch depth per ontology is auto-set to `{ols_rows}`.")
        fetch_metadata = st.checkbox(
            "Fetch OLS metadata",
            value=True,
            help="Fetch definition/comment/example for returned OLS suggestions (slower).",
        )
        timeout = st.number_input(
            "OLS request timeout (seconds)",
            min_value=0.5,
            value=3.0,
            step=0.5,
            help="Network timeout per OLS API request.",
        )

        args.append("--use-ols-api")
        args.extend(["--ontologies", ontologies])
        args.extend(["--ols-rows", str(ols_rows)])
        args.extend(["--top-n-ols", str(top_n_ols)])
        args.extend(["--request-timeout", str(timeout)])
        if fetch_metadata:
            args.append("--ols-fetch-metadata")
    else:
        min_score = st.number_input(
            "Minimum score",
            min_value=0.0,
            max_value=1.0,
            value=0.82,
            step=0.01,
            help="Local-vs-local similarity threshold (0 to 1). Higher is stricter.",
        )
        args.extend(["--right-terms", to_relpath(right_ctx.terms_tsv)])
        args.extend(["--right-source", right_slug.upper()])
        args.extend(["--min-score", str(min_score)])

    submitted = st.button("Generate candidates", type="primary")
    if submitted:
        if not left_ctx.terms_tsv.is_file():
            st.error(f"Missing terms TSV for left source: `{to_relpath(left_ctx.terms_tsv)}`")
            return
        if mode == "Local vs OLS" and not selected_ontologies:
            st.error("Select at least one OLS ontology.")
            return
        if mode == "Local vs Local" and right_terms_path is not None and not right_terms_path.is_file():
            st.error(f"Missing terms TSV for right source: `{to_relpath(right_terms_path)}`")
            return
        result = run_python_script("scripts/suggest_pairwise_alignments.py", args)
        show_command_result(result)

    st.subheader("Candidates Preview")
    candidates_df = read_tsv(left_ctx.candidates_tsv)
    if candidates_df.empty and not left_ctx.candidates_tsv.is_file():
        st.info("No candidates file yet. Generate candidates first.")
        return
    st.caption(f"Rows: {len(candidates_df)}")
    render_clickable_dataframe(candidates_df.head(200), use_container_width=True, hide_index=True)
