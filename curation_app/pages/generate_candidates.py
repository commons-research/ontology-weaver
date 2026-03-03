"""Candidate generation page."""

from __future__ import annotations

import shlex
import subprocess
import sys

import streamlit as st

from curation_app.config import DEFAULT_CURATED_FILE, DEFAULT_OLS_ONTOLOGIES_FILE, ROOT_DIR
from curation_app.context import (
    STATE_SOURCE_ID,
    enabled_source_ids,
    load_manifest,
    source_context,
    source_ids,
)
from curation_app.helpers import (
    CommandResult,
    read_tsv,
    render_clickable_dataframe,
    show_command_result,
    to_relpath,
)

DEFAULT_OLS_ONTOLOGIES = ["chebi", "obi", "ms", "chmo", "edam"]
STATE_PAGE = "active_page"


def _ontology_display(
    ontology: str,
    label_map: dict[str, str],
    desc_map: dict[str, str],
) -> str:
    label = label_map.get(ontology, "").strip()
    description = desc_map.get(ontology, "").strip()
    if description and len(description) > 100:
        description = description[:97].rstrip() + "..."
    if label and description:
        return f"{ontology} - {label}: {description}"
    if label:
        return f"{ontology} - {label}"
    return ontology


def _ols_catalog() -> tuple[list[str], dict[str, str], dict[str, str], dict[str, str]]:
    df = read_tsv(DEFAULT_OLS_ONTOLOGIES_FILE)
    if df.empty or "ontology" not in df.columns:
        return DEFAULT_OLS_ONTOLOGIES.copy(), {}, {}, {}

    options: list[str] = []
    label_map: dict[str, str] = {}
    desc_map: dict[str, str] = {}
    url_map: dict[str, str] = {}
    seen: set[str] = set()

    for _, row in df.iterrows():
        ontology = str(row.get("ontology", "") or "").strip().lower()
        if not ontology or ontology in seen:
            continue
        seen.add(ontology)
        options.append(ontology)
        label_map[ontology] = str(row.get("label", "") or "").strip()
        desc_map[ontology] = str(row.get("description", "") or "").strip()
        url_map[ontology] = str(row.get("ols_url", "") or row.get("url", "") or "").strip()

    if not options:
        return DEFAULT_OLS_ONTOLOGIES.copy(), {}, {}, {}
    return options, label_map, desc_map, url_map


def _run_generate_with_progress(args: list[str]) -> CommandResult:
    script_path = (ROOT_DIR / "scripts/suggest_pairwise_alignments.py").resolve()
    cmd = [sys.executable, str(script_path), *args, "--emit-progress"]
    command_text = " ".join(shlex.quote(part) for part in cmd)

    progress_box = st.progress(0.0, text="Starting candidate generation...")
    log_box = st.empty()
    other_lines: list[str] = []
    stderr_lines: list[str] = []

    with subprocess.Popen(
        cmd,
        cwd=ROOT_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    ) as proc:
        assert proc.stdout is not None
        for raw in proc.stdout:
            line = raw.rstrip("\n")
            if line.startswith("PROGRESS\t"):
                parts = line.split("\t", 3)
                if len(parts) >= 4:
                    try:
                        current = int(parts[1])
                        total = int(parts[2])
                    except ValueError:
                        current = 0
                        total = 0
                    phase = parts[3].strip() or "processing"
                    frac = (current / total) if total > 0 else 0.0
                    frac = max(0.0, min(1.0, frac))
                    progress_box.progress(frac, text=f"{phase}: {current}/{total}")
                continue
            if line.strip():
                other_lines.append(line)
                log_box.code("\n".join(other_lines[-8:]), language="text")

        # Drain stderr at end.
        if proc.stderr is not None:
            stderr_text = proc.stderr.read().strip()
            if stderr_text:
                stderr_lines.append(stderr_text)
        returncode = proc.wait()

    if returncode == 0:
        progress_box.progress(1.0, text="Generation complete")
    else:
        progress_box.progress(0.0, text="Generation failed")

    return CommandResult(
        command=command_text,
        returncode=returncode,
        stdout="\n".join(other_lines).strip(),
        stderr="\n".join(stderr_lines).strip(),
    )


def render() -> None:
    st.title("Generate Pairwise Candidates")
    st.caption(
        "This module creates candidate matches between terms in your selected source schema and either "
        "another local schema or OLS ontologies."
    )
    st.markdown(
        "1. Select the left source schema.\n"
        "2. Choose mode: Local vs OLS or Local vs Local.\n"
        "3. Select target ontology/ontologies (or right local schema).\n"
        "4. Set optional filters and click **Generate candidates**."
    )

    manifest_df = load_manifest()
    source_options = enabled_source_ids(manifest_df) or source_ids(manifest_df)
    if not source_options:
        st.warning("No source slug found in manifest.")
        return

    current_source = str(st.session_state.get(STATE_SOURCE_ID, source_options[0])).strip().lower()
    if current_source not in source_options:
        current_source = source_options[0]

    st.subheader("Comparison Setup")
    mode_key = f"generate_mode_{current_source}"
    if mode_key not in st.session_state:
        st.session_state[mode_key] = "Local vs OLS"
    st.markdown("**Mode**")
    mode_btn_col1, mode_btn_col2 = st.columns(2)
    with mode_btn_col1:
        if st.button(
            "Local vs OLS",
            type="primary" if st.session_state[mode_key] == "Local vs OLS" else "secondary",
            use_container_width=True,
            key=f"{mode_key}_ols",
        ):
            st.session_state[mode_key] = "Local vs OLS"
            st.rerun()
    with mode_btn_col2:
        if st.button(
            "Local vs Local",
            type="primary" if st.session_state[mode_key] == "Local vs Local" else "secondary",
            use_container_width=True,
            key=f"{mode_key}_local",
        ):
            st.session_state[mode_key] = "Local vs Local"
            st.rerun()

    mode = st.session_state[mode_key]
    st.caption(
        "Local vs OLS: match local terms against OLS search results. "
        "Local vs Local: match terms between two local ontology exports."
    )

    left_slug = current_source
    st.session_state[STATE_SOURCE_ID] = left_slug
    left_col, right_col = st.columns(2)
    with left_col:
        st.text_input(
            "Left source slug",
            value=left_slug,
            disabled=True,
            help="Left source is controlled from the sidebar Source ID selector.",
        )
        st.caption("To switch source, use the Source ID selector in the left sidebar.")
    left_ctx = source_context(left_slug, manifest_df)

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
        ols_options, label_map, desc_map, url_map = _ols_catalog()
        default_ontologies = [o for o in DEFAULT_OLS_ONTOLOGIES if o in ols_options]
        if not default_ontologies:
            default_ontologies = ols_options[: min(5, len(ols_options))]
        selected_key = f"generate_selected_ontologies_{left_slug}"
        if selected_key not in st.session_state:
            st.session_state[selected_key] = default_ontologies.copy()
        with right_col:
            st.markdown("**OLS ontology picker**")
            search_token = st.text_input(
                "Search ontology",
                value="",
                key=f"generate_ols_search_{left_slug}",
                help="Search by ontology ID, label, or description.",
            )
            needle = search_token.strip().lower()
            filtered_ontologies = []
            for ontology in ols_options:
                label = label_map.get(ontology, "").lower()
                desc = desc_map.get(ontology, "").lower()
                hay = f"{ontology} {label} {desc}"
                if not needle or needle in hay:
                    filtered_ontologies.append(ontology)
            picker_options = filtered_ontologies[:200]
            if not picker_options:
                st.info("No ontology found for this search.")
            picked = st.selectbox(
                "Search results",
                options=picker_options,
                format_func=lambda ontology: _ontology_display(ontology, label_map, desc_map),
                key=f"generate_ols_pick_{left_slug}",
                disabled=not bool(picker_options),
            )
            add_col, open_col = st.columns(2)
            if add_col.button("Add ontology", key=f"generate_ols_add_{left_slug}", disabled=not bool(picker_options)):
                selected_values = [str(x) for x in st.session_state.get(selected_key, [])]
                if picked and picked not in selected_values:
                    selected_values.append(picked)
                st.session_state[selected_key] = selected_values
                st.rerun()
            picked_url = url_map.get(picked, "").strip() if picker_options else ""
            if picked_url:
                open_col.link_button(
                    "Open OLS page",
                    picked_url,
                    use_container_width=True,
                )
            selected_values = [str(x) for x in st.session_state.get(selected_key, [])]
            selected_values = st.multiselect(
                "Selected ontologies",
                options=ols_options,
                default=[x for x in selected_values if x in ols_options],
                format_func=lambda ontology: _ontology_display(ontology, label_map, desc_map),
                help="Ontologies used in Local vs OLS generation.",
            )
            st.session_state[selected_key] = selected_values
            selected_ontologies = selected_values

    include_existing_curated = st.checkbox(
        "Include pairs already curated",
        value=False,
        help="If off, candidate pairs already present in curated alignments are excluded.",
    )

    st.caption("Curator is fixed to `auto` at generation time.")
    top_n_ols = 3
    fetch_metadata = True
    ols_rows = 5
    timeout = 3.0
    max_left_terms = 0
    with st.expander("Advanced settings", expanded=False):
        focus = st.text_input(
            "Focus filter (normalized label contains)",
            value="",
            help=(
                "Optional substring filter on normalized labels (lowercased, punctuation/formatting removed). "
                "Example: 'chemical entity'."
            ),
        )
        if mode == "Local vs OLS":
            top_n_ols = st.number_input(
                "Top N output hits per left term",
                min_value=1,
                value=3,
                step=1,
                help=(
                    "How many best OLS matches to keep per left term in output candidates."
                ),
            )
            fetch_metadata = st.checkbox(
                "Fetch OLS metadata",
                value=True,
                help="Fetch definition/comment/example for returned OLS suggestions (slower).",
            )
            ols_rows = st.number_input(
                "OLS fetch depth per ontology",
                min_value=1,
                value=5,
                step=1,
                help="Rows requested from OLS API per ontology before Top N output filtering.",
            )
            timeout = st.number_input(
                "OLS request timeout (seconds)",
                min_value=0.5,
                value=3.0,
                step=0.5,
                help="Network timeout per OLS API request.",
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
        ols_rows = max(int(top_n_ols), int(ols_rows))

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
        result = _run_generate_with_progress(args)
        show_command_result(result)
        if result.returncode == 0:
            st.success("Next step: move to Curate candidates to review and validate matches.")
            if st.button("Go to Curate candidates", key=f"go_curate_{left_slug}"):
                st.session_state[STATE_PAGE] = "Curate candidates"
                st.rerun()

    st.subheader("Candidates Preview")
    candidates_df = read_tsv(left_ctx.candidates_tsv)
    if candidates_df.empty and not left_ctx.candidates_tsv.is_file():
        st.info("No candidates file yet. Generate candidates first.")
        return
    st.caption(f"Rows: {len(candidates_df)}")
    render_clickable_dataframe(candidates_df.head(200), use_container_width=True, hide_index=True)
