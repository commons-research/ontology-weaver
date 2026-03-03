"""TTL extraction page."""

from __future__ import annotations

import re
from pathlib import Path

import streamlit as st

from curation_app.context import (
    STATE_SOURCE_ID,
    enabled_source_ids,
    load_manifest,
    source_context,
    source_ids,
)
from curation_app.helpers import (
    render_table_preview,
    run_python_script,
    show_command_result,
    to_relpath,
)

TTL_PREFIX_PATTERNS = (
    re.compile(r"^\s*@prefix\s+[A-Za-z][\w-]*:\s*<([^>]+)>\s*\.\s*$"),
    re.compile(r"^\s*PREFIX\s+[A-Za-z][\w-]*:\s*<([^>]+)>\s*$", re.IGNORECASE),
)


def _ttl_namespace_suggestions(ttl_path: Path) -> list[str]:
    if not ttl_path.is_file():
        return []

    seen: set[str] = set()
    suggestions: list[str] = []
    with ttl_path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            for pattern in TTL_PREFIX_PATTERNS:
                match = pattern.match(line)
                if not match:
                    continue
                namespace = match.group(1).strip()
                if not namespace or namespace in seen:
                    continue
                seen.add(namespace)
                suggestions.append(namespace)
    return suggestions


def render() -> None:
    st.title("Extract Terms From TTL")

    manifest_df = load_manifest()
    options = enabled_source_ids(manifest_df) or source_ids(manifest_df)
    if not options:
        st.warning("No source slug found in manifest.")
        return

    current_source = str(st.session_state.get(STATE_SOURCE_ID, options[0])).strip().lower()
    if current_source not in options:
        current_source = options[0]
    selected_source = st.selectbox("Source slug", options=options, index=options.index(current_source))
    st.session_state[STATE_SOURCE_ID] = selected_source

    ctx = source_context(selected_source, manifest_df)
    namespace_suggestions = _ttl_namespace_suggestions(ctx.download_ttl)
    if namespace_suggestions:
        default_prefix = ctx.namespace_prefix if ctx.namespace_prefix in namespace_suggestions else namespace_suggestions[0]
        namespace_prefix = st.selectbox(
            "Namespace prefix",
            options=namespace_suggestions,
            index=namespace_suggestions.index(default_prefix),
            help="Detected from the TTL file prefix declarations.",
        )
    else:
        namespace_prefix = st.text_input("Namespace prefix", value=ctx.namespace_prefix)

    if not ctx.download_ttl.is_file():
        st.warning(f"TTL file is missing for this source: `{to_relpath(ctx.download_ttl)}`")

    if st.button("Extract terms", type="primary"):
        if not namespace_prefix.strip():
            st.error("Namespace prefix is required.")
            return

        args = [to_relpath(ctx.download_ttl), namespace_prefix.strip(), to_relpath(ctx.terms_tsv)]
        result = run_python_script("scripts/extract_ttl_terms.py", args)
        show_command_result(result)

    st.subheader("Extracted Terms Preview")
    render_table_preview(ctx.terms_tsv, key=f"extract_terms_preview_{selected_source}")
