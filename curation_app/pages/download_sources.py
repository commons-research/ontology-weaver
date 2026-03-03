"""Step 0: external source download page."""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import streamlit as st

from curation_app.config import DEFAULT_MANIFEST, DOWNLOADS_DIR
from curation_app.helpers import (
    read_tsv,
    render_file_download,
    run_python_script,
    show_command_result,
    to_path,
    to_relpath,
    write_tsv,
)

MANIFEST_COLUMNS = ["source_id", "url", "enabled", "description"]
SLUG_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
STATE_MANIFEST_KEY = "download_sources_manifest_df"
STATE_MANIFEST_PATH = "download_sources_manifest_path"


def _enabled_to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _manifest_for_editor(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=MANIFEST_COLUMNS)

    normalized = df.copy()
    for col in MANIFEST_COLUMNS:
        if col not in normalized.columns:
            normalized[col] = ""
    normalized = normalized[MANIFEST_COLUMNS].fillna("")
    normalized["source_id"] = normalized["source_id"].astype(str).str.strip().str.lower()
    normalized["url"] = normalized["url"].astype(str).str.strip()
    normalized["description"] = normalized["description"].astype(str).str.strip()
    normalized["enabled"] = normalized["enabled"].apply(_enabled_to_bool)
    return normalized


def _editor_to_manifest(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    cleaned_rows: list[dict[str, str]] = []
    errors: list[str] = []
    seen: set[str] = set()

    for i, row in df.iterrows():
        row_num = i + 1
        source_id = str(row.get("source_id", "") or "").strip().lower()
        url = str(row.get("url", "") or "").strip()
        description = str(row.get("description", "") or "").strip()
        enabled = _enabled_to_bool(row.get("enabled", False))

        if not source_id and not url and not description:
            continue
        if not source_id:
            errors.append(f"Row {row_num}: source_id is required.")
            continue
        if not SLUG_PATTERN.match(source_id):
            errors.append(
                f"Row {row_num}: source_id '{source_id}' must match `{SLUG_PATTERN.pattern}`."
            )
            continue
        if source_id in seen:
            errors.append(f"Row {row_num}: duplicate source_id '{source_id}'.")
            continue
        if not url:
            errors.append(f"Row {row_num}: url is required for source_id '{source_id}'.")
            continue

        seen.add(source_id)
        cleaned_rows.append(
            {
                "source_id": source_id,
                "url": url,
                "enabled": "1" if enabled else "0",
                "description": description,
            }
        )

    return pd.DataFrame(cleaned_rows, columns=MANIFEST_COLUMNS), errors


def _download_path_for(source_id: str) -> Path:
    return DOWNLOADS_DIR / f"{source_id}.ttl"


def render() -> None:
    st.title("Step 0: Download External Sources")

    manifest_input = st.text_input("Manifest TSV", value=to_relpath(DEFAULT_MANIFEST))
    manifest_path = to_path(manifest_input)
    if st.session_state.get(STATE_MANIFEST_PATH) != str(manifest_path):
        manifest_df = read_tsv(manifest_path)
        st.session_state[STATE_MANIFEST_KEY] = _manifest_for_editor(manifest_df)
        st.session_state[STATE_MANIFEST_PATH] = str(manifest_path)

    if STATE_MANIFEST_KEY not in st.session_state:
        st.session_state[STATE_MANIFEST_KEY] = pd.DataFrame(columns=MANIFEST_COLUMNS)

    st.caption("Download path is auto-generated as `registry/downloads/<source_id>.ttl`.")
    st.subheader("Manifest")
    edited_df = st.data_editor(
        st.session_state[STATE_MANIFEST_KEY],
        num_rows="dynamic",
        use_container_width=True,
        hide_index=True,
        column_config={
            "source_id": st.column_config.TextColumn("source_id", help="Slug (lowercase recommended)."),
            "url": st.column_config.TextColumn("url", help="Remote ontology URL."),
            "enabled": st.column_config.CheckboxColumn("enabled", default=True),
            "description": st.column_config.TextColumn("description"),
        },
        key="external_sources_editor",
    )
    st.session_state[STATE_MANIFEST_KEY] = edited_df

    with st.form("add_manifest_source", clear_on_submit=True):
        st.caption("Add source")
        slug = st.text_input("source_id (slug)", value="")
        url = st.text_input("url", value="")
        description = st.text_input("description", value="")
        enabled = st.checkbox("enabled", value=True)
        add_clicked = st.form_submit_button("Add row")
        if add_clicked:
            new_row = pd.DataFrame(
                [
                    {
                        "source_id": slug.strip().lower(),
                        "url": url.strip(),
                        "enabled": enabled,
                        "description": description.strip(),
                    }
                ],
                columns=MANIFEST_COLUMNS,
            )
            st.session_state[STATE_MANIFEST_KEY] = pd.concat(
                [st.session_state[STATE_MANIFEST_KEY], new_row], ignore_index=True
            )
            st.rerun()

    cleaned_manifest_df, manifest_errors = _editor_to_manifest(st.session_state[STATE_MANIFEST_KEY])
    if manifest_errors:
        st.warning("Fix manifest issues before saving or downloading:")
        for error in manifest_errors:
            st.write(f"- {error}")

    col_save, col_reload = st.columns(2)
    with col_save:
        if st.button("Save manifest", type="secondary"):
            if manifest_errors:
                st.error("Manifest not saved due to validation errors.")
            else:
                write_tsv(cleaned_manifest_df, manifest_path)
                st.success(f"Saved `{to_relpath(manifest_path)}`")
    with col_reload:
        if st.button("Reload from disk"):
            refreshed = read_tsv(manifest_path)
            st.session_state[STATE_MANIFEST_KEY] = _manifest_for_editor(refreshed)
            st.rerun()

    all_sources = cleaned_manifest_df["source_id"].tolist() if not cleaned_manifest_df.empty else []
    enabled_ids = (
        cleaned_manifest_df.loc[cleaned_manifest_df["enabled"] == "1", "source_id"].tolist()
        if not cleaned_manifest_df.empty
        else []
    )

    default_sources = enabled_ids[: min(3, len(enabled_ids))] if enabled_ids else all_sources[: min(3, len(all_sources))]

    selected_source_ids = st.multiselect(
        "Source IDs",
        options=all_sources,
        default=default_sources,
        help="Select one or more sources. Leave empty and use 'Download all enabled' to fetch all enabled rows.",
    )

    col1, col2, col3 = st.columns(3)
    with col1:
        download_all = st.checkbox("Download all enabled", value=False)
    with col2:
        force = st.checkbox("Force overwrite", value=False)
    with col3:
        dry_run = st.checkbox("Dry run", value=False)

    timeout = st.number_input("Request timeout (seconds)", min_value=1.0, value=30.0, step=1.0)

    run_clicked = st.button("Run download", type="primary")
    if run_clicked:
        if manifest_errors:
            st.error("Cannot run download until manifest errors are fixed.")
            return

        write_tsv(cleaned_manifest_df, manifest_path)
        args = ["--manifest", to_relpath(manifest_path), "--timeout", str(timeout)]
        if download_all:
            args.append("--all")
        for source_id in selected_source_ids:
            args.extend(["--source-id", source_id])
        if force:
            args.append("--force")
        if dry_run:
            args.append("--dry-run")

        result = run_python_script("scripts/download_external_sources.py", args)
        show_command_result(result)

    if not cleaned_manifest_df.empty:
        st.subheader("Downloaded Files")
        inspect_ids = selected_source_ids if selected_source_ids else enabled_ids
        inspect_df = (
            cleaned_manifest_df[cleaned_manifest_df["source_id"].isin(inspect_ids)]
            if inspect_ids
            else cleaned_manifest_df
        )

        for _, row in inspect_df.iterrows():
            source_id = str(row.get("source_id", "")).strip()
            download_path = _download_path_for(source_id)
            if not download_path.is_file():
                st.write(f"- {source_id}: `{to_relpath(download_path)}` (missing)")
                continue
            size = download_path.stat().st_size
            st.write(f"- {source_id}: `{to_relpath(download_path)}` ({size:,} bytes)")
            render_file_download(
                download_path,
                label=f"Download {Path(download_path).name}",
                key=f"download_source_{source_id}",
            )
