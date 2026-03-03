"""External source download page."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import re
from pathlib import Path

import pandas as pd
import streamlit as st

from curation_app.config import DEFAULT_MANIFEST, DOWNLOADS_DIR
from curation_app.context import STATE_SOURCE_ID
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
DOWNLOAD_META_PATH = Path("registry/downloads_meta.json")


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


def _read_download_meta() -> dict[str, dict[str, str]]:
    if not DOWNLOAD_META_PATH.is_file():
        return {}
    try:
        payload = json.loads(DOWNLOAD_META_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    out: dict[str, dict[str, str]] = {}
    for key, value in payload.items():
        if isinstance(value, dict):
            out[str(key).strip().lower()] = {str(k): str(v) for k, v in value.items()}
    return out


def _write_download_meta(meta: dict[str, dict[str, str]]) -> None:
    DOWNLOAD_META_PATH.parent.mkdir(parents=True, exist_ok=True)
    DOWNLOAD_META_PATH.write_text(json.dumps(meta, indent=2), encoding="utf-8")


def _mtime_utc_text(path: Path) -> str:
    ts = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    return ts.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _last_fetched_for(source_id: str, path: Path, meta: dict[str, dict[str, str]]) -> str:
    sid = source_id.strip().lower()
    value = meta.get(sid, {}).get("last_fetched_utc", "").strip()
    if value:
        return value
    if path.is_file():
        return _mtime_utc_text(path)
    return "-"


def render() -> None:
    st.title("Fetch schemas")

    manifest_path = to_path(DEFAULT_MANIFEST)
    if STATE_MANIFEST_KEY not in st.session_state:
        manifest_df = read_tsv(manifest_path)
        st.session_state[STATE_MANIFEST_KEY] = _manifest_for_editor(manifest_df)
    st.caption(f"Manifest: `{to_relpath(manifest_path)}`")
    st.caption("Download path is auto-generated as `registry/downloads/<source_id>.ttl`.")

    cleaned_manifest_df, manifest_errors = _editor_to_manifest(st.session_state[STATE_MANIFEST_KEY])
    if manifest_errors:
        st.warning("Fix manifest issues before saving or downloading:")
        for error in manifest_errors:
            st.write(f"- {error}")

    all_sources = cleaned_manifest_df["source_id"].tolist() if not cleaned_manifest_df.empty else []
    enabled_ids = (
        cleaned_manifest_df.loc[cleaned_manifest_df["enabled"] == "1", "source_id"].tolist()
        if not cleaned_manifest_df.empty
        else []
    )

    download_meta = _read_download_meta()

    with st.expander("1) Is your schema already loaded?", expanded=True):
        loaded_choice = st.radio(
            "Already loaded?",
            options=["Yes", "No"],
            horizontal=True,
            help="If yes, select your schema in the left sidebar and continue to Extract terms.",
        )
        active_slug = str(st.session_state.get(STATE_SOURCE_ID, "") or "").strip().lower()
        if loaded_choice == "Yes":
            if active_slug:
                st.success(
                    f"Great. Current loaded schema source is `{active_slug}`. "
                    "Go to **Extract terms** next."
                )
            else:
                st.info("Select your source in the left sidebar, then go to **Extract terms**.")
        else:
            st.info("Download your schema below, then select it in the sidebar and continue.")

        if enabled_ids:
            status_rows: list[dict[str, str]] = []
            for source_id in enabled_ids:
                path = _download_path_for(source_id)
                status_rows.append(
                    {
                        "source_id": source_id,
                        "loaded": "yes" if path.is_file() else "no",
                        "last_fetched_utc": _last_fetched_for(source_id, path, download_meta),
                        "size_bytes": f"{path.stat().st_size:,}" if path.is_file() else "-",
                    }
                )
            st.dataframe(pd.DataFrame(status_rows), use_container_width=True, hide_index=True)

    with st.expander("2) Manage schema registry", expanded=False):
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

    with st.expander("3) Download schemas", expanded=False):
        default_sources = (
            enabled_ids[: min(3, len(enabled_ids))]
            if enabled_ids
            else all_sources[: min(3, len(all_sources))]
        )

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

            if result.returncode == 0 and not dry_run:
                impacted = enabled_ids if download_all else selected_source_ids
                refreshed_meta = _read_download_meta()
                for source_id in impacted:
                    path = _download_path_for(source_id)
                    if path.is_file():
                        refreshed_meta.setdefault(source_id, {})
                        refreshed_meta[source_id]["last_fetched_utc"] = _mtime_utc_text(path)
                _write_download_meta(refreshed_meta)
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

    if not cleaned_manifest_df.empty:
        st.subheader("Downloaded Files")
        inspect_df = cleaned_manifest_df[
            cleaned_manifest_df["source_id"].isin(enabled_ids)
        ] if enabled_ids else cleaned_manifest_df

        for _, row in inspect_df.iterrows():
            source_id = str(row.get("source_id", "")).strip()
            download_path = _download_path_for(source_id)
            if not download_path.is_file():
                st.write(
                    f"- {source_id}: `{to_relpath(download_path)}` (missing, last fetched: "
                    f"{_last_fetched_for(source_id, download_path, download_meta)})"
                )
                continue
            size = download_path.stat().st_size
            last_fetched = _last_fetched_for(source_id, download_path, download_meta)
            st.write(
                f"- {source_id}: `{to_relpath(download_path)}` ({size:,} bytes, last fetched: {last_fetched})"
            )
            render_file_download(
                download_path,
                label=f"Download {Path(download_path).name}",
                key=f"download_source_{source_id}",
            )
