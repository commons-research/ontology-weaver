"""Automatic background SQLite sync for curated candidate files."""

from __future__ import annotations

import csv
from pathlib import Path
import tempfile

import pandas as pd
import streamlit as st

from curation_app.config import (
    DEFAULT_SQLITE_DB,
    REGISTRY_DIR,
)
from curation_app.context import source_context
from curation_app.helpers import read_tsv, run_python_script, to_relpath

STATE_SYNC_FINGERPRINT = "auto_sync_fingerprint"
STATE_SYNC_LAST_ERROR = "auto_sync_last_error"


def _file_signature(path: Path) -> str:
    if not path.is_file():
        return f"{to_relpath(path)}:missing"
    stat = path.stat()
    return f"{to_relpath(path)}:{stat.st_mtime_ns}:{stat.st_size}"


def _row_records_for_source(source_id: str, df: pd.DataFrame) -> list[dict[str, str]]:
    if df.empty:
        return []
    records: list[dict[str, str]] = []
    for row in df.to_dict(orient="records"):
        out = {str(k): str(v or "") for k, v in row.items()}
        alignment_id = str(out.get("alignment_id", "")).strip()
        if alignment_id:
            out["alignment_id"] = f"{source_id.upper()}__{alignment_id}"
        else:
            out["alignment_id"] = f"{source_id.upper()}__AUTO_{len(records)+1:06d}"
        records.append(out)
    return records


def auto_sync_sqlite(manifest_df: pd.DataFrame) -> tuple[bool, str]:
    """Sync all enabled-source candidate files to SQLite + reconciled exports.

    Returns (ok, message). Sync is skipped when file signatures are unchanged.
    """
    if manifest_df.empty or "source_id" not in manifest_df.columns:
        return True, "No manifest sources found; auto-sync skipped."

    enabled_mask = manifest_df.get("enabled", pd.Series([""] * len(manifest_df), index=manifest_df.index))
    enabled_values = enabled_mask.astype(str).str.lower()
    enabled = manifest_df.loc[enabled_values.isin(["1", "true", "yes", "y", "on"]), "source_id"].tolist()
    all_sources = [str(x).strip().lower() for x in (enabled or manifest_df["source_id"].tolist()) if str(x).strip()]
    if not all_sources:
        return True, "No active sources; auto-sync skipped."

    candidate_paths: list[Path] = []
    signatures: list[str] = []
    for src in all_sources:
        ctx = source_context(src, manifest_df)
        candidate_paths.append(ctx.candidates_tsv)
        signatures.append(_file_signature(ctx.candidates_tsv))

    fingerprint = "|".join(sorted(signatures))
    if st.session_state.get(STATE_SYNC_FINGERPRINT) == fingerprint:
        return True, "Auto-sync up to date."

    merged_rows: list[dict[str, str]] = []
    all_columns: set[str] = set()
    for src, path in zip(all_sources, candidate_paths):
        df = read_tsv(path)
        if df.empty:
            continue
        source_rows = _row_records_for_source(src, df)
        merged_rows.extend(source_rows)
        for row in source_rows:
            all_columns.update(row.keys())

    if not merged_rows:
        st.session_state[STATE_SYNC_FINGERPRINT] = fingerprint
        st.session_state[STATE_SYNC_LAST_ERROR] = ""
        return True, "No candidate rows found; auto-sync skipped."

    fieldnames = sorted(all_columns)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="",
        suffix="_all_candidates.tsv",
        delete=False,
    ) as handle:
        merged_path = Path(handle.name)
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows(merged_rows)

    reconciled_output = REGISTRY_DIR / "reconciled_mappings.tsv"
    grouped_output = REGISTRY_DIR / "reconciled_canonical_groups.tsv"
    args = [
        "--db",
        to_relpath(DEFAULT_SQLITE_DB),
        "--pair-candidates",
        str(merged_path),
        "--pair-alignments",
        str(merged_path),
        "--status",
        "approved",
        "--reconciled-output",
        to_relpath(reconciled_output),
        "--grouped-output",
        to_relpath(grouped_output),
    ]
    result = run_python_script("scripts/sync_alignment_sqlite.py", args)
    try:
        merged_path.unlink(missing_ok=True)
    except OSError:
        pass

    if result.returncode != 0:
        st.session_state[STATE_SYNC_LAST_ERROR] = (result.stderr or result.stdout or "").strip()
        return False, "Background DB sync failed."

    st.session_state[STATE_SYNC_FINGERPRINT] = fingerprint
    st.session_state[STATE_SYNC_LAST_ERROR] = ""
    return True, "Background DB sync updated."
