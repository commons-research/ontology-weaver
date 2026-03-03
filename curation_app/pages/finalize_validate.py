"""Review curated dataset and export updated source TTL."""

from __future__ import annotations

from pathlib import Path
import re

import pandas as pd
import streamlit as st

from curation_app.context import active_source_context
from curation_app.helpers import (
    file_to_bytes,
    read_tsv,
    render_clickable_dataframe,
    to_relpath,
)

VIEW_OPTIONS = [
    "All rows",
    "Reviewed rows (not needs_review)",
    "Automatically matched and manually validated",
    "Original terms kept",
    "Manually added terms",
]


def _safe_text(value: object) -> str:
    return str(value or "").strip()


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
        "canonical_from",
        "canonical_term_iri",
        "match_method",
        "suggestion_source",
        "notes",
    ]:
        if col not in out.columns:
            out[col] = ""
    return out


def _canonical_target_iri(row: pd.Series) -> str:
    canonical = _safe_text(row.get("canonical_term_iri"))
    if canonical:
        return canonical
    # Backward compatibility: approved right-side rows may still rely on right_term_iri.
    if _safe_text(row.get("status")) == "approved":
        if _safe_text(row.get("canonical_from")) in {"right", "manual", ""}:
            return _safe_text(row.get("right_term_iri"))
    return ""


def _build_replacements(df: pd.DataFrame) -> tuple[dict[str, str], list[str]]:
    approved = df[df["status"] == "approved"].copy()
    by_left: dict[str, set[str]] = {}
    for _, row in approved.iterrows():
        left_iri = _safe_text(row.get("left_term_iri"))
        target_iri = _canonical_target_iri(row)
        if not left_iri or not target_iri or left_iri == target_iri:
            continue
        by_left.setdefault(left_iri, set()).add(target_iri)

    warnings: list[str] = []
    replacements: dict[str, str] = {}
    for left_iri, targets in by_left.items():
        sorted_targets = sorted(targets)
        replacements[left_iri] = sorted_targets[0]
        if len(sorted_targets) > 1:
            warnings.append(
                f"Conflicting approved targets for `{left_iri}`: {', '.join(sorted_targets)}. "
                f"Using `{sorted_targets[0]}`."
            )
    return replacements, warnings


def _apply_iri_replacements(ttl_text: str, replacements: dict[str, str]) -> str:
    updated = ttl_text
    # Longest first avoids partial overlaps when IRIs share prefixes.
    for left_iri in sorted(replacements.keys(), key=len, reverse=True):
        right_iri = replacements[left_iri]
        updated = updated.replace(f"<{left_iri}>", f"<{right_iri}>")
    return updated


def _parse_ttl_prefixes(ttl_text: str) -> dict[str, str]:
    prefixes: dict[str, str] = {}
    in_header = True
    for line in ttl_text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if in_header and stripped.startswith("#"):
            continue
        match = re.match(r"@prefix\s+([A-Za-z][\w\-]*)?:\s*<([^>]+)>\s*\.", stripped)
        if match:
            prefix = (match.group(1) or "").strip()
            ns = match.group(2).strip()
            prefixes[prefix] = ns
            continue
        match = re.match(r"PREFIX\s+([A-Za-z][\w\-]*)?:\s*<([^>]+)>", stripped, flags=re.IGNORECASE)
        if match:
            prefix = (match.group(1) or "").strip()
            ns = match.group(2).strip()
            prefixes[prefix] = ns
            continue
        # Header may include @base; keep reading until first non-directive statement.
        if re.match(r"@base\s+<[^>]+>\s*\.", stripped, flags=re.IGNORECASE):
            continue
        if in_header:
            in_header = False
            break
    return prefixes


def _build_qname_candidates(iri: str, prefixes: dict[str, str]) -> list[str]:
    out: list[str] = []
    for prefix, ns in prefixes.items():
        if iri.startswith(ns):
            local = iri[len(ns):]
            if local:
                out.append(f"{prefix}:{local}" if prefix else f":{local}")
    return sorted(set(out), key=len, reverse=True)


def _replace_qname_token(text: str, qname: str, replacement_iri: str) -> str:
    # Replace QName only when it appears as a standalone RDF term token.
    escaped = re.escape(qname)
    pattern = re.compile(rf"(?<![\w:/#.-]){escaped}(?![\w:/#.-])")
    return pattern.sub(f"<{replacement_iri}>", text)


def _apply_iri_and_qname_replacements(ttl_text: str, replacements: dict[str, str]) -> str:
    updated = _apply_iri_replacements(ttl_text, replacements)
    prefixes = _parse_ttl_prefixes(ttl_text)
    for left_iri in sorted(replacements.keys(), key=len, reverse=True):
        right_iri = replacements[left_iri]
        for qname in _build_qname_candidates(left_iri, prefixes):
            updated = _replace_qname_token(updated, qname, right_iri)
    return updated


def _split_namespace_local(iri: str) -> tuple[str, str]:
    if "#" in iri:
        ns, local = iri.rsplit("#", 1)
        return ns + "#", local
    if "/" in iri:
        ns, local = iri.rsplit("/", 1)
        return ns + "/", local
    return "", ""


def _sanitize_prefix_name(value: str) -> str:
    name = re.sub(r"[^A-Za-z0-9_]", "", (value or "").strip().lower())
    if not name:
        return "ext"
    if not re.match(r"^[A-Za-z_]", name):
        return f"ext_{name}"
    return name


def _safe_qname_local(local: str) -> bool:
    return bool(re.match(r"^[A-Za-z_][A-Za-z0-9_.-]*$", local or ""))


def _insert_prefixes_in_header(ttl_text: str, new_prefixes: dict[str, str]) -> str:
    if not new_prefixes:
        return ttl_text
    lines = ttl_text.splitlines()
    insert_at = 0
    for i, line in enumerate(lines):
        stripped = line.strip()
        if re.match(r"@prefix\s+.*\.\s*$", stripped, flags=re.IGNORECASE):
            insert_at = i + 1
            continue
        if re.match(r"@base\s+<[^>]+>\s*\.\s*$", stripped, flags=re.IGNORECASE):
            insert_at = i + 1
            continue
        if stripped == "":
            continue
        break
    prefix_lines = [f"@prefix {p}: <{ns}> ." for p, ns in sorted(new_prefixes.items())]
    out = lines[:insert_at] + prefix_lines + lines[insert_at:]
    return "\n".join(out) + ("\n" if ttl_text.endswith("\n") else "")


def _compact_replacement_iris_with_prefixes(
    ttl_text: str,
    export_df: pd.DataFrame,
    replacements: dict[str, str],
) -> tuple[str, dict[str, str]]:
    if not replacements:
        return ttl_text, {}
    existing_prefixes = _parse_ttl_prefixes(ttl_text)
    iri_to_source: dict[str, str] = {}
    for _, row in export_df.iterrows():
        target = _canonical_target_iri(row)
        if not target:
            continue
        source = _safe_text(row.get("canonical_term_source")) or _safe_text(row.get("right_source"))
        if source and target not in iri_to_source:
            iri_to_source[target] = source

    iri_to_qname: dict[str, str] = {}
    new_prefixes: dict[str, str] = {}
    used_prefixes = set(existing_prefixes.keys())

    unique_targets = sorted(set(replacements.values()), key=len, reverse=True)
    for iri in unique_targets:
        ns, local = _split_namespace_local(iri)
        if not ns or not local or not _safe_qname_local(local):
            continue
        preferred = _sanitize_prefix_name(iri_to_source.get(iri, "ext"))
        prefix = preferred
        counter = 2
        while True:
            existing_ns = existing_prefixes.get(prefix) or new_prefixes.get(prefix)
            if existing_ns is None:
                break
            if existing_ns == ns:
                break
            prefix = f"{preferred}{counter}"
            counter += 1
        if prefix not in existing_prefixes and prefix not in new_prefixes:
            new_prefixes[prefix] = ns
        used_prefixes.add(prefix)
        iri_to_qname[iri] = f"{prefix}:{local}"

    updated = ttl_text
    for iri in unique_targets:
        qname = iri_to_qname.get(iri)
        if not qname:
            continue
        updated = updated.replace(f"<{iri}>", qname)

    updated = _insert_prefixes_in_header(updated, new_prefixes)
    return updated, new_prefixes


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


def render() -> None:
    st.title("Review and Export")

    ctx = active_source_context()
    if ctx is None:
        st.warning("No source slug available. Configure Download External Sources first.")
        return

    candidates_path = ctx.candidates_tsv
    df = _ensure_columns(read_tsv(candidates_path))
    if df.empty and not candidates_path.is_file():
        st.warning("No curated candidate file found yet. Please run Generate and Curate first.")
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

    st.subheader("Export Updated Source TTL")
    status_options = sorted(df["status"].dropna().unique().tolist())
    default_statuses = ["approved"] if "approved" in status_options else status_options
    export_statuses = st.multiselect(
        "Statuses to export",
        options=status_options,
        default=default_statuses,
        help="Usually keep only approved rows for replacements.",
    )
    export_df = shown[shown["status"].isin(export_statuses)] if export_statuses else shown.iloc[0:0]
    source_ttl_path = ctx.download_ttl
    if not source_ttl_path.is_file():
        st.warning(f"Source TTL not found: `{to_relpath(source_ttl_path)}`")
        return
    source_ttl_text = source_ttl_path.read_text(encoding="utf-8", errors="replace")
    replacements, replacement_warnings = _build_replacements(export_df)
    ttl_text = _apply_iri_and_qname_replacements(source_ttl_text, replacements)
    ttl_text, added_prefixes = _compact_replacement_iris_with_prefixes(ttl_text, export_df, replacements)
    for msg in replacement_warnings:
        st.warning(msg)
    st.caption(
        f"Applied {len(replacements)} IRI replacement(s) on `{to_relpath(source_ttl_path)}` "
        f"from {len(export_df)} filtered row(s)."
    )
    if added_prefixes:
        st.caption(
            "Added external prefixes: "
            + ", ".join(f"`{p}: <{ns}>`" for p, ns in sorted(added_prefixes.items()))
        )
    st.code(ttl_text[:12000], language="turtle")

    export_dir = Path("registry/exports")
    export_dir.mkdir(parents=True, exist_ok=True)
    ttl_path = export_dir / f"{ctx.source_id}_updated.ttl"

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
