"""Source-driven workflow context utilities."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import streamlit as st

from curation_app.config import DEFAULT_MANIFEST, DOWNLOADS_DIR, IMPORTS_DIR, REGISTRY_DIR, WORK_DIR
from curation_app.helpers import read_tsv


SOURCE_NAMESPACE_HINTS = {
    "emi": "https://w3id.org/emi#",
}
STATE_SOURCE_ID = "active_source_id"


@dataclass(frozen=True)
class SourceContext:
    """Derived workflow paths and labels for an active source slug."""

    source_id: str
    source_label: str
    download_ttl: Path
    terms_tsv: Path
    review_tsv: Path
    queue_tsv: Path
    namespace_prefix: str


def load_manifest() -> pd.DataFrame:
    """Load external source manifest."""
    df = read_tsv(DEFAULT_MANIFEST)
    if df.empty:
        return df
    for col in ("source_id", "enabled", "url", "description"):
        if col not in df.columns:
            df[col] = ""
    return df


def source_ids(manifest_df: pd.DataFrame) -> list[str]:
    """Return manifest source ids in table order."""
    if manifest_df.empty:
        return []
    return [value.strip() for value in manifest_df["source_id"].tolist() if str(value).strip()]


def enabled_source_ids(manifest_df: pd.DataFrame) -> list[str]:
    """Return enabled source ids."""
    if manifest_df.empty:
        return []
    mask = manifest_df["enabled"].str.lower().isin(["1", "true", "yes", "y", "on"])
    return [value.strip() for value in manifest_df.loc[mask, "source_id"].tolist() if str(value).strip()]


def _fallback_ttl(source_id: str) -> Path:
    return DOWNLOADS_DIR / f"{source_id}.ttl"


def source_context(source_id: str, manifest_df: pd.DataFrame) -> SourceContext:
    """Build source-derived workflow context."""
    slug = source_id.strip().lower()
    return SourceContext(
        source_id=slug,
        source_label=slug.upper(),
        download_ttl=_fallback_ttl(slug),
        terms_tsv=IMPORTS_DIR / f"{slug}_terms.tsv",
        review_tsv=REGISTRY_DIR / f"pair_alignment_candidates_{slug}.tsv",
        queue_tsv=WORK_DIR / f"pair_alignment_candidates_{slug}.tsv",
        namespace_prefix=SOURCE_NAMESPACE_HINTS.get(slug, ""),
    )


def active_source_context() -> SourceContext | None:
    """Return active source context from Streamlit session state."""
    manifest_df = load_manifest()
    ids = enabled_source_ids(manifest_df) or source_ids(manifest_df)
    if not ids:
        return None

    selected = str(st.session_state.get(STATE_SOURCE_ID, ids[0])).strip().lower()
    if selected not in ids:
        selected = ids[0]
        st.session_state[STATE_SOURCE_ID] = selected
    return source_context(selected, manifest_df)
