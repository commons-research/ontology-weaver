"""OLS ontology catalog fetch and preview page."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import tempfile

import streamlit as st

from curation_app.config import DEFAULT_OLS_ONTOLOGIES_FILE
from curation_app.helpers import (
    file_to_bytes,
    read_tsv,
    render_clickable_dataframe,
    run_python_script,
    to_relpath,
)

META_PATH = Path("registry/ols_ontologies_meta.json")


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _catalog_digest(path: Path) -> str:
    if not path.is_file():
        return ""
    return _sha256_bytes(file_to_bytes(path))


def _read_meta() -> dict[str, str]:
    if not META_PATH.is_file():
        return {}
    try:
        return json.loads(META_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_meta(*, digest: str, rows: int) -> None:
    META_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "digest": digest,
        "rows": str(rows),
        "last_fetched_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "last_checked_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    }
    META_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _mark_checked_now(meta: dict[str, str], *, digest: str, rows: int) -> None:
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    payload = {
        "digest": digest,
        "rows": str(rows),
        "last_checked_utc": now,
        "last_fetched_utc": str(meta.get("last_fetched_utc") or ""),
    }
    META_PATH.parent.mkdir(parents=True, exist_ok=True)
    META_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _fixed_fetch_to(path: Path) -> tuple[bool, str]:
    args = [
        "--output",
        to_relpath(path),
        "--timeout",
        "10",
        "--page-size",
        "200",
        "--fetch-details",
    ]
    result = run_python_script("scripts/fetch_ols_ontologies.py", args)
    if result.returncode != 0:
        msg = (result.stderr or result.stdout or "Unknown fetch error").strip()
        return False, msg
    return True, (result.stdout or "").strip()


def render() -> None:
    st.title("OLS Ontology Catalog")
    st.write(
        "Fetch ontology IDs, short descriptions, and links (homepage/OLS page) from OLS4 "
        "for use in candidate generation."
    )
    st.caption("Catalog is cached locally in `registry/ols_ontologies.tsv`. Metadata is always fetched.")

    meta = _read_meta()
    if DEFAULT_OLS_ONTOLOGIES_FILE.is_file():
        rows = len(read_tsv(DEFAULT_OLS_ONTOLOGIES_FILE))
        fetched = str(meta.get("last_fetched_utc") or "")
        checked = str(meta.get("last_checked_utc") or "")
        if not fetched:
            fetched = datetime.fromtimestamp(
                DEFAULT_OLS_ONTOLOGIES_FILE.stat().st_mtime, tz=timezone.utc
            ).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        checked_text = checked if checked else "never"
        st.caption(f"Cached rows: {rows} | Last fetch (UTC): {fetched} | Last check (UTC): {checked_text}")
    else:
        st.info("No local OLS catalog file yet.")

    c1, c2 = st.columns(2)
    with c1:
        if st.button("Check for updates", type="primary"):
            with tempfile.NamedTemporaryFile(
                mode="w", suffix="_ols_ontologies.tsv", delete=False, dir="registry"
            ) as tmp:
                tmp_path = Path(tmp.name)
            ok, msg = _fixed_fetch_to(tmp_path)
            if not ok:
                st.error(f"Update check failed: {msg}")
                tmp_path.unlink(missing_ok=True)
            else:
                new_digest = _catalog_digest(tmp_path)
                current_digest = _catalog_digest(DEFAULT_OLS_ONTOLOGIES_FILE)
                if not DEFAULT_OLS_ONTOLOGIES_FILE.is_file() or new_digest != current_digest:
                    DEFAULT_OLS_ONTOLOGIES_FILE.parent.mkdir(parents=True, exist_ok=True)
                    DEFAULT_OLS_ONTOLOGIES_FILE.write_bytes(tmp_path.read_bytes())
                    rows = len(read_tsv(DEFAULT_OLS_ONTOLOGIES_FILE))
                    _write_meta(digest=new_digest, rows=rows)
                    st.success(f"Catalog updated ({rows} rows).")
                else:
                    rows = len(read_tsv(DEFAULT_OLS_ONTOLOGIES_FILE))
                    _mark_checked_now(meta, digest=current_digest, rows=rows)
                    st.info("No updates found in OLS catalog.")
                tmp_path.unlink(missing_ok=True)
    with c2:
        if st.button("Refresh now (force)"):
            ok, msg = _fixed_fetch_to(DEFAULT_OLS_ONTOLOGIES_FILE)
            if not ok:
                st.error(f"Refresh failed: {msg}")
            else:
                rows = len(read_tsv(DEFAULT_OLS_ONTOLOGIES_FILE))
                _write_meta(digest=_catalog_digest(DEFAULT_OLS_ONTOLOGIES_FILE), rows=rows)
                st.success(f"Catalog refreshed ({rows} rows).")

    catalog_df = read_tsv(DEFAULT_OLS_ONTOLOGIES_FILE)
    st.subheader("Catalog Preview")
    if catalog_df.empty and not DEFAULT_OLS_ONTOLOGIES_FILE.is_file():
        st.info("No local OLS catalog file yet. Click 'Check for updates'.")
        return
    st.caption(f"Rows: {len(catalog_df)}")
    render_clickable_dataframe(catalog_df.head(500), use_container_width=True, hide_index=True)
