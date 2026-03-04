"""OLS ontology catalog fetch and preview page."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import tempfile

import pandas as pd
from rdflib import Graph, URIRef
from rdflib.namespace import OWL, RDF
import streamlit as st

from curation_app.config import DEFAULT_OLS_ONTOLOGIES_FILE
from curation_app.helpers import (
    file_to_bytes,
    list_files,
    read_tsv,
    render_clickable_dataframe,
    run_python_script,
    to_path,
    to_relpath,
)

META_PATH = Path("registry/ols_ontologies_meta.json")
DEFAULT_DOWNLOAD_OUTPUT_DIR = "registry/downloads/ontologies"
DEFAULT_DOWNLOAD_TIMEOUT_SECONDS = 20.0


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


def _guess_rdf_format(path: Path) -> str | None:
    suffix = path.suffix.lower()
    if suffix in {".ttl", ".turtle"}:
        return "turtle"
    if suffix in {".rdf", ".owl", ".xml"}:
        return "xml"
    if suffix in {".json", ".jsonld"}:
        return "json-ld"
    if suffix in {".nt"}:
        return "nt"
    if suffix in {".nq"}:
        return "nquads"
    if suffix in {".trig"}:
        return "trig"
    return None


@st.cache_data(show_spinner=False)
def _extract_ontology_version(path_str: str, mtime: float) -> dict[str, str]:
    del mtime
    path = Path(path_str)
    graph = Graph()
    guessed = _guess_rdf_format(path)
    try:
        if guessed:
            graph.parse(path, format=guessed)
        else:
            graph.parse(path)
    except Exception as err:
        return {
            "ontology_iri_file": "",
            "version_iri_file": "",
            "version_info_file": "",
            "parse_error": str(err),
        }

    ontology_subjects = {
        subject for subject in graph.subjects(RDF.type, OWL.Ontology) if isinstance(subject, URIRef)
    }
    if not ontology_subjects:
        ontology_subjects = {
            subject for subject in graph.subjects(OWL.versionIRI, None) if isinstance(subject, URIRef)
        }
    if not ontology_subjects:
        ontology_subjects = {
            subject for subject in graph.subjects(OWL.versionInfo, None) if isinstance(subject, URIRef)
        }

    ontology_iri = ""
    version_iri = ""
    version_info = ""
    if ontology_subjects:
        subject = sorted(str(item) for item in ontology_subjects)[0]
        ontology_iri = subject
        subject_ref = URIRef(subject)
        version_iri = next((str(obj) for obj in graph.objects(subject_ref, OWL.versionIRI)), "")
        version_info = next((str(obj) for obj in graph.objects(subject_ref, OWL.versionInfo)), "")

    return {
        "ontology_iri_file": ontology_iri,
        "version_iri_file": version_iri,
        "version_info_file": version_info,
        "parse_error": "",
    }


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
    st.subheader("Download Selected Ontologies")
    st.caption(
        "Select ontology IDs from the fetched OLS catalog and download their RDF files locally "
        "(for example: `owl`, `rdfs`, `skos`)."
    )
    if catalog_df.empty:
        st.info("Fetch the OLS catalog first to enable ontology downloads.")
    else:
        catalog_df = catalog_df.copy()
        ontology_ids = [str(value).strip().lower() for value in catalog_df["ontology"].tolist() if str(value).strip()]
        label_by_id = {
            str(row.get("ontology", "")).strip().lower(): str(row.get("label", "")).strip()
            for _, row in catalog_df.iterrows()
        }
        suggested_defaults = [ont for ont in ("owl", "rdfs", "skos") if ont in ontology_ids]

        selected_ontologies = st.multiselect(
            "Ontology IDs (searchable)",
            options=ontology_ids,
            default=suggested_defaults,
            format_func=lambda ont: f"{ont} — {label_by_id.get(ont, '')}".rstrip(" — "),
            key="ols_download_selected_ids",
        )

        if st.button(
            "Download selected ontologies",
            type="primary",
            disabled=not selected_ontologies,
            key="ols_download_button",
        ):
            args = [
                "--catalog",
                to_relpath(DEFAULT_OLS_ONTOLOGIES_FILE),
                "--output-dir",
                DEFAULT_DOWNLOAD_OUTPUT_DIR,
                "--timeout",
                str(float(DEFAULT_DOWNLOAD_TIMEOUT_SECONDS)),
            ]
            for ontology in selected_ontologies:
                args.extend(["--ontology", ontology])
            result = run_python_script("scripts/download_ols_ontologies.py", args)
            if result.returncode != 0:
                msg = (result.stderr or result.stdout or "Unknown download error").strip()
                st.error(f"Ontology download failed: {msg}")
            else:
                st.success((result.stdout or "Ontology downloads completed.").strip())

        output_dir_path = to_path(DEFAULT_DOWNLOAD_OUTPUT_DIR)
        if output_dir_path.is_dir():
            downloaded = list_files(output_dir_path, "*")
            if downloaded:
                st.caption(f"Downloaded files in `{to_relpath(output_dir_path)}`")
                preview_files = downloaded[:300]
                catalog_lookup = {
                    str(row.get("ontology", "")).strip().lower(): {
                        "catalog_version_iri": str(row.get("version_iri", "")).strip(),
                        "catalog_last_loaded": str(row.get("last_loaded", "")).strip(),
                        "catalog_ontology_iri": str(row.get("ontology_iri", "")).strip(),
                    }
                    for _, row in catalog_df.iterrows()
                }
                rows: list[dict[str, str | int]] = []
                for path in preview_files:
                    ontology_id = path.stem.strip().lower()
                    file_info = _extract_ontology_version(str(path), path.stat().st_mtime)
                    catalog_info = catalog_lookup.get(
                        ontology_id,
                        {
                            "catalog_version_iri": "",
                            "catalog_last_loaded": "",
                            "catalog_ontology_iri": "",
                        },
                    )
                    rows.append(
                        {
                            "file": path.name,
                            "ontology_id": ontology_id,
                            "size_bytes": path.stat().st_size,
                            "ontology_iri_file": file_info["ontology_iri_file"],
                            "version_iri_file": file_info["version_iri_file"],
                            "version_info_file": file_info["version_info_file"],
                            "catalog_version_iri": catalog_info["catalog_version_iri"],
                            "catalog_last_loaded": catalog_info["catalog_last_loaded"],
                            "catalog_ontology_iri": catalog_info["catalog_ontology_iri"],
                            "parse_error": file_info["parse_error"],
                        }
                    )
                versions_df = pd.DataFrame(rows)
                render_clickable_dataframe(versions_df, use_container_width=True, hide_index=True)

    st.subheader("Catalog Preview")
    if catalog_df.empty and not DEFAULT_OLS_ONTOLOGIES_FILE.is_file():
        st.info("No local OLS catalog file yet. Click 'Check for updates'.")
        return
    st.caption(f"Rows: {len(catalog_df)}")
    render_clickable_dataframe(catalog_df.head(500), use_container_width=True, hide_index=True)
