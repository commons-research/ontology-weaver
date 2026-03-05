"""Interactive candidate curation page."""

from __future__ import annotations

import html
import json
import re
import urllib.request
from difflib import SequenceMatcher
from urllib.parse import quote, quote_plus, urlencode, urlparse

import pandas as pd
import streamlit as st
from rdflib import Graph, URIRef
from rdflib.namespace import OWL, RDF, RDFS, SKOS

from curation_app.context import active_source_context
from curation_app.helpers import (
    dataframe_to_tsv_bytes,
    normalize_notes_for_approval,
    read_tsv,
    render_clickable_dataframe,
    to_path,
    to_relpath,
    utc_now_timestamp,
    write_tsv,
)


REQUIRED_COLUMNS = [
    "alignment_id",
    "left_source",
    "left_term_iri",
    "left_label",
    "left_definition",
    "left_comment",
    "left_example",
    "left_term_kind",
    "right_source",
    "right_term_iri",
    "right_label",
    "right_definition",
    "right_comment",
    "right_example",
    "right_term_kind",
    "relation",
    "status",
    "canonical_from",
    "canonical_term_iri",
    "canonical_term_label",
    "canonical_term_source",
    "reviewer",
    "date_reviewed",
    "logs",
    "curation_comment",
    "ols_search_url",
    "bioportal_search_url",
    "suggestion_source",
]

RELATIONS = ["exact", "close", "broad", "narrow", "related"]
MAPPING_RELATIONS_DIR = to_path("registry/downloads/ontologies")
MAPPING_RELATION_ONTOLOGY_IDS = ("owl", "rdfs", "skos")
MAPPING_RELATION_PLACEHOLDER = "(select mapping relation)"
STATUSES = ["needs_review", "approved", "rejected", "deprecated"]
CANONICAL_FROM = ["", "left", "right", "manual"]
KIND_MISMATCH_SCORE_FACTOR = 0.75

MAPPING_GUIDANCE: dict[str, dict[str, str]] = {
    "owl:equivalentClass": {
        "tier": "recommended",
        "when": "Use when both terms denote the same class meaning across ontologies.",
        "example": "ex:Automobile owl:equivalentClass ex:Car",
    },
    "owl:equivalentProperty": {
        "tier": "recommended",
        "when": "Use when both terms are properties with the same intended semantics.",
        "example": "ex:birthDate owl:equivalentProperty schema:birthDate",
    },
    "rdfs:subClassOf": {
        "tier": "recommended",
        "when": "Use when source class is narrower than target class.",
        "example": "ex:GraduateStudent rdfs:subClassOf ex:Student",
    },
    "rdfs:subPropertyOf": {
        "tier": "recommended",
        "when": "Use when source property is a specialization of target property.",
        "example": "ex:hasBiologicalMother rdfs:subPropertyOf ex:hasParent",
    },
    "skos:exactMatch": {
        "tier": "recommended",
        "when": "Use for near-equivalent cross-scheme concepts when you avoid OWL commitment.",
        "example": "ex:ConceptA skos:exactMatch ex:ConceptB",
    },
    "skos:closeMatch": {
        "tier": "recommended",
        "when": "Use when concepts are very close but not strictly identical in all contexts.",
        "example": "ex:ConceptA skos:closeMatch ex:ConceptB",
    },
    "skos:broadMatch": {
        "tier": "recommended",
        "when": "Use when source concept is broader than target concept.",
        "example": "ex:ConceptA skos:broadMatch ex:ConceptB",
    },
    "skos:narrowMatch": {
        "tier": "recommended",
        "when": "Use when source concept is narrower than target concept.",
        "example": "ex:ConceptA skos:narrowMatch ex:ConceptB",
    },
    "skos:relatedMatch": {
        "tier": "recommended",
        "when": "Use for associative cross-scheme relation without hierarchy or equivalence.",
        "example": "ex:ConceptA skos:relatedMatch ex:ConceptC",
    },
    "skos:mappingRelation": {
        "tier": "advanced",
        "when": "Generic parent mapping relation; prefer a specific SKOS mapping relation above.",
        "example": "ex:ConceptA skos:mappingRelation ex:ConceptB",
    },
    "owl:disjointUnionOf": {
        "tier": "advanced",
        "when": "Modeling axiom for class decomposition, not a routine alignment mapping.",
        "example": "ex:Vehicle owl:disjointUnionOf (ex:Car ex:Bus ex:Bicycle)",
    },
}

MAPPING_GUIDANCE_ORDER = [
    "owl:equivalentClass",
    "owl:equivalentProperty",
    "rdfs:subClassOf",
    "rdfs:subPropertyOf",
    "skos:exactMatch",
    "skos:closeMatch",
    "skos:broadMatch",
    "skos:narrowMatch",
    "skos:relatedMatch",
    "skos:mappingRelation",
    "owl:disjointUnionOf",
]

STATE_PATH = "curation_path"
STATE_DF = "curation_df"
STATE_DIRTY = "curation_dirty"
STATE_MTIME = "curation_mtime"
STATE_SELECTED_ALIGNMENT = "curation_selected_alignment_id"
STATE_KEPT_LEFT_TERMS = "curation_kept_left_terms"
STATE_LEFT_TERM_INDEX = "curation_left_term_index"
STATE_CURATOR = "active_curator"


def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "logs" not in out.columns:
        if "notes" in out.columns:
            out["logs"] = out["notes"].fillna("")
        else:
            out["logs"] = ""
    if "curation_comment" not in out.columns:
        out["curation_comment"] = ""
    for col in REQUIRED_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    return out


def _load_df(path_text: str) -> pd.DataFrame:
    df = read_tsv(path_text)
    return _ensure_columns(df)


def _file_mtime(path_text: str) -> float | None:
    resolved = to_path(path_text)
    if not resolved.is_file():
        return None
    return resolved.stat().st_mtime


def _autosave_if_dirty(candidate_file: str) -> None:
    if not st.session_state.get(STATE_DIRTY):
        return
    write_tsv(st.session_state[STATE_DF], candidate_file)
    st.session_state[STATE_DIRTY] = False
    st.session_state[STATE_MTIME] = _file_mtime(candidate_file)
    st.caption("Auto-saved candidate file.")


def _set_review_fields(df: pd.DataFrame, idx: int, reviewer: str) -> None:
    if reviewer.strip():
        df.at[idx, "reviewer"] = reviewer.strip()
    df.at[idx, "date_reviewed"] = utc_now_timestamp()


def _apply_approve_left(df: pd.DataFrame, idx: int, reviewer: str, relation: str, logs: str) -> None:
    df.at[idx, "status"] = "approved"
    df.at[idx, "canonical_from"] = "left"
    df.at[idx, "canonical_term_iri"] = df.at[idx, "left_term_iri"]
    df.at[idx, "canonical_term_label"] = df.at[idx, "left_label"]
    df.at[idx, "canonical_term_source"] = df.at[idx, "left_source"]
    df.at[idx, "relation"] = relation
    df.at[idx, "suggestion_source"] = "manual_curated"
    df.at[idx, "logs"] = normalize_notes_for_approval(logs)
    _set_review_fields(df, idx, reviewer)


def _apply_approve_right(df: pd.DataFrame, idx: int, reviewer: str, relation: str, logs: str) -> None:
    df.at[idx, "status"] = "approved"
    df.at[idx, "canonical_from"] = "right"
    df.at[idx, "canonical_term_iri"] = df.at[idx, "right_term_iri"]
    df.at[idx, "canonical_term_label"] = df.at[idx, "right_label"]
    df.at[idx, "canonical_term_source"] = df.at[idx, "right_source"]
    df.at[idx, "relation"] = relation
    df.at[idx, "suggestion_source"] = "manual_curated"
    df.at[idx, "logs"] = normalize_notes_for_approval(logs)
    _set_review_fields(df, idx, reviewer)


def _apply_approve_manual(
    df: pd.DataFrame,
    idx: int,
    reviewer: str,
    relation: str,
    logs: str,
    manual_iri: str,
    manual_label: str,
    manual_source: str,
) -> bool:
    if not (manual_iri.strip() and manual_label.strip() and manual_source.strip()):
        return False

    df.at[idx, "status"] = "approved"
    df.at[idx, "canonical_from"] = "manual"
    df.at[idx, "canonical_term_iri"] = manual_iri.strip()
    df.at[idx, "canonical_term_label"] = manual_label.strip()
    df.at[idx, "canonical_term_source"] = manual_source.strip()
    df.at[idx, "relation"] = relation
    df.at[idx, "suggestion_source"] = "manual_curated"
    df.at[idx, "logs"] = normalize_notes_for_approval(logs)
    _set_review_fields(df, idx, reviewer)
    return True


def _apply_reject(df: pd.DataFrame, idx: int, reviewer: str, logs: str) -> None:
    df.at[idx, "status"] = "rejected"
    df.at[idx, "canonical_from"] = ""
    df.at[idx, "canonical_term_iri"] = ""
    df.at[idx, "canonical_term_label"] = ""
    df.at[idx, "canonical_term_source"] = ""
    df.at[idx, "logs"] = logs.strip()
    _set_review_fields(df, idx, reviewer)


def _append_log(existing_log: str, new_log: str) -> str:
    existing = existing_log.strip()
    entry = new_log.strip()
    if not entry:
        return existing
    if not existing:
        return entry
    if entry.lower() in existing.lower():
        return existing
    return f"{existing} | {entry}"


def _filtered_df(df: pd.DataFrame, statuses: list[str], search: str) -> pd.DataFrame:
    out = df.copy()
    if statuses:
        out = out[out["status"].isin(statuses)]

    token = search.strip().lower()
    if token:
        haystack = (
            out["alignment_id"].str.lower()
            + " "
            + out["left_label"].str.lower()
            + " "
            + out["right_label"].str.lower()
            + " "
            + out["logs"].str.lower()
            + " "
            + out["curation_comment"].str.lower()
        )
        out = out[haystack.str.contains(token, na=False)]
    return out


def _is_http(value: str) -> bool:
    text = str(value or "").strip().lower()
    return text.startswith("http://") or text.startswith("https://")


def _format_link_or_text(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return "-"
    escaped = html.escape(text)
    if _is_http(text):
        return f'<a href="{escaped}" target="_blank">{escaped}</a>'
    return escaped


def _compact_text(value: str, limit: int = 220) -> tuple[str, str]:
    text = str(value or "").strip()
    if not text:
        return "-", ""
    if len(text) <= limit:
        return text, ""
    short = text[:limit].rstrip() + "..."
    return short, text


def _display_source(value: object) -> str:
    text = str(value or "").strip()
    return text.upper() if text else "-"


def _display_kind(value: object) -> str:
    text = str(value or "").strip().lower()
    return text if text else "-"


def _kind_mismatch(left_kind: object, right_kind: object) -> bool:
    left = str(left_kind or "").strip().lower()
    right = str(right_kind or "").strip().lower()
    return bool(left and right and left != right)


def _ols_search_url(query: str) -> str:
    return "https://www.ebi.ac.uk/ols4/search?q=" + quote_plus(query or "")


def _bioportal_search_url(query: str) -> str:
    return "https://bioportal.bioontology.org/search?query=" + quote_plus(query or "")


def _infer_label_from_iri(iri: str) -> str:
    trimmed = (iri or "").rstrip("/#")
    if "#" in trimmed:
        return trimmed.rsplit("#", 1)[1]
    if "/" in trimmed:
        return trimmed.rsplit("/", 1)[1]
    return iri


def _next_alignment_id(df: pd.DataFrame) -> str:
    max_id = 0
    if "alignment_id" not in df.columns:
        return "CAND_0001"
    for value in df["alignment_id"].astype(str):
        match = re.fullmatch(r"CAND_(\d+)", value.strip())
        if not match:
            continue
        max_id = max(max_id, int(match.group(1)))
    return f"CAND_{max_id + 1:04d}"


def _normalize_label(text: str) -> str:
    value = (text or "").strip()
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", value)
    value = re.sub(r"\([^)]*\)", " ", value)
    value = value.replace("_", " ").replace("-", " ")
    value = value.lower()
    value = re.sub(r"^(a|an|the)\s+", "", value)
    value = re.sub(r"[^a-z0-9\s]", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def _tokenize(normalized_label: str) -> set[str]:
    if not normalized_label:
        return set()
    return {part for part in normalized_label.split() if part}


def _jaccard_score(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 0.0
    union = a | b
    if not union:
        return 0.0
    return len(a & b) / len(union)


def _manual_match_score(
    left_label: str,
    right_label: str,
    left_kind: str = "",
    right_kind: str = "",
) -> float:
    left_norm = _normalize_label(left_label)
    right_norm = _normalize_label(right_label)
    if left_norm and left_norm == right_norm:
        score = 1.0
    else:
        fuzzy = SequenceMatcher(None, left_norm, right_norm).ratio()
        jac = _jaccard_score(_tokenize(left_norm), _tokenize(right_norm))
        score = max(fuzzy, jac)
    if _kind_mismatch(left_kind, right_kind):
        score *= KIND_MISMATCH_SCORE_FACTOR
    return score


def _relation_for_score(score: float) -> str:
    return ""


def _normalize_mapping_relation(value: object, allowed: set[str]) -> str:
    rel = str(value or "").strip()
    if rel in allowed:
        return rel
    return ""


def _normalize_kind(value: object) -> str:
    kind = str(value or "").strip().lower()
    if kind in {"class", "property", "individual"}:
        return kind
    return ""


def _derived_export_mapping_labels(relation: str, left_kind: object, right_kind: object) -> list[str]:
    del left_kind, right_kind
    rel_bucket = relation.strip()
    if rel_bucket:
        return [rel_bucket]
    return []


def _short_local_name(iri: str) -> str:
    if "#" in iri:
        return iri.rsplit("#", 1)[-1]
    if "/" in iri:
        return iri.rsplit("/", 1)[-1]
    return iri


def _curie_for_iri(iri: str) -> str:
    if iri.startswith("http://www.w3.org/2002/07/owl#"):
        return "owl:" + _short_local_name(iri)
    if iri.startswith("http://www.w3.org/2000/01/rdf-schema#"):
        return "rdfs:" + _short_local_name(iri)
    if iri.startswith("http://www.w3.org/2004/02/skos/core#"):
        return "skos:" + _short_local_name(iri)
    return iri


def _build_mapping_relation_entries(merged: Graph) -> list[dict[str, str]]:
    skos_mapping_root = SKOS.mappingRelation
    skos_candidates: set[URIRef] = {skos_mapping_root}
    changed = True
    while changed:
        changed = False
        for subj, _, obj in merged.triples((None, RDFS.subPropertyOf, None)):
            if isinstance(subj, URIRef) and isinstance(obj, URIRef) and obj in skos_candidates and subj not in skos_candidates:
                skos_candidates.add(subj)
                changed = True

    keyword_tokens = ("equivalent", "same as", "subclass", "subproperty", "see also", "mapping", "match")
    structural_candidates: set[URIRef] = set()
    for subj in set(merged.subjects(RDF.type, RDF.Property)) | set(merged.subjects(RDF.type, OWL.ObjectProperty)) | set(
        merged.subjects(RDF.type, OWL.AnnotationProperty)
    ):
        if not isinstance(subj, URIRef):
            continue
        iri = str(subj)
        if iri.startswith("http://www.w3.org/2002/07/owl#") or iri.startswith("http://www.w3.org/2000/01/rdf-schema#"):
            text_parts = [iri.lower()]
            for obj in merged.objects(subj, RDFS.label):
                text_parts.append(str(obj).lower())
            for obj in merged.objects(subj, RDFS.comment):
                text_parts.append(str(obj).lower())
            blob = " ".join(text_parts)
            if any(tok in blob for tok in keyword_tokens):
                structural_candidates.add(subj)

    candidates = structural_candidates | skos_candidates

    entries: list[dict[str, str]] = []
    for iri_ref in sorted(
        candidates,
        key=lambda u: (
            0 if str(u).startswith("http://www.w3.org/2002/07/owl#")
            else 1 if str(u).startswith("http://www.w3.org/2000/01/rdf-schema#")
            else 2 if str(u).startswith("http://www.w3.org/2004/02/skos/core#")
            else 3,
            _curie_for_iri(str(u)).lower(),
        ),
    ):
        iri = str(iri_ref)
        label = ""
        definition = ""
        for obj in merged.objects(iri_ref, RDFS.label):
            label = str(obj).strip()
            if label:
                break
        for obj in merged.objects(iri_ref, RDFS.comment):
            definition = str(obj).strip()
            if definition:
                break
        if not definition:
            for obj in merged.objects(iri_ref, SKOS.definition):
                definition = str(obj).strip()
                if definition:
                    break
        entries.append(
            {
                "iri": iri,
                "curie": _curie_for_iri(iri),
                "label": label or _short_local_name(iri),
                "definition": definition or "",
            }
        )

    return entries


def _guess_rdf_format(path: Path) -> str | None:
    suffix = path.suffix.lower()
    if suffix in {".ttl", ".turtle"}:
        return "turtle"
    if suffix in {".rdf", ".owl", ".xml"}:
        return "xml"
    if suffix in {".json", ".jsonld"}:
        return "json-ld"
    if suffix == ".nt":
        return "nt"
    if suffix == ".nq":
        return "nquads"
    if suffix == ".trig":
        return "trig"
    return None


def _find_mapping_relation_files() -> list[Path]:
    if not MAPPING_RELATIONS_DIR.is_dir():
        return []
    files = [
        path
        for path in MAPPING_RELATIONS_DIR.iterdir()
        if path.is_file() and path.stem.strip().lower() in MAPPING_RELATION_ONTOLOGY_IDS
    ]
    files.sort(key=lambda p: p.name.lower())
    return files


def _load_mapping_relations_from_local_ontologies() -> tuple[dict | None, str]:
    source_files = _find_mapping_relation_files()
    if not source_files:
        return (
            None,
            "No local mapping ontologies found. Download `owl`, `rdfs`, and `skos` first "
            "in `Fetch schemas and ontologies`.",
        )

    merged = Graph()
    parse_failures: list[str] = []
    for path in source_files:
        try:
            guessed = _guess_rdf_format(path)
            if guessed:
                merged.parse(path, format=guessed)
            else:
                merged.parse(path)
        except Exception as exc:
            parse_failures.append(f"{path.name}: {exc}")

    entries = _build_mapping_relation_entries(merged)
    if not entries:
        msg = "No mapping relation terms found in local ontology files."
        if parse_failures:
            msg = msg + " Parse issues: " + "; ".join(parse_failures[:3])
        return None, msg

    payload = {
        "catalog_version": "local-files-v1",
        "sources": [to_relpath(path) for path in source_files],
        "relations": entries,
    }
    msg = f"Loaded {len(entries)} relation term(s) from local ontology files."
    if parse_failures:
        msg = msg + " Some files could not be parsed."
    return payload, msg


def _first_text(value: object) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        for item in value:
            if isinstance(item, str) and item.strip():
                return item.strip()
    if isinstance(value, dict):
        for subvalue in value.values():
            text = _first_text(subvalue)
            if text:
                return text
    return ""


def _extract_annotation_value(annotations: object, keys: list[str]) -> str:
    if not isinstance(annotations, dict):
        return ""
    for key in keys:
        if key in annotations:
            text = _first_text(annotations.get(key))
            if text:
                return text
    return ""


def _extract_annotation_by_substring(annotations: object, needle: str) -> str:
    if not isinstance(annotations, dict):
        return ""
    target = needle.strip().lower()
    if not target:
        return ""
    for key, value in annotations.items():
        key_text = str(key).strip().lower()
        if target in key_text:
            text = _first_text(value)
            if text:
                return text
    return ""


def _mapping_guidance_text(rel: str, fallback_definition: str) -> str:
    entry = MAPPING_GUIDANCE.get(rel, {})
    when = str(entry.get("when", "")).strip()
    example = str(entry.get("example", "")).strip()
    if when and example:
        return f"{when} Example: `{example}`"
    if when:
        return when
    return fallback_definition


def _infer_ontology_from_iri(iri: str) -> str:
    parsed = urlparse(iri)
    path = parsed.path or ""
    if "/obo/" in path:
        tail = path.rsplit("/", 1)[-1]
        if "_" in tail:
            return tail.split("_", 1)[0].strip().lower()
    if "/ontologies/" in path:
        after = path.split("/ontologies/", 1)[1]
        onto = after.split("/", 1)[0].strip().lower()
        if onto:
            return onto
    return ""


def _lookup_ols_hit_by_iri(
    iri: str, timeout: float = 4.0
) -> tuple[str, dict[str, str]]:
    """Best-effort OLS lookup by IRI/short form; returns (ontology_prefix, metadata_hint)."""
    clean_iri = iri.strip()
    if not clean_iri:
        return "", {"label": "", "definition": "", "comment": "", "example": "", "term_api_url": ""}

    short_form = _infer_label_from_iri(clean_iri)
    search_params = [
        {"q": clean_iri, "rows": 50, "exact": "true"},
        {"q": clean_iri, "rows": 50},
    ]
    if short_form:
        search_params.extend(
            [
                {"q": short_form, "rows": 50, "exact": "true"},
                {"q": short_form, "rows": 50},
            ]
        )

    def fetch_docs(params: dict[str, object]) -> list[dict[str, object]]:
        url = f"https://www.ebi.ac.uk/ols4/api/search?{urlencode(params)}"
        try:
            with urllib.request.urlopen(url, timeout=timeout) as response:
                payload = json.loads(response.read().decode("utf-8", errors="replace"))
        except Exception:
            return []
        docs = payload.get("response", {}).get("docs", [])
        if not isinstance(docs, list):
            return []
        return [doc for doc in docs if isinstance(doc, dict)]

    def doc_to_hint(doc: dict[str, object]) -> dict[str, str]:
        definition = _first_text(doc.get("description")) or _first_text(doc.get("definition"))
        comment = _first_text(doc.get("comment"))
        example = _first_text(doc.get("example"))
        return {
            "label": _first_text(doc.get("label")),
            "definition": definition,
            "comment": comment,
            "example": example,
            "term_api_url": "",
        }

    def normalize_iri(text: str) -> str:
        value = str(text or "").strip()
        if not value:
            return ""
        value = value.rstrip("/")
        value = value.replace("https://", "http://", 1)
        return value

    def score_doc(doc: dict[str, object]) -> int:
        score = 0
        doc_iri = str(doc.get("iri", "") or "").strip()
        doc_short = str(doc.get("short_form", "") or "").strip()
        onto_prefix = str(doc.get("ontology_prefix", "") or "").strip().lower()
        if not onto_prefix:
            return -1

        clean_norm = normalize_iri(clean_iri)
        doc_norm = normalize_iri(doc_iri)
        if doc_norm and clean_norm and doc_norm == clean_norm:
            score += 100

        if short_form:
            short_lower = short_form.lower()
            if doc_short and doc_short.lower() == short_lower:
                score += 35
            doc_iri_lower = doc_iri.lower()
            if doc_iri_lower.endswith("/" + short_lower) or doc_iri_lower.endswith("#" + short_lower):
                score += 20
            if short_lower in doc_iri_lower:
                score += 5
        return score

    best_doc: dict[str, object] | None = None
    best_score = -1
    for params in search_params:
        docs = fetch_docs(params)
        for doc in docs:
            score = score_doc(doc)
            if score > best_score:
                best_score = score
                best_doc = doc

    if best_doc is not None and best_score > 0:
        onto_prefix = str(best_doc.get("ontology_prefix", "") or "").strip().lower()
        if onto_prefix:
            return onto_prefix, doc_to_hint(best_doc)
    return "", {"label": "", "definition": "", "comment": "", "example": "", "term_api_url": ""}


def _fetch_ols_metadata_for_iri(iri: str, ontology: str, timeout: float = 4.0) -> dict[str, str]:
    if not iri.strip() or not ontology.strip():
        return {"label": "", "definition": "", "comment": "", "example": "", "term_api_url": ""}
    iri_encoded = quote(iri.strip(), safe="")
    iri_double_encoded = quote(iri_encoded, safe="")
    ontology_encoded = quote(ontology.strip().lower(), safe="")
    terms_global_direct_url = f"https://www.ebi.ac.uk/ols4/api/terms/{iri_double_encoded}"
    terms_global_fallback_url = f"https://www.ebi.ac.uk/ols4/api/terms?iri={iri_encoded}"
    terms_direct_url = f"https://www.ebi.ac.uk/ols4/api/ontologies/{ontology_encoded}/terms/{iri_encoded}"
    terms_fallback_url = (
        f"https://www.ebi.ac.uk/ols4/api/ontologies/{ontology_encoded}/terms?iri={iri_encoded}"
    )
    props_direct_url = f"https://www.ebi.ac.uk/ols4/api/ontologies/{ontology_encoded}/properties/{iri_encoded}"
    props_fallback_url = (
        f"https://www.ebi.ac.uk/ols4/api/ontologies/{ontology_encoded}/properties?iri={iri_encoded}"
    )
    indiv_scoped_direct_url = (
        f"https://www.ebi.ac.uk/ols4/api/ontologies/{ontology_encoded}/individuals/{iri_double_encoded}"
    )
    indiv_scoped_fallback_url = (
        f"https://www.ebi.ac.uk/ols4/api/ontologies/{ontology_encoded}/individuals?iri={iri_encoded}"
    )
    indiv_global_direct_url = f"https://www.ebi.ac.uk/ols4/api/individuals/{iri_double_encoded}"
    indiv_global_fallback_url = f"https://www.ebi.ac.uk/ols4/api/individuals?iri={iri_encoded}"

    def fetch_direct(url: str) -> dict[str, object] | None:
        try:
            with urllib.request.urlopen(url, timeout=timeout) as response:
                data = json.loads(response.read().decode("utf-8", errors="replace"))
            if isinstance(data, dict):
                return data
        except Exception:
            return None
        return None

    def fetch_embedded_first(url: str) -> dict[str, object] | None:
        try:
            with urllib.request.urlopen(url, timeout=timeout) as response:
                wrapper = json.loads(response.read().decode("utf-8", errors="replace"))
            if not isinstance(wrapper, dict):
                return None
            embedded = wrapper.get("_embedded", {})
            if not isinstance(embedded, dict):
                return None
            for key in ("terms", "properties", "individuals"):
                entries = embedded.get(key, [])
                if isinstance(entries, list) and entries:
                    first = entries[0]
                    if isinstance(first, dict):
                        return first
        except Exception:
            return None
        return None

    payload: dict[str, object] | None = None
    used_url = terms_fallback_url
    for url in (
        # Ontology Term Controller (robust default when ontology id is known)
        terms_fallback_url,
        terms_direct_url,
        # Global term endpoints as fallback
        terms_global_direct_url,
        terms_global_fallback_url,
        # Property and individual fallbacks
        props_direct_url,
        props_fallback_url,
        indiv_scoped_direct_url,
        indiv_scoped_fallback_url,
        indiv_global_direct_url,
        indiv_global_fallback_url,
    ):
        if (
            url.endswith(f"/terms/{iri_double_encoded}")
            or url.endswith(f"/terms/{iri_encoded}")
            or url.endswith(f"/properties/{iri_encoded}")
            or url.endswith(f"/individuals/{iri_double_encoded}")
        ):
            payload = fetch_direct(url)
        else:
            payload = fetch_embedded_first(url)
        if payload is not None:
            used_url = url
            break

    if payload is None:
        return {"label": "", "definition": "", "comment": "", "example": "", "term_api_url": ""}

    annotations = payload.get("annotation", payload.get("annotations", {}))
    label = _first_text(payload.get("label"))
    definition = (
        _first_text(payload.get("description"))
        or _first_text(payload.get("definition"))
        or _extract_annotation_value(
            annotations,
            [
                "http://www.w3.org/2004/02/skos/core#definition",
                "http://purl.obolibrary.org/obo/IAO_0000115",
                "definition",
            ],
        )
        or _extract_annotation_by_substring(annotations, "definition")
    )
    comment = (
        _first_text(payload.get("comment"))
        or _extract_annotation_value(
            annotations,
            ["http://www.w3.org/2000/01/rdf-schema#comment", "comment"],
        )
        or _extract_annotation_by_substring(annotations, "comment")
    )
    example = (
        _first_text(payload.get("example"))
        or _extract_annotation_value(
            annotations,
            [
                "http://purl.org/vocab/vann/example",
                "http://www.w3.org/2004/02/skos/core#example",
                "example",
            ],
        )
        or _extract_annotation_by_substring(annotations, "example")
    )
    return {
        "label": label,
        "definition": definition,
        "comment": comment,
        "example": example,
        "term_api_url": used_url,
    }


def _fetch_ols_metadata_for_entity(
    *,
    iri: str,
    ontology: str,
    entity_kind: str,
    timeout: float = 4.0,
) -> dict[str, str]:
    """Fetch OLS metadata prioritizing endpoint type requested by user."""
    clean_kind = (entity_kind or "").strip().lower()
    if clean_kind in {"class", "term"}:
        return _fetch_ols_metadata_for_iri(iri, ontology, timeout=timeout)

    # Reuse the robust parser by selecting endpoint priority via temporary URL ordering.
    iri_encoded = quote(iri.strip(), safe="")
    iri_double_encoded = quote(iri_encoded, safe="")
    ontology_encoded = quote(ontology.strip().lower(), safe="")
    terms_global_direct_url = f"https://www.ebi.ac.uk/ols4/api/terms/{iri_double_encoded}"
    terms_global_fallback_url = f"https://www.ebi.ac.uk/ols4/api/terms?iri={iri_encoded}"
    terms_direct_url = f"https://www.ebi.ac.uk/ols4/api/ontologies/{ontology_encoded}/terms/{iri_encoded}"
    terms_fallback_url = (
        f"https://www.ebi.ac.uk/ols4/api/ontologies/{ontology_encoded}/terms?iri={iri_encoded}"
    )
    props_direct_url = f"https://www.ebi.ac.uk/ols4/api/ontologies/{ontology_encoded}/properties/{iri_encoded}"
    props_fallback_url = (
        f"https://www.ebi.ac.uk/ols4/api/ontologies/{ontology_encoded}/properties?iri={iri_encoded}"
    )
    indiv_scoped_direct_url = (
        f"https://www.ebi.ac.uk/ols4/api/ontologies/{ontology_encoded}/individuals/{iri_double_encoded}"
    )
    indiv_scoped_fallback_url = (
        f"https://www.ebi.ac.uk/ols4/api/ontologies/{ontology_encoded}/individuals?iri={iri_encoded}"
    )
    indiv_global_direct_url = f"https://www.ebi.ac.uk/ols4/api/individuals/{iri_double_encoded}"
    indiv_global_fallback_url = f"https://www.ebi.ac.uk/ols4/api/individuals?iri={iri_encoded}"

    if clean_kind == "property":
        ordered = (
            props_direct_url,
            props_fallback_url,
            terms_fallback_url,
            terms_direct_url,
            terms_global_direct_url,
            terms_global_fallback_url,
            indiv_scoped_direct_url,
            indiv_scoped_fallback_url,
            indiv_global_direct_url,
            indiv_global_fallback_url,
        )
    elif clean_kind == "individual":
        ordered = (
            indiv_scoped_direct_url,
            indiv_scoped_fallback_url,
            indiv_global_direct_url,
            indiv_global_fallback_url,
            terms_fallback_url,
            terms_direct_url,
            terms_global_direct_url,
            terms_global_fallback_url,
            props_direct_url,
            props_fallback_url,
        )
    else:
        ordered = (
            terms_fallback_url,
            terms_direct_url,
            terms_global_direct_url,
            terms_global_fallback_url,
            props_direct_url,
            props_fallback_url,
            indiv_scoped_direct_url,
            indiv_scoped_fallback_url,
            indiv_global_direct_url,
            indiv_global_fallback_url,
        )

    # Inline minimal fetch logic to preserve the same extraction behavior.
    def fetch_direct(url: str) -> dict[str, object] | None:
        try:
            with urllib.request.urlopen(url, timeout=timeout) as response:
                data = json.loads(response.read().decode("utf-8", errors="replace"))
            if isinstance(data, dict):
                return data
        except Exception:
            return None
        return None

    def fetch_embedded_first(url: str) -> dict[str, object] | None:
        try:
            with urllib.request.urlopen(url, timeout=timeout) as response:
                wrapper = json.loads(response.read().decode("utf-8", errors="replace"))
            if not isinstance(wrapper, dict):
                return None
            embedded = wrapper.get("_embedded", {})
            if not isinstance(embedded, dict):
                return None
            for key in ("terms", "properties", "individuals"):
                entries = embedded.get(key, [])
                if isinstance(entries, list) and entries:
                    first = entries[0]
                    if isinstance(first, dict):
                        return first
        except Exception:
            return None
        return None

    payload: dict[str, object] | None = None
    used_url = ordered[0]
    for url in ordered:
        if (
            "/terms/" in url
            or "/properties/" in url
            or "/individuals/" in url
        ):
            payload = fetch_direct(url)
        else:
            payload = fetch_embedded_first(url)
        if payload is not None:
            used_url = url
            break

    if payload is None:
        return {"label": "", "definition": "", "comment": "", "example": "", "term_api_url": ""}

    annotations = payload.get("annotation", payload.get("annotations", {}))
    label = _first_text(payload.get("label"))
    definition = (
        _first_text(payload.get("description"))
        or _first_text(payload.get("definition"))
        or _extract_annotation_value(
            annotations,
            [
                "http://www.w3.org/2004/02/skos/core#definition",
                "http://purl.obolibrary.org/obo/IAO_0000115",
                "definition",
            ],
        )
        or _extract_annotation_by_substring(annotations, "definition")
    )
    comment = (
        _first_text(payload.get("comment"))
        or _extract_annotation_value(
            annotations,
            ["http://www.w3.org/2000/01/rdf-schema#comment", "comment"],
        )
        or _extract_annotation_by_substring(annotations, "comment")
    )
    example = (
        _first_text(payload.get("example"))
        or _extract_annotation_value(
            annotations,
            [
                "http://purl.org/vocab/vann/example",
                "http://www.w3.org/2004/02/skos/core#example",
                "example",
            ],
        )
        or _extract_annotation_by_substring(annotations, "example")
    )
    return {
        "label": label,
        "definition": definition,
        "comment": comment,
        "example": example,
        "term_api_url": used_url,
    }


def _render_term_card(
    *,
    side: str,
    title: str,
    fields: list[tuple[str, str]],
    selected: bool | None = None,
    score: float | None = None,
) -> None:
    classes = [f"term-card {side}"]
    if selected is not None:
        classes.append("selected" if selected else "unselected")
    elif side == "right":
        classes.append("neutral")
    score_html = ""
    if score is not None and not pd.isna(score):
        pct = max(0, min(100, int(round(score * 100))))
        if score >= 0.90:
            ring_color = "#16a34a"
        elif score >= 0.75:
            ring_color = "#d97706"
        else:
            ring_color = "#dc2626"
        score_html = (
            f'<div class="score-ring" style="--p:{pct}; --ring-color:{ring_color};">'
            f'<div class="score-ring-inner">{pct}%</div>'
            "</div>"
        )
    row_html_parts: list[str] = []
    for label, value in fields:
        compact, full = _compact_text(str(value))
        row_html_parts.append(
            (
                '<div class="card-row">'
                f'<strong>{html.escape(label)}:</strong> '
                f'<span title="{html.escape(full)}">{_format_link_or_text(compact)}</span>'
                "</div>"
            )
        )
    rows = "".join(row_html_parts)
    st.markdown(
        (
            f'<div class="{" ".join(classes)}">'
            '<div class="term-card-head">'
            f'<div class="term-card-title">{html.escape(title)}</div>'
            f"{score_html}"
            "</div>"
            f"{rows}"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def render() -> None:
    st.title("Curate Candidate Alignments")
    st.markdown(
        """
        <style>
          .term-card {
            border-radius: 12px;
            padding: 0.55rem 0.65rem;
            margin-bottom: 0.45rem;
            border: 1px solid #d6dbe1;
            background: #f8fafc;
            font-size: 0.88rem;
          }
          .term-card.left {
            border-left: 6px solid #1f77b4;
            background: #f4f8ff;
          }
          .term-card.left.selected {
            border-left: 6px solid #2f9e44;
            background: #ebfff1;
            border-color: #86efac;
          }
          .term-card.right {
            border-left: 6px solid #2f9e44;
            background: #f4fff7;
          }
          .term-card.right.neutral {
            border-left: 6px solid #94a3b8;
            background: #f8fafc;
            border-color: #cbd5e1;
          }
          .term-card.right.selected {
            border-left: 6px solid #2f9e44;
            background: #ebfff1;
            border-color: #86efac;
          }
          .term-card.right.unselected {
            border-left: 6px solid #c92a2a;
            background: #fff5f5;
            border-color: #fecaca;
          }
          .term-card-title {
            font-weight: 700;
            margin-bottom: 0.2rem;
            font-size: 0.92rem;
          }
          .term-card-head {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 0.45rem;
            margin-bottom: 0.2rem;
          }
          .card-row {
            margin: 0.12rem 0;
            line-height: 1.18;
          }
          .score-ring {
            width: 31px;
            height: 31px;
            border-radius: 50%;
            background: conic-gradient(var(--ring-color, #16a34a) calc(var(--p) * 1%), #e2e8f0 0);
            display: grid;
            place-items: center;
            flex: 0 0 auto;
          }
          .score-ring-inner {
            width: 22px;
            height: 22px;
            border-radius: 50%;
            background: #ffffff;
            display: grid;
            place-items: center;
            font-size: 0.52rem;
            font-weight: 700;
            color: #1f2937;
          }
        </style>
        """,
        unsafe_allow_html=True,
    )

    ctx = active_source_context()
    if ctx is None:
        st.warning("No source slug available. Configure sources in Download External Sources first.")
        return

    candidate_file = to_relpath(ctx.candidates_tsv)
    st.caption(f"Active candidates file: `{candidate_file}`")
    active_curator = str(st.session_state.get(STATE_CURATOR, "") or "").strip()
    if active_curator:
        st.caption(f"Active curator: `{active_curator}`")
    else:
        st.error("Set a Curator name in the left sidebar before starting curation.")
        return

    if (
        STATE_PATH not in st.session_state
        or st.session_state.get(STATE_PATH) != candidate_file
        or STATE_DF not in st.session_state
    ):
        st.session_state[STATE_PATH] = candidate_file
        st.session_state[STATE_DF] = _load_df(candidate_file)
        st.session_state[STATE_DIRTY] = False
        st.session_state[STATE_MTIME] = _file_mtime(candidate_file)
        st.session_state[STATE_KEPT_LEFT_TERMS] = []

    if STATE_KEPT_LEFT_TERMS not in st.session_state:
        st.session_state[STATE_KEPT_LEFT_TERMS] = []

    current_mtime = _file_mtime(candidate_file)
    loaded_mtime = st.session_state.get(STATE_MTIME)
    if current_mtime != loaded_mtime:
        if st.session_state.get(STATE_DIRTY):
            st.warning(
                "Candidates file changed on disk, but you have unsaved edits in memory. "
                "Use 'Reload from disk' to refresh."
            )
        else:
            st.session_state[STATE_DF] = _load_df(candidate_file)
            st.session_state[STATE_MTIME] = current_mtime
            st.info("Reloaded latest candidates from disk.")

    col_reload, col_save = st.columns(2)
    with col_reload:
        if st.button("Reload from disk"):
            st.session_state[STATE_DF] = _load_df(candidate_file)
            st.session_state[STATE_DIRTY] = False
            st.session_state[STATE_MTIME] = _file_mtime(candidate_file)
    with col_save:
        if st.button("Save candidate file", type="primary"):
            write_tsv(st.session_state[STATE_DF], candidate_file)
            st.session_state[STATE_DIRTY] = False
            st.session_state[STATE_MTIME] = _file_mtime(candidate_file)
            st.success(f"Saved `{candidate_file}`")

    relation_catalog, catalog_msg = _load_mapping_relations_from_local_ontologies()
    if relation_catalog is None:
        st.error(catalog_msg or "Mapping relation catalog unavailable.")
        return
    relations = relation_catalog.get("relations", [])
    relation_options = [str(x.get("curie", "")).strip() for x in relations if str(x.get("curie", "")).strip()]
    relation_definitions = {
        str(x.get("curie", "")).strip(): str(x.get("definition", "")).strip() or str(x.get("label", "")).strip()
        for x in relations
        if str(x.get("curie", "")).strip()
    }
    relation_allowed = set(relation_options)
    if not relation_options:
        st.error("Mapping relation catalog has no relation entries.")
        return
    if catalog_msg:
        st.caption(catalog_msg)

    df = st.session_state[STATE_DF]
    if df.empty and not to_path(candidate_file).is_file():
        st.warning("No candidate file found for this source. Please run Generate Pairwise Candidates first.")
        return

    term_groups = (
        df.groupby(["left_source", "left_term_iri"], dropna=False)["status"]
        .apply(lambda series: any(str(value) == "needs_review" for value in series))
        .reset_index(name="has_needs_review")
    )
    total_terms = len(term_groups)
    needs_review_terms = int(term_groups["has_needs_review"].sum()) if total_terms else 0
    curated_terms = max(0, total_terms - needs_review_terms)
    progress = (curated_terms / total_terms) if total_terms else 0.0

    st.subheader("Curation Progress")
    st.progress(progress, text=f"{curated_terms}/{total_terms} terms curated ({progress * 100:.1f}%)")

    st.subheader("Filter")
    available_statuses = sorted(df["status"].dropna().unique().tolist())
    default_statuses = ["needs_review"] if "needs_review" in available_statuses else available_statuses
    selected_statuses = st.multiselect(
        "Status",
        options=available_statuses,
        default=default_statuses,
    )
    search_text = st.text_input("Search (left/right labels, logs, curation comments)", value="")

    filtered = _filtered_df(df, selected_statuses, search_text)
    st.caption(f"Filtered rows: {len(filtered)}")

    left_terms_df = (
        filtered[
            [
                "left_source",
                "left_term_iri",
                "left_label",
                "left_definition",
                "left_comment",
                "left_example",
                "left_term_kind",
            ]
        ]
        .drop_duplicates()
        .copy()
    )

    token = search_text.strip().lower()
    if token:
        label_series = left_terms_df["left_label"].astype(str).str.lower()
        left_terms_df = left_terms_df[label_series.str.contains(token, na=False)]
    left_terms_df = left_terms_df.drop_duplicates(subset=["left_source", "left_term_iri", "left_label"]).reset_index(
        drop=True
    )

    if left_terms_df.empty:
        st.info("No left terms available for current filters. Generate or reload candidates first.")
    else:
        left_keys = list(left_terms_df[["left_source", "left_term_iri", "left_label"]].itertuples(index=False, name=None))
        if STATE_LEFT_TERM_INDEX not in st.session_state:
            st.session_state[STATE_LEFT_TERM_INDEX] = 0
        left_idx = int(st.session_state.get(STATE_LEFT_TERM_INDEX, 0))
        left_idx = max(0, min(left_idx, len(left_keys) - 1))
        selected_left = st.selectbox(
            "Left term",
            options=left_keys,
            index=left_idx,
            format_func=lambda item: f"{item[2]} ({item[0]})",
            help="Select a left ontology term to review all candidate matches on the right.",
        )
        selected_idx = left_keys.index(selected_left)
        st.session_state[STATE_LEFT_TERM_INDEX] = selected_idx
        left_source, left_iri, left_label = selected_left
        left_term_key = f"{left_source}|{left_iri}"
        left_row_series = left_terms_df[
            (left_terms_df["left_source"] == left_source) & (left_terms_df["left_term_iri"] == left_iri)
        ].iloc[0]

        group_df = filtered[
            (filtered["left_source"] == left_source) & (filtered["left_term_iri"] == left_iri)
        ].copy()
        if not group_df.empty:
            group_df["_match_score_num"] = pd.to_numeric(group_df["match_score"], errors="coerce").fillna(-1.0)
            group_df = group_df.sort_values(by=["_match_score_num", "right_label"], ascending=[False, True])
        kept_left_terms = set(str(x) for x in st.session_state.get(STATE_KEPT_LEFT_TERMS, []))
        left_is_kept = left_term_key in kept_left_terms
        if not group_df.empty and (group_df["status"] == "rejected").all():
            left_is_kept = True

        selected_alignment_id = None
        row_idx = None
        row = None
        if not group_df.empty:
            selected_alignment_id = str(st.session_state.get(STATE_SELECTED_ALIGNMENT, ""))
            available_alignment_ids = group_df["alignment_id"].astype(str).tolist()
            if selected_alignment_id not in available_alignment_ids:
                selected_alignment_id = ""
                st.session_state[STATE_SELECTED_ALIGNMENT] = ""
            if selected_alignment_id:
                row_idx = int(df.index[df["alignment_id"] == selected_alignment_id][0])
                row = df.loc[row_idx]

        default_relation = _normalize_mapping_relation(
            row.get("relation", "") if row is not None else "", relation_allowed
        )
        relation_select_options = [MAPPING_RELATION_PLACEHOLDER] + relation_options
        selected_mapping_relation = st.selectbox(
            "Mapping relation (OWL/RDFS/SKOS)",
            options=relation_select_options,
            index=(
                relation_select_options.index(default_relation)
                if default_relation in relation_select_options
                else 0
            ),
            key=f"mapping_relation_{left_term_key}",
            help="Searchable dropdown for semantic mapping relation between source term and selected mapped term.",
        )
        mapping_relation_selected = selected_mapping_relation in relation_allowed
        if mapping_relation_selected:
            st.caption(
                _mapping_guidance_text(
                    selected_mapping_relation,
                    relation_definitions.get(selected_mapping_relation, ""),
                )
            )
        else:
            st.caption("Select a mapping relation to enable mapping between source and target entities.")
        if mapping_relation_selected:
            preview_labels = _derived_export_mapping_labels(
                selected_mapping_relation,
                left_row_series.get("left_term_kind", ""),
                row.get("right_term_kind", "") if row is not None else "",
            )
            if preview_labels:
                st.caption("Derived export mappings: " + ", ".join(f"`{x}`" for x in preview_labels))
        with st.expander("Mapping guidance", expanded=False):
            recommended_ordered = [
                rel for rel in MAPPING_GUIDANCE_ORDER if rel in relation_options and MAPPING_GUIDANCE.get(rel, {}).get("tier") != "advanced"
            ]
            advanced_ordered = [
                rel for rel in MAPPING_GUIDANCE_ORDER if rel in relation_options and MAPPING_GUIDANCE.get(rel, {}).get("tier") == "advanced"
            ]
            remaining = [rel for rel in relation_options if rel not in set(recommended_ordered + advanced_ordered)]

            st.markdown("**Recommended first (ontology reconciliation)**")
            for rel in recommended_ordered:
                st.markdown(
                    f"- `{rel}`: {_mapping_guidance_text(rel, relation_definitions.get(rel, ''))}"
                )
            if not recommended_ordered:
                st.markdown("- No recommended relations available from local mapping ontologies.")

            st.markdown("**Advanced / specific modeling cases**")
            for rel in advanced_ordered:
                st.markdown(
                    f"- `{rel}`: {_mapping_guidance_text(rel, relation_definitions.get(rel, ''))}"
                )
            for rel in remaining:
                st.markdown(f"- `{rel}`: {relation_definitions.get(rel, '')}")

        st.subheader("Side-by-side context")
        left_col, right_col = st.columns(2)
        with left_col:
            st.markdown(f"**{_display_source(left_source)} term**")
            left_card_selected = True if mapping_relation_selected else left_is_kept
            _render_term_card(
                side="left",
                title=left_label,
                selected=left_card_selected,
                fields=[
                    ("Source", _display_source(left_row_series["left_source"])),
                    ("Kind", _display_kind(left_row_series.get("left_term_kind", ""))),
                    ("IRI", str(left_row_series["left_term_iri"])),
                    ("Definition", str(left_row_series.get("left_definition", "") or "-")),
                    ("Comment", str(left_row_series.get("left_comment", "") or "-")),
                    ("Example", str(left_row_series.get("left_example", "") or "-")),
                ],
            )
            if st.button("Keep current term", key=f"keep_left_{left_term_key}"):
                kept_left_terms.add(left_term_key)
                st.session_state[STATE_KEPT_LEFT_TERMS] = sorted(kept_left_terms)
                st.session_state[STATE_SELECTED_ALIGNMENT] = ""
                st.rerun()
        with right_col:
            st.markdown("**Matched terms**")
            if group_df.empty:
                st.info("No right-side candidates found for this left term yet.")
            else:
                for _, right_row in group_df.iterrows():
                    current_alignment_id = str(right_row["alignment_id"])
                    is_selected = current_alignment_id == selected_alignment_id
                    is_kind_mismatch = _kind_mismatch(
                        left_row_series.get("left_term_kind", ""),
                        right_row.get("right_term_kind", ""),
                    )
                    has_right_decision = bool(selected_alignment_id)
                    if mapping_relation_selected:
                        card_state = is_selected if has_right_decision else None
                    else:
                        if left_is_kept:
                            card_state = False
                        elif has_right_decision:
                            card_state = is_selected
                        else:
                            card_state = None
                    title = str(right_row["right_label"] or right_row["right_term_iri"] or "(no label)")
                    if is_selected:
                        title = f"{title} [selected]"
                    _render_term_card(
                        side="right",
                        title=title,
                        selected=card_state,
                        score=float(pd.to_numeric(right_row["match_score"], errors="coerce"))
                        if str(right_row.get("match_score", "")).strip()
                        else None,
                        fields=[
                            ("Source", _display_source(right_row["right_source"])),
                            ("Kind", _display_kind(right_row.get("right_term_kind", ""))),
                            ("Kind warning", "mismatch" if is_kind_mismatch else "-"),
                            ("IRI", str(right_row["right_term_iri"])),
                            ("Match score", str(right_row["match_score"])),
                            ("Status", str(right_row["status"])),
                            ("Definition", str(right_row["right_definition"] or "-")),
                            ("Comment", str(right_row["right_comment"] or "-")),
                            ("Example", str(right_row["right_example"] or "-")),
                        ],
                    )
                    select_col, delete_col = st.columns(2)
                    if select_col.button(
                        "Select this match",
                        key=f"select_match_{current_alignment_id}",
                    ):
                        st.session_state[STATE_SELECTED_ALIGNMENT] = current_alignment_id
                        kept_left_terms.discard(left_term_key)
                        st.session_state[STATE_KEPT_LEFT_TERMS] = sorted(kept_left_terms)
                        st.rerun()
                    if delete_col.button(
                        "Delete this match",
                        key=f"delete_match_{current_alignment_id}",
                    ):
                        mask = df["alignment_id"].astype(str) == current_alignment_id
                        if bool(mask.any()):
                            st.session_state[STATE_DF] = df.loc[~mask].reset_index(drop=True)
                            if st.session_state.get(STATE_SELECTED_ALIGNMENT) == current_alignment_id:
                                st.session_state[STATE_SELECTED_ALIGNMENT] = ""
                            st.session_state[STATE_DIRTY] = True
                            st.success("Deleted candidate match.")
                            st.rerun()

            st.markdown("**Expand search manually**")
            ols_link = _ols_search_url(left_label)
            st.markdown(f"[Open OLS search for this term]({ols_link})")

            manual_url = st.text_input(
                "Manual candidate term IRI",
                value="",
                key=f"manual_candidate_url_{left_term_key}",
                help="Paste a full term IRI (must start with http:// or https://).",
            )
            manual_ontology_id = st.text_input(
                "Ontology ID (required)",
                value="",
                key=f"manual_candidate_ontology_{left_term_key}",
                help="OLS ontology id (for example: chebi, obi, mesh, biolink, uniprotrdfs).",
            )
            manual_entity_kind = st.selectbox(
                "Entity type (required)",
                options=["Class", "Property", "Individual"],
                index=0,
                key=f"manual_candidate_entity_kind_{left_term_key}",
                help="Chooses which OLS controller endpoint is queried first.",
            )
            if st.button("Add manual candidate", key=f"add_manual_candidate_{left_term_key}"):
                iri = manual_url.strip()
                ontology_id = manual_ontology_id.strip().lower()
                entity_kind = manual_entity_kind.strip().lower()
                if not iri:
                    st.error("Manual candidate URL/IRI is required.")
                elif not _is_http(iri):
                    st.error("Manual candidate must be a valid IRI starting with http:// or https://.")
                elif not ontology_id:
                    st.error("Ontology ID is required.")
                elif entity_kind not in {"class", "property", "individual"}:
                    st.error("Entity type is required.")
                else:
                    existing_mask = (
                        (df["left_source"] == left_source)
                        & (df["left_term_iri"] == left_iri)
                        & (df["right_term_iri"] == iri)
                    )
                    if bool(existing_mask.any()):
                        st.warning("This candidate URL already exists for the selected left term.")
                    else:
                        _, search_hint = _lookup_ols_hit_by_iri(iri)
                        ontology_for_metadata = ontology_id
                        right_source_value = ontology_id.upper()
                        metadata = (
                            _fetch_ols_metadata_for_entity(
                                iri=iri,
                                ontology=ontology_for_metadata,
                                entity_kind=entity_kind,
                            )
                            if ontology_for_metadata
                            else {"label": "", "definition": "", "comment": "", "example": "", "term_api_url": ""}
                        )
                        # If direct term lookup is sparse, keep useful fields from OLS search hit.
                        for key in ("label", "definition", "comment", "example", "term_api_url"):
                            if not metadata.get(key, "").strip():
                                metadata[key] = search_hint.get(key, "")

                        if not metadata.get("term_api_url", "").strip() or not metadata.get("label", "").strip():
                            st.error(
                                "No OLS match found for this ontology ID + IRI. "
                                "Card was not created."
                            )
                            return

                        right_label_value = metadata["label"].strip()
                        match_score_value = _manual_match_score(
                            left_label,
                            right_label_value,
                            left_kind=str(left_row_series.get("left_term_kind", "") or ""),
                            right_kind=entity_kind,
                        )

                        new_row = {col: "" for col in df.columns}
                        new_row["alignment_id"] = _next_alignment_id(df)
                        new_row["left_source"] = left_source
                        new_row["left_term_iri"] = left_iri
                        new_row["left_label"] = left_label
                        new_row["left_definition"] = str(left_row_series.get("left_definition", "") or "")
                        new_row["left_comment"] = str(left_row_series.get("left_comment", "") or "")
                        new_row["left_example"] = str(left_row_series.get("left_example", "") or "")
                        new_row["left_term_kind"] = str(left_row_series.get("left_term_kind", "") or "")
                        new_row["right_source"] = right_source_value
                        new_row["right_term_iri"] = iri
                        new_row["right_label"] = right_label_value
                        new_row["right_definition"] = metadata["definition"]
                        new_row["right_comment"] = metadata["comment"]
                        new_row["right_example"] = metadata["example"]
                        new_row["right_term_kind"] = entity_kind
                        new_row["ols_term_api_url"] = metadata["term_api_url"] or iri
                        new_row["match_method"] = "manual_url_lexical"
                        new_row["match_score"] = f"{match_score_value:.2f}"
                        new_row["relation"] = _relation_for_score(match_score_value)
                        new_row["suggestion_source"] = "manual_search"
                        new_row["canonical_from"] = ""
                        new_row["canonical_term_iri"] = ""
                        new_row["canonical_term_label"] = ""
                        new_row["canonical_term_source"] = ""
                        new_row["ols_search_url"] = _ols_search_url(left_label)
                        new_row["bioportal_search_url"] = _bioportal_search_url(left_label)
                        new_row["status"] = "needs_review"
                        new_row["curator"] = "auto"
                        new_row["reviewer"] = active_curator
                        new_row["date_reviewed"] = ""
                        new_row["logs"] = (
                            f"Manual {entity_kind} candidate added by URL with ontology id '{ontology_id}'."
                        )
                        new_row["curation_comment"] = ""
                        if "normalized_left_label" in df.columns:
                            new_row["normalized_left_label"] = str(left_label).strip().lower()
                        if "normalized_right_label" in df.columns:
                            new_row["normalized_right_label"] = str(right_label_value).strip().lower()

                        st.session_state[STATE_DF] = pd.concat(
                            [st.session_state[STATE_DF], pd.DataFrame([new_row], columns=df.columns)],
                            ignore_index=True,
                        )
                        st.session_state[STATE_DIRTY] = True
                        st.session_state[STATE_SELECTED_ALIGNMENT] = str(new_row["alignment_id"])
                        kept_left_terms.discard(left_term_key)
                        st.session_state[STATE_KEPT_LEFT_TERMS] = sorted(kept_left_terms)
                        st.success("Manual candidate added.")
                        st.rerun()
        if mapping_relation_selected:
            decision_made = row is not None and row_idx is not None
        else:
            decision_made = left_is_kept or (row is not None and row_idx is not None)
        if row is not None:
            selected_left_kind = left_row_series.get("left_term_kind", "")
            selected_right_kind = row.get("right_term_kind", "")
            if _kind_mismatch(selected_left_kind, selected_right_kind):
                st.warning(
                    "Kind mismatch: selected match is "
                    f"`{_display_kind(selected_right_kind)}` while left term is "
                    f"`{_display_kind(selected_left_kind)}`."
                )

        st.markdown("**Curation comment**")
        curation_comment = st.text_area(
            "curation_comment (optional)",
            value="",
            key=f"curation_comment_{left_term_key}",
            help="Saved separately from automated logs for this validation step.",
        )
        validate_col = st.columns([1, 2, 1])[1]
        if validate_col.button(
            "Validate",
            key=f"validate_selection_{left_term_key}",
            type="primary",
            disabled=not decision_made,
            use_container_width=True,
        ):
            if left_is_kept:
                if not group_df.empty:
                    mask = (df["left_source"] == left_source) & (df["left_term_iri"] == left_iri)
                    idxs = df.index[mask].tolist()
                    for idx in idxs:
                        existing_logs = str(df.at[idx, "logs"] or "").strip()
                        log_entry = "Kept current left term; rejected right-side candidate matches."
                        df.at[idx, "status"] = "rejected"
                        df.at[idx, "canonical_from"] = ""
                        df.at[idx, "canonical_term_iri"] = ""
                        df.at[idx, "canonical_term_label"] = ""
                        df.at[idx, "canonical_term_source"] = ""
                        df.at[idx, "relation"] = ""
                        df.at[idx, "logs"] = _append_log(existing_logs, log_entry)
                        df.at[idx, "curation_comment"] = curation_comment.strip()
                        df.at[idx, "date_reviewed"] = utc_now_timestamp()
                        if active_curator:
                            df.at[idx, "reviewer"] = active_curator

                st.session_state[STATE_DIRTY] = True
                st.session_state[STATE_LEFT_TERM_INDEX] = min(selected_idx + 1, len(left_keys) - 1)
                st.session_state[STATE_SELECTED_ALIGNMENT] = ""
                st.rerun()
            else:
                mask = (df["left_source"] == left_source) & (df["left_term_iri"] == left_iri)
                idxs = df.index[mask].tolist()
                for idx in idxs:
                    existing_logs = str(df.at[idx, "logs"] or "").strip()
                    if idx == row_idx:
                        df.at[idx, "status"] = "approved"
                        df.at[idx, "canonical_from"] = "right"
                        df.at[idx, "canonical_term_iri"] = df.at[idx, "right_term_iri"]
                        df.at[idx, "canonical_term_label"] = df.at[idx, "right_label"]
                        df.at[idx, "canonical_term_source"] = df.at[idx, "right_source"]
                        df.at[idx, "relation"] = selected_mapping_relation if mapping_relation_selected else ""
                        df.at[idx, "suggestion_source"] = "manual_curated"
                        log_entry = "Validated selected right-side match."
                    else:
                        df.at[idx, "status"] = "rejected"
                        df.at[idx, "canonical_from"] = ""
                        df.at[idx, "canonical_term_iri"] = ""
                        df.at[idx, "canonical_term_label"] = ""
                        df.at[idx, "canonical_term_source"] = ""
                        df.at[idx, "relation"] = ""
                        log_entry = "Not selected for this left term."
                    df.at[idx, "logs"] = _append_log(existing_logs, log_entry)
                    df.at[idx, "curation_comment"] = curation_comment.strip()
                    df.at[idx, "date_reviewed"] = utc_now_timestamp()
                    if active_curator:
                        df.at[idx, "reviewer"] = active_curator

                kept_left_terms.discard(left_term_key)
                st.session_state[STATE_KEPT_LEFT_TERMS] = sorted(kept_left_terms)
                st.session_state[STATE_DIRTY] = True
                st.session_state[STATE_LEFT_TERM_INDEX] = min(selected_idx + 1, len(left_keys) - 1)
                st.session_state[STATE_SELECTED_ALIGNMENT] = ""
                st.rerun()

        skip_col = st.columns([1, 2, 1])[1]
        if skip_col.button("Skip term", key=f"skip_term_{left_term_key}", use_container_width=True):
            st.session_state[STATE_LEFT_TERM_INDEX] = min(selected_idx + 1, len(left_keys) - 1)
            st.session_state[STATE_SELECTED_ALIGNMENT] = ""
            st.rerun()

    st.subheader("Working table preview")
    table_status = st.radio(
        "Table status filter",
        options=["Needs review", "Curated", "All"],
        horizontal=True,
        index=0,
        help="Curated means status is not needs_review.",
    )
    table_df = filtered.copy()
    if table_status == "Needs review":
        table_df = table_df[table_df["status"] == "needs_review"]
    elif table_status == "Curated":
        table_df = table_df[table_df["status"] != "needs_review"]
    st.caption(f"Table rows: {len(table_df)}")
    render_clickable_dataframe(table_df, use_container_width=True, hide_index=True)

    st.download_button(
        label="Download working candidate TSV",
        data=dataframe_to_tsv_bytes(df),
        file_name=to_path(candidate_file).name,
        mime="text/tab-separated-values",
        key="download_working_candidates",
    )

    if st.session_state.get(STATE_DIRTY):
        _autosave_if_dirty(candidate_file)
