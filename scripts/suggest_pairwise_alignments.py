#!/usr/bin/env python3
"""Generate pairwise alignment candidates.

Supported modes:
1) local-vs-local:
   Compare two local term TSV exports with fuzzy matching.
2) local-vs-ols:
   Compare local terms to top OLS API suggestions.

Output rows are written with ephemeral IDs (`CAND_XXXX`) and are intended for
manual curation before promotion into curated alignments (`ALIGN_XXXX`).
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import replace
import json
import re
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Iterable

OLS_API_SEARCH_URL = "https://www.ebi.ac.uk/ols4/api/search"
DEFAULT_ONTOLOGIES = ["chebi", "obi", "ms", "chmo", "edam"]
KIND_MISMATCH_SCORE_FACTOR = 0.75


@dataclass(frozen=True)
class Term:
    """Representation of a term row from local ontology exports."""

    iri: str
    label: str
    term_type: str
    term_kind: str
    normalized_label: str
    token_set: frozenset[str]
    definition: str
    comment: str
    example: str


@dataclass(frozen=True)
class OlsSuggestion:
    """One candidate term suggestion returned from OLS API."""

    iri: str
    label: str
    ontology: str
    ontology_label: str
    entity_kind: str
    score: float
    definition: str
    comment: str
    example: str
    term_api_url: str


@dataclass(frozen=True)
class PairSuggestion:
    """Pairwise suggestion between one left term and one right term."""

    left: Term
    right_source: str
    right_term_kind: str
    right_term_iri: str
    right_label: str
    right_definition: str
    right_comment: str
    right_example: str
    ols_term_api_url: str
    normalized_right_label: str
    match_method: str
    match_score: float
    relation: str
    suggestion_source: str
    notes: str


@dataclass(frozen=True)
class Config:
    """Runtime configuration for candidate generation."""

    left_terms: Path
    left_source: str
    right_terms: Path | None
    right_source: str
    use_ols_api: bool
    curated_alignments: Path
    output: Path
    min_score: float
    focus: str
    ontologies: list[str]
    ols_rows: int
    ols_fetch_metadata: bool
    request_timeout: float
    top_n_ols: int
    max_left_terms: int
    curator: str
    include_existing_curated: bool


def parse_args() -> Config:
    """Parse CLI args into config."""
    parser = argparse.ArgumentParser(description="Suggest pairwise alignment candidates")
    parser.add_argument(
        "--left-terms",
        type=Path,
        required=True,
        help="Path to left-side terms TSV (iri,label,type)",
    )
    parser.add_argument(
        "--left-source",
        required=True,
        help="Left source name (e.g., EMI, ENPKG)",
    )

    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        "--right-terms",
        type=Path,
        help="Path to right-side terms TSV for local-vs-local mode",
    )
    mode_group.add_argument(
        "--use-ols-api",
        action="store_true",
        help="Use OLS API as right side for local-vs-ols mode",
    )

    parser.add_argument(
        "--right-source",
        default="",
        help="Right source name for local-vs-local mode (e.g., ENPKG)",
    )
    parser.add_argument(
        "--curated-alignments",
        type=Path,
        default=Path("registry/pair_alignments.tsv"),
        help="Path to curated pair alignments TSV for duplicate exclusion",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("registry/pair_alignment_candidates.tsv"),
        help="Output TSV for candidate pair alignments",
    )
    parser.add_argument(
        "--min-score",
        type=float,
        default=0.82,
        help="Minimum fuzzy score for local-vs-local candidates (0-1)",
    )
    parser.add_argument(
        "--focus",
        default="",
        help="Optional normalized label filter (e.g., 'chemical entity')",
    )
    parser.add_argument(
        "--ontologies",
        default=",".join(DEFAULT_ONTOLOGIES),
        help="Comma-separated ontology prefixes for OLS mode",
    )
    parser.add_argument(
        "--ols-rows",
        type=int,
        default=5,
        help="Rows requested from OLS API per ontology",
    )
    parser.add_argument(
        "--ols-fetch-metadata",
        action="store_true",
        help="Fetch definition/comment/example for top OLS suggestion (slower)",
    )
    parser.add_argument(
        "--request-timeout",
        type=float,
        default=3.0,
        help="Per-request timeout in seconds for OLS API calls (default: 3.0)",
    )
    parser.add_argument(
        "--top-n-ols",
        type=int,
        default=1,
        help="Number of top OLS hits to keep per left term in local-vs-OLS mode",
    )
    parser.add_argument(
        "--max-left-terms",
        type=int,
        default=0,
        help="Optional limit on number of left-side terms to process (0 = no limit)",
    )
    parser.add_argument("--curator", default="auto", help="Curator value in output rows")
    parser.add_argument(
        "--include-existing-curated",
        action="store_true",
        help="Do not exclude pairs already present in curated alignments",
    )

    args = parser.parse_args()

    if not (0.0 <= args.min_score <= 1.0):
        raise SystemExit("--min-score must be between 0 and 1")
    if args.ols_rows <= 0:
        raise SystemExit("--ols-rows must be > 0")
    if args.request_timeout <= 0:
        raise SystemExit("--request-timeout must be > 0")
    if args.top_n_ols <= 0:
        raise SystemExit("--top-n-ols must be > 0")
    if args.max_left_terms < 0:
        raise SystemExit("--max-left-terms must be >= 0")

    if args.right_terms and not args.right_source.strip():
        raise SystemExit("--right-source is required when --right-terms is used")

    ontologies = [o.strip().lower() for o in args.ontologies.split(",") if o.strip()]
    if not ontologies:
        ontologies = DEFAULT_ONTOLOGIES.copy()

    return Config(
        left_terms=args.left_terms,
        left_source=args.left_source.strip(),
        right_terms=args.right_terms,
        right_source=args.right_source.strip(),
        use_ols_api=bool(args.use_ols_api),
        curated_alignments=args.curated_alignments,
        output=args.output,
        min_score=args.min_score,
        focus=normalize_label(args.focus),
        ontologies=ontologies,
        ols_rows=args.ols_rows,
        ols_fetch_metadata=bool(args.ols_fetch_metadata),
        request_timeout=args.request_timeout,
        top_n_ols=args.top_n_ols,
        max_left_terms=args.max_left_terms,
        curator=args.curator.strip(),
        include_existing_curated=bool(args.include_existing_curated),
    )


def ols_search_url(query: str) -> str:
    """Build OLS web search URL for manual review."""
    return "https://www.ebi.ac.uk/ols4/search?q=" + urllib.parse.quote_plus(query)


def bioportal_search_url(query: str) -> str:
    """Build BioPortal search URL for manual review."""
    return "https://bioportal.bioontology.org/search?query=" + urllib.parse.quote_plus(query)


def utc_now_timestamp() -> str:
    """Return current UTC timestamp in compact ISO-8601 form."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_label(text: str) -> str:
    """Normalize label text for robust lexical matching."""
    if text is None:
        return ""
    value = text.strip()
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", value)
    value = re.sub(r"\([^)]*\)", " ", value)
    value = value.replace("_", " ").replace("-", " ")
    value = value.lower()
    value = re.sub(r"^(a|an|the)\s+", "", value)
    value = re.sub(r"[^a-z0-9\s]", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def infer_label_from_iri(iri: str) -> str:
    """Infer a readable label from IRI tail when no label is present."""
    trimmed = iri.rstrip("/#")
    if "#" in trimmed:
        return trimmed.rsplit("#", 1)[1]
    if "/" in trimmed:
        return trimmed.rsplit("/", 1)[1]
    return iri


def tokenize(normalized_label: str) -> frozenset[str]:
    """Tokenize normalized label to token set for Jaccard similarity."""
    if not normalized_label:
        return frozenset()
    return frozenset(part for part in normalized_label.split() if part)


def infer_term_kind_from_type(term_type: str) -> str:
    value = (term_type or "").strip()
    if not value:
        return ""
    if value.endswith("#Class") or value.endswith("/Class") or value.endswith("#Concept"):
        return "class"
    if "Property" in value or value.endswith("#Property") or value.endswith("/Property"):
        return "property"
    if value.endswith("#NamedIndividual") or value.endswith("/NamedIndividual"):
        return "individual"
    return ""


def infer_ols_entity_kind(doc: dict[str, object]) -> str:
    fields = [
        str(doc.get("entity_type", "") or ""),
        str(doc.get("type", "") or ""),
        str(doc.get("semantic_type", "") or ""),
    ]
    text = " ".join(fields).lower()
    if "property" in text:
        return "property"
    if "individual" in text:
        return "individual"
    if "class" in text or "concept" in text:
        return "class"
    return ""


def load_terms(path: Path) -> list[Term]:
    """Load terms from TSV with columns iri,label,type."""
    if not path.is_file():
        raise SystemExit(f"File not found: {path}")

    rows: list[Term] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for row in reader:
            iri = (row.get("iri", "") or "").strip()
            if not iri:
                continue
            raw_label = (row.get("label", "") or "").strip() or infer_label_from_iri(iri)
            normalized = normalize_label(raw_label)
            rows.append(
                Term(
                    iri=iri,
                    label=raw_label,
                    term_type=(row.get("type", "") or "").strip(),
                    term_kind=(
                        (row.get("term_kind", "") or "").strip().lower()
                        or infer_term_kind_from_type((row.get("type", "") or "").strip())
                    ),
                    normalized_label=normalized,
                    token_set=tokenize(normalized),
                    definition=(row.get("definition", "") or "").strip(),
                    comment=(row.get("comment", "") or "").strip(),
                    example=(row.get("example", "") or "").strip(),
                )
            )
    return rows


def jaccard_score(a: frozenset[str], b: frozenset[str]) -> float:
    """Return Jaccard similarity for two token sets."""
    if not a and not b:
        return 0.0
    union = a | b
    if not union:
        return 0.0
    return len(a & b) / len(union)


def local_match(left: Term, right: Term) -> tuple[str, float]:
    """Compute lexical match method and score for one local term pair."""
    if left.normalized_label == right.normalized_label and left.normalized_label:
        return "exact_normalized", 1.0

    fuzzy = SequenceMatcher(None, left.normalized_label, right.normalized_label).ratio()
    jac = jaccard_score(left.token_set, right.token_set)
    score = max(fuzzy, jac)
    method = "token_jaccard" if jac >= fuzzy and jac >= 0.5 else "fuzzy_ratio"
    return method, score


def apply_kind_penalty(score: float, left_kind: str, right_kind: str) -> float:
    """Penalize score when both kinds are known and mismatch."""
    lk = (left_kind or "").strip().lower()
    rk = (right_kind or "").strip().lower()
    if lk and rk and lk != rk:
        return score * KIND_MISMATCH_SCORE_FACTOR
    return score


def relation_from_score(method: str, score: float) -> str:
    """Map lexical similarity to a default semantic relation."""
    if method == "exact_normalized" or score >= 0.99:
        return "exact"
    if score >= 0.85:
        return "close"
    return "related"


def build_local_local_candidates(
    left_terms: Iterable[Term],
    right_terms: Iterable[Term],
    min_score: float,
    focus: str,
) -> list[PairSuggestion]:
    """Build one-to-one local-vs-local candidates using greedy best scoring."""
    scored: list[tuple[Term, Term, str, float]] = []

    for left in left_terms:
        if focus and focus not in left.normalized_label:
            continue
        for right in right_terms:
            if focus and focus not in right.normalized_label:
                continue
            method, score = local_match(left, right)
            score = apply_kind_penalty(score, left.term_kind, right.term_kind)
            if score >= min_score:
                scored.append((left, right, method, score))

    scored.sort(key=lambda item: item[3], reverse=True)

    used_left: set[str] = set()
    used_right: set[str] = set()
    candidates: list[PairSuggestion] = []
    for left, right, method, score in scored:
        if left.iri in used_left or right.iri in used_right:
            continue
        used_left.add(left.iri)
        used_right.add(right.iri)
        candidates.append(
            PairSuggestion(
                left=left,
                right_source="",
                right_term_kind=right.term_kind,
                right_term_iri=right.iri,
                right_label=right.label,
                right_definition=(right.definition if hasattr(right, "definition") else ""),
                right_comment=(right.comment if hasattr(right, "comment") else ""),
                right_example=(right.example if hasattr(right, "example") else ""),
                ols_term_api_url="",
                normalized_right_label=right.normalized_label,
                match_method=method,
                match_score=score,
                relation=relation_from_score(method, score),
                suggestion_source="local_fuzzy",
                notes="Auto-suggested local pair; review definition/scope before approval",
            )
        )
    return candidates


def query_ols_suggestions(
    query: str,
    ontologies: list[str],
    rows_per_ontology: int,
    request_timeout: float,
    fetch_metadata: bool,
    metadata_limit: int = 1,
) -> list[OlsSuggestion]:
    """Query OLS API and return deduplicated best suggestions ranked by label similarity.

    Network/API failures are tolerated and produce an empty result.
    """
    normalized_query = normalize_label(query)
    by_iri: dict[str, OlsSuggestion] = {}

    for ontology in ontologies:
        params = urllib.parse.urlencode(
            {"q": query, "ontology": ontology, "rows": rows_per_ontology}
        )
        url = f"{OLS_API_SEARCH_URL}?{params}"
        try:
            with urllib.request.urlopen(url, timeout=request_timeout) as response:
                payload = json.loads(response.read().decode("utf-8", errors="replace"))
        except Exception:
            continue

        docs = payload.get("response", {}).get("docs", [])
        for doc in docs:
            iri = str(doc.get("iri", "") or "").strip()
            label = str(doc.get("label", "") or "").strip()
            # OLS term endpoints require ontology identifier (prefix), not display name.
            onto_prefix = str(doc.get("ontology_prefix", "") or ontology).strip().lower()
            onto_label = str(doc.get("ontology_name", "") or onto_prefix).strip()
            if not iri:
                continue

            score = SequenceMatcher(None, normalized_query, normalize_label(label)).ratio()
            if normalize_label(label) == normalized_query:
                score = 1.0

            candidate = OlsSuggestion(
                iri=iri,
                label=label,
                ontology=onto_prefix,
                ontology_label=onto_label,
                entity_kind=infer_ols_entity_kind(doc),
                score=score,
                definition="",
                comment="",
                example="",
                term_api_url="",
            )
            existing = by_iri.get(iri)
            if existing is None or candidate.score > existing.score:
                by_iri[iri] = candidate

    ranked = sorted(by_iri.values(), key=lambda item: item.score, reverse=True)
    if fetch_metadata and ranked:
        limit = min(max(metadata_limit, 0), len(ranked))
        if limit == 0:
            return ranked
        enriched: list[OlsSuggestion] = []
        for idx, item in enumerate(ranked):
            if idx < limit:
                metadata = fetch_ols_term_metadata(
                    ontology=item.ontology,
                    iri=item.iri,
                    request_timeout=request_timeout,
                )
                enriched.append(
                    replace(
                        item,
                        definition=metadata["definition"],
                        comment=metadata["comment"],
                        example=metadata["example"],
                        term_api_url=metadata["term_api_url"],
                    )
                )
            else:
                enriched.append(item)
        ranked = enriched
    return ranked


def build_local_ols_candidates(
    left_terms: Iterable[Term],
    focus: str,
    ontologies: list[str],
    rows_per_ontology: int,
    request_timeout: float,
    fetch_metadata: bool,
    top_n_ols: int,
) -> list[PairSuggestion]:
    """Build local-vs-OLS candidates using top-N OLS suggestions per local term."""
    candidates: list[PairSuggestion] = []

    for left in left_terms:
        if focus and focus not in left.normalized_label:
            continue

        suggestions = query_ols_suggestions(
            query=left.label,
            ontologies=ontologies,
            rows_per_ontology=rows_per_ontology,
            request_timeout=request_timeout,
            fetch_metadata=fetch_metadata,
            metadata_limit=top_n_ols,
        )
        if not suggestions:
            continue
        for rank, hit in enumerate(suggestions[:top_n_ols], start=1):
            right_normalized = normalize_label(hit.label)
            method = "ols_api_top"
            score = apply_kind_penalty(hit.score, left.term_kind, hit.entity_kind)
            relation = relation_from_score(method, score)

            candidates.append(
                PairSuggestion(
                    left=left,
                    right_source=hit.ontology,
                    right_term_kind=hit.entity_kind,
                    right_term_iri=hit.iri,
                    right_label=hit.label,
                    right_definition=hit.definition,
                    right_comment=hit.comment,
                    right_example=hit.example,
                    ols_term_api_url=hit.term_api_url,
                    normalized_right_label=right_normalized,
                    match_method=method,
                    match_score=score,
                    relation=relation,
                    suggestion_source="ols_api",
                    notes=(
                        f"Auto-suggested OLS pair (rank {rank}/{min(top_n_ols, len(suggestions))}); "
                        "review definition/scope before approval"
                    ),
                )
            )

    return candidates


def probe_ols_api(ontologies: list[str], request_timeout: float) -> bool:
    """Return True if OLS API is reachable with a quick probe request."""
    if not ontologies:
        return False
    params = urllib.parse.urlencode(
        {"q": "chemical entity", "ontology": ontologies[0], "rows": 1}
    )
    url = f"{OLS_API_SEARCH_URL}?{params}"
    try:
        with urllib.request.urlopen(url, timeout=request_timeout) as response:
            _ = response.read(256)
    except Exception:
        return False
    return True


def first_text(value: object) -> str:
    """Return a readable text value from string/list values."""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        for item in value:
            if isinstance(item, str) and item.strip():
                return item.strip()
    if isinstance(value, dict):
        for subvalue in value.values():
            text = first_text(subvalue)
            if text:
                return text
    return ""


def extract_annotation_value(annotations: object, keys: list[str]) -> str:
    """Return first annotation value for one of the requested keys."""
    if not isinstance(annotations, dict):
        return ""
    for key in keys:
        if key in annotations:
            text = first_text(annotations.get(key))
            if text:
                return text
    return ""


def fetch_ols_term_metadata(
    ontology: str, iri: str, request_timeout: float
) -> dict[str, str]:
    """Fetch term metadata from OLS term endpoint.

    Returns empty strings when term metadata is missing or request fails.
    """
    iri_encoded = urllib.parse.quote(iri, safe="")
    ontology_encoded = urllib.parse.quote(ontology, safe="")
    terms_direct_url = (
        f"https://www.ebi.ac.uk/ols4/api/ontologies/{ontology_encoded}/terms/{iri_encoded}"
    )
    terms_fallback_url = (
        f"https://www.ebi.ac.uk/ols4/api/ontologies/{ontology_encoded}/terms?iri={iri_encoded}"
    )
    props_direct_url = (
        f"https://www.ebi.ac.uk/ols4/api/ontologies/{ontology_encoded}/properties/{iri_encoded}"
    )
    props_fallback_url = (
        f"https://www.ebi.ac.uk/ols4/api/ontologies/{ontology_encoded}/properties?iri={iri_encoded}"
    )

    payload: dict[str, object] | None = None
    used_url = terms_direct_url

    def fetch_direct(url: str) -> dict[str, object] | None:
        try:
            with urllib.request.urlopen(url, timeout=request_timeout) as response:
                data = json.loads(response.read().decode("utf-8", errors="replace"))
            if isinstance(data, dict):
                return data
        except Exception:
            return None
        return None

    def fetch_embedded_first(url: str) -> dict[str, object] | None:
        try:
            with urllib.request.urlopen(url, timeout=request_timeout) as response:
                wrapper = json.loads(response.read().decode("utf-8", errors="replace"))
            if not isinstance(wrapper, dict):
                return None
            embedded = wrapper.get("_embedded", {})
            if not isinstance(embedded, dict):
                return None
            for key in ("terms", "properties"):
                entries = embedded.get(key, [])
                if isinstance(entries, list) and entries:
                    first = entries[0]
                    if isinstance(first, dict):
                        return first
        except Exception:
            return None
        return None

    for url in (
        terms_direct_url,
        terms_fallback_url,
        props_direct_url,
        props_fallback_url,
    ):
        if url.endswith(f"/terms/{iri_encoded}") or url.endswith(f"/properties/{iri_encoded}"):
            payload = fetch_direct(url)
        else:
            payload = fetch_embedded_first(url)
        if payload is not None:
            used_url = url
            break

    if payload is None:
        return {
            "definition": "",
            "comment": "",
            "example": "",
            "term_api_url": terms_direct_url,
        }

    annotations = payload.get("annotation", payload.get("annotations", {}))
    definition = (
        first_text(payload.get("description"))
        or first_text(payload.get("definition"))
        or extract_annotation_value(
            annotations,
            [
                "http://www.w3.org/2004/02/skos/core#definition",
                "http://purl.obolibrary.org/obo/IAO_0000115",
                "definition",
            ],
        )
    )
    comment = (
        first_text(payload.get("comment"))
        or extract_annotation_value(
            annotations,
            [
                "http://www.w3.org/2000/01/rdf-schema#comment",
                "comment",
            ],
        )
    )
    example = (
        first_text(payload.get("example"))
        or extract_annotation_value(
            annotations,
            [
                "http://purl.org/vocab/vann/example",
                "http://www.w3.org/2004/02/skos/core#example",
                "example",
            ],
        )
    )
    return {
        "definition": definition,
        "comment": comment,
        "example": example,
        "term_api_url": used_url,
    }


def pair_key(
    left_source: str,
    left_iri: str,
    right_source: str,
    right_iri: str,
) -> tuple[tuple[str, str], tuple[str, str]]:
    """Return order-independent key for one pairwise alignment row."""
    a = (left_source.strip().lower(), left_iri.strip())
    b = (right_source.strip().lower(), right_iri.strip())
    return tuple(sorted((a, b)))


def load_curated_pair_keys(path: Path) -> set[tuple[tuple[str, str], tuple[str, str]]]:
    """Load existing curated pair keys to prevent duplicate suggestions."""
    if not path.is_file():
        return set()

    keys: set[tuple[tuple[str, str], tuple[str, str]]] = set()
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for row in reader:
            left_source = (row.get("left_source", "") or "").strip()
            left_iri = (row.get("left_term_iri", "") or "").strip()
            right_source = (row.get("right_source", "") or "").strip()
            right_iri = (row.get("right_term_iri", "") or "").strip()
            if left_source and left_iri and right_source and right_iri:
                keys.add(pair_key(left_source, left_iri, right_source, right_iri))
    return keys


def filter_existing_curated_pairs(
    suggestions: list[PairSuggestion],
    left_source: str,
    right_source: str,
    curated_keys: set[tuple[tuple[str, str], tuple[str, str]]],
) -> tuple[list[PairSuggestion], int]:
    """Filter suggestions that are already in curated alignments."""
    kept: list[PairSuggestion] = []
    excluded = 0
    for item in suggestions:
        current_key = pair_key(
            left_source,
            item.left.iri,
            item.right_source or right_source,
            item.right_term_iri,
        )
        if current_key in curated_keys:
            excluded += 1
            continue
        kept.append(item)
    return kept, excluded


def write_candidate_rows(
    output: Path,
    suggestions: list[PairSuggestion],
    left_source: str,
    fallback_right_source: str,
    curator: str,
) -> None:
    """Write pairwise suggestions to candidate TSV output."""
    output.parent.mkdir(parents=True, exist_ok=True)
    headers = [
        "alignment_id",
        "left_source",
        "left_term_kind",
        "left_term_iri",
        "left_label",
        "left_definition",
        "left_comment",
        "left_example",
        "right_source",
        "right_term_kind",
        "right_term_iri",
        "right_label",
        "right_definition",
        "right_comment",
        "right_example",
        "ols_term_api_url",
        "normalized_left_label",
        "normalized_right_label",
        "match_method",
        "match_score",
        "relation",
        "suggestion_source",
        "canonical_from",
        "canonical_term_iri",
        "canonical_term_label",
        "canonical_term_source",
        "ols_search_url",
        "bioportal_search_url",
        "status",
        "curator",
        "reviewer",
        "date_added",
        "date_reviewed",
        "notes",
    ]

    now_ts = utc_now_timestamp()
    left_source_norm = left_source.strip().upper()
    fallback_right_source_norm = fallback_right_source.strip().upper()
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=headers,
            delimiter="\t",
            lineterminator="\n",
        )
        writer.writeheader()
        for idx, item in enumerate(suggestions, start=1):
            current_right_source = (item.right_source or fallback_right_source).strip().upper()
            canonical_from = "right" if item.suggestion_source == "ols_api" else ""
            canonical_term_iri = item.right_term_iri if canonical_from == "right" else ""
            canonical_term_label = item.right_label if canonical_from == "right" else ""
            canonical_term_source = current_right_source if canonical_from == "right" else ""
            writer.writerow(
                {
                    "alignment_id": f"CAND_{idx:04d}",
                    "left_source": left_source_norm,
                    "left_term_kind": item.left.term_kind,
                    "left_term_iri": item.left.iri,
                    "left_label": item.left.label,
                    "left_definition": item.left.definition,
                    "left_comment": item.left.comment,
                    "left_example": item.left.example,
                    "right_source": current_right_source or fallback_right_source_norm,
                    "right_term_kind": item.right_term_kind,
                    "right_term_iri": item.right_term_iri,
                    "right_label": item.right_label,
                    "right_definition": item.right_definition,
                    "right_comment": item.right_comment,
                    "right_example": item.right_example,
                    "ols_term_api_url": item.ols_term_api_url,
                    "normalized_left_label": item.left.normalized_label,
                    "normalized_right_label": item.normalized_right_label,
                    "match_method": item.match_method,
                    "match_score": f"{item.match_score:.2f}",
                    "relation": item.relation,
                    "suggestion_source": item.suggestion_source,
                    "canonical_from": canonical_from,
                    "canonical_term_iri": canonical_term_iri,
                    "canonical_term_label": canonical_term_label,
                    "canonical_term_source": canonical_term_source,
                    "ols_search_url": ols_search_url(item.left.label),
                    "bioportal_search_url": bioportal_search_url(item.left.label),
                    "status": "needs_review",
                    "curator": curator,
                    "reviewer": "",
                    "date_added": now_ts,
                    "date_reviewed": "",
                    "notes": item.notes,
                }
            )


def main() -> int:
    """Run CLI pairwise candidate generation."""
    config = parse_args()
    left_terms = load_terms(config.left_terms)
    if config.max_left_terms > 0:
        left_terms = left_terms[: config.max_left_terms]

    if config.use_ols_api:
        if not probe_ols_api(config.ontologies, config.request_timeout):
            suggestions = []
            print(
                "Warning: OLS API probe failed. "
                "Wrote empty candidate set quickly (check network or increase --request-timeout)."
            )
        else:
            suggestions = build_local_ols_candidates(
                left_terms=left_terms,
                focus=config.focus,
                ontologies=config.ontologies,
                rows_per_ontology=config.ols_rows,
                request_timeout=config.request_timeout,
                fetch_metadata=config.ols_fetch_metadata,
                top_n_ols=config.top_n_ols,
            )
        resolved_right_source = "ols"
    else:
        assert config.right_terms is not None
        right_terms = load_terms(config.right_terms)
        suggestions = build_local_local_candidates(
            left_terms=left_terms,
            right_terms=right_terms,
            min_score=config.min_score,
            focus=config.focus,
        )
        # Inject the configured right source for local-vs-local mode.
        suggestions = [
            PairSuggestion(
                left=item.left,
                right_source=config.right_source,
                right_term_kind=item.right_term_kind,
                right_term_iri=item.right_term_iri,
                right_label=item.right_label,
                right_definition=item.right_definition,
                right_comment=item.right_comment,
                right_example=item.right_example,
                ols_term_api_url=item.ols_term_api_url,
                normalized_right_label=item.normalized_right_label,
                match_method=item.match_method,
                match_score=item.match_score,
                relation=item.relation,
                suggestion_source=item.suggestion_source,
                notes=item.notes,
            )
            for item in suggestions
        ]
        resolved_right_source = config.right_source

    excluded_existing = 0
    if not config.include_existing_curated:
        curated_keys = load_curated_pair_keys(config.curated_alignments)
        suggestions, excluded_existing = filter_existing_curated_pairs(
            suggestions=suggestions,
            left_source=config.left_source,
            right_source=resolved_right_source,
            curated_keys=curated_keys,
        )

    write_candidate_rows(
        output=config.output,
        suggestions=suggestions,
        left_source=config.left_source,
        fallback_right_source=resolved_right_source,
        curator=config.curator,
    )

    print(f"Wrote {len(suggestions)} candidate pair row(s) to {config.output}")
    if not config.include_existing_curated:
        print(
            f"Excluded {excluded_existing} pair(s) already present in {config.curated_alignments}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
