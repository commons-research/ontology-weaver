#!/usr/bin/env python3
"""Fetch OWL/RDFS/SKOS mapping relation terms into a local JSON catalog."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from rdflib import Graph, URIRef
from rdflib.namespace import OWL, RDF, RDFS, SKOS


CATALOG_URLS = {
    "owl": "https://www.w3.org/2002/07/owl#",
    "rdfs": "https://www.w3.org/2000/01/rdf-schema#",
    "skos": "https://www.w3.org/2004/02/skos/core#",
}


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


def build_catalog() -> dict:
    merged = Graph()
    for _, url in CATALOG_URLS.items():
        merged.parse(url)

    # SKOS mapping family: mappingRelation + all transitive subproperties.
    skos_mapping_root = SKOS.mappingRelation
    skos_candidates: set[URIRef] = {skos_mapping_root}
    changed = True
    while changed:
        changed = False
        for subj, _, obj in merged.triples((None, RDFS.subPropertyOf, None)):
            if isinstance(subj, URIRef) and isinstance(obj, URIRef) and obj in skos_candidates and subj not in skos_candidates:
                skos_candidates.add(subj)
                changed = True

    # OWL/RDFS terms relevant to mapping semantics.
    keyword_tokens = ("equivalent", "same as", "subclass", "subproperty", "see also", "mapping", "match")
    structural_candidates: set[URIRef] = set()
    prop_types = {RDF.Property, OWL.ObjectProperty, OWL.AnnotationProperty}
    for prop_type in prop_types:
        for subj in merged.subjects(RDF.type, prop_type):
            if not isinstance(subj, URIRef):
                continue
            iri = str(subj)
            if iri.startswith("http://www.w3.org/2002/07/owl#") or iri.startswith("http://www.w3.org/2000/01/rdf-schema#"):
                text_parts = [iri.lower()]
                text_parts.extend(str(x).lower() for x in merged.objects(subj, RDFS.label))
                text_parts.extend(str(x).lower() for x in merged.objects(subj, RDFS.comment))
                blob = " ".join(text_parts)
                if any(tok in blob for tok in keyword_tokens):
                    structural_candidates.add(subj)

    candidates = structural_candidates | skos_candidates
    ordered = sorted(
        candidates,
        key=lambda u: (
            0 if str(u).startswith("http://www.w3.org/2002/07/owl#")
            else 1 if str(u).startswith("http://www.w3.org/2000/01/rdf-schema#")
            else 2 if str(u).startswith("http://www.w3.org/2004/02/skos/core#")
            else 3,
            _curie_for_iri(str(u)).lower(),
        ),
    )

    rows: list[dict[str, str]] = []
    for iri_ref in ordered:
        iri = str(iri_ref)
        label = next((str(x).strip() for x in merged.objects(iri_ref, RDFS.label) if str(x).strip()), "")
        definition = next((str(x).strip() for x in merged.objects(iri_ref, RDFS.comment) if str(x).strip()), "")
        if not definition:
            definition = next((str(x).strip() for x in merged.objects(iri_ref, SKOS.definition) if str(x).strip()), "")
        rows.append(
            {
                "iri": iri,
                "curie": _curie_for_iri(iri),
                "label": label or _short_local_name(iri),
                "definition": definition,
            }
        )

    return {
        "catalog_version": "1",
        "sources": CATALOG_URLS,
        "relations": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch mapping relations catalog from OWL/RDFS/SKOS.")
    parser.add_argument(
        "--out",
        default="registry/mapping_relations_catalog.json",
        help="Output JSON path (default: registry/mapping_relations_catalog.json)",
    )
    args = parser.parse_args()

    payload = build_catalog()
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote {out} ({len(payload.get('relations', []))} relation term(s))")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

