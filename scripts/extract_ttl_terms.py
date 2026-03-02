#!/usr/bin/env python3
"""Extract ontology terms and curation-relevant metadata from a Turtle file.

The script converts Turtle to N-Triples via `rapper`, then selects resources in a
namespace that are typed as class/property/concept-like terms.
"""

from __future__ import annotations

import argparse
import csv
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

LABEL_PREDICATE = "http://www.w3.org/2000/01/rdf-schema#label"
COMMENT_PREDICATE = "http://www.w3.org/2000/01/rdf-schema#comment"
DOMAIN_PREDICATE = "http://www.w3.org/2000/01/rdf-schema#domain"
RANGE_PREDICATE = "http://www.w3.org/2000/01/rdf-schema#range"
SUBCLASS_PREDICATE = "http://www.w3.org/2000/01/rdf-schema#subClassOf"
VANN_EXAMPLE_PREDICATE = "http://purl.org/vocab/vann/example"
SKOS_DEFINITION_PREDICATE = "http://www.w3.org/2004/02/skos/core#definition"
IAO_DEFINITION_PREDICATE = "http://purl.obolibrary.org/obo/IAO_0000115"
TYPE_PREDICATE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
KEEP_TYPES = {
    "http://www.w3.org/2002/07/owl#Class",
    "http://www.w3.org/2000/01/rdf-schema#Class",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property",
    "http://www.w3.org/2002/07/owl#ObjectProperty",
    "http://www.w3.org/2002/07/owl#DatatypeProperty",
    "http://www.w3.org/2002/07/owl#AnnotationProperty",
    "http://www.w3.org/2004/02/skos/core#Concept",
}

TRIPLE_RE = re.compile(r"^<([^>]*)> <([^>]*)> (.*) \.$")
IRI_OBJECT_RE = re.compile(r"^<([^>]*)>$")
LABEL_RE = re.compile(
    r'^"((?:[^"\\]|\\.)*)"(?:(?:@[A-Za-z0-9-]+)|(?:\^\^<[^>]+>))?$'
)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Extract terms from ontology Turtle")
    parser.add_argument("input_ttl", type=Path, help="Input Turtle file path")
    parser.add_argument(
        "namespace_prefix", help="Namespace prefix to keep, e.g. https://w3id.org/emi#"
    )
    parser.add_argument("output_tsv", type=Path, help="Output TSV path")
    return parser.parse_args()


def ensure_rapper_available() -> None:
    """Raise an error if rapper is missing from PATH."""
    if shutil.which("rapper") is None:
        raise SystemExit("rapper is required but not found in PATH")


def turtle_to_ntriples(input_ttl: Path) -> str:
    """Convert Turtle file to an N-Triples temporary file and return its path."""
    with tempfile.NamedTemporaryFile(prefix="ttl_terms_", suffix=".nt", delete=False) as tmp:
        tmp_path = tmp.name

    with open(tmp_path, "w", encoding="utf-8", newline="") as out:
        subprocess.run(
            ["rapper", "-q", "-i", "turtle", str(input_ttl), "-o", "ntriples"],
            check=True,
            stdout=out,
            stderr=subprocess.PIPE,
            text=True,
        )
    return tmp_path


def unescape_literal(value: str) -> str:
    """Unescape basic Turtle literal sequences used in labels."""
    return (
        value.replace(r"\\n", "\n")
        .replace(r"\\t", "\t")
        .replace(r"\\r", "\r")
        .replace(r'\"', '"')
        .replace(r"\\", "\\")
    )


def append_unique(mapping: dict[str, list[str]], key: str, value: str) -> None:
    """Append value to mapping[key] only when not empty and not duplicate."""
    if not value:
        return
    if key not in mapping:
        mapping[key] = [value]
        return
    if value not in mapping[key]:
        mapping[key].append(value)


def infer_label_from_iri(iri: str) -> str:
    """Infer fallback label from IRI tail."""
    trimmed = iri.rstrip("/#")
    if "#" in trimmed:
        return trimmed.rsplit("#", 1)[1]
    if "/" in trimmed:
        return trimmed.rsplit("/", 1)[1]
    return iri


def extract_terms(
    nt_path: Path, namespace_prefix: str
) -> list[tuple[str, str, str, str, str, str, str, str, str]]:
    """Extract sorted term rows from N-Triples with curation context fields."""
    labels: dict[str, str] = {}
    types: dict[str, str] = {}
    comments: dict[str, list[str]] = {}
    definitions: dict[str, list[str]] = {}
    examples: dict[str, list[str]] = {}
    domains: dict[str, list[str]] = {}
    ranges: dict[str, list[str]] = {}
    parents: dict[str, list[str]] = {}

    with nt_path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            line = line.rstrip("\n")
            match = TRIPLE_RE.match(line)
            if not match:
                continue

            subject, predicate, obj = match.group(1), match.group(2), match.group(3)
            if not subject.startswith(namespace_prefix):
                continue

            if predicate == LABEL_PREDICATE:
                label_match = LABEL_RE.match(obj)
                if label_match:
                    labels[subject] = unescape_literal(label_match.group(1))
            elif predicate == COMMENT_PREDICATE:
                comment_match = LABEL_RE.match(obj)
                if comment_match:
                    append_unique(comments, subject, unescape_literal(comment_match.group(1)))
            elif predicate in {SKOS_DEFINITION_PREDICATE, IAO_DEFINITION_PREDICATE}:
                def_match = LABEL_RE.match(obj)
                if def_match:
                    append_unique(
                        definitions, subject, unescape_literal(def_match.group(1))
                    )
            elif predicate == VANN_EXAMPLE_PREDICATE:
                example_match = LABEL_RE.match(obj)
                if example_match:
                    append_unique(examples, subject, unescape_literal(example_match.group(1)))
            elif predicate == DOMAIN_PREDICATE:
                domain_match = IRI_OBJECT_RE.match(obj)
                if domain_match:
                    append_unique(domains, subject, domain_match.group(1))
            elif predicate == RANGE_PREDICATE:
                range_match = IRI_OBJECT_RE.match(obj)
                if range_match:
                    append_unique(ranges, subject, range_match.group(1))
            elif predicate == SUBCLASS_PREDICATE:
                parent_match = IRI_OBJECT_RE.match(obj)
                if parent_match:
                    append_unique(parents, subject, parent_match.group(1))
            elif predicate == TYPE_PREDICATE:
                type_match = IRI_OBJECT_RE.match(obj)
                if type_match and type_match.group(1) in KEEP_TYPES:
                    types[subject] = type_match.group(1)

    rows: list[tuple[str, str, str, str, str, str, str, str, str]] = []
    for iri, term_type in types.items():
        label = labels.get(iri, "") or infer_label_from_iri(iri)
        rows.append(
            (
                iri,
                label,
                term_type,
                " | ".join(definitions.get(iri, [])),
                " | ".join(comments.get(iri, [])),
                " | ".join(examples.get(iri, [])),
                " | ".join(domains.get(iri, [])),
                " | ".join(ranges.get(iri, [])),
                " | ".join(parents.get(iri, [])),
            )
        )
    rows.sort(key=lambda item: item[0])
    return rows


def write_tsv(
    path: Path, rows: list[tuple[str, str, str, str, str, str, str, str, str]]
) -> None:
    """Write extracted term rows to TSV output."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t", lineterminator="\n")
        writer.writerow(
            [
                "iri",
                "label",
                "type",
                "definition",
                "comment",
                "example",
                "domain_iris",
                "range_iris",
                "parent_iris",
            ]
        )
        writer.writerows(rows)


def main() -> int:
    """Run term extraction CLI."""
    args = parse_args()

    if not args.input_ttl.is_file():
        print(f"Input TTL not found: {args.input_ttl}", file=sys.stderr)
        return 1

    ensure_rapper_available()

    nt_temp_path = Path(turtle_to_ntriples(args.input_ttl))
    try:
        rows = extract_terms(nt_temp_path, args.namespace_prefix)
    finally:
        nt_temp_path.unlink(missing_ok=True)

    write_tsv(args.output_tsv, rows)
    print(f"Extracted {len(rows)} term row(s) to {args.output_tsv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
