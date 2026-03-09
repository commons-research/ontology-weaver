#!/usr/bin/env python3
"""Export canonical-centric reconciled mappings from curated pair alignments.

Main output is source-term-to-canonical mappings (no left/right columns), so it
is directly usable as "use canonical term X for source term Y".
Optionally also writes a grouped canonical summary with associated terms.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Export canonical-centric reconciled mappings")
    parser.add_argument(
        "--alignments",
        type=Path,
        default=Path("registry/pair_alignment_candidates.tsv"),
        help="Path to reviewed candidate TSV (approved rows are exported)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("registry/reconciled_mappings.tsv"),
        help="Output TSV path for source->canonical mappings",
    )
    parser.add_argument(
        "--grouped-output",
        type=Path,
        default=Path("registry/reconciled_canonical_groups.tsv"),
        help="Output TSV path for canonical term groups",
    )
    parser.add_argument(
        "--status",
        default="approved",
        help="Export rows with this status only (default: approved)",
    )
    return parser.parse_args()


def read_rows(path: Path) -> list[dict[str, str]]:
    """Read TSV into list of row dictionaries."""
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        return list(reader)


def clean(value: str) -> str:
    """Return stripped string with None-safe fallback."""
    return (value or "").strip()


def stable_alignment_id(row: dict[str, str]) -> str:
    """Return queue alignment_id or derive a stable internal ID for shared-ledger rows."""
    explicit = clean(row.get("alignment_id", ""))
    if explicit:
        return explicit
    source_term_iri = clean(row.get("source_term_iri", "")) or clean(row.get("left_term_iri", ""))
    if source_term_iri:
        return f"LEDGER::{source_term_iri}"
    return ""


def append_source_mapping(
    out_rows: list[dict[str, str]],
    seen_keys: set[tuple[str, str]],
    *,
    alignment_id: str,
    source_term_source: str,
    source_term_iri: str,
    source_term_label: str,
    canonical_term_iri: str,
    canonical_term_label: str,
    canonical_term_source: str,
    relation: str,
    suggestion_source: str,
    curator: str,
    reviewer: str,
    date_added: str,
    date_reviewed: str,
    notes: str,
) -> None:
    """Append one source->canonical mapping row if not duplicate and not self-mapping."""
    if not source_term_iri:
        return
    if source_term_iri == canonical_term_iri:
        return

    dedupe_key = (source_term_iri, canonical_term_iri)
    if dedupe_key in seen_keys:
        return
    seen_keys.add(dedupe_key)

    out_rows.append(
        {
            "mapping_id": f"REC_{len(out_rows) + 1:04d}",
            "alignment_id": alignment_id,
            "source_term_source": source_term_source,
            "source_term_iri": source_term_iri,
            "source_term_label": source_term_label,
            "canonical_term_iri": canonical_term_iri,
            "canonical_term_label": canonical_term_label,
            "canonical_term_source": canonical_term_source,
            "relation": relation,
            "mapping_status": "approved",
            "suggestion_source": suggestion_source,
            "curator": curator,
            "reviewer": reviewer,
            "date_added": date_added,
            "date_reviewed": date_reviewed,
            "notes": notes,
        }
    )


def build_source_to_canonical_rows(
    alignment_rows: list[dict[str, str]],
    status_filter: str,
) -> list[dict[str, str]]:
    """Build canonical-centric source->canonical mapping rows."""
    out_rows: list[dict[str, str]] = []
    seen_keys: set[tuple[str, str]] = set()

    for row in alignment_rows:
        status = clean(row.get("status", "")).lower()
        if status != status_filter.lower():
            continue

        alignment_id = stable_alignment_id(row)
        relation = clean(row.get("relation", ""))
        suggestion_source = clean(row.get("suggestion_source", "")) or "approved_ledger"
        curator = clean(row.get("curator", ""))
        reviewer = clean(row.get("reviewer", ""))
        date_reviewed = clean(row.get("date_reviewed", ""))
        date_added = clean(row.get("date_added", "")) or date_reviewed
        notes = clean(row.get("notes", "")) or clean(row.get("curation_comment", ""))

        left_source = clean(row.get("source_term_source", "")) or clean(row.get("left_source", ""))
        left_iri = clean(row.get("source_term_iri", "")) or clean(row.get("left_term_iri", ""))
        left_label = clean(row.get("source_term_label", "")) or clean(row.get("left_label", ""))
        canonical_iri = clean(row.get("canonical_term_iri", ""))
        canonical_label = clean(row.get("canonical_term_label", ""))
        canonical_source = clean(row.get("canonical_term_source", ""))
        if not (canonical_iri and canonical_label and canonical_source):
            continue

        append_source_mapping(
            out_rows,
            seen_keys,
            alignment_id=alignment_id,
            source_term_source=left_source,
            source_term_iri=left_iri,
            source_term_label=left_label,
            canonical_term_iri=canonical_iri,
            canonical_term_label=canonical_label,
            canonical_term_source=canonical_source,
            relation=relation,
            suggestion_source=suggestion_source,
            curator=curator,
            reviewer=reviewer,
            date_added=date_added,
            date_reviewed=date_reviewed,
            notes=notes,
        )

    return out_rows


def write_tsv(path: Path, headers: list[str], rows: list[dict[str, str]]) -> None:
    """Write rows to TSV with explicit header ordering."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=headers,
            delimiter="\t",
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)


def build_group_rows(mapping_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Group source->canonical mappings by canonical term."""
    groups: dict[tuple[str, str, str], list[dict[str, str]]] = {}
    for row in mapping_rows:
        key = (
            clean(row.get("canonical_term_iri", "")),
            clean(row.get("canonical_term_label", "")),
            clean(row.get("canonical_term_source", "")),
        )
        groups.setdefault(key, []).append(row)

    out: list[dict[str, str]] = []
    for key in sorted(groups.keys()):
        canonical_iri, canonical_label, canonical_source = key
        group_rows = groups[key]
        source_pairs = sorted(
            {
                (
                    clean(item.get("source_term_source", "")),
                    clean(item.get("source_term_iri", "")),
                    clean(item.get("source_term_label", "")),
                )
                for item in group_rows
            }
        )
        out.append(
            {
                "canonical_term_iri": canonical_iri,
                "canonical_term_label": canonical_label,
                "canonical_term_source": canonical_source,
                "mapped_term_count": str(len(source_pairs)),
                "mapped_terms": " | ".join(
                    f"{src}:{iri}:{label}" for src, iri, label in source_pairs
                ),
                "alignment_ids": " | ".join(
                    sorted({clean(item.get("alignment_id", "")) for item in group_rows})
                ),
            }
        )
    return out


def export_mappings(
    input_path: Path,
    output_path: Path,
    grouped_output_path: Path,
    status_filter: str,
) -> int:
    """Export canonical-centric mappings and grouped canonical summary."""
    if not input_path.is_file():
        print(f"File not found: {input_path}")
        return 1

    alignment_rows = read_rows(input_path)
    mapping_rows = build_source_to_canonical_rows(alignment_rows, status_filter=status_filter)
    group_rows = build_group_rows(mapping_rows)

    mapping_headers = [
        "mapping_id",
        "alignment_id",
        "source_term_source",
        "source_term_iri",
        "source_term_label",
        "canonical_term_iri",
        "canonical_term_label",
        "canonical_term_source",
        "relation",
        "mapping_status",
        "suggestion_source",
        "curator",
        "reviewer",
        "date_added",
        "date_reviewed",
        "notes",
    ]
    write_tsv(output_path, mapping_headers, mapping_rows)

    grouped_headers = [
        "canonical_term_iri",
        "canonical_term_label",
        "canonical_term_source",
        "mapped_term_count",
        "mapped_terms",
        "alignment_ids",
    ]
    write_tsv(grouped_output_path, grouped_headers, group_rows)

    print(
        f"Exported {len(mapping_rows)} source->canonical mapping row(s) to {output_path}"
    )
    print(
        f"Exported {len(group_rows)} canonical group row(s) to {grouped_output_path}"
    )
    return 0


def main() -> int:
    """Run CLI export command."""
    args = parse_args()
    return export_mappings(
        input_path=args.alignments,
        output_path=args.output,
        grouped_output_path=args.grouped_output,
        status_filter=args.status,
    )


if __name__ == "__main__":
    raise SystemExit(main())
