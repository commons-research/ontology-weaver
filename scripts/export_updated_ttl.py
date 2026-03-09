#!/usr/bin/env python3
"""Build enriched source TTL and mapping TTL from the shared approved ledger."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
from rdflib import Graph

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from curation_app.pages.finalize_validate import (
    _apply_iri_and_qname_replacements,
    _build_mapping_triples,
    _build_replacements,
    _compact_ttl_iris_with_prefixes,
    _ensure_columns,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export enriched source TTL and mapping TTL from a shared approved ledger."
    )
    parser.add_argument("--ledger", type=Path, required=True, help="Shared approved-ledger TSV path.")
    parser.add_argument("--source-ttl", type=Path, required=True, help="Downloaded source TTL path.")
    parser.add_argument("--output", type=Path, required=True, help="Output path for enriched TTL.")
    parser.add_argument(
        "--mapping-output",
        type=Path,
        required=True,
        help="Output path for standalone mapping TTL.",
    )
    parser.add_argument(
        "--status",
        action="append",
        default=["approved"],
        help="Ledger status to include. Repeat for multiple values. Default: approved.",
    )
    return parser.parse_args()


def load_ledger(path: Path) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(path)
    return _ensure_columns(pd.read_csv(path, sep="\t").fillna(""))


def build_exports_for_ledger(
    ledger_path: Path,
    source_ttl_path: Path,
    statuses: list[str] | None = None,
) -> tuple[str, str]:
    if not ledger_path.is_file():
        raise FileNotFoundError(ledger_path)
    if not source_ttl_path.is_file():
        raise FileNotFoundError(source_ttl_path)

    df = load_ledger(ledger_path)
    selected_statuses = [str(value or "").strip() for value in (statuses or ["approved"]) if str(value or "").strip()]
    export_df = df[df["status"].isin(selected_statuses)] if selected_statuses else df.iloc[0:0]

    source_ttl_text = source_ttl_path.read_text(encoding="utf-8", errors="replace")
    replacements, replacement_warnings = _build_replacements(export_df)
    ttl_text = _apply_iri_and_qname_replacements(source_ttl_text, replacements)

    mapping_ttl_text, _, mapping_notes = _build_mapping_triples(export_df)
    mapping_export_text = ""
    if mapping_ttl_text:
        ttl_text = ttl_text.rstrip() + "\n" + mapping_ttl_text
        mapping_export_text, _ = _compact_ttl_iris_with_prefixes(mapping_ttl_text, export_df, replacements)
    ttl_text, _ = _compact_ttl_iris_with_prefixes(ttl_text, export_df, replacements)

    if replacement_warnings:
        raise RuntimeError("\n".join(replacement_warnings))
    if mapping_notes:
        raise RuntimeError("\n".join(mapping_notes))

    return ttl_text, (mapping_export_text.strip() + "\n" if mapping_export_text.strip() else "")


def validate_turtle(text: str) -> None:
    if not text.strip():
        return
    graph = Graph()
    graph.parse(data=text, format="turtle")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def main() -> None:
    args = parse_args()
    ttl_text, mapping_text = build_exports_for_ledger(
        ledger_path=args.ledger,
        source_ttl_path=args.source_ttl,
        statuses=args.status,
    )
    validate_turtle(ttl_text)
    validate_turtle(mapping_text)
    write_text(args.output, ttl_text)
    write_text(args.mapping_output, mapping_text)


if __name__ == "__main__":
    main()
