#!/usr/bin/env python3
"""Finalize curated candidate rows into stable pair alignments.

Curator workflow:
1) edit rows directly in `pair_alignment_candidates.tsv`,
2) set `status` to a final value (e.g., approved/rejected/deprecated),
3) run this script to move finalized rows into `pair_alignments.tsv`.
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import re
import sys

from validate_pair_alignments import validate_file

ALIGN_ID_RE = re.compile(r"^ALIGN_(\d{4})$")
FINAL_STATUSES = {"approved", "rejected", "deprecated"}
DATE_ONLY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass(frozen=True)
class Config:
    """Runtime configuration for finalizing candidate rows."""

    candidates_file: Path
    curated_file: Path
    statuses: set[str]
    keep_candidates: bool
    skip_duplicates: bool
    dry_run: bool


def parse_args() -> Config:
    """Parse CLI arguments into script config."""
    parser = argparse.ArgumentParser(
        description="Move finalized candidate rows into curated pair alignments"
    )
    parser.add_argument(
        "--candidates-file",
        type=Path,
        default=Path("registry/pair_alignment_candidates.tsv"),
        help="Path to pair alignment candidates TSV",
    )
    parser.add_argument(
        "--curated-file",
        type=Path,
        default=Path("registry/pair_alignments.tsv"),
        help="Path to curated pair alignments TSV",
    )
    parser.add_argument(
        "--statuses",
        default="approved",
        help="Comma-separated statuses to finalize (default: approved)",
    )
    parser.add_argument(
        "--keep-candidates",
        action="store_true",
        help="Keep finalized rows in candidate file (default: remove finalized rows)",
    )
    parser.add_argument(
        "--no-skip-duplicates",
        action="store_true",
        help="Do not skip rows that already exist in curated file",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print summary only; do not write files",
    )
    args = parser.parse_args()

    statuses = {part.strip().lower() for part in args.statuses.split(",") if part.strip()}
    if not statuses:
        raise SystemExit("--statuses must contain at least one status")
    unknown = sorted(statuses - FINAL_STATUSES)
    if unknown:
        raise SystemExit(
            f"--statuses contains unsupported values: {', '.join(unknown)} "
            f"(allowed: {', '.join(sorted(FINAL_STATUSES))})"
        )

    return Config(
        candidates_file=args.candidates_file,
        curated_file=args.curated_file,
        statuses=statuses,
        keep_candidates=bool(args.keep_candidates),
        skip_duplicates=not bool(args.no_skip_duplicates),
        dry_run=bool(args.dry_run),
    )


def read_tsv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    """Read TSV file and return header and row dictionaries."""
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        return reader.fieldnames or [], list(reader)


def write_tsv(path: Path, header: list[str], rows: list[dict[str, str]]) -> None:
    """Write TSV rows preserving header order."""
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=header,
            delimiter="\t",
            lineterminator="\n",
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)


def merge_headers(primary: list[str], secondary: list[str]) -> list[str]:
    """Return header order with all primary fields plus missing secondary fields."""
    merged = list(primary)
    for col in secondary:
        if col not in merged:
            merged.append(col)
    return merged


def next_align_id(rows: list[dict[str, str]]) -> str:
    """Return next stable ALIGN_XXXX id based on curated rows."""
    max_num = 0
    for row in rows:
        match = ALIGN_ID_RE.fullmatch((row.get("alignment_id", "") or "").strip())
        if match:
            max_num = max(max_num, int(match.group(1)))
    return f"ALIGN_{max_num + 1:04d}"


def pair_key(row: dict[str, str]) -> tuple[tuple[str, str], tuple[str, str]]:
    """Build an order-independent key for duplicate pair detection."""
    left = (
        (row.get("left_source", "") or "").strip().lower(),
        (row.get("left_term_iri", "") or "").strip(),
    )
    right = (
        (row.get("right_source", "") or "").strip().lower(),
        (row.get("right_term_iri", "") or "").strip(),
    )
    return tuple(sorted((left, right)))


def utc_now_timestamp() -> str:
    """Return current UTC timestamp in compact ISO-8601 form."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_timestamp(value: str) -> str:
    """Normalize legacy date-only values into full UTC timestamps."""
    text = (value or "").strip()
    if not text:
        return ""
    if DATE_ONLY_RE.fullmatch(text):
        return f"{text}T00:00:00Z"
    return text


def clean_notes_for_status(notes: str, status: str) -> str:
    """Return cleaned notes after review decision.

    Auto-generated pre-review notes are replaced when row is approved.
    """
    text = (notes or "").strip()
    status_lc = status.lower()
    if status_lc == "approved":
        lower = text.lower()
        if not text or ("auto-suggested" in lower and "before approval" in lower):
            return "Approved after manual review."
    return text


def set_canonical_fields_from_row(row: dict[str, str], canonical_from: str) -> None:
    """Populate canonical_term_* fields based on canonical_from side."""
    if canonical_from == "left":
        row["canonical_term_iri"] = (row.get("left_term_iri", "") or "").strip()
        row["canonical_term_label"] = (row.get("left_label", "") or "").strip()
        row["canonical_term_source"] = (row.get("left_source", "") or "").strip()
    elif canonical_from == "right":
        row["canonical_term_iri"] = (row.get("right_term_iri", "") or "").strip()
        row["canonical_term_label"] = (row.get("right_label", "") or "").strip()
        row["canonical_term_source"] = (row.get("right_source", "") or "").strip()


def canonicalize_decision_fields(row: dict[str, str], status: str) -> None:
    """Ensure canonical decision fields are coherent for finalized rows."""
    canonical_from = (row.get("canonical_from", "") or "").strip().lower()
    if status == "approved" and canonical_from == "":
        canonical_from = "right"
        row["canonical_from"] = canonical_from

    if canonical_from in {"left", "right"}:
        set_canonical_fields_from_row(row, canonical_from)


def canonicalize_reviewed_row(row: dict[str, str]) -> dict[str, str]:
    """Return curated-ready row with review defaults applied."""
    out = dict(row)
    status = (out.get("status", "") or "").strip().lower()
    out["date_added"] = normalize_timestamp((out.get("date_added", "") or ""))
    out["date_reviewed"] = normalize_timestamp((out.get("date_reviewed", "") or ""))
    if status in FINAL_STATUSES and not (out.get("date_reviewed", "") or "").strip():
        out["date_reviewed"] = utc_now_timestamp()

    # Finalized rows represent reviewed decisions.
    if status == "approved":
        suggestion_source = (out.get("suggestion_source", "") or "").strip().lower()
        if suggestion_source in {"", "ols_api", "local_fuzzy"}:
            out["suggestion_source"] = "manual_curated"
    canonicalize_decision_fields(out, status)
    out["notes"] = clean_notes_for_status((out.get("notes", "") or ""), status)
    return out


def finalize_candidates(config: Config) -> int:
    """Move finalized candidate rows to curated alignments and validate outputs."""
    if not config.candidates_file.is_file():
        print(f"File not found: {config.candidates_file}", file=sys.stderr)
        return 1

    cand_header, cand_rows = read_tsv(config.candidates_file)
    if config.curated_file.is_file():
        curated_header, curated_rows = read_tsv(config.curated_file)
        curated_header = merge_headers(curated_header, cand_header)
    else:
        curated_header = list(cand_header)
        curated_rows = []

    existing_keys = {pair_key(row) for row in curated_rows}

    moved_count = 0
    skipped_duplicates = 0
    kept_candidates: list[dict[str, str]] = []
    new_curated_rows = list(curated_rows)

    for cand in cand_rows:
        status = (cand.get("status", "") or "").strip().lower()
        if status not in config.statuses:
            kept_candidates.append(cand)
            continue

        row_for_curated = canonicalize_reviewed_row(cand)
        row_key = pair_key(row_for_curated)
        if config.skip_duplicates and row_key in existing_keys:
            skipped_duplicates += 1
            if config.keep_candidates:
                kept_candidates.append(cand)
            continue

        row_for_curated["alignment_id"] = next_align_id(new_curated_rows)
        new_curated_rows.append(row_for_curated)
        existing_keys.add(row_key)
        moved_count += 1

        if config.keep_candidates:
            kept_candidates.append(cand)

    if config.dry_run:
        print(
            f"Dry run: would move {moved_count} row(s), "
            f"skip {skipped_duplicates} duplicate row(s)"
        )
        return 0

    write_tsv(config.curated_file, curated_header, new_curated_rows)
    write_tsv(config.candidates_file, cand_header, kept_candidates)

    curated_errors = validate_file(config.curated_file, kind="curated")
    candidate_errors = validate_file(config.candidates_file, kind="candidate")
    if curated_errors or candidate_errors:
        for err in curated_errors + candidate_errors:
            print(err)
        return 1

    print(
        f"Finalized {moved_count} row(s) to {config.curated_file}; "
        f"skipped duplicates: {skipped_duplicates}; "
        f"{'kept' if config.keep_candidates else 'removed'} finalized rows from candidates"
    )
    return 0


def main() -> int:
    """Run finalization CLI."""
    config = parse_args()
    return finalize_candidates(config)


if __name__ == "__main__":
    raise SystemExit(main())
