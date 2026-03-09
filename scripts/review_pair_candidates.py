#!/usr/bin/env python3
"""Interactive terminal reviewer for pair alignment candidates.

This script presents one candidate row at a time in a side-by-side view:
- left term context (label, definition, comment, example),
- right term context (label, definition, comment, example).

Curators can choose quick actions:
1) approve with left as canonical,
2) approve with right as canonical,
3) approve with manual canonical term,
4) reject,
5) skip.
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
from pathlib import Path
import re
import shutil
import sys
import textwrap


AUTO_NOTES_RE = re.compile(r"auto-suggested.*before approval", re.IGNORECASE)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Review pair alignment candidates in a side-by-side terminal view"
    )
    parser.add_argument(
        "--candidates-file",
        type=Path,
        default=Path("registry/pair_alignment_candidates.tsv"),
        help="Candidate TSV path",
    )
    parser.add_argument(
        "--status-filter",
        default="needs_review",
        help="Review only rows with this status (default: needs_review)",
    )
    parser.add_argument(
        "--reviewer",
        default="",
        help="Reviewer name written when action updates a row",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=0,
        help="Maximum matching rows to review (0 = all)",
    )
    parser.add_argument(
        "--backup",
        action="store_true",
        help="Write backup file before saving changes",
    )
    return parser.parse_args()


def utc_now_timestamp() -> str:
    """Return current UTC timestamp in compact ISO-8601 format."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_tsv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    """Read TSV rows with header."""
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


def normalize_notes_for_approval(notes: str) -> str:
    """Replace stale auto-suggested notes for approved decisions."""
    text = (notes or "").strip()
    if not text or AUTO_NOTES_RE.search(text):
        return "Approved after manual review."
    return text


def terminal_width(default: int = 140) -> int:
    """Return current terminal width with a safe default fallback."""
    try:
        return shutil.get_terminal_size((default, 30)).columns
    except Exception:
        return default


def side_by_side_lines(
    left_title: str,
    left_value: str,
    right_title: str,
    right_value: str,
    total_width: int,
) -> list[str]:
    """Render one left/right field as wrapped side-by-side text lines."""
    gap = 4
    col_width = max(30, (total_width - gap) // 2)
    left_header = f"{left_title}:"
    right_header = f"{right_title}:"
    left_text = left_value.strip() or "-"
    right_text = right_value.strip() or "-"

    left_block = [left_header] + textwrap.wrap(left_text, width=col_width) or [left_header, "-"]
    right_block = [right_header] + textwrap.wrap(right_text, width=col_width) or [right_header, "-"]
    max_lines = max(len(left_block), len(right_block))
    left_block.extend([""] * (max_lines - len(left_block)))
    right_block.extend([""] * (max_lines - len(right_block)))

    lines: list[str] = []
    for i in range(max_lines):
        lines.append(
            f"{left_block[i]:<{col_width}}{' ' * gap}{right_block[i]:<{col_width}}"
        )
    return lines


def display_row(row: dict[str, str], index: int, total: int) -> None:
    """Print one candidate row in side-by-side comparison format."""
    width = terminal_width()
    print("\n" + "=" * width)
    print(
        f"[{index}/{total}] {row.get('alignment_id', '')}  "
        f"match={row.get('match_method', '')} score={row.get('match_score', '')}  "
        f"relation={row.get('relation', '')} status={row.get('status', '')}"
    )
    print("-" * width)

    fields = [
        ("source", row.get("left_source", ""), "source", row.get("right_source", "")),
        ("iri", row.get("left_term_iri", ""), "iri", row.get("right_term_iri", "")),
        ("label", row.get("left_label", ""), "label", row.get("right_label", "")),
        ("definition", row.get("left_definition", ""), "definition", row.get("right_definition", "")),
        ("comment", row.get("left_comment", ""), "comment", row.get("right_comment", "")),
        ("example", row.get("left_example", ""), "example", row.get("right_example", "")),
    ]
    for lt, lv, rt, rv in fields:
        for line in side_by_side_lines(f"LEFT {lt}", lv, f"RIGHT {rt}", rv, width):
            print(line)
        print("-" * width)

    print(f"OLS URL: {row.get('ols_search_url', '')}")
    print(f"BioPortal URL: {row.get('bioportal_search_url', '')}")
    print(f"Current canonical_from: {row.get('canonical_from', '')}")
    print(f"Current canonical_term_iri: {row.get('canonical_term_iri', '')}")


def clear_canonical_fields(row: dict[str, str]) -> None:
    """Clear canonical decision fields in-place."""
    row["canonical_from"] = ""
    row["canonical_term_iri"] = ""
    row["canonical_term_label"] = ""
    row["canonical_term_source"] = ""
    row["canonical_term_kind"] = ""


def set_canonical_from_left(row: dict[str, str]) -> None:
    """Set canonical decision fields from left term values."""
    row["canonical_from"] = "left"
    row["canonical_term_iri"] = (row.get("left_term_iri", "") or "").strip()
    row["canonical_term_label"] = (row.get("left_label", "") or "").strip()
    row["canonical_term_source"] = (row.get("left_source", "") or "").strip()
    row["canonical_term_kind"] = (row.get("left_term_kind", "") or "").strip()


def set_canonical_from_right(row: dict[str, str]) -> None:
    """Set canonical decision fields from right term values."""
    row["canonical_from"] = "right"
    row["canonical_term_iri"] = (row.get("right_term_iri", "") or "").strip()
    row["canonical_term_label"] = (row.get("right_label", "") or "").strip()
    row["canonical_term_source"] = (row.get("right_source", "") or "").strip()
    row["canonical_term_kind"] = (row.get("right_term_kind", "") or "").strip()


def set_review_metadata(row: dict[str, str], reviewer: str) -> None:
    """Set generic review metadata fields."""
    if reviewer:
        row["reviewer"] = reviewer
    row["date_reviewed"] = utc_now_timestamp()


def action_prompt() -> str:
    """Prompt user for action key."""
    print("Action: [1] approve-left  [2] approve-right  [3] approve-manual  [4] reject  [5] skip  [q] quit")
    return input("> ").strip().lower()


def apply_action(row: dict[str, str], action: str, reviewer: str) -> bool:
    """Apply one action to row. Return True when action handled."""
    if action == "1":
        row["status"] = "approved"
        set_canonical_from_left(row)
        set_review_metadata(row, reviewer)
        row["suggestion_source"] = "manual_curated"
        row["notes"] = normalize_notes_for_approval(row.get("notes", ""))
        return True

    if action == "2":
        row["status"] = "approved"
        set_canonical_from_right(row)
        set_review_metadata(row, reviewer)
        row["suggestion_source"] = "manual_curated"
        row["notes"] = normalize_notes_for_approval(row.get("notes", ""))
        return True

    if action == "3":
        iri = input("canonical_term_iri: ").strip()
        label = input("canonical_term_label: ").strip()
        source = input("canonical_term_source (e.g. chebi/obi/ms): ").strip()
        kind = input("canonical_term_kind (class/property/individual): ").strip()
        if not (iri and label and source and kind):
            print("Manual canonical requires all fields; action canceled.")
            return True
        row["status"] = "approved"
        row["canonical_from"] = "manual"
        row["canonical_term_iri"] = iri
        row["canonical_term_label"] = label
        row["canonical_term_source"] = source
        row["canonical_term_kind"] = kind
        set_review_metadata(row, reviewer)
        row["suggestion_source"] = "manual_curated"
        row["notes"] = normalize_notes_for_approval(row.get("notes", ""))
        return True

    if action == "4":
        row["status"] = "rejected"
        clear_canonical_fields(row)
        set_review_metadata(row, reviewer)
        return True

    if action == "5":
        return True

    if action == "q":
        raise KeyboardInterrupt

    print("Unknown action. Use 1/2/3/4/5/q.")
    return False


def main() -> int:
    """Run interactive review session."""
    args = parse_args()
    if not args.candidates_file.is_file():
        print(f"File not found: {args.candidates_file}", file=sys.stderr)
        return 1

    header, rows = read_tsv(args.candidates_file)
    status_filter = (args.status_filter or "").strip().lower()
    target_indexes = [
        i for i, row in enumerate(rows) if (row.get("status", "") or "").strip().lower() == status_filter
    ]
    if args.max_rows > 0:
        target_indexes = target_indexes[: args.max_rows]

    if not target_indexes:
        print(f"No rows with status={status_filter} in {args.candidates_file}")
        return 0

    print(f"Reviewing {len(target_indexes)} row(s) from {args.candidates_file}")

    try:
        for pos, idx in enumerate(target_indexes, start=1):
            row = rows[idx]
            display_row(row, pos, len(target_indexes))
            while True:
                action = action_prompt()
                handled = apply_action(row, action, reviewer=args.reviewer)
                if handled:
                    break
    except KeyboardInterrupt:
        print("\nReview interrupted; saving current changes.")

    if args.backup:
        backup_path = args.candidates_file.with_suffix(args.candidates_file.suffix + ".bak")
        write_tsv(backup_path, header, rows)
        print(f"Backup written: {backup_path}")

    write_tsv(args.candidates_file, header, rows)
    print(f"Saved: {args.candidates_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
