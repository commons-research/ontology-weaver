#!/usr/bin/env python3
"""Download external ontology/vocabulary sources from a TSV manifest."""

from __future__ import annotations

import argparse
import csv
import hashlib
import os
import shutil
import sys
import tempfile
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path


DEFAULT_MANIFEST = Path("registry/external_sources.tsv")
DEFAULT_DOWNLOADS_DIR = Path("registry/downloads")
DOWNLOAD_CHUNK_SIZE = 1024 * 1024


@dataclass(frozen=True)
class SourceEntry:
    """One downloadable source described in the manifest."""

    source_id: str
    url: str
    download_path: Path
    enabled: bool
    description: str


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Download external ontology/vocabulary files from a TSV manifest."
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=DEFAULT_MANIFEST,
        help="TSV manifest file (default: registry/external_sources.tsv)",
    )
    parser.add_argument(
        "--source-id",
        action="append",
        default=[],
        help="Source ID to download (repeat for multiple IDs).",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Download all enabled entries from the manifest.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List manifest entries and exit.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite files that already exist.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="Per-request timeout in seconds (default: 30).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show planned actions without downloading files.",
    )
    args = parser.parse_args()

    if args.timeout <= 0:
        raise SystemExit("--timeout must be > 0")

    return args


def parse_bool(text: str) -> bool:
    """Parse truthy values from manifest text."""
    value = (text or "").strip().lower()
    return value in {"1", "true", "yes", "y", "on"}


def load_manifest(path: Path) -> list[SourceEntry]:
    """Load and validate source entries from TSV manifest."""
    if not path.is_file():
        raise SystemExit(f"Manifest file not found: {path}")

    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        required = {"source_id", "url"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise SystemExit(
                f"Manifest missing required column(s): {', '.join(sorted(missing))}"
            )

        entries: list[SourceEntry] = []
        seen_ids: set[str] = set()
        for row_num, row in enumerate(reader, start=2):
            source_id = (row.get("source_id", "") or "").strip()
            url = (row.get("url", "") or "").strip()
            enabled = parse_bool((row.get("enabled", "") or "").strip() or "1")
            description = (row.get("description", "") or "").strip()
            normalized_source_id = source_id.lower()

            if not source_id:
                raise SystemExit(f"Manifest row {row_num}: source_id is required")
            if normalized_source_id in seen_ids:
                raise SystemExit(f"Manifest row {row_num}: duplicate source_id '{source_id}'")
            if not url:
                raise SystemExit(f"Manifest row {row_num}: url is required")
            if any(char.isspace() for char in normalized_source_id):
                raise SystemExit(
                    f"Manifest row {row_num}: source_id must not contain whitespace "
                    f"(got '{source_id}')"
                )
            download_path = DEFAULT_DOWNLOADS_DIR / f"{normalized_source_id}.ttl"

            seen_ids.add(normalized_source_id)
            entries.append(
                SourceEntry(
                    source_id=normalized_source_id,
                    url=url,
                    download_path=download_path,
                    enabled=enabled,
                    description=description,
                )
            )

    return entries


def format_entry(entry: SourceEntry) -> str:
    """Return a readable line for one manifest entry."""
    status = "enabled" if entry.enabled else "disabled"
    desc = f" | {entry.description}" if entry.description else ""
    return f"{entry.source_id}\t{status}\t{entry.url}\t{entry.download_path}{desc}"


def pick_entries(
    entries: list[SourceEntry],
    source_ids: list[str],
    select_all: bool,
) -> list[SourceEntry]:
    """Select entries based on CLI options."""
    by_id = {entry.source_id: entry for entry in entries}

    if source_ids:
        unique_ids: list[str] = []
        seen: set[str] = set()
        for source_id in source_ids:
            clean_id = source_id.strip().lower()
            if not clean_id or clean_id in seen:
                continue
            seen.add(clean_id)
            unique_ids.append(clean_id)

        missing = [source_id for source_id in unique_ids if source_id not in by_id]
        if missing:
            raise SystemExit(
                f"Unknown source_id value(s): {', '.join(missing)}. "
                "Run with --list to inspect available IDs."
            )

        return [by_id[source_id] for source_id in unique_ids]

    if select_all:
        return [entry for entry in entries if entry.enabled]

    # Default behavior: enabled entries only.
    return [entry for entry in entries if entry.enabled]


def file_sha256(path: Path) -> str:
    """Return SHA-256 digest for a file."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(DOWNLOAD_CHUNK_SIZE)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def download_to_path(url: str, output_path: Path, timeout: float) -> tuple[int, str]:
    """Download one URL to output path atomically; return (byte_size, sha256)."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile(
        prefix=f"{output_path.name}.",
        suffix=".tmp",
        dir=str(output_path.parent),
        delete=False,
    ) as tmp:
        tmp_path = Path(tmp.name)

    request = urllib.request.Request(
        url=url,
        headers={"User-Agent": "potential-funicular-downloader/1.0"},
    )

    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            with tmp_path.open("wb") as out:
                shutil.copyfileobj(response, out, length=DOWNLOAD_CHUNK_SIZE)
        os.replace(tmp_path, output_path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise

    size = output_path.stat().st_size
    sha256 = file_sha256(output_path)
    return size, sha256


def main() -> int:
    """Run CLI."""
    args = parse_args()
    entries = load_manifest(args.manifest)

    if args.list:
        for entry in entries:
            print(format_entry(entry))
        return 0

    selected = pick_entries(entries, args.source_id, args.all)
    if not selected:
        print("No sources selected.", file=sys.stderr)
        return 1

    failures = 0
    for entry in selected:
        destination = entry.download_path
        if destination.exists() and not args.force:
            print(f"SKIP  {entry.source_id} (exists): {destination}")
            continue

        if args.dry_run:
            print(f"PLAN  {entry.source_id}: {entry.url} -> {destination}")
            continue

        print(f"GET   {entry.source_id}: {entry.url}")
        try:
            size, sha256 = download_to_path(entry.url, destination, args.timeout)
            print(f"SAVED {entry.source_id}: {destination} ({size} bytes, sha256={sha256})")
        except urllib.error.HTTPError as err:
            failures += 1
            print(
                f"FAIL  {entry.source_id}: HTTP {err.code} while downloading {entry.url}",
                file=sys.stderr,
            )
        except urllib.error.URLError as err:
            failures += 1
            print(
                f"FAIL  {entry.source_id}: network error ({err.reason}) for {entry.url}",
                file=sys.stderr,
            )
        except Exception as err:  # pragma: no cover - defensive catch for CLI robustness
            failures += 1
            print(f"FAIL  {entry.source_id}: {err}", file=sys.stderr)

    if failures:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
