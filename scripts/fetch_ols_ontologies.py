#!/usr/bin/env python3
"""Fetch ontology catalog from OLS4 API and write a TSV file."""

from __future__ import annotations

import argparse
import csv
import json
import sys
import urllib.parse
import urllib.request
from pathlib import Path

DEFAULT_OUTPUT = Path("registry/ols_ontologies.tsv")
OLS_ONTOLOGIES_URL = "https://www.ebi.ac.uk/ols4/api/ontologies"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch OLS ontology catalog")
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Output TSV path (default: registry/ols_ontologies.tsv)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="Per-request timeout in seconds (default: 10.0)",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=200,
        help="Number of ontologies per API page (default: 200)",
    )
    parser.add_argument(
        "--fetch-details",
        action="store_true",
        help="Fetch per-ontology detail payloads for richer metadata (slower).",
    )
    args = parser.parse_args()
    if args.timeout <= 0:
        raise SystemExit("--timeout must be > 0")
    if args.page_size <= 0:
        raise SystemExit("--page-size must be > 0")
    return args


def _as_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return " | ".join(str(item).strip() for item in value if str(item).strip())
    return str(value).strip()


def _as_url(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        for candidate in value:
            text = str(candidate).strip()
            if text.startswith("http://") or text.startswith("https://"):
                return text
        return ""
    text = str(value).strip()
    if text.startswith("http://") or text.startswith("https://"):
        return text
    return ""


def _first_text(mapping: dict[str, object], keys: list[str]) -> str:
    for key in keys:
        value = _as_text(mapping.get(key))
        if value:
            return value
    return ""


def _extract_row(item: dict[str, object]) -> tuple[str, str, str, str, str, str, str, str, str]:
    config = item.get("config", {}) if isinstance(item.get("config"), dict) else {}
    loaded = (
        item.get("loadedOntology", {})
        if isinstance(item.get("loadedOntology"), dict)
        else {}
    )

    ontology = (
        _as_text(item.get("ontologyId"))
        or _as_text(config.get("preferredPrefix")).lower()
        or _as_text(config.get("id")).lower()
    )
    label = _as_text(config.get("title")) or _as_text(item.get("title")) or ontology
    description = (
        _as_text(config.get("description"))
        or _as_text(loaded.get("description"))
        or _as_text(item.get("description"))
    )
    ontology_iri = _first_text(
        loaded,
        ["iri", "ontologyIri", "ontologyIRI"],
    ) or _first_text(item, ["ontologyIri", "ontologyIRI", "iri"])
    version_iri = _first_text(
        loaded,
        ["versionIri", "versionIRI"],
    ) or _first_text(item, ["versionIri", "versionIRI"])
    last_loaded = _first_text(
        loaded,
        ["updated", "lastLoaded", "loaded", "loadDate"],
    ) or _first_text(item, ["loaded", "lastLoaded", "updated", "loadDate"])
    homepage_url = (
        _as_url(config.get("homepage"))
        or _as_url(loaded.get("homepage"))
        or _as_url(item.get("homepage"))
    )
    ols_url = f"https://www.ebi.ac.uk/ols4/ontologies/{urllib.parse.quote(ontology.lower(), safe='')}"
    preferred_url = homepage_url or ols_url
    return (
        ontology.lower(),
        label,
        description,
        preferred_url,
        homepage_url,
        ols_url,
        ontology_iri,
        version_iri,
        last_loaded,
    )


def _fetch_detail(ontology: str, timeout: float) -> dict[str, object]:
    detail_url = f"{OLS_ONTOLOGIES_URL}/{urllib.parse.quote(ontology, safe='')}"
    with urllib.request.urlopen(detail_url, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8", errors="replace"))


def _enrich_with_detail(
    row: tuple[str, str, str, str, str, str, str, str, str],
    detail: dict[str, object],
) -> tuple[str, str, str, str, str, str, str, str, str]:
    (
        ontology,
        label,
        description,
        preferred_url,
        homepage_url,
        ols_url,
        ontology_iri,
        version_iri,
        last_loaded,
    ) = row

    config = detail.get("config", {}) if isinstance(detail.get("config"), dict) else {}
    loaded = (
        detail.get("loadedOntology", {})
        if isinstance(detail.get("loadedOntology"), dict)
        else {}
    )

    ontology_iri = ontology_iri or _first_text(
        loaded, ["iri", "ontologyIri", "ontologyIRI"]
    ) or _first_text(config, ["uri", "id", "iri"])
    version_iri = version_iri or _first_text(
        loaded, ["versionIri", "versionIRI"]
    ) or _first_text(config, ["versionIri", "versionIRI"])
    last_loaded = last_loaded or _first_text(
        loaded, ["updated", "lastLoaded", "loaded", "loadDate"]
    )
    homepage_url = homepage_url or _as_url(config.get("homepage")) or _as_url(loaded.get("homepage"))
    preferred_url = homepage_url or preferred_url or ols_url

    return (
        ontology,
        label,
        description,
        preferred_url,
        homepage_url,
        ols_url,
        ontology_iri,
        version_iri,
        last_loaded,
    )


def fetch_all(
    timeout: float,
    page_size: int,
    fetch_details: bool,
) -> list[tuple[str, str, str, str, str, str, str, str, str]]:
    page = 0
    total_pages = 1
    seen: set[str] = set()
    rows: list[tuple[str, str, str, str, str, str, str, str, str]] = []

    while page < total_pages:
        query = urllib.parse.urlencode({"size": page_size, "page": page})
        url = f"{OLS_ONTOLOGIES_URL}?{query}"
        with urllib.request.urlopen(url, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8", errors="replace"))

        page_info = payload.get("page", {}) if isinstance(payload.get("page"), dict) else {}
        total_pages = int(page_info.get("totalPages", total_pages) or total_pages)

        embedded = payload.get("_embedded", {}) if isinstance(payload.get("_embedded"), dict) else {}
        ontologies = embedded.get("ontologies", []) if isinstance(embedded.get("ontologies"), list) else []

        for item in ontologies:
            if not isinstance(item, dict):
                continue
            (
                ontology,
                label,
                description,
                preferred_url,
                homepage_url,
                ols_url,
                ontology_iri,
                version_iri,
                last_loaded,
            ) = _extract_row(item)
            if not ontology or ontology in seen:
                continue
            seen.add(ontology)
            rows.append(
                (
                    ontology,
                    label,
                    description,
                    preferred_url,
                    homepage_url,
                    ols_url,
                    ontology_iri,
                    version_iri,
                    last_loaded,
                )
            )
        page += 1

    if fetch_details:
        enriched_rows: list[tuple[str, str, str, str, str, str, str, str, str]] = []
        for row in rows:
            ontology = row[0]
            try:
                detail_payload = _fetch_detail(ontology, timeout=timeout)
                enriched_rows.append(_enrich_with_detail(row, detail_payload))
            except Exception:
                enriched_rows.append(row)
        rows = enriched_rows

    rows.sort(key=lambda row: row[0])
    return rows


def write_rows(path: Path, rows: list[tuple[str, str, str, str, str, str, str, str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t", lineterminator="\n")
        writer.writerow(
            [
                "ontology",
                "label",
                "description",
                "url",
                "homepage_url",
                "ols_url",
                "ontology_iri",
                "version_iri",
                "last_loaded",
            ]
        )
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    try:
        rows = fetch_all(
            timeout=args.timeout,
            page_size=args.page_size,
            fetch_details=bool(args.fetch_details),
        )
    except Exception as err:
        print(f"Failed to fetch OLS ontologies: {err}", file=sys.stderr)
        return 1

    write_rows(args.output, rows)
    print(f"Fetched {len(rows)} ontology row(s) to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
