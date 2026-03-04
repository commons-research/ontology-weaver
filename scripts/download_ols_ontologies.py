#!/usr/bin/env python3
"""Download selected ontology RDF files using the local OLS catalog."""

from __future__ import annotations

import argparse
import csv
import json
import sys
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Iterable

DEFAULT_CATALOG = Path("registry/ols_ontologies.tsv")
DEFAULT_OUTPUT_DIR = Path("registry/downloads/ontologies")
OLS_API_ROOT = "https://www.ebi.ac.uk/ols4/api/ontologies"
OLS_UI_ROOT = "https://www.ebi.ac.uk/ols4/ontologies"

CORE_ONTOLOGY_FALLBACKS: dict[str, list[str]] = {
    "owl": [
        "https://www.w3.org/2002/07/owl",
        "http://www.w3.org/2002/07/owl",
        "https://www.w3.org/2002/07/owl#",
        "http://www.w3.org/2002/07/owl#",
    ],
    "rdfs": [
        "https://www.w3.org/2000/01/rdf-schema",
        "http://www.w3.org/2000/01/rdf-schema",
        "https://www.w3.org/2000/01/rdf-schema#",
        "http://www.w3.org/2000/01/rdf-schema#",
    ],
    "skos": [
        "https://www.w3.org/2004/02/skos/core",
        "http://www.w3.org/2004/02/skos/core",
        "https://www.w3.org/2004/02/skos/core#",
        "http://www.w3.org/2004/02/skos/core#",
    ],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download selected ontologies from OLS using the fetched catalog."
    )
    parser.add_argument(
        "--catalog",
        type=Path,
        default=DEFAULT_CATALOG,
        help="Path to OLS catalog TSV (default: registry/ols_ontologies.tsv)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory to store downloaded ontology files.",
    )
    parser.add_argument(
        "--ontology",
        action="append",
        default=[],
        help="Ontology ID to download (repeatable).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=20.0,
        help="HTTP timeout in seconds (default: 20).",
    )
    args = parser.parse_args()
    if args.timeout <= 0:
        raise SystemExit("--timeout must be > 0")
    if not args.ontology:
        raise SystemExit("Provide at least one --ontology value.")
    return args


def _is_http_url(text: str) -> bool:
    low = text.lower()
    return low.startswith("http://") or low.startswith("https://")


def _read_catalog_rows(path: Path) -> dict[str, dict[str, str]]:
    if not path.is_file():
        raise SystemExit(f"Catalog not found: {path}")
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        rows: dict[str, dict[str, str]] = {}
        for row in reader:
            ontology = (row.get("ontology") or "").strip().lower()
            if ontology:
                rows[ontology] = {k: (v or "").strip() for k, v in row.items()}
        return rows


def _fetch_json(url: str, timeout: float) -> dict[str, object]:
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        raw = response.read()
    return json.loads(raw.decode("utf-8", errors="replace"))


def _link_hrefs(raw_links: object) -> list[str]:
    hrefs: list[str] = []
    if not isinstance(raw_links, dict):
        return hrefs
    for key, value in raw_links.items():
        if isinstance(value, dict):
            href = str(value.get("href") or "").strip()
            if href:
                hrefs.append(href)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    href = str(item.get("href") or "").strip()
                    if href:
                        hrefs.append(href)
        # Keep likely download-related keys first.
        if "download" in str(key).lower():
            hrefs = hrefs[-1:] + hrefs[:-1]
    return hrefs


def _candidate_urls(ontology: str, row: dict[str, str], detail: dict[str, object]) -> list[str]:
    candidates: list[str] = []

    # Prefer explicit OLS API download endpoints first.
    candidates.extend(
        [
            f"{OLS_API_ROOT}/{urllib.parse.quote(ontology, safe='')}/download",
            f"{OLS_API_ROOT}/{urllib.parse.quote(ontology, safe='')}/download?format=ttl",
            f"{OLS_API_ROOT}/{urllib.parse.quote(ontology, safe='')}/download?format=rdfxml",
        ]
    )

    links = detail.get("_links")
    for href in _link_hrefs(links):
        if _is_http_url(href):
            candidates.append(href)

    config = detail.get("config", {}) if isinstance(detail.get("config"), dict) else {}
    loaded = detail.get("loadedOntology", {}) if isinstance(detail.get("loadedOntology"), dict) else {}

    for key in (
        "versionIri",
        "versionIRI",
        "iri",
        "ontologyIri",
        "ontologyIRI",
        "uri",
        "id",
    ):
        value = str(loaded.get(key) or config.get(key) or detail.get(key) or "").strip()
        if _is_http_url(value):
            candidates.append(value)

    for key in ("version_iri", "ontology_iri", "url", "homepage_url", "ols_url"):
        value = str(row.get(key) or "").strip()
        if _is_http_url(value):
            candidates.append(value)
            # Convert OLS UI URLs into API download endpoints.
            if value.startswith(OLS_UI_ROOT + "/"):
                tail = value.rsplit("/", 1)[-1].strip().lower()
                if tail:
                    candidates.extend(
                        [
                            f"{OLS_API_ROOT}/{urllib.parse.quote(tail, safe='')}/download",
                            f"{OLS_API_ROOT}/{urllib.parse.quote(tail, safe='')}/download?format=ttl",
                            f"{OLS_API_ROOT}/{urllib.parse.quote(tail, safe='')}/download?format=rdfxml",
                        ]
                    )

    candidates.extend(CORE_ONTOLOGY_FALLBACKS.get(ontology, []))

    if not candidates:
        api_url = f"{OLS_API_ROOT}/{urllib.parse.quote(ontology, safe='')}"
        candidates.append(api_url)

    # De-duplicate while preserving order.
    deduped: list[str] = []
    seen: set[str] = set()
    for url in candidates:
        if url not in seen:
            deduped.append(url)
            seen.add(url)
    return deduped


def _looks_like_rdf(data: bytes, content_type: str, source_url: str) -> bool:
    ct = content_type.lower()
    if any(
        token in ct
        for token in (
            "text/turtle",
            "application/rdf+xml",
            "application/ld+json",
            "application/n-triples",
            "application/trig",
            "application/n-quads",
        )
    ):
        return True

    if "application/json" in ct and "/api/ontologies/" in source_url:
        return False
    if "text/html" in ct:
        return False

    sample = data[:1200].decode("utf-8", errors="ignore").lower()
    rdf_markers = (
        "@prefix",
        "prefix ",
        "<rdf:rdf",
        "owl:ontology",
        "rdf:type owl:",
        " skos:",
        " rdfs:",
    )
    return any(marker in sample for marker in rdf_markers)


def _extension_from(content_type: str, url: str) -> str:
    ct = content_type.lower()
    if "text/turtle" in ct:
        return ".ttl"
    if "application/rdf+xml" in ct or "application/xml" in ct or "text/xml" in ct:
        return ".rdf"
    if "application/ld+json" in ct or "application/json" in ct:
        return ".jsonld"
    parsed = urllib.parse.urlparse(url)
    suffix = Path(parsed.path).suffix.lower()
    if suffix in {".ttl", ".rdf", ".owl", ".xml", ".nt", ".nq", ".trig", ".jsonld"}:
        return suffix
    return ".ttl"


def _fetch_first_rdf(urls: Iterable[str], timeout: float) -> tuple[bytes, str, str]:
    last_error = "No candidate URL succeeded."
    for url in urls:
        try:
            request = urllib.request.Request(
                url,
                headers={
                    "Accept": (
                        "text/turtle,application/rdf+xml,application/ld+json,"
                        "application/n-triples;q=0.9,*/*;q=0.5"
                    )
                },
            )
            with urllib.request.urlopen(request, timeout=timeout) as response:
                data = response.read()
                content_type = str(response.headers.get("Content-Type") or "")
            if _looks_like_rdf(data, content_type, url):
                return data, content_type, url
            last_error = f"Non-RDF response from {url} ({content_type or 'unknown content type'})"
        except Exception as err:
            last_error = f"{url}: {err}"
    raise RuntimeError(last_error)


def main() -> int:
    args = parse_args()
    catalog = _read_catalog_rows(args.catalog)
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    requested = [str(item).strip().lower() for item in args.ontology if str(item).strip()]
    missing = [ont for ont in requested if ont not in catalog]
    if missing:
        print("Missing ontology IDs in catalog: " + ", ".join(sorted(missing)), file=sys.stderr)
        return 1

    failures: list[tuple[str, str]] = []
    successes = 0

    for ontology in requested:
        row = catalog[ontology]
        detail_url = f"{OLS_API_ROOT}/{urllib.parse.quote(ontology, safe='')}"
        detail_payload: dict[str, object] = {}
        try:
            detail_payload = _fetch_json(detail_url, timeout=args.timeout)
        except Exception:
            detail_payload = {}

        urls = _candidate_urls(ontology, row, detail_payload)
        try:
            payload, content_type, used_url = _fetch_first_rdf(urls, timeout=args.timeout)
            ext = _extension_from(content_type, used_url)
            out_path = output_dir / f"{ontology}{ext}"
            out_path.write_bytes(payload)
            successes += 1
            print(f"[ok] {ontology} -> {out_path} ({len(payload)} bytes) via {used_url}")
        except Exception as err:
            failures.append((ontology, str(err)))
            print(f"[fail] {ontology}: {err}", file=sys.stderr)

    print(f"\nDownloaded {successes}/{len(requested)} ontology file(s) to {output_dir}")
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
