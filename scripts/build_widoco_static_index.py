#!/usr/bin/env python3
"""Build a file://-friendly static WIDOCO index page.

WIDOCO's index-en.html loads section fragments with jQuery .load(), which is
often blocked for local files. This script inlines those fragments.
"""

from __future__ import annotations

import argparse
from pathlib import Path


SECTION_MAP = {
    "abstract": "abstract-en.html",
    "introduction": "introduction-en.html",
    "nstable": "ns-en.html",
    "overview": "overview-en.html",
    "description": "description-en.html",
    "references": "references-en.html",
    "crossref": "crossref-en.html",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inline WIDOCO sections into a static index HTML.")
    parser.add_argument(
        "widoco_output_dir",
        nargs="?",
        default="registry/schema_docs/cli_widoco_test",
        help="Path to WIDOCO output directory (default: %(default)s)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(args.widoco_output_dir).resolve()
    index_path = root / "index-en.html"
    sections_dir = root / "sections"
    if not index_path.exists():
        raise SystemExit(f"[error] Missing: {index_path}")
    if not sections_dir.exists():
        raise SystemExit(f"[error] Missing: {sections_dir}")

    html = index_path.read_text(encoding="utf-8")

    # Remove runtime section loads.
    html = html.replace('$("#abstract").load("sections/abstract-en.html"); \n', "")
    html = html.replace('$("#introduction").load("sections/introduction-en.html"); \n', "")
    html = html.replace('$("#nstable").load("sections/ns-en.html"); \n', "")
    html = html.replace('$("#overview").load("sections/overview-en.html"); \n', "")
    html = html.replace('$("#description").load("sections/description-en.html"); \n', "")
    html = html.replace('$("#references").load("sections/references-en.html"); \n', "")
    html = html.replace('$("#crossref").load("sections/crossref-en.html", null, loadHash); \n', "loadHash();\n")

    for div_id, filename in SECTION_MAP.items():
        section_file = sections_dir / filename
        section_html = section_file.read_text(encoding="utf-8")
        marker = f'<div id="{div_id}"></div>'
        html = html.replace(marker, f'<div id="{div_id}">{section_html}</div>')

    out_path = root / "index-static-en.html"
    out_path.write_text(html, encoding="utf-8")
    print(f"[ok] Wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
