#!/usr/bin/env python3
"""Generate a file://-friendly WebVOWL index that auto-loads ontology JSON."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


ONLOAD_SNIPPET = "window.onload = webvowl.app().initialize;"
ONLOAD_REPLACEMENT = """window.onload = function () {
            var app = webvowl.app();
            app.initialize();
            if (window.__WIDOCO_ONTOLOGY_JSON__) {
                try {
                    webvowl.gr.options().loadingModule().directInput(JSON.stringify(window.__WIDOCO_ONTOLOGY_JSON__));
                } catch (e) {
                    console.error("Failed to auto-load embedded ontology JSON", e);
                }
            }
        };"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build webvowl/index-file.html with embedded ontology JSON."
    )
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
    webvowl_dir = root / "webvowl"
    index_path = webvowl_dir / "index.html"
    ontology_path = webvowl_dir / "data" / "ontology.json"
    if not index_path.exists():
        raise SystemExit(f"[error] Missing: {index_path}")
    if not ontology_path.exists():
        raise SystemExit(f"[error] Missing: {ontology_path}")

    index_html = index_path.read_text(encoding="utf-8")
    ontology = json.loads(ontology_path.read_text(encoding="utf-8"))
    embed = "<script>\nwindow.__WIDOCO_ONTOLOGY_JSON__ = " + json.dumps(ontology, separators=(",", ":")) + ";\n</script>\n"

    if embed not in index_html:
        index_html = index_html.replace("<script src=\"js/d3.min.js\"></script>", embed + "<script src=\"js/d3.min.js\"></script>")

    if ONLOAD_SNIPPET in index_html:
        index_html = index_html.replace(ONLOAD_SNIPPET, ONLOAD_REPLACEMENT)

    out_path = webvowl_dir / "index-file.html"
    out_path.write_text(index_html, encoding="utf-8")
    print(f"[ok] Wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
