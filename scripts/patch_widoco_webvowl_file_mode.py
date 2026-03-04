#!/usr/bin/env python3
"""Patch WIDOCO WebVOWL bundle to behave in file:// mode.

When opened directly from disk, automatic ontology loading via XHR fails due
to browser restrictions. This patch disables that auto-load silently for
file:// URLs.
"""

from __future__ import annotations

import argparse
from pathlib import Path


TARGET_SNIPPET = "loadingModule.parseUrlAndLoadOntology(); // loads automatically the ontology provided by the parameters"
REPLACEMENT = """if (window.location && window.location.protocol === "file:") {
        // file:// mode: skip automatic XHR-based ontology loading
      } else {
        loadingModule.parseUrlAndLoadOntology(); // loads automatically the ontology provided by the parameters
      }"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Patch WIDOCO webvowl.app.js for file:// mode.")
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
    js_path = root / "webvowl" / "js" / "webvowl.app.js"
    if not js_path.exists():
        raise SystemExit(f"[error] Missing file: {js_path}")

    text = js_path.read_text(encoding="utf-8")
    if REPLACEMENT in text:
        print(f"[ok] Already patched: {js_path}")
        return 0
    if TARGET_SNIPPET not in text:
        raise SystemExit("[error] Target snippet not found; unexpected WebVOWL version/layout.")

    js_path.write_text(text.replace(TARGET_SNIPPET, REPLACEMENT), encoding="utf-8")
    print(f"[ok] Patched: {js_path}")
    print("[info] file:// auto-load is disabled silently.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
