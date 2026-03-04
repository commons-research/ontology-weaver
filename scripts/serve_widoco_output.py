#!/usr/bin/env python3
"""Serve a WIDOCO output folder over local HTTP.

WIDOCO output uses XHR requests for section fragments and WebVOWL JSON, which
does not work reliably when opened via file:// URLs in modern browsers.
"""

from __future__ import annotations

import argparse
import functools
import http.server
import os
import socketserver
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Serve a local WIDOCO output directory over HTTP."
    )
    parser.add_argument(
        "folder",
        nargs="?",
        default="registry/schema_docs/cli_widoco_test",
        help="WIDOCO output directory (default: %(default)s)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8765,
        help="Port to bind (default: %(default)s)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    folder = Path(args.folder).resolve()
    if not folder.exists() or not folder.is_dir():
        raise SystemExit(f"[error] Not a directory: {folder}")

    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(folder))
    with socketserver.TCPServer(("127.0.0.1", args.port), handler) as httpd:
        print(f"[ok] Serving: {folder}")
        print(f"[ok] Main doc: http://127.0.0.1:{args.port}/index-en.html")
        print(f"[ok] WebVOWL:  http://127.0.0.1:{args.port}/webvowl/index.html#")
        print("[info] Press Ctrl+C to stop.")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n[info] Stopped.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
