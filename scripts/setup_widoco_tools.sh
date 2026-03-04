#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEFAULT_OUT="${ROOT_DIR}/tools/widoco.jar"

usage() {
  cat <<'USAGE'
Setup WIDOCO tooling (download widoco jar locally).

Usage:
  ./scripts/setup_widoco_tools.sh [--latest] [--version <tag>] [--output <path>]

Options:
  --latest           Download latest release (default).
  --version <tag>    Download specific release tag (example: v1.4.25).
  --output <path>    Output jar path (default: tools/widoco.jar).
  -h, --help         Show this help.

Examples:
  ./scripts/setup_widoco_tools.sh
  ./scripts/setup_widoco_tools.sh --version v1.4.25
  ./scripts/setup_widoco_tools.sh --output tools/widoco/widoco.jar
USAGE
}

MODE="latest"
VERSION_TAG=""
OUT_PATH="${DEFAULT_OUT}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --latest)
      MODE="latest"
      shift
      ;;
    --version)
      [[ $# -ge 2 ]] || { echo "Missing value for --version" >&2; exit 1; }
      MODE="version"
      VERSION_TAG="$2"
      shift 2
      ;;
    --output)
      [[ $# -ge 2 ]] || { echo "Missing value for --output" >&2; exit 1; }
      OUT_PATH="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required but not found in PATH." >&2
  exit 1
fi
if ! command -v python >/dev/null 2>&1; then
  echo "python is required but not found in PATH." >&2
  exit 1
fi

if [[ "${MODE}" == "latest" ]]; then
  API_URL="https://api.github.com/repos/dgarijo/Widoco/releases/latest"
else
  API_URL="https://api.github.com/repos/dgarijo/Widoco/releases/tags/${VERSION_TAG}"
fi

echo "Resolving WIDOCO release via: ${API_URL}"
RELEASE_JSON="$(curl -L --fail "${API_URL}")"

JAR_URL="$(printf '%s' "${RELEASE_JSON}" | python -c '
import json, sys
try:
    payload = json.load(sys.stdin)
except Exception:
    print("", end="")
    raise SystemExit(0)
assets = payload.get("assets", []) if isinstance(payload, dict) else []
picked = ""
for item in assets:
    if not isinstance(item, dict):
        continue
    name = str(item.get("name", "")).lower()
    url = str(item.get("browser_download_url", "")).strip()
    if not url:
        continue
    if not name.endswith(".jar"):
        continue
    if "javadoc" in name or "sources" in name:
        continue
    if "widoco" in name:
        picked = url
        break
print(picked, end="")
')"

if [[ -z "${JAR_URL}" ]]; then
  echo "Could not find a WIDOCO jar asset in GitHub release payload." >&2
  exit 1
fi

OUT_ABS="${OUT_PATH}"
if [[ "${OUT_ABS}" != /* ]]; then
  OUT_ABS="${ROOT_DIR}/${OUT_PATH}"
fi
OUT_DIR="$(dirname "${OUT_ABS}")"
mkdir -p "${OUT_DIR}"

echo "Downloading WIDOCO jar from:"
echo "  ${JAR_URL}"
curl -L --fail "${JAR_URL}" -o "${OUT_ABS}"

if [[ ! -s "${OUT_ABS}" ]]; then
  echo "Download failed or file is empty: ${OUT_ABS}" >&2
  exit 1
fi

echo "Installed WIDOCO jar:"
echo "  ${OUT_ABS}"
echo
echo "Next:"
echo "  1) In the app Documentation tab, choose generator: WIDOCO"
echo "  2) Or set WIDOCO_JAR=${OUT_ABS}"
