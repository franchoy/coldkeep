#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/.." && pwd)

cd "$REPO_ROOT"

if ! command -v rg >/dev/null 2>&1; then
  echo "[smart-quotes] ERROR: ripgrep (rg) is required" >&2
  exit 2
fi

# Detect common smart quote Unicode characters that often sneak in via editor autocorrect.
pattern='[“”‘’]'

if matches=$(rg -n "$pattern" --glob '**/*.go' .); then
  :
else
  status=$?
  if [[ "$status" -eq 1 ]]; then
    echo "[smart-quotes] ok: no smart quotes found in Go files"
    exit 0
  fi
  echo "[smart-quotes] ERROR: scan failed" >&2
  exit "$status"
fi

if [[ -n "$matches" ]]; then
  echo "[smart-quotes] ERROR: smart quotes detected in Go files:" >&2
  echo "$matches" >&2
  echo "[smart-quotes] Use plain ASCII quotes in code/comments (\" '), not smart quotes." >&2
  exit 1
fi

echo "[smart-quotes] ok: no smart quotes found in Go files"
