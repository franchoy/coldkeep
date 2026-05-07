#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/.." && pwd)

cd "$REPO_ROOT"

# Detect common smart quote Unicode characters that often sneak in via editor autocorrect.
# " " ' ' = U+201C U+201D U+2018 U+2019
# Use grep -r with PCRE support for UTF-8 detection.

if grep -r -P '[\x{201C}\x{201D}\x{2018}\x{2019}]' --include='*.go' . 2>/dev/null | grep -q .; then
  echo "[smart-quotes] ERROR: smart quotes detected in Go files:" >&2
  grep -r -P -n '[\x{201C}\x{201D}\x{2018}\x{2019}]' --include='*.go' . 2>/dev/null >&2
  echo "[smart-quotes] Use plain ASCII quotes in code/comments (\" '), not smart quotes." >&2
  exit 1
fi

echo "[smart-quotes] ok: no smart quotes found in Go files"
