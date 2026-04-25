#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/.." && pwd)

cd "$REPO_ROOT"

search_versioned_writers() {
  local pattern="INSERT INTO (logical_file|chunk)"

  if command -v rg >/dev/null 2>&1; then
    rg -n "$pattern" internal cmd \
      --glob '**/*.go' \
      --glob '!**/*_test.go'
    return 0
  fi

  # Fallback for minimal CI images where ripgrep is unavailable.
  find internal cmd -type f -name '*.go' ! -name '*_test.go' -print0 \
    | xargs -0 grep -nE "$pattern" || true
}

# Policy:
# - In non-test Go code, only internal/storage/store.go may insert into
#   logical_file or chunk. This prevents hidden write paths from bypassing
#   chunker-version resolution and invariants.
violations=$(search_versioned_writers | grep -v '^internal/storage/store.go:' || true)

if [[ -n "$violations" ]]; then
  echo "[versioned-row-writers] ERROR: found non-test INSERT INTO logical_file/chunk outside internal/storage/store.go" >&2
  echo "$violations" >&2
  exit 1
fi

echo "[versioned-row-writers] ok: non-test logical_file/chunk inserts are scoped to internal/storage/store.go"
