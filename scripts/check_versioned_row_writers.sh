#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/.." && pwd)

if ! command -v rg >/dev/null 2>&1; then
  echo "[versioned-row-writers] ERROR: rg (ripgrep) is required" >&2
  exit 2
fi

cd "$REPO_ROOT"

# Policy:
# - In non-test Go code, only internal/storage/store.go may insert into
#   logical_file or chunk. This prevents hidden write paths from bypassing
#   chunker-version resolution and invariants.
violations=$(rg -n "INSERT INTO (logical_file|chunk)" internal cmd \
  --glob '**/*.go' \
  --glob '!**/*_test.go' \
  | grep -v '^internal/storage/store.go:' || true)

if [[ -n "$violations" ]]; then
  echo "[versioned-row-writers] ERROR: found non-test INSERT INTO logical_file/chunk outside internal/storage/store.go" >&2
  echo "$violations" >&2
  exit 1
fi

echo "[versioned-row-writers] ok: non-test logical_file/chunk inserts are scoped to internal/storage/store.go"
