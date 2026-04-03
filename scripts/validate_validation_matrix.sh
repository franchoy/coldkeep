#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/.." && pwd)
MATRIX_FILE="$REPO_ROOT/VALIDATION_MATRIX.md"

require_pattern() {
  local file="$1"
  local pattern="$2"
  local description="$3"

  if grep -Eq "$pattern" "$file"; then
    echo "[validation-matrix] ok: $description"
  else
    echo "[validation-matrix] ERROR: missing $description" >&2
    return 1
  fi
}

if [[ ! -f "$MATRIX_FILE" ]]; then
  echo "[validation-matrix] ERROR: missing $MATRIX_FILE" >&2
  exit 1
fi

echo "[validation-matrix] checking required validation evidence rows"

require_pattern "$MATRIX_FILE" '^# v1\.0 Validation Matrix$' 'matrix title'
require_pattern "$MATRIX_FILE" '^## Scope$' 'scope section'
require_pattern "$MATRIX_FILE" '^## Guarantees to Evidence$' 'guarantees-to-evidence section'
require_pattern "$MATRIX_FILE" '^\| Deterministic, byte-identical restore \|' 'deterministic restore row'
require_pattern "$MATRIX_FILE" '^\| No exposure of partially written or inconsistent data \|' 'partial/inconsistent exposure row'
require_pattern "$MATRIX_FILE" '^\| Non-destructive garbage collection \|' 'non-destructive GC row'
require_pattern "$MATRIX_FILE" '^\| Atomic restore operations \|' 'atomic restore row'
require_pattern "$MATRIX_FILE" '^\| Safe concurrent storage operations \|' 'safe concurrency row'
require_pattern "$MATRIX_FILE" '^\| Deep corruption detection .*\|' 'deep corruption detection row'
require_pattern "$MATRIX_FILE" '^\| Corrective health gate contract stability \|' 'doctor/health-gate row'
require_pattern "$MATRIX_FILE" '^## Exit Criteria$' 'exit criteria section'
require_pattern "$MATRIX_FILE" '^1\. Every guarantee row remains mapped to at least one automated test and/or verify check\.$' 'exit criteria mapping guard'

echo "[validation-matrix] validation matrix coverage checks passed"