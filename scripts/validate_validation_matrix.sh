#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/.." && pwd)
MATRIX_FILE="$REPO_ROOT/VALIDATION_MATRIX.md"
README_FILE="$REPO_ROOT/README.md"

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

if [[ ! -f "$README_FILE" ]]; then
  echo "[validation-matrix] ERROR: missing $README_FILE" >&2
  exit 1
fi

require_readme_guarantee_bullet() {
  local bullet="$1"
  if grep -Fqx -- "$bullet" "$README_FILE"; then
    echo "[validation-matrix] ok: README contains guarantee bullet: ${bullet#- }"
  else
    echo "[validation-matrix] ERROR: missing README guarantee bullet: ${bullet#- }" >&2
    return 1
  fi
}

echo "[validation-matrix] checking required validation evidence rows"

summary_bullet_count=$(awk '
  /^### Summary$/ { in_summary=1; next }
  /^### Core invariants$/ { in_summary=0 }
  in_summary && /^- / { count++ }
  END { print count + 0 }
' "$README_FILE")

if [[ "$summary_bullet_count" -ne 5 ]]; then
  echo "[validation-matrix] ERROR: expected 5 README guarantee summary bullets, found $summary_bullet_count" >&2
  exit 1
fi
echo "[validation-matrix] ok: README guarantee summary bullet count is 5"

require_readme_guarantee_bullet '- deterministic, byte-identical restore'
require_readme_guarantee_bullet '- no exposure of partially written or inconsistent data'
require_readme_guarantee_bullet '- non-destructive garbage collection'
require_readme_guarantee_bullet '- atomic restore operations'
require_readme_guarantee_bullet '- safe concurrent storage operations'

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