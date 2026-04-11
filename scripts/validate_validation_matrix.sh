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
  in_summary && /^### / { in_summary=0 }
  in_summary && /^- / { count++ }
  END { print count + 0 }
' "$README_FILE")

if [[ "$summary_bullet_count" -ne 5 ]]; then
  echo "[validation-matrix] ERROR: expected 5 README guarantee summary bullets, found $summary_bullet_count" >&2
  exit 1
fi
echo "[validation-matrix] ok: README guarantee summary bullet count is 5"

require_pattern "$README_FILE" '^### (Core invariants|Guarantees \(G[0-9]+-G[0-9]+\))$' 'README guarantees heading (legacy or ranged style)'

require_readme_guarantee_bullet '- deterministic, byte-identical restore'
require_readme_guarantee_bullet '- no exposure of partially written or inconsistent data'
require_readme_guarantee_bullet '- GC is reference-safe: no reachable chunk is ever deleted'
require_readme_guarantee_bullet '- Atomic restore replacement (within single-node local filesystem semantics)'
require_readme_guarantee_bullet '- Safe in-process concurrent storage operations'

require_pattern "$MATRIX_FILE" '^# v1\.0 Validation Matrix$' 'matrix title'
require_pattern "$MATRIX_FILE" '^## Scope$' 'scope section'
require_pattern "$MATRIX_FILE" '^## Guarantees to Evidence$' 'guarantees-to-evidence section'
require_pattern "$MATRIX_FILE" '^| G1 |' 'G1: deterministic restore row'
require_pattern "$MATRIX_FILE" '^| G2 |' 'G2: repeat store does not drift chunk graph row'
require_pattern "$MATRIX_FILE" '^| G3 |' 'G3: partial/inconsistent exposure row'
require_pattern "$MATRIX_FILE" '^| G4 |' 'G4: reference-safe GC row'
require_pattern "$MATRIX_FILE" '^| G5 |' 'G5: atomic restore replacement row'
require_pattern "$MATRIX_FILE" '^| G6 |' 'G6: safe in-process concurrency row'
require_pattern "$MATRIX_FILE" '^| G7 |' 'G7: deep corruption detection row'
require_pattern "$MATRIX_FILE" '^| G8 |' 'G8: doctor/health-gate row'
require_pattern "$MATRIX_FILE" '^## Post-v1\.0 Extension Guarantees \(v1\.1\+\)$' 'post-v1.0 extension guarantees section'
require_pattern "$MATRIX_FILE" '^| G9 |' 'G9: batch CLI orchestration row'
require_pattern "$MATRIX_FILE" '^| G10 |' 'G10: physical graph audit row'
require_pattern "$MATRIX_FILE" '^| G11 |' 'G11: audited GC root gate row'
require_pattern "$MATRIX_FILE" '^| G12 |' 'G12: invariant classification row'
require_pattern "$MATRIX_FILE" '^| G13 |' 'G13: batch maintenance semantics row'
require_pattern "$MATRIX_FILE" '^## Exit Criteria$' 'exit criteria section'
require_pattern "$MATRIX_FILE" '^1\. Every guarantee row remains mapped to at least one automated test and/or verify check\.$' 'exit criteria mapping guard'

# Coverage assertion: every G* row must have at least one test or verify evidence
awk -F '|' '/^\| G[0-9]+ / {
  id=$2; gsub(/^ +| +$/, "", id);
  verify=$4; gsub(/^ +| +$/, "", verify);
  test=$5; gsub(/^ +| +$/, "", test);
  if (verify == "" && test == "") {
    printf "[validation-matrix] ERROR: guarantee %s has no test or verify evidence\n", id > "/dev/stderr";
    exit 1;
  } else {
    printf "[validation-matrix] ok: guarantee %s is covered by test or verify evidence\n", id;
  }
}' "$MATRIX_FILE"

echo "[validation-matrix] validation matrix coverage checks passed"