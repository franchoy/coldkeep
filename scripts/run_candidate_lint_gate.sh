#!/usr/bin/env bash
set -Eeuo pipefail
set -o pipefail

readonly EXPECTED_GOLANGCI_LINT_VERSION="2.9.0"

usage() {
  cat <<'EOF'
Usage: scripts/run_candidate_lint_gate.sh <run|verify> EVIDENCE_DIR

Runs or verifies the fail-closed Phase 21 Go lint stage. EVIDENCE_DIR must be
an existing absolute directory below /tmp that is dedicated to the current
exact-head candidate validation.
EOF
}

if [[ $# -ne 2 ]]; then
  usage >&2
  exit 2
fi

readonly MODE="$1"
readonly EVIDENCE_DIR="$2"
if [[ "$MODE" != "run" && "$MODE" != "verify" ]]; then
  usage >&2
  exit 2
fi
if [[ "$EVIDENCE_DIR" != /tmp/* || ! -d "$EVIDENCE_DIR" ]]; then
  echo "[candidate-lint] ERROR: EVIDENCE_DIR must be an existing directory below /tmp" >&2
  exit 2
fi

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/.." && pwd)
readonly SCRIPT_DIR REPO_ROOT
readonly LINT_LOG="$EVIDENCE_DIR/golangci-lint.log"
readonly LINT_STATUS="$EVIDENCE_DIR/golangci-lint.status"
readonly LINT_VERSION="$EVIDENCE_DIR/golangci-lint.version"
readonly LINTER_BIN="${COLDKEEP_GOLANGCI_LINT_BIN:-golangci-lint}"
readonly FINDING_PATTERN='^[^[:space:]].*:[0-9]+:[0-9]+: .+ \([[:alnum:]_-]+\)$|^[1-9][0-9]* issues?:$'

verify_evidence() {
  if [[ ! -f "$LINT_STATUS" || "$(<"$LINT_STATUS")" != "PASS" ]]; then
    echo "[candidate-lint] ERROR: lint status is not PASS" >&2
    return 1
  fi
  if [[ ! -f "$LINT_VERSION" || "$(<"$LINT_VERSION")" != "$EXPECTED_GOLANGCI_LINT_VERSION" ]]; then
    echo "[candidate-lint] ERROR: lint version evidence is not $EXPECTED_GOLANGCI_LINT_VERSION" >&2
    return 1
  fi
  if [[ ! -f "$LINT_LOG" ]]; then
    echo "[candidate-lint] ERROR: lint log is missing" >&2
    return 1
  fi
  if grep -Eq -- "$FINDING_PATTERN" "$LINT_LOG"; then
    echo "[candidate-lint] ERROR: lint log contains actionable findings" >&2
    return 1
  fi
  echo "LOCAL_CANDIDATE_LINT=PASS"
}

if [[ "$MODE" == "verify" ]]; then
  verify_evidence
  exit
fi

umask 077
printf 'FAIL\n' > "$LINT_STATUS"
: > "$LINT_LOG"
: > "$LINT_VERSION"

cd "$REPO_ROOT"
version_output="$("$LINTER_BIN" version 2>&1)" || {
  printf '%s\n' "$version_output" >&2
  echo "[candidate-lint] ERROR: cannot execute golangci-lint" >&2
  exit 1
}
if [[ ! "$version_output" =~ (^|[[:space:]])version[[:space:]]+2[.]9[.]0([[:space:]]|$) ]]; then
  printf '%s\n' "$version_output" >&2
  echo "[candidate-lint] ERROR: expected golangci-lint $EXPECTED_GOLANGCI_LINT_VERSION" >&2
  exit 1
fi
printf '%s\n' "$EXPECTED_GOLANGCI_LINT_VERSION" > "$LINT_VERSION"

config_path="$("$LINTER_BIN" config path 2>&1)" || {
  printf '%s\n' "$config_path" >&2
  echo "[candidate-lint] ERROR: cannot resolve golangci-lint config" >&2
  exit 1
}
if [[ "$(realpath -e -- "$config_path")" != "$REPO_ROOT/.golangci.yml" ]]; then
  echo "[candidate-lint] ERROR: effective config is not $REPO_ROOT/.golangci.yml" >&2
  exit 1
fi
"$LINTER_BIN" config verify

set +e
NO_COLOR=1 "$LINTER_BIN" run 2>&1 | tee "$LINT_LOG"
pipeline_status=("${PIPESTATUS[@]}")
set -e
lint_status="${pipeline_status[0]}"
tee_status="${pipeline_status[1]}"

if [[ "$tee_status" -ne 0 ]]; then
  echo "[candidate-lint] ERROR: failed to write lint evidence" >&2
  exit 1
fi
if grep -Eq -- "$FINDING_PATTERN" "$LINT_LOG"; then
  echo "[candidate-lint] ERROR: golangci-lint reported actionable findings" >&2
  exit 1
fi
if [[ "$lint_status" -ne 0 ]]; then
  echo "[candidate-lint] ERROR: golangci-lint exited with status $lint_status" >&2
  exit "$lint_status"
fi

printf 'PASS\n' > "$LINT_STATUS"
verify_evidence
