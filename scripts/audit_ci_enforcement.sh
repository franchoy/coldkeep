#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/audit_ci_enforcement.sh [--repo owner/repo] [--local-only] [--remote-only]

Verifies the repo-side CI gate invariants and, when GitHub API access is
available, audits the repository protection settings needed to make CI
mandatory for merges and releases.

Expected GitHub-side policy names:
  - Protect mainline branches
  - Protect release tags
EOF
}

REPO=""
LOCAL_ONLY=0
REMOTE_ONLY=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo)
      if [[ $# -lt 2 ]]; then
        echo "[audit] ERROR: --repo requires owner/repo" >&2
        exit 2
      fi
      REPO="$2"
      shift 2
      ;;
    --local-only)
      LOCAL_ONLY=1
      shift
      ;;
    --remote-only)
      REMOTE_ONLY=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[audit] ERROR: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ "$LOCAL_ONLY" -eq 1 && "$REMOTE_ONLY" -eq 1 ]]; then
  echo "[audit] ERROR: --local-only and --remote-only are mutually exclusive" >&2
  exit 2
fi

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/.." && pwd)
WORKFLOW_FILE="$REPO_ROOT/.github/workflows/ci.yml"

require_pattern() {
  local file="$1"
  local pattern="$2"
  local description="$3"

  if grep -Eq "$pattern" "$file"; then
    echo "[audit] ok: $description"
  else
    echo "[audit] ERROR: missing $description" >&2
    return 1
  fi
}

check_local_workflow() {
  echo "[audit] checking local workflow invariants"
  require_pattern "$WORKFLOW_FILE" 'name: CI' 'CI workflow file'
  require_pattern "$WORKFLOW_FILE" 'tags:\s*\[\s*"v\*"\s*\]' 'release tag trigger (v*)'
  require_pattern "$WORKFLOW_FILE" 'merge_group:' 'merge queue trigger'
  require_pattern "$WORKFLOW_FILE" 'name:\s*CI Required Gate' 'aggregate required gate job'
  require_pattern "$WORKFLOW_FILE" 'COLDKEEP_SMOKE_RESET_DB:\s*1' 'isolated smoke reset toggle'
  require_pattern "$WORKFLOW_FILE" 'go test -race -count=1 -short ./tests/\.\.\.' 'integration correctness race run'
}

require_gh() {
  if ! command -v gh >/dev/null 2>&1; then
    echo "[audit] ERROR: gh CLI is required for remote protection checks" >&2
    exit 2
  fi
}

resolve_repo() {
  if [[ -n "$REPO" ]]; then
    return
  fi

  REPO=$(gh repo view --json nameWithOwner --jq .nameWithOwner)
  if [[ -z "$REPO" ]]; then
    echo "[audit] ERROR: could not resolve repository; pass --repo owner/repo" >&2
    exit 2
  fi
}

check_remote_policy() {
  require_gh
  resolve_repo

  echo "[audit] checking remote protection policy for $REPO"

  local rulesets_json
  rulesets_json=$(gh api "repos/$REPO/rulesets")

  if [[ "$rulesets_json" == "[]" ]]; then
    echo "[audit] ERROR: no repository rulesets found" >&2
    echo "[audit] Create at least the rulesets 'Protect mainline branches' and 'Protect release tags'." >&2
    return 1
  fi

  if grep -Fq '"name": "Protect mainline branches"' <<<"$rulesets_json"; then
    echo "[audit] ok: ruleset 'Protect mainline branches' exists"
  else
    echo "[audit] ERROR: missing ruleset 'Protect mainline branches'" >&2
    return 1
  fi

  if grep -Fq '"name": "Protect release tags"' <<<"$rulesets_json"; then
    echo "[audit] ok: ruleset 'Protect release tags' exists"
  else
    echo "[audit] ERROR: missing ruleset 'Protect release tags'" >&2
    return 1
  fi

  local protection_json
  if ! protection_json=$(gh api "repos/$REPO/branches/main/protection" 2>/dev/null); then
    echo "[audit] ERROR: could not read main branch protection" >&2
    echo "[audit] Use a token with repository admin access and rerun the audit." >&2
    return 1
  fi

  if grep -Fq 'CI Required Gate' <<<"$protection_json"; then
    echo "[audit] ok: main requires 'CI Required Gate'"
  else
    echo "[audit] ERROR: main does not require 'CI Required Gate'" >&2
    return 1
  fi

  if grep -Fq '"required_pull_request_reviews": null' <<<"$protection_json"; then
    echo "[audit] ERROR: main does not require pull requests / reviews" >&2
    return 1
  fi
  echo "[audit] ok: main has pull request review protection enabled"
}

status=0

if [[ "$REMOTE_ONLY" -eq 0 ]]; then
  check_local_workflow || status=1
fi

if [[ "$LOCAL_ONLY" -eq 0 ]]; then
  check_remote_policy || status=1
fi

if [[ "$status" -ne 0 ]]; then
  echo "[audit] FAILED: CI is not yet guaranteed end-to-end" >&2
  exit "$status"
fi

echo "[audit] PASSED: CI enforcement prerequisites are in place"