#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/audit_ci_enforcement.sh [--repo owner/repo] [--local-only] [--remote-only]

Verifies the repo-side CI gate invariants and, when GitHub API access is
available, audits the repository protection settings needed to make CI
mandatory for merges and releases.

Remote audit prerequisites:
  - gh CLI installed and authenticated (`gh auth login`)
  - jq installed

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
VALIDATION_MATRIX_FILE="$REPO_ROOT/VALIDATION_MATRIX.md"

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
  require_pattern "$WORKFLOW_FILE" 'needs:\s*\[quality, integration-correctness\]' 'smoke job depends on quality and integration-correctness'
  require_pattern "$WORKFLOW_FILE" 'needs:\s*\[quality, integration-correctness, integration-stress, smoke\]' 'required gate depends on all upstream jobs'
  require_pattern "$WORKFLOW_FILE" 'if:\s*\$\{\{ always\(\) \}\}' 'required gate always evaluates upstream results'
  require_pattern "$WORKFLOW_FILE" 'name:\s*Check shell script syntax' 'shell script syntax validation step'
  require_pattern "$WORKFLOW_FILE" 'name:\s*Audit validation matrix coverage' 'validation matrix CI audit step'
  require_pattern "$WORKFLOW_FILE" 'COLDKEEP_SMOKE_RESET_DB:\s*1' 'isolated smoke reset toggle'
  require_pattern "$WORKFLOW_FILE" 'go test -race -count=1 -short ./tests/\.\.\.' 'integration correctness race run'
  require_pattern "$WORKFLOW_FILE" '^  integration-stress:$' 'integration stress job'
  require_pattern "$WORKFLOW_FILE" '^  integration-long-run:$' 'integration long-run job'
  require_pattern "$WORKFLOW_FILE" '^  smoke:$' 'smoke job'
  require_pattern "$WORKFLOW_FILE" 'name:\s*Upload smoke artifacts on failure' 'smoke failure artifact upload step'
  require_pattern "$WORKFLOW_FILE" 'if:\s*\$\{\{ failure\(\) \}\}' 'smoke artifact upload is failure-only'
  require_pattern "$WORKFLOW_FILE" 'uses:\s*actions/upload-artifact@v4' 'smoke artifact upload action'
  require_pattern "$WORKFLOW_FILE" 'go test -race -count=1 ./tests/\.\.\.' 'integration stress race run'
  require_pattern "$WORKFLOW_FILE" 'COLDKEEP_LONG_RUN:\s*1' 'long-run env gate in CI'
  require_pattern "$WORKFLOW_FILE" "go test -race -count=1 ./tests -run 'TestStoreGCVerifyRestoreDeleteLoopStability\|TestRandomizedLongRunLifecycleSoak'" 'dedicated long-run test command'
  require_pattern "$WORKFLOW_FILE" 'QUALITY_RESULT.*!= "success"' 'required gate rejects skipped quality job'
  require_pattern "$WORKFLOW_FILE" 'INTEGRATION_CORRECTNESS_RESULT.*!= "success"' 'required gate rejects skipped integration correctness'
  require_pattern "$WORKFLOW_FILE" 'INTEGRATION_STRESS_RESULT.*!= "success"' 'required gate rejects skipped integration stress'
  require_pattern "$WORKFLOW_FILE" 'SMOKE_RESULT.*!= "success"' 'required gate rejects skipped smoke job'
  require_pattern "$VALIDATION_MATRIX_FILE" '^# v1\.0 Validation Matrix$' 'validation matrix artifact'
    require_pattern "$VALIDATION_MATRIX_FILE" '^\| G1 \|' 'validation matrix deterministic restore row (G1)'
    require_pattern "$VALIDATION_MATRIX_FILE" '^\| G4 \|' 'validation matrix reference-safe GC row (G4)'
    require_pattern "$VALIDATION_MATRIX_FILE" '^\| G5 \|' 'validation matrix atomic restore replacement row (G5)'
    require_pattern "$VALIDATION_MATRIX_FILE" '^\| G6 \|' 'validation matrix safe in-process concurrency row (G6)'
}

require_gh() {
  if ! command -v gh >/dev/null 2>&1; then
    echo "[audit] ERROR: gh CLI is required for remote protection checks" >&2
    echo "[audit]        Install it first, for example on Ubuntu:" >&2
    echo "[audit]          sudo apt install gh" >&2
    echo "[audit]        Then authenticate with:" >&2
    echo "[audit]          gh auth login" >&2
    if [[ "${EUID:-$(id -u)}" -eq 0 && -n "${SUDO_USER:-}" ]]; then
      echo "[audit]        Note: running under sudo can bypass your user-scoped gh auth/session." >&2
      echo "[audit]              Prefer running the remote audit without sudo." >&2
    fi
    exit 2
  fi
}

require_gh_auth() {
  if ! gh auth status >/dev/null 2>&1; then
    echo "[audit] ERROR: gh CLI is installed but not authenticated" >&2
    echo "[audit]        Authenticate first with:" >&2
    echo "[audit]          gh auth login" >&2
    if [[ "${EUID:-$(id -u)}" -eq 0 && -n "${SUDO_USER:-}" ]]; then
      echo "[audit]        Note: running under sudo uses root's GitHub auth context, not ${SUDO_USER}'s." >&2
      echo "[audit]              Prefer running the remote audit without sudo." >&2
    fi
    exit 2
  fi
}

gh_api() {
  local endpoint="$1"
  local output

  if ! output=$(gh api "$endpoint" 2>&1); then
    echo "[audit] ERROR: GitHub API request failed for: $endpoint" >&2
    echo "[audit]        Verify repository access and GitHub auth/token scopes." >&2
    echo "$output" >&2
    return 1
  fi

  printf '%s\n' "$output"
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
  require_gh_auth
  resolve_repo

  if ! command -v jq >/dev/null 2>&1; then
    echo "[audit] ERROR: jq is required for remote policy inspection" >&2
    echo "[audit]        Install it first, for example on Ubuntu:" >&2
    echo "[audit]          sudo apt install jq" >&2
    exit 2
  fi

  echo "[audit] checking remote protection policy for $REPO"

  local rulesets_json
  rulesets_json=$(gh_api "repos/$REPO/rulesets") || return 1

  if [[ "$rulesets_json" == "[]" ]]; then
    echo "[audit] ERROR: no repository rulesets found" >&2
    echo "[audit] Create at least the rulesets 'Protect mainline branches' and 'Protect release tags'." >&2
    return 1
  fi

  # --- Ruleset: Protect mainline branches ---
  local mainline_id
  mainline_id=$(echo "$rulesets_json" | jq -r '.[] | select(.name == "Protect mainline branches") | .id')
  if [[ -z "$mainline_id" ]]; then
    echo "[audit] ERROR: missing ruleset 'Protect mainline branches'" >&2
    return 1
  fi
  echo "[audit] ok: ruleset 'Protect mainline branches' exists (id=${mainline_id})"

  local mainline_detail
  mainline_detail=$(gh_api "repos/$REPO/rulesets/${mainline_id}") || return 1

  local mainline_enforcement
  mainline_enforcement=$(echo "$mainline_detail" | jq -r '.enforcement // "disabled"')
  if [[ "$mainline_enforcement" != "active" ]]; then
    echo "[audit] ERROR: ruleset 'Protect mainline branches' enforcement is '${mainline_enforcement}', not 'active'" >&2
    return 1
  fi
  echo "[audit] ok: ruleset 'Protect mainline branches' is active"

  # Verify no-direct-push rule is present
  if echo "$mainline_detail" | jq -e '.rules[] | select(.type == "creation" or .type == "update" or .type == "deletion" or .type == "non_fast_forward")' > /dev/null 2>&1; then
    echo "[audit] ok: mainline ruleset includes branch protection rules (creation/update/deletion/non_fast_forward)"
  else
    echo "[audit] WARN: mainline ruleset may be missing branch protection rules (creation, update, deletion, non_fast_forward)" >&2
  fi

  # Verify required status checks include CI Required Gate
  local mainline_required_checks
  mainline_required_checks=$(echo "$mainline_detail" | jq -r '
    [.rules[] | select(.type == "required_status_checks")
     | .parameters.required_status_checks[]?.context] | join(",")')
  if echo "$mainline_required_checks" | grep -Fq "CI Required Gate"; then
    echo "[audit] ok: mainline ruleset requires 'CI Required Gate' status check"
  else
    echo "[audit] ERROR: mainline ruleset does not include 'CI Required Gate' as a required status check" >&2
    echo "[audit]        found: ${mainline_required_checks:-<none>}" >&2
    return 1
  fi

  # Verify bypass actors are not overly permissive
  local bypass_count
  bypass_count=$(echo "$mainline_detail" | jq '[.bypass_actors // [] | .[] | select(.bypass_mode == "always")] | length')
  if [[ "$bypass_count" -gt 0 ]]; then
    echo "[audit] WARN: mainline ruleset has ${bypass_count} actor(s) with always-bypass permission — review them" >&2
  else
    echo "[audit] ok: mainline ruleset has no always-bypass actors"
  fi

  # --- Ruleset: Protect release tags ---
  local tags_id
  tags_id=$(echo "$rulesets_json" | jq -r '.[] | select(.name == "Protect release tags") | .id')
  if [[ -z "$tags_id" ]]; then
    echo "[audit] ERROR: missing ruleset 'Protect release tags'" >&2
    return 1
  fi
  echo "[audit] ok: ruleset 'Protect release tags' exists (id=${tags_id})"

  local tags_detail
  tags_detail=$(gh_api "repos/$REPO/rulesets/${tags_id}") || return 1

  local tags_enforcement
  tags_enforcement=$(echo "$tags_detail" | jq -r '.enforcement // "disabled"')
  if [[ "$tags_enforcement" != "active" ]]; then
    echo "[audit] ERROR: ruleset 'Protect release tags' enforcement is '${tags_enforcement}', not 'active'" >&2
    return 1
  fi
  echo "[audit] ok: ruleset 'Protect release tags' is active"

  # Verify tag pattern targets v*
  local tag_pattern
  local normalized_tag_pattern
  tag_pattern=$(echo "$tags_detail" | jq -r '
    [.conditions.ref_name.include // [] | .[] | select(startswith("refs/tags/"))] | join(",")')
  normalized_tag_pattern=$(echo "$tag_pattern" | tr -d '"')
  if echo "$normalized_tag_pattern" | grep -Eq '(^|,)refs/tags/v\*(,|$)'; then
    echo "[audit] ok: release tags ruleset targets refs/tags/v*"
  else
    echo "[audit] ERROR: release tags ruleset is not constraining refs/tags/v* (found: ${tag_pattern:-<none>})" >&2
    return 1
  fi

  # Verify release CI gating if tag ruleset exposes status/workflow gates.
  local tags_required_checks
  local tags_has_required_workflows
  tags_required_checks=$(echo "$tags_detail" | jq -r '
    [.rules[] | select(.type == "required_status_checks")
     | .parameters.required_status_checks[]?.context] | join(",")')
  tags_has_required_workflows=$(echo "$tags_detail" | jq -r '
    any(.rules[]?; .type == "required_workflows" or .type == "workflows")')

  if [[ -n "$tags_required_checks" ]]; then
    if echo "$tags_required_checks" | grep -Fq "CI Required Gate"; then
      echo "[audit] ok: release tags ruleset requires 'CI Required Gate' status check"
    else
      echo "[audit] WARN: release tags ruleset has required status checks but not 'CI Required Gate' (found: ${tags_required_checks})" >&2
    fi
  elif [[ "$tags_has_required_workflows" == "true" ]]; then
    echo "[audit] ok: release tags ruleset uses required workflow gates"
  else
    echo "[audit] WARN: release tags ruleset does not expose status/workflow gate rules; CI enforcement for releases may rely on process or separate automation" >&2
  fi

  # Verify tag deletion is blocked
  if echo "$tags_detail" | jq -e '.rules[] | select(.type == "deletion")' > /dev/null 2>&1; then
    echo "[audit] ok: release tags ruleset blocks deletions"
  else
    echo "[audit] ERROR: release tags ruleset does not block tag deletions" >&2
    return 1
  fi

  # --- Branch protection (legacy API, best-effort) ---
  local protection_json
  if ! protection_json=$(gh api "repos/$REPO/branches/main/protection" 2>/dev/null); then
    echo "[audit] WARN: could not read legacy main branch protection (may not be configured — rulesets are preferred)" >&2
  else
    if echo "$protection_json" | jq -e '.required_status_checks.contexts[]? | select(. == "CI Required Gate")' > /dev/null 2>&1; then
      echo "[audit] ok: legacy branch protection also requires 'CI Required Gate'"
    else
      echo "[audit] WARN: legacy branch protection does not list 'CI Required Gate' (ruleset is the authoritative gate)" >&2
    fi

    if echo "$protection_json" | jq -e '.required_pull_request_reviews | . != null' > /dev/null 2>&1; then
      echo "[audit] ok: main has pull request review protection enabled"
    else
      echo "[audit] WARN: legacy branch protection does not require pull request reviews" >&2
    fi

    if echo "$protection_json" | jq -e '.allow_force_pushes.enabled == false' > /dev/null 2>&1; then
      echo "[audit] ok: force pushes to main are disabled"
    else
      echo "[audit] WARN: force pushes to main may be permitted — verify in settings" >&2
    fi

    if echo "$protection_json" | jq -e '.allow_deletions.enabled == false' > /dev/null 2>&1; then
      echo "[audit] ok: deletions of main are disabled"
    else
      echo "[audit] WARN: deletions of main may be permitted — verify in settings" >&2
    fi
  fi
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