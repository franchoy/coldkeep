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
WORKFLOW_FILE="${COLDKEEP_CI_WORKFLOW_FILE:-$REPO_ROOT/.github/workflows/ci.yml}"
CODEQL_WORKFLOW_FILE="${COLDKEEP_CODEQL_WORKFLOW_FILE:-$REPO_ROOT/.github/workflows/codeql.yml}"
VALIDATION_MATRIX_FILE="${COLDKEEP_VALIDATION_MATRIX_FILE:-$REPO_ROOT/VALIDATION_MATRIX.md}"

require_pattern() {
  local file="$1"
  local pattern="$2"
  local description="$3"

  if grep -Eq -- "$pattern" "$file"; then
    echo "[audit] ok: $description"
  else
    echo "[audit] ERROR: missing $description" >&2
    return 1
  fi
}

require_content_pattern() {
  local content="$1"
  local pattern="$2"
  local description="$3"

  if grep -Eq -- "$pattern" <<<"$content"; then
    echo "[audit] ok: $description"
  else
    echo "[audit] ERROR: missing $description" >&2
    return 1
  fi
}

extract_job_block() {
  local job_name="$1"
  awk -v job_name="$job_name" '
    $0 ~ ("^  " job_name ":$") {
      in_job = 1
    }
    in_job && $0 ~ "^  [A-Za-z0-9_-]+:$" && $0 !~ ("^  " job_name ":$") {
      exit
    }
    in_job {
      print
    }
  ' "$WORKFLOW_FILE"
}

extract_step_block_from_content() {
  local content="$1"
  local step_name="$2"
  awk -v step_name="$step_name" '
    $0 == "      - name: " step_name {
      in_step = 1
    }
    in_step && $0 ~ "^      - name: " && $0 != "      - name: " step_name {
      exit
    }
    in_step {
      print
    }
  ' <<<"$content"
}

check_local_workflow() {
  local check_status=0
  local adversarial_block=""
  local deterministic_g6_block=""

  echo "[audit] checking local workflow invariants"
  require_pattern "$WORKFLOW_FILE" 'name: CI' 'CI workflow file' || check_status=1
  require_pattern "$WORKFLOW_FILE" '^  push:$' 'CI push trigger' || check_status=1
  require_pattern "$WORKFLOW_FILE" '^\s+- main$' 'CI push branch retains main' || check_status=1
  require_pattern "$WORKFLOW_FILE" '^\s+- release/\*\*$' 'CI push branch includes release/**' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'tags:\s*\[\s*"v\*"\s*\]' 'release tag trigger (v*)' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'merge_group:' 'merge queue trigger' || check_status=1
  require_pattern "$WORKFLOW_FILE" '^  workflow_dispatch:\s*$' 'CI workflow_dispatch trigger' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'name:\s*CI Required Gate' 'aggregate required gate job' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'needs:\s*\[quality, correctness-matrix\]' 'smoke job depends on quality and correctness-matrix' || check_status=1
  require_pattern "$WORKFLOW_FILE" '^  cross-platform:$' 'cross-platform job exists' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'os:\s*\[ubuntu-latest, macos-latest, windows-latest\]' 'cross-platform job runs native ubuntu, macOS, and Windows matrix' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'name:\s*Run path safety cross-platform tests' 'cross-platform path safety step' || check_status=1
  require_pattern "$WORKFLOW_FILE" "go test ./internal/pathsafe/\\.\\.\\. -run 'TrustedRoot\\|Symlink\\|Alias\\|WritePath' -count=1" 'cross-platform path safety command covers trusted-root and alias checks' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'name:\s*Run storage restore cross-platform tests' 'cross-platform storage restore step' || check_status=1
  require_pattern "$WORKFLOW_FILE" "go test ./internal/storage/\\.\\.\\. -run 'TestRestore\\(FileByStoredPath\\(UsesPhysicalPathIdentity\\|UsesLexicalPhysicalPathIdentityAboveAlias\\|PrefixMode\\|PrefixModeCreatesMissingParents\\|OverrideMode\\|RejectsSymlinkedPrefixRoot\\)\\|WithTrustedRootAllowsOuterAliasForExactOutputPath\\|StoredPath\\(PrefixAllowsOuterAliasAboveTrustedRoot\\|OverrideAllowsOuterAliasAboveDerivedRoot\\|PrefixRejectsSymlinkedTargetInTrustedRoot\\|OverrideRejectsSymlinkedTargetInTrustedRoot\\|OriginalRejectsInjectedSymlinkBelowDerivedTrustedRoot\\)\\|RejectsSymlinkedTargetInTrustedRoot\\)' -count=1" 'cross-platform storage restore command scopes to trusted restore paths' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'name:\s*Run engine restore cross-platform tests' 'cross-platform engine restore step' || check_status=1
  require_pattern "$WORKFLOW_FILE" "go test ./internal/engine/\\.\\.\\. -run '\\^TestRestore' -count=1" 'cross-platform engine restore command scopes to restore tests' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'name:\s*Run snapshot restore cross-platform tests' 'cross-platform snapshot restore step' || check_status=1
  require_pattern "$WORKFLOW_FILE" "go test ./internal/snapshot/\\.\\.\\. -run '\\^TestRestoreSnapshot' -count=1" 'cross-platform snapshot restore command scopes to snapshot restore tests' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'needs:\s*\[quality, correctness-matrix, integration-stress, integration-long-run, adversarial, smoke, legacy-compatibility, benchmark-matrix, cross-platform\]' 'required gate depends on all upstream jobs including long-run, adversarial, legacy compatibility, benchmark matrix, and cross-platform' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'if:\s*\$\{\{ always\(\) \}\}' 'required gate always evaluates upstream results' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'name:\s*Check smart quotes in Go files' 'smart-quote guard step' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'run:\s*bash scripts/check_smart_quotes\.sh' 'smart-quote guard command' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'name:\s*Check shell script syntax' 'shell script syntax validation step' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'name:\s*Lint shell scripts \(ShellCheck\)' 'shell script lint step (ShellCheck)' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'uses:\s*ludeeus/action-shellcheck@2\.0\.0' 'ShellCheck action pinned version' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'scandir:\s*\./scripts' 'ShellCheck scan directory is scripts/' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'name:\s*Audit validation matrix coverage' 'validation matrix CI audit step' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'name:\s*Enforce versioned row writer scope' 'versioned row writer scope guard step' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'run:\s*bash scripts/check_versioned_row_writers\.sh' 'versioned row writer scope guard command' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'COLDKEEP_SMOKE_RESET_DB:\s*1' 'isolated smoke reset toggle' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'go test -race -count=1 -short( -json)? ./tests/integration/\.\.\.' 'integration correctness race run (integration only)' || check_status=1
  require_pattern "$WORKFLOW_FILE" "go test -race -count=1 ./tests/integration/... -run 'TestPhase7SnapshotRetentionLifecycleCLIIntegration'" 'explicit Phase 7 snapshot retention lifecycle gate' || check_status=1
  require_pattern "$WORKFLOW_FILE" '^  integration-stress:$' 'integration stress job' || check_status=1
  require_pattern "$WORKFLOW_FILE" '^  integration-long-run:$' 'integration long-run job' || check_status=1
  require_pattern "$WORKFLOW_FILE" '^  adversarial:$' 'adversarial job exists' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'name:\s*Run adversarial validation \(G1.*G17\)' 'adversarial workflow step names batch coverage through G17' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'go test -race -count=1 ./tests/adversarial/\.\.\.' 'adversarial job targets adversarial suite' || check_status=1
  require_pattern "$WORKFLOW_FILE" "go test -race -count=1 ./tests/adversarial/... -run 'TestAdversarialG14\\|TestAdversarialG15\\|TestAdversarialG16\\|TestAdversarialG17'" 'explicit G14-G17 adversarial gate command' || check_status=1
  adversarial_block="$(extract_job_block adversarial)"
  if [[ -z "$adversarial_block" ]]; then
    echo "[audit] ERROR: missing adversarial job block content" >&2
    check_status=1
  else
    require_content_pattern "$adversarial_block" '^    services:$' 'adversarial job declares services' || check_status=1
    require_content_pattern "$adversarial_block" '^      postgres:$' 'adversarial job provisions postgres service' || check_status=1
    require_content_pattern "$adversarial_block" 'image:\s*postgres:16' 'adversarial job pins postgres service image' || check_status=1
    deterministic_g6_block="$(extract_step_block_from_content "$adversarial_block" "Run deterministic G6 PostgreSQL interleaving regression")"
    if [[ -z "$deterministic_g6_block" ]]; then
      echo "[audit] ERROR: missing deterministic G6 PostgreSQL regression step block" >&2
      check_status=1
    else
      require_content_pattern "$deterministic_g6_block" '^      - name: Run deterministic G6 PostgreSQL interleaving regression$' 'deterministic G6 PostgreSQL regression step' || check_status=1
      require_content_pattern "$deterministic_g6_block" 'COLDKEEP_TEST_DB:\s*1' 'deterministic G6 PostgreSQL regression enables DB gate' || check_status=1
      require_content_pattern "$deterministic_g6_block" 'COLDKEEP_DB_AUTO_BOOTSTRAP:\s*true' 'deterministic G6 PostgreSQL regression enables DB bootstrap' || check_status=1
      require_content_pattern "$deterministic_g6_block" 'COLDKEEP_REQUIRE_DETERMINISTIC_G6_POSTGRES:\s*1' 'deterministic G6 PostgreSQL regression forbids DB skip' || check_status=1
      require_content_pattern "$deterministic_g6_block" 'COLDKEEP_REQUIRE_DETERMINISTIC_G6_RETRY_CASE:\s*1' 'deterministic G6 PostgreSQL regression requires retry-case execution' || check_status=1
      require_content_pattern "$deterministic_g6_block" 'go test -v -race -count=1 ./tests/adversarial/\.\.\.' 'deterministic G6 PostgreSQL regression targets adversarial package explicitly' || check_status=1
      require_content_pattern "$deterministic_g6_block" "-run '\\^TestAdversarialG6DeterministicStoreInterleavingPostgres\\$'" 'deterministic G6 PostgreSQL regression uses exact selector' || check_status=1
    fi
  fi
  require_pattern "$WORKFLOW_FILE" '^  smoke:$' 'smoke job' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'name:\s*Upload smoke artifacts on failure' 'smoke failure artifact upload step' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'if:\s*\$\{\{ failure\(\) \}\}' 'smoke artifact upload is failure-only' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'uses:\s*actions/upload-artifact@v[45]' 'smoke artifact upload action' || check_status=1
  require_pattern "$WORKFLOW_FILE" './tests/integration/\.\.\.' 'integration stress race run (integration only)' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'COLDKEEP_LONG_RUN:\s*1' 'long-run env gate in CI' || check_status=1
  require_pattern "$WORKFLOW_FILE" "go test -race -count=1 ./tests/integration/... -run 'TestStoreGCVerifyRestoreDeleteLoopStability\\|TestRandomizedLongRunLifecycleSoak\\|TestSnapshotRetentionChurnLongRun'" 'dedicated long-run test command' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'QUALITY_RESULT.*!= "success"' 'required gate rejects skipped quality job' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'CORRECTNESS_MATRIX_RESULT.*!= "success"' 'required gate rejects skipped correctness matrix' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'INTEGRATION_STRESS_RESULT.*!= "success"' 'required gate rejects skipped integration stress' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'INTEGRATION_LONG_RUN_RESULT.*!= "success"' 'required gate rejects skipped integration long-run job' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'ADVERSARIAL_RESULT.*!= "success"' 'required gate rejects skipped adversarial job' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'SMOKE_RESULT.*!= "success"' 'required gate rejects skipped smoke job' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'BENCHMARK_RESULT.*!= "success"' 'required gate rejects skipped benchmark job' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'CROSS_PLATFORM_RESULT.*!= "success"' 'required gate rejects skipped cross-platform job' || check_status=1
  require_pattern "$CODEQL_WORKFLOW_FILE" 'name:\s*CodeQL' 'CodeQL workflow file' || check_status=1
  require_pattern "$CODEQL_WORKFLOW_FILE" '^  push:$' 'CodeQL push trigger' || check_status=1
  require_pattern "$CODEQL_WORKFLOW_FILE" '^\s+- main$' 'CodeQL push branch retains main' || check_status=1
  require_pattern "$CODEQL_WORKFLOW_FILE" '^\s+- release/\*\*$' 'CodeQL push branch includes release/**' || check_status=1
  require_pattern "$CODEQL_WORKFLOW_FILE" '^  workflow_dispatch:\s*$' 'CodeQL workflow_dispatch trigger' || check_status=1
  require_pattern "$CODEQL_WORKFLOW_FILE" 'language:\s*actions' 'CodeQL retains actions analysis' || check_status=1
  require_pattern "$CODEQL_WORKFLOW_FILE" 'language:\s*go' 'CodeQL retains Go analysis' || check_status=1
  require_pattern "$CODEQL_WORKFLOW_FILE" 'language:\s*python' 'CodeQL retains Python analysis' || check_status=1
  require_pattern "$CODEQL_WORKFLOW_FILE" '^  schedule:$' 'CodeQL weekly schedule trigger' || check_status=1
  require_pattern "$REPO_ROOT/internal/pathsafe/pathsafe_test.go" 'filepath\.EvalSymlinks\(t\.TempDir\(\)\)' 'generic symlink-component test retains canonical-path-specific coverage outside restore enforcement' || check_status=1
  if grep -Eq 'filepath\.EvalSymlinks\(t\.TempDir\(\)\)' "$REPO_ROOT/internal/storage/"*test.go 2>/dev/null; then
    echo "[audit] ERROR: storage restore tests must not canonicalize t.TempDir() with filepath.EvalSymlinks" >&2
    check_status=1
  else
    echo "[audit] ok: storage restore tests preserve native lexical temp paths"
  fi
  require_pattern "$VALIDATION_MATRIX_FILE" '^# (v1\.0 )?Validation Matrix$' 'validation matrix artifact (legacy or current style)' || check_status=1
  require_pattern "$VALIDATION_MATRIX_FILE" '^\| G1 \|' 'validation matrix deterministic restore row (G1)' || check_status=1
  require_pattern "$VALIDATION_MATRIX_FILE" '^\| G2 \|' 'validation matrix repeat store does not drift chunk graph row (G2)' || check_status=1
  require_pattern "$VALIDATION_MATRIX_FILE" '^\| G3 \|' 'validation matrix partial/inconsistent exposure row (G3)' || check_status=1
  require_pattern "$VALIDATION_MATRIX_FILE" '^\| G4 \|' 'validation matrix reference-safe GC row (G4)' || check_status=1
  require_pattern "$VALIDATION_MATRIX_FILE" '^\| G5 \|' 'validation matrix atomic restore replacement row (G5)' || check_status=1
  require_pattern "$VALIDATION_MATRIX_FILE" '^\| G6 \|' 'validation matrix safe in-process concurrency row (G6)' || check_status=1
  require_pattern "$VALIDATION_MATRIX_FILE" '^\| G7 \|' 'validation matrix deep corruption detection row (G7)' || check_status=1
  require_pattern "$VALIDATION_MATRIX_FILE" '^\| G8 \|' 'validation matrix doctor/health-gate row (G8)' || check_status=1
  require_pattern "$VALIDATION_MATRIX_FILE" '^\| G9 \|' 'validation matrix batch CLI orchestration row (G9)' || check_status=1
  require_pattern "$VALIDATION_MATRIX_FILE" '^\| G10 \|' 'validation matrix physical graph audit row (G10)' || check_status=1
  require_pattern "$VALIDATION_MATRIX_FILE" '^\| G11 \|' 'validation matrix audited GC root gate row (G11)' || check_status=1
  require_pattern "$VALIDATION_MATRIX_FILE" '^\| G12 \|' 'validation matrix invariant classification row (G12)' || check_status=1
  require_pattern "$VALIDATION_MATRIX_FILE" '^\| G13 \|' 'validation matrix batch maintenance semantics row (G13)' || check_status=1
  require_pattern "$VALIDATION_MATRIX_FILE" '^\| G14 \|' 'validation matrix snapshot-retained GC safety row (G14)' || check_status=1
  require_pattern "$VALIDATION_MATRIX_FILE" '^\| G15 \|' 'validation matrix snapshot delete semantics row (G15)' || check_status=1
  require_pattern "$VALIDATION_MATRIX_FILE" '^\| G16 \|' 'validation matrix snapshot-retention observability row (G16)' || check_status=1
  require_pattern "$VALIDATION_MATRIX_FILE" '^\| G17 \|' 'validation matrix snapshot reachability integrity row (G17)' || check_status=1

  return "$check_status"
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
      echo "[audit] ERROR: release tags ruleset has required status checks but not 'CI Required Gate' (found: ${tags_required_checks})" >&2
      return 1
    fi
  elif [[ "$tags_has_required_workflows" == "true" ]]; then
    echo "[audit] ok: release tags ruleset uses required workflow gates"
  else
    echo "[audit] ERROR: release tags ruleset does not expose status/workflow gate rules" >&2
    return 1
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
