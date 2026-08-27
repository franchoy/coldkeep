#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/audit_ci_enforcement.sh [--repo owner/repo] [--local-only] [--remote-only] [--paired-launcher FILE]

Verifies the repo-side CI gate invariants and, when GitHub API access is
available, audits the repository protection settings needed to make CI
mandatory for merges and releases.

Remote audit prerequisites:
  - gh CLI installed and authenticated (`gh auth login`)
  - jq installed

Expected GitHub-side policy names:
  - Protect mainline branches
  - Protect release branches
  - Protect release tags
EOF
}

REPO=""
LOCAL_ONLY=0
REMOTE_ONLY=0
PAIRED_LAUNCHER_FILE=""

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
    --paired-launcher)
      if [[ $# -lt 2 ]]; then
        echo "[audit] ERROR: --paired-launcher requires a workflow path" >&2
        exit 2
      fi
      PAIRED_LAUNCHER_FILE="$2"
      shift 2
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
BENCHMARK_BASELINE_WORKFLOW_FILE="${COLDKEEP_BENCHMARK_BASELINE_WORKFLOW_FILE:-$REPO_ROOT/.github/workflows/benchmark-baseline.yml}"
BENCHMARK_GATE_FILE="${COLDKEEP_BENCHMARK_GATE_FILE:-$REPO_ROOT/scripts/benchmark_gate.py}"
TIMING_VALIDATOR_FILE="${COLDKEEP_TIMING_VALIDATOR_FILE:-$REPO_ROOT/scripts/validate_regression_thresholds.py}"
CANDIDATE_LINT_GATE_FILE="${COLDKEEP_CANDIDATE_LINT_GATE_FILE:-$REPO_ROOT/scripts/run_candidate_lint_gate.sh}"
VALIDATION_MATRIX_FILE="${COLDKEEP_VALIDATION_MATRIX_FILE:-$REPO_ROOT/VALIDATION_MATRIX.md}"
PAIRED_REFERENCE_MANIFEST_FILE="${COLDKEEP_PAIRED_REFERENCE_MANIFEST_FILE:-$REPO_ROOT/benchmarks/paired/reference-v1.13.json}"
PAIRED_THRESHOLD_POLICY_FILE="${COLDKEEP_PAIRED_THRESHOLD_POLICY_FILE:-$REPO_ROOT/benchmarks/paired/threshold-policy-v1.13.json}"
NATIVE_UNIX_TEST_FILE="${COLDKEEP_NATIVE_UNIX_TEST_FILE:-$REPO_ROOT/internal/coordination/native_lock_unix_test.go}"
NATIVE_WINDOWS_TEST_FILE="${COLDKEEP_NATIVE_WINDOWS_TEST_FILE:-$REPO_ROOT/internal/coordination/native_lock_windows_test.go}"
COORDINATOR_NATIVE_TEST_FILE="${COLDKEEP_COORDINATOR_NATIVE_TEST_FILE:-$REPO_ROOT/internal/coordination/coordinator_native_test.go}"
SNAPSHOT_EVIDENCE_VALIDATOR_FILE="${COLDKEEP_SNAPSHOT_EVIDENCE_VALIDATOR_FILE:-$REPO_ROOT/scripts/validate_snapshot_evidence_names.sh}"
RELEASE_LINEARITY_VALIDATOR_FILE="${COLDKEEP_RELEASE_LINEARITY_VALIDATOR_FILE:-$REPO_ROOT/scripts/validate_release_linearity.sh}"
RELEASE_BENCHMARK_EVIDENCE_FILE="${COLDKEEP_RELEASE_BENCHMARK_EVIDENCE_FILE:-$REPO_ROOT/scripts/release_benchmark_evidence.sh}"
RELEASE_BENCHMARK_RUNNER_FILE="${COLDKEEP_RELEASE_BENCHMARK_RUNNER_FILE:-$REPO_ROOT/scripts/run_release_benchmark_evidence.sh}"

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

require_executable_file() {
  local file="$1"
  local description="$2"

  if [[ -f "$file" && ! -L "$file" && -x "$file" ]]; then
    echo "[audit] ok: $description"
  else
    echo "[audit] ERROR: $description must be an executable regular non-symlink file" >&2
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

extract_job_block_from_file() {
  local file="$1"
  local job_name="$2"
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
  ' "$file"
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

check_paired_launcher_output_ownership() {
  local file="$1"

  python3 - "$file" <<'PY'
import pathlib
import posixpath
import re
import shlex
import sys

source = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8")
assignments = {
    match.group(1): match.group(3)
    for match in re.finditer(
        r"(?m)^[ \t]*([A-Za-z_][A-Za-z0-9_]*)=([\"'])(.*?)\2[ \t]*$", source
    )
}
errors: list[str] = []


def error(message: str) -> None:
    if message not in errors:
        errors.append(message)


def expand(value: str) -> str:
    value = value.strip().strip("\"'")
    for _ in range(12):
        previous = value
        for name, replacement in assignments.items():
            value = value.replace("${" + name + "}", replacement)
            value = re.sub(r"\$" + re.escape(name) + r"\b", replacement, value)
        if value == previous:
            break
    value = value.replace("${GITHUB_WORKSPACE}", "@workspace")
    value = re.sub(r"\$GITHUB_WORKSPACE\b", "@workspace", value)
    value = value.replace("${RUNNER_TEMP}", "@runner_temp")
    value = re.sub(r"\$RUNNER_TEMP\b", "@runner_temp", value)
    value = value.rstrip("/") or "/"
    if not value.startswith(("@workspace", "@runner_temp", "/")):
        value = "@workspace/" + value.lstrip("./")
    return re.sub(r"/{2,}", "/", value)


def shell_tokens(line: str) -> list[str]:
    try:
        return shlex.split(line.rstrip(" \\"))
    except ValueError:
        return []


def command_blocks() -> list[dict[str, object]]:
    blocks: list[dict[str, object]] = []
    pattern = re.compile(r"paired_benchmark_gate\.py[ \t]+(sample|decision)\b")
    for match in pattern.finditer(source):
        step_start = source.rfind("\n      - name:", 0, match.start())
        if step_start < 0:
            step_start = source.rfind("\n    steps:", 0, match.start())
        if step_start < 0:
            step_start = 0
        step_end = source.find("\n      - name:", match.end())
        if step_end < 0:
            step_end = len(source)
        tail = source[match.end():step_end]
        output = re.search(
            r"--output-dir[ \t]+(?:\"([^\"\n]+)\"|'([^'\n]+)'|([^\s\\]+))", tail
        )
        if output is None:
            error(f"{match.group(1)} command must pass --output-dir")
            continue
        token = next(group for group in output.groups() if group is not None)
        blocks.append(
            {
                "kind": match.group(1),
                "start": match.start(),
                "end": step_end,
                "step_start": step_start,
                "token": token,
                "path": expand(token),
            }
        )
    return blocks


def creation_targets(text: str) -> tuple[set[str], set[str]]:
    created: set[str] = set()
    populated: set[str] = set()
    for raw_line in text.splitlines():
        line = raw_line.strip()
        tokens = shell_tokens(line)
        if not tokens:
            continue
        command = tokens[0]
        operands = [token for token in tokens[1:] if not token.startswith("-")]
        if command in {"mkdir", "touch"}:
            created.update(expand(token) for token in operands)
        elif command == "install" and "-d" in tokens:
            created.update(expand(token) for token in operands)
        elif command in {"cp", "mv", "rsync", "git"} and operands:
            populated.add(expand(operands[-1]))
        elif command in {"tar", "unzip"}:
            for option in ("-C", "--directory", "-d"):
                if option in tokens and tokens.index(option) + 1 < len(tokens):
                    populated.add(expand(tokens[tokens.index(option) + 1]))
        elif command == "ln" and "-s" in tokens and operands:
            populated.add(expand(operands[-1]))
        elif command == "rm":
            populated.update(expand(token) for token in operands)
    return created, populated


blocks = command_blocks()
sample_paths: list[str] = []
all_output_paths = {str(block["path"]) for block in blocks}

for block in blocks:
    kind = str(block["kind"])
    output_path = str(block["path"])
    token = str(block["token"])
    start = int(block["start"])
    step_start = int(block["step_start"])
    prefix = source[:start]
    step_prefix = source[step_start:start]

    if kind == "sample":
        sample_paths.append(output_path)

    if output_path in {".", "/", "@workspace", "@runner_temp"}:
        error(f"{kind} output must be a nonexistent child below a contained parent")
    if not output_path.startswith(("@workspace/", "@runner_temp/")):
        error(f"{kind} output must remain below a permitted contained parent")
    if any(part == ".." for part in output_path.split("/")):
        error(f"{kind} output must not use traversal")
    if output_path.startswith(("@workspace/candidate", "@workspace/reference")):
        error(f"{kind} output must not reuse a repository checkout path")

    parent = posixpath.dirname(output_path)
    created, populated = creation_targets(prefix)
    if parent not in created:
        error(f"{kind} output parent must exist before harness invocation")
    if output_path in created:
        error(f"{kind} output must not be created before harness invocation")
    if output_path in populated:
        error(f"{kind} output must not be populated, checked out, extracted, or recreated")

    assertions = re.finditer(
        r"test[ \t]+![ \t]+-e[ \t]+(?:\"([^\"\n]+)\"|'([^'\n]+)'|([^\s\\]+))",
        step_prefix,
    )
    asserted_paths = {
        expand(next(group for group in assertion.groups() if group is not None))
        for assertion in assertions
    }
    if output_path not in asserted_paths:
        error(f"{kind} output requires an exact nonexistence assertion before invocation")

    for checkout in re.finditer(r"uses:[ \t]*actions/checkout@[^\n]+", prefix):
        checkout_end = source.find("\n      - name:", checkout.end())
        if checkout_end < 0 or checkout_end > start:
            checkout_end = start
        checkout_block = source[checkout.start():checkout_end]
        path_match = re.search(r"(?m)^[ \t]*path:[ \t]*([^\n#]+)", checkout_block)
        if path_match and expand(path_match.group(1)) == output_path:
            error(f"{kind} output must not be an actions/checkout destination")

    upload = re.search(r"uses:[ \t]*actions/upload-artifact@[^\n]+", source[int(block["end"]):])
    if upload is None:
        error(f"{kind} output must be uploaded after harness execution")
        continue
    upload_start = int(block["end"]) + upload.start()
    upload_end = source.find("\n      - name:", upload_start + 1)
    if upload_end < 0:
        upload_end = len(source)
    upload_block = source[upload_start:upload_end]
    upload_path = re.search(r"(?m)^[ \t]*path:[ \t]*([^\n#]+)", upload_block)
    if upload_path is None or expand(upload_path.group(1)) != output_path:
        error(f"{kind} upload path must equal the harness-owned output path")

if "${{ matrix.profile }}" in source and any(
    "${{ matrix.profile }}" not in path for path in sample_paths
):
    error("sample output must be distinct for every matrix profile")
if len(sample_paths) != len(set(sample_paths)):
    error("sample profiles must not reuse an output directory")
if len(all_output_paths) != len(blocks):
    error("sample and decision commands must use distinct output directories")

generated: set[str] = set()
for name, value in assignments.items():
    if re.search(r"mktemp|openssl[ \t]+rand|uuidgen|\$RANDOM", value):
        generated.add(name)
for name in generated:
    mask = re.search(r"::add-mask::[^\n]*(?:\$\{" + re.escape(name) + r"\}|\$" + re.escape(name) + r"\b)", source)
    prints = [
        match
        for match in re.finditer(
            r"(?m)^[ \t]*(?:echo|printf)[^\n]*(?:\$\{" + re.escape(name) + r"\}|\$" + re.escape(name) + r"\b)",
            source,
        )
        if "::add-mask::" not in match.group(0)
    ]
    if prints and (mask is None or prints[0].start() < mask.start()):
        error("generated dynamic paths and identifiers must be masked before printing")

if re.search(r"--mode[ \t]+production\b", source):
    error("paired launcher must remain diagnostic-only")
if re.search(r"reference-v1\.13\.json|threshold-policy-v1\.13\.json|--manifest|--threshold", source):
    error("paired launcher must not create manifest or threshold authority")

if errors:
    for message in errors:
        print(f"[audit] ERROR: {message}", file=sys.stderr)
    raise SystemExit(1)
print("[audit] ok: paired launcher preserves harness-owned nonexistent output children")
print("[audit] ok: paired launcher uploads the exact sample and decision outputs")
print("[audit] ok: static GitHub runtime aliases are permitted metadata")
PY
}

check_paired_launcher() {
  local file="$1"
  local check_status=0
  local prohibited_env='^[[:space:]]+(COLDKEEP_KEY|COLDKEEP_AES_GCM_FIXTURE_HEX|DB_HOST|DB_PORT|DB_USER|DB_PASSWORD|DB_NAME|DB_SSLMODE|POSTGRES_USER|POSTGRES_PASSWORD|POSTGRES_DB):'

  echo "[audit] checking paired diagnostic launcher confidentiality and lifecycle"
  if [[ ! -f "$file" || -L "$file" ]]; then
    echo "[audit] ERROR: paired launcher must be a regular non-symlink file" >&2
    return 1
  fi
  require_pattern "$file" '^    timeout-minutes: 45$' 'paired launcher outer timeout is 45 minutes' || check_status=1
  require_pattern "$file" 'ci-paired-w1-v2' 'paired launcher selects workers=1 v2 fixture' || check_status=1
  require_pattern "$file" 'ci-paired-w4-v2' 'paired launcher selects workers=4 v2 fixture' || check_status=1
  require_pattern "$file" '--pairs 10' 'paired launcher retains ten diagnostic pairs' || check_status=1
  require_pattern "$file" '--command-timeout-seconds 600' 'paired launcher retains 600-second command safety timeout' || check_status=1
  require_pattern "$file" 'paired_benchmark_gate\.py sample' 'paired launcher uses strict sampler' || check_status=1
  require_pattern "$file" 'paired_benchmark_gate\.py decision' 'paired launcher writes a matrix decision' || check_status=1
  require_pattern "$file" 'if: \$\{\{ always\(\) \}\}' 'paired launcher always uploads finalized evidence' || check_status=1
  require_pattern "$file" 'set \+x' 'paired launcher disables shell tracing before sensitive setup' || check_status=1
  for variable in GITHUB_WORKSPACE RUNNER_TEMP HOME; do
    require_pattern "$file" "::add-mask::.*\\\$${variable}" "paired launcher masks ${variable}" || check_status=1
  done
  if grep -Eq '^    services:|^      services:' "$file"; then
    echo "[audit] ERROR: paired launcher must provision its isolated container after masking" >&2
    check_status=1
  else
    echo "[audit] ok: paired launcher does not use pre-step service provisioning"
  fi
  if grep -Eq "$prohibited_env" "$file"; then
    echo "[audit] ERROR: paired launcher exposes prohibited values through YAML env" >&2
    check_status=1
  else
    echo "[audit] ok: paired launcher has no prohibited YAML env exposure"
  fi
  if grep -Eq 'GITHUB_ENV|set -x' "$file"; then
    echo "[audit] ERROR: paired launcher persists or traces sensitive runtime values" >&2
    check_status=1
  else
    echo "[audit] ok: paired launcher neither persists nor traces sensitive runtime values"
  fi
  if grep -Eq 'echo.*(GITHUB_WORKSPACE|RUNNER_TEMP|COLDKEEP_KEY|DB_PASSWORD|DB_NAME)' "$file" \
    && ! grep -Eq '::add-mask::.*(GITHUB_WORKSPACE|RUNNER_TEMP)' "$file"; then
    echo "[audit] ERROR: paired launcher may print a prohibited path or runtime value" >&2
    check_status=1
  fi
  check_paired_launcher_output_ownership "$file" || check_status=1
  return "$check_status"
}

check_local_workflow() {
  local check_status=0
  local adversarial_block=""
  local benchmark_authorize_block=""
  local benchmark_calibration_block=""
  local deterministic_g6_block=""
  local benchmark_permissions_block=""
  local benchmark_sample_block=""
  local cache_disabled_count=0
  local checkout_count=0
  local credential_disabled_count=0
  local quality_block=""
  local quality_checkout_block=""
  local current_branch=""
  local lineage_candidate_ref=""
  local lineage_identity_valid=0
  local pr_identity=""
  local validation_ref=""
  local validator_test_block=""
  local validator_real_block=""
	local benchmark_integrity_block=""
	local benchmark_timing_block=""
	local correctness_matrix_block=""
  local postgres_internal_contracts_block=""
  local setup_go_count=0
  local trusted_checkout_count=0
  local upload_v5_count=0
  local upload_v6_count=0
  local upload_v7_count=0

  echo "[audit] checking local workflow invariants"
  require_executable_file "$SNAPSHOT_EVIDENCE_VALIDATOR_FILE" 'tracked-source snapshot evidence validator' || check_status=1
  require_executable_file "$RELEASE_LINEARITY_VALIDATOR_FILE" 'branch-relative release-linearity validator' || check_status=1
  require_executable_file "$RELEASE_BENCHMARK_EVIDENCE_FILE" 'release benchmark evidence lifecycle validator' || check_status=1
  require_executable_file "$RELEASE_BENCHMARK_RUNNER_FILE" 'external-transient release benchmark evidence runner' || check_status=1
  # shellcheck disable=SC2016 # Match literal validator variables and pathspec.
  require_pattern "$SNAPSHOT_EVIDENCE_VALIDATOR_FILE" 'git -C "\$repo_root" grep -F -- "func \$\{name\}\(" -- '\''\*\.go'\''' 'snapshot evidence validator enumerates tracked Go source only' || check_status=1
  # shellcheck disable=SC2016 # Match literal validator variables.
  require_pattern "$RELEASE_LINEARITY_VALIDATOR_FILE" 'merge-base "\$candidate_commit" refs/remotes/origin/main' 'release-linearity validator computes the candidate-relative merge base' || check_status=1
  # shellcheck disable=SC2016 # Match literal validator variables.
  require_pattern "$RELEASE_LINEARITY_VALIDATOR_FILE" 'rev-list --merges "\$\{base\}\.\.\$\{candidate_commit\}"' 'release-linearity validator rejects only post-base candidate merges' || check_status=1
  require_pattern "$RELEASE_BENCHMARK_RUNNER_FILE" 'mktemp -d' 'release benchmark runner uses an external transient root' || check_status=1
  require_pattern "$RELEASE_BENCHMARK_EVIDENCE_FILE" '\.release-evidence/v1\.13\.14' 'release benchmark lifecycle uses the canonical repository evidence root' || check_status=1
  if ! "$SNAPSHOT_EVIDENCE_VALIDATOR_FILE" --repo-root "$REPO_ROOT"; then
    echo "[audit] ERROR: tracked-source snapshot evidence validation failed" >&2
    check_status=1
  fi
  if ! "$RELEASE_BENCHMARK_EVIDENCE_FILE" inventory --repo-root "$REPO_ROOT"; then
    echo "[audit] ERROR: release benchmark evidence inventory failed" >&2
    check_status=1
  fi
  current_branch=$(git -C "$REPO_ROOT" symbolic-ref --quiet --short HEAD 2>/dev/null || true)
  validation_ref="${GITHUB_HEAD_REF:-$current_branch}"
  if [[ "$validation_ref" == release/* || "${GITHUB_REF_TYPE:-}" == "tag" && "${GITHUB_REF_NAME:-}" == v* ]]; then
    lineage_candidate_ref=HEAD
    lineage_identity_valid=1
    if [[ -n "${GITHUB_HEAD_REF:-}" ]]; then
      if [[ "${GITHUB_EVENT_NAME:-}" != "pull_request" ]]; then
        echo "[audit] ERROR: release PR head identity requires GITHUB_EVENT_NAME=pull_request" >&2
        lineage_identity_valid=0
      elif [[ -z "${GITHUB_REPOSITORY:-}" ]]; then
        echo "[audit] ERROR: release PR head identity requires GITHUB_REPOSITORY" >&2
        lineage_identity_valid=0
      elif [[ -z "${GITHUB_EVENT_PATH:-}" || ! -r "${GITHUB_EVENT_PATH:-}" ]]; then
        echo "[audit] ERROR: release PR head identity requires a readable GITHUB_EVENT_PATH" >&2
        lineage_identity_valid=0
      else
        pr_identity=$(jq -cer '
          .pull_request as $pr
          | select($pr.base.ref == "main")
          | select($pr.head.ref == env.GITHUB_HEAD_REF)
          | select($pr.head.repo.full_name == env.GITHUB_REPOSITORY)
          | $pr.head.sha
          | select(type == "string" and test("^[0-9a-f]{40}$"))
        ' "$GITHUB_EVENT_PATH" 2>/dev/null || true)
        if [[ -z "$pr_identity" ]]; then
          echo "[audit] ERROR: release PR event identity is malformed, non-main, or not from the authoritative repository" >&2
          lineage_identity_valid=0
        elif ! git -C "$REPO_ROOT" rev-parse --verify --quiet --end-of-options "${pr_identity}^{commit}" >/dev/null; then
          echo "[audit] ERROR: release PR head SHA does not resolve to a local commit: $pr_identity" >&2
          lineage_identity_valid=0
        else
          lineage_candidate_ref="$pr_identity"
          echo "[audit] ok: release PR lineage candidate is authoritative same-repository head $lineage_candidate_ref"
        fi
      fi
    fi
    if [[ "$lineage_identity_valid" -ne 1 ]]; then
      check_status=1
    elif ! "$RELEASE_LINEARITY_VALIDATOR_FILE" --repo-root "$REPO_ROOT" --candidate-ref "$lineage_candidate_ref"; then
      echo "[audit] ERROR: current release candidate contains a release-local merge" >&2
      check_status=1
    fi
  else
    echo "[audit] ok: real release-linearity check not required for context ${validation_ref:-detached}"
  fi
  require_pattern "$WORKFLOW_FILE" 'name: CI' 'CI workflow file' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'uses:\s*golangci/golangci-lint-action@v9' 'hosted quality uses golangci-lint action v9' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'version:\s*v2\.6\.2' 'hosted quality pins golangci-lint v2.6.2' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'name:\s*Test candidate lint gate contract' 'hosted quality tests the candidate lint gate contract' || check_status=1
  require_pattern "$WORKFLOW_FILE" "go test -count=1 ./scripts -run '\^TestCandidateLintGate'" 'hosted quality runs candidate lint gate regression tests' || check_status=1
  require_pattern "$CANDIDATE_LINT_GATE_FILE" '^set -Eeuo pipefail$' 'candidate lint gate enables fail-closed shell semantics' || check_status=1
  require_pattern "$CANDIDATE_LINT_GATE_FILE" '^set -o pipefail$' 'candidate lint gate preserves pipeline failures' || check_status=1
  require_pattern "$CANDIDATE_LINT_GATE_FILE" '^readonly EXPECTED_GOLANGCI_LINT_VERSION="2\.6\.2"$' 'candidate lint gate pins golangci-lint v2.6.2' || check_status=1
  require_pattern "$CANDIDATE_LINT_GATE_FILE" 'config path' 'candidate lint gate resolves the effective repository config' || check_status=1
  require_pattern "$CANDIDATE_LINT_GATE_FILE" 'config verify' 'candidate lint gate verifies the effective repository config' || check_status=1
  require_pattern "$CANDIDATE_LINT_GATE_FILE" 'pipeline_status=\("\$\{PIPESTATUS\[@\]\}"\)' 'candidate lint gate captures lint and tee pipeline statuses' || check_status=1
  # shellcheck disable=SC2016 # The audit pattern must match the literal gate variables.
  require_pattern "$CANDIDATE_LINT_GATE_FILE" 'grep -Eq -- "\$FINDING_PATTERN" "\$LINT_LOG"' 'candidate lint gate rejects findings independently of wrapper status' || check_status=1
  require_pattern "$CANDIDATE_LINT_GATE_FILE" 'LOCAL_CANDIDATE_LINT=PASS' 'candidate lint gate emits PASS only after evidence verification' || check_status=1
  require_pattern "$BENCHMARK_BASELINE_WORKFLOW_FILE" '^name: Benchmark Gate Calibration and Baseline Capture$' 'manual benchmark calibration workflow' || check_status=1
  require_pattern "$BENCHMARK_BASELINE_WORKFLOW_FILE" '^  workflow_dispatch:$' 'benchmark calibration is manually dispatched' || check_status=1
  require_pattern "$BENCHMARK_BASELINE_WORKFLOW_FILE" '^  contents: read$' 'benchmark calibration has read-only repository permission' || check_status=1
  benchmark_authorize_block="$(extract_job_block_from_file "$BENCHMARK_BASELINE_WORKFLOW_FILE" authorize)"
  benchmark_sample_block="$(extract_job_block_from_file "$BENCHMARK_BASELINE_WORKFLOW_FILE" sample)"
  benchmark_calibration_block="$(extract_job_block_from_file "$BENCHMARK_BASELINE_WORKFLOW_FILE" calibration)"
  benchmark_permissions_block="$(awk '
    /^permissions:$/ { in_permissions = 1 }
    in_permissions && /^env:$/ { exit }
    in_permissions { print }
  ' "$BENCHMARK_BASELINE_WORKFLOW_FILE")"
  if [[ "$benchmark_permissions_block" != $'permissions:\n  contents: read' ]]; then
    echo "[audit] ERROR: benchmark workflow permissions must be exactly contents read" >&2
    check_status=1
  else
    echo "[audit] ok: benchmark workflow permissions are exactly contents read"
  fi
  require_content_pattern "$benchmark_authorize_block" 'if \[\[ "\$\{TRUSTED_REF\}" != "refs/heads/main" \]\]; then' 'benchmark authorization is fail-closed on refs/heads/main' || check_status=1
  require_content_pattern "$benchmark_authorize_block" 'if ! \[\[ "\$\{SOURCE_SHA\}" =~ \^\[0-9a-f\]\{40\}\$ \]\]; then' 'benchmark source_sha uses strict lowercase full-SHA validation' || check_status=1
  require_content_pattern "$benchmark_authorize_block" 'if \[\[ "\$\{SOURCE_SHA\}" != "\$\{TRUSTED_SHA\}" \]\]; then' 'benchmark source_sha must equal trusted github.sha' || check_status=1
  require_content_pattern "$benchmark_sample_block" '^    needs: authorize$' 'benchmark sample job depends on trusted-source authorization' || check_status=1
  if grep -Eq 'ref:.*inputs\.source_sha' "$BENCHMARK_BASELINE_WORKFLOW_FILE"; then
    echo "[audit] ERROR: benchmark checkout cannot use inputs.source_sha" >&2
    check_status=1
  else
    echo "[audit] ok: benchmark checkout does not use inputs.source_sha"
  fi
  checkout_count="$(grep -Ec 'uses: actions/checkout@' "$BENCHMARK_BASELINE_WORKFLOW_FILE")"
  trusted_checkout_count="$(grep -Fc "ref: \${{ github.sha }}" "$BENCHMARK_BASELINE_WORKFLOW_FILE")"
  credential_disabled_count="$(grep -Ec '^\s+persist-credentials: false$' "$BENCHMARK_BASELINE_WORKFLOW_FILE")"
  if [[ "$checkout_count" -eq 0 || "$trusted_checkout_count" -ne "$checkout_count" ]]; then
    echo "[audit] ERROR: benchmark checkouts must use trusted github.sha" >&2
    check_status=1
  else
    echo "[audit] ok: benchmark checkouts use trusted github.sha"
  fi
  if [[ "$credential_disabled_count" -ne "$checkout_count" ]]; then
    echo "[audit] ERROR: benchmark checkouts must disable persisted credentials" >&2
    check_status=1
  else
    echo "[audit] ok: benchmark checkouts disable persisted credentials"
  fi
  setup_go_count="$(grep -Ec 'uses: actions/setup-go@' "$BENCHMARK_BASELINE_WORKFLOW_FILE")"
  cache_disabled_count="$(grep -Ec '^\s+cache: false$' "$BENCHMARK_BASELINE_WORKFLOW_FILE")"
  if [[ "$setup_go_count" -eq 0 || "$cache_disabled_count" -ne "$setup_go_count" ]] \
    || grep -Eq '^\s+cache: true$|uses: actions/cache@' "$BENCHMARK_BASELINE_WORKFLOW_FILE"; then
    echo "[audit] ERROR: benchmark setup-go caching must be disabled" >&2
    check_status=1
  else
    echo "[audit] ok: benchmark setup-go caching is disabled"
  fi
  require_content_pattern "$benchmark_sample_block" '^\s+python3 scripts/benchmark_gate\.py sample \\$' 'benchmark sample harness runs from trusted checkout' || check_status=1
  require_content_pattern "$benchmark_calibration_block" '^\s+python3 scripts/benchmark_gate\.py calibrate \\$' 'benchmark calibration harness runs from trusted checkout' || check_status=1
  require_content_pattern "$benchmark_calibration_block" 'path: \$\{\{ runner\.temp \}\}/benchmark-calibration-input' 'benchmark calibration artifacts use runner.temp' || check_status=1
  require_content_pattern "$benchmark_calibration_block" 'actual = report\.get\("provenance", \{\}\)\.get\("source_commit"\)' 'benchmark calibration reads artifact source provenance' || check_status=1
  require_content_pattern "$benchmark_calibration_block" 'if actual != expected:' 'benchmark calibration requires artifact provenance to match github.sha' || check_status=1
  if grep -Eq 'continue-on-error|\|\| true|^\s+set \+e$' <<<"$benchmark_authorize_block"; then
    echo "[audit] ERROR: benchmark source validation must not use broad failure suppression" >&2
    check_status=1
  else
    echo "[audit] ok: benchmark source validation has no broad failure suppression"
  fi
  require_pattern "$BENCHMARK_BASELINE_WORKFLOW_FILE" 'runs-on: ubuntu-24\.04' 'benchmark calibration pins the runner family' || check_status=1
  require_pattern "$BENCHMARK_BASELINE_WORKFLOW_FILE" "go-version: '1\\.26\\.7'" 'benchmark calibration pins the certified Go patch' || check_status=1
  require_pattern "$BENCHMARK_BASELINE_WORKFLOW_FILE" '^  GOTOOLCHAIN: local$' 'benchmark calibration disables automatic toolchain switching' || check_status=1
  require_pattern "$BENCHMARK_BASELINE_WORKFLOW_FILE" '^\s+check-latest: false$' 'benchmark calibration disables latest-version resolution' || check_status=1
  require_pattern "$BENCHMARK_BASELINE_WORKFLOW_FILE" 'postgres:16@sha256:33f923b05f64ca54ac4401c01126a6b92afe839a0aa0a52bc5aeb5cc958e5f20' 'benchmark calibration pins the PostgreSQL image digest' || check_status=1
  require_pattern "$BENCHMARK_BASELINE_WORKFLOW_FILE" '^\s+compression: \[none, zstd\]$' 'benchmark calibration fixes compression profiles' || check_status=1
  require_pattern "$BENCHMARK_BASELINE_WORKFLOW_FILE" '^\s+workers: \[1, 4\]$' 'benchmark calibration fixes worker profiles' || check_status=1
  require_pattern "$BENCHMARK_BASELINE_WORKFLOW_FILE" '^\s+replicate: \[1, 2\]$' 'benchmark calibration uses two independent matrix jobs' || check_status=1
  require_pattern "$BENCHMARK_BASELINE_WORKFLOW_FILE" '^\s+sample_count=10$' 'benchmark calibration fixes ten measured samples' || check_status=1
  require_pattern "$BENCHMARK_BASELINE_WORKFLOW_FILE" '^\s+sample_count=5$' 'benchmark capture fixes five measured samples' || check_status=1
  require_pattern "$BENCHMARK_BASELINE_WORKFLOW_FILE" 'python3 scripts/benchmark_gate\.py sample' 'benchmark calibration uses the strict sampler' || check_status=1
  require_pattern "$BENCHMARK_BASELINE_WORKFLOW_FILE" '^\s+--dataset ci-stable-v1 \\$' 'benchmark calibration fixes the fixture identity' || check_status=1
  require_pattern "$BENCHMARK_BASELINE_WORKFLOW_FILE" '^\s+--warmups 1 \\$' 'benchmark calibration fixes one excluded warmup' || check_status=1
  require_pattern "$BENCHMARK_BASELINE_WORKFLOW_FILE" 'python3 scripts/benchmark_gate\.py calibrate' 'benchmark calibration evaluates the fixed matrix' || check_status=1
  require_pattern "$WORKFLOW_FILE" '^  benchmark-integrity:$' 'hard benchmark integrity job family' || check_status=1
  require_pattern "$WORKFLOW_FILE" '^  benchmark-timing-advisory:$' 'hosted benchmark timing advisory job family' || check_status=1
  benchmark_integrity_block="$(extract_job_block benchmark-integrity)"
  benchmark_timing_block="$(extract_job_block benchmark-timing-advisory)"
  require_content_pattern "$benchmark_integrity_block" 'ci-paired-w1-v2' 'integrity matrix selects the bounded workers=1 fixture' || check_status=1
  require_content_pattern "$benchmark_integrity_block" 'ci-paired-w4-v2' 'integrity matrix selects the bounded workers=4 fixture' || check_status=1
  require_content_pattern "$benchmark_integrity_block" 'python3 scripts/benchmark_gate\.py integrity' 'integrity matrix uses the hard candidate-only interface' || check_status=1
  require_content_pattern "$benchmark_integrity_block" '--command-timeout-seconds 600' 'integrity matrix fixes the 600-second command timeout' || check_status=1
  require_content_pattern "$benchmark_integrity_block" "go-version: '1\.26\.7'" 'integrity matrix pins the certified Go patch version' || check_status=1
  require_content_pattern "$benchmark_integrity_block" 'postgres:16@sha256:33f923b05f64ca54ac4401c01126a6b92afe839a0aa0a52bc5aeb5cc958e5f20' 'integrity matrix pins PostgreSQL by digest' || check_status=1
  require_content_pattern "$benchmark_integrity_block" 'if-no-files-found: error' 'integrity artifact rejects missing evidence' || check_status=1
  require_content_pattern "$benchmark_integrity_block" 'if: \$\{\{ always\(\) \}\}' 'integrity artifact finalization and upload always run' || check_status=1
  require_content_pattern "$benchmark_integrity_block" 'sha256sum --check checksums\.sha256' 'integrity artifact checksum inventory is verified' || check_status=1
  require_content_pattern "$benchmark_timing_block" '^\s+--dataset small \\$' 'timing advisory retains the historical small fixture' || check_status=1
  require_content_pattern "$benchmark_timing_block" 'scripts/validate_regression_thresholds\.py check' 'timing advisory retains the historical comparator' || check_status=1
  require_content_pattern "$benchmark_timing_block" '--policy hosted-advisory' 'timing comparator has informational authority' || check_status=1
  require_content_pattern "$benchmark_timing_block" '^\s+set \+e$' 'timing advisory disables errexit only for comparator evaluation' || check_status=1
  require_content_pattern "$benchmark_timing_block" '^\s+set -e$' 'timing advisory restores errexit immediately after comparator evaluation' || check_status=1
  if ! grep -A1 -E '^\s+comparator_exit=\$\?$' <<<"$benchmark_timing_block" | grep -Eq '^\s+set -e$'; then
    echo "[audit] ERROR: timing advisory must restore errexit immediately after capturing comparator exit" >&2
    check_status=1
  else
    echo "[audit] ok: timing advisory restores errexit immediately after capturing comparator exit"
  fi
  require_content_pattern "$benchmark_timing_block" '\[\[ -s "\$\{report\}" \]\]' 'timing advisory requires a machine-readable report' || check_status=1
  require_content_pattern "$benchmark_timing_block" 'verify-advisory-exit' 'timing advisory verifies exact classification and exit code' || check_status=1
  require_content_pattern "$benchmark_timing_block" 'comparator_exit=\$\?' 'timing advisory captures the comparator exit exactly' || check_status=1
  require_content_pattern "$benchmark_timing_block" '^\s+0\|10\|11\|12\)$' 'timing advisory narrowly accepts valid informational exit codes' || check_status=1
  require_content_pattern "$benchmark_timing_block" '^\s+2\)$' 'timing advisory preserves evaluator exit code 2 as failure' || check_status=1
  if ! grep -A1 -E '^\s+2\)$' <<<"$benchmark_timing_block" | grep -Eq '^\s+exit 2$'; then
    echo "[audit] ERROR: timing advisory must return failure for evaluator exit code 2" >&2
    check_status=1
  else
    echo "[audit] ok: timing advisory returns failure for evaluator exit code 2"
  fi
  require_content_pattern "$benchmark_timing_block" 'GITHUB_STEP_SUMMARY' 'timing advisory publishes its classification summary' || check_status=1
  require_content_pattern "$benchmark_timing_block" 'historical_v1\.9|benchmarks/v1\.9/baselines' 'timing advisory alone cites historical v1.9 baselines' || check_status=1
  require_content_pattern "$benchmark_timing_block" 'if-no-files-found: error' 'timing artifact rejects missing evidence' || check_status=1
  require_content_pattern "$benchmark_timing_block" 'if: \$\{\{ always\(\) \}\}' 'timing artifact upload always runs' || check_status=1
  require_content_pattern "$benchmark_timing_block" 'actual_inventory=.*find .*checksums\.sha256' 'timing artifact inventory is enumerated exhaustively' || check_status=1
  require_content_pattern "$benchmark_timing_block" 'benchmark\.json\\ntiming-advisory\.json' 'timing artifact inventory is restricted to the report and observation' || check_status=1
  require_content_pattern "$benchmark_timing_block" 'sha256sum benchmark\.json timing-advisory\.json > checksums\.sha256' 'timing artifact creates exhaustive checksums' || check_status=1
  require_content_pattern "$benchmark_timing_block" 'sha256sum --check checksums\.sha256' 'timing artifact verifies checksums' || check_status=1
  checksum_line="$(grep -nEm1 'sha256sum --check checksums\.sha256' <<<"$benchmark_timing_block" | cut -d: -f1 || true)"
  evaluator_failure_line="$(grep -nEm1 '^\s+2\)$' <<<"$benchmark_timing_block" | cut -d: -f1 || true)"
  if [[ -z "$checksum_line" || -z "$evaluator_failure_line" || "$checksum_line" -ge "$evaluator_failure_line" ]]; then
    echo "[audit] ERROR: timing checksums must be finalized before evaluator code 2 fails the job" >&2
    check_status=1
  else
    echo "[audit] ok: timing checksums are finalized before evaluator code 2 fails the job"
  fi
  require_pattern "$TIMING_VALIDATOR_FILE" '^TIMING_ROW_OPTIONAL_FIELDS = \{"diagnostic_final_state"\}$' 'historical timing treats diagnostic final state as optional' || check_status=1
  require_pattern "$TIMING_VALIDATOR_FILE" 'not legacy and "diagnostic_final_state" in row' 'optional timing diagnostic final state is validated when present' || check_status=1
  require_pattern "$TIMING_VALIDATOR_FILE" '"BENCHMARK_TIMING_EVALUATION_FAILURE": 2' 'timing evaluator failure maps exactly to exit code 2' || check_status=1
  require_pattern "$TIMING_VALIDATOR_FILE" '^EXECUTION_STATS_OMITTABLE_ZERO_FIELDS = \{' 'timing validator models Go omitempty counters explicitly' || check_status=1
  omitempty_block="$(awk '
    /^EXECUTION_STATS_OMITTABLE_ZERO_FIELDS = \{/ { in_block = 1 }
    in_block { print }
    in_block && /^\}$/ { exit }
  ' "$TIMING_VALIDATOR_FILE")"
  for field in container_append_count fsync_count container_open_count container_close_count snapshot_metadata_write_count; do
    require_content_pattern "$omitempty_block" "\"${field}\"" "timing validator models Go omitempty field ${field}" || check_status=1
  done
  if grep -Eq 'benchmark_contract\.hard_final_state' "$TIMING_VALIDATOR_FILE"; then
    echo "[audit] ERROR: historical timing advisory must not require hard diagnostic final state" >&2
    check_status=1
  else
    echo "[audit] ok: historical timing advisory does not require hard diagnostic final state"
  fi
  require_pattern "$BENCHMARK_GATE_FILE" '^RAW_ROW_FIELDS = \{' 'hard integrity keeps an explicit raw row contract' || check_status=1
  require_pattern "$BENCHMARK_GATE_FILE" '^def hard_final_state\(' 'hard integrity still requires diagnostic final-state authority' || check_status=1
  require_pattern "$BENCHMARK_GATE_FILE" '^INTEGRITY_SAMPLE_COUNT = 2$' 'integrity interface fixes two candidate samples' || check_status=1
  require_pattern "$BENCHMARK_GATE_FILE" '^INTEGRITY_COMMAND_TIMEOUT_SECONDS = 600$' 'integrity interface fixes the command ceiling' || check_status=1
  require_pattern "$BENCHMARK_GATE_FILE" '"warmup_count": 0' 'integrity interface has no warmup invocation' || check_status=1
  require_pattern "$BENCHMARK_GATE_FILE" '"performance_authority": False' 'integrity evidence is ineligible for performance authority' || check_status=1
  workflow_baseline_count=$(grep -c 'benchmarks/v1\.9/baselines/' "$WORKFLOW_FILE" || true)
  timing_baseline_count=$(grep -c 'benchmarks/v1\.9/baselines/' <<<"$benchmark_timing_block" || true)
  if [[ "$workflow_baseline_count" -ne "$timing_baseline_count" ]]; then
    echo "[audit] ERROR: historical baselines appear outside timing advisory policy" >&2
    check_status=1
  else
    echo "[audit] ok: historical baselines appear only under timing advisory policy"
  fi
  if grep -Eq 'continue-on-error|\|\| true' <<<"$benchmark_integrity_block$benchmark_timing_block"; then
    echo "[audit] ERROR: benchmark integrity or advisory execution uses broad failure suppression" >&2
    check_status=1
  else
    echo "[audit] ok: benchmark execution has no broad failure suppression"
  fi
  for profile in none-w1 none-w4 zstd-w1 zstd-w4; do
    if [[ "$(grep -c -- "profile: $profile" <<<"$benchmark_integrity_block")" -ne 1 ]]; then
      echo "[audit] ERROR: integrity matrix must contain profile $profile exactly once" >&2
      check_status=1
    fi
    if [[ "$(grep -c -- "profile: $profile" <<<"$benchmark_timing_block")" -ne 1 ]]; then
      echo "[audit] ERROR: timing advisory matrix must contain profile $profile exactly once" >&2
      check_status=1
    fi
  done
  if [[ "$(grep -c -- 'dataset: ci-paired-w1-v2' <<<"$benchmark_integrity_block")" -ne 2 ]]; then
    echo "[audit] ERROR: integrity matrix must bind exactly two profiles to the bounded workers=1 fixture" >&2
    check_status=1
  fi
  if [[ "$(grep -c -- 'dataset: ci-paired-w4-v2' <<<"$benchmark_integrity_block")" -ne 2 ]]; then
    echo "[audit] ERROR: integrity matrix must bind exactly two profiles to the bounded workers=4 fixture" >&2
    check_status=1
  fi
  if [[ -e "$PAIRED_REFERENCE_MANIFEST_FILE" ]]; then
    echo "[audit] ERROR: paired reference manifest exists before governance authorization" >&2
    check_status=1
  else
    echo "[audit] ok: no paired reference manifest exists"
  fi
  if [[ -e "$PAIRED_THRESHOLD_POLICY_FILE" ]]; then
    echo "[audit] ERROR: paired threshold policy exists before threshold authorization" >&2
    check_status=1
  else
    echo "[audit] ok: no paired threshold policy exists"
  fi
  if grep -Eq '^  (push|pull_request|merge_group|schedule):' "$BENCHMARK_BASELINE_WORKFLOW_FILE"; then
    echo "[audit] ERROR: benchmark calibration workflow must remain manual-only" >&2
    check_status=1
  else
    echo "[audit] ok: benchmark calibration workflow has no automatic trigger"
  fi
  if grep -Eq '^\s+(contents|actions|checks|issues|pull-requests):\s*write|^\s*write-all\s*$' "$BENCHMARK_BASELINE_WORKFLOW_FILE"; then
    echo "[audit] ERROR: benchmark calibration workflow must not receive write permission" >&2
    check_status=1
  else
    echo "[audit] ok: benchmark calibration workflow has no repository write permission"
  fi
  if grep -Eqi 'git (commit|push)|gh (pr|release)|create-pull-request' "$BENCHMARK_BASELINE_WORKFLOW_FILE"; then
    echo "[audit] ERROR: benchmark calibration workflow must remain artifact-only" >&2
    check_status=1
  else
    echo "[audit] ok: benchmark calibration workflow cannot commit, push, release, or open a pull request"
  fi
  if grep -Eq 'scripts/(benchmark_gate\.py (sample|compare)|paired_benchmark_gate\.py)' "$WORKFLOW_FILE"; then
    echo "[audit] ERROR: required CI contains an unauthorized benchmark sampler, comparator, or paired gate" >&2
    check_status=1
  else
    echo "[audit] ok: required CI uses only the authorized integrity and advisory interfaces"
  fi
  if grep -Eqi 'paired_benchmark_gate|benchmark-paired|paired[ _-]benchmark' "$WORKFLOW_FILE"; then
    echo "[audit] ERROR: required CI contains a premature paired benchmark job or dependency" >&2
    check_status=1
  else
    echo "[audit] ok: required CI contains no paired benchmark job or dependency"
  fi
  require_pattern "$WORKFLOW_FILE" '^  push:$' 'CI push trigger' || check_status=1
  require_pattern "$WORKFLOW_FILE" '^\s+- main$' 'CI push branch retains main' || check_status=1
  require_pattern "$WORKFLOW_FILE" '^\s+- release/\*\*$' 'CI push branch includes release/**' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'tags:\s*\[\s*"v\*"\s*\]' 'release tag trigger (v*)' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'merge_group:' 'merge queue trigger' || check_status=1
  require_pattern "$WORKFLOW_FILE" '^  workflow_dispatch:\s*$' 'CI workflow_dispatch trigger' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'name:\s*CI Required Gate' 'aggregate required gate job' || check_status=1
  ci_setup_go_count="$(grep -Ec 'uses: actions/setup-go@' "$WORKFLOW_FILE")"
  ci_exact_go_count="$(grep -Ec "^\s+go-version: '1\\.26\\.7'$" "$WORKFLOW_FILE")"
  ci_check_latest_false_count="$(grep -Ec '^\s+check-latest: false$' "$WORKFLOW_FILE")"
  if [[ "$ci_setup_go_count" -eq 0 || "$ci_exact_go_count" -ne "$ci_setup_go_count" ]]; then
    echo "[audit] ERROR: every required CI setup-go step must pin Go 1.26.7 exactly" >&2
    check_status=1
  else
    echo "[audit] ok: every required CI setup-go step pins Go 1.26.7 exactly"
  fi
  if [[ "$ci_check_latest_false_count" -ne "$ci_setup_go_count" ]]; then
    echo "[audit] ERROR: every required CI setup-go step must disable latest-version resolution" >&2
    check_status=1
  else
    echo "[audit] ok: every required CI setup-go step disables latest-version resolution"
  fi
  require_pattern "$WORKFLOW_FILE" '^  GOTOOLCHAIN: local$' 'required CI disables automatic toolchain switching' || check_status=1
  codeql_setup_go_count="$(grep -Ec 'uses: actions/setup-go@' "$CODEQL_WORKFLOW_FILE")"
  codeql_exact_go_count="$(grep -Ec "^\s+go-version: '1\\.26\\.7'$" "$CODEQL_WORKFLOW_FILE")"
  codeql_check_latest_false_count="$(grep -Ec '^\s+check-latest: false$' "$CODEQL_WORKFLOW_FILE")"
  if [[ "$codeql_setup_go_count" -eq 0 || "$codeql_exact_go_count" -ne "$codeql_setup_go_count" || "$codeql_check_latest_false_count" -ne "$codeql_setup_go_count" ]]; then
    echo "[audit] ERROR: CodeQL setup-go must pin Go 1.26.7 and disable latest-version resolution" >&2
    check_status=1
  else
    echo "[audit] ok: CodeQL setup-go pins Go 1.26.7 without latest-version resolution"
  fi
  require_pattern "$CODEQL_WORKFLOW_FILE" '^  GOTOOLCHAIN: local$' 'CodeQL disables automatic toolchain switching' || check_status=1
  upload_v5_count=$(grep -c 'actions/upload-artifact@v5' "$WORKFLOW_FILE" || true)
  upload_v6_count=$(grep -c 'actions/upload-artifact@v6' "$WORKFLOW_FILE" || true)
  upload_v7_count=$(grep -c 'actions/upload-artifact@v7' "$WORKFLOW_FILE" || true)
  if [[ "$upload_v7_count" -ne 6 || "$upload_v5_count" -ne 0 || "$upload_v6_count" -ne 0 ]]; then
    echo "[audit] ERROR: required CI expects exactly six upload-artifact@v7 uses and zero v5/v6 uses" >&2
    check_status=1
  else
    echo "[audit] ok: CI artifact uploads use actions/upload-artifact@v7 exactly six times"
  fi
  quality_block="$(extract_job_block quality)"
  if [[ -z "$quality_block" ]]; then
    echo "[audit] ERROR: missing quality job block content" >&2
    check_status=1
  else
    quality_checkout_block="$(extract_step_block_from_content "$quality_block" "Checkout")"
    quality_plain_block="$(extract_step_block_from_content "$quality_block" "Test packages (plain codec)")"
    quality_aes_gcm_block="$(extract_step_block_from_content "$quality_block" "Test packages (aes-gcm codec)")"
    validator_test_block="$(extract_step_block_from_content "$quality_block" "Test release-state validator")"
    python_suite_block="$(extract_step_block_from_content "$quality_block" "Run complete Python validation suite")"
    validator_real_block="$(extract_step_block_from_content "$quality_block" "Validate repository release state")"
    require_content_pattern "$quality_checkout_block" '^        uses: actions/checkout@v6$' 'quality checkout uses actions/checkout@v6' || check_status=1
    require_content_pattern "$quality_checkout_block" '^          fetch-depth: 0$' 'quality checkout fetch-depth is 0' || check_status=1
    require_content_pattern "$validator_test_block" '^      - name: Test release-state validator$' 'release-state validator test step' || check_status=1
    require_content_pattern "$validator_test_block" '^        run: python3 scripts/test_validate_release_state\.py$' 'release-state validator test command' || check_status=1
    require_content_pattern "$python_suite_block" '^      - name: Run complete Python validation suite$' 'complete Python validation suite step' || check_status=1
    require_content_pattern "$python_suite_block" "^        run: python3 -m unittest discover -s scripts -p 'test_\\*\\.py' -v$" 'canonical complete Python validation command' || check_status=1
    require_content_pattern "$validator_real_block" '^      - name: Validate repository release state$' 'release-state validator real-state step' || check_status=1
    require_content_pattern "$validator_real_block" '^        run: python3 scripts/validate_release_state\.py --state auto$' 'release-state validator real-state command' || check_status=1
    require_content_pattern "$validator_real_block" 'refs/heads/release/' 'release-state validator release branch condition' || check_status=1
    require_content_pattern "$validator_real_block" 'refs/heads/main' 'release-state validator main condition' || check_status=1
    require_content_pattern "$validator_real_block" 'refs/tags/v' 'release-state validator tag condition' || check_status=1
    require_content_pattern "$validator_real_block" 'github\.event_name == .pull_request.' 'release-state validator pull-request condition' || check_status=1
    require_content_pattern "$validator_real_block" 'github\.head_ref' 'release-state validator pull-request head condition' || check_status=1
    require_content_pattern "$quality_plain_block" '^          COLDKEEP_CODEC: plain$' 'SQLite quality plain codec environment' || check_status=1
    require_content_pattern "$quality_plain_block" '^        run: go test -race -count=1 \./cmd/\.\.\. \./internal/\.\.\.$' 'SQLite quality plain package command' || check_status=1
    require_content_pattern "$quality_aes_gcm_block" '^          COLDKEEP_CODEC: aes-gcm$' 'SQLite quality AES-GCM codec environment' || check_status=1
    require_content_pattern "$quality_aes_gcm_block" '^        run: go test -race -count=1 \./cmd/\.\.\. \./internal/\.\.\.$' 'SQLite quality AES-GCM package command' || check_status=1
    if grep -Eq 'continue-on-error|\|\| true' <<<"$validator_test_block$validator_real_block$python_suite_block"; then
      echo "[audit] ERROR: release-state and Python validation steps must remain blocking" >&2
      check_status=1
    else
      echo "[audit] ok: release-state and Python validation steps are blocking"
    fi
  fi
  require_pattern "$WORKFLOW_FILE" 'needs:\s*\[quality, correctness-matrix\]' 'smoke job depends on quality and correctness-matrix' || check_status=1
	correctness_matrix_block="$(extract_job_block correctness-matrix)"
	if [[ -z "$correctness_matrix_block" ]]; then
	  echo "[audit] ERROR: missing correctness-matrix job block content" >&2
	  check_status=1
	else
	  correctness_integration_block="$(extract_step_block_from_content "$correctness_matrix_block" "Run integration tests (correctness tier)")"
	  if [[ -z "$correctness_integration_block" ]]; then
	    echo "[audit] ERROR: missing integration correctness execution-proof step block" >&2
	    check_status=1
	  else
	    require_content_pattern "$correctness_integration_block" 'COLDKEEP_TEST_DB:\s*1' 'integration correctness execution proof enables DB gate' || check_status=1
	    require_content_pattern "$correctness_integration_block" 'go test -race -count=1 -short -json \./tests/integration/\.\.\.' 'integration correctness execution proof uses JSON evidence' || check_status=1
	    require_content_pattern "$correctness_integration_block" 'TestRoundTripStoreRestore' 'required PostgreSQL storage round-trip execution proof' || check_status=1
	    require_content_pattern "$correctness_integration_block" 'TestRemoveWithSharedChunksRefCount' 'required PostgreSQL storage remove execution proof' || check_status=1
	    require_content_pattern "$correctness_integration_block" 'TestStartupRecoveryResyncsPreexistingQuarantinedOrphanConflictState' 'required PostgreSQL recovery execution proof' || check_status=1
	    require_content_pattern "$correctness_integration_block" 'github.com/franchoy/coldkeep/tests/integration' 'integration correctness execution proof binds the integration package' || check_status=1
	    require_content_pattern "$correctness_integration_block" 'if codec == "plain"' 'integration correctness execution proof scopes recovery and remove markers to plain codec' || check_status=1
	    require_content_pattern "$correctness_integration_block" 'json\.loads\(raw_line\)' 'integration correctness execution proof rejects malformed JSON' || check_status=1
	    require_content_pattern "$correctness_integration_block" 'if not events:' 'integration correctness execution proof rejects empty JSON' || check_status=1
	    require_content_pattern "$correctness_integration_block" 'event\.get\("Action"\) == "skip"' 'integration correctness execution proof rejects required skips' || check_status=1
	    require_content_pattern "$correctness_integration_block" 'event\.get\("Action"\) == "pass"' 'integration correctness execution proof requires pass events' || check_status=1
	    require_content_pattern "$correctness_integration_block" 'print\("required execution-proof failure:", file=sys\.stderr\)' 'integration correctness execution-proof parser' || check_status=1
	    require_content_pattern "$correctness_integration_block" 'status=\$\{PIPESTATUS\[0\]\}' 'integration correctness execution proof preserves test status' || check_status=1
	    require_content_pattern "$correctness_integration_block" 'status=\$\?' 'integration correctness execution proof propagates parser status' || check_status=1
	    # shellcheck disable=SC2016 # The audit pattern must match the literal $status.
	    require_content_pattern "$correctness_integration_block" 'exit "\$status"' 'integration correctness execution proof remains blocking' || check_status=1
	    if grep -Eq 'continue-on-error|go test .*\|\| true' <<<"$correctness_integration_block"; then
	      echo "[audit] ERROR: integration correctness execution-proof step must not suppress broad failures" >&2
	      check_status=1
	    else
	      echo "[audit] ok: integration correctness execution-proof step does not suppress broad failures"
	    fi
	  fi
	  postgres_internal_contracts_block="$(extract_step_block_from_content "$correctness_matrix_block" "Run required PostgreSQL internal package contracts")"
	  if [[ -z "$postgres_internal_contracts_block" ]]; then
	    echo "[audit] ERROR: missing required PostgreSQL internal package contracts step block" >&2
	    check_status=1
	  else
	    require_content_pattern "$postgres_internal_contracts_block" "^      - name: Run required PostgreSQL internal package contracts$" 'required PostgreSQL internal package contracts step' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" "if: \\$\\{\\{ matrix\.codec == 'plain' \\}\\}" 'PostgreSQL internal package contracts run only for plain codec' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'COLDKEEP_TEST_DB:\s*1' 'PostgreSQL internal package contracts enable DB gate' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'COLDKEEP_DB_AUTO_BOOTSTRAP:\s*true' 'PostgreSQL internal package contracts enable auto-bootstrap' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'DB_HOST:\s*127\.0\.0\.1' 'PostgreSQL internal package contracts set DB host' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'DB_PORT:\s*5432' 'PostgreSQL internal package contracts set DB port' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'DB_USER:\s*coldkeep' 'PostgreSQL internal package contracts set DB user' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'DB_PASSWORD:\s*coldkeep' 'PostgreSQL internal package contracts set DB password' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'DB_NAME:\s*coldkeep' 'PostgreSQL internal package contracts set DB name' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'DB_SSLMODE:\s*disable' 'PostgreSQL internal package contracts set DB SSL mode' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'go test -race -count=1 -json' 'PostgreSQL internal package contracts use race JSON test execution' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" './internal/testutil/backendtest' 'PostgreSQL internal package contracts include harness package' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" './internal/catalog' 'PostgreSQL internal package contracts include catalog package' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" './internal/db' 'PostgreSQL internal package contracts include DB package' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" './internal/engine' 'PostgreSQL internal package contracts include engine package' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" './internal/maintenance' 'PostgreSQL internal package contracts include maintenance package' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" './internal/container' 'PostgreSQL internal package contracts include container package' || check_status=1
	    container_package_count="$(grep -Fc './internal/container' <<<"$postgres_internal_contracts_block")"
	    if [[ "$container_package_count" -ne 1 ]]; then
	      echo "[audit] ERROR: PostgreSQL internal package contracts must include ./internal/container exactly once (found $container_package_count)" >&2
	      check_status=1
	    else
	      echo "[audit] ok: PostgreSQL internal package contracts include container package exactly once"
	    fi
	    require_content_pattern "$postgres_internal_contracts_block" 'python3 - .*output_file' 'PostgreSQL internal package contracts parse JSON execution evidence' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'expected PostgreSQL pass missing' 'PostgreSQL internal package contracts require PostgreSQL test pass events' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'TestSCH001AndSCH002BootstrapVersionAndIdempotency/postgres' 'PostgreSQL internal package contracts prove Phase 5 bootstrap execution' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'TestSCH009PostgresVersionElevenAutoMigration/postgres' 'PostgreSQL internal package contracts prove Phase 5 migration execution' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'TestCatalogContractCurrentFileQueriesAcrossBackends/postgres' 'PostgreSQL internal package contracts prove catalog current-file execution' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'TestCatalogContractRepositoryConfigurationAcrossBackends/postgres' 'PostgreSQL internal package contracts prove catalog configuration execution' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'TestCatalogContractSnapshotGraphAcrossBackends/postgres' 'PostgreSQL internal package contracts prove catalog graph execution' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'TestCatalogContractChunkPlacementsAcrossBackends/postgres' 'PostgreSQL internal package contracts prove catalog placement execution' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'TestCatalogContractRestorePlansAcrossBackends/postgres' 'PostgreSQL internal package contracts prove catalog restore-plan execution' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'TestCatalogContractGCPlansAcrossBackends/postgres' 'PostgreSQL internal package contracts prove catalog GC-plan execution' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'TestEngineCurrentFilesAndConfigurationAcrossBackends/postgres' 'PostgreSQL internal package contracts prove engine current-file and configuration execution' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'TestEngineStoreFolderAcrossBackends/postgres' 'PostgreSQL internal package contracts prove engine folder-store execution' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'TestEngineGarbageCollectionPlanAcrossBackends/postgres' 'PostgreSQL internal package contracts prove engine GC-plan execution' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'TestEngineRepairAcrossBackends/postgres' 'PostgreSQL internal package contracts prove engine repair execution' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'TestEngineRecoverAcrossBackendsAndIsIdempotent/postgres' 'PostgreSQL internal package contracts prove engine recovery execution' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'TestEngineDoctorAcrossBackends/postgres' 'PostgreSQL internal package contracts prove engine Doctor execution' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'TestEngineReadStatsAndInspectAcrossBackends/postgres' 'PostgreSQL internal package contracts prove Phase 7 stats and inspect execution' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'TestEngineReadSnapshotViewsAcrossBackends/postgres' 'PostgreSQL internal package contracts prove Phase 7 snapshot-view execution' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'TestEngineReadVerifyAcrossBackends/postgres' 'PostgreSQL internal package contracts prove Phase 7 verification execution' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'TestEngineReadContextAndErrorsAcrossBackends/postgres' 'PostgreSQL internal package contracts prove Phase 7 context and error execution' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'TestEngineSnapshotSelectorsAcrossBackends/postgres' 'PostgreSQL internal package contracts prove Phase 8 selector execution' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'TestEngineSnapshotSelectorErrorsAcrossBackends/postgres' 'PostgreSQL internal package contracts prove Phase 8 selector error execution' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'TestEngineMutationStoreRemoveAcrossBackends/postgres' 'PostgreSQL internal package contracts prove Phase 9 store/remove execution' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'TestEngineMutationSnapshotLifecycleAcrossBackends/postgres' 'PostgreSQL internal package contracts prove Phase 9 snapshot lifecycle execution' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'TestEngineMutationRestoreAcrossBackends/postgres' 'PostgreSQL internal package contracts prove Phase 9 restore execution' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'TestEngineMutationErrorsAcrossBackends/postgres' 'PostgreSQL internal package contracts prove Phase 9 error and rollback execution' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'TestEngineGCDryRunAcrossBackends/postgres' 'PostgreSQL internal package contracts prove Phase 9 GC dry-run execution' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'TestBackendTransactionCommitRollbackAcrossBackends/postgres' 'PostgreSQL internal package contracts prove Phase 10 transaction execution' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'TestBackendForUpdateLockReleaseAcrossBackends/postgres' 'PostgreSQL internal package contracts prove Phase 10 FOR UPDATE execution' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'TestBackendNowaitAndSkipLockedAcrossBackends/postgres' 'PostgreSQL internal package contracts prove Phase 10 NOWAIT and SKIP LOCKED execution' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'TestBackendBlockedLockCancellationAcrossBackends/postgres' 'PostgreSQL internal package contracts prove Phase 10 blocked-lock cancellation execution' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'TestMutationRowsAffectedContractAcrossBackends/postgres' 'PostgreSQL internal package contracts prove Phase 17 mutation-cardinality execution' || check_status=1
	    require_content_pattern "$postgres_internal_contracts_block" 'TestContainerRowLockIntegrationAcrossBackends/postgres' 'PostgreSQL internal package contracts prove Phase 10 container row-lock integration execution' || check_status=1
	    if grep -Eq 'continue-on-error|\|\| true' <<<"$postgres_internal_contracts_block"; then
	      echo "[audit] ERROR: PostgreSQL internal package contracts step must remain blocking" >&2
	      check_status=1
	    else
	      echo "[audit] ok: PostgreSQL internal package contracts step is blocking"
	    fi
	  fi
	fi
  require_pattern "$WORKFLOW_FILE" '^  cross-platform:$' 'cross-platform job exists' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'os:\s*\[ubuntu-latest, macos-latest, windows-latest\]' 'cross-platform job runs native ubuntu, macOS, and Windows matrix' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'name:\s*Run native coordination runtime tests' 'cross-platform native coordination runtime step' || check_status=1
  require_pattern "$WORKFLOW_FILE" "go test -v -count=1 -run '\\^\\(TestNativeLock\\|TestWindowsNativeLock\\|TestProductionCoordinator\\)' ./internal/coordination" 'cross-platform native coordination command covers native backends and production Coordinator' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'name:\s*Run Windows secure-rename boundary tests' 'native Windows secure-rename boundary step' || check_status=1
  require_pattern "$WORKFLOW_FILE" "if:\s*runner\.os == 'Windows'" 'secure-rename boundary step is Windows-only' || check_status=1
  require_pattern "$WORKFLOW_FILE" "go test -v -count=1 -run '\\^TestWindowsRenameBuffer' ./internal/fsx/secureinstall" 'native Windows secure-rename boundary command' || check_status=1
  require_pattern "$NATIVE_UNIX_TEST_FILE" '^func TestNativeLockContentionAndReacquire' 'Unix native coordination source retains contention runtime test' || check_status=1
  require_pattern "$NATIVE_WINDOWS_TEST_FILE" '^func TestWindowsNativeLockContentionAndReacquire' 'Windows native coordination source retains contention runtime test' || check_status=1
  require_pattern "$COORDINATOR_NATIVE_TEST_FILE" '^func TestProductionCoordinatorsShareProcessRegistryAndProtectSuccessor' 'production Coordinator source retains registry and successor runtime test' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'name:\s*Run path safety cross-platform tests' 'cross-platform path safety step' || check_status=1
  require_pattern "$WORKFLOW_FILE" "go test ./internal/pathsafe/\\.\\.\\. -run 'TrustedRoot\\|Symlink\\|Alias\\|WritePath' -count=1" 'cross-platform path safety command covers trusted-root and alias checks' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'name:\s*Run storage restore cross-platform tests' 'cross-platform storage restore step' || check_status=1
  require_pattern "$WORKFLOW_FILE" "go test ./internal/storage/\\.\\.\\. -run 'TestRestore\\(FileByStoredPath\\(UsesPhysicalPathIdentity\\|UsesLexicalPhysicalPathIdentityAboveAlias\\|PrefixMode\\|PrefixModeCreatesMissingParents\\|OverrideMode\\|RejectsSymlinkedPrefixRoot\\)\\|WithTrustedRootAllowsOuterAliasForExactOutputPath\\|StoredPath\\(PrefixAllowsOuterAliasAboveTrustedRoot\\|OverrideAllowsOuterAliasAboveDerivedRoot\\|PrefixRejectsSymlinkedTargetInTrustedRoot\\|OverrideRejectsSymlinkedTargetInTrustedRoot\\|OriginalRejectsInjectedSymlinkBelowDerivedTrustedRoot\\)\\|RejectsSymlinkedTargetInTrustedRoot\\)' -count=1" 'cross-platform storage restore command scopes to trusted restore paths' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'name:\s*Run engine restore cross-platform tests' 'cross-platform engine restore step' || check_status=1
  require_pattern "$WORKFLOW_FILE" "go test ./internal/engine/\\.\\.\\. -run '\\^TestRestore' -count=1" 'cross-platform engine restore command scopes to restore tests' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'name:\s*Run snapshot restore cross-platform tests' 'cross-platform snapshot restore step' || check_status=1
  require_pattern "$WORKFLOW_FILE" "go test ./internal/snapshot/\\.\\.\\. -run '\\^TestRestoreSnapshot' -count=1" 'cross-platform snapshot restore command scopes to snapshot restore tests' || check_status=1
  vulnerability_block="$(extract_job_block vulnerability)"
  require_content_pattern "$vulnerability_block" '^    runs-on: \$\{\{ matrix\.os \}\}$' 'reachable-vulnerability scan runs on its native OS matrix' || check_status=1
  require_content_pattern "$vulnerability_block" 'os:\s*\[ubuntu-latest, windows-latest\]' 'reachable-vulnerability scan covers native Linux and Windows' || check_status=1
  require_content_pattern "$vulnerability_block" 'name:\s*Run blocking reachable-vulnerability scan' 'blocking reachable-vulnerability scan step' || check_status=1
  require_content_pattern "$vulnerability_block" '^        run: go run golang\.org/x/vuln/cmd/govulncheck@v1\.7\.0 \./\.\.\.$' 'ordinary-output govulncheck v1.7.0 command' || check_status=1
  if grep -Eq 'continue-on-error|\|\| true|(^|[[:space:]])--?json([[:space:]]|$)|--?format[= ](json|sarif|openvex)' <<<"$vulnerability_block"; then
    echo "[audit] ERROR: reachable-vulnerability scan must remain blocking and use ordinary output semantics" >&2
    check_status=1
  else
    echo "[audit] ok: reachable-vulnerability scan is blocking and uses ordinary output semantics"
  fi
  require_pattern "$WORKFLOW_FILE" 'needs:\s*\[quality, correctness-matrix, integration-stress, integration-long-run, adversarial, smoke, legacy-compatibility, benchmark-integrity, benchmark-timing-advisory, cross-platform, vulnerability\]' 'required gate depends separately on benchmark, cross-platform, and vulnerability evaluation' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'VULNERABILITY_RESULT:\s*\$\{\{ needs\.vulnerability\.result \}\}' 'required gate captures vulnerability result' || check_status=1
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
  require_pattern "$WORKFLOW_FILE" 'go test -race -count=1 -json ./tests/adversarial/\.\.\.' 'adversarial job targets adversarial suite with JSON evidence' || check_status=1
  require_pattern "$WORKFLOW_FILE" "go test -race -count=1 ./tests/adversarial/... -run 'TestAdversarialG14\\|TestAdversarialG15\\|TestAdversarialG16\\|TestAdversarialG17'" 'explicit G14-G17 adversarial gate command' || check_status=1
  adversarial_block="$(extract_job_block adversarial)"
  if [[ -z "$adversarial_block" ]]; then
    echo "[audit] ERROR: missing adversarial job block content" >&2
    check_status=1
  else
    require_content_pattern "$adversarial_block" '^    runs-on: ubuntu-latest$' 'adversarial coordination proof runs on Linux' || check_status=1
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
    adversarial_validation_block="$(extract_step_block_from_content "$adversarial_block" "Run adversarial validation (G1–G17)")"
    if [[ -z "$adversarial_validation_block" ]]; then
      echo "[audit] ERROR: missing adversarial coordination execution-proof step block" >&2
      check_status=1
    else
      require_content_pattern "$adversarial_validation_block" 'COLDKEEP_TEST_DB:\s*1' 'adversarial coordination proof enables DB gate' || check_status=1
      require_content_pattern "$adversarial_validation_block" 'COLDKEEP_LONG_RUN:\s*1' 'adversarial coordination proof enables long-run gate' || check_status=1
      require_content_pattern "$adversarial_validation_block" 'go test -race -count=1 -json \./tests/adversarial/\.\.\.' 'adversarial coordination proof uses JSON execution evidence' || check_status=1
      require_content_pattern "$adversarial_validation_block" 'TestAdversarialG6IndependentProcessRepositoryContention/plain' 'independent-process plain execution proof' || check_status=1
      require_content_pattern "$adversarial_validation_block" 'TestAdversarialG6IndependentProcessRepositoryContention/aes-gcm' 'independent-process AES-GCM execution proof' || check_status=1
      require_content_pattern "$adversarial_validation_block" 'TestAdversarialG6KilledLeaseHolderReleasesRepository' 'killed-holder execution proof' || check_status=1
      require_content_pattern "$adversarial_validation_block" 'TestAdversarialG6LiveGCExcludesIndependentStoreProcess' 'live-GC execution proof' || check_status=1
      require_content_pattern "$adversarial_validation_block" 'github.com/franchoy/coldkeep/tests/adversarial' 'adversarial coordination execution proof binds the adversarial package' || check_status=1
      require_content_pattern "$adversarial_validation_block" 'json\.loads\(raw_line\)' 'adversarial coordination execution proof rejects malformed JSON' || check_status=1
      require_content_pattern "$adversarial_validation_block" 'if not events:' 'adversarial coordination execution proof rejects empty JSON' || check_status=1
      require_content_pattern "$adversarial_validation_block" 'event\.get\("Action"\) == "skip"' 'adversarial coordination execution proof rejects required skips' || check_status=1
      require_content_pattern "$adversarial_validation_block" 'event\.get\("Action"\) == "pass"' 'adversarial coordination execution proof requires pass events' || check_status=1
      require_content_pattern "$adversarial_validation_block" 'print\("required execution-proof failure:", file=sys\.stderr\)' 'adversarial coordination execution-proof parser' || check_status=1
      require_content_pattern "$adversarial_validation_block" 'status=\$\{PIPESTATUS\[0\]\}' 'adversarial coordination proof preserves test status' || check_status=1
      require_content_pattern "$adversarial_validation_block" 'status=\$\?' 'adversarial coordination proof propagates parser status' || check_status=1
      # shellcheck disable=SC2016 # The audit pattern must match the literal $status.
      require_content_pattern "$adversarial_validation_block" 'exit "\$status"' 'adversarial coordination proof remains blocking' || check_status=1
      if grep -Eq 'continue-on-error|\|\| true' <<<"$adversarial_validation_block"; then
        echo "[audit] ERROR: adversarial coordination execution-proof step must not suppress broad failures" >&2
        check_status=1
      else
        echo "[audit] ok: adversarial coordination execution-proof step does not suppress broad failures"
      fi
    fi
  fi
  require_pattern "$WORKFLOW_FILE" '^  smoke:$' 'smoke job' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'name:\s*Upload smoke artifacts on failure' 'smoke failure artifact upload step' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'if:\s*\$\{\{ failure\(\) \}\}' 'smoke artifact upload is failure-only' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'uses:\s*actions/upload-artifact@v7' 'smoke artifact upload action' || check_status=1
  require_pattern "$WORKFLOW_FILE" './tests/integration/\.\.\.' 'integration stress race run (integration only)' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'COLDKEEP_LONG_RUN:\s*1' 'long-run env gate in CI' || check_status=1
  require_pattern "$WORKFLOW_FILE" "go test -race -count=1 ./tests/integration/... -run 'TestStoreGCVerifyRestoreDeleteLoopStability\\|TestRandomizedLongRunLifecycleSoak\\|TestSnapshotRetentionChurnLongRun'" 'dedicated long-run test command' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'QUALITY_RESULT.*!= "success"' 'required gate rejects skipped quality job' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'CORRECTNESS_MATRIX_RESULT.*!= "success"' 'required gate rejects skipped correctness matrix' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'INTEGRATION_STRESS_RESULT.*!= "success"' 'required gate rejects skipped integration stress' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'INTEGRATION_LONG_RUN_RESULT.*!= "success"' 'required gate rejects skipped integration long-run job' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'ADVERSARIAL_RESULT.*!= "success"' 'required gate rejects skipped adversarial job' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'SMOKE_RESULT.*!= "success"' 'required gate rejects skipped smoke job' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'BENCHMARK_INTEGRITY_RESULT.*!= "success"' 'required gate rejects skipped benchmark integrity job' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'BENCHMARK_TIMING_ADVISORY_RESULT.*!= "success"' 'required gate rejects skipped benchmark timing advisory job' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'CROSS_PLATFORM_RESULT.*!= "success"' 'required gate rejects skipped cross-platform job' || check_status=1
  require_pattern "$WORKFLOW_FILE" 'VULNERABILITY_RESULT.*!= "success"' 'required gate rejects skipped vulnerability job' || check_status=1
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

  # --- Ruleset: Protect release branches ---
  local release_id
  release_id=$(echo "$rulesets_json" | jq -r '.[] | select(.name == "Protect release branches") | .id')
  if [[ -z "$release_id" ]]; then
    echo "[audit] ERROR: missing ruleset 'Protect release branches'" >&2
    return 1
  fi
  echo "[audit] ok: ruleset 'Protect release branches' exists (id=${release_id})"

  local release_detail
  release_detail=$(gh_api "repos/$REPO/rulesets/${release_id}") || return 1
  if [[ "$(echo "$release_detail" | jq -r '.enforcement // "disabled"')" != "active" ]]; then
    echo "[audit] ERROR: ruleset 'Protect release branches' is not active" >&2
    return 1
  fi
  if [[ "$(echo "$release_detail" | jq -c '.conditions.ref_name.include // []')" != '["refs/heads/release/**/*"]' ]]; then
    echo "[audit] ERROR: release ruleset target is not exactly refs/heads/release/**/*" >&2
    return 1
  fi
  if [[ "$(echo "$release_detail" | jq '[.bypass_actors // [] | .[]] | length')" -ne 0 ]]; then
    echo "[audit] ERROR: release ruleset has bypass actors" >&2
    return 1
  fi
  local release_rules
  release_rules=$(echo "$release_detail" | jq -c '[.rules[].type] | sort')
  if [[ "$release_rules" != '["non_fast_forward"]' ]]; then
    echo "[audit] ERROR: release ruleset must contain only non_fast_forward; found ${release_rules}" >&2
    return 1
  fi
  echo "[audit] ok: release ruleset is active for refs/heads/release/**/* with zero bypass actors"
  echo "[audit] ok: release ruleset preserves non_fast_forward and omits required_linear_history"

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
  if [[ -n "$PAIRED_LAUNCHER_FILE" ]]; then
    check_paired_launcher "$PAIRED_LAUNCHER_FILE" || status=1
  fi
fi

if [[ "$LOCAL_ONLY" -eq 0 ]]; then
  check_remote_policy || status=1
fi

if [[ "$status" -ne 0 ]]; then
  echo "[audit] FAILED: CI is not yet guaranteed end-to-end" >&2
  exit "$status"
fi

echo "[audit] PASSED: CI enforcement prerequisites are in place"
