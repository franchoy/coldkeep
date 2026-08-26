#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
  echo "Usage: scripts/run_release_benchmark_evidence.sh [--repo-root PATH] [--candidate-sha SHA] [--binary PATH]" >&2
}

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "$script_dir/.." && pwd)
candidate_sha=""
binary=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo-root|--candidate-sha|--binary)
      [[ $# -ge 2 ]] || {
        echo "release benchmark runner: $1 requires a value" >&2
        exit 2
      }
      case "$1" in
        --repo-root) repo_root="$2" ;;
        --candidate-sha) candidate_sha="$2" ;;
        --binary) binary="$2" ;;
      esac
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "release benchmark runner: unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

repo_root=$(cd -- "$repo_root" && pwd -P)
git_root=$(git -C "$repo_root" rev-parse --show-toplevel)
git_root=$(cd -- "$git_root" && pwd -P)
[[ "$repo_root" == "$git_root" ]] || {
  echo "release benchmark runner: --repo-root must name the Git repository root" >&2
  exit 2
}

if [[ -z "$candidate_sha" ]]; then
  candidate_sha=$(git -C "$repo_root" rev-parse HEAD)
fi
[[ "$candidate_sha" =~ ^[0-9a-f]{40}$ ]] || {
  echo "release benchmark runner: invalid candidate SHA: $candidate_sha" >&2
  exit 2
}
if [[ -z "$binary" ]]; then
  binary="$repo_root/coldkeep"
fi
binary=$(cd -- "$(dirname -- "$binary")" && pwd -P)/$(basename -- "$binary")
[[ -x "$binary" ]] || {
  echo "release benchmark runner: binary is not executable: $binary" >&2
  exit 2
}

retained_root="$repo_root/.release-evidence/v1.13.14/$candidate_sha"
if [[ -e "$retained_root" ]]; then
  "$repo_root/scripts/release_benchmark_evidence.sh" validate \
    --bundle-root "$retained_root" \
    --candidate-sha "$candidate_sha"
  "$repo_root/scripts/release_benchmark_evidence.sh" inventory \
    --repo-root "$repo_root"
  echo "release benchmark runner: reusing valid exact-SHA evidence at $retained_root"
  exit 0
fi

work_root=$(mktemp -d "${TMPDIR:-/tmp}/coldkeep-v1.13.14-benchmark-evidence.XXXXXXXX")
case "$work_root/" in
  "$repo_root"/*)
    echo "release benchmark runner: transient root must be external to the repository" >&2
    exit 2
    ;;
esac
cleanup() {
  rm -rf -- "$work_root"
}
trap cleanup EXIT
trap 'cleanup; exit 130' INT
trap 'cleanup; exit 143' TERM

export COLDKEEP_CODEC=aes-gcm
export COLDKEEP_CONTAINER_LOCK_RETRY_ATTEMPTS=12
export COLDKEEP_CONTAINER_LOCK_RETRY_BASE_WAIT_MS=15
export COLDKEEP_CONTAINER_LOCK_RETRY_MAX_WAIT_MS=900

go_version=$(go version)
postgres_version=$(psql --version)
readonly postgres_digest="sha256:33f923b05f64ca54ac4401c01126a6b92afe839a0aa0a52bc5aeb5cc958e5f20"

while read -r profile compression workers dataset; do
  output_dir="$work_root/profiles/${profile}/integrity"
  test ! -e "$output_dir"
  COLDKEEP_COMPRESSION="$compression" \
    python3 "$repo_root/scripts/benchmark_gate.py" integrity \
      --binary "$binary" \
      --output-dir "$output_dir" \
      --compression "$compression" \
      --workers "$workers" \
      --dataset "$dataset" \
      --command-timeout-seconds 600 \
      --source-commit "$candidate_sha" \
      --go-version "$go_version" \
      --postgres-version "$postgres_version" \
      --database-image-digest "$postgres_digest"
  (cd -- "$output_dir" && sha256sum --check checksums.sha256)
done <<'EOF'
none-w1 none 1 ci-paired-w1-v2
none-w4 none 4 ci-paired-w4-v2
zstd-w1 zstd 1 ci-paired-w1-v2
zstd-w4 zstd 4 ci-paired-w4-v2
EOF

while read -r profile compression workers mode baseline; do
  evidence_dir="$work_root/profiles/${profile}/timing"
  mkdir -p -- "$evidence_dir"
  COLDKEEP_COMPRESSION="$compression" \
    "$binary" benchmark run \
      --dataset small \
      --workers "$workers" \
      --repeat 1 \
      --output json \
      | tee "$evidence_dir/benchmark.json"

  set +e
  python3 "$repo_root/scripts/validate_regression_thresholds.py" check \
    "$evidence_dir/benchmark.json" \
    --baseline "$repo_root/benchmarks/v1.9/baselines/$baseline" \
    --mode "$mode" \
    --policy hosted-advisory \
    --json-report "$evidence_dir/timing-advisory.json"
  comparator_exit=$?
  set -e

  test -s "$evidence_dir/timing-advisory.json"
  python3 "$repo_root/scripts/validate_regression_thresholds.py" verify-advisory-exit \
    --report "$evidence_dir/timing-advisory.json" \
    --observed-exit-code "$comparator_exit"
  case "$comparator_exit" in
    0|10|11|12) ;;
    *) echo "invalid timing-advisory exit: $comparator_exit" >&2; exit 2 ;;
  esac
  (
    cd -- "$evidence_dir"
    sha256sum benchmark.json timing-advisory.json > checksums.sha256
    sha256sum --check checksums.sha256
  )
done <<'EOF'
none-w1 none 1 uncompressed benchmark-baseline-v1.9-packed-aes-gcm-none-small-w1-r1.json
none-w4 none 4 uncompressed benchmark-baseline-v1.9-packed-aes-gcm-none-small-w4-r1.json
zstd-w1 zstd 1 compressed benchmark-baseline-v1.9-packed-aes-gcm-zstd-small-w1-r1.json
zstd-w4 zstd 4 compressed benchmark-baseline-v1.9-packed-aes-gcm-zstd-small-w4-r1.json
EOF

benchmark_tool_id=$(sha256sum "$repo_root/scripts/benchmark_gate.py")
benchmark_tool_id=${benchmark_tool_id%% *}
timing_tool_id=$(sha256sum "$repo_root/scripts/validate_regression_thresholds.py")
timing_tool_id=${timing_tool_id%% *}

"$repo_root/scripts/release_benchmark_evidence.sh" prepare \
  --bundle-root "$work_root" \
  --candidate-sha "$candidate_sha" \
  --source-commit "$candidate_sha" \
  --go-version "$go_version" \
  --postgres-version "$postgres_version" \
  --database-image-digest "$postgres_digest" \
  --benchmark-tool-id "$benchmark_tool_id" \
  --timing-tool-id "$timing_tool_id"
"$repo_root/scripts/release_benchmark_evidence.sh" promote \
  --repo-root "$repo_root" \
  --bundle-root "$work_root" \
  --candidate-sha "$candidate_sha"
"$repo_root/scripts/release_benchmark_evidence.sh" inventory \
  --repo-root "$repo_root"
