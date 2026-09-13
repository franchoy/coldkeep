#!/usr/bin/env bash
set -Eeuo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
readonly release_version="v1.13.14"
readonly expected_profiles="none-w1,none-w4,zstd-w1,zstd-w4"
readonly profiles=(none-w1 none-w4 zstd-w1 zstd-w4)

usage() {
  cat >&2 <<'EOF'
Usage:
  scripts/release_benchmark_evidence.sh prepare --bundle-root PATH --candidate-sha SHA --source-commit SHA --go-version TEXT [--benchmark-tool-id TEXT] [--timing-tool-id TEXT]
  scripts/release_benchmark_evidence.sh validate --bundle-root PATH --candidate-sha SHA [--require-current-provenance]
  scripts/release_benchmark_evidence.sh promote --repo-root PATH --bundle-root PATH --candidate-sha SHA
  scripts/release_benchmark_evidence.sh inventory --repo-root PATH [--require-clean-worktree]
EOF
}

fail() {
  echo "release benchmark evidence: $*" >&2
  exit 1
}

require_value() {
  local option="$1"
  local value="${2:-}"
  [[ -n "$value" ]] || fail "$option requires a non-empty value"
}

canonical_dir() {
  local path="$1"
  [[ -d "$path" ]] || fail "directory does not exist: $path"
  (cd -- "$path" && pwd -P)
}

require_sha() {
  local value="$1"
  [[ "$value" =~ ^[0-9a-f]{40}$ ]] || fail "candidate SHA must be 40 lowercase hexadecimal characters: $value"
}

require_repo_root() {
  local root
  root=$(canonical_dir "$1")
  local git_root
  git_root=$(git -C "$root" rev-parse --show-toplevel 2>/dev/null) || fail "not a Git repository root: $root"
  git_root=$(canonical_dir "$git_root")
  [[ "$root" == "$git_root" ]] || fail "not a Git repository root: $root"
  printf '%s\n' "$root"
}

manifest_value() {
  local manifest="$1"
  local key="$2"
  local lines
  lines=$(grep -F "${key}=" "$manifest" || true)
  [[ "$(printf '%s\n' "$lines" | sed '/^$/d' | wc -l)" -eq 1 ]] || fail "manifest key ${key} is missing or duplicated"
  printf '%s\n' "${lines#*=}"
}

verify_checksum_file() {
  local directory="$1"
  local checksum_file="$2"
  [[ -s "$directory/$checksum_file" ]] || fail "missing checksum manifest: $directory/$checksum_file"
  if ! (cd -- "$directory" && sha256sum --check "$checksum_file" >/dev/null); then
    fail "checksum validation failed: $directory/$checksum_file"
  fi
}

require_exact_entries() {
  local directory="$1"
  shift
  local expected actual
  expected=$(printf '%s\n' "$@" | LC_ALL=C sort)
  actual=$(find "$directory" -mindepth 1 -maxdepth 1 -printf '%f\n' | LC_ALL=C sort)
  [[ "$actual" == "$expected" ]] || fail "unexpected inventory in $directory (expected: $(printf '%s' "$expected" | tr '\n' ' '); actual: $(printf '%s' "$actual" | tr '\n' ' '))"
}

validate_profile() {
  local bundle_root="$1"
  local profile="$2"
  local profile_root="$bundle_root/profiles/$profile"
  local integrity_root="$profile_root/integrity"
  local timing_root="$profile_root/timing"

  [[ -d "$profile_root" ]] || fail "missing profile $profile"
  require_exact_entries "$profile_root" checksums.sha256 integrity timing
  [[ -d "$integrity_root" ]] || fail "missing integrity evidence for profile $profile"
  [[ -s "$integrity_root/benchmark-integrity.json" ]] || fail "missing benchmark-integrity.json for profile $profile"
  verify_checksum_file "$integrity_root" checksums.sha256
  [[ -d "$timing_root" ]] || fail "missing timing evidence for profile $profile"
  require_exact_entries "$timing_root" benchmark.json checksums.sha256 timing-advisory.json
  verify_checksum_file "$timing_root" checksums.sha256
  verify_checksum_file "$profile_root" checksums.sha256
}

validate_bundle() {
  local bundle_root
  bundle_root=$(canonical_dir "$1")
  local candidate_sha="$2"
  local require_current="${3:-0}"
  require_sha "$candidate_sha"

  if find "$bundle_root" -type l -print -quit | grep -q .; then
    fail "bundle contains a symbolic link: $bundle_root"
  fi
  if find "$bundle_root" -mindepth 1 \( -name '*.partial' -o -name '.staging-*' \) -print | grep -q .; then
    fail "bundle contains unexpected staging/incomplete content: $bundle_root"
  fi
  local manifest="$bundle_root/manifest.txt"
  [[ -s "$manifest" ]] || fail "missing top manifest"
  local format
  format=$(manifest_value "$manifest" format)
  case "$format" in
    coldkeep-release-benchmark-evidence-v1)
      require_exact_entries "$bundle_root" bundle-checksums.sha256 manifest.txt profiles
      [[ "$require_current" -eq 0 ]] || fail "historical benchmark bundle does not establish current execution provenance"
      ;;
    coldkeep-release-benchmark-evidence-v2)
      require_exact_entries "$bundle_root" \
        bundle-checksums.sha256 database-provenance.before.json \
        database-provenance.after.json database-provenance-comparison.json \
        manifest.txt profiles
      python3 "$script_dir/benchmark_gate.py" database-provenance validate \
        --input "$bundle_root/database-provenance.before.json"
      python3 "$script_dir/benchmark_gate.py" database-provenance validate \
        --input "$bundle_root/database-provenance.after.json"
      python3 "$script_dir/benchmark_gate.py" database-provenance validate-comparison \
        --input "$bundle_root/database-provenance-comparison.json" \
        --before "$bundle_root/database-provenance.before.json" \
        --after "$bundle_root/database-provenance.after.json"
      ;;
    *) fail "top manifest format mismatch" ;;
  esac
  require_exact_entries "$bundle_root/profiles" "${profiles[@]}"

  [[ "$(manifest_value "$manifest" release)" == "$release_version" ]] || fail "top manifest release mismatch"
  [[ "$(manifest_value "$manifest" candidate_sha)" == "$candidate_sha" ]] || fail "top manifest candidate SHA mismatch"
  [[ "$(manifest_value "$manifest" source_commit)" == "$candidate_sha" ]] || fail "top manifest source commit mismatch"
  [[ "$(manifest_value "$manifest" expected_profiles)" == "$expected_profiles" ]] || fail "top manifest profile set mismatch"
  [[ -n "$(manifest_value "$manifest" go_version)" ]] || fail "top manifest Go identity is empty"
  if [[ "$format" == "coldkeep-release-benchmark-evidence-v1" ]]; then
    [[ -n "$(manifest_value "$manifest" postgres_version)" ]] || fail "top manifest PostgreSQL identity is empty"
    [[ -n "$(manifest_value "$manifest" database_image_digest)" ]] || fail "top manifest database image identity is empty"
  else
    local provenance_file provenance_hash
    for provenance_file in database-provenance.before.json database-provenance.after.json database-provenance-comparison.json; do
      provenance_hash=$(sha256sum "$bundle_root/$provenance_file")
      provenance_hash=${provenance_hash%% *}
      [[ "$(manifest_value "$manifest" "${provenance_file//[.-]/_}_sha256")" == "$provenance_hash" ]] \
        || fail "top manifest provenance hash mismatch for $provenance_file"
    done
  fi
  [[ -n "$(manifest_value "$manifest" benchmark_tool_id)" ]] || fail "top manifest benchmark tool identity is empty"
  [[ -n "$(manifest_value "$manifest" timing_tool_id)" ]] || fail "top manifest timing tool identity is empty"

  local profile profile_checksum
  for profile in "${profiles[@]}"; do
    validate_profile "$bundle_root" "$profile"
    if [[ "$format" == "coldkeep-release-benchmark-evidence-v2" ]]; then
      python3 "$script_dir/benchmark_gate.py" database-provenance verify-aggregate \
        --input "$bundle_root/database-provenance.before.json" \
        --aggregate "$bundle_root/profiles/$profile/integrity/aggregate.json"
    fi
    [[ "$(manifest_value "$manifest" "profile.${profile}.path")" == "profiles/$profile" ]] || fail "top manifest path mismatch for profile $profile"
    profile_checksum=$(sha256sum "$bundle_root/profiles/$profile/checksums.sha256")
    profile_checksum=${profile_checksum%% *}
    [[ "$(manifest_value "$manifest" "profile.${profile}.checksums_sha256")" == "$profile_checksum" ]] || fail "top manifest checksum identity mismatch for profile $profile"
  done
  verify_checksum_file "$bundle_root" bundle-checksums.sha256
  if [[ "$format" == "coldkeep-release-benchmark-evidence-v1" ]]; then
    echo "BENCHMARK_PROVENANCE_STATUS: HISTORICAL_EXECUTION_PROVENANCE_UNESTABLISHED"
  else
    echo "BENCHMARK_PROVENANCE_STATUS: CURRENT_VERIFIED"
  fi
}

write_profile_checksums() {
  local profile_root="$1"
  local temporary="$profile_root/.checksums.sha256.tmp"
  (
    cd -- "$profile_root"
    find integrity timing -type f -print | LC_ALL=C sort | while IFS= read -r path; do
      sha256sum "$path"
    done
  ) > "$temporary"
  mv -- "$temporary" "$profile_root/checksums.sha256"
}

prepare_bundle() {
  local bundle_root
  bundle_root=$(canonical_dir "$1")
  local candidate_sha="$2"
  local source_commit="$3"
  local go_version="$4"
  local benchmark_tool_id="$5"
  local timing_tool_id="$6"

  require_sha "$candidate_sha"
  require_sha "$source_commit"
  [[ "$source_commit" == "$candidate_sha" ]] || fail "source commit must equal candidate SHA"
  [[ -n "$go_version" ]] || fail "Go identity must be non-empty"
  [[ ! -e "$bundle_root/manifest.txt" && ! -e "$bundle_root/bundle-checksums.sha256" ]] || fail "bundle is already prepared"
  [[ -d "$bundle_root/profiles" ]] || fail "missing profiles directory"
  [[ -s "$bundle_root/database-provenance.before.json" ]] || fail "missing pre-run database provenance"
  [[ -s "$bundle_root/database-provenance.after.json" ]] || fail "missing post-run database provenance"
  [[ -s "$bundle_root/database-provenance-comparison.json" ]] || fail "missing database provenance comparison"
  python3 "$script_dir/benchmark_gate.py" database-provenance validate-comparison \
    --input "$bundle_root/database-provenance-comparison.json" \
    --before "$bundle_root/database-provenance.before.json" \
    --after "$bundle_root/database-provenance.after.json"

  local profile profile_checksum manifest_tmp bundle_checksum_tmp provenance_file provenance_hash
  for profile in "${profiles[@]}"; do
    [[ -d "$bundle_root/profiles/$profile" ]] || fail "missing profile $profile"
    [[ -s "$bundle_root/profiles/$profile/integrity/benchmark-integrity.json" ]] || fail "missing benchmark-integrity.json for profile $profile"
    verify_checksum_file "$bundle_root/profiles/$profile/integrity" checksums.sha256
    require_exact_entries "$bundle_root/profiles/$profile/timing" benchmark.json checksums.sha256 timing-advisory.json
    verify_checksum_file "$bundle_root/profiles/$profile/timing" checksums.sha256
    write_profile_checksums "$bundle_root/profiles/$profile"
  done

  manifest_tmp="$bundle_root/.manifest.txt.tmp"
  {
    echo "format=coldkeep-release-benchmark-evidence-v2"
    echo "release=$release_version"
    echo "candidate_sha=$candidate_sha"
    echo "source_commit=$source_commit"
    echo "expected_profiles=$expected_profiles"
    echo "go_version=$go_version"
    for provenance_file in database-provenance.before.json database-provenance.after.json database-provenance-comparison.json; do
      provenance_hash=$(sha256sum "$bundle_root/$provenance_file")
      provenance_hash=${provenance_hash%% *}
      echo "${provenance_file//[.-]/_}_sha256=$provenance_hash"
    done
    echo "benchmark_tool_id=$benchmark_tool_id"
    echo "timing_tool_id=$timing_tool_id"
    for profile in "${profiles[@]}"; do
      profile_checksum=$(sha256sum "$bundle_root/profiles/$profile/checksums.sha256")
      profile_checksum=${profile_checksum%% *}
      echo "profile.${profile}.path=profiles/$profile"
      echo "profile.${profile}.checksums_sha256=$profile_checksum"
    done
  } > "$manifest_tmp"
  mv -- "$manifest_tmp" "$bundle_root/manifest.txt"

  bundle_checksum_tmp="$bundle_root/.bundle-checksums.sha256.tmp"
  (
    cd -- "$bundle_root"
    sha256sum manifest.txt
    sha256sum database-provenance.before.json database-provenance.after.json database-provenance-comparison.json
    for profile in "${profiles[@]}"; do
      sha256sum "profiles/$profile/checksums.sha256"
    done
  ) > "$bundle_checksum_tmp"
  mv -- "$bundle_checksum_tmp" "$bundle_root/bundle-checksums.sha256"
  validate_bundle "$bundle_root" "$candidate_sha" 1
  echo "release benchmark evidence prepared: $bundle_root"
}

staging_dir=""
cleanup_staging() {
  if [[ -n "$staging_dir" && -d "$staging_dir" ]]; then
    rm -rf -- "$staging_dir"
  fi
}
trap cleanup_staging EXIT
trap 'cleanup_staging; exit 130' INT
trap 'cleanup_staging; exit 143' TERM

promote_bundle() {
  local repo_root
  repo_root=$(require_repo_root "$1")
  local bundle_root
  bundle_root=$(canonical_dir "$2")
  local candidate_sha="$3"
  require_sha "$candidate_sha"
  validate_bundle "$bundle_root" "$candidate_sha" 1

  case "$bundle_root/" in
    "$repo_root"/*) fail "transient bundle root must be external to the repository: $bundle_root" ;;
  esac

  local evidence_parent="$repo_root/.release-evidence/v1.13.14"
  local final_root="$evidence_parent/$candidate_sha"
  mkdir -p -- "$evidence_parent"
  if [[ -e "$final_root" ]]; then
    if ! (validate_bundle "$final_root" "$candidate_sha" 1); then
      fail "existing exact-SHA evidence is invalid; refusing overwrite: $final_root"
    fi
    if ! cmp -s -- "$bundle_root/bundle-checksums.sha256" "$final_root/bundle-checksums.sha256"; then
      fail "existing exact-SHA evidence differs from the complete candidate bundle: $final_root"
    fi
    echo "release benchmark evidence already valid: $final_root"
    return
  fi

  staging_dir=$(mktemp -d "$evidence_parent/.staging-${candidate_sha}.XXXXXXXX")
  cp -a -- "$bundle_root/." "$staging_dir/"
  validate_bundle "$staging_dir" "$candidate_sha" 1

  if [[ -n "${COLDKEEP_EVIDENCE_TEST_STAGE_READY_FIFO:-}" ]]; then
    printf '%s\n' "$staging_dir" > "$COLDKEEP_EVIDENCE_TEST_STAGE_READY_FIFO"
    if [[ -n "${COLDKEEP_EVIDENCE_TEST_STAGE_RELEASE_FIFO:-}" ]]; then
      read -r _ < "$COLDKEEP_EVIDENCE_TEST_STAGE_RELEASE_FIFO"
    fi
  fi

  [[ ! -e "$final_root" ]] || fail "exact-SHA evidence appeared during promotion: $final_root"
  mv -- "$staging_dir" "$final_root"
  staging_dir=""
  validate_bundle "$final_root" "$candidate_sha" 1
  echo "release benchmark evidence promoted: $final_root"
}

inventory_evidence() {
  local repo_root
  repo_root=$(require_repo_root "$1")
  local require_clean="$2"
  local evidence_root="$repo_root/.release-evidence"
  local release_root="$evidence_root/v1.13.14"

  if [[ -d "$release_root" ]]; then
    local entry name
    while IFS= read -r entry; do
      name=$(basename -- "$entry")
      [[ "$name" != .staging-* ]] || fail "unexpected staging directory remains: $entry"
      [[ -d "$entry" && "$name" =~ ^[0-9a-f]{40}$ ]] || fail "unexpected incomplete retained entry: $entry"
      validate_bundle "$entry" "$name"
    done < <(find "$release_root" -mindepth 1 -maxdepth 1 -print | LC_ALL=C sort)
    git -C "$repo_root" check-ignore -q .release-evidence || fail ".release-evidence is not covered by repository ignore policy"
  fi

  if [[ "$require_clean" -eq 1 ]]; then
    local status
    status=$(git -C "$repo_root" status --porcelain=v1 --untracked-files=all)
    [[ -z "$status" ]] || fail "ordinary untracked-inclusive worktree is not clean: $status"
  fi

  echo "RELEASE_EVIDENCE_CANONICAL_ROOT: $release_root/<candidate-SHA>/"
  echo "RELEASE_EVIDENCE_STAGING_DIRECTORIES: 0"
  echo "RELEASE_EVIDENCE_INVENTORY: PASS"
}

[[ $# -gt 0 ]] || {
  usage
  exit 2
}
mode="$1"
shift

repo_root=""
bundle_root=""
candidate_sha=""
source_commit=""
go_version=""
benchmark_tool_id="unspecified"
timing_tool_id="unspecified"
require_clean_worktree=0
require_current_provenance=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo-root|--bundle-root|--candidate-sha|--source-commit|--go-version|--benchmark-tool-id|--timing-tool-id)
      [[ $# -ge 2 ]] || fail "$1 requires a value"
      option="$1"
      value="$2"
      case "$option" in
        --repo-root) repo_root="$value" ;;
        --bundle-root) bundle_root="$value" ;;
        --candidate-sha) candidate_sha="$value" ;;
        --source-commit) source_commit="$value" ;;
        --go-version) go_version="$value" ;;
        --benchmark-tool-id) benchmark_tool_id="$value" ;;
        --timing-tool-id) timing_tool_id="$value" ;;
      esac
      shift 2
      ;;
    --require-clean-worktree)
      require_clean_worktree=1
      shift
      ;;
    --require-current-provenance)
      require_current_provenance=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *) fail "unknown argument: $1" ;;
  esac
done

case "$mode" in
  prepare)
    require_value --bundle-root "$bundle_root"
    require_value --candidate-sha "$candidate_sha"
    require_value --source-commit "$source_commit"
    require_value --go-version "$go_version"
    prepare_bundle "$bundle_root" "$candidate_sha" "$source_commit" "$go_version" "$benchmark_tool_id" "$timing_tool_id"
    ;;
  validate)
    require_value --bundle-root "$bundle_root"
    require_value --candidate-sha "$candidate_sha"
    validate_bundle "$bundle_root" "$candidate_sha" "$require_current_provenance"
    echo "release benchmark evidence validation: PASS"
    ;;
  promote)
    require_value --repo-root "$repo_root"
    require_value --bundle-root "$bundle_root"
    require_value --candidate-sha "$candidate_sha"
    promote_bundle "$repo_root" "$bundle_root" "$candidate_sha"
    ;;
  inventory)
    require_value --repo-root "$repo_root"
    inventory_evidence "$repo_root" "$require_clean_worktree"
    ;;
  *)
    usage
    exit 2
    ;;
esac
