#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: scripts/validate_release_linearity.sh [--repo-root PATH] [--candidate-ref COMMIT_OR_REF]" >&2
}

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "$script_dir/.." && pwd)
candidate_ref=HEAD

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo-root)
      if [[ $# -lt 2 ]]; then
        echo "release linearity validator: --repo-root requires a path" >&2
        exit 2
      fi
      repo_root="$2"
      shift 2
      ;;
    --candidate-ref)
      if [[ $# -lt 2 || -z "$2" ]]; then
        echo "release linearity validator: --candidate-ref requires a commit or ref" >&2
        exit 2
      fi
      candidate_ref="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "release linearity validator: unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ ! -d "$repo_root" ]]; then
  echo "release linearity validator: not a Git repository root: $repo_root" >&2
  exit 2
fi
repo_root=$(cd -- "$repo_root" && pwd -P)
if ! git_root=$(git -C "$repo_root" rev-parse --show-toplevel 2>/dev/null); then
  echo "release linearity validator: not a Git repository root: $repo_root" >&2
  exit 2
fi
git_root=$(cd -- "$git_root" && pwd -P)
if [[ "$repo_root" != "$git_root" ]]; then
  echo "release linearity validator: not a Git repository root: $repo_root" >&2
  exit 2
fi

if ! git -C "$repo_root" show-ref --verify --quiet refs/remotes/origin/main; then
  echo "release linearity validator: missing refs/remotes/origin/main" >&2
  exit 1
fi

origin_main=$(git -C "$repo_root" rev-parse refs/remotes/origin/main)
if ! candidate_commit=$(git -C "$repo_root" rev-parse --verify --end-of-options "${candidate_ref}^{commit}" 2>/dev/null); then
  echo "release linearity validator: candidate ref does not resolve to a commit: $candidate_ref" >&2
  exit 1
fi
if ! base=$(git -C "$repo_root" merge-base "$candidate_commit" refs/remotes/origin/main); then
  echo "release linearity validator: no merge base between $candidate_ref and refs/remotes/origin/main" >&2
  exit 1
fi
if [[ -z "$base" ]]; then
  echo "release linearity validator: no merge base between $candidate_ref and refs/remotes/origin/main" >&2
  exit 1
fi

local_merges=$(git -C "$repo_root" rev-list --merges "${base}..${candidate_commit}")
if [[ -n "$local_merges" ]]; then
  echo "release linearity validator: release-local merge commit(s) detected after merge base $base:" >&2
  printf '%s\n' "$local_merges" >&2
  exit 1
fi

echo "RELEASE_LINEARITY_ORIGIN_MAIN: $origin_main"
echo "RELEASE_LINEARITY_CANDIDATE_REF: $candidate_ref"
echo "RELEASE_LINEARITY_CANDIDATE_SHA: $candidate_commit"
echo "RELEASE_LINEARITY_MERGE_BASE: $base"
echo "RELEASE_LINEARITY_LOCAL_MERGES: NONE"
echo "RELEASE_LINEARITY: PASS"
