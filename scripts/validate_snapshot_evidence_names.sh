#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: scripts/validate_snapshot_evidence_names.sh [--repo-root PATH]" >&2
}

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "$script_dir/.." && pwd)

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo-root)
      if [[ $# -lt 2 ]]; then
        echo "snapshot evidence validator: --repo-root requires a path" >&2
        exit 2
      fi
      repo_root="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "snapshot evidence validator: unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ ! -d "$repo_root" ]]; then
  echo "snapshot evidence validator: not a Git repository root: $repo_root" >&2
  exit 2
fi
repo_root=$(cd -- "$repo_root" && pwd -P)
if ! git_root=$(git -C "$repo_root" rev-parse --show-toplevel 2>/dev/null); then
  echo "snapshot evidence validator: not a Git repository root: $repo_root" >&2
  exit 2
fi
git_root=$(cd -- "$git_root" && pwd -P)
if [[ "$repo_root" != "$git_root" ]]; then
  echo "snapshot evidence validator: not a Git repository root: $repo_root" >&2
  exit 2
fi

readonly evidence_names=(
  TestListRetainedLogicalFileIDs
  TestIsLogicalFileReferencedBySnapshot
  TestComputeReachabilitySummary
  TestRemoveFailsWhenLogicalFileIsRetainedBySnapshot
  TestRunGCDoesNotDeleteSnapshotRetainedContainer
  TestRunGCDryRunDoesNotCountSnapshotRetainedContainerAsReclaimable
  TestAdversarialG14SnapshotRetainedGCGuardUnderChurn
  TestDeleteSnapshotRemovesSnapshotRowsOnly
  TestAdversarialG17RetentionRootTransitionChurn
  TestRunStatsResultIncludesSnapshotRetentionVisibility
  TestRunStatsCommandJSONIncludesSnapshotRetention
  TestAdversarialG16SnapshotQueryContractChaos
  TestVerifySystemStandardPassesWithConsistentSnapshotReachability
  TestVerifySystemStandardDetectsOrphanSnapshotLogicalReference
  TestVerifySystemStandardDetectsSnapshotInvalidLifecycleState
  TestVerifySystemStandardDetectsSnapshotRetainedMissingChunkGraph
  TestFormatDoctorTextReportGoldenHealthy
  TestFormatDoctorTextReportGoldenDegraded
  TestAdversarialG15CorruptedSnapshotMetadataDetectionConservativeGC
)

status=0
for name in "${evidence_names[@]}"; do
  if ! git -C "$repo_root" grep -F -- "func ${name}(" -- '*.go' >/dev/null; then
    echo "missing evidence: ${name}" >&2
    status=1
  fi
done

if [[ "$status" -ne 0 ]]; then
  exit "$status"
fi

echo "snapshot evidence names: OK (${#evidence_names[@]} tracked declarations)"
