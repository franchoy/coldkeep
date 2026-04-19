#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/run_snapshot_release_gate.sh [--count N]

Runs the v1.4 snapshot release-gate integration tests in PostgreSQL mode.

Environment defaults (override as needed):
  COLDKEEP_TEST_DB=1
  DB_HOST=127.0.0.1
  DB_PORT=5432
  DB_USER=coldkeep
  DB_PASSWORD=coldkeep
  DB_NAME=coldkeep

Example:
  bash scripts/run_snapshot_release_gate.sh --count 3
EOF
}

COUNT=1
while [[ $# -gt 0 ]]; do
  case "$1" in
    --count)
      if [[ $# -lt 2 ]]; then
        echo "[snapshot-gate] ERROR: --count requires a value" >&2
        exit 2
      fi
      COUNT="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[snapshot-gate] ERROR: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if ! [[ "$COUNT" =~ ^[1-9][0-9]*$ ]]; then
  echo "[snapshot-gate] ERROR: --count must be a positive integer" >&2
  exit 2
fi

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/.." && pwd)
cd "$REPO_ROOT"

: "${COLDKEEP_TEST_DB:=1}"
: "${DB_HOST:=127.0.0.1}"
: "${DB_PORT:=5432}"
: "${DB_USER:=coldkeep}"
: "${DB_PASSWORD:=coldkeep}"
: "${DB_NAME:=coldkeep}"

export COLDKEEP_TEST_DB DB_HOST DB_PORT DB_USER DB_PASSWORD DB_NAME

TEST_REGEX='TestSnapshotDiffSummaryJSONContractMatchesDetailedSummary|TestSnapshotListTreeJSONContract|TestSnapshotStatsLineageEnhancedFieldsJSONContract|TestCLIJSONOutputContracts|TestSnapshotCreateLifecycleIntegration|TestSnapshotPhase2BehaviorRegressionIntegration|TestSnapshotPhase3LineageBehaviorIntegration|TestSnapshotLineageSafetyMixedChainDeleteMiddleIntegration|TestSnapshotCrossFeatureInteractionIntegration'

echo "[snapshot-gate] running v1.4 snapshot release gate (count=${COUNT})"
go test ./tests/integration -run "$TEST_REGEX" -count "$COUNT" -v

echo "[snapshot-gate] PASS"
