#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/run_phase7_v17_binary_gate.sh [--count N]

Runs the Phase 7 released-v1.7 binary compatibility proof test.
This gate is strict: it fails if COLDKEEP_V17_BIN is not set to an executable path.

Environment defaults (override as needed):
  COLDKEEP_TEST_DB=1
  DB_HOST=127.0.0.1
  DB_PORT=5432
  DB_USER=coldkeep
  DB_PASSWORD=coldkeep
  DB_NAME=coldkeep
  DB_SSLMODE=disable

Required:
  COLDKEEP_V17_BIN=/absolute/path/to/released/coldkeep-v1.7

Example:
  COLDKEEP_V17_BIN=/opt/coldkeep/v1.7/coldkeep \
    bash scripts/run_phase7_v17_binary_gate.sh --count 1
EOF
}

COUNT=1
while [[ $# -gt 0 ]]; do
  case "$1" in
    --count)
      if [[ $# -lt 2 ]]; then
        echo "[phase7-v17-gate] ERROR: --count requires a value" >&2
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
      echo "[phase7-v17-gate] ERROR: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if ! [[ "$COUNT" =~ ^[1-9][0-9]*$ ]]; then
  echo "[phase7-v17-gate] ERROR: --count must be a positive integer" >&2
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
: "${DB_SSLMODE:=disable}"

if [[ -z "${COLDKEEP_V17_BIN:-}" ]]; then
  echo "[phase7-v17-gate] ERROR: COLDKEEP_V17_BIN is required for actual released-v1.7 compatibility proof" >&2
  exit 2
fi

if [[ ! -x "$COLDKEEP_V17_BIN" ]]; then
  echo "[phase7-v17-gate] ERROR: COLDKEEP_V17_BIN is not executable: $COLDKEEP_V17_BIN" >&2
  exit 2
fi

export COLDKEEP_TEST_DB DB_HOST DB_PORT DB_USER DB_PASSWORD DB_NAME DB_SSLMODE COLDKEEP_V17_BIN
export COLDKEEP_REQUIRE_V17_BINARY_PROOF=1

echo "[phase7-v17-gate] running released-v1.7 compatibility proof (count=${COUNT})"
go test ./tests/integration -run '^TestPhase7BuildFixtureWithActualV17BinaryIntegration$' -count "$COUNT" -v

echo "[phase7-v17-gate] PASS"
