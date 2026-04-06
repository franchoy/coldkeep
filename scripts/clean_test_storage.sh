#!/usr/bin/env bash
# Clean up all Coldkeep test storage directories and containers for a fresh test run.
# Usage: bash scripts/clean_test_storage.sh

set -euo pipefail


# Remove only known test/temporary storage directories. DO NOT delete persistent/real data directories!
# If you use a persistent containers directory for real data, it will NOT be deleted by this script.
CANDIDATES=(
  ".ci-storage"
  "/tmp/coldkeep*"
  "/tmp/coldkeep-test-*"
)

# Optional broader cleanup for Go testing.TempDir leftovers.
# Disabled by default to avoid deleting unrelated /tmp/Test* directories on shared machines.
if [[ "${COLDKEEP_CLEAN_AGGRESSIVE_TMP:-0}" == "1" ]]; then
  CANDIDATES+=("/tmp/Test*")
fi

for dir in "${CANDIDATES[@]}"; do
  echo "Removing $dir ..."
  shopt -s nullglob
  matches=( $dir )
  shopt -u nullglob

  if [[ "${#matches[@]}" -eq 0 ]]; then
    echo "No matches for $dir"
    continue
  fi

  for path in "${matches[@]}"; do
    rm -rf -- "$path"
    echo "Removed $path"
  done
done

echo "Test storage cleanup complete."
