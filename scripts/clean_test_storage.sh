#!/bin/bash
# Clean up all Coldkeep test storage directories and containers for a fresh test run.
# Usage: bash scripts/clean_test_storage.sh

set -e


# Remove only known test/temporary storage directories. DO NOT delete persistent/real data directories!
# If you use a persistent containers directory for real data, it will NOT be deleted by this script.
CANDIDATES=(
  ".ci-storage"
  "/tmp/coldkeep*"
  "/tmp/Test*"
  "/tmp/coldkeep-test-*"
)

for dir in "${CANDIDATES[@]}"; do
  echo "Removing $dir ..."
  rm -rf $dir
  echo "Removed $dir"
done

echo "Test storage cleanup complete."
