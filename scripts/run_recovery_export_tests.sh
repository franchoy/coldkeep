#!/usr/bin/env bash
# Required recovery/coordination/operator suite with lifetime-scoped readers.
set -euo pipefail
if [ "$#" -ne 1 ]; then
  echo 'requires one new output directory' >&2
  exit 2
fi
exec python3 "$(dirname -- "$0")/run_staged_package_tests.py" --scope recovery --output "$1"
