#!/usr/bin/env bash
# Complete command/internal package route; real prerequisites live through child exit.
set -euo pipefail
if [ "$#" -ne 1 ]; then
  echo 'requires one new output directory' >&2
  exit 2
fi
exec python3 "$(dirname -- "$0")/run_staged_package_tests.py" --scope packages --output "$1"
