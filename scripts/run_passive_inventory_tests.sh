#!/usr/bin/env bash
# Passive selection with lifetime-scoped recovery readers and full fixtures.
set -euo pipefail
if [ "$#" -ne 1 ]; then
  echo 'requires one new output directory' >&2
  exit 2
fi
exec python3 "$(dirname -- "$0")/run_staged_package_tests.py" --scope passive --output "$1"
