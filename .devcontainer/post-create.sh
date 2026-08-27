#!/usr/bin/env bash
set -Eeuo pipefail

readonly expected_go_version="go1.26.7"
readonly repository_root="/workspaces/coldkeep"

actual_go_version="$(go env GOVERSION)"
if [[ "$actual_go_version" != "$expected_go_version" ]]; then
  echo "expected ${expected_go_version}, got ${actual_go_version}" >&2
  exit 1
fi
if [[ "$(go env GOTOOLCHAIN)" != "local" ]]; then
  echo "GOTOOLCHAIN must be local" >&2
  exit 1
fi

if ! git config --global --get-all safe.directory | grep -Fxq "$repository_root"; then
  git config --global --add safe.directory "$repository_root"
fi

for _ in $(seq 1 30); do
  if pg_isready -h postgres -p 5432 -U coldkeep -d coldkeep >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
pg_isready -h postgres -p 5432 -U coldkeep -d coldkeep >/dev/null

go mod download

echo "DEVCONTAINER_BOOTSTRAP: PASS"
