#!/usr/bin/env bash
set -euo pipefail

HISTORICAL_SHA=2212ddd2981f3535fb71f38232bda6c1b7b35e97
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/.." && pwd)
TASK_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/coldkeep-schema-v17-fence.XXXXXX")
HISTORICAL_ROOT="$TASK_ROOT/historical"
CURRENT_SQLITE="$TASK_ROOT/current-v17.sqlite"
HISTORICAL_SQLITE="$TASK_ROOT/historical-v16.sqlite"
CURRENT_OVERLAY="$TASK_ROOT/current-overlay.json"

cleanup() {
  if [[ -n "${POSTGRES_V16_DB:-}" ]]; then
    dropdb --if-exists "$POSTGRES_V16_DB" >/dev/null 2>&1 || true
  fi
  rm -rf -- "$TASK_ROOT"
}
trap cleanup EXIT

for tool in git go sha256sum psql createdb dropdb pg_dump tar cp awk cmp grep sed diff; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "[schema-v17-fence] ERROR: required tool is unavailable: $tool" >&2
    exit 1
  fi
done

mkdir -p "$HISTORICAL_ROOT"
git -C "$REPO_ROOT" archive "$HISTORICAL_SHA" | tar -x -C "$HISTORICAL_ROOT"
cp "$REPO_ROOT/scripts/testdata/schema_v17_pre_v17_sqlite_probe_test.go" \
  "$HISTORICAL_ROOT/internal/db/schema_v17_pre_v17_sqlite_probe_test.go"

printf '{"Replace":{"%s":"%s"}}\n' \
  "$REPO_ROOT/internal/db/schema_v17_pre_v17_sqlite_probe_test.go" \
  "$REPO_ROOT/scripts/testdata/schema_v17_pre_v17_sqlite_probe_test.go" >"$CURRENT_OVERLAY"

echo "[schema-v17-fence] current SQLite bootstrap"
(
  cd "$REPO_ROOT"
  COLDKEEP_SCHEMA_V17_PROBE_DB="$CURRENT_SQLITE" \
  COLDKEEP_SCHEMA_V17_PROBE_ACTION=prepare-current \
    go test -tags schema_v17_historical_probe -overlay "$CURRENT_OVERLAY" \
      ./internal/db -run '^TestSchemaV17HistoricalSQLiteProbe$' -count=1 -v
)

echo "[schema-v17-fence] historical SQLite v16 operation"
(
  cd "$HISTORICAL_ROOT"
  COLDKEEP_SCHEMA_V17_PROBE_DB="$HISTORICAL_SQLITE" \
  COLDKEEP_SCHEMA_V17_PROBE_ACTION=historical-valid \
    go test -tags schema_v17_historical_probe ./internal/db \
      -run '^TestSchemaV17HistoricalSQLiteProbe$' -count=1 -v
)

SQLITE_BEFORE=$(sha256sum "$CURRENT_SQLITE" | awk '{print $1}')
echo "[schema-v17-fence] historical SQLite rejects fenced v17"
(
  cd "$HISTORICAL_ROOT"
  COLDKEEP_SCHEMA_V17_PROBE_DB="$CURRENT_SQLITE" \
  COLDKEEP_SCHEMA_V17_PROBE_ACTION=historical-reject \
    go test -tags schema_v17_historical_probe ./internal/db \
      -run '^TestSchemaV17HistoricalSQLiteProbe$' -count=1 -v
)
SQLITE_AFTER=$(sha256sum "$CURRENT_SQLITE" | awk '{print $1}')
if [[ "$SQLITE_BEFORE" != "$SQLITE_AFTER" ]]; then
  echo "[schema-v17-fence] ERROR: historical SQLite rejection changed repository bytes" >&2
  exit 1
fi

: "${DB_HOST:=127.0.0.1}"
: "${DB_PORT:=5432}"
: "${DB_USER:=coldkeep}"
: "${DB_PASSWORD:=coldkeep-development-only}"
: "${DB_SSLMODE:=disable}"
export DB_HOST DB_PORT DB_USER DB_PASSWORD DB_SSLMODE
export PGHOST="$DB_HOST" PGPORT="$DB_PORT" PGUSER="$DB_USER"
export PGPASSWORD="$DB_PASSWORD" PGSSLMODE="$DB_SSLMODE"

SERVER_VERSION=$(psql -Atqc 'SHOW server_version' postgres)
if [[ "$SERVER_VERSION" != 16.15* ]]; then
  echo "[schema-v17-fence] ERROR: PostgreSQL server is $SERVER_VERSION, require 16.15" >&2
  exit 1
fi

POSTGRES_V16_DB="coldkeep_fence_${$}_v16"
createdb --maintenance-db="${COLDKEEP_TEST_DB_MAINTENANCE:-postgres}" "$POSTGRES_V16_DB"

echo "[schema-v17-fence] build exact historical and current CLIs"
(
  cd "$HISTORICAL_ROOT"
  go build -o "$TASK_ROOT/coldkeep-v16" ./cmd/coldkeep
)
(
  cd "$REPO_ROOT"
  go build -o "$TASK_ROOT/coldkeep-v17" ./cmd/coldkeep
)

echo "[schema-v17-fence] historical PostgreSQL v16 full CLI operation"
COLDKEEP_DB_AUTO_BOOTSTRAP=true DB_NAME="$POSTGRES_V16_DB" \
  "$TASK_ROOT/coldkeep-v16" stats >/dev/null

echo "[schema-v17-fence] current PostgreSQL migration to fenced v17"
COLDKEEP_DB_AUTO_BOOTSTRAP=false DB_NAME="$POSTGRES_V16_DB" \
  "$TASK_ROOT/coldkeep-v17" stats >/dev/null

PG_BEFORE="$TASK_ROOT/postgres-before.sql"
PG_AFTER="$TASK_ROOT/postgres-after.sql"
pg_dump --schema=public --no-owner --no-privileges "$POSTGRES_V16_DB" \
  | sed '/^\\restrict /d; /^\\unrestrict /d' >"$PG_BEFORE"

echo "[schema-v17-fence] historical PostgreSQL full CLI rejects fenced v17"
if COLDKEEP_DB_AUTO_BOOTSTRAP=false DB_NAME="$POSTGRES_V16_DB" \
  "$TASK_ROOT/coldkeep-v16" stats >"$TASK_ROOT/historical-cli.out" 2>&1; then
  echo "[schema-v17-fence] ERROR: historical full CLI accepted fenced PostgreSQL v17" >&2
  exit 1
fi
if ! grep -qi 'version' "$TASK_ROOT/historical-cli.out"; then
  echo "[schema-v17-fence] ERROR: historical full CLI did not report old-column rejection" >&2
  sed -n '1,120p' "$TASK_ROOT/historical-cli.out" >&2
  exit 1
fi

pg_dump --schema=public --no-owner --no-privileges "$POSTGRES_V16_DB" \
  | sed '/^\\restrict /d; /^\\unrestrict /d' >"$PG_AFTER"
if ! cmp -s "$PG_BEFORE" "$PG_AFTER"; then
  echo "[schema-v17-fence] ERROR: historical full CLI changed semantic PostgreSQL state" >&2
  diff -u "$PG_BEFORE" "$PG_AFTER" >&2 || true
  exit 1
fi

OWNER_TABLE=$(psql -Atqc "SELECT COALESCE(to_regclass('public.repository_operation_owner')::text, '')" "$POSTGRES_V16_DB")
OWNER_ROWS=0
if [[ -n "$OWNER_TABLE" ]]; then
  OWNER_ROWS=$(psql -Atqc 'SELECT COUNT(*) FROM repository_operation_owner' "$POSTGRES_V16_DB")
fi
if [[ "$OWNER_ROWS" != 0 ]]; then
  echo "[schema-v17-fence] ERROR: historical rejection left $OWNER_ROWS coordination owner row(s)" >&2
  exit 1
fi

echo "[schema-v17-fence] PASS"
