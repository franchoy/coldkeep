#!/usr/bin/env bash
# Focused Phase 1B evidence; DB_* must select a disposable PostgreSQL 16 server.
set -euo pipefail
if [ "$#" -ne 1 ] || [ -z "${DB_HOST:-}" ] || [ -z "${DB_PORT:-}" ] || [ -z "${DB_USER:-}" ] || [ -z "${DB_PASSWORD:-}" ] || [ "${COLDKEEP_PHASE1B_TEST:-}" != 1 ]; then
  echo 'usage: COLDKEEP_PHASE1B_TEST=1 DB_HOST=... DB_PORT=... DB_USER=... DB_PASSWORD=... run_passive_inventory_tests.sh <new-output-dir> (disposable PostgreSQL only)' >&2
  exit 2
fi
export GOTOOLCHAIN=local CGO_ENABLED=1 GOWORK=off
if [ "$(go env GOVERSION)" != go1.26.10 ]; then
  echo 'Go 1.26.10 is required on PATH' >&2
  exit 2
fi
mkdir -- "$1"
output=$(cd -- "$1" && pwd -P)
export COLDKEEP_PHASE1B_REPORT_DIR="$output"
status=0
go test -race -count=1 -json -timeout=4m ./internal/recoveryexport ./internal/coordination >"$output/go.jsonl" 2>"$output/go.stderr" || status=$?
python3 - "$output/go.jsonl" <<'PY' || status=$?
import json,sys
from pathlib import Path
records=[json.loads(s) for s in Path(sys.argv[1]).read_text().splitlines() if s.strip()]
required={
 'github.com/franchoy/coldkeep/v2/internal/recoveryexport':{
 'TestSupportedPostgresMigrationColumnOrder',
 'TestCatalogMissingOrRenamedColumn',
 'TestCatalogMissingOrRenamedColumn/ALTER_TABLE_logical_file_DROP_COLUMN_original_name',
 'TestCatalogMissingOrRenamedColumn/ALTER_TABLE_logical_file_RENAME_COLUMN_original_name_TO_future_name',
 'TestCatalogRowSecurityCannotHideClosure',
 'TestCatalogRowSecurityCannotHideClosure/filtered-reader-refused',
 'TestCatalogRowSecurityCannotHideClosure/already-authorized-owner-sees-full-catalog',
 'TestCleanupJoinsAutomaticRollbackAndDriverClose',
 'TestCleanupJoinsAutomaticRollbackAndDriverClose/rollback-in-flight',
 'TestCleanupJoinsAutomaticRollbackAndDriverClose/bad-connection-close-in-flight',
 'TestCleanupPreservesDriverErrors',
 'TestCleanupPreservesDriverErrors/explicit-rollback',
 'TestCleanupPreservesDriverErrors/automatic-rollback',
 'TestPostgresCanceledQueryClosesSession',
 'TestPostgresCanceledQueryClosesSession/cancel',
 'TestPostgresCanceledQueryClosesSession/deadline',
 'TestPostgresStalledStartupCancellation',
 'TestPostgresStalledStartupCancellation/startup',
 'TestPostgresStalledStartupCancellation/begin',
 'TestConsumerFailureClosesSession',
 'TestCompatibleFullCatalogCut','TestPassiveRefusals','TestLegacyAndPackedInventory',
 'TestFailureClosureAndCancellation','TestReadOnlySnapshotAndAmbientIsolation',
 'TestReadOnlyWriteAttemptAndQueryFailure','TestExecutionBounds',
 'TestRepairAndRetiredPhysicalClosure','TestClosedResourcesAndLostLeaseNeverComplete',
 'TestPlaintextWithoutKeyAndMixedRecipe','TestNoTransientSourceMutation','TestPassiveCallBoundary','TestPinOnlyAndUnreferencedContainer'},
 'github.com/franchoy/coldkeep/v2/internal/coordination':{
 'TestExistingLeasePreparationAndMetadata','TestExistingLeaseLegacyContention','TestExistingLeaseUnsafeAndLost'}}
passes={(e['Package'],e.get('Test','')) for e in records if e.get('Action')=='pass'}
failures=[e for e in records if e.get('Action') in ('fail','skip')]
missing=[p+'/'+t for p,tests in required.items() for t in tests|{''} if (p,t) not in passes]
result={'schema':'coldkeep-phase1b-events/v1','missing':missing,'failures_or_skips':failures,'passed_tests':sum(bool(t) for p,t in passes),'status':'PASS' if not missing and not failures else 'FAIL'}
Path(sys.argv[1]).with_name('acceptance-events.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result))
sys.exit(bool(missing or failures))
PY
printf 'go/checker final status=%s\n' "$status"
exit "$status"
