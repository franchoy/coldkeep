#!/usr/bin/env bash
# Phase 1C: fresh disposable PostgreSQL 16 plus explicitly selected matched clients.
set -euo pipefail
if [ "$#" -ne 1 ] || [ "${COLDKEEP_PHASE1C_TEST:-}" != 1 ] || [ "${COLDKEEP_PHASE1B_TEST:-}" != 1 ] || [ -z "${DB_HOST:-}" ] || [ -z "${DB_PORT:-}" ] || [ -z "${DB_USER:-}" ] || [ -z "${DB_PASSWORD:-}" ] || [ -z "${COLDKEEP_PG_DUMP:-}" ] || [ -z "${COLDKEEP_PG_RESTORE:-}" ]; then
  echo 'requires one new output directory, explicit disposable DB_* credentials, COLDKEEP_PHASE1B_TEST=1, COLDKEEP_PHASE1C_TEST=1 and absolute COLDKEEP_PG_DUMP/COLDKEEP_PG_RESTORE paths' >&2
  exit 2
fi
export GOTOOLCHAIN=local GOWORK=off CGO_ENABLED=1
if [ "$(go env GOVERSION)" != go1.26.10 ]; then
  echo 'Go 1.26.10 is required on PATH' >&2
  exit 2
fi
mkdir -- "$1"
output=$(cd -- "$1" && pwd -P)
export COLDKEEP_PHASE1B_REPORT_DIR="$output" COLDKEEP_PHASE1C_REPORT_DIR="$output"
status=0
go test -race -count=1 -json -timeout=4m ./internal/recoveryexport ./internal/coordination >"$output/go.jsonl" 2>"$output/go.stderr" || status=$?
python3 - "$output/go.jsonl" <<'PY' || status=$?
import json,sys
from pathlib import Path
records=[json.loads(s) for s in Path(sys.argv[1]).read_text().splitlines() if s.strip()]
required={
 'github.com/franchoy/coldkeep/v2/internal/recoveryexport':{
 'TestRecoveryExportRoundTrip/plaintext=false','TestRecoveryExportRoundTrip/plaintext=true',
 'TestRecoveryExportSameSnapshot','TestRecoveryHistoricalClosure','TestRecoveryExplicitPreparation',
 'TestRecoveryCleanupBeforePublication/blocked-success',
 'TestRecoveryCleanupBeforePublication/driver-close-error',
 'TestRecoveryCleanupBeforePublication/automatic-rollback-error',
 'TestRecoveryChildCancellationJoins','TestRecoveryExportStages','TestRecoveryOwnedDumpAndCopy',
 'TestRecoverySourceCallBoundary','TestRecoveryServerVersionRefusal',
 'TestRecoveryJSONPredecodeLimits',
 'TestRecoveryJSONIndependentLimits',
 'TestRecoveryJSONFieldAliases',
 'TestRecoveryJSONStrictness',
 'TestRecoveryJSONCancellation',
 'TestRecoveryCredentialDirectoryPlacement',
 'TestRecoverySubscriptionRefusal',
 'TestRecoverySubscriptionOtherDatabase',
 'TestRecoveryFilesystemFailures','TestRecoveryCancellationStages','TestRecoveryDestinationAndLimits',
 'TestRecoveryToolAndConfigRefusals','TestRecoveryAmbientIsolation','TestRecoveryCompletedChecker',
 'TestSupportedPostgresMigrationColumnOrder','TestCatalogMissingOrRenamedColumn',
 'TestCatalogRowSecurityCannotHideClosure','TestCleanupJoinsAutomaticRollbackAndDriverClose',
 'TestCleanupPreservesDriverErrors','TestPostgresCanceledQueryClosesSession',
 'TestPostgresStalledStartupCancellation','TestPassiveCallBoundary'},
 'github.com/franchoy/coldkeep/v2/internal/coordination':{
 'TestPrepareExisting','TestPrepareExistingChildContention','TestExistingLeasePreparationAndMetadata',
 'TestExistingLeaseLegacyContention','TestExistingLeaseUnsafeAndLost'}}
for parent, cases in {
 'TestRecoveryExportStages': 'archive-written payload-copy payloads-written staged-verified source-cleaned before-publication published',
 'TestRecoveryFilesystemFailures': 'short-write enospc payload-close manifest-sync directory-sync post-publication-sync destination-digest source-missing source-changed archive-truncated',
 'TestRecoveryCancellationStages': 'archive-written payload-copy source-cleaned before-publication',
 'TestRecoveryDestinationAndLimits': 'source-contained source-alias existing relative archive manifest total members duration overflow',
 'TestRecoveryToolAndConfigRefusals': 'missing-tool wrong-major mismatched-client nonzero empty stdout-overflow stderr-overflow expired-snapshot privilege policy schema hostile-database hostile-user',
 'TestRecoveryCompletedChecker': 'produce-and-remove-source missing-marker missing-member object-altered manifest-altered marker-generation future-version wrong-size wrong-digest overflow duplicate-member duplicate-key traversal absolute symlink fifo hardlink extra nested depth manifest-limit count-limit total-limit unknown-field archive-header',
 'TestRecoveryOwnedDumpAndCopy': 'child-started payload-copy',
 'TestRecoveryJSONPredecodeLimits': 'members excluded',
 'TestRecoveryJSONFieldAliases': 'MEMBERS memberſ excluded-uppercase duplicate-MEMBERS duplicate-memberſ nested-duplicate',
 'TestRecoveryJSONStrictness': 'duplicate-key depth long-string long-key trailing-value unknown-field',
 'TestRecoveryCredentialDirectoryPlacement': 'source-equal source-descendant source-alias destination-equal destination-descendant destination-alias sibling-parent outside-alias',
 'TestRecoverySubscriptionRefusal': 'restricted-reader database-owner',
}.items():
 required['github.com/franchoy/coldkeep/v2/internal/recoveryexport'].update(parent+'/'+case for case in cases.split())
passes={(e['Package'],e.get('Test','')) for e in records if e.get('Action')=='pass'}
failures=[e for e in records if e.get('Action') in ('fail','skip')]
missing=[p+'/'+t for p,tests in required.items() for t in tests|{''} if (p,t) not in passes]
result={'schema':'coldkeep-phase1c-events/v1','missing':missing,'failures_or_skips':failures,'passed_tests':sum(bool(t) for p,t in passes),'status':'PASS' if not missing and not failures else 'FAIL'}
Path(sys.argv[1]).with_name('acceptance-events.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result))
sys.exit(bool(missing or failures))
PY
printf 'go/checker final status=%s\n' "$status"
exit "$status"
