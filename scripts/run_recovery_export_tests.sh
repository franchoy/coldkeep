#!/usr/bin/env bash
# Phase 1D: fresh disposable PostgreSQL 16 plus explicitly selected matched clients.
set -euo pipefail
if [ "$#" -ne 1 ] || [ "${COLDKEEP_PHASE1C_TEST:-}" != 1 ] || [ "${COLDKEEP_PHASE1B_TEST:-}" != 1 ] || [ -z "${DB_HOST:-}" ] || [ -z "${DB_PORT:-}" ] || [ -z "${DB_USER:-}" ] || [ -z "${DB_PASSWORD:-}" ] || [ -z "${COLDKEEP_PG_DUMP:-}" ] || [ -z "${COLDKEEP_PG_RESTORE:-}" ] || [ -z "${COLDKEEP_RECOVERY_IMAGE:-}" ]; then
  echo 'requires one new output directory, explicit disposable DB_* credentials, COLDKEEP_PHASE1B_TEST=1, COLDKEEP_PHASE1C_TEST=1 absolute COLDKEEP_PG_DUMP/COLDKEEP_PG_RESTORE paths and COLDKEEP_RECOVERY_IMAGE'  >&2
  exit 2
fi
export GOTOOLCHAIN=local GOWORK=off CGO_ENABLED=1
if [ "$(go env GOVERSION)" != go1.26.10 ]; then
  echo 'Go 1.26.10 is required on PATH' >&2
  exit 2
fi
mkdir -- "$1"
output=$(cd -- "$1" && pwd -P)
export COLDKEEP_PHASE1B_REPORT_DIR="$output" COLDKEEP_PHASE1C_REPORT_DIR="$output" COLDKEEP_PHASE1D_REPORT_DIR="$output"
reader_dir=$(mktemp -d)
trap 'rm -rf -- "$reader_dir"' EXIT
CGO_ENABLED=0 go build -o "$reader_dir/coldkeep-recovery" ./cmd/coldkeep-recovery
python3 scripts/build_phase3a_readers.py "$reader_dir/phase3a" "$output/phase3a-readers.json"
export COLDKEEP_PHASE3A_READERS="$reader_dir/phase3a"
export COLDKEEP_RECOVERY_READER="$reader_dir/coldkeep-recovery"
sha256sum "$COLDKEEP_RECOVERY_READER" >"$output/staged-reader.sha256"
docker image inspect "$COLDKEEP_RECOVERY_IMAGE" --format '{{.Id}} {{json .RepoDigests}}' >"$output/recovery-image.txt"
status=0
go test -race -count=1 -json -timeout=10m ./internal/recoveryexport ./internal/coordination ./cmd/coldkeep-recovery >"$output/go.jsonl" 2>"$output/go.stderr" || status=$?
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
 'TestRecoveryImportHonorsInventoryDuration','TestRecoveryOwnedInventoryDeadline',
 'TestRecoveryReceiptBoundDerivation','TestRecoveryImportReceiptPublicationBudget','TestRecoveryImportReceiptScaleRestore',
 'TestRecoveryImportRoundTrip','TestRecoveryImportTrustAndPackage','TestRecoveryImportSemanticAdmission',
 'TestRecoveryImportTargetRefusals','TestRecoveryImportContention','TestRecoveryImportFailures',
 'TestRecoveryImportCleanupJoins','TestRecoveryImportCredentialJoin','TestRecoveryImportChildJoin',
 'TestRecoveryImportCopyAndBounds','TestRecoveryImportFreshnessUnderReservation','TestRecoveryImportSourceBoundary',
 'TestRecoveryImportFullHistoricalClosure','TestRecoveryOriginalLoss','TestRecoveryPostLiveGC',
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
 'github.com/franchoy/coldkeep/v2/cmd/coldkeep-recovery':{
 'TestRecoveryReaderReceiptWithinAdvertisedScale','TestRecoveryRestoreConsumesLargeReceiptBeforeBackend',
 'TestRecoveryReceiptByteBoundary','TestRecoveryReceiptObjectBoundary','TestRecoveryReceiptStrictness',
 'TestRecoveryReceiptCancellation','TestRecoveryReceiptRegularInput','TestRecoveryConfigLimitRemainsSmall'},
 'github.com/franchoy/coldkeep/v2/internal/coordination':{
 'TestPrepareExisting','TestPrepareExistingChildContention','TestExistingLeasePreparationAndMetadata',
 'TestExistingLeaseLegacyContention','TestExistingLeaseUnsafeAndLost'}}
for parent, cases in {
 'TestRecoveryImportReceiptPublicationBudget': 'bytes objects compatible',
 'TestRecoveryOwnedInventoryDeadline': 'earlier-parent lower-inventory expired-parent',
 'TestRecoveryImportTrustAndPackage': 'untrusted generation digest missing incomplete corrupt unsupported',
 'TestRecoveryImportSemanticAdmission': 'unsafe-name reserved-name id format rows reader key-declaration name missing-object extra-object payload-header',
 'TestRecoveryImportTargetRefusals': 'populated system privileged membership incompatible existing-root package-alias no-exclusive missing-key wrong-key unsupported-key ambient',
 'TestRecoveryImportFailures': 'database-reserved owned copied restored active-cleaned before-ready ready-published credential-close release',
 'TestRecoveryImportCleanupJoins': 'blocked-success driver-error automatic-rollback-error',
 'TestRecoveryImportCopyAndBounds': 'member-substitution symlink hardlink count bytes expanded-sql short-write enospc close fsync archive-drift canceled-before-ready',
 'TestRecoveryOriginalLoss': 'plaintext=true plaintext=false',
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
for parent, cases in {
 'TestRecoveryReceiptObjectBoundary': 'objects OBJECTS objectſ',
 'TestRecoveryReceiptStrictness': 'duplicate alias-duplicate unicode-duplicate nested-duplicate unknown truncated trailing-value trailing-garbage depth string key',
 'TestRecoveryReceiptCancellation': 'before during',
 'TestRecoveryReceiptRegularInput': 'symlink hardlink directory',
}.items():
 required['github.com/franchoy/coldkeep/v2/cmd/coldkeep-recovery'].update(parent+'/'+case for case in cases.split())
# Phase 3A adds requirements; all 244 accepted requirements above remain intact.
required['github.com/franchoy/coldkeep/v2/internal/recoveryexport'].update({
 'TestAdoptionRoundTrip','TestAdoptionOldReceiptCompatibility','TestAdoptionOwnershipContinuity',
 'TestAdoptionIdentityConstraints','TestAdoptionOperatorAndDatabaseReservation','TestAdoptionEngineAdmission',
 'TestAdoptionCheckpointWrongSource',
})
for parent,cases in {
 'TestAdoptionStalePlanAndBounds':'stale-plan aggregate deadline operation-length sqlite-file',
 'TestAdoptionTransactionFaults':'before-ddl during-ddl during-data before-commit after-commit',
 'TestAdoptionEarlyRefusals':'sqlite expected-schema request plan missing-key retained-receipt checkpoint-failure checkpoint-corrupt checkpoint-missing',
 'TestAdoptionHistoricalIsolation':'plaintext=true plaintext=false',
 'TestAdoptionWriterFences':'d19 accepted current',
 'TestAdoptionCleanupOwnership':'false true',
 'TestAdoptionProfileRefusals':'future missing-table extra-table unknown-column wrong-type missing-lineage unknown-capability constraint orphan identity-bound row-budget text-budget',
 'TestAdoptionMigrationCleanup':'rollback driver-close cancellation',
 'TestAdoptionExportCleanup':'rollback credential child',
 'TestAdoptionImportProfiles':'mixed-version false-schema17 unknown-reader repository-conflict catalog-missing-lineage receipt-v1 receipt-reader receipt-identity',
}.items():
 required['github.com/franchoy/coldkeep/v2/internal/recoveryexport'].add(parent)
 required['github.com/franchoy/coldkeep/v2/internal/recoveryexport'].update(parent+'/'+case for case in cases.split())
# A1/A2 follow-up: retain the complete submitted set above and require each
# semantic case below in the same blocking runner and race-enabled Go child.
required['github.com/franchoy/coldkeep/v2/internal/recoveryexport'].update({
 'TestPhase3AFollowupCanonicalRecovery','TestPhase3AFollowupMetadataEnvelope',
 'TestPhase3AFollowupA2ExportRefusal','TestPhase3AFollowupRestoreBeforePins',
})
for parent,cases in {
 'TestPhase3AFollowupA1IdentityConstraintSet':'valid foreign-key-relocated duplicate-check-padding',
 'TestPhase3AFollowupA1AdmissionBoundaries':'export-no-complete import-no-ready',
 'TestPhase3AFollowupA2ConstraintMetadataBound':'valid oversized-existing-name oversized-unexpected-name compressed-small',
 'TestPhase3AFollowupConstraintRefusals':'missing extra wrong-kind wrong-definition not-validated deferrable no-inherit many-records',
 'TestPhase3AFollowupMetadataCleanup':'preflight definition',
}.items():
 required['github.com/franchoy/coldkeep/v2/internal/recoveryexport'].add(parent)
 required['github.com/franchoy/coldkeep/v2/internal/recoveryexport'].update(parent+'/'+case for case in cases.split())
# Phase 3B requirements remain in this ordinary blocking race invocation.
required['github.com/franchoy/coldkeep/v2/internal/coordination'].add('TestExistingLeaseLossNeverRevives')
required['github.com/franchoy/coldkeep/v2/internal/recoveryexport'].update({
 'TestBindingMigrationAndRecovery','TestBindingSourcesAndOfflineRelocation',
 'TestBindingExactPathsAndBudgets','TestBindingReservationsThroughPublication','TestBindingNativeByteRootMapping',
})
for parent,cases in {
 'TestBindingSourceCapacityAndAvailability':'read-only missing empty-substitution',
 'TestBindingRecoveryAdmissionBoundaries':'future mixed wrong-repository malformed-restored-head restore-before-pins export-no-complete',
 'TestBindingCommittedIntentConflicts':'prior-descriptor generation manifest source',
 'TestBindingIntentConflicts':'future plan request descriptor prior duplicate trailing oversized pending-staging-status missing-descriptor-no-reconstruction',
 'TestBindingUnsafeControlOwner':'marker descriptor',
 'TestBindingCrashCuts':'after-intent before-ddl during-ddl during-data before-commit after-commit published:root-marker.v1.json published:installation.v1.json before-intent-removal',
 'TestBindingSubstitutedRoots':'absent empty unrelated copied-marker catalog-unavailable weaker-session',
 'TestBindingEarlyRefusals':'same-mount-descriptor descriptor-under-root unsafe-directory wrong-db wrong-key ambient stale-plan stale-revision retained-v1 retained-v2 retained-v3 retained-malformed deadline catalog-budget checkpoint-budget',
 'TestBindingSchemaAdmission':'column type nullability missing relocated duplicate wrong-kind wrong-definition not-valid deferrable compressed head lineage oversized',
 'TestBindingActiveSubstitution':'root control lock',
 'TestBindingProcessTermination':'before-intent after-intent before-commit after-commit published:root-marker.v1.json published:installation.v1.json',
 'TestBindingExportCleanup':'rollback credential child',
 'TestBindingCleanupJoins':'inspection migration cancellation rollback-error close-error',
 'TestBindingPublicationFailures':'file-write: file-sync: file-rename: directory-sync: file-close:',
 'TestBindingIndependentRecovery':'plaintext=true plaintext=false',
 'TestBindingOldWritersAndReaders':'schema17 schema18 schema19',
 'TestBindingDockerProfile':'non-root-readonly-source recreation-requires-reconnect empty-substitute missing-bind-source',
}.items():
 required['github.com/franchoy/coldkeep/v2/internal/recoveryexport'].add(parent)
 required['github.com/franchoy/coldkeep/v2/internal/recoveryexport'].update(parent+'/'+case for case in cases.split())
for which in ['marker','descriptor']:
 parent='TestBindingControlAdmission/'+which
 required['github.com/franchoy/coldkeep/v2/internal/recoveryexport'].add(parent)
 required['github.com/franchoy/coldkeep/v2/internal/recoveryexport'].update(parent+'/'+case for case in 'missing future truncated oversized duplicate trailing symlink fifo hardlink permissions wrong-identity stale-revision'.split())
required['github.com/franchoy/coldkeep/v2/internal/recoveryexport'].add('TestBindingControlAdmission')
# Phase 3B R1: raw configuration fidelity and actual staged operator behavior.
for parent,cases in {
 'TestRecoveryConfigUnicodeRefusals':'invalid-utf8 overlong-utf8 encoded-surrogate truncated-utf8 lone-low-surrogate lone-high-surrogate high-high low-high high-scalar high-gap-low high-escaped-backslash-low paired-then-low escaped-quote-then-high key-surrogate password-surrogate',
 'TestRecoveryConfigExactPathControls':'literal-replacement-rune escaped-replacement-rune paired-surrogates pair-minimum pair-maximum scalar-boundaries literal-backslash-u doubled-backslash-u escaped-quote adjacent-pairs whitespace-case-nonascii standard-escapes base64-native-invalid-utf8',
 'TestRecoveryConfigByteBoundary':'exact-value exact-padding excess-value excess-padding excess-trailing-value',
 'TestRecoveryConfigSyntaxRefusals':'unknown-field trailing-value trailing-garbage truncated malformed-escape incomplete-unicode nonhex-unicode unescaped-control typed-value',
 'TestRecoveryConfigRepresentationPolicy':'both-nonempty both-nonempty-invalid-base64 invalid-base64 plain-only base64-only empty-plain empty-base64 null-plain null-base64',
}.items():
 required['github.com/franchoy/coldkeep/v2/cmd/coldkeep-recovery'].add(parent)
 required['github.com/franchoy/coldkeep/v2/cmd/coldkeep-recovery'].update(parent+'/'+case for case in cases.split())
required['github.com/franchoy/coldkeep/v2/internal/recoveryexport'].update({
 'TestBindingOperatorConfigFidelity','TestBindingOperatorNativePathControls',
 'TestBindingOperatorHistoricalRestore','TestBindingOperatorMutationObserver',
})
for case in 'raw-invalid-utf8 lone-low-surrogate lone-high-surrogate'.split():
 parent='TestBindingOperatorConfigFidelity/'+case
 required['github.com/franchoy/coldkeep/v2/internal/recoveryexport'].add(parent)
 required['github.com/franchoy/coldkeep/v2/internal/recoveryexport'].update(parent+'/'+command for command in ['preview','apply'])
required['github.com/franchoy/coldkeep/v2/internal/recoveryexport'].update(
 'TestBindingOperatorNativePathControls/'+case for case in 'literal-replacement-rune escaped-replacement-rune paired-surrogates literal-backslash-u whitespace-case-nonascii base64-native-byte'.split())
passes={(e['Package'],e.get('Test','')) for e in records if e.get('Action')=='pass'}
failures=[e for e in records if e.get('Action') in ('fail','skip')]
missing=[p+'/'+t for p,tests in required.items() for t in tests|{''} if (p,t) not in passes]
result={'schema':'coldkeep-phase1d-events/v1','missing':missing,'failures_or_skips':failures,'passed_tests':sum(bool(t) for p,t in passes),'status':'PASS' if not missing and not failures else 'FAIL'}
Path(sys.argv[1]).with_name('acceptance-events.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result))
sys.exit(bool(missing or failures))
PY
printf 'go/checker final status=%s\n' "$status"
exit "$status"
