#!/usr/bin/env python3
"""Enforce the accepted passive identities and reject every observed skip/failure."""
import argparse
import json
from pathlib import Path


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
# Keep every accepted passive identity and require the same new observer controls
# reached by this route's unchanged package/test selection.
required['github.com/franchoy/coldkeep/v2/internal/recoveryexport'].update({
 'TestBindingSessionObservation',
 'TestBindingSessionObservation/disappearance',
 'TestBindingSessionObservation/held-session',
 'TestBindingSessionObservation/query-error',
 'TestBindingSessionObservation/canceled-after-zero',
 'TestBindingSessionObservation/query-deadline',
 'TestInventorySessionObservation',
 'TestInventorySessionObservation/disappearance',
 'TestInventorySessionObservation/held-session',
 'TestInventorySessionObservation/query-error',
 'TestInventorySessionObservation/canceled-after-zero',
 'TestInventorySessionObservation/query-deadline',
 'TestExecutionBounds/total-object',
})


def required_identities():
    return {(package, test) for package, tests in required.items() for test in tests | {""}}


def check_events(events):
    passes = {(event["Package"], event.get("Test", ""))
              for event in events if event.get("Action") == "pass"}
    failures = [event for event in events if event.get("Action") in ("fail", "skip")]
    missing = [package + "/" + test for package, test in sorted(required_identities() - passes)]
    return {"schema": "coldkeep-phase1b-events/v1", "missing": missing,
            "failures_or_skips": failures,
            "required_tests": sum(map(len, required.values())), "required_package_passes": len(required),
            "passed_tests": sum(bool(test) for _, test in passes),
            "status": "PASS" if not missing and not failures else "FAIL"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("events", type=Path)
    args = parser.parse_args()
    events = [json.loads(line) for line in args.events.read_text().splitlines() if line.strip()]
    result = check_events(events)
    args.events.with_name("acceptance-events.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))
    return int(result["status"] != "PASS")


if __name__ == "__main__":
    raise SystemExit(main())
