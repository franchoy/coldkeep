#!/usr/bin/env python3
"""Fail-closed action 2.02 Go execution proof, used by the integrated runner."""
import json
from pathlib import Path

PREFIX = 'github.com/franchoy/coldkeep/v2/'
REQUIRED = {
    PREFIX+'internal/proofdaemon': {
        'TestProofDTOContract', 'TestProofPhysicalCloseJoin', 'TestProofFixture',
        'TestProofRuntimeHistoryRestore', 'TestProofLeaseBeforeCatalogAndProcess',
        'TestProofDisconnectDrain', 'TestProofEnvelope', 'TestProofHTTP', 'TestProofNativeSQLitePersistence',
        *('TestProofEnvelope/'+case for case in ('rows','size','name','backend','missing','output','unprepared','wrong-key','wrong-root')),
        *('TestProofHTTP/'+case for case in ('missing','wrong','host','origin','no_origin')),
    },
    PREFIX+'internal/webui': {'TestProofEmbeddedManifest'},
}
CRITICAL = {
    'go_dto_openapi_agreement', 'physical_driver_close_join_and_no_reconnect',
    'fixture_old_writer_copied', 'engine_history_restore_and_collision',
    'lease_before_catalog_process_and_failure_cleanup',
    'catalog_actual_read_bounds_and_tuple_refusals',
    'auth_origin_host_expiry_replay_no_mutation_bounds',
    'sqlite_real_localwriter_close_reopen_restore',
    'embedded_manifest_missing_corrupt_refusal',
    'disconnect_continues_shutdown_joins_before_release',
}

def validate(records, exit_code):
    if exit_code != 0:
        raise ValueError('nonzero Go test exit')
    if not records or any(r.get('Action') in ('fail','skip') for r in records):
        raise ValueError('empty, failed or skipped proof stream')
    for package, tests in REQUIRED.items():
        for test in tests:
            for action in ('run','pass'):
                if not any(r.get('Package')==package and r.get('Test')==test and r.get('Action')==action for r in records):
                    raise ValueError('missing required '+action+' '+test)
    text=''.join(r.get('Output','') for r in records)
    for event in CRITICAL:
        if 'PROOF_EVENT '+event not in text:
            raise ValueError('missing critical event '+event)
    return {'required_tests':sum(map(len,REQUIRED.values())), 'critical_events':sorted(CRITICAL)}

if __name__=='__main__':
    import sys
    print(json.dumps(validate([json.loads(s) for s in Path(sys.argv[1]).read_text().splitlines()], int(sys.argv[2]))))
