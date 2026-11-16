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

BYTE_FIELDS = ('logical_file_original_name','snapshot_path_path','chunk_chunk_hash',
               'snapshot_id','snapshot_label','container_filename')
def scalar_cases(parent, sqlite):
    cases = {parent}
    for field in BYTE_FIELDS:
        cases.add(parent+'/'+field)
        names = ['ascii_exact','ascii_over','utf8_exact','utf8_over','empty']
        if sqlite: names.append('nul_over')
        if field == 'snapshot_label': names.append('null')
        cases.update(parent+'/'+field+'/'+name for name in names)
    return cases

REQUIRED[PREFIX+'internal/proofdaemon'].update({
    'TestProofReviewRejectsRestoreRootTrimAlias',
    *('TestProofReviewRejectsRestoreRootTrimAlias/'+x for x in ('ascii_space','unicode_nonbreaking_space')),
    'TestProofReviewRestoreRootValidControls',
    *('TestProofReviewRestoreRootValidControls/'+x for x in ('ordinary_sibling','internal_space')),
    'TestProofReviewRejectsOversizedNULLabelBeforeHistory',
    'TestProofReviewLabelBoundaryControls',
    *('TestProofReviewLabelBoundaryControls/'+x for x in ('ascii_at_1024','utf8_1024_bytes','ascii_over_1024','utf8_over_1024','empty','bounded_nul')),
    'TestProofSQLiteEncodingAdmission',
    *('TestProofSQLiteEncodingAdmission/'+x for x in ('UTF-16le','UTF-16be')),
    *scalar_cases('TestProofScalarByteFieldsSQLite',True),
})
CRITICAL.update({'restore_alias_refused_preserved','restore_valid_root_hash_metadata',
                 'nul_label_refused_before_engine_preserved','scalar_byte_fields_sqlite',
                 'sqlite_utf16_refused_before_reads'})
POSTGRES_REQUIRED = {PREFIX+'internal/proofdaemon':scalar_cases('TestProofPostgresFixture',False)}
POSTGRES_CRITICAL = {'postgres_old_writer_fixture_prepared','scalar_byte_fields_postgres','postgres_encoding_admission'}

def validate_postgres(records, exit_code):
    return validate(records,exit_code,POSTGRES_REQUIRED,POSTGRES_CRITICAL)

def validate(records, exit_code, required=REQUIRED, critical=CRITICAL):
    if exit_code != 0:
        raise ValueError('nonzero Go test exit')
    if not records or any(r.get('Action') in ('fail','skip') for r in records):
        raise ValueError('empty, failed or skipped proof stream')
    for package, tests in required.items():
        for test in tests:
            for action in ('run','pass'):
                if not any(r.get('Package')==package and r.get('Test')==test and r.get('Action')==action for r in records):
                    raise ValueError('missing required '+action+' '+test)
    text=''.join(r.get('Output','') for r in records)
    for event in critical:
        if 'PROOF_EVENT '+event not in text:
            raise ValueError('missing critical event '+event)
    return {'required_tests':sum(map(len,required.values())), 'critical_events':sorted(critical)}

if __name__=='__main__':
    import sys
    print(json.dumps(validate([json.loads(s) for s in Path(sys.argv[1]).read_text().splitlines()], int(sys.argv[2]))))
