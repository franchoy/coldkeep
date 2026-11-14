#!/usr/bin/env python3
"""Keep the complete package and exact remote-consumer gates blocking."""
import ast
import hashlib
from pathlib import Path
import re
import sys
import textwrap

ROOT = Path(__file__).resolve().parents[1]
# Bind the reviewed inline installer, including every refusal and native exit.
# Runtime verifier mutations separately exercise its origin/graph/binary checks.
CONSUMER_SHA256 = 'd5ea961b26f881942dd594e562474c179a804e9fe24a2f262e96160fd856d62b'


def section(text, name, indent):
    match = re.search(r'^' + ' ' * indent + re.escape(name) + r':\n(.*?)(?=^' + ' ' * indent + r'[\w-]+:|\Z)', text, re.M | re.S)
    return match.group(0) if match else ''


def step(text, name):
    anchor = '      - name: ' + name + '\n'
    return anchor + text.split(anchor, 1)[1].split('      - name:', 1)[0] if anchor in text else ''


def validate(workflow, runner=None, wrappers=None):
    errors = []

    def need(condition, description):
        if not condition:
            errors.append(description)

    packages = section(workflow, 'package-tests', 2)
    for job in ('package-tests', 'correctness-matrix'):
        runner_lines = [line for line in section(workflow, job, 2).splitlines()
                        if line.startswith('    runs-on:')]
        need(runner_lines == ['    runs-on: ubuntu-24.04'],
             'PostgreSQL 16 client runner pinned: ' + job)
    execute = step(packages, 'Run complete staged package tests')
    need('codec: [plain, aes-gcm]' in packages, 'complete package matrix retains both codecs')
    need('fetch-depth: 0' in step(packages, 'Checkout'), 'complete package historical checkout')
    need("go-version: '1.26.11'" in step(packages, 'Setup Go'), 'complete package pinned Go')
    need('run: python3 scripts/prepare_daemon.py\n' in step(packages, 'Prepare embedded assets for complete package tests'), 'complete package embedded preparation')
    need('postgresql-client-16' in step(packages, 'Install package PostgreSQL 16 clients'), 'complete package matched clients')
    need('        run: bash scripts/run_package_tests.sh "$RUNNER_TEMP/package-tests"\n' in execute, 'complete package staging command')
    need('COLDKEEP_CODEC: ${{ matrix.codec }}' in execute, 'complete package codec selection')
    for value in ('COLDKEEP_PHASE1B_TEST: 1', 'COLDKEEP_PHASE1C_TEST: 1',
                  'COLDKEEP_PG_DUMP: /usr/lib/postgresql/16/bin/pg_dump',
                  'COLDKEEP_PG_RESTORE: /usr/lib/postgresql/16/bin/pg_restore',
                  "COLDKEEP_KEY: ${{ matrix.codec == 'aes-gcm' && env.COLDKEEP_AES_GCM_FIXTURE_HEX || '' }}"):
        need(value in execute, 'complete package prerequisite: ' + value.split(':', 1)[0])
    image = 'postgres:16.15-bookworm@sha256:bb3e1a57e5407e0a5280b4211980a5e537f4abd234a87014ac979849a78dd825'
    need('image: ' + image in packages and 'COLDKEEP_RECOVERY_IMAGE: ' + image in execute, 'complete package pinned service and recovery image')
    need(not re.search(r'^    if:|continue-on-error|\|\| true|; true', packages, re.M), 'complete package job remains blocking')
    for name in ('Run complete staged package tests', 'Checkout', 'Setup Go', 'Prepare embedded assets for complete package tests', 'Install package PostgreSQL 16 clients'):
        need(not re.search(r'^        if:', step(packages, name), re.M), 'complete package step remains unconditional: ' + name)
    upload = step(packages, 'Upload complete package evidence')
    need('if: ${{ always() }}' in upload and 'if-no-files-found: error' in upload, 'complete package failure evidence retained')
    for job in ('legacy-compatibility', 'smoke', 'benchmark-integrity', 'benchmark-timing-advisory', 'critical-coverage-report', 'ci-required'):
        block = section(workflow, job, 2)
        line = next((s for s in block.splitlines() if s.startswith('    needs:')), '')
        need('package-tests' in line and 'quality' in line, 'complete package downstream dependency: ' + job)
    aggregate = section(workflow, 'ci-required', 2)
    need("PACKAGE_TESTS_RESULT: ${{ needs['package-tests'].result }}" in aggregate, 'complete package aggregate result')
    need('[ "${PACKAGE_TESTS_RESULT}" != "success" ] ||' in aggregate, 'complete package aggregate rejects non-success')
    need('bash scripts/run_recovery_export_tests.sh "$RUNNER_TEMP/integration-diag/recovery-export"' in workflow, 'dedicated recovery gate retained')
    correctness = section(workflow, 'correctness-matrix', 2)
    passive_name = 'Run passive inventory PostgreSQL contracts'
    clients_name = 'Install recovery PostgreSQL 16 clients'
    passive = step(correctness, passive_name)
    clients = step(correctness, clients_name)
    need('    timeout-minutes: 40\n' in correctness, 'serial staged correctness campaign budget')
    need('        run: bash scripts/run_passive_inventory_tests.sh "$RUNNER_TEMP/integration-diag/passive-inventory"\n' in passive,
         'passive inventory staged command retained')
    need('        if: ${{ matrix.codec == \'plain\' }}\n' in passive and
         not re.search(r'continue-on-error|\|\| true|; true', passive), 'passive inventory remains blocking in plain matrix')
    need('postgresql-client-16' in clients and clients_name in correctness and passive_name in correctness
         and correctness.index(clients_name) < correctness.index(passive_name), 'matched clients precede passive inventory')
    for value in ('COLDKEEP_PHASE1B_TEST: 1', 'COLDKEEP_PHASE1C_TEST: 1', 'COLDKEEP_CODEC: plain',
                  "COLDKEEP_KEY: ''", 'COLDKEEP_RECOVERY_IMAGE: ' + image,
                  'COLDKEEP_PG_DUMP: /usr/lib/postgresql/16/bin/pg_dump',
                  'COLDKEEP_PG_RESTORE: /usr/lib/postgresql/16/bin/pg_restore'):
        need(value in passive, 'passive inventory prerequisite: ' + value.split(':', 1)[0])
    if runner is None:
        runner = (ROOT / 'scripts/run_staged_package_tests.py').read_text()
    if wrappers is None:
        wrappers = {scope: (ROOT / 'scripts' / name).read_text() for scope, name in
                    [('packages', 'run_package_tests.sh'), ('recovery', 'run_recovery_export_tests.sh'),
                     ('passive', 'run_passive_inventory_tests.sh')]}
    for scope, body in wrappers.items():
        active = '\n'.join(line for line in body.splitlines() if line and not line.startswith('#'))
        expected = '\n'.join(['set -euo pipefail', 'if [ "$#" -ne 1 ]; then',
                              "  echo 'requires one new output directory' >&2", '  exit 2', 'fi',
                              'exec python3 "$(dirname -- "$0")/run_staged_package_tests.py" --scope ' + scope + ' --output "$1"'])
        need(active == expected, 'complete ' + scope + ' wrapper delegates only to staged runner')
    need(set(wrappers) == {'packages', 'recovery', 'passive'}, 'all three staged wrapper contracts present')
    tree = ast.parse(runner)
    commands = next((ast.literal_eval(node.value) for node in tree.body if isinstance(node, ast.Assign)
                     and any(isinstance(t, ast.Name) and t.id == 'COMMANDS' for t in node.targets)), {})
    need(commands.get('packages') == ['go', 'test', '-race', '-count=1', '-p=1', '-json', '-timeout=35m', './cmd/...', './internal/...'], 'complete unfiltered package command')
    need(commands.get('passive') == ['go', 'test', '-race', '-count=1', '-p=1', '-skip', '^TestRecovery', '-json', '-timeout=15m',
                                    './internal/recoveryexport', './internal/coordination'], 'passive inventory selection and staging budget retained')

    remote = section(workflow, 'remote-candidate-install', 2)
    consumer = step(remote, 'Install exact public candidate through an isolated consumer')
    need('        if: github.ref_type != \'tag\'\n' in consumer and '        shell: python\n' in consumer, 'remote consumer runs on every non-tag platform')
    source = textwrap.dedent(consumer.split('        run: |\n', 1)[1]) if '        run: |\n' in consumer else ''
    need(hashlib.sha256(source.encode()).hexdigest() == CONSUMER_SHA256, 'remote consumer reviewed body preserves isolation, exact origin, selected graph and binary checks')
    need("if: runner.os != 'Windows' && github.ref_type == 'tag'" in step(remote, 'Install exact public tag outside a module (Unix)'), 'Unix tag installer remains tag-only')
    need("if: runner.os == 'Windows' && github.ref_type == 'tag'" in step(remote, 'Install exact public tag outside a module (Windows)'), 'Windows tag installer remains tag-only')
    upload = step(remote, 'Upload remote consumer identity evidence')
    need("if: ${{ always() && github.ref_type != 'tag' }}" in upload and 'if-no-files-found: error' in upload, 'remote consumer failure evidence retained')
    return errors


def main():
    errors = validate(Path(sys.argv[1]).read_text())
    for error in errors:
        print('[audit] ERROR: ' + error)
    if not errors:
        print('[audit] ok: complete staged package matrix and reviewed remote consumer remain required')
    return bool(errors)


if __name__ == '__main__':
    sys.exit(main())
