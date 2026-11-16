"""Mutation controls for the required package and remote consumer jobs."""
from pathlib import Path
import unittest

from check_ci_followup import validate

ROOT = Path(__file__).resolve().parents[1]


class FollowupGateTests(unittest.TestCase):
    def test_current_workflow(self):
        self.assertEqual(validate((ROOT / '.github/workflows/ci.yml').read_text()), [])

    def test_postgresql_client_jobs_reject_moving_or_unprovisioned_runners(self):
        workflow = (ROOT / '.github/workflows/ci.yml').read_text()
        for job in ('package-tests', 'correctness-matrix'):
            before, after = workflow.split('  ' + job + ':\n', 1)
            for runner in ('ubuntu-latest', 'ubuntu-26.04', '${{ vars.RUNNER }}'):
                with self.subTest(job=job, runner=runner):
                    changed = before + '  ' + job + ':\n' + after.replace(
                        '    runs-on: ubuntu-24.04\n', '    runs-on: ' + runner + '\n', 1)
                    self.assertNotEqual(changed, workflow)
                    self.assertIn('PostgreSQL 16 client runner pinned: ' + job, validate(changed))

    def test_package_coverage_and_dependencies_remain_blocking(self):
        workflow = (ROOT / '.github/workflows/ci.yml').read_text()
        for old, new, expected in [
            ('codec: [plain, aes-gcm]', 'codec: [plain]', 'both codecs'),
            ('bash scripts/run_package_tests.sh', 'go test ./internal/engine', 'staging command'),
            ('COLDKEEP_CODEC: ${{ matrix.codec }}', 'COLDKEEP_CODEC: plain', 'codec selection'),
            ('COLDKEEP_PHASE1C_TEST: 1', 'COLDKEEP_PHASE1C_TEST: 0', 'prerequisite'),
            ('      - name: Run complete staged package tests\n', '      - name: Run complete staged package tests\n        continue-on-error: true\n', 'blocking'),
            ('      - name: Run complete staged package tests\n', '      - name: Run complete staged package tests\n        if: false\n', 'unconditional'),
            ("PACKAGE_TESTS_RESULT: ${{ needs['package-tests'].result }}", 'PACKAGE_TESTS_RESULT: success', 'aggregate result'),
            ('[ "${PACKAGE_TESTS_RESULT}" != "success" ] ||', '[ "${PACKAGE_TESTS_RESULT}" = "failure" ] ||', 'rejects non-success'),
            ('scripts/run_recovery_export_tests.sh', 'scripts/omitted.sh', 'dedicated recovery'),
        ]:
            with self.subTest(mutation=old):
                changed = workflow.replace(old, new)
                self.assertNotEqual(changed, workflow)
                self.assertTrue(any(expected in error for error in validate(changed)), validate(changed))
        for job in ('legacy-compatibility', 'smoke', 'benchmark-integrity', 'benchmark-timing-advisory', 'critical-coverage-report', 'ci-required'):
            with self.subTest(job=job):
                before, after = workflow.split('  ' + job + ':\n', 1)
                changed = before + '  ' + job + ':\n' + after.replace(', package-tests', '', 1)
                self.assertIn('complete package downstream dependency: ' + job, validate(changed))
        runner = (ROOT / 'scripts/run_staged_package_tests.py').read_text()
        for old, new in [('"./cmd/...", "./internal/..."', '"./internal/engine"'), ('"-race",', ''), ('"-count=1",', ''), ('"-p=1",', ''), ('"-timeout=35m"', '"-timeout=0"')]:
            with self.subTest(runner=old):
                self.assertIn('complete unfiltered package command', validate(workflow, runner.replace(old, new)))

    def test_wrappers_cannot_bypass_staging_or_required_events(self):
        workflow = (ROOT / '.github/workflows/ci.yml').read_text()
        wrappers = {scope: (ROOT / 'scripts' / name).read_text() for scope, name in
                    [('packages', 'run_package_tests.sh'), ('recovery', 'run_recovery_export_tests.sh'),
                     ('passive', 'run_passive_inventory_tests.sh')]}
        for scope in wrappers:
            for mutation in ('exec go test -race -count=1 ./internal/engine\n',
                             wrappers[scope].replace('exec python3', 'echo python3'),
                             wrappers[scope].replace('--scope ' + scope, '--scope omitted')):
                with self.subTest(scope=scope, mutation=mutation):
                    self.assertTrue(any('wrapper delegates' in error for error in
                                        validate(workflow, wrappers=dict(wrappers, **{scope: mutation}))))

    def test_passive_inventory_requires_complete_prerequisites_and_selection(self):
        workflow = (ROOT / '.github/workflows/ci.yml').read_text()
        anchor = '      - name: Run passive inventory PostgreSQL contracts\n'
        before, after = workflow.split(anchor, 1)
        for old, new in [('COLDKEEP_PHASE1C_TEST: 1', 'COLDKEEP_PHASE1C_TEST: 0'),
                         ('COLDKEEP_PG_DUMP: /usr/lib/postgresql/16/bin/pg_dump', 'COLDKEEP_PG_DUMP: /missing'),
                         ('bash scripts/run_passive_inventory_tests.sh', 'go test ./internal/coordination')]:
            with self.subTest(mutation=old):
                changed = before + anchor + after.replace(old, new, 1)
                self.assertTrue(any('passive inventory' in error for error in validate(changed)))
        changed = workflow.replace(anchor, anchor + '        continue-on-error: true\n')
        self.assertIn('passive inventory remains blocking in plain matrix', validate(changed))
        runner = (ROOT / 'scripts/run_staged_package_tests.py').read_text()
        self.assertIn('passive inventory selection and staging budget retained',
                      validate(workflow, runner.replace('"^TestRecovery"', '"TestBinding"')))

    def test_remote_identity_controls_cannot_be_removed(self):
        workflow = (ROOT / '.github/workflows/ci.yml').read_text()
        for old, new in [
            ("origin(resolved, expected)", 'pass'),
            ("origin(final_origin, expected)", 'pass'),
            ("GOENV='off'", "GOENV=''"),
            ("GOWORK='off'", "GOWORK='auto'"),
            ("GOSUMDB='sum.golang.org'", "GOSUMDB='off'"),
            ("graph(command('graph-after'", "graph(command('graph-omitted'"),
            ("binary_identity(command('binary-build-info'", "print(command('binary-build-info'"),
            ("directives(json.loads(command('download-go-mod'", "print(json.loads(command('download-go-mod'"),
            ("'GOMODCACHE', 'mod'", "'GOMODCACHE', '/ambient'"),
            ("require(result.returncode == 0", "require(True"),
        ]:
            with self.subTest(mutation=old):
                changed = workflow.replace(old, new)
                self.assertNotEqual(changed, workflow)
                self.assertTrue(any('reviewed body' in error for error in validate(changed)))


if __name__ == '__main__':
    unittest.main()
