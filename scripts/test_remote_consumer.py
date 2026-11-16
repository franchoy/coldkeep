"""Exercise the actual checkout-free workflow consumer verifier."""
import json
import os
from pathlib import Path
import tempfile
import textwrap
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]


def consumer_source():
    workflow = (ROOT / '.github/workflows/ci.yml').read_text()
    section = workflow.split('      - name: Install exact public candidate through an isolated consumer\n', 1)[1]
    return textwrap.dedent(section.split('        run: |\n', 1)[1].split('      - name:', 1)[0])


class RemoteConsumerTests(unittest.TestCase):
    def setUp(self):
        self.scope = {'__name__': 'consumer_verifier_test'}
        exec(compile(consumer_source(), 'ci-consumer-inline.py', 'exec'), self.scope)
        self.module = self.scope['MODULE']
        self.sha = 'a' * 40
        self.version = 'v2.0.0-20261109104307-ac9382c9e620'
        self.identity = {'Path': self.module, 'Version': self.version,
                         'Origin': {'VCS': 'git', 'URL': 'https://github.com/franchoy/coldkeep', 'Hash': self.sha}}

    def test_full_origin_authority(self):
        self.scope['origin'](self.identity, self.sha)
        mutations = [dict(Path=self.module + '-other'), dict(Replace={'Path': '/local'}), dict(Error='unavailable'),
                     dict(Version='(devel)'), dict(Origin={'VCS': 'git', 'URL': 'https://example.com/coldkeep', 'Hash': self.sha}),
                     dict(Origin={'VCS': 'git', 'URL': 'https://github.com/franchoy/coldkeep', 'Hash': 'b' * 40}),
                     dict(Origin={'VCS': 'hg', 'URL': 'https://github.com/franchoy/coldkeep', 'Hash': self.sha})]
        for mutation in mutations:
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                self.scope['origin'](dict(self.identity, **mutation), self.sha)

    def test_directives_refuse_go_parser_grouped_or_single_results(self):
        self.scope['directives']({'Replace': None, 'Exclude': None})
        for key in ('Replace', 'Exclude'):
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, 'directives refused'):
                self.scope['directives']({key: [{'Path': self.module, 'Version': self.version}]})

    def test_actual_selected_graph_not_require_line(self):
        with tempfile.TemporaryDirectory() as directory:
            cache = Path(directory)
            download = {'Dir': str(cache / 'candidate'), 'GoMod': str(cache / 'candidate/go.mod'), 'Sum': 'h1:sum', 'GoModSum': 'h1:mod'}
            candidate = dict(download, Path=self.module, Version=self.version)
            rows = [{'Path': 'coldkeep-ci-consumer', 'Main': True}, candidate]
            encode = lambda value: '\n'.join(json.dumps(row) for row in value)
            self.scope['graph'](encode(rows), self.version, download, cache)
            for mutation in [{'Version': 'v2.0.1'}, {'Main': True}, {'Replace': {'Dir': '/local'}}, {'Dir': '/outside'},
                             {'GoMod': '/outside/go.mod'}, {'Sum': 'h1:wrong'}, {'GoModSum': 'h1:wrong'}, {'Error': 'broken'}]:
                with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                    self.scope['graph'](encode([rows[0], dict(candidate, **mutation)]), self.version, download, cache)
            for invalid in [[], [rows[0]], rows + [candidate]]:
                with self.subTest(rows=invalid), self.assertRaises(ValueError):
                    self.scope['graph'](encode(invalid), self.version, download, cache)

    def test_exact_binary_fields(self):
        value = '\n'.join(['coldkeep: go1.26.11', '\tpath\t' + self.module + '/cmd/coldkeep',
                           '\tmod\t' + self.module + '\t' + self.version + '\th1:sum', '\tbuild\tCGO_ENABLED=1'])
        self.scope['binary_identity'](value, self.version, 'h1:sum')
        for wrong in [value.replace('go1.26.11', 'go1.26.9'), value.replace('/cmd/coldkeep', '/cmd/other'),
                      value.replace(self.version, '(devel)'), value.replace(self.version, self.version + '0'),
                      value.replace('h1:sum', 'h1:wrong'), value.replace('CGO_ENABLED=1', 'CGO_ENABLED=0'),
                      value + '\n\t=>\t/local', '', value.replace('\th1:sum', '')]:
            with self.subTest(value=wrong), self.assertRaises(ValueError):
                self.scope['binary_identity'](wrong, self.version, 'h1:sum')

    def test_native_failure_preserved_and_ambient_configuration_sanitized(self):
        with tempfile.TemporaryDirectory() as directory:
            environment = dict(TRIGGER_SHA=self.sha, CANDIDATE_QUERY=self.sha, EVENT_REF_TYPE='branch',
                               PUBLIC_REPOSITORY_URL='https://github.com/franchoy/coldkeep.git', RUNNER_TEMP=directory,
                               GOENV='/hostile/settings', GOFLAGS='-modfile=/hostile/go.mod', GOWORK='/hostile/go.work',
                               GOPATH='/hostile/gopath', GOSUMDB='off', GIT_CONFIG_COUNT='1')
            observed = []

            def fail_command(_args, **kwargs):
                observed.append(kwargs['env'])
                return mock.Mock(returncode=17)

            with mock.patch.dict(os.environ, environment), mock.patch.object(self.scope['subprocess'], 'run', side_effect=fail_command):
                with self.assertRaisesRegex(ValueError, 'command failed: 17'):
                    self.scope['main']()
            self.assertEqual(len(observed), 1)
            child = observed[0]
            for key, wanted in {'GOENV': 'off', 'GOWORK': 'off', 'GOFLAGS': '', 'GOSUMDB': 'sum.golang.org', 'GOTOOLCHAIN': 'local'}.items():
                self.assertEqual(child[key], wanted)
            self.assertNotIn('GIT_CONFIG_COUNT', child)
            self.assertTrue(Path(child['GOPATH']).is_relative_to(directory))
            result = json.loads((Path(directory) / 'remote-install-evidence/result.json').read_text())
            self.assertFalse(result['success'])
            self.assertTrue(result['cleanup_complete'])
            self.assertEqual(list(Path(directory).glob('coldkeep-consumer-*')), [])

    def test_full_orchestration_refuses_bad_results_and_missing_binary(self):
        for fault in ('none', 'missing-json', 'malformed-json', 'wrong-cli', 'missing-binary', 'post-install-upgrade'):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as directory:
                environment = dict(TRIGGER_SHA=self.sha, CANDIDATE_QUERY=self.sha, EVENT_REF_TYPE='branch',
                                   PUBLIC_REPOSITORY_URL='https://github.com/franchoy/coldkeep.git', RUNNER_TEMP=directory,
                                   GITHUB_STEP_SUMMARY=str(Path(directory) / 'summary'))
                selected = None
                graph_reads = 0

                def native(args, **kwargs):
                    nonlocal selected, graph_reads
                    child, cwd = kwargs['env'], kwargs['cwd']
                    output = ''
                    if args[:2] == ['go', 'env']:
                        facts = {key: child.get(key, '') for key in args[3:]}
                        facts.update(GOVERSION='go1.26.11', GOENV='', GOMOD=str(cwd / 'go.mod') if (cwd / 'go.mod').exists() else os.devnull)
                        output = json.dumps(facts)
                    elif args[:4] == ['go', 'list', '-m', '-json']:
                        output = json.dumps(self.identity)
                        if args[-1].endswith(self.sha):
                            output = '' if fault == 'missing-json' else ('{' if fault == 'malformed-json' else output)
                    elif args[:3] == ['go', 'mod', 'download']:
                        source = Path(child['GOMODCACHE']) / 'candidate'
                        source.mkdir()
                        (source / 'go.mod').write_text('module ' + self.module + '\n')
                        selected = dict(self.identity, Dir=str(source), GoMod=str(source / 'go.mod'), Sum='h1:sum', GoModSum='h1:mod')
                        output = json.dumps(selected)
                    elif args[:4] == ['go', 'mod', 'edit', '-json']:
                        output = '{}'
                    elif args[:3] == ['go', 'mod', 'init']:
                        (cwd / 'go.mod').write_text('module coldkeep-ci-consumer\n')
                        (cwd / 'go.sum').write_text('fixture\n')
                    elif args[:5] == ['go', 'list', '-mod=readonly', '-m', '-json']:
                        graph_reads += 1
                        candidate = dict(selected)
                        if graph_reads == 2 and fault == 'post-install-upgrade':
                            candidate['Version'] = 'v2.0.1'
                        output = json.dumps(candidate)
                    elif args[:2] == ['go', 'install']:
                        if fault != 'missing-binary':
                            (Path(child['GOBIN']) / ('coldkeep.exe' if os.name == 'nt' else 'coldkeep')).write_bytes(b'fixture binary')
                    elif args[:3] == ['go', 'version', '-m']:
                        output = '\n'.join(['coldkeep: go1.26.11', '\tpath\t' + self.module + '/cmd/coldkeep',
                                            '\tmod\t' + self.module + '\t' + self.version + '\th1:sum', '\tbuild\tCGO_ENABLED=1'])
                    elif args[-1] == 'version':
                        if fault == 'missing-binary':
                            raise FileNotFoundError('binary absent')
                        output = 'coldkeep version ' + ('wrong' if fault == 'wrong-cli' else '2.0.0-dev.0')
                    kwargs['stdout'].write(output.encode())
                    return mock.Mock(returncode=0)

                with mock.patch.dict(os.environ, environment), mock.patch.object(self.scope['subprocess'], 'run', side_effect=native):
                    if fault == 'none':
                        self.scope['main']()
                    else:
                        with self.assertRaises((ValueError, FileNotFoundError)):
                            self.scope['main']()
                result = json.loads((Path(directory) / 'remote-install-evidence/result.json').read_text())
                self.assertEqual(result['success'], fault == 'none')
                self.assertTrue(result['cleanup_complete'])
                self.assertEqual(list(Path(directory).glob('coldkeep-consumer-*')), [])


if __name__ == '__main__':
    unittest.main()
