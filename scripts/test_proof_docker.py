"""Exercise real orchestration with a stateful Docker process boundary."""
import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock
from urllib.parse import urlparse

import check_phase2_events as events
import proof_docker as proof
from proof_process import execute, ProcessCleanupError


class DockerBoundary:
    def __init__(self):
        self.containers, self.networks, self.images = {}, {}, set()
        self.calls = []
        self.configs = {}
        self.containers['unrelated-container'] = 'other'
        self.networks['unrelated-network'] = 'other'
        self.images.add('unrelated-image')

    def __call__(self, command, **kwargs):
        env = kwargs['env']
        self.calls.append((list(command), dict(env), kwargs['timeout']))
        for config in Path(env.get('PROOF_PRIVATE', '/absent')).glob('*.json'):
            self.configs[config.name] = json.loads(config.read_text())
        result, status = '', 0
        if command[:2] == ['docker', 'version']:
            result = json.dumps({'Version': '29.9.2'})
        elif command[:2] == ['docker', 'build']:
            self.images.add(command[command.index('-t') + 1])
        elif command[:3] == ['docker', 'image', 'inspect']:
            result = 'sha256:disposable-image'
        elif command[:3] == ['docker', 'image', 'ls']:
            tag = command[-1].split('=', 1)[1]
            result = tag if tag in self.images else ''
        elif command[:3] == ['docker', 'image', 'rm']:
            self.images.discard(command[-1])
        elif command[:3] == ['docker', 'network', 'create']:
            self.networks[command[-1]] = command[command.index('--label') + 1]
            result = command[-1]
        elif command[:3] == ['docker', 'network', 'ls']:
            if '--filter' in command:
                label = command[-1].removeprefix('label=')
                result = '\n'.join(name for name, owner in self.networks.items() if owner == label)
            else:
                result = '\n'.join(self.networks)
        elif command[:3] == ['docker', 'network', 'rm']:
            self.networks.pop(command[-1], None)
        elif command[:3] == ['docker', 'network', 'inspect']:
            result = '[{}]'
        elif command[:2] == ['docker', 'run']:
            name = command[command.index('--name') + 1]
            label = command[command.index('--label') + 1]
            self.containers[name] = label
            if '--rm' in command:
                self.containers.pop(name)
            if name.endswith('-second'):
                status, result = 1, 'repository is busy'
            else:
                result = name
        elif command[:2] == ['docker', 'compose']:
            project = command[command.index('-p') + 1]
            if 'up' in command:
                self.containers[project + '-daemon-1'] = 'com.docker.compose.project=' + project
                if env['PROOF_SOURCE'].endswith('absent-source'):
                    status = 1
            elif 'down' in command:
                self.containers.pop(project + '-daemon-1', None)
            elif 'ps' in command:
                result = project + '-daemon-1'
        elif command[:2] == ['docker', 'ps']:
            if '--filter' in command:
                label = command[-1].removeprefix('label=')
                result = '\n'.join(name for name, owner in self.containers.items() if owner == label)
            else:
                result = '\n'.join(self.containers)
        elif command[:2] == ['docker', 'rm']:
            self.containers.pop(command[-1], None)
        elif command[:2] == ['docker', 'inspect']:
            network = next(name for name in self.networks if name.startswith('ck-phase2-'))
            fmt = command[-1]
            if fmt == '{{json .NetworkSettings.Networks}}':
                result = json.dumps({network: {'IPAddress': '172.22.0.2'}})
            elif fmt == '{{json .NetworkSettings}}':
                result = json.dumps({'Networks': {network: {'IPAddress': '172.22.0.3'}}})
            elif fmt == '{{json .HostConfig}}':
                result = json.dumps(dict.fromkeys(('PortBindings','ReadonlyRootfs','Tmpfs','Memory','PidsLimit','CapDrop','SecurityOpt')))
            elif fmt == '{{json .HostConfig.PortBindings}}':
                result = '{}'
            elif fmt == '{{json .Mounts}}':
                result = '[]'
            elif fmt == '{{.Config.User}}':
                result = '1000:1000'
            elif fmt == '{{.State.Running}}':
                result = 'true'
            elif fmt == '{{.State.ExitCode}}':
                result = '0'
            else:
                raise AssertionError('raw or unexpected Docker inspect')
        elif command[:2] == ['go', 'test']:
            records = [{'Package': package, 'Test': test, 'Action': action}
                       for package, tests in events.POSTGRES_REQUIRED.items()
                       for test in tests for action in ('run', 'pass')]
            records += [{'Action': 'output', 'Output': 'PROOF_EVENT ' + event}
                        for event in events.POSTGRES_CRITICAL]
            result = '\n'.join(json.dumps(record) for record in records)
        elif command[:2] == ['go', 'run']:
            result = 'PROOF_EVENT postgres_credential_channels_tcp_success_and_refusals\n'
        elif command[0] == 'ip':
            result = '[{"addr_info":[{"family":"inet","scope":"global","local":"192.0.2.1"}]}]'
        return subprocess.CompletedProcess(command, status, result.encode(), b'')


class DockerCredentialsAndCleanup(unittest.TestCase):
    def exercise(self, *, stage=None, fault='after', cleanup_fault=None, cleanup_kind='nonzero'):
        boundary = DockerBoundary()
        original = proof.Commands.call
        injected = []

        def call(instance, name, command, **kwargs):
            def failed():
                injected.append(name)
                if fault == 'start':
                    raise OSError('injected unavailable child')
                if fault == 'timeout':
                    raise subprocess.TimeoutExpired(command, 1)
                if fault == 'interrupt':
                    raise KeyboardInterrupt
                raise proof.CommandFailure(name, 'injected failure')
            if name == cleanup_fault:
                def cleanup_failed(*args, **kwargs):
                    if cleanup_kind == 'timeout':
                        error = subprocess.TimeoutExpired(command, 1)
                        error.process_cleanup = {'complete': True, 'simulated': True}
                        raise error
                    if cleanup_kind == 'start':
                        raise OSError('injected cleanup start error')
                    if cleanup_kind == 'uncertain':
                        error = ProcessCleanupError('injected unresolved descendant')
                        error.process_cleanup = {'complete': False, 'failures': ['injected unknown process']}
                        raise error
                    return subprocess.CompletedProcess(command, 1, b'', b'injected cleanup failure')
                with mock.patch.object(proof, 'execute', side_effect=cleanup_failed):
                    return original(instance, name, command, **kwargs)
            if name == stage and fault == 'start':
                with mock.patch.object(proof, 'execute', side_effect=lambda *a, **k: failed()):
                    return original(instance, name, command, **kwargs)
            result = original(instance, name, command, **kwargs)
            if name == stage:
                failed()
            return result

        with tempfile.TemporaryDirectory() as temporary:
            out = Path(temporary) / 'proof'
            with contextlib.ExitStack() as stack:
                stack.enter_context(mock.patch.object(proof, 'execute', side_effect=boundary))
                stack.enter_context(mock.patch.object(proof.Commands, 'call', call))
                stack.enter_context(mock.patch.object(proof, 'campaign', return_value={}))
                stack.enter_context(mock.patch.object(proof, 'port', return_value=18766))
                stack.enter_context(mock.patch.object(proof.os, 'getuid', return_value=1000))
                response = stack.enter_context(mock.patch.object(proof.urllib.request, 'urlopen'))
                response.return_value.__enter__.return_value.status = 200
                stack.enter_context(mock.patch.dict(os.environ, {'PGHOSTADDR': '192.0.2.99',
                    'PGSERVICE': 'poison', 'PGPASSWORD': 'unrelated-ambient', 'DB_PASSWORD': 'ambient-cli'}))
                stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
                if stage or cleanup_fault:
                    with self.assertRaises(RuntimeError):
                        proof.run_proof(out)
                else:
                    proof.run_proof(out)
            facts = json.loads((out/'docker.json').read_text())
            self.assertEqual((out/'working').exists(), cleanup_kind == 'uncertain')
            self.assertEqual(facts['working_secret_scan']['match_count'], 0)
            self.assertEqual(facts['retained_secret_scan']['match_count'], 0)
            self.assertEqual(boundary.containers, {'unrelated-container': 'other'})
            self.assertEqual(boundary.networks, {'unrelated-network': 'other'})
            self.assertEqual(boundary.images, {'unrelated-image'})
            if stage:
                self.assertEqual(injected, [stage])
                self.assertIsNotNone(facts['primary_failure'])
            return facts, boundary

    def test_password_free_configs_and_exact_child_channels(self):
        facts, boundary = self.exercise()
        self.assertTrue(facts['cleanup_complete'])
        self.assertEqual(set(boundary.configs), {'setup.json', 'container.json', 'second.json'})
        for config in boundary.configs.values():
            self.assertIsNone(urlparse(config['catalog']).password)
        expected = {'postgres-create': ['POSTGRES_PASSWORD'], 'credential-controls': ['PGPASSWORD'],
                    'postgres-fixture': ['PGPASSWORD'], 'missing-mount': ['PGPASSWORD'],
                    'compose-up': ['PGPASSWORD'], 'container-contention': ['PGPASSWORD'],
                    'cli-postgres-history': ['DB_PASSWORD']}
        actual = {record['stage']: record['credential_variables'] for record in facts['commands']
                  if record['credential_variables']}
        self.assertEqual(actual, expected)
        secret = next(env['POSTGRES_PASSWORD'] for _, env, _ in boundary.calls if 'POSTGRES_PASSWORD' in env)
        for command, env, timeout in boundary.calls:
            self.assertGreater(timeout, 0)
            self.assertNotIn(secret, ' '.join(command))
            self.assertNotIn('--env-file', command)
            self.assertNotIn('PGHOSTADDR', env)
            self.assertNotIn('PGSERVICE', env)
            if 'POSTGRES_PASSWORD' in env:
                self.assertIn('POSTGRES_PASSWORD', command)
            if 'DB_PASSWORD' in env:
                self.assertIn('DB_PASSWORD', command)

    def test_failure_after_each_creation_stage_removes_owned_resources(self):
        for stage in ('postgres-create', 'postgres-fixture', 'compose-up',
                      'container-contention', 'cli-postgres-history'):
            with self.subTest(stage=stage):
                facts, _ = self.exercise(stage=stage)
                self.assertTrue(facts['cleanup_complete'])

    def test_start_failure_timeout_lost_id_and_handleable_interrupt(self):
        for stage, fault in [('postgres-fixture', 'start'), ('postgres-create', 'timeout'),
                             ('postgres-fixture', 'interrupt')]:
            with self.subTest(stage=stage, fault=fault):
                facts, _ = self.exercise(stage=stage, fault=fault)
                self.assertTrue(facts['cleanup_complete'])

    def test_cleanup_failure_continues_and_never_passes(self):
        for kind in ('nonzero', 'timeout', 'start'):
            with self.subTest(kind=kind):
                facts, _ = self.exercise(cleanup_fault='compose-cleanup', cleanup_kind=kind)
                self.assertFalse(facts['cleanup_complete'])
                self.assertTrue(facts['private_directory_removed'])
                self.assertTrue(facts['cleanup_failures'])

    def test_residual_observation_failure_is_unknown(self):
        facts, _ = self.exercise(cleanup_fault='owned_container-residuals')
        self.assertIsNone(facts['owned_container_residuals'])
        self.assertFalse(facts['cleanup_complete'])

    def test_unresolved_local_child_retains_working_files_and_fails(self):
        facts, _ = self.exercise(cleanup_fault='compose-cleanup', cleanup_kind='uncertain')
        self.assertFalse(facts['local_process_cleanup_complete'])
        self.assertFalse(facts['private_directory_removed'])
        self.assertFalse(facts['cleanup_complete'])

    def test_output_and_argv_credential_refused_without_logging_value(self):
        with tempfile.TemporaryDirectory() as temporary:
            out = Path(temporary)
            secret = proof.secrets.token_hex(32)
            commands = proof.Commands(out, {}, secret)
            with self.assertRaises(proof.CommandFailure):
                commands.run('argv', ['echo', secret])
            with mock.patch.object(proof, 'execute', return_value=subprocess.CompletedProcess([], 0, secret.encode(), b'')):
                with self.assertRaises(proof.CommandFailure):
                    commands.run('output', ['tool'])
            self.assertEqual(proof.scan_files(out, secret)['match_count'], 0)


class BoundedProcesses(unittest.TestCase):
    def run_tree(self, *, detached, early_exit):
        with tempfile.TemporaryDirectory() as temporary:
            pidfile = Path(temporary)/'child.pid'
            code = ('import subprocess,time,pathlib; '
                    'p=subprocess.Popen(["sleep","60"],start_new_session=' + repr(detached) + '); '
                    'pathlib.Path(__import__("sys").argv[1]).write_text(str(p.pid)); '
                    + ('raise SystemExit(7)' if early_exit else 'time.sleep(60)'))
            unrelated = subprocess.Popen(['sleep', '60'], start_new_session=True)
            started = time.monotonic()
            try:
                if early_exit:
                    result = execute([sys.executable, '-c', code, str(pidfile)], timeout=5,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    self.assertEqual(result.returncode, 7)
                    cleanup = result.process_cleanup
                else:
                    with self.assertRaises(subprocess.TimeoutExpired) as caught:
                        execute([sys.executable, '-c', code, str(pidfile)], timeout=1,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    cleanup = caught.exception.process_cleanup
                self.assertLess(time.monotonic() - started, 6)
                self.assertTrue(cleanup['complete'], cleanup)
                self.assertGreaterEqual(cleanup['terminated_count'], 1)
                self.assertIsNone(unrelated.poll(), 'unrelated process was signalled')
                pid = int(pidfile.read_text())
                self.assertFalse((Path('/proc')/str(pid)).exists(), 'owned child was not reaped')
            finally:
                unrelated.kill()
                unrelated.wait(timeout=2)

    def test_real_timeout_kills_and_joins_child_group(self):
        self.run_tree(detached=False, early_exit=False)

    def test_real_timeout_reaps_detached_session_holding_pipes(self):
        self.run_tree(detached=True, early_exit=False)

    def test_early_nonzero_parent_exit_reaps_detached_pipe_holder(self):
        self.run_tree(detached=True, early_exit=True)

    def test_missing_supervisor_report_never_qualifies(self):
        code = 'import os,signal; os.kill(os.getppid(),signal.SIGKILL)'
        with self.assertRaises(ProcessCleanupError) as caught:
            execute([sys.executable, '-c', code], timeout=3, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.assertFalse(caught.exception.process_cleanup['complete'])


if __name__ == '__main__':
    unittest.main()
