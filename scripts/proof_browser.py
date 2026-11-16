#!/usr/bin/env python3
"""Run the real embedded binary, browser, independent reread and process lease proof."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
import urllib.request

ROOT = Path(__file__).resolve().parent.parent

def port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]

def wait_ready(process, url):
    for _ in range(200):
        if process.poll() is not None:
            raise RuntimeError('daemon exited before ready')
        try:
            with urllib.request.urlopen(url, timeout=0.2) as r:
                if r.status == 200:
                    return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError('daemon not ready')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    output = Path(args.output).resolve()
    output.mkdir(mode=0o700)
    binary = Path(args.binary).resolve()
    fixture = output / 'working-fixture'
    env = os.environ.copy()
    env.update(COLDKEEP_PROOF_FIXTURE=str(fixture), COLDKEEP_PROOF_ADDRESS='127.0.0.1:' + str(port()))
    with (output / 'fixture.log').open('wb') as log:
        subprocess.run(['go', 'test', '-count=1', './internal/proofdaemon', '-run', '^TestProofFixture$', '-v'], cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
    config_path = fixture / 'config.json'
    config = json.loads(config_path.read_text())
    facts = {'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(), 'process_runtime': 'Go binary; Node is only the headless test driver', 'events': []}
    with (output / 'daemon.log').open('wb') as log:
        daemon = subprocess.Popen([str(binary), '--config', str(config_path)], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
        try:
            wait_ready(daemon, config['public_url'])
            subprocess.run(['node', 'web/scripts/browser-proof.mjs', str(config_path), str(output / 'browser.json')], cwd=ROOT, check=True)
            if daemon.poll() is not None:
                raise RuntimeError('browser closure stopped daemon')
            facts['events'].append('browser_closed_daemon_alive')
            other = dict(config)
            fresh = fixture / 'second-output'
            fresh.mkdir(mode=0o700)
            other['restore_root'] = str(fresh)
            # Existing invalid catalog: contention must precede database opening.
            invalid = fixture / 'invalid.db'
            invalid.write_bytes(b'not a database')
            other['catalog'] = str(invalid)
            other_path = fixture / 'contender.json'
            other_path.write_text(json.dumps(other))
            other_path.chmod(0o600)
            result = subprocess.run([str(binary), '--config', str(other_path)], cwd=ROOT, capture_output=True, text=True, timeout=10)
            (output / 'contender.log').write_text(result.stdout + result.stderr)
            if result.returncode == 0 or 'repository is busy' not in result.stderr:
                raise RuntimeError('second process failed to prove canonical-root contention')
            facts['events'].append('second_process_refused_after_browser_close_before_catalog')
        finally:
            if daemon.poll() is None:
                daemon.send_signal(signal.SIGTERM)
            code = daemon.wait(timeout=60)
            facts['daemon_exit'] = code
            if code != 0:
                raise RuntimeError('daemon did not shut down cleanly')
    facts['events'].append('shutdown_joined')
    # Reacquire with a fresh destination, then shut down without any browser.
    config['restore_root'] = str(fixture / 'second-output')
    config_path.write_text(json.dumps(config))
    with (output / 'reacquire.log').open('wb') as log:
        daemon = subprocess.Popen([str(binary), '--config', str(config_path)], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
        try:
            wait_ready(daemon, config['public_url'])
            facts['events'].append('new_process_acquired_after_shutdown')
        finally:
            daemon.send_signal(signal.SIGTERM)
            if daemon.wait(timeout=60) != 0:
                raise RuntimeError('reacquired daemon cleanup failed')
    facts['credentials_removed'] = not (fixture / 'runtime/bootstrap').exists()
    if not facts['credentials_removed']:
        raise RuntimeError('bootstrap remained after shutdown')
    (output / 'process.json').write_text(json.dumps(facts, indent=2) + '\n')
    # These copies are owned and disposable; preserve observations, not secrets.
    import shutil
    shutil.rmtree(fixture)
    print('PROOF_EVENT browser_close_process_contention_shutdown_cleanup')

if __name__ == '__main__':
    main()
