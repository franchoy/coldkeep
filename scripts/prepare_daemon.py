#!/usr/bin/env python3
"""Build pinned static UI then Go; bind generated assets to a source checkpoint."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

ROOT = Path(__file__).resolve().parent.parent
ASSETS = ROOT / 'internal/webui/assets'

def digest(data):
    return hashlib.sha256(data).hexdigest()

def run(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()

def fingerprint():
    paths = [p for p in (ROOT / 'web').rglob('*') if p.is_file()
             and not {'node_modules', 'dist'} & set(p.relative_to(ROOT / 'web').parts)]
    paths += [ROOT / p for p in ('scripts/prepare_daemon.py', 'scripts/generate_proof_types.py',
                                'docs/release/v2.0/proof-openapi.json')]
    return digest(''.join(p.relative_to(ROOT).as_posix() + '\0' + digest(p.read_bytes()) + '\n'
                          for p in sorted(paths)).encode())

def check():
    manifest = json.loads((ASSETS / 'manifest.json').read_text())
    if manifest['input_digest'] != fingerprint():
        raise SystemExit('stale embedded assets: frontend inputs changed; prepare again')
    for name, expected in manifest['files'].items():
        if digest((ASSETS / name).read_bytes()) != expected:
            raise SystemExit('stale or damaged embedded asset: ' + name)
    return manifest

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--check', action='store_true')
    parser.add_argument('--allow-dirty', action='store_true', help='development only, marks manifest dirty')
    parser.add_argument('--output', default='bin/coldkeepd')
    args = parser.parse_args()
    if args.check:
        check()
        print('prepared frontend fingerprint and assets match')
        return
    os.environ['GOTOOLCHAIN'] = 'local'
    os.environ['GOWORK'] = 'off'
    if run('go', 'env', 'GOVERSION') != 'go1.26.10':
        raise SystemExit('Go 1.26.10 required')
    if run('node', '--version') != 'v24.22.2' or run('npm', '--version') != '11.19.1':
        raise SystemExit('prepared build requires Node 24.22.2 / npm 11.19.1')
    dirty = bool(run('git', 'status', '--porcelain=v1', '--untracked-files=all'))
    if dirty and not args.allow_dirty:
        raise SystemExit('clean source checkpoint required; --allow-dirty is diagnostic only')
    subprocess.run(['npm', '--prefix', 'web', 'ci', '--ignore-scripts'], cwd=ROOT, check=True)
    subprocess.run(['npm', '--prefix', 'web', 'run', 'build'], cwd=ROOT, check=True)
    for p in ASSETS.iterdir():
        if p.name != 'UNPREPARED':
            if p.is_dir():
                shutil.rmtree(p)
            else:
                p.unlink()
    shutil.copytree(ROOT / 'web/dist', ASSETS, dirs_exist_ok=True)
    files = {p.relative_to(ASSETS).as_posix(): digest(p.read_bytes()) for p in sorted(ASSETS.rglob('*'))
             if p.is_file() and p.name != 'UNPREPARED'}
    manifest = {'source_revision': run('git', 'rev-parse', 'HEAD'),
                'source_tree': run('git', 'rev-parse', 'HEAD^{tree}'), 'source_dirty': dirty,
                'input_digest': fingerprint(), 'lock_digest': digest((ROOT / 'web/package-lock.json').read_bytes()),
                'node': run('node', '--version'), 'npm': run('npm', '--version'),
                'go': run('go', 'version'), 'cgo': run('go', 'env', 'CGO_ENABLED'),
                'build_flags': ['-trimpath', '-buildvcs=false'], 'files': files,
                'asset_digest': digest(''.join(n + '\0' + h + '\n' for n,h in files.items()).encode())}
    (ASSETS / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    check()
    target = Path(args.output).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(['go', 'build', '-trimpath', '-buildvcs=false', '-o', str(target), './cmd/coldkeepd'], cwd=ROOT, check=True)
    subprocess.run([str(target), '--diagnostics'], check=True)
    print('Prepared binary SHA-256: ' + digest(target.read_bytes()))

if __name__ == '__main__':
    main()
