#!/usr/bin/env python3
"""Missing/stale embedded assets, CLI-only source install and no-CGO refusal."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
from prepare_daemon import check

ROOT=Path(__file__).resolve().parent.parent

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--output',required=True)
    args=parser.parse_args()
    out=Path(args.output).resolve();out.mkdir(mode=0o700)
    manifest=check()
    facts={'prepared_manifest':manifest,'events':[]}
    # A copied source checkout has no generated assets or frontend dependencies.
    with tempfile.TemporaryDirectory(prefix='coldkeep-phase2-package-') as td:
        temp=Path(td);archive=temp/'source.tar'
        with archive.open('wb') as f:subprocess.run(['git','archive','HEAD'],cwd=ROOT,stdout=f,check=True)
        src=temp/'source';src.mkdir()
        with tarfile.open(archive) as f:f.extractall(src,filter='data')
        env=os.environ.copy();env.update(GOBIN=str(temp/'cli'),GOTOOLCHAIN='local',GOWORK='off')
        def run(name,cmd,cwd=src,expected=0):
            with (out/(name+'.log')).open('wb') as log:r=subprocess.run(cmd,cwd=cwd,env=env,stdout=log,stderr=subprocess.STDOUT)
            if r.returncode!=expected:raise RuntimeError(name+' unexpected exit '+str(r.returncode))
        run('cli-install',['go','install','./cmd/coldkeep'])
        run('cli-version',[str(temp/'cli/coldkeep'),'version'])
        deps=subprocess.check_output(['go','list','-deps','./cmd/coldkeep'],cwd=src,env=env,text=True)
        if 'internal/webui' in deps or 'internal/proofdaemon' in deps:raise RuntimeError('frontend entered CLI graph')
        facts['events'].append('cli_only_install_without_frontend_assets')
        run('missing-assets-build',['go','build','-o',str(temp/'missing'),'./cmd/coldkeepd'])
        run('missing-assets-diagnostic',[str(temp/'missing'),'--diagnostics'],expected=2)
        if 'embedded_assets_missing_or_invalid' not in (out/'missing-assets-diagnostic.log').read_text():raise RuntimeError('missing assets not named')
        facts['events'].append('missing_assets_diagnostic_refused')
        # Source staleness experiment in private copy; original source untouched.
        import shutil
        shutil.copytree(ROOT/'internal/webui/assets',src/'internal/webui/assets',dirs_exist_ok=True)
        run('fingerprint-clean',['python3','scripts/prepare_daemon.py','--check'])
        p=src/'web/src/style.css';p.write_text(p.read_text()+'\n/* stale-input mutation */\n')
        run('fingerprint-stale',['python3','scripts/prepare_daemon.py','--check'],expected=1)
        if 'stale embedded assets' not in (out/'fingerprint-stale.log').read_text():raise RuntimeError('stale refusal missing')
        facts['events'].append('frontend_source_staleness_refused')
        env['CGO_ENABLED']='0'
        run('no-cgo-build',['go','build','-trimpath','-buildvcs=false','-o',str(temp/'no-cgo'),'./cmd/coldkeepd'],cwd=ROOT)
        # ReadConfig refuses SQLite by named capability before any DB/root open.
        config=temp/'sqlite.json';config.write_text(json.dumps({'fixture_only':True,'backend':'sqlite','catalog':'/missing','snapshot':'selected'}));config.chmod(0o600)
        run('no-cgo-sqlite',[str(temp/'no-cgo'),'--config',str(config)],expected=2)
        if 'sqlite_cgo_required' not in (out/'no-cgo-sqlite.log').read_text():raise RuntimeError('no-CGO refusal not named')
        facts['events'].append('sqlite_cgo_required_without_fallback')
        facts['no_cgo_binary_sha256']=hashlib.sha256((temp/'no-cgo').read_bytes()).hexdigest()
    (out/'packaging.json').write_text(json.dumps(facts,indent=2)+'\n')
    print('PROOF_EVENT prepared_assets_cli_independence_stale_absent_nocgo')

if __name__=='__main__':main()
