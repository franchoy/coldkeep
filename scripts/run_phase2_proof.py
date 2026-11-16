#!/usr/bin/env python3
"""Blocking ordinary-Linux integrated action 2.02 proof; no required-case skip."""
import argparse
import json
import os
from pathlib import Path
import platform
import subprocess
from check_phase2_events import REQUIRED,validate,validate_postgres
from proof_tcp import validate_campaign,parse_execution,require_confined

ROOT=Path(__file__).resolve().parent.parent

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--output',required=True)
    args=parser.parse_args()
    out=Path(args.output).resolve();out.mkdir(mode=0o700)
    if platform.system()!='Linux' or platform.machine()!='x86_64':raise SystemExit('selected Linux x86-64 proof unavailable')
    os.environ.update(GOTOOLCHAIN='local',GOWORK='off',CGO_ENABLED='1')
    def run(label,cmd):
        with (out/(label+'.log')).open('wb') as f:subprocess.run(cmd,cwd=ROOT,stdout=f,stderr=subprocess.STDOUT,check=True)
    run('prepared',['python3','scripts/prepare_daemon.py'])
    names=sorted({t.split('/')[0] for ts in REQUIRED.values() for t in ts})
    with (out/'go.jsonl').open('wb') as output,(out/'go.stderr').open('wb') as error:
        result=subprocess.run(['go','test','-race','-count=1','-timeout=3m','-json','./internal/proofdaemon','./internal/webui','-run','^('+'|'.join(names)+')$'],cwd=ROOT,stdout=output,stderr=error)
    facts=validate([json.loads(s) for s in (out/'go.jsonl').read_text().splitlines()],result.returncode)
    (out/'required-events.json').write_text(json.dumps(facts,indent=2)+'\n')
    run('browser',['python3','scripts/proof_browser.py','--binary','bin/coldkeepd','--output',str(out/'browser')])
    run('packaging',['python3','scripts/proof_packaging.py','--output',str(out/'packaging')])
    run('docker',['python3','scripts/proof_docker.py','--output',str(out/'docker')])
    docker=json.loads((out/'docker/docker.json').read_text())
    if not docker['cleanup_complete']:
        raise ValueError('Docker cleanup missing')
    tcp=docker['tcp']
    validate_campaign(tcp,docker['exposure_targets'])
    if [x['target'] for x in tcp['host_observations']] != docker['host_exposure_targets'] or not docker['host_exposure_targets']:
        raise ValueError('host socket observations missing')
    for item in tcp['host_observations']:
        require_confined(parse_execution(item['execution'],item['target']))
    validate_postgres([json.loads(line) for line in (out/'docker/postgres-fixture.log').read_text().splitlines()],0)
    print('PROOF_EVENT tcp_controls_and_all_exposure_targets_validated')
    print('PROOF_EVENT integrated_phase2_all_selected_packages_pass')

if __name__=='__main__':main()
