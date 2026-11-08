#!/usr/bin/env python3
"""Owned Docker comparison; no database host port or host configuration changes."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import time
import urllib.request
from proof_browser import port
from proof_tcp import campaign, endpoint as numeric_endpoint

ROOT = Path(__file__).resolve().parent.parent
PG_IMAGE = 'postgres:16.15-bookworm@sha256:bb3e1a57e5407e0a5280b4211980a5e537f4abd234a87014ac979849a78dd825'

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    out = Path(args.output).resolve()
    out.mkdir(mode=0o700)
    work = out / 'working'
    work.mkdir(mode=0o700)
    uid,gid = os.getuid(),os.getgid()
    if uid == 0:
        raise SystemExit('non-root operator required for this proof')
    for name in ('payload','output','private','runtime'):
        (work/name).mkdir(mode=0o700)
    label = 'coldkeep.phase2=' + secrets.token_hex(6)
    network = 'ck-phase2-' + label.split('=')[1]
    pg = None
    probe_network = None
    project = network
    daemon_image = network+'-daemon'
    env = os.environ.copy()
    env['PROOF_IMAGE'] = daemon_image
    facts = {'events': [], 'mount_roles': {'source':'read-only immutable fixtures', 'payload':'copied writable payload and legacy control lock', 'catalog':'disposable PostgreSQL private network, bounded tmpfs', 'output':'explicit fresh host directory', 'private':'read-only operator config/key', 'runtime':'private writable bootstrap, removed on cleanup', 'tmp':'64 MiB tmpfs'}, 'limitations':['No external LAN peer or physical failure-domain proof; host and isolated sibling-network observations only.','IPv6 host ::1 refusal qualified; no routed external IPv6 peer available.','Fixed mounts; stop and recreate after uncertain reattachment; hot reattachment unimplemented.']}
    def run(name,cmd,check=True,**kwargs):
        with (out/(name+'.log')).open('wb') as log:
            result=subprocess.run(cmd,cwd=ROOT,stdout=log,stderr=subprocess.STDOUT,env=env,**kwargs)
        if check and result.returncode:
            raise RuntimeError(name+' failed; see retained raw log')
        return result.returncode
    def capture(*cmd):
        return subprocess.check_output(cmd,cwd=ROOT,text=True,env=env).strip()
    compose=['docker','compose','-p',project,'-f','deploy/phase2/compose.yaml']
    try:
        version=json.loads(capture('docker','version','--format','{{json .Server}}'))
        facts['engine']=version
        if int(version['Version'].split('.')[0]) < 28:
            raise RuntimeError('localhost publication requires Docker >=28; no host upgrade authorized')
        run('docker-info',['docker','info'])
        run('prepared-check',['python3','scripts/prepare_daemon.py','--check'])
        run('cli-image-build',['docker','build','-t',network+'-cli','-f','Dockerfile','.'])
        run('daemon-image-build',['docker','build','-t',daemon_image,'-f','deploy/phase2/Dockerfile','.'])
        facts['cli_image']=capture('docker','image','inspect',network+'-cli','--format','{{.Id}}')
        facts['daemon_image']=capture('docker','image','inspect',daemon_image,'--format','{{.Id}}')
        run('cli-version',['docker','run','--rm',network+'-cli','version'])
        run('daemon-diagnostics',['docker','run','--rm',daemon_image,'--diagnostics'])
        capture('docker','network','create','--label',label,network)
        secret=secrets.token_hex(32)
        envfile=work/'private/postgres.env'
        envfile.write_text('POSTGRES_USER=ck_phase2_admin\nPOSTGRES_PASSWORD='+secret+'\nPOSTGRES_DB=ck_phase2_proof\n');envfile.chmod(0o600)
        pg=capture('docker','run','-d','--label',label,'--network',network,'--network-alias','catalog','--memory','1g','--pids-limit','128','--tmpfs','/var/lib/postgresql/data:rw,size=1073741824','--env-file',str(envfile),PG_IMAGE)
        for _ in range(100):
            if subprocess.run(['docker','exec',pg,'pg_isready','-U','ck_phase2_admin','-d','ck_phase2_proof'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode==0:
                break
            time.sleep(.2)
        else:
            raise RuntimeError('database startup failed')
        address=json.loads(capture('docker','inspect',pg,'--format','{{json .NetworkSettings.Networks}}'))[network]['IPAddress']
        endpoint='127.0.0.1:'+str(port())
        env.update(PROOF_UID=str(uid),PROOF_GID=str(gid),PROOF_PORT=endpoint.split(':')[1],PROOF_SOURCE=str(ROOT/'internal/storage/testdata/compress_v1_18_supplemental'),PROOF_PAYLOAD=str(work/'payload'),PROOF_OUTPUT=str(work/'output'),PROOF_PRIVATE=str(work/'private'),PROOF_RUNTIME=str(work/'runtime'),PROOF_NETWORK=network)
        host={'fixture_only':True,'backend':'postgres','catalog':f'postgres://ck_phase2_admin:{secret}@{address}:5432/ck_phase2_proof?sslmode=disable','payload_root':str(work/'payload'),'snapshot':'proof-history','key_file':str(work/'private/key'),'restore_root':str(work/'output'),'runtime_dir':str(work/'runtime'),'listen':endpoint,'public_url':'http://'+endpoint,'container':False}
        setup=work/'private/setup.json';setup.write_text(json.dumps(host));setup.chmod(0o600)
        env['COLDKEEP_PROOF_PG_CONFIG']=str(setup)
        run('postgres-fixture',['go','test','-race','-count=1','-json','./internal/proofdaemon','-run','^TestProofPostgresFixture$'])
        if any('PROOF_EVENT '+event not in (out/'postgres-fixture.log').read_text() for event in ('postgres_old_writer_fixture_prepared','scalar_byte_fields_postgres','postgres_encoding_admission')):
            raise RuntimeError('PostgreSQL fixture event missing')
        from check_phase2_events import validate_postgres
        facts['postgres_requirements']=validate_postgres([json.loads(line) for line in (out/'postgres-fixture.log').read_text().splitlines()],0)
        container={**host,'catalog':f'postgres://ck_phase2_admin:{secret}@catalog:5432/ck_phase2_proof?sslmode=disable','payload_root':'/data/payload','restore_root':'/data/output','runtime_dir':'/runtime','key_file':'/private/key','listen':'0.0.0.0:18766','container':True}
        (work/'private/container.json').write_text(json.dumps(container));(work/'private/container.json').chmod(0o600)
        # Long bind syntax must refuse a missing source, without creating it.
        real_source=env['PROOF_SOURCE'];missing=work/'absent-source';env['PROOF_SOURCE']=str(missing)
        if run('missing-mount',[*compose,'up','-d'],check=False)==0 or missing.exists():
            raise RuntimeError('missing bind was silently created or accepted')
        run('missing-mount-cleanup',[*compose,'down'])
        env['PROOF_SOURCE']=real_source
        run('compose-up',[*compose,'up','-d'])
        daemon=capture(*compose,'ps','-q','daemon')
        for _ in range(150):
            try:
                with urllib.request.urlopen(host['public_url'],timeout=.3) as response:
                    if response.status==200:break
            except OSError:time.sleep(.1)
        else:
            run('daemon-failure-log',['docker','logs',daemon],check=False)
            raise RuntimeError('container daemon did not start')
        inspect=json.loads(capture('docker','inspect',daemon))[0]
        facts['daemon_configuration']={k:inspect['HostConfig'][k] for k in ('PortBindings','ReadonlyRootfs','Tmpfs','Memory','PidsLimit','CapDrop','SecurityOpt')}
        facts['daemon_user']=inspect['Config']['User'];facts['mounts']=inspect['Mounts']
        if facts['daemon_user'] in ('','0','0:0') or any(m['Source']=='/var/run/docker.sock' for m in inspect['Mounts']):raise RuntimeError('unsafe container identity/mount')
        dbinspect=json.loads(capture('docker','inspect',pg))[0]
        facts['database_port_bindings']=dbinspect['HostConfig']['PortBindings']
        if facts['database_port_bindings']:raise RuntimeError('database host port exists')
        run('container-processes',['docker','top',daemon,'-eo','pid,user,comm,args'])
        run('browser',['node','web/scripts/browser-proof.mjs',str(setup),str(out/'browser.json')])
        if capture('docker','inspect',daemon,'--format','{{.State.Running}}')!='true':raise RuntimeError('browser closure lost daemon')
        # A second CGO-disabled daemon in this same mount/network must contend
        # on the original lock. Supply a separate empty output for validation.
        second=work/'second-output';second.mkdir(mode=0o700)
        second_config={**container,'restore_root':'/data/second'}
        (work/'private/second.json').write_text(json.dumps(second_config));(work/'private/second.json').chmod(0o600)
        second_command=['docker','run','--rm','--network',network,'--user',f'{uid}:{gid}','--mount',f'type=bind,src={work / "payload"},dst=/data/payload','--mount',f'type=bind,src={second},dst=/data/second','--mount',f'type=bind,src={work / "private"},dst=/private,readonly','--mount',f'type=bind,src={work / "runtime"},dst=/runtime',daemon_image,'--config','/private/second.json']
        if run('container-contention',second_command,check=False)==0 or 'repository is busy' not in (out/'container-contention.log').read_text():raise RuntimeError('container lock contention absent')
        # Host IPv4 non-loopback and IPv6 loopback are not published endpoints.
        ips=[]
        for interface in json.loads(capture('ip','-j','address','show')):
            for a in interface.get('addr_info',[]):
                if a['family']=='inet' and a['scope']=='global':ips.append(a['local'])
        probe_network=network+'-probe'
        capture('docker','network','create','--label',label,probe_network)
        app_address=inspect['NetworkSettings']['Networks'][network]['IPAddress']
        targets=[numeric_endpoint(x,int(endpoint.split(':')[1])) for x in ips]+[numeric_endpoint(app_address,18766)]
        host_targets=[numeric_endpoint(x,int(endpoint.split(':')[1])) for x in ips]+[numeric_endpoint('::1',int(endpoint.split(':')[1]))]
        facts['exposure_targets']=targets
        facts['host_exposure_targets']=host_targets
        facts['tcp']=campaign(ROOT,out,work,facts['daemon_image'],probe_network,label,targets,host_targets)
        facts['network']=json.loads(capture('docker','network','inspect',network))[0]
        facts['events']=['postgres_embedded_browser_restore','nonroot_fixed_mounts_bounded_tmp','missing_mount_refused','loopback_only_publication','browser_closed_container_kept_lease','container_second_owner_refused','cli_image_runs']
        # Retained CLI actually reads this PostgreSQL catalog through its own path.
        clienv=work/'private/cli.env'
        clienv.write_text('DB_HOST=catalog\nDB_PORT=5432\nDB_USER=ck_phase2_admin\nDB_PASSWORD='+secret+'\nDB_NAME=ck_phase2_proof\nDB_SSLMODE=disable\nCOLDKEEP_STORAGE_DIR=/data/payload\n');clienv.chmod(0o600)
        run('compose-stop',[*compose,'stop','-t','60'])
        run('daemon-clean-shutdown',['docker','logs',daemon])
        if capture('docker','inspect',daemon,'--format','{{.State.ExitCode}}')!='0':raise RuntimeError('container shutdown failure')
        run('cli-postgres-history',['docker','run','--rm','--network',network,'--user',f'{uid}:{gid}','--env-file',str(clienv),'--mount',f'type=bind,src={work / "payload"},dst=/data/payload',network+'-cli','snapshot','list'])
        facts['events'].append('retained_cli_postgres_history')
    finally:
        if 'PROOF_NETWORK' in env:
            run('compose-cleanup',[*compose,'down'],check=False)
        if pg:run('postgres-cleanup',['docker','rm','-f',pg],check=False)
        if probe_network:run('probe-network-cleanup',['docker','network','rm',probe_network],check=False)
        run('network-cleanup',['docker','network','rm',network],check=False)
        facts['owned_container_residuals']=capture('docker','ps','-aq','--filter','label='+label)
        facts['owned_network_residuals']=capture('docker','network','ls','-q','--filter','label='+label)
        facts['owned_project_residuals']=capture('docker','ps','-aq','--filter','label=com.docker.compose.project='+project)
        shutil.rmtree(work)
        facts['owned_image_cleanup']={tag:run('image-cleanup-'+tag,['docker','image','rm',tag],check=False) for tag in (network+'-cli',daemon_image)}
        facts['cleanup_complete']=(not facts['owned_container_residuals'] and not facts['owned_network_residuals']
                                   and not facts['owned_project_residuals'] and not any(facts['owned_image_cleanup'].values()))
        (out/'docker.json').write_text(json.dumps(facts,indent=2)+'\n')
    if not facts['cleanup_complete']:raise RuntimeError('owned Docker resources remain')
    print('PROOF_EVENT postgres_docker_cli_daemon_mount_exposure_cleanup')

if __name__=='__main__':main()
