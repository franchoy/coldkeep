#!/usr/bin/env python3
"""Owned Docker comparison; no database host port or host configuration changes."""
import argparse
import json
import os
from pathlib import Path
import secrets
import signal
import shutil
import subprocess
import time
import urllib.request
from proof_process import execute, ProcessCleanupError
from proof_browser import port
from proof_tcp import campaign, endpoint as numeric_endpoint

ROOT = Path(__file__).resolve().parent.parent
PG_IMAGE = 'postgres:16.15-bookworm@sha256:bb3e1a57e5407e0a5280b4211980a5e537f4abd234a87014ac979849a78dd825'

class CommandFailure(RuntimeError):
    def __init__(self, stage, outcome):
        super().__init__(stage + ': ' + outcome)
        self.stage, self.outcome = stage, outcome


def clean_environment(source):
    # pq consumes PGHOSTADDR and service/default settings as well as PGPASSWORD.
    return {key: value for key, value in source.items()
            if not key.startswith(('PG', 'DB_', 'POSTGRES_'))}


def private_json(path, value):
    with open(path, 'x', opener=lambda name, flags: os.open(name, flags, 0o600)) as stream:
        json.dump(value, stream)


def scan_files(root, secret):
    scanned, matches = 0, []
    for path in sorted(root.rglob('*')):
        if path.is_symlink():
            raise RuntimeError('unexpected symlink in owned evidence')
        if path.is_file():
            scanned += 1
            if secret.encode() in path.read_bytes():
                matches.append(str(path.relative_to(root)))
    return {'scanned_files': scanned, 'matching_files': matches, 'match_count': len(matches)}


class Commands:
    def __init__(self, out, env, secret):
        self.out, self.env, self.secret = out, env, secret
        self.records = []

    def call(self, name, command, *, child_env=None, timeout=120, check=True, capture=False):
        env = dict(self.env if child_env is None else child_env)
        if any(self.secret in str(arg) for arg in command):
            raise CommandFailure(name, 'credential in argv refused')
        credential_names = sorted(key for key, value in env.items() if value == self.secret)
        record = {'stage': name, 'timeout_seconds': timeout, 'credential_variables': credential_names}
        self.records.append(record)
        try:
            result = execute(command, cwd=ROOT, env=env, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, timeout=timeout)
        except (OSError, subprocess.TimeoutExpired, KeyboardInterrupt, ProcessCleanupError) as error:
            record['outcome'] = type(error).__name__
            record['process_cleanup'] = getattr(error, 'process_cleanup', {'complete': isinstance(error, OSError), 'child_not_started': isinstance(error, OSError)})
            output = (getattr(error, 'stdout', None) or b'') + (getattr(error, 'stderr', None) or b'')
            if self.secret.encode() not in output:
                (self.out / (name + '.log')).write_bytes(output)
            raise CommandFailure(name, record['outcome']) from None
        record.update(outcome='completed', exit_code=result.returncode)
        record['process_cleanup'] = getattr(result, 'process_cleanup', {'complete': True})
        # Retain no secret-bearing diagnostics, including unexpected tool output.
        stdout, stderr = result.stdout or b'', result.stderr or b''
        if self.secret.encode() in stdout or self.secret.encode() in stderr:
            record['outcome'] = 'credential output refused'
            raise CommandFailure(name, 'credential in child output refused')
        if not capture or result.returncode:
            (self.out / (name + '.log')).write_bytes(stdout + stderr)
        if check and result.returncode:
            raise CommandFailure(name, 'nonzero exit ' + str(result.returncode))
        return stdout.decode().strip() if capture else result.returncode

    def run(self, name, command, **kwargs):
        return self.call(name, command, **kwargs)

    def capture(self, *command, name='observation', **kwargs):
        return self.call(name, list(command), capture=True, **kwargs)


def cleanup(commands, work, out, label, project, compose, env, image_tags, facts, *, ownership_checked=True):
    failures = []

    def attempt(stage, action):
        try:
            return action()
        except BaseException as error:
            failures.append({'stage': stage, 'type': type(error).__name__,
                             'outcome': getattr(error, 'outcome', 'failed')})
            return None

    # A failed Compose cleanup never prevents label/project discovery or removal.
    if ownership_checked and 'PROOF_NETWORK' in env:
        attempt('compose-cleanup', lambda: commands.run('compose-cleanup', [*compose, 'down', '--timeout', '10'], timeout=30))
    selectors = [('owned_container', ['docker', 'ps', '-aq', '--filter', 'label=' + label]),
                 ('owned_project', ['docker', 'ps', '-aq', '--filter', 'label=com.docker.compose.project=' + project]),
                 ('owned_network', ['docker', 'network', 'ls', '-q', '--filter', 'label=' + label])]
    if not ownership_checked:
        selectors, image_tags = [], ()
    for kind, query in selectors:
        found = attempt(kind + '-discovery', lambda query=query, kind=kind:
                        commands.capture(*query, name=kind + '-discovery', timeout=20))
        if found is not None:
            for index, identity in enumerate(found.split()):
                command = ['docker', 'network', 'rm', identity] if kind == 'owned_network' else ['docker', 'rm', '-f', identity]
                stage = kind + '-cleanup-' + str(index)
                attempt(stage, lambda command=command, stage=stage: commands.run(stage, command, timeout=30))
    # Images are addressed by pre-recorded unique tags, never global pruning.
    facts['owned_image_cleanup'] = {}
    for index, tag in enumerate(image_tags):
        query = ['docker', 'image', 'ls', '-q', '--filter', 'reference=' + tag]
        found = attempt('image-discovery-' + str(index), lambda query=query:
                        commands.capture(*query, name='image-discovery', timeout=20))
        if found:
            facts['owned_image_cleanup'][tag] = attempt('image-cleanup-' + str(index), lambda tag=tag:
                commands.run('image-cleanup-' + tag, ['docker', 'image', 'rm', tag], timeout=30))
        elif found is not None:
            facts['owned_image_cleanup'][tag] = 0
    for kind, query in selectors:
        facts[kind + '_residuals'] = attempt(kind + '-residuals', lambda query=query, kind=kind:
            commands.capture(*query, name=kind + '-residuals', timeout=20))
    facts['owned_image_residuals'] = {}
    for index, tag in enumerate(image_tags):
        facts['owned_image_residuals'][tag] = attempt('image-residuals-' + str(index), lambda tag=tag:
            commands.capture('docker', 'image', 'ls', '-q', '--filter', 'reference=' + tag, name='image-residuals', timeout=20))
    facts['local_process_cleanup_complete'] = all(record.get('process_cleanup', {}).get('complete', True)
                                                 for record in commands.records + facts.get('tcp_process_cleanup', []))
    def remove_work():
        if not facts['local_process_cleanup_complete']:
            raise RuntimeError('private directory retained for unresolved child ownership')
        if work.parent != out or work.name != 'working' or work.is_symlink():
            raise RuntimeError('owned private directory identity changed')
        shutil.rmtree(work)
    attempt('private-directory', remove_work)
    facts['private_directory_removed'] = not work.exists()
    facts['cleanup_failures'] = failures
    facts['cleanup_complete'] = (not failures and facts['private_directory_removed'] and facts['local_process_cleanup_complete']
        and all(facts[kind + '_residuals'] == '' for kind, _ in selectors)
        and all(value == '' for value in facts['owned_image_residuals'].values()))


def run_proof(output):
    uid,gid = os.getuid(),os.getgid()
    if uid == 0:
        raise SystemExit('non-root operator required for this proof')
    out = Path(output).resolve()
    out.mkdir(mode=0o700)
    work = out / 'working'
    work.mkdir(mode=0o700)
    for name in ('payload','output','private','runtime'):
        (work/name).mkdir(mode=0o700)
    label = 'coldkeep.phase2=' + secrets.token_hex(6)
    network = 'ck-phase2-' + label.split('=')[1]
    pg = network + '-postgres'
    probe_network = network + '-probe'
    project = network
    daemon_image = network+'-daemon'
    env = clean_environment(os.environ)
    env['PROOF_IMAGE'] = daemon_image
    facts = {'events': [], 'mount_roles': {'source':'read-only immutable fixtures', 'payload':'copied writable payload and legacy control lock', 'catalog':'disposable PostgreSQL private network, bounded tmpfs', 'output':'explicit fresh host directory', 'private':'read-only operator config/key', 'runtime':'private writable bootstrap, removed on cleanup', 'tmp':'64 MiB tmpfs'}, 'limitations':['No external LAN peer or physical failure-domain proof; host and isolated sibling-network observations only.','IPv6 host ::1 refusal qualified; no routed external IPv6 peer available.','Fixed mounts; stop and recreate after uncertain reattachment; hot reattachment unimplemented.']}
    secret = secrets.token_hex(32)
    commands = Commands(out, env, secret)
    run, capture = commands.run, commands.capture
    compose=['docker','compose','-p',project,'-f','deploy/phase2/compose.yaml']
    image_tags = (network + '-cli', daemon_image)
    facts['owned_identifiers'] = {'label': label, 'project': project, 'postgres': pg,
                                 'networks': [network, probe_network], 'images': list(image_tags),
                                 'containers': [network + suffix for suffix in
                                     ('-postgres', '-version', '-diagnostics', '-second', '-history', '-probe-tcp')]}
    facts['limitations'] += ['Password environment delivery is not encryption; relevant processes, privileged observers and Docker container metadata can expose it.',
                            'Cleanup cannot be guaranteed after SIGKILL, host loss or an unreachable Docker daemon.']
    primary = None
    ownership_checked = False
    try:
        version=json.loads(capture('docker','version','--format','{{json .Server}}'))
        facts['engine']=version
        if int(version['Version'].split('.')[0]) < 28:
            raise RuntimeError('localhost publication requires Docker >=28; no host upgrade authorized')
        # Refuse even an unlikely random-name/label collision before creating or
        # cleaning resources. An existing resource never becomes ours by name.
        existing_names = capture('docker', 'ps', '-a', '--format', '{{.Names}}').splitlines()
        existing_networks = capture('docker', 'network', 'ls', '--format', '{{.Name}}').splitlines()
        if (set(existing_names) & set(facts['owned_identifiers']['containers'] + [project + '-daemon-1'])
                or set(existing_networks) & {network, probe_network}):
            raise RuntimeError('owned identifier collision')
        preflight = [capture('docker','ps','-aq','--filter','label='+label),
                     capture('docker','ps','-aq','--filter','label=com.docker.compose.project='+project),
                     capture('docker','network','ls','-q','--filter','label='+label)]
        preflight += [capture('docker','image','ls','-q','--filter','reference='+tag) for tag in image_tags]
        if any(preflight):
            raise RuntimeError('owned selector collision')
        ownership_checked = True
        facts['ownership_preflight_complete'] = True
        run('docker-info',['docker','info'])
        run('prepared-check',['python3','scripts/prepare_daemon.py','--check'])
        run('cli-image-build',['docker','build','-t',network+'-cli','-f','Dockerfile','.'], timeout=900)
        run('daemon-image-build',['docker','build','-t',daemon_image,'-f','deploy/phase2/Dockerfile','.'], timeout=900)
        facts['cli_image']=capture('docker','image','inspect',network+'-cli','--format','{{.Id}}')
        facts['daemon_image']=capture('docker','image','inspect',daemon_image,'--format','{{.Id}}')
        run('cli-version',['docker','run','--rm','--name',network+'-version','--label',label,network+'-cli','version'])
        run('daemon-diagnostics',['docker','run','--rm','--name',network+'-diagnostics','--label',label,daemon_image,'--diagnostics'])
        capture('docker','network','create','--label',label,network)
        capture('docker','run','-d','--name',pg,'--label',label,'--network',network,'--network-alias','catalog','--memory','1g','--pids-limit','128','--tmpfs','/var/lib/postgresql/data:rw,size=1073741824',
                '--env','POSTGRES_USER=ck_phase2_admin','--env','POSTGRES_DB=ck_phase2_proof','--env','POSTGRES_PASSWORD',PG_IMAGE,
                name='postgres-create', child_env=dict(env, POSTGRES_PASSWORD=secret))
        for _ in range(100):
            if run('postgres-readiness',['docker','exec',pg,'pg_isready','-U','ck_phase2_admin','-d','ck_phase2_proof'], check=False, timeout=5)==0:
                break
            time.sleep(.2)
        else:
            raise RuntimeError('database startup failed')
        address=json.loads(capture('docker','inspect',pg,'--format','{{json .NetworkSettings.Networks}}'))[network]['IPAddress']
        endpoint='127.0.0.1:'+str(port())
        env.update(PROOF_UID=str(uid),PROOF_GID=str(gid),PROOF_PORT=endpoint.split(':')[1],PROOF_SOURCE=str(ROOT/'internal/storage/testdata/compress_v1_18_supplemental'),PROOF_PAYLOAD=str(work/'payload'),PROOF_OUTPUT=str(work/'output'),PROOF_PRIVATE=str(work/'private'),PROOF_RUNTIME=str(work/'runtime'),PROOF_NETWORK=network)
        host={'fixture_only':True,'backend':'postgres','catalog':f'postgres://ck_phase2_admin@{address}:5432/ck_phase2_proof?sslmode=disable','payload_root':str(work/'payload'),'snapshot':'proof-history','key_file':str(work/'private/key'),'restore_root':str(work/'output'),'runtime_dir':str(work/'runtime'),'listen':endpoint,'public_url':'http://'+endpoint,'container':False}
        setup=work/'private/setup.json';private_json(setup, host)
        fixture_env = dict(env, COLDKEEP_PROOF_PG_CONFIG=str(setup), PGPASSWORD=secret)
        run('credential-controls', ['go', 'run', './scripts/testdata/proof_credentials'], child_env=fixture_env, timeout=180)
        credential_event = 'PROOF_EVENT postgres_credential_channels_tcp_success_and_refusals'
        if (out/'credential-controls.log').read_text().splitlines().count(credential_event) != 1:
            raise RuntimeError('required credential controls event missing or repeated')
        facts['credential_controls'] = credential_event
        run('postgres-fixture',['go','test','-race','-count=1','-timeout=3m','-json','./internal/proofdaemon','-run','^TestProofPostgresFixture$'], child_env=fixture_env, timeout=240)
        if any('PROOF_EVENT '+event not in (out/'postgres-fixture.log').read_text() for event in ('postgres_old_writer_fixture_prepared','scalar_byte_fields_postgres','postgres_encoding_admission')):
            raise RuntimeError('PostgreSQL fixture event missing')
        from check_phase2_events import validate_postgres
        facts['postgres_requirements']=validate_postgres([json.loads(line) for line in (out/'postgres-fixture.log').read_text().splitlines()],0)
        container={**host,'catalog':f'postgres://ck_phase2_admin@catalog:5432/ck_phase2_proof?sslmode=disable','payload_root':'/data/payload','restore_root':'/data/output','runtime_dir':'/runtime','key_file':'/private/key','listen':'0.0.0.0:18766','container':True}
        private_json(work/'private/container.json', container)
        # Long bind syntax must refuse a missing source, without creating it.
        real_source=env['PROOF_SOURCE'];missing=work/'absent-source';env['PROOF_SOURCE']=str(missing)
        if run('missing-mount',[*compose,'up','-d'],check=False, child_env=dict(env, PGPASSWORD=secret))==0 or missing.exists():
            raise RuntimeError('missing bind was silently created or accepted')
        run('missing-mount-cleanup',[*compose,'down'])
        env['PROOF_SOURCE']=real_source
        run('compose-up',[*compose,'up','-d'], child_env=dict(env, PGPASSWORD=secret))
        daemon=capture(*compose,'ps','-q','daemon')
        for _ in range(150):
            try:
                with urllib.request.urlopen(host['public_url'],timeout=.3) as response:
                    if response.status==200:break
            except OSError:time.sleep(.1)
        else:
            run('daemon-failure-log',['docker','logs',daemon],check=False)
            raise RuntimeError('container daemon did not start')
        inspect={key: json.loads(capture('docker','inspect',daemon,'--format','{{json .'+key+'}}')) for key in ('HostConfig','Mounts','NetworkSettings')}
        inspect['Config']={'User': capture('docker','inspect',daemon,'--format','{{.Config.User}}')}
        facts['daemon_configuration']={k:inspect['HostConfig'][k] for k in ('PortBindings','ReadonlyRootfs','Tmpfs','Memory','PidsLimit','CapDrop','SecurityOpt')}
        facts['daemon_user']=inspect['Config']['User'];facts['mounts']=inspect['Mounts']
        if facts['daemon_user'] in ('','0','0:0') or any(m['Source']=='/var/run/docker.sock' for m in inspect['Mounts']):raise RuntimeError('unsafe container identity/mount')
        facts['database_port_bindings']=json.loads(capture('docker','inspect',pg,'--format','{{json .HostConfig.PortBindings}}'))
        if facts['database_port_bindings']:raise RuntimeError('database host port exists')
        run('container-processes',['docker','top',daemon,'-eo','pid,user,comm,args'])
        run('browser',['node','web/scripts/browser-proof.mjs',str(setup),str(out/'browser.json')])
        if capture('docker','inspect',daemon,'--format','{{.State.Running}}')!='true':raise RuntimeError('browser closure lost daemon')
        # A second CGO-disabled daemon in this same mount/network must contend
        # on the original lock. Supply a separate empty output for validation.
        second=work/'second-output';second.mkdir(mode=0o700)
        second_config={**container,'restore_root':'/data/second'}
        private_json(work/'private/second.json', second_config)
        second_command=['docker','run','--rm','--name',network+'-second','--label',label,'--env','PGPASSWORD','--network',network,'--user',f'{uid}:{gid}','--mount',f'type=bind,src={work / "payload"},dst=/data/payload','--mount',f'type=bind,src={second},dst=/data/second','--mount',f'type=bind,src={work / "private"},dst=/private,readonly','--mount',f'type=bind,src={work / "runtime"},dst=/runtime',daemon_image,'--config','/private/second.json']
        if run('container-contention',second_command,check=False,child_env=dict(env,PGPASSWORD=secret))==0 or 'repository is busy' not in (out/'container-contention.log').read_text():raise RuntimeError('container lock contention absent')
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
        facts['tcp']=campaign(ROOT,out,work,facts['daemon_image'],probe_network,label,targets,host_targets,
                              process_records=facts.setdefault('tcp_process_cleanup', []))
        facts['network']=json.loads(capture('docker','network','inspect',network))[0]
        facts['events']=['postgres_embedded_browser_restore','nonroot_fixed_mounts_bounded_tmp','missing_mount_refused','loopback_only_publication','browser_closed_container_kept_lease','container_second_owner_refused','cli_image_runs']
        # Retained CLI actually reads this PostgreSQL catalog through its own path.
        run('compose-stop',[*compose,'stop','-t','60'])
        run('daemon-clean-shutdown',['docker','logs',daemon])
        if capture('docker','inspect',daemon,'--format','{{.State.ExitCode}}')!='0':raise RuntimeError('container shutdown failure')
        run('cli-postgres-history',['docker','run','--rm','--name',network+'-history','--label',label,'--network',network,'--user',f'{uid}:{gid}','--env','DB_PASSWORD','--env','DB_HOST=catalog','--env','DB_PORT=5432','--env','DB_USER=ck_phase2_admin','--env','DB_NAME=ck_phase2_proof','--env','DB_SSLMODE=disable','--env','COLDKEEP_STORAGE_DIR=/data/payload','--mount',f'type=bind,src={work / "payload"},dst=/data/payload',network+'-cli','snapshot','list'], child_env=dict(env,DB_PASSWORD=secret))
        facts['events'].append('retained_cli_postgres_history')
    except BaseException as error:
        primary = {'type': type(error).__name__, 'stage': getattr(error, 'stage', 'proof'),
                   'outcome': getattr(error, 'outcome', 'failed')}
    finally:
        # Scan while private configs still exist, including all five former sinks.
        try:
            facts['working_secret_scan'] = scan_files(out, secret)
        except BaseException as error:
            facts['working_secret_scan'] = {'error': type(error).__name__}
        cleanup(commands, work, out, label, project, compose, env, image_tags, facts,
                ownership_checked=ownership_checked)
        facts['primary_failure'] = primary
        facts['commands'] = commands.records
        private_json(out/'docker.json', facts)
        facts['retained_secret_scan'] = scan_files(out, secret)
        # The final record contains only counts/paths and the already scanned facts.
        (out/'docker.json').write_text(json.dumps(facts,indent=2)+'\n')
    if primary or not facts['cleanup_complete']:
        raise RuntimeError('Docker proof failed or owned cleanup incomplete; see docker.json') from None
    if any(facts[key].get('match_count') != 0 for key in ('working_secret_scan', 'retained_secret_scan')):
        raise RuntimeError('credential scan failed; see nonsecret scan facts')
    print('PROOF_EVENT postgres_docker_cli_daemon_mount_exposure_cleanup')
    return facts


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt
    previous = signal.signal(signal.SIGTERM, interrupted)
    try:
        run_proof(args.output)
    finally:
        signal.signal(signal.SIGTERM, previous)


if __name__=='__main__':main()
