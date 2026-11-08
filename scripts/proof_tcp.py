"""Private TCP proof protocol: invalid execution never proves confinement."""
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import subprocess
import time

CONTROL_NAMES = {'http_200', 'http_403', 'http_404', 'stalled', 'closed',
                 'missing_helper', 'missing_container', 'missing_result', 'malformed_result'}
REACHABLE = {'http_200', 'http_403', 'http_404', 'stalled'}
CONNECT_MS = 1000
OUTER_SECONDS = 10

class InvalidProof(ValueError):
    pass

class Reachable(ValueError):
    pass

def endpoint(address, port):
    ip = ipaddress.ip_address(address)
    if not 0 < port < 65536 or ip.is_unspecified:
        raise InvalidProof('invalid numeric endpoint')
    return f'[{ip}]:{port}' if ip.version == 6 else f'{ip}:{port}'

def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise InvalidProof('duplicate result field')
        result[key] = value
    return result

def parse_execution(execution, target):
    if execution.get('timeout') or execution.get('exit_code') != 0:
        raise InvalidProof('helper execution failed or outer timeout')
    raw = execution.get('stdout', '')
    if not isinstance(raw, str) or not 0 < len(raw.encode()) <= 4096 or execution.get('stderr'):
        raise InvalidProof('missing, oversized or diagnostic result')
    try:
        result = json.loads(raw, object_pairs_hook=unique_object)
    except (ValueError, TypeError) as error:
        raise InvalidProof('malformed result') from error
    fields = {'target', 'stage', 'outcome', 'deadline_ms', 'elapsed_ms', 'errno', 'error'}
    if not isinstance(result, dict) or set(result) != fields:
        raise InvalidProof('unexpected result shape')
    if result['target'] != target or result['stage'] != 'connect' or result['deadline_ms'] != CONNECT_MS:
        raise InvalidProof('wrong target/stage/deadline')
    if type(result['elapsed_ms']) is not int or not 0 <= result['elapsed_ms'] <= 5000:
        raise InvalidProof('invalid elapsed time')
    if type(result['errno']) is not int or not isinstance(result['error'], str):
        raise InvalidProof('invalid error data')
    outcome = result['outcome']
    if outcome == 'connected':
        if result['errno'] != 0 or result['error']:
            raise InvalidProof('inconsistent connection result')
    elif outcome == 'refused':
        if result['errno'] != 111 or not result['error']:
            raise InvalidProof('not an explicit Linux ECONNREFUSED')
    elif outcome == 'timeout':
        if result['errno'] not in (0, 110) or not result['error'] or result['elapsed_ms'] < CONNECT_MS-50:
            raise InvalidProof('not a bounded connect timeout')
    elif outcome == 'no_route':
        if result['errno'] not in (101, 113) or not result['error']:
            raise InvalidProof('not an identified route result')
    else:
        raise InvalidProof('unsupported or unexpected local socket outcome')
    return result

def require_confined(result):
    if result['outcome'] == 'connected':
        raise Reachable('TCP established to '+result['target'])
    if result['outcome'] not in ('refused', 'timeout', 'no_route'):
        raise InvalidProof('invalid confinement observation')

def execute(command, out, name):
    start = time.monotonic()
    try:
        p = subprocess.run(command, capture_output=True, timeout=OUTER_SECONDS)
        record = dict(command=command, exit_code=p.returncode, timeout=False,
                      stdout=p.stdout.decode(errors='replace'), stderr=p.stderr.decode(errors='replace'))
    except subprocess.TimeoutExpired as error:
        record = dict(command=command, exit_code=None, timeout=True,
                      stdout=(error.stdout or b'').decode(errors='replace'),
                      stderr=(error.stderr or b'').decode(errors='replace'))
    record['outer_budget_seconds'] = OUTER_SECONDS
    record['duration_seconds'] = time.monotonic()-start
    (out/(name+'.json')).write_text(json.dumps(record, indent=2)+'\n')
    return record

def validate_campaign(facts, targets):
    controls = facts['controls']
    if set(controls) != CONTROL_NAMES:
        raise InvalidProof('missing or unexpected required P2 control')
    accepts = {name: sum(r.get('event') == 'accepted' and r.get('control') == name
                        for r in facts['listener_records']) for name in REACHABLE}
    for name, item in controls.items():
        if name in REACHABLE:
            result = parse_execution(item['execution'], item['target'])
            if result['outcome'] != 'connected' or accepts[name] < 1:
                raise InvalidProof('reachable control did not connect/accept')
            if name == 'stalled' and (result['elapsed_ms'] >= CONNECT_MS or item['execution']['duration_seconds'] >= 3):
                raise InvalidProof('stalled control waited for a response')
            try:
                require_confined(result)
            except Reachable:
                continue
            raise InvalidProof('reachable control incorrectly confined')
        elif name == 'closed':
            if parse_execution(item['execution'], item['target'])['outcome'] != 'refused':
                raise InvalidProof('closed control lacks explicit refusal')
        else:
            try:
                parse_execution(item['execution'], item['target'])
            except InvalidProof:
                continue
            raise InvalidProof('invalid control accepted')
    diagnostic = facts['mismatched_host']
    if diagnostic['exit_code'] == 0 or diagnostic['timeout'] or '403' not in diagnostic['stderr']:
        raise InvalidProof('missing mismatched Host 403 diagnostic')
    if not any(r.get('event') == 'request' and r.get('control') == 'http_403'
               and 'Host: mismatched.invalid' in r.get('request', '') for r in facts['listener_records']):
        raise InvalidProof('mismatched Host was not independently observed')
    observations = facts['observations']
    if [r['target'] for r in observations] != targets or len(set(targets)) != len(targets) or not targets:
        raise InvalidProof('missing, duplicate or unexpected exposure target')
    for item in observations:
        require_confined(parse_execution(item['execution'], item['target']))
    return {'required_controls': sorted(CONTROL_NAMES), 'targets': targets,
            'connect_budget_ms': CONNECT_MS, 'outer_budget_seconds': OUTER_SECONDS}

def campaign(root, out, work, image, network, label, targets, host_targets):
    """One owned sibling-network namespace for controls and all real probes."""
    binary = work/'tcp-probe'
    env = dict(os.environ, GOTOOLCHAIN='local', GOWORK='off', CGO_ENABLED='0')
    build = ['go', 'build', '-trimpath', '-buildvcs=false', '-o', str(binary), './scripts/proofprobe']
    with (out/'tcp-build.log').open('wb') as log:
        subprocess.run(build, cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
    binary.chmod(0o555)
    container = network+'-tcp'
    facts = {'controls': {}, 'observations': [], 'host_observations': [], 'container': container,
             'network': network, 'image': image, 'build_command': build,
             'source_sha256': hashlib.sha256((root/'scripts/proofprobe/main.go').read_bytes()).hexdigest(),
             'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest()}
    def probe(name, target, command=None):
        return {'target': target, 'execution': execute(command or ['docker','exec',container,'/probe','probe',target],out,name)}
    try:
        startup = execute(['docker','run','-d','--name',container,'--label',label,'--network',network,
                           '--read-only','--cap-drop','ALL','--security-opt','no-new-privileges:true',
                           '--user','65532:65532','--pids-limit','32','--memory','64m',
                           '--mount',f'type=bind,src={binary},dst=/probe,readonly',
                           '--entrypoint','/probe',image,'fixture'],out,'tcp-startup')
        if startup['exit_code'] != 0 or startup['timeout']:
            raise InvalidProof('probe container startup failed')
        deadline = time.monotonic()+OUTER_SECONDS
        while True:
            logs = execute(['docker','logs',container],out,'tcp-readiness')
            records = [json.loads(line) for line in logs['stdout'].splitlines()]
            if logs['exit_code'] != 0 or logs['timeout'] or logs['stderr']:
                raise InvalidProof('probe container readiness invalid')
            if records == [{'closed_target':'127.0.0.1:18774','event':'ready'}]:
                break
            if time.monotonic() >= deadline:
                raise InvalidProof('probe readiness timeout')
            time.sleep(.05)
        facts['namespace'] = execute(['docker','inspect','--format','{{json .NetworkSettings}}',container],out,'tcp-namespace')
        for index,name in enumerate(('http_200','http_403','http_404','stalled','closed')):
            target = endpoint('127.0.0.1',18770+index)
            facts['controls'][name] = probe('tcp-control-'+name,target)
        target = endpoint('127.0.0.1',18770)
        facts['controls']['missing_helper'] = probe('tcp-missing-helper',target,['docker','exec',container,'/missing-helper','probe',target])
        facts['controls']['missing_container'] = probe('tcp-missing-container',target,['docker','exec',container+'-absent','/probe','probe',target])
        for name, raw in [('missing_result',''),('malformed_result','{"truncated":')]:
            facts['controls'][name] = {'target':target,'execution':{'exit_code':0,'timeout':False,'stdout':raw,'stderr':''}}
        facts['mismatched_host'] = execute(['docker','exec',container,'wget','-T','2','-O','-',
                                          '--header','Host: mismatched.invalid','http://127.0.0.1:18771/'],out,'tcp-host-403')
        listener = execute(['docker','logs',container],out,'tcp-listener')
        if listener['exit_code'] != 0 or listener['timeout'] or listener['stderr']:
            raise InvalidProof('listener observation failed')
        facts['listener_records'] = [json.loads(line) for line in listener['stdout'].splitlines()]
        for index,target in enumerate(targets):
            item = probe('tcp-sibling-'+str(index),target)
            facts['observations'].append(item)
            require_confined(parse_execution(item['execution'],target))
        for index,target in enumerate(host_targets):
            item = probe('tcp-host-'+str(index),target,[str(binary),'probe',target])
            facts['host_observations'].append(item)
            require_confined(parse_execution(item['execution'],target))
        facts['validation'] = validate_campaign(facts,targets)
    finally:
        facts['cleanup'] = execute(['docker','rm','-f',container],out,'tcp-cleanup')
        (out/'tcp.json').write_text(json.dumps(facts,indent=2)+'\n')
    if facts['cleanup']['exit_code'] != 0 or facts['cleanup']['timeout']:
        raise InvalidProof('probe container cleanup failed')
    return facts
