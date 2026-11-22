"""Mutate the exact parser/campaign gate used by the Docker and integrated runners."""
import copy
import json
import unittest
import proof_tcp as tcp

class TCPProof(unittest.TestCase):
    target = '127.0.0.1:18770'

    def execution(self, outcome='connected'):
        value = dict(target=self.target,stage='connect',outcome=outcome,deadline_ms=1000,
                     elapsed_ms=0,errno=0,error='')
        if outcome == 'refused': value.update(errno=111,error='connect: connection refused')
        return dict(exit_code=0,timeout=False,stdout=json.dumps(value),stderr='',duration_seconds=.1)

    def campaign(self):
        controls = {name:dict(target=self.target,execution=self.execution()) for name in tcp.REACHABLE}
        controls['closed'] = dict(target=self.target,execution=self.execution('refused'))
        for name in tcp.CONTROL_NAMES-controls.keys():
            controls[name] = dict(target=self.target,execution=dict(exit_code=127,timeout=False,stdout='',stderr='missing'))
        return dict(controls=controls,listener_records=[dict(event='accepted',control=name) for name in tcp.REACHABLE]+[
            dict(event='request',control='http_403',request='Host: mismatched.invalid')],
            mismatched_host=dict(exit_code=1,timeout=False,stderr='403'),
            observations=[dict(target=self.target,execution=self.execution('refused'))])

    def test_established_connection_always_fails_confinement(self):
        result=tcp.parse_execution(self.execution(),self.target)
        with self.assertRaises(tcp.Reachable): tcp.require_confined(result)

    def test_refusal_timeout_and_route_remain_distinct(self):
        for outcome,errno,elapsed in [('refused',111,0),('timeout',0,1000),('timeout',110,1000),('no_route',101,0),('no_route',113,0)]:
            execution=self.execution()
            value=json.loads(execution['stdout']);value.update(outcome=outcome,errno=errno,elapsed_ms=elapsed,error='connect error')
            execution['stdout']=json.dumps(value)
            with self.subTest(outcome=outcome,errno=errno):
                result=tcp.parse_execution(execution,self.target)
                self.assertEqual(result['outcome'],outcome)
                tcp.require_confined(result)

    def test_execution_failures_cannot_be_negative_evidence(self):
        for change in ({'exit_code':1},{'exit_code':125},{'exit_code':127},{'timeout':True},
                       {'stdout':''},{'stdout':'{'},{'stdout':'{}\n{}'},{'stdout':'x'*4097},
                       {'stdout':self.execution()['stdout']+'\n'+self.execution()['stdout']},
                       {'stdout':self.execution()['stdout'].replace('"errno": 0','"errno": 0, "errno": 111')},
                       {'stderr':'unexpected diagnostic'}):
            with self.subTest(change=change),self.assertRaises(tcp.InvalidProof):
                tcp.parse_execution(dict(self.execution('refused'),**change),self.target)

    def test_wrong_target_or_local_socket_error_invalidates_proof(self):
        for change in ({'target':'192.0.2.1:80'},{'stage':'read'},{'deadline_ms':0},
                       {'elapsed_ms':-1},{'errno':True},{'outcome':'invalid'},
                       {'outcome':'refused','errno':24,'error':'too many open files'},
                       {'outcome':'refused','errno':97,'error':'family unavailable'},
                       {'outcome':'timeout','elapsed_ms':0,'error':'read timed out'},
                       {'outcome':'no_route','errno':115,'error':'in progress'}):
            with self.subTest(change=change),self.assertRaises(tcp.InvalidProof):
                execution=self.execution();value=json.loads(execution['stdout']);value.update(change)
                execution['stdout']=json.dumps(value)
                tcp.parse_execution(execution,self.target)

    def test_every_control_is_mandatory_and_must_show_expected_behavior(self):
        facts=self.campaign();tcp.validate_campaign(facts,[self.target])
        for name in tcp.CONTROL_NAMES:
            with self.subTest(name=name):
                missing=copy.deepcopy(facts);del missing['controls'][name]
                with self.assertRaises(tcp.InvalidProof): tcp.validate_campaign(missing,[self.target])
                bad=copy.deepcopy(facts)
                bad['controls'][name]['execution']=self.execution('refused' if name in tcp.REACHABLE else 'connected')
                with self.assertRaises(tcp.InvalidProof): tcp.validate_campaign(bad,[self.target])

    def test_missing_accept_host_diagnostic_or_real_target_fails(self):
        for change in ({'listener_records':[]},{'observations':[]},
                       {'observations':[self.campaign()['observations'][0]]*2},
                       {'mismatched_host':dict(exit_code=0,timeout=False,stderr='403')}):
            with self.subTest(change=change),self.assertRaises(tcp.InvalidProof):
                tcp.validate_campaign(dict(self.campaign(),**change),[self.target])
        facts=self.campaign();facts['observations'][0]['execution']=self.execution()
        with self.assertRaises(tcp.Reachable): tcp.validate_campaign(facts,[self.target])
        facts=self.campaign();facts['observations'][0]['execution']['exit_code']=125
        with self.assertRaises(tcp.InvalidProof): tcp.validate_campaign(facts,[self.target])

if __name__ == '__main__': unittest.main()
