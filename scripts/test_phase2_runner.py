"""Fresh mutations against the same event checker used by the proof runner."""
import unittest
import check_phase2_events as proof

class Phase2Events(unittest.TestCase):
    def stream(self):
        records=[{'Package':p,'Test':t,'Action':a} for p,tests in proof.REQUIRED.items()
                 for t in tests for a in ('run','pass')]
        records += [{'Action':'output','Output':'PROOF_EVENT '+e} for e in proof.CRITICAL]
        return records

    def test_complete_stream_and_nonzero_exit(self):
        self.assertEqual(proof.validate(self.stream(),0)['required_tests'],sum(map(len,proof.REQUIRED.values())))
        with self.assertRaisesRegex(ValueError,'nonzero'):
            proof.validate(self.stream(),1)

    def test_every_missing_event_refuses(self):
        for event in proof.CRITICAL:
            with self.subTest(event=event), self.assertRaisesRegex(ValueError,'critical'):
                proof.validate([r for r in self.stream() if r.get('Output')!='PROOF_EVENT '+event],0)

    def test_required_skip_and_missing_case_refuse(self):
        for package,tests in proof.REQUIRED.items():
            for test in tests:
                with self.subTest(test=test):
                    records=[r for r in self.stream() if (r.get('Package'),r.get('Test'))!=(package,test)]
                    with self.assertRaisesRegex(ValueError,'missing'):
                        proof.validate(records,0)
                    records.append({'Package':package,'Test':test,'Action':'skip'})
                    with self.assertRaisesRegex(ValueError,'skipped'):
                        proof.validate(records,0)
