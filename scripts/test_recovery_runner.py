"""Exercise the real blocking runner with synthetic Go event/process mutations."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from check_recovery_test_events import required
from check_passive_test_events import required as passive_required
from run_staged_package_tests import IMAGE


ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "scripts/run_recovery_export_tests.sh"
NEW_EVENT = "TestAdoptionOperatorAndDatabaseReservation"


class RecoveryRunnerTests(unittest.TestCase):
    required_events = required
    runner = RUNNER
    required_event = NEW_EVENT
    required_package = "github.com/franchoy/coldkeep/v2/internal/recoveryexport"
    def run_mutation(self, mutation):
        with tempfile.TemporaryDirectory(prefix="coldkeep-runner-mutation-") as directory:
            root = Path(directory)
            event_file = root / "events.jsonl"
            event_file.write_text("")
            self.assertIn(self.required_event, self.required_events[self.required_package])
            records = [{"Action": "pass", "Package": package, "Test": test}
                       for package, tests in self.required_events.items() for test in tests | {""}]
            if mutation == "missing":
                records = [e for e in records if e["Test"] != self.required_event]
            if mutation == "skip":
                next(e for e in records if e["Test"] == self.required_event)["Action"] = "skip"
            event_file.write_text("".join(json.dumps(e) + "\n" for e in records))
            fake = root / "bin"
            fake.mkdir()
            go = fake / "go"
            go.write_text("""#!/usr/bin/env python3
import os,sys,pathlib
if sys.argv[1:]==['env','GOVERSION']:
 print('go1.26.10')
elif sys.argv[1]=='build':
 output=pathlib.Path(sys.argv[sys.argv.index('-o')+1])
 output.write_text('synthetic reader identity')
 output.chmod(0o700)
elif sys.argv[1]=='test':
 print(pathlib.Path(os.environ['SYNTHETIC_EVENTS']).read_text(),end='')
 sys.exit(int(os.environ['SYNTHETIC_EXIT']))
else:
 sys.exit(99)
""")
            go.chmod(0o700)
            docker = fake / "docker"
            image_json = json.dumps([{"Id": "sha256:" + "1" * 64, "RepoDigests": [IMAGE]}])
            docker.write_text("#!/usr/bin/env python3\nimport sys\nif sys.argv[1]=='image': print(" + repr(image_json) + ")\n")
            docker.chmod(0o700)
            for name in ("pg_dump", "pg_restore"):
                client = fake / name
                client.write_text("#!/bin/sh\nprintf '" + name + " (PostgreSQL) 16.15\\n'\n")
                client.chmod(0o700)
            psql = fake / "psql"
            psql.write_text("#!/usr/bin/env python3\nprint('{\"databases\": [], \"roles\": [], \"sessions\": []}')\n")
            psql.chmod(0o700)
            env = dict(os.environ, PATH=str(fake) + os.pathsep + os.environ["PATH"],
                       COLDKEEP_PHASE1B_TEST="1", COLDKEEP_PHASE1C_TEST="1",
                       DB_HOST="127.0.0.1", DB_PORT="1", DB_USER="synthetic", DB_PASSWORD="synthetic",
                       COLDKEEP_PG_DUMP=str(fake / "pg_dump"), COLDKEEP_PG_RESTORE=str(fake / "pg_restore"),
                       COLDKEEP_RECOVERY_IMAGE=IMAGE, SYNTHETIC_EVENTS=str(event_file),
                       SYNTHETIC_EXIT="17" if mutation == "child-exit" else "0")
            result = subprocess.run(["bash", str(self.runner), str(root / "out")], cwd=ROOT, env=env,
                                    capture_output=True, text=True, timeout=30)
            return result

    def test_complete_events_pass(self):
        result = self.run_mutation("none")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_nonzero_go_exit_is_blocking_with_complete_events(self):
        result = self.run_mutation("child-exit")
        self.assertEqual(result.returncode, 17, result.stdout + result.stderr)

    def test_missing_new_required_event_is_blocking(self):
        result = self.run_mutation("missing")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(self.required_event, result.stdout)

    def test_required_skip_is_blocking(self):
        result = self.run_mutation("skip")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('"Action": "skip"', result.stdout)


class RetainedRecoveryRunnerTests(RecoveryRunnerTests):
    required_event = "TestRecoveryPostLiveGC"


class ConstraintFollowupRunnerTests(RecoveryRunnerTests):
    required_event = "TestPhase3AFollowupA1IdentityConstraintSet/foreign-key-relocated"


class MetadataFollowupRunnerTests(RecoveryRunnerTests):
    required_event = "TestPhase3AFollowupA2ConstraintMetadataBound/compressed-small"


class BindingRunnerTests(RecoveryRunnerTests):
    required_event = "TestBindingDockerProfile/recreation-requires-reconnect"


class BindingConfigRefusalRunnerTests(RecoveryRunnerTests):
    required_event = "TestBindingOperatorConfigFidelity/lone-low-surrogate/apply"


class BindingConfigPositiveRunnerTests(RecoveryRunnerTests):
    required_event = "TestBindingOperatorNativePathControls/base64-native-byte"


class OperatorConfigUnicodeRunnerTests(RecoveryRunnerTests):
    required_package = "github.com/franchoy/coldkeep/v2/cmd/coldkeep-recovery"
    required_event = "TestRecoveryConfigUnicodeRefusals/invalid-utf8"


class OperatorConfigLimitRunnerTests(RecoveryRunnerTests):
    required_package = "github.com/franchoy/coldkeep/v2/cmd/coldkeep-recovery"
    required_event = "TestRecoveryConfigByteBoundary/excess-padding"


class PassiveRunnerTests(RecoveryRunnerTests):
    required_events = passive_required
    runner = ROOT / "scripts/run_passive_inventory_tests.sh"
    required_event = "TestCatalogRowSecurityCannotHideClosure/filtered-reader-refused"
