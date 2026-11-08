"""Exercise the real blocking runner with synthetic Go event/process mutations."""
import ast
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "scripts/run_recovery_export_tests.sh"
NEW_EVENT = "TestRecoveryPostLiveGC"


class RecoveryRunnerTests(unittest.TestCase):
    def run_mutation(self, mutation):
        with tempfile.TemporaryDirectory(prefix="coldkeep-runner-mutation-") as directory:
            root = Path(directory)
            event_file = root / "events.jsonl"
            event_file.write_text("")
            source = RUNNER.read_text().split("<<'PY' || status=$?\n", 1)[1].split("\nPY\n", 1)[0]
            tree = ast.parse(source)
            boundary = next(node.lineno for node in tree.body if isinstance(node, ast.Assign)
                            and any(isinstance(t, ast.Name) and t.id == "passes" for t in node.targets))
            # Evaluate only the required-event declarations, before the checker.
            namespace = {}
            import sys
            from unittest.mock import patch
            with patch.object(sys, "argv", ["checker", str(event_file)]):
                exec("\n".join(source.splitlines()[:boundary - 1]), namespace)
            required = namespace["required"]
            self.assertIn(NEW_EVENT, required["github.com/franchoy/coldkeep/v2/internal/recoveryexport"])
            records = [{"Action": "pass", "Package": package, "Test": test}
                       for package, tests in required.items() for test in tests | {""}]
            if mutation == "missing":
                records = [e for e in records if e["Test"] != NEW_EVENT]
            if mutation == "skip":
                next(e for e in records if e["Test"] == NEW_EVENT)["Action"] = "skip"
            event_file.write_text("".join(json.dumps(e) + "\n" for e in records))
            fake = root / "bin"
            fake.mkdir()
            go = fake / "go"
            go.write_text("""#!/usr/bin/env python3
import os,sys,pathlib
if sys.argv[1:]==['env','GOVERSION']:
 print('go1.26.10')
elif sys.argv[1]=='build':
 pathlib.Path(sys.argv[sys.argv.index('-o')+1]).write_text('synthetic reader identity')
elif sys.argv[1]=='test':
 print(pathlib.Path(os.environ['SYNTHETIC_EVENTS']).read_text(),end='')
 sys.exit(int(os.environ['SYNTHETIC_EXIT']))
else:
 sys.exit(99)
""")
            go.chmod(0o700)
            docker = fake / "docker"
            docker.write_text("#!/bin/sh\nprintf 'synthetic image identity\\n'\n")
            docker.chmod(0o700)
            env = dict(os.environ, PATH=str(fake) + os.pathsep + os.environ["PATH"],
                       COLDKEEP_PHASE1B_TEST="1", COLDKEEP_PHASE1C_TEST="1",
                       DB_HOST="127.0.0.1", DB_PORT="1", DB_USER="synthetic", DB_PASSWORD="synthetic",
                       COLDKEEP_PG_DUMP="/synthetic/pg_dump", COLDKEEP_PG_RESTORE="/synthetic/pg_restore",
                       COLDKEEP_RECOVERY_IMAGE="synthetic", SYNTHETIC_EVENTS=str(event_file),
                       SYNTHETIC_EXIT="17" if mutation == "child-exit" else "0")
            result = subprocess.run(["bash", str(RUNNER), str(root / "out")], cwd=ROOT, env=env,
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
        self.assertIn(NEW_EVENT, result.stdout)

    def test_required_skip_is_blocking(self):
        result = self.run_mutation("skip")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('"Action": "skip"', result.stdout)
