"""Qualify staging refusal, process lifetime, event scope and cleanup failures."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import check_recovery_test_events as events
import check_passive_test_events as passive_events
import run_staged_package_tests as runner


OBSERVER_IDENTITIES = {
    ("github.com/franchoy/coldkeep/v2/internal/recoveryexport", name)
    for name in ("TestBindingSessionObservation",
                 "TestBindingSessionObservation/disappearance",
                 "TestBindingSessionObservation/held-session",
                 "TestBindingSessionObservation/query-error",
                 "TestBindingSessionObservation/canceled-after-zero",
                 "TestBindingSessionObservation/query-deadline")
}
OBSERVER_IDENTITIES.update({
    ("github.com/franchoy/coldkeep/v2/internal/recoveryexport", name)
    for name in ("TestInventorySessionObservation",
                 "TestInventorySessionObservation/disappearance",
                 "TestInventorySessionObservation/held-session",
                 "TestInventorySessionObservation/query-error",
                 "TestInventorySessionObservation/canceled-after-zero",
                 "TestInventorySessionObservation/query-deadline")
})
EXECUTION_BOUND_IDENTITIES = {
    ("github.com/franchoy/coldkeep/v2/internal/recoveryexport", name)
    for name in ("TestExecutionBounds", "TestExecutionBounds/total-object")
}
FIXTURE_IDENTITIES = {
    ("github.com/franchoy/coldkeep/v2/internal/recoveryexport", name)
    for name in events.FIXTURE_HYGIENE
}


class StagingPrerequisiteTests(unittest.TestCase):
    def test_prerequisite_failures_are_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("pg_dump", "pg_restore"):
                (root / name).write_text("fixture")
                (root / name).chmod(0o700)
            good = dict(DB_HOST="127.0.0.1", DB_PORT="1", DB_USER="fixture", DB_PASSWORD="fixture",
                        COLDKEEP_PHASE1B_TEST="1", COLDKEEP_PHASE1C_TEST="1", COLDKEEP_CODEC="plain",
                        COLDKEEP_PG_DUMP=str(root / "pg_dump"), COLDKEEP_PG_RESTORE=str(root / "pg_restore"),
                        COLDKEEP_RECOVERY_IMAGE=runner.IMAGE)
            for mutation in ("none", "missing-flag", "missing-client", "wrong-client", "mismatched-client",
                             "missing-image", "wrong-image", "wrong-digest", "image-unavailable", "wrong-go"):
                with self.subTest(mutation=mutation):
                    env = dict(good)
                    if mutation == "missing-flag":
                        env.pop("COLDKEEP_PHASE1C_TEST")
                    if mutation == "missing-client":
                        env["COLDKEEP_PG_DUMP"] = str(root / "missing")
                    if mutation in ("wrong-image", "missing-image"):
                        env["COLDKEEP_RECOVERY_IMAGE"] = "postgres:latest" if mutation == "wrong-image" else ""

                    def command(args, _env):
                        if args[0] == "go":
                            return 0, "go1.26.9" if mutation == "wrong-go" else "go1.26.11", ""
                        if args[0] == "docker":
                            if mutation == "image-unavailable":
                                raise RuntimeError("image inspect failed")
                            digest = "postgres@sha256:wrong" if mutation == "wrong-digest" else runner.IMAGE
                            return 0, json.dumps([{"Id": "sha256:" + "1" * 64, "RepoDigests": [digest]}]), ""
                        version = "16.15"
                        if mutation == "wrong-client":
                            version = "15.9"
                        if mutation == "mismatched-client" and args[0].endswith("pg_restore"):
                            version = "16.14"
                        return 0, f"{Path(args[0]).name} (PostgreSQL) {version}\n", ""

                    for scope in ("packages", "passive"):
                        with self.subTest(scope=scope), patch.object(runner, "command", side_effect=command), \
                             patch.object(runner.os, "getuid", return_value=1000):
                            if mutation == "none":
                                self.assertEqual(runner.prerequisites(env, scope)["clients"]["dump"], "16.15")
                            else:
                                with self.assertRaises((ValueError, RuntimeError)):
                                    runner.prerequisites(env, scope)

    def test_reader_provenance_refusals(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            phase = root / "phase3a"
            phase.mkdir()
            source = phase / "accepted"
            source.mkdir()
            original = b"accepted immutable source\n"
            (source / "original.go").write_bytes(original)
            blob = hashlib.sha1(b"blob " + str(len(original)).encode() + b"\0" + original).hexdigest()
            binaries = [phase / "accepted-writer", phase / "accepted-reader", phase / "current-writer", root / "coldkeep-recovery"]
            for binary in binaries:
                binary.write_text("source verified executable")
                binary.chmod(0o700)
            good = {"accepted": {"commit": "accepted-commit", "tree": "accepted-tree",
                                 "original_source_hashes": {"original.go": hashlib.sha256(original).hexdigest()},
                                 "writer_sha256": runner.digest(binaries[0]), "reader_sha256": runner.digest(binaries[1])},
                    "current_writer_sha256": runner.digest(binaries[2])}

            def command(args, _env):
                return (0, "accepted-tree\n", "") if args[1] == "rev-parse" else (0, f"100644 blob {blob}\toriginal.go\0", "")

            report = root / "provenance.json"
            for mutation in ("none", "wrong-commit", "wrong-tree", "missing-source", "changed-source", "wrong-binary", "missing-reader"):
                with self.subTest(mutation=mutation):
                    value = json.loads(json.dumps(good))
                    if mutation == "wrong-commit":
                        value["accepted"]["commit"] = "other-commit"
                    if mutation == "wrong-tree":
                        value["accepted"]["tree"] = "other-tree"
                    if mutation == "missing-source":
                        value["accepted"]["original_source_hashes"] = {}
                    if mutation == "wrong-binary":
                        value["accepted"]["reader_sha256"] = "0" * 64
                    (source / "original.go").write_bytes(b"changed" if mutation == "changed-source" else original)
                    if mutation == "missing-reader":
                        binaries[1].unlink()
                    report.write_text(json.dumps(value))
                    with patch.object(runner, "BASES", {"accepted": "accepted-commit"}), patch.object(runner, "command", side_effect=command):
                        if mutation == "none":
                            self.assertEqual(len(runner.verify_readers(root, report, {})), 4)
                        else:
                            with self.assertRaises((ValueError, FileNotFoundError)):
                                runner.verify_readers(root, report, {})


class StagingLifetimeTests(unittest.TestCase):
    def run_case(self, mutation, scope="packages"):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "out"
            owned = root / "owned-readers"
            owned.mkdir()
            unrelated = root / "unrelated"
            unrelated.write_text("preserved")
            seen = []
            checker = passive_events if scope == "passive" else events
            passes = [{"Package": package, "Test": test, "Action": "pass"}
                      for package, test in checker.required_identities()]
            if mutation == "required-skip":
                passes[0]["Action"] = "skip"
            if mutation == "missing-event":
                passes.pop()
            if mutation == "unrelated-skip":
                passes.append({"Package": "unrelated/platform", "Test": "platform-only", "Action": "skip"})
            if mutation in ("binding-failure", "binding-skip"):
                passes.append({"Package": "github.com/franchoy/coldkeep/v2/internal/recoveryexport",
                               "Test": "TestBindingDockerProfile", "Action": "fail" if mutation == "binding-failure" else "skip"})
            if mutation == "missing-package":
                passes = [event for event in passes if event["Test"]]

            def command(args, env, **kwargs):
                if args[:2] == ["go", "test"]:
                    self.assertTrue(owned.exists())
                    self.assertEqual(Path(env["COLDKEEP_RECOVERY_READER"]).parent, owned)
                    self.assertEqual(args, runner.COMMANDS[scope])
                    seen.append("go-child")
                    if mutation == "child-start":
                        raise OSError("cannot start child")
                    if mutation in ("child-timeout", "child-incomplete"):
                        error = subprocess.TimeoutExpired(args, 0.1)
                        error.process_cleanup = {"complete": mutation == "child-timeout", "remaining": []}
                        raise error
                    kwargs["stdout"].write("".join(json.dumps(item) + "\n" for item in passes).encode())
                    return (17 if mutation == "child-exit" else 0), "", ""
                return 0, "source-identity", ""

            def inventory(_env):
                seen.append("inventory")
                if seen.count("inventory") == 2 and mutation == "inventory-error":
                    raise RuntimeError("residual observation unavailable")
                return {"containers": ["residual"] if seen.count("inventory") == 2 and mutation == "residual" else [],
                        "volumes": ["residual-volume"] if seen.count("inventory") == 2 and mutation == "residual-volume" else []}

            remove = shutil.rmtree

            def cleanup(path):
                self.assertIn("go-child", seen)
                seen.append("cleanup")
                if mutation == "cleanup-error":
                    raise OSError("owned cleanup failed")
                remove(path)

            with patch.object(runner, "prerequisites", return_value={}), \
                 patch.object(runner, "resource_inventory", side_effect=inventory), \
                 patch.object(runner, "command", side_effect=command), \
                 patch.object(runner, "verify_readers", return_value={"reader": "verified"}), \
                 patch.object(runner.tempfile, "mkdtemp", return_value=str(owned)), \
                 patch.object(runner.shutil, "rmtree", side_effect=cleanup):
                status = runner.run(scope, output)
            facts = json.loads((output / "execution.json").read_text())
            self.assertEqual(unrelated.read_text(), "preserved")
            self.assertEqual(owned.exists(), mutation in ("cleanup-error", "child-incomplete"))
            if mutation == "child-incomplete":
                self.assertNotIn("cleanup", seen)
                self.assertEqual(facts["cleanup"]["readers_retained_reason"], "child process cleanup incomplete")
            else:
                self.assertGreater(seen.index("cleanup"), seen.index("go-child"))
            self.assertEqual(facts["exit"], status)
            return status, facts

    def test_child_lifetime_and_all_failures_remain_blocking(self):
        for mutation in ("none", "unrelated-skip", "child-exit", "child-start", "child-timeout", "child-incomplete", "required-skip", "missing-event",
                         "cleanup-error", "inventory-error", "residual", "residual-volume"):
            with self.subTest(mutation=mutation):
                status, facts = self.run_case(mutation)
                self.assertEqual(status == 0, mutation in ("none", "unrelated-skip"))
                if mutation == "child-exit":
                    self.assertEqual(status, 17)
                if mutation == "inventory-error":
                    self.assertIn("inventory_error", facts["cleanup"])

    def test_real_subprocess_timeout_joins_before_return(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "late-write"
            pid_file = Path(directory) / "pid"
            code = ("import time,pathlib,os;pathlib.Path(" + repr(str(pid_file)) + ").write_text(str(os.getpid()));"
                    "time.sleep(2);pathlib.Path(" + repr(str(marker)) + ").write_text('late')")
            with self.assertRaises(subprocess.TimeoutExpired):
                runner.command([sys.executable, "-c", code], os.environ.copy(), timeout=0.3)
            self.assertTrue(pid_file.exists())
            with self.assertRaises(ProcessLookupError):
                os.kill(int(pid_file.read_text()), 0)
            self.assertFalse(marker.exists())

    def test_passive_scope_keeps_readers_and_every_failure_blocking(self):
        for mutation in ("none", "unrelated-skip", "child-exit", "child-start", "child-timeout", "child-incomplete",
                         "required-skip", "missing-event", "missing-package", "binding-failure", "binding-skip",
                         "cleanup-error", "inventory-error", "residual", "residual-volume"):
            with self.subTest(mutation=mutation):
                status, facts = self.run_case(mutation, scope="passive")
                self.assertEqual(status == 0, mutation == "none")
                self.assertEqual(facts["scope"], "passive")
                if mutation == "child-exit":
                    self.assertEqual(status, 17)


class RecoveryEventContractTests(unittest.TestCase):
    def test_accepted_structural_contract(self):
        identities = events.required_identities()
        self.assertTrue(OBSERVER_IDENTITIES <= identities)
        self.assertTrue(EXECUTION_BOUND_IDENTITIES <= identities)
        # Preserve the exact accepted identities while permitting the explicitly
        # scoped C1 additions. Counts alone cannot hide an inherited removal or
        # substitution: the frozen predecessor digest still binds every name.
        inherited = {(package, test) for package, test in identities - FIXTURE_IDENTITIES
                     if package != "github.com/franchoy/coldkeep/v2/internal/generation"
                     and not test.startswith(("TestRecoveryUpgrade", "TestRecoveryGeneration"))}
        self.assertEqual(len(inherited), 583)
        pairs = sorted(inherited - OBSERVER_IDENTITIES - EXECUTION_BOUND_IDENTITIES)
        actual = hashlib.sha256(json.dumps(pairs, separators=(",", ":"), ensure_ascii=True).encode()).hexdigest()
        self.assertEqual(len(pairs), 569)
        self.assertEqual(actual, "8eb9e04d2061e1c8659cb89a38e43648c0c241d2349900d0bf749503f9f19062")

    def test_observer_controls_are_required_in_both_routes(self):
        for checker in (events, passive_events):
            for identity in OBSERVER_IDENTITIES | EXECUTION_BOUND_IDENTITIES | FIXTURE_IDENTITIES:
                for action in ("missing", "skip", "fail"):
                    with self.subTest(checker=checker.__name__, identity=identity, action=action):
                        rows = [{"Package": p, "Test": t, "Action": "pass"}
                                for p, t in checker.required_identities()]
                        if action == "missing":
                            rows = [row for row in rows if (row["Package"], row["Test"]) != identity]
                        else:
                            next(row for row in rows if (row["Package"], row["Test"]) == identity)["Action"] = action
                        self.assertEqual(checker.check_events(rows)["status"], "FAIL")

    def test_missing_and_skipped_binding_events_fail_broad_route(self):
        for name in ("TestBindingDockerProfile", "TestBindingOldWritersAndReaders"):
            for action in ("missing", "skip"):
                with self.subTest(name=name, action=action):
                    rows = [{"Package": p, "Test": t, "Action": "pass"} for p, t in events.required_identities()]
                    if action == "missing":
                        rows = [row for row in rows if row["Test"] != name]
                    else:
                        next(row for row in rows if row["Test"] == name)["Action"] = "skip"
                    self.assertEqual(events.check_events(rows, broad=True)["status"], "FAIL")


class PassiveEventContractTests(unittest.TestCase):
    def test_accepted_structural_contract(self):
        identities = passive_events.required_identities()
        self.assertTrue(OBSERVER_IDENTITIES <= identities)
        self.assertTrue(EXECUTION_BOUND_IDENTITIES <= identities)
        self.assertTrue(FIXTURE_IDENTITIES <= identities)
        self.assertEqual(len(identities - FIXTURE_IDENTITIES), 51)
        new_bound_child = {pair for pair in EXECUTION_BOUND_IDENTITIES if "/" in pair[1]}
        pairs = sorted(identities - OBSERVER_IDENTITIES - new_bound_child - FIXTURE_IDENTITIES)
        actual = hashlib.sha256(json.dumps(pairs, separators=(",", ":"), ensure_ascii=True).encode()).hexdigest()
        self.assertEqual(len(pairs), 38)
        self.assertEqual(actual, "0b635526686ec50d5076d9393593a701f8037d9b5aaeb92edfdc8d2512010dbb")

    def test_passive_command_retains_selection_and_serializes_packages(self):
        self.assertEqual(runner.COMMANDS["passive"], [
            "go", "test", "-race", "-count=1", "-p=1", "-skip", "^TestRecovery", "-json", "-timeout=15m",
            "./internal/recoveryexport", "./internal/coordination"])

    def test_every_mandatory_identity_and_package_pass_is_required(self):
        records = [{"Package": p, "Test": t, "Action": "pass"} for p, t in passive_events.required_identities()]
        for index, record in enumerate(records):
            with self.subTest(identity=(record["Package"], record["Test"])):
                result = passive_events.check_events(records[:index] + records[index + 1:])
                self.assertEqual(result["status"], "FAIL")
                self.assertEqual(result["missing"], [record["Package"] + "/" + record["Test"]])
