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

C2_IDENTITIES = {(package, name) for package, names in events.C2_REQUIRED.items() for name in names}


class StagingPrerequisiteTests(unittest.TestCase):
    def test_accepted_c1_is_an_additional_required_untouched_role(self):
        import build_phase3a_readers as builder
        expected = {"c1": "06bde38380dd3a42be5c180beb99bf1aa509b8ac",
                    "pre3c": "3aa9cae7b15037e6f51c22311e2b2ee0c0178b86",
                    "pre3b": "cf70de06d2ab2da6f64fa7b13fa9610af07e32f2",
                    "d19": "d3581602aee2599547564322fe083baae1d52063",
                    "accepted": "1229563116f71067c74801516bcd64c92e7b6ed2"}
        self.assertEqual(runner.BASES, expected)
        self.assertEqual(builder.BASES, expected)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "phase3a/c1"
            source.mkdir(parents=True)
            original = b"untouched C1 source\n"
            blob = hashlib.sha1(b"blob " + str(len(original)).encode() + b"\0" + original).hexdigest()
            source_file = source / "original.go"
            binary = root / "phase3a/c1-reader"
            current = root / "phase3a/current-writer"
            operator = root / "coldkeep-recovery"
            report = root / "provenance.json"
            for mutation in ("none", "missing-role", "wrong-commit", "wrong-tree", "missing-source",
                             "changed-source", "changed-mode", "changed-binary", "missing-binary",
                             "wrong-compiler-record", "wrong-compiler-binary",
                             "missing-digest", "null-digest", "empty-digest", "short-digest",
                             "uppercase-digest", "nonhex-digest", "nonstring-digest"):
                with self.subTest(mutation=mutation):
                    source_file.write_bytes(original if mutation != "changed-source" else b"substitution")
                    source_file.chmod(0o755 if mutation == "changed-mode" else 0o644)
                    for path in (binary, current, operator):
                        path.write_bytes(b"binary")
                        path.chmod(0o700)
                    value = {"c1": {"commit": expected["c1"], "tree": "c1-tree", "compiler": "go1.26.11",
                                    "original_source_hashes": {"original.go": hashlib.sha256(original).hexdigest()},
                                    "reader_sha256": runner.digest(binary)},
                             "current_writer_sha256": runner.digest(current)}
                    if mutation == "missing-role": value.pop("c1")
                    if mutation == "wrong-commit": value["c1"]["commit"] = expected["pre3c"]
                    if mutation == "wrong-tree": value["c1"]["tree"] = "other-tree"
                    if mutation == "missing-source": value["c1"]["original_source_hashes"] = {}
                    if mutation == "changed-binary": binary.write_bytes(b"changed")
                    if mutation == "missing-binary": binary.unlink()
                    if mutation == "wrong-compiler-record": value["c1"]["compiler"] = "go1.26.10"
                    if mutation.endswith("-digest"):
                        # A same-compiler substitute must never become the role's
                        # new expected identity when its recorded digest is absent.
                        original_digest = value["c1"]["reader_sha256"]
                        malformed = {"null-digest": None, "empty-digest": "",
                                     "short-digest": original_digest[:-1],
                                     "uppercase-digest": original_digest.upper(),
                                     "nonhex-digest": "g" * 64, "nonstring-digest": 1}
                        if mutation == "missing-digest":
                            del value["c1"]["reader_sha256"]
                        else:
                            value["c1"]["reader_sha256"] = malformed[mutation]
                        binary.write_bytes(b"same-compiler substituted reader")
                    report.write_text(json.dumps(value))
                    def command(args, _env):
                        if args[:2] == ["go", "version"]:
                            return 0, str(binary) + (": go1.26.10\n" if mutation == "wrong-compiler-binary" else ": go1.26.11\n"), ""
                        return (0, "c1-tree\n", "") if args[1] == "rev-parse" else (0, f"100644 blob {blob}\toriginal.go\0", "")
                    with patch.object(runner, "BASES", {"c1": expected["c1"]}), patch.object(runner, "command", side_effect=command):
                        if mutation == "none":
                            self.assertEqual(set(runner.verify_readers(root, report, {})),
                                             {"c1-reader", "current-writer", "coldkeep-recovery"})
                        else:
                            with self.assertRaises((ValueError, OSError)):
                                runner.verify_readers(root, report, {})

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
                             "missing-image", "wrong-image", "wrong-digest", "image-unavailable", "wrong-go", "wrong-node"):
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
                        if args[0] == "node":
                            return 0, "v1.0.0" if mutation == "wrong-node" else "v24.22.2", ""
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
                            if mutation == "none" or (mutation == "wrong-node" and scope == "passive"):
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
            for mutation in ("none", "wrong-commit", "wrong-tree", "missing-source", "changed-source", "wrong-binary",
                             "missing-writer-digest", "null-writer-digest", "missing-reader-digest",
                             "null-reader-digest", "missing-current-writer-digest", "null-current-writer-digest",
                             "missing-reader"):
                with self.subTest(mutation=mutation):
                    value = json.loads(json.dumps(good))
                    for binary in binaries:
                        binary.write_text("source verified executable")
                        binary.chmod(0o700)
                    if mutation.endswith("-digest"):
                        role = mutation.split("-", 1)[1].removesuffix("-digest")
                        owner, field, binary = {
                            "writer": (value["accepted"], "writer_sha256", binaries[0]),
                            "reader": (value["accepted"], "reader_sha256", binaries[1]),
                            "current-writer": (value, "current_writer_sha256", binaries[2]),
                        }[role]
                        if mutation.startswith("missing-"):
                            del owner[field]
                        else:
                            owner[field] = None
                        binary.write_text("substituted executable")
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


    def test_current_operator_hash_has_no_separate_provenance_record(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            writer = root / "phase3a/current-writer"
            writer.parent.mkdir()
            operator = root / "coldkeep-recovery"
            for path in (writer, operator):
                path.write_bytes(b"current executable")
                path.chmod(0o700)
            report = root / "provenance.json"
            report.write_text(json.dumps({"current_writer_sha256": runner.digest(writer)}))
            with patch.object(runner, "BASES", {}):
                before = runner.verify_readers(root, report, {})
                self.assertEqual(before["coldkeep-recovery"], runner.digest(operator))
                operator.write_bytes(b"new current operator bytes")
                after = runner.verify_readers(root, report, {})
                self.assertEqual(after["coldkeep-recovery"], runner.digest(operator))
                self.assertNotEqual(before["coldkeep-recovery"], after["coldkeep-recovery"])
                self.assertEqual(before["current-writer"], after["current-writer"])


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
                 patch.object(runner, "verify_readers", side_effect=[{"c1-reader": "verified"}, {"c1-reader": "changed" if mutation == "changed-c1-during-child" else "verified"}]), \
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
                         "cleanup-error", "inventory-error", "residual", "residual-volume", "changed-c1-during-child"):
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
        inherited = {(package, test) for package, test in identities - FIXTURE_IDENTITIES - C2_IDENTITIES
                     if package != "github.com/franchoy/coldkeep/v2/internal/generation"
                     and not test.startswith(("TestRecoveryUpgrade", "TestRecoveryGeneration"))}
        self.assertEqual(len(inherited), 583)
        pairs = sorted(inherited - OBSERVER_IDENTITIES - EXECUTION_BOUND_IDENTITIES)
        actual = hashlib.sha256(json.dumps(pairs, separators=(",", ":"), ensure_ascii=True).encode()).hexdigest()
        self.assertEqual(len(pairs), 569)
        self.assertEqual(actual, "8eb9e04d2061e1c8659cb89a38e43648c0c241d2349900d0bf749503f9f19062")

    def test_every_accepted_c1_requirement_is_retained_exactly(self):
        pairs = sorted(events.required_identities() - FIXTURE_IDENTITIES - C2_IDENTITIES)
        self.assertEqual(len(pairs), 726)
        self.assertEqual(hashlib.sha256(json.dumps(pairs, separators=(",", ":"), ensure_ascii=True).encode()).hexdigest(),
                         "ca0f0c1619544bdb5ee5a1d3176095337a8295c20e14cdda2f8c451e1ad8ff65")

    def test_each_c2_requirement_blocks_missing_skip_and_failure(self):
        good = [{"Package": package, "Test": name, "Action": "pass"} for package, name in events.required_identities()]
        self.assertEqual(len(C2_IDENTITIES), 140)
        self.assertTrue({("github.com/franchoy/coldkeep/v2/internal/generation", name)
                         for name in ("TestGenerationUnicodeBoundaries",
                                      "TestGenerationValidationShortCircuit")} <= C2_IDENTITIES)
        for package, name in C2_IDENTITIES:
            for action in ("missing", "skip", "fail"):
                with self.subTest(name=name, action=action):
                    changed = [event for event in good if (event["Package"], event["Test"]) != (package, name)]
                    if action != "missing":
                        changed.append({"Package": package, "Test": name, "Action": action})
                    self.assertEqual(events.check_events(changed)["status"], "FAIL")
                    self.assertEqual(events.check_events(changed, broad=True)["status"], "FAIL")

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
