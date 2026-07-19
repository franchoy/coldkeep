#!/usr/bin/env python3
"""Isolated fixture tests for the release-state validator."""

from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest import mock

import release_state_support
import validate_release_state
from release_state_support import ProcessResult, resolved_executable, run_process


SCRIPT = Path(__file__).with_name("validate_release_state.py").resolve()
GITHUB_KEYS = ("GITHUB_EVENT_NAME", "GITHUB_REF", "GITHUB_REF_NAME", "GITHUB_HEAD_REF")


def write(root: Path, relative: str, content: str) -> None:
    target = root / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")


def git(root: Path, *args: str) -> None:
    run_process(
        [resolved_executable("git"), "-C", str(root), *args],
        check=True,
    )


def run_validator(
    root: Path,
    *args: str,
    env: dict[str, str] | None = None,
    cwd: Path | None = None,
) -> ProcessResult:
    """Run the validator through the reviewed isolated process boundary."""
    actual_env = os.environ.copy()
    for key in GITHUB_KEYS:
        actual_env.pop(key, None)
    if env:
        actual_env.update(env)
    return run_process(
        [sys.executable, str(SCRIPT), "--repo-root", str(root), *args],
        cwd=cwd or (root if root.is_dir() else SCRIPT.parent),
        env=actual_env,
        check=False,
    )


def replace(root: Path, relative: str, old: str, new: str) -> None:
    target = root / relative
    text = target.read_text(encoding="utf-8")
    if old not in text:
        raise AssertionError(f"fixture replacement missing in {relative}: {old!r}")
    target.write_text(text.replace(old, new, 1), encoding="utf-8")


class Fixture:
    def __init__(self) -> None:
        """Create one minimal, isolated Git release-state repository."""
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "repo"
        self.root.mkdir()
        self.write_default()
        git(self.root, "init")
        git(self.root, "config", "user.name", "fixture")
        git(self.root, "config", "user.email", "fixture@example.invalid")
        git(self.root, "add", ".")
        git(self.root, "commit", "-m", "fixture")
        git(self.root, "checkout", "-b", "release/v1.13.10")

    def close(self) -> None:
        self.tmp.cleanup()

    def write_default(self) -> None:
        write(self.root, "internal/version/version.go", "package version\nconst (\n Major = 1\n Minor = 13\n Patch = 10\n)\n")
        write(self.root, "internal/version/version_test.go", "package version\nfunc TestStringReturnsSemverFromConstants() { _ = \"1.13.10\" }\n")
        write(self.root, "PRE_RELEASE_CHECKLIST.md", "# Pre-release Checklist\nexpected_version=\"1.13.10\"\n")
        write(self.root, "CHANGELOG.md", "# Changelog\n\n## v1.13.10 - Unreleased — Fixture\n\n## v1.13.8 - 2026-01-01 — Previous\n")
        write(
            self.root,
            "README.md",
            "# Coldkeep\n"
            "![Status](https://img.shields.io/badge/status-v1.13.10%20active-blue)\n\n"
            "## Current release state\n\nv1.13.10 is active.\n",
        )
        write(self.root, "docs/release/v1.13/README.md", "# v1.13\n\n## Current Release State\n\nv1.13.10 is active. Phase 1 is next.\n")
        header = "# Fixture\n\n**Release:** `v1.13.10 — Fixture Release`\n**Status:** Active\n**Branch:** `release/v1.13.10`\n"
        write(
            self.root,
            "docs/release/v1.13/v1.13.10-scope.md",
            header
            + "\n## Phase summary\n\n- Phase 0: Complete\n- Phase 1: Next\n"
            "- Phase 2: Not started\n",
        )
        write(
            self.root,
            "docs/release/v1.13/v1.13.10-phase-list.md",
            header
            + "\n## Phase 0 — Setup\n\n**Status:** Complete\n\n"
            "[contract](v1.13.10-release-state-validator-contract.md)\n\n"
            "## Phase 1 — Work\n\n**Status:** Next\n\n"
            "## Phase 2 — Later\n\n**Status:** Not started\n",
        )
        write(
            self.root,
            "docs/release/v1.13/v1.13.10-validation-checklist.md",
            header
            + "\n## Phase 0\n\n**Phase status:** Complete\n\n"
            "## Phase 1\n\n**Phase status:** Next\n\n"
            "## Phase 2\n\n**Phase status:** Not started\n",
        )
        write(
            self.root,
            "docs/release/v1.13/v1.13.x-release-train.md",
            "# Train\n\n### `v1.13.8 — Previous`\n\n"
            "**Status:** Released and operationally closed\n\n"
            "### `v1.13.10 — Fixture Release`\n\n**Status:** Active\n\n"
            "Current definition.\n\n"
            "## Historical proposed continuation and final disposition\n\n"
            "### Historical proposed `v1.13.10 — Old Proposal`\n\n"
            "Pending historical planning.\n",
        )
        write(self.root, "docs/release/v1.13/v1.13.10-release-train-reconciliation.md", "# Reconciliation\n")
        write(self.root, "docs/release/v1.13/v1.13.10-release-state-validator-contract.md", "# Contract\n")
        prior_header = "# Prior\n\n**Release:** `v1.13.8 — Previous`\n**Status:** Released and operationally closed\n"
        write(self.root, "docs/release/v1.13/v1.13.8-scope.md", prior_header)
        write(
            self.root,
            "docs/release/v1.13/v1.13.8-release-gate.md",
            "# Gate\n\n**Release:** `v1.13.8 — Previous`\n"
            "**Status:** Passed and released\n\n## Final verdict\n\nREADY\n",
        )

    def commit(self) -> None:
        git(self.root, "add", ".")
        git(self.root, "commit", "-m", "mutate")

    def complete_release(self, with_gate: bool = True) -> None:
        for relative in (
            "docs/release/v1.13/v1.13.10-scope.md",
            "docs/release/v1.13/v1.13.10-phase-list.md",
            "docs/release/v1.13/v1.13.10-validation-checklist.md",
        ):
            replace(self.root, relative, "**Status:** Active", "**Status:** Ready for release")
        for relative in ("docs/release/v1.13/v1.13.10-phase-list.md", "docs/release/v1.13/v1.13.10-validation-checklist.md"):
            replace(self.root, relative, "Next", "Complete")
            replace(self.root, relative, "Not started", "Complete")
        replace(self.root, "CHANGELOG.md", "v1.13.10 - Unreleased", "v1.13.10 - 2026-01-02")
        replace(self.root, "README.md", "v1.13.10 is active", "v1.13.10 is ready for release")
        replace(self.root, "docs/release/v1.13/README.md", "v1.13.10 is active", "v1.13.10 is ready for release")
        if with_gate:
            write(
                self.root,
                "docs/release/v1.13/v1.13.10-release-gate.md",
                "# Gate\n\n**Status:** Passed — awaiting publication\n\n"
                "## Final verdict\n\nREADY\n",
            )

    def run(
        self,
        *args: str,
        env: dict[str, str] | None = None,
        cwd: Path | None = None,
    ) -> ProcessResult:
        return run_validator(self.root, *args, env=env, cwd=cwd)


class ReleaseStateValidatorTests(unittest.TestCase):
    def fixture(self) -> Fixture:
        fixture = Fixture()
        self.addCleanup(fixture.close)
        return fixture

    def assert_rules(self, process: ProcessResult, expected: list[str], code: int = 1) -> None:
        self.assertEqual(process.returncode, code, process.stdout + process.stderr)
        found = [line[1:8] for line in process.stdout.splitlines() if line.startswith("[CKRS")]
        self.assertEqual(found, expected, process.stdout)

    def assert_ok(self, process: ProcessResult) -> None:
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        self.assertIn("[release-state] OK", process.stdout)
        self.assertEqual(process.stderr, "")

    def test_01_valid_development(self) -> None:
        self.assert_ok(self.fixture().run("--state", "auto"))

    def test_02_malformed_source(self) -> None:
        fixture = self.fixture(); replace(fixture.root, "internal/version/version.go", "Major = 1", "Major = bad")
        self.assert_rules(fixture.run(), ["CKRS001"])

    def test_03_missing_source_component(self) -> None:
        fixture = self.fixture(); replace(fixture.root, "internal/version/version.go", " Patch = 10\n", "")
        self.assert_rules(fixture.run(), ["CKRS001"])

    def test_04_duplicate_source_component(self) -> None:
        fixture = self.fixture()
        path = fixture.root / "internal/version/version.go"
        write(fixture.root, "internal/version/version.go", path.read_text() + "Major = 1\n")
        self.assert_rules(fixture.run(), ["CKRS001"])

    def test_05_version_test_mismatch(self) -> None:
        fixture = self.fixture(); replace(fixture.root, "internal/version/version_test.go", "1.13.10", "1.13.9")
        self.assert_rules(fixture.run(), ["CKRS002"])

    def test_06_checklist_version_mismatch(self) -> None:
        fixture = self.fixture(); replace(fixture.root, "PRE_RELEASE_CHECKLIST.md", "1.13.10", "1.13.9")
        self.assert_rules(fixture.run(), ["CKRS003"])

    def test_07_changelog_version_mismatch(self) -> None:
        fixture = self.fixture(); replace(fixture.root, "CHANGELOG.md", "v1.13.10", "v1.13.9")
        self.assert_rules(fixture.run(), ["CKRS004"])

    def test_08_changelog_lifecycle_mismatch(self) -> None:
        fixture = self.fixture()
        fixture.complete_release()
        self.assert_ok(fixture.run("--state", "pre-release"))
        replace(fixture.root, "CHANGELOG.md", "2026-01-02", "Unreleased")
        self.assert_rules(fixture.run("--state", "pre-release"), ["CKRS004"])

    def test_09_root_readme_mismatch(self) -> None:
        fixture = self.fixture()
        replace(fixture.root, "README.md", "v1.13.10 is active", "v1.13.9 is active")
        replace(fixture.root, "README.md", "v1.13.10%20active", "v1.13.9%20active")
        self.assert_rules(fixture.run(), ["CKRS005"])

    def test_10_release_readme_mismatch(self) -> None:
        fixture = self.fixture(); replace(fixture.root, "docs/release/v1.13/README.md", "v1.13.10 is active", "v1.13.9 is active")
        self.assert_rules(fixture.run(), ["CKRS006"])

    def test_11_tracker_mismatch(self) -> None:
        fixture = self.fixture(); replace(fixture.root, "docs/release/v1.13/v1.13.10-scope.md", "Fixture Release", "Other Release")
        self.assert_rules(fixture.run(), ["CKRS007", "CKRS008"])

    def test_12_duplicate_train_definition(self) -> None:
        fixture = self.fixture()
        replace(
            fixture.root,
            "docs/release/v1.13/v1.13.x-release-train.md",
            "Current definition.",
            "Current definition.\n\n### `v1.13.10 — Fixture Release`\n",
        )
        self.assert_rules(fixture.run(), ["CKRS008"])

    def test_13_previous_unreleased(self) -> None:
        fixture = self.fixture(); replace(fixture.root, "CHANGELOG.md", "v1.13.8 - 2026-01-01", "v1.13.8 - Unreleased")
        self.assert_rules(fixture.run(), ["CKRS009"])

    def test_14_missing_prior_gate(self) -> None:
        fixture = self.fixture(); (fixture.root / "docs/release/v1.13/v1.13.8-release-gate.md").unlink()
        self.assert_rules(fixture.run(), ["CKRS010"])

    def test_15_current_prior_branch_active(self) -> None:
        fixture = self.fixture(); replace(fixture.root, "docs/release/v1.13/README.md", "v1.13.10 is active", "release/v1.13.8 is active\nv1.13.10 is active")
        self.assert_rules(fixture.run(), ["CKRS011"])

    def test_16_missing_phase(self) -> None:
        fixture = self.fixture(); replace(fixture.root, "docs/release/v1.13/v1.13.10-validation-checklist.md", "## Phase 1\n\n**Phase status:** Next\n\n", "")
        self.assert_rules(fixture.run(), ["CKRS012"])

    def test_17_duplicate_phase(self) -> None:
        fixture = self.fixture()
        relative = "docs/release/v1.13/v1.13.10-phase-list.md"
        content = (fixture.root / relative).read_text()
        write(
            fixture.root,
            relative,
            content + "\n## Phase 2 — Duplicate\n\n**Status:** Not started\n",
        )
        self.assert_rules(fixture.run(), ["CKRS012"])

    def test_18_phase_gap(self) -> None:
        fixture = self.fixture()
        replace(
            fixture.root,
            "docs/release/v1.13/v1.13.10-phase-list.md",
            "Phase 2 — Later",
            "Phase 3 — Later",
        )
        replace(
            fixture.root,
            "docs/release/v1.13/v1.13.10-validation-checklist.md",
            "Phase 2\n",
            "Phase 3\n",
        )
        self.assert_rules(fixture.run(), ["CKRS012"])

    def test_19_invalid_progression(self) -> None:
        fixture = self.fixture(); replace(fixture.root, "docs/release/v1.13/v1.13.10-phase-list.md", "**Status:** Next", "**Status:** Not started")
        self.assert_rules(fixture.run(), ["CKRS013", "CKRS014"])

    def test_20_phase_active_invalid(self) -> None:
        fixture = self.fixture(); replace(fixture.root, "docs/release/v1.13/v1.13.10-phase-list.md", "**Status:** Next", "**Status:** Active")
        self.assert_rules(fixture.run(), ["CKRS013", "CKRS014"])

    def test_21_current_pointer_mismatch(self) -> None:
        fixture = self.fixture(); replace(fixture.root, "docs/release/v1.13/README.md", "Phase 1 is next", "Phase 2 is next")
        self.assert_rules(fixture.run(), ["CKRS014"])

    def test_22_missing_artifact(self) -> None:
        fixture = self.fixture()
        replace(
            fixture.root,
            "docs/release/v1.13/v1.13.10-phase-list.md",
            "v1.13.10-release-state-validator-contract.md",
            "missing.md",
        )
        self.assert_rules(fixture.run(), ["CKRS015"])

    def test_23_escape_artifact(self) -> None:
        fixture = self.fixture()
        replace(
            fixture.root,
            "docs/release/v1.13/v1.13.10-phase-list.md",
            "v1.13.10-release-state-validator-contract.md",
            "../../escape.md",
        )
        self.assert_rules(fixture.run(), ["CKRS015"])

    def test_24_wrong_branch(self) -> None:
        fixture = self.fixture(); git(fixture.root, "checkout", "-b", "wrong-branch")
        self.assert_rules(fixture.run("--state", "development"), ["CKRS016"])

    def test_25_unknown_detached(self) -> None:
        fixture = self.fixture(); git(fixture.root, "checkout", "--detach")
        process = fixture.run(); self.assertEqual(process.returncode, 2); self.assertIn("git-context", process.stderr)

    def test_26_lightweight_tag(self) -> None:
        fixture = self.fixture(); fixture.complete_release(); fixture.commit(); git(fixture.root, "tag", "v1.13.10")
        self.assert_rules(fixture.run("--state", "released"), ["CKRS017"])

    def test_27_wrong_annotated_target(self) -> None:
        fixture = self.fixture()
        fixture.complete_release()
        fixture.commit()
        old = run_process(
            [resolved_executable("git"), "-C", str(fixture.root), "rev-parse", "HEAD"],
            check=True,
        ).stdout.strip()
        git(fixture.root, "tag", "-a", "v1.13.10", old, "-m", "tag")
        write(fixture.root, "x", "x\n")
        fixture.commit()
        self.assert_rules(fixture.run("--state", "released"), ["CKRS017"])

    def test_28_missing_pre_release_gate(self) -> None:
        fixture = self.fixture(); fixture.complete_release(False)
        self.assert_rules(fixture.run("--state", "pre-release"), ["CKRS018"])

    def test_29_premature_passed_gate(self) -> None:
        fixture = self.fixture(); write(fixture.root, "docs/release/v1.13/v1.13.10-release-gate.md", "# Gate\n\n**Status:** Passed\n")
        self.assert_rules(fixture.run(), ["CKRS018"])

    def test_30_missing_required_document(self) -> None:
        fixture = self.fixture(); (fixture.root / "docs/release/v1.13/v1.13.10-release-train-reconciliation.md").unlink()
        self.assert_rules(fixture.run(), ["CKRS019"])

    def test_31_historical_branch_accepted(self) -> None:
        fixture = self.fixture()
        relative = "docs/release/v1.13/v1.13.x-release-train.md"
        content = (fixture.root / relative).read_text()
        write(fixture.root, relative, content + "release/v1.13.8 is active historically\n")
        self.assert_ok(fixture.run())

    def test_32_historical_pending_accepted(self) -> None:
        fixture = self.fixture(); self.assert_ok(fixture.run())

    def test_33_retired_proposal_accepted(self) -> None:
        fixture = self.fixture(); self.assert_ok(fixture.run())

    def test_34_pr_merge_ref_accepted(self) -> None:
        fixture = self.fixture(); git(fixture.root, "checkout", "--detach")
        self.assert_ok(
            fixture.run(
                env={
                    "GITHUB_EVENT_NAME": "pull_request",
                    "GITHUB_REF": "refs/pull/7/merge",
                    "GITHUB_REF_NAME": "7/merge",
                    "GITHUB_HEAD_REF": "release/v1.13.10",
                },
            ),
        )

    def test_35_pr_wrong_head_rejected(self) -> None:
        fixture = self.fixture(); git(fixture.root, "checkout", "--detach")
        process = fixture.run(
            env={
                "GITHUB_EVENT_NAME": "pull_request",
                "GITHUB_REF": "refs/pull/7/merge",
                "GITHUB_HEAD_REF": "release/v1.13.9",
            },
        )
        self.assertEqual(process.returncode, 2)

    def test_36_main_merged_not_tagged(self) -> None:
        fixture = self.fixture(); fixture.complete_release(); fixture.commit(); git(fixture.root, "branch", "-M", "main")
        self.assert_ok(fixture.run())

    def test_37_detached_annotated_tag(self) -> None:
        fixture = self.fixture()
        fixture.complete_release()
        fixture.commit()
        git(fixture.root, "tag", "-a", "v1.13.10", "-m", "tag")
        git(fixture.root, "checkout", "--detach")
        self.assert_ok(fixture.run())

    def test_38_skipped_previous_patch(self) -> None:
        fixture = self.fixture(); self.assert_ok(fixture.run())

    def test_39_different_working_directory(self) -> None:
        fixture = self.fixture(); self.assert_ok(fixture.run(cwd=Path("/")))

    def test_40_explicit_root(self) -> None:
        fixture = self.fixture(); self.assert_ok(fixture.run("--state", "development"))

    def test_41_human_deterministic(self) -> None:
        fixture = self.fixture()
        replace(fixture.root, "PRE_RELEASE_CHECKLIST.md", "1.13.10", "1.13.9")
        first, second = fixture.run(), fixture.run()
        self.assertEqual(first.stdout, second.stdout)

    def test_42_json_deterministic(self) -> None:
        fixture = self.fixture()
        first, second = fixture.run("--json"), fixture.run("--json")
        self.assertEqual(first.stdout, second.stdout)
        self.assertEqual(json.loads(first.stdout)["status"], "ok")

    def test_43_json_one_object_no_stderr(self) -> None:
        fixture = self.fixture(); process = fixture.run("--json"); self.assertEqual(len(process.stdout.splitlines()), 1); self.assertEqual(process.stderr, "")

    def test_44_internal_error_stderr(self) -> None:
        fixture = self.fixture()
        process = run_validator(fixture.root / "missing")
        self.assertEqual(process.returncode, 2)
        self.assertEqual(process.stdout, "")
        self.assertIn("[release-state] ERROR repository-layout", process.stderr)

    def test_45_missing_git_executable(self) -> None:
        fixture = self.fixture()
        process = fixture.run(env={"PATH": ""})
        self.assertEqual(process.returncode, 2)
        self.assertIn("[release-state] ERROR git", process.stderr)

    def test_46_git_executable_is_absolute(self) -> None:
        self.assertTrue(Path(resolved_executable("git")).is_absolute())

    def test_47_process_helper_disables_shell(self) -> None:
        completed = ProcessResult(["command"], 0, "", "")
        with mock.patch.object(
            release_state_support.subprocess,
            "run",
            return_value=completed,
        ) as process_run:
            run_process(["command"], check=False)
        self.assertIs(process_run.call_args.kwargs["shell"], False)

    def test_48_unexpected_exception_is_deterministic(self) -> None:
        first = io.StringIO()
        second = io.StringIO()
        with mock.patch.object(
            validate_release_state,
            "validate_root",
            side_effect=RuntimeError("fixture failure"),
        ):
            with redirect_stderr(first):
                first_status = validate_release_state.main([])
            with redirect_stderr(second):
                second_status = validate_release_state.main([])
        self.assertEqual(first_status, 2)
        self.assertEqual(second_status, 2)
        self.assertEqual(first.getvalue(), second.getvalue())
        self.assertIn("[release-state] ERROR internal: fixture failure", first.getvalue())


if __name__ == "__main__":
    unittest.main(verbosity=2)
