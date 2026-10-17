#!/usr/bin/env python3
"""Isolated fixture tests for the release-state validator."""

from __future__ import annotations

import io
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

import release_state_support
import validate_release_state
import validate_governance
from release_state_support import (
    LifecycleBoundaries,
    ProcessResult,
    lifecycle_boundaries_match_topology,
    lifecycle_progression_valid,
    resolved_executable,
    run_process,
)


def parse_release_state_human_output(output: str) -> dict[str, object]:
    """Parse one unambiguous human summary and its ordered CKRS rule list."""
    lines = output.splitlines()
    summaries = [
        line for line in lines if line.startswith("[release-state] ")
    ]
    if len(summaries) != 1:
        raise AssertionError(f"expected one release-state summary, got {summaries!r}")
    summary = summaries[0]
    match = re.fullmatch(r"\[release-state\] (OK|FAILED)(?: ([^\n]+))?", summary)
    if match is None:
        raise AssertionError(f"malformed release-state summary: {summary!r}")
    fields: dict[str, str] = {}
    for token in (match.group(2) or "").split():
        if token.count("=") != 1:
            raise AssertionError(f"ambiguous release-state token: {token!r}")
        key, value = token.split("=", 1)
        if not re.fullmatch(r"[a-z][a-z0-9_]*", key) or not value:
            raise AssertionError(f"invalid release-state token: {token!r}")
        if key in fields:
            raise AssertionError(f"duplicate release-state field: {key}")
        fields[key] = value
    rules = []
    for line in lines:
        rule = re.match(r"^\[(CKRS[0-9]{3})\](?: |$)", line)
        if rule:
            rules.append(rule.group(1))
    return {"status": match.group(1), "fields": fields, "rules": rules}


SCRIPT = Path(__file__).with_name("validate_release_state.py").resolve()
GITHUB_KEYS = (
    "GITHUB_EVENT_NAME",
    "GITHUB_REF",
    "GITHUB_REF_NAME",
    "GITHUB_HEAD_REF",
    "GITHUB_BASE_REF",
    "GITHUB_EVENT_PATH",
    "GITHUB_REPOSITORY",
    "GITHUB_REPOSITORY_ID",
    "GITHUB_SHA",
    "GITHUB_ACTIONS",
    "GITHUB_REF_TYPE",
    "GITHUB_RUN_ID",
    "GITHUB_RUN_ATTEMPT",
    "GITHUB_JOB",
    "GITHUB_WORKFLOW",
    "GITHUB_TOKEN",
)
FIXTURE_GIT_CONFIG = (
    "-c",
    "maintenance.auto=false",
    "-c",
    "gc.auto=0",
)


def write(root: Path, relative: str, content: str) -> None:
    target = root / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")


def git(root: Path, *args: str) -> None:
    # Fixture-owned repositories must not outlive awaited Git through auto work.
    run_process(
        [
            resolved_executable("git"),
            *FIXTURE_GIT_CONFIG,
            "-C",
            str(root),
            *args,
        ],
        check=True,
    )


class FixtureGitWrapperTests(unittest.TestCase):
    def test_git_wrapper_disables_automatic_maintenance(self) -> None:
        root = Path("/TEST_FIXTURE_ONLY/repository")
        for suffix in (
            ("init",),
            ("commit", "--allow-empty", "-m", "TEST_FIXTURE_ONLY"),
            ("rev-parse", "HEAD"),
        ):
            with self.subTest(suffix=suffix):
                completed = ProcessResult(["TEST_FIXTURE_ONLY"], 0, "", "")
                with mock.patch.object(
                    sys.modules[__name__],
                    "run_process",
                    return_value=completed,
                ) as process:
                    result = git(root, *suffix)
                self.assertIsNone(result)
                process.assert_called_once_with(
                    [
                        resolved_executable("git"),
                        "-c",
                        "maintenance.auto=false",
                        "-c",
                        "gc.auto=0",
                        "-C",
                        str(root),
                        *suffix,
                    ],
                    check=True,
                )

        failure = subprocess.CalledProcessError(9, ["TEST_FIXTURE_ONLY"])
        with mock.patch.object(
            sys.modules[__name__],
            "run_process",
            side_effect=failure,
        ):
            with self.assertRaises(subprocess.CalledProcessError) as raised:
                git(root, "status")
        self.assertIs(raised.exception, failure)

        actual_run_process = run_process
        contrary = os.environ.copy()
        contrary.update(
            {
                "GIT_CONFIG_COUNT": "2",
                "GIT_CONFIG_KEY_0": "maintenance.auto",
                "GIT_CONFIG_VALUE_0": "true",
                "GIT_CONFIG_KEY_1": "gc.auto",
                "GIT_CONFIG_VALUE_1": "1",
            }
        )
        observed: list[ProcessResult] = []

        def execute(argv: list[str], **kwargs: object) -> ProcessResult:
            process = actual_run_process(argv, env=contrary, **kwargs)
            observed.append(process)
            return process

        with tempfile.TemporaryDirectory() as directory:
            repository = Path(directory) / "repo"
            repository.mkdir()
            with mock.patch.object(
                sys.modules[__name__],
                "run_process",
                side_effect=execute,
            ):
                git(repository, "init")
                git(repository, "config", "--get", "maintenance.auto")
                git(repository, "config", "--get", "gc.auto")
            self.assertEqual(
                [process.stdout.strip() for process in observed[-2:]],
                ["false", "0"],
            )

            clean_environment = os.environ.copy()
            for key in tuple(clean_environment):
                if key.startswith("GIT_CONFIG"):
                    clean_environment.pop(key)
            for key in ("maintenance.auto", "gc.auto"):
                with self.subTest(local_config=key):
                    local = actual_run_process(
                        [
                            resolved_executable("git"),
                            "-C",
                            str(repository),
                            "config",
                            "--local",
                            "--get",
                            key,
                        ],
                        env=clean_environment,
                        check=False,
                    )
                    self.assertEqual(local.returncode, 1, local.stdout + local.stderr)


SUCCESSOR_COMPATIBILITY_BASE = "3d32e1662fdc0de251fd87bc5af6a309fcf3b6a6"
SUCCESSOR_HISTORICAL_PATHS = (
    Path("docs/release/v1.13/v1.13.17-release-state.md"),
    Path("docs/release/v1.13/v1.13.17-release-state-validator-contract.md"),
    Path("internal/version/version.go"),
    Path("internal/version/version_test.go"),
    Path("CHANGELOG.md"),
    Path("CONTRIBUTING.md"),
    Path("PRE_RELEASE_CHECKLIST.md"),
    Path("AGENTS.md"),
    Path("README.md"),
    Path("SECURITY.md"),
    Path(".github/copilot-instructions.md"),
    Path(".github/instructions/ci.instructions.md"),
    Path(".github/prompts/critical-path-coverage.prompt.md"),
    Path(".github/prompts/regression-fix.prompt.md"),
    Path(".github/workflows/ci.yml"),
    Path("docs/release/v1.13/README.md"),
    Path("docs/release/v1.13/v1.13.x-release-train.md"),
)
SUCCESSOR_TERMINAL_ONLY_PATHS = (
    validate_governance.TERMINAL_DISPOSITION_REFERENCE_FILE,
    validate_governance.SECURITY_TOOLCHAIN_PROGRESSION_REFERENCE_FILE,
)


def copy_successor_fixture_source(
    source_root: Path,
    destination_root: Path,
    *,
    compatibility_base: str = SUCCESSOR_COMPATIBILITY_BASE,
) -> None:
    """Copy the tracked source plus the explicit current publication pair."""
    tracked = run_process(
        [resolved_executable("git"), "-C", str(source_root), "ls-files", "-z"],
        check=True,
    ).stdout.split("\0")
    paths = {Path(item) for item in tracked if item}
    for relative in (
        validate_governance.CURRENT_RELEASE_BODY,
        validate_governance.CURRENT_RELEASE_BODY_CHECKSUM,
    ):
        source = source_root / relative
        if source.exists() or source.is_symlink():
            paths.add(relative)
    paths.difference_update(SUCCESSOR_TERMINAL_ONLY_PATHS)
    paths.difference_update(validate_governance.V18_AUTHORITY_FILES)
    for relative in sorted(paths):
        source = source_root / relative
        destination = destination_root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination, follow_symlinks=False)
    # Compatibility fixtures model the historical successor lifecycle, not the
    # terminal serialization route added by this change. Keep their production
    # Python current while anchoring the two route-selector documents at C.
    for relative in SUCCESSOR_HISTORICAL_PATHS:
        if not (source_root / relative).is_file():
            raise FileNotFoundError(
                f"required compatibility source is unavailable: {relative}"
            )
        historical = run_process(
            [
                resolved_executable("git"),
                "-C",
                str(source_root),
                "show",
                f"{compatibility_base}:{relative.as_posix()}",
            ],
            check=True,
        )
        (destination_root / relative).write_text(
            historical.stdout, encoding="utf-8"
        )
    for relative in SUCCESSOR_TERMINAL_ONLY_PATHS:
        if (destination_root / relative).exists() or (
            destination_root / relative
        ).is_symlink():
            raise AssertionError(f"historical fixture retained terminal path: {relative}")
    state = (
        destination_root / validate_governance.CANONICAL_CURRENT_STATE_FILE
    ).read_text(encoding="utf-8")
    contract = (
        destination_root
        / "docs/release/v1.13/v1.13.17-release-state-validator-contract.md"
    ).read_text(encoding="utf-8")
    forbidden = (
        "NONE_TERMINAL_DISPOSITION_DECLARED",
        "TERMINAL_DISPOSITION_REFERENCE:",
        "coldkeep-terminal-disposition-expectations:",
        "SECURITY_TOOLCHAIN_PROGRESSION_REFERENCE:",
        "coldkeep-security-toolchain-progression-expectations:",
    )
    combined = state + "\n" + contract
    retained = [marker for marker in forbidden if marker in combined]
    if retained:
        raise AssertionError(
            f"historical fixture retained terminal selectors: {retained}"
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


def run_validator_script(
    script: Path,
    root: Path,
    *args: str,
    env: dict[str, str] | None = None,
) -> ProcessResult:
    """Run an isolated repository's own copied validator implementation."""
    actual_env = os.environ.copy()
    for key in GITHUB_KEYS:
        actual_env.pop(key, None)
    if env:
        actual_env.update(env)
    return run_process(
        [sys.executable, str(script), "--repo-root", str(root), *args],
        cwd=root,
        env=actual_env,
        check=False,
    )


class FixtureEnvironmentIsolationV2Tests(unittest.TestCase):
    """Prove fixture launchers remove every ambient GitHub context field."""

    def captured_environment(self, *, outer: dict[str, str], explicit: dict[str, str] | None = None) -> dict[str, str]:
        captured: dict[str, str] = {}

        def capture(*args: object, **kwargs: object) -> ProcessResult:
            captured.update(kwargs["env"])
            return ProcessResult(["TEST_FIXTURE_ONLY"], 0, "", "")

        with mock.patch.dict(os.environ, outer, clear=True), mock.patch.object(
            sys.modules[__name__], "run_process", side_effect=capture
        ):
            run_validator(Path("/TEST_FIXTURE_ONLY/repository"), env=explicit)
        return captured

    def real_fixture_run(
        self,
        fixture: "_V2ProgressionFixture",
        *,
        outer: dict[str, str],
        explicit: dict[str, str] | None = None,
        state: str = "auto",
    ) -> tuple[ProcessResult, dict[str, object], dict[str, str]]:
        """Record one delegated real validator child and prove parent restoration."""
        actual_run_process = run_process
        observed: list[dict[str, object]] = []

        def delegate(argv: list[str], **kwargs: object) -> ProcessResult:
            observed.append(
                {
                    "argv": list(argv),
                    "cwd": kwargs.get("cwd"),
                    "env": dict(kwargs["env"]),
                }
            )
            return actual_run_process(argv, **kwargs)

        original_parent = dict(os.environ)
        with mock.patch.dict(os.environ, outer, clear=True):
            patched_parent = dict(os.environ)
            with mock.patch.object(
                sys.modules[__name__], "run_process", side_effect=delegate
            ):
                completed, payload = fixture.run_json(state, env=explicit)
            self.assertEqual(os.environ, patched_parent)
        self.assertEqual(os.environ, original_parent)
        self.assertEqual(len(observed), 1)
        self.assertEqual(observed[0]["argv"][0], sys.executable)
        self.assertEqual(Path(observed[0]["argv"][1]).resolve(), SCRIPT)
        return completed, payload, observed[0]["env"]

    def contaminated_outer(self, **github: str) -> dict[str, str]:
        outer = dict(os.environ)
        outer["COLDKEEP_TEST_UNRELATED"] = "retained"
        outer.update(github)
        return outer

    def test_github_keys_cover_every_production_context_field(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            production = set(release_state_support.github_context())
        self.assertTrue(production.issubset(set(GITHUB_KEYS)))
        self.assertIn("GITHUB_REPOSITORY_ID", GITHUB_KEYS)

    def test_run_validator_explicit_repository_id_override_wins_after_cleanup(self) -> None:
        env = self.captured_environment(
            outer={"GITHUB_REPOSITORY_ID": "999999999"},
            explicit={"GITHUB_REPOSITORY_ID": "1158711822"},
        )
        self.assertEqual(env["GITHUB_REPOSITORY_ID"], "1158711822")
        fixture = _V2ProgressionFixture()
        self.addCleanup(fixture.close)
        h3 = fixture.make_h3()
        _, exact = fixture.push_context(h3)
        outer = self.contaminated_outer(GITHUB_REPOSITORY_ID="999999999")
        completed, payload, child = self.real_fixture_run(
            fixture, outer=outer, explicit=exact
        )
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertEqual(
            payload["evidence_scope"],
            "GITHUB_H2_SUCCESSOR_PUSH_CONTEXT_CONSISTENCY",
        )
        self.assertEqual(child["GITHUB_REPOSITORY_ID"], "1158711822")
        for name, value in (
            ("missing", None),
            ("wrong", "1158711821"),
            ("boolean-like", "true"),
            ("zero", "0"),
            ("signed", "+1158711822"),
            ("whitespace", " 1158711822 "),
            ("partial", "115871182"),
        ):
            with self.subTest(explicit_repository_id=name):
                invalid = dict(exact)
                if value is None:
                    invalid.pop("GITHUB_REPOSITORY_ID")
                else:
                    invalid["GITHUB_REPOSITORY_ID"] = value
                failed, failed_payload, failed_child = self.real_fixture_run(
                    fixture, outer=outer, explicit=invalid,
                    state="post-release-terminal-serialization-candidate",
                )
                self.assertEqual(failed.returncode, 1)
                self.assertIn(
                    "CKRS016",
                    [item["rule"] for item in failed_payload["violations"]],
                )
                self.assertEqual(
                    failed_child.get("GITHUB_REPOSITORY_ID"), value
                )

    def test_run_validator_preserves_unrelated_parent_environment_and_restores_outer_environment(self) -> None:
        outer = {"COLDKEEP_TEST_UNRELATED": "retained", "GITHUB_SHA": "a" * 40}
        with mock.patch.dict(os.environ, outer, clear=True):
            env = self.captured_environment(outer=dict(os.environ))
            self.assertEqual(os.environ, outer)
        self.assertEqual(env["COLDKEEP_TEST_UNRELATED"], "retained")
        self.assertNotIn("GITHUB_SHA", env)
        fixture = _V2ProgressionFixture()
        self.addCleanup(fixture.close)
        h3 = fixture.make_h3()
        complete_outer = self.contaminated_outer(
            **{key: f"ambient-{index}" for index, key in enumerate(GITHUB_KEYS)}
        )
        completed, payload, child = self.real_fixture_run(
            fixture, outer=complete_outer
        )
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertEqual(payload["evidence_scope"], "LOCAL_H2_SUCCESSOR_CANDIDATE")
        self.assertEqual(child["COLDKEEP_TEST_UNRELATED"], "retained")
        self.assertTrue(all(key not in child for key in GITHUB_KEYS))
        _, push = fixture.push_context(h3)
        push["GITHUB_REPOSITORY_ID"] = "wrong"
        failed, failed_payload, failed_child = self.real_fixture_run(
            fixture,
            outer=complete_outer,
            explicit=push,
            state="post-release-terminal-serialization-candidate",
        )
        self.assertEqual(failed.returncode, 1)
        self.assertIn(
            "CKRS016", [item["rule"] for item in failed_payload["violations"]]
        )
        self.assertEqual(failed_child["COLDKEEP_TEST_UNRELATED"], "retained")

    def test_run_validator_strips_ambient_repository_id_before_local_fixture(self) -> None:
        env = self.captured_environment(outer={"GITHUB_REPOSITORY_ID": "999999999"})
        self.assertNotIn("GITHUB_REPOSITORY_ID", env)
        fixture = _V2ProgressionFixture()
        self.addCleanup(fixture.close)
        fixture.make_h3()
        outer = self.contaminated_outer(GITHUB_REPOSITORY_ID="999999999")
        completed, payload, child = self.real_fixture_run(fixture, outer=outer)
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertEqual(payload["state"], "post-release-terminal-serialization-candidate")
        self.assertEqual(payload["evidence_scope"], "LOCAL_H2_SUCCESSOR_CANDIDATE")
        self.assertEqual(payload["security_progression"], "H2_SUCCESSOR_STRUCTURAL_ONLY")
        self.assertNotIn("GITHUB_REPOSITORY_ID", child)
        self.assertEqual(child["COLDKEEP_TEST_UNRELATED"], "retained")

    def test_run_validator_strips_full_ambient_pr_context_before_fixture_override(self) -> None:
        ambient = {key: f"ambient-{index}" for index, key in enumerate(GITHUB_KEYS)}
        explicit = {"GITHUB_EVENT_NAME": "pull_request", "GITHUB_REF": "refs/pull/123/merge"}
        env = self.captured_environment(outer=ambient, explicit=explicit)
        self.assertEqual({key: env[key] for key in explicit}, explicit)
        self.assertTrue(all(key not in env for key in set(GITHUB_KEYS) - set(explicit)))
        fixture = _V2ProgressionFixture()
        self.addCleanup(fixture.close)
        h3 = fixture.make_h3()
        outer = self.contaminated_outer(**ambient)
        completed, payload, local_child = self.real_fixture_run(
            fixture, outer=outer
        )
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertEqual(payload["evidence_scope"], "LOCAL_H2_SUCCESSOR_CANDIDATE")
        self.assertTrue(all(key not in local_child for key in GITHUB_KEYS))
        _, direct = fixture.pr_context(h3, h3)
        completed, payload, direct_child = self.real_fixture_run(
            fixture, outer=outer, explicit=direct
        )
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertEqual(
            payload["evidence_scope"],
            "GITHUB_H2_SUCCESSOR_PR_DIRECT_CONTEXT_CONSISTENCY",
        )
        self.assertEqual(
            {key: direct_child[key] for key in direct}, direct
        )
        self.assertTrue(
            all(key not in direct_child for key in set(GITHUB_KEYS) - set(direct))
        )
        synthetic = fixture.synthetic_merge(h3)
        fixture.checkout_detached(synthetic)
        _, synthetic_environment = fixture.pr_context(h3, synthetic)
        completed, payload, synthetic_child = self.real_fixture_run(
            fixture, outer=outer, explicit=synthetic_environment
        )
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertEqual(
            payload["evidence_scope"],
            "GITHUB_H2_SUCCESSOR_PR_SYNTHETIC_CONTEXT_CONSISTENCY",
        )
        self.assertEqual(
            synthetic_child["GITHUB_EVENT_PATH"],
            synthetic_environment["GITHUB_EVENT_PATH"],
        )

    def test_run_validator_strips_full_ambient_push_context_before_fixture_override(self) -> None:
        ambient = {key: f"ambient-{index}" for index, key in enumerate(GITHUB_KEYS)}
        explicit = {"GITHUB_EVENT_NAME": "push", "GITHUB_REPOSITORY_ID": "1158711822"}
        env = self.captured_environment(outer=ambient, explicit=explicit)
        self.assertEqual({key: env[key] for key in explicit}, explicit)
        self.assertTrue(all(key not in env for key in set(GITHUB_KEYS) - set(explicit)))
        fixture = _V2ProgressionFixture()
        self.addCleanup(fixture.close)
        h3 = fixture.make_h3()
        outer = self.contaminated_outer(**ambient)
        completed, payload, local_child = self.real_fixture_run(
            fixture, outer=outer
        )
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertEqual(payload["evidence_scope"], "LOCAL_H2_SUCCESSOR_CANDIDATE")
        self.assertTrue(all(key not in local_child for key in GITHUB_KEYS))
        _, push = fixture.push_context(h3)
        for attachment in ("attached", "detached"):
            with self.subTest(attachment=attachment):
                if attachment == "detached":
                    fixture.checkout_detached(h3)
                completed, payload, child = self.real_fixture_run(
                    fixture, outer=outer, explicit=push
                )
                self.assertEqual(
                    completed.returncode, 0, completed.stdout + completed.stderr
                )
                self.assertEqual(
                    payload["evidence_scope"],
                    "GITHUB_H2_SUCCESSOR_PUSH_CONTEXT_CONSISTENCY",
                )
                self.assertEqual({key: child[key] for key in push}, push)
                self.assertTrue(
                    all(key not in child for key in set(GITHUB_KEYS) - set(push))
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
        self.write_active_trackers(header)
        self.write_prior_release()

    def write_active_trackers(self, header: str) -> None:
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

    def write_prior_release(self) -> None:
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

    def enable_lifecycle_boundaries(
        self,
        merge: int = 0,
        publication: int = 1,
        closure: int = 2,
    ) -> None:
        write(
            self.root,
            "docs/release/v1.13/v1.13.10-release-state-validator-contract.md",
            "# Contract\n\n"
            f"**Merge-complete phase:** {merge}\n"
            f"**Tag/publication phase:** {publication}\n"
            f"**Post-publication closure phase:** {closure}\n",
        )

    def enable_immutable_transition(
        self,
        model: str = "immutable-transition-v1",
        repository: str = "fixture/coldkeep",
    ) -> None:
        path = (
            self.root
            / "docs/release/v1.13/v1.13.10-release-state-validator-contract.md"
        )
        content = path.read_text(encoding="utf-8")
        path.write_text(
            content
            + f"**Lifecycle declaration model:** {model}\n"
            + f"**Canonical repository:** {repository}\n",
            encoding="utf-8",
        )

    def rev_parse(self, value: str) -> str:
        return run_process(
            [resolved_executable("git"), "-C", str(self.root), "rev-parse", value],
            check=True,
        ).stdout.strip()

    def prepare_immutable_pre_release(self) -> str:
        self.prepare_boundary_pre_release()
        self.enable_immutable_transition()
        self.commit()
        return self.rev_parse("HEAD")

    def create_immutable_merge(self) -> tuple[str, str, str]:
        candidate = self.prepare_immutable_pre_release()
        base = self.rev_parse(f"{candidate}^")
        git(self.root, "branch", "-f", "main", base)
        git(self.root, "checkout", "main")
        git(
            self.root,
            "merge",
            "--no-ff",
            "release/v1.13.10",
            "-m",
            "TEST_FIXTURE_ONLY immutable merge",
        )
        return base, candidate, self.rev_parse("HEAD")

    def write_push_event(
        self,
        ref: str,
        before: str,
        after: str,
        *,
        repository: str = "fixture/coldkeep",
        created: bool = False,
        deleted: bool = False,
        forced: bool = False,
    ) -> Path:
        path = self.root / "push-event.json"
        path.write_text(
            json.dumps(
                {
                    "ref": ref,
                    "before": before,
                    "after": after,
                    "created": created,
                    "deleted": deleted,
                    "forced": forced,
                    "repository": {"full_name": repository},
                },
            ),
            encoding="utf-8",
        )
        return path

    def push_env(
        self,
        ref: str,
        head: str,
        event: Path,
        *,
        repository: str = "fixture/coldkeep",
        ref_type: str = "branch",
    ) -> dict[str, str]:
        return {
            "GITHUB_ACTIONS": "true",
            "GITHUB_EVENT_NAME": "push",
            "GITHUB_REF": ref,
            "GITHUB_REF_NAME": ref.removeprefix("refs/heads/").removeprefix(
                "refs/tags/"
            ),
            "GITHUB_REF_TYPE": ref_type,
            "GITHUB_EVENT_PATH": str(event),
            "GITHUB_REPOSITORY": repository,
            "GITHUB_SHA": head,
        }

    def write_strict_pr_event(
        self,
        base_sha: str,
        head_sha: str,
        merge_sha: object,
        *,
        repository: str = "fixture/coldkeep",
        base_repository: str | None = None,
        head_repository: str | None = None,
        include_merge_sha: bool = True,
    ) -> Path:
        path = self.root / "strict-pr-event.json"
        pull_request: dict[str, object] = {
            "base": {
                "ref": "main",
                "sha": base_sha,
                "repo": {
                    "full_name": base_repository or repository,
                },
            },
            "head": {
                "ref": "release/v1.13.10",
                "sha": head_sha,
                "repo": {
                    "full_name": head_repository or repository,
                },
            },
        }
        if include_merge_sha:
            pull_request["merge_commit_sha"] = merge_sha
        path.write_text(
            json.dumps(
                {
                    "repository": {"full_name": repository},
                    "number": 7,
                    "pull_request": pull_request,
                },
            ),
            encoding="utf-8",
        )
        return path

    def strict_pr_env(self, event: Path, checked_out: str) -> dict[str, str]:
        return {
            "GITHUB_ACTIONS": "true",
            "GITHUB_EVENT_NAME": "pull_request",
            "GITHUB_REF": "refs/pull/7/merge",
            "GITHUB_REF_NAME": "7/merge",
            "GITHUB_HEAD_REF": "release/v1.13.10",
            "GITHUB_BASE_REF": "main",
            "GITHUB_EVENT_PATH": str(event),
            "GITHUB_REPOSITORY": "fixture/coldkeep",
            "GITHUB_SHA": checked_out,
        }

    def set_phase_states(self, statuses: list[str]) -> None:
        for relative, field in (
            ("docs/release/v1.13/v1.13.10-phase-list.md", "Status"),
            (
                "docs/release/v1.13/v1.13.10-validation-checklist.md",
                "Phase status",
            ),
        ):
            target = self.root / relative
            lines = target.read_text(encoding="utf-8").splitlines()
            phase = None
            output = []
            for line in lines:
                match = re.match(r"^## Phase (\d+)\b", line)
                if match:
                    phase = int(match.group(1))
                if phase is not None and line.startswith(f"**{field}:**"):
                    line = f"**{field}:** {statuses[phase]}"
                output.append(line)
            target.write_text("\n".join(output) + "\n", encoding="utf-8")
        scope = self.root / "docs/release/v1.13/v1.13.10-scope.md"
        prefix = scope.read_text(encoding="utf-8").split("## Phase summary", 1)[0]
        summary = "\n".join(
            f"- Phase {number}: {status}"
            for number, status in enumerate(statuses)
        )
        scope.write_text(
            prefix + "## Phase summary\n\n" + summary + "\n",
            encoding="utf-8",
        )
        if "Next" in statuses:
            next_phase = statuses.index("Next")
            release_readme = self.root / "docs/release/v1.13/README.md"
            content = release_readme.read_text(encoding="utf-8")
            content = re.sub(r"Phase \d+ is next", f"Phase {next_phase} is next", content)
            release_readme.write_text(content, encoding="utf-8")

    def set_phase_topology(self, statuses: list[str]) -> None:
        """Replace both phase replicas with one contiguous fixture topology."""
        header = (
            "# Fixture\n\n"
            "**Release:** `v1.13.10 — Fixture Release`\n"
            "**Status:** Active\n"
            "**Branch:** `release/v1.13.10`\n"
        )
        phase_blocks = []
        checklist_blocks = []
        for number, status in enumerate(statuses):
            phase_blocks.append(
                f"## Phase {number} — Fixture phase {number}\n\n"
                f"**Status:** {status}\n"
            )
            checklist_blocks.append(
                f"## Phase {number}\n\n**Phase status:** {status}\n"
            )
        write(
            self.root,
            "docs/release/v1.13/v1.13.10-phase-list.md",
            header + "\n" + "\n".join(phase_blocks),
        )
        write(
            self.root,
            "docs/release/v1.13/v1.13.10-validation-checklist.md",
            header + "\n" + "\n".join(checklist_blocks),
        )
        summary = "\n".join(
            f"- Phase {number}: {status}"
            for number, status in enumerate(statuses)
        )
        write(
            self.root,
            "docs/release/v1.13/v1.13.10-scope.md",
            header + "\n## Phase summary\n\n" + summary + "\n",
        )
        if "Next" in statuses:
            next_phase = statuses.index("Next")
            write(
                self.root,
                "docs/release/v1.13/README.md",
                "# v1.13\n\n## Current Release State\n\n"
                f"v1.13.10 is active. Phase {next_phase} is next.\n",
            )

    def prepare_boundary_candidate(self) -> None:
        self.enable_lifecycle_boundaries()
        for relative in (
            "docs/release/v1.13/v1.13.10-scope.md",
            "docs/release/v1.13/v1.13.10-phase-list.md",
            "docs/release/v1.13/v1.13.10-validation-checklist.md",
        ):
            replace(
                self.root,
                relative,
                "**Status:** Active",
                "**Status:** Ready for release",
            )
        replace(
            self.root,
            "CHANGELOG.md",
            "v1.13.10 - Unreleased",
            "v1.13.10 - 2026-01-02",
        )
        replace(
            self.root,
            "README.md",
            "v1.13.10 is active",
            "v1.13.10 is ready for release",
        )
        replace(
            self.root,
            "docs/release/v1.13/README.md",
            "v1.13.10 is active",
            "v1.13.10 is ready for release",
        )
        write(
            self.root,
            "docs/release/v1.13/v1.13.10-release-gate.md",
            "# Gate\n\n"
            "**Status:** Passed — pre-publication prerequisites complete\n\n"
            "## Final verdict\n\nPASS — PRE-PUBLICATION GATE CLOSED\n",
        )

    def prepare_boundary_pre_release(self) -> None:
        self.prepare_boundary_candidate()
        self.set_phase_states(["Next", "Not started", "Not started"])
        replace(
            self.root,
            "docs/release/v1.13/v1.13.10-release-gate.md",
            "Passed — pre-publication prerequisites complete",
            "Passed — pre-merge prerequisites complete",
        )

    def publish_boundary_pending(self) -> str:
        self.prepare_boundary_candidate()
        self.commit()
        published = run_process(
            [resolved_executable("git"), "-C", str(self.root), "rev-parse", "HEAD"],
            check=True,
        ).stdout.strip()
        git(self.root, "tag", "-a", "v1.13.10", "-m", "tag")
        for relative in (
            "docs/release/v1.13/v1.13.10-scope.md",
            "docs/release/v1.13/v1.13.10-phase-list.md",
            "docs/release/v1.13/v1.13.10-validation-checklist.md",
        ):
            replace(
                self.root,
                relative,
                "**Status:** Ready for release",
                "**Status:** Published; post-publication closure pending",
            )
        replace(
            self.root,
            "README.md",
            "v1.13.10 is ready for release",
            "v1.13.10 is published; post-publication closure pending",
        )
        replace(
            self.root,
            "docs/release/v1.13/README.md",
            "v1.13.10 is ready for release",
            "v1.13.10 is published; post-publication closure pending",
        )
        self.set_phase_states(["Complete", "Complete", "Next"])
        write(
            self.root,
            "docs/release/v1.13/v1.13.10-release-gate.md",
            "# Gate\n\n"
            "**Status:** Passed and released — closure pending\n\n"
            "## Final verdict\n\nPASS — PUBLICATION COMPLETE; CLOSURE PENDING\n",
        )
        self.commit()
        return published

    def close_boundary_release(self) -> str:
        published = self.publish_boundary_pending()
        for relative in (
            "docs/release/v1.13/v1.13.10-scope.md",
            "docs/release/v1.13/v1.13.10-phase-list.md",
            "docs/release/v1.13/v1.13.10-validation-checklist.md",
        ):
            replace(
                self.root,
                relative,
                "**Status:** Published; post-publication closure pending",
                "**Status:** Released and operationally closed",
            )
        replace(
            self.root,
            "README.md",
            "v1.13.10 is published; post-publication closure pending",
            "v1.13.10 is published and operationally closed",
        )
        replace(
            self.root,
            "docs/release/v1.13/README.md",
            "v1.13.10 is published; post-publication closure pending",
            "v1.13.10 is published and operationally closed",
        )
        self.set_phase_states(["Complete", "Complete", "Complete"])
        write(
            self.root,
            "docs/release/v1.13/v1.13.10-release-gate.md",
            "# Gate\n\n**Status:** Passed and released\n\n"
            "## Final verdict\n\nPASS — PUBLISHED AND OPERATIONALLY CLOSED\n",
        )
        self.commit()
        return published

    def close_post_release(self) -> None:
        for relative in (
            "docs/release/v1.13/v1.13.10-scope.md",
            "docs/release/v1.13/v1.13.10-phase-list.md",
            "docs/release/v1.13/v1.13.10-validation-checklist.md",
        ):
            replace(
                self.root,
                relative,
                "**Status:** Ready for release",
                "**Status:** Released and operationally closed",
            )
        replace(
            self.root,
            "README.md",
            "v1.13.10 is ready for release",
            "v1.13.10 is published and operationally closed",
        )
        replace(
            self.root,
            "docs/release/v1.13/README.md",
            "v1.13.10 is ready for release",
            "v1.13.10 is published and operationally closed",
        )
        write(
            self.root,
            "docs/release/v1.13/v1.13.10-release-gate.md",
            "# Gate\n\n**Status:** Passed and released\n\n"
            "## Final verdict\n\nPASS — PUBLISHED AND OPERATIONALLY CLOSED\n",
        )

    def publish_then_close(self) -> str:
        self.complete_release()
        self.commit()
        published = run_process(
            [resolved_executable("git"), "-C", str(self.root), "rev-parse", "HEAD"],
            check=True,
        ).stdout.strip()
        git(self.root, "tag", "-a", "v1.13.10", "-m", "tag")
        self.close_post_release()
        self.commit()
        return published

    def write_pr_event(
        self,
        *,
        base: str = "main",
        head: str = "release/v1.13.10",
        repository: str = "fixture/coldkeep",
    ) -> Path:
        path = self.root / "event.json"
        path.write_text(
            json.dumps(
                {
                    "pull_request": {
                        "base": {"ref": base},
                        "head": {
                            "ref": head,
                            "repo": {"full_name": repository},
                        },
                    },
                },
            ),
            encoding="utf-8",
        )
        return path

    def post_release_pr_env(
        self,
        event_path: Path,
        head: str = "release/v1.13.10",
    ) -> dict[str, str]:
        return {
            "GITHUB_EVENT_NAME": "pull_request",
            "GITHUB_REF": "refs/pull/7/merge",
            "GITHUB_REF_NAME": "7/merge",
            "GITHUB_HEAD_REF": head,
            "GITHUB_EVENT_PATH": str(event_path),
            "GITHUB_REPOSITORY": "fixture/coldkeep",
        }

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

    def select_failed_predecessor(self, fixture: Fixture) -> Path:
        base = fixture.rev_parse("HEAD")
        git(fixture.root, "tag", "-a", "v1.13.9", "-m", "TEST_FIXTURE_ONLY")
        tag_object = fixture.rev_parse("refs/tags/v1.13.9")
        replace(
            fixture.root,
            "CHANGELOG.md",
            "## v1.13.8 - 2026-01-01 — Previous",
            "## v1.13.9 - 2026-01-01 [failed-publication-predecessor] — Previous",
        )
        fixture.set_phase_topology(
            ["Complete", "Complete", "Next", "Not started", "Not started", "Not started"]
        )
        write(
            fixture.root,
            "docs/release/v1.13/v1.13.10-release-state-validator-contract.md",
            "# Contract\n\n"
            "**Merge-complete phase:** 3\n"
            "**Tag/publication phase:** 4\n"
            "**Post-publication closure phase:** 5\n"
            "**Lifecycle declaration model:** immutable-transition-v1\n"
            "**Canonical repository:** fixture/coldkeep\n"
            "**Predecessor disposition model:** failed-publication-predecessor-v1\n",
        )
        path = fixture.root / "docs/release/v1.13/v1.13.10-predecessor-disposition.md"
        write(
            fixture.root,
            "docs/release/v1.13/v1.13.10-predecessor-disposition.md",
            "# Disposition\n\n"
            "<!-- coldkeep-predecessor-disposition:start -->\n"
            "```text\n"
            "SCHEMA: coldkeep-failed-publication-predecessor/v1\n"
            "SUCCESSOR_VERSION: 1.13.10\n"
            "PREDECESSOR_VERSION: 1.13.9\n"
            "CANONICAL_REPOSITORY: fixture/coldkeep\n"
            "PREDECESSOR_REF: refs/tags/v1.13.9\n"
            f"TAG_OBJECT: {tag_object}\n"
            "TAG_OBJECT_TYPE: tag\n"
            f"DIRECT_TARGET: {base}\n"
            "DIRECT_TARGET_DECLARED_TYPE: commit\n"
            "DIRECT_TARGET_ACTUAL_TYPE: commit\n"
            f"SUCCESSOR_BASE: {base}\n"
            "ORIGINAL_TAG_CI_RUN: 1\n"
            "ORIGINAL_TAG_CI_ATTEMPT: 1\n"
            "ORIGINAL_TAG_CI_CONCLUSION: failure\n"
            "TAG_CERTIFICATE: withheld\n"
            "GITHUB_RELEASE_OBSERVATION: absent-including-drafts\n"
            "GITHUB_RELEASE_OBSERVED_AT_UTC: 2026-01-02T03:04:05Z\n"
            "PUBLICATION_AUTHORITY: not-granted\n"
            "TERMINAL_CLOSURE: not-established\n"
            "PREDECESSOR_FINAL_PHASE: not-started-not-authorized\n"
            "DIAGNOSIS_ARCHIVE_BYTES: 1\n"
            f"DIAGNOSIS_ARCHIVE_SHA256: {'1' * 64}\n"
            f"DIAGNOSIS_MANIFEST_SHA256: {'2' * 64}\n"
            "```\n"
            "<!-- coldkeep-predecessor-disposition:end -->\n",
        )
        return path

    def test_01_valid_development(self) -> None:
        self.assert_ok(self.fixture().run("--state", "auto"))

    def test_selected_failed_predecessor_route_passes(self) -> None:
        fixture = self.fixture()
        self.select_failed_predecessor(fixture)
        self.assert_ok(fixture.run("--state", "development"))

    def test_selected_failed_predecessor_missing_file_is_ckrs010(self) -> None:
        fixture = self.fixture()
        self.select_failed_predecessor(fixture).unlink()
        process = fixture.run("--state", "development")
        self.assertEqual(process.returncode, 1)
        self.assertIn("[CKRS010]", process.stdout)
        self.assertNotIn("[CKRS019]", process.stdout)

    def test_unselected_disposition_file_is_ckrs019_only(self) -> None:
        fixture = self.fixture()
        write(
            fixture.root,
            "docs/release/v1.13/v1.13.10-predecessor-disposition.md",
            "TEST_FIXTURE_ONLY\n",
        )
        process = fixture.run("--state", "development")
        self.assertEqual(process.returncode, 1)
        self.assertIn("[CKRS019]", process.stdout)
        self.assertNotIn("[CKRS010]", process.stdout)

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

    def test_49_release_branch_post_release_closed(self) -> None:
        fixture = self.fixture()
        fixture.publish_then_close()
        self.assert_ok(fixture.run("--state", "auto"))
        self.assertIn("state=post-release-closed", fixture.run().stdout)

    def test_50_main_post_release_closed(self) -> None:
        fixture = self.fixture()
        fixture.publish_then_close()
        git(fixture.root, "branch", "-M", "main")
        self.assert_ok(fixture.run("--state", "auto"))

    def test_51_same_repository_post_release_pr(self) -> None:
        fixture = self.fixture()
        fixture.publish_then_close()
        event = fixture.write_pr_event()
        git(fixture.root, "checkout", "--detach")
        self.assert_ok(fixture.run(env=fixture.post_release_pr_env(event)))

    def test_52_post_release_pr_fork_rejected(self) -> None:
        fixture = self.fixture()
        fixture.publish_then_close()
        event = fixture.write_pr_event(repository="fork/coldkeep")
        git(fixture.root, "checkout", "--detach")
        self.assert_rules(
            fixture.run(env=fixture.post_release_pr_env(event)),
            ["CKRS005", "CKRS006", "CKRS007", "CKRS017"],
        )

    def test_53_post_release_pr_wrong_base_rejected(self) -> None:
        fixture = self.fixture()
        fixture.publish_then_close()
        event = fixture.write_pr_event(base="develop")
        git(fixture.root, "checkout", "--detach")
        self.assertNotEqual(
            fixture.run(env=fixture.post_release_pr_env(event)).returncode,
            0,
        )

    def test_54_post_release_pr_head_mismatch_rejected(self) -> None:
        fixture = self.fixture()
        fixture.publish_then_close()
        event = fixture.write_pr_event(head="release/v1.13.9")
        git(fixture.root, "checkout", "--detach")
        self.assertNotEqual(
            fixture.run(env=fixture.post_release_pr_env(event)).returncode,
            0,
        )

    def test_55_post_release_pr_malformed_event_rejected(self) -> None:
        fixture = self.fixture()
        fixture.publish_then_close()
        event = fixture.root / "event.json"
        event.write_text("{", encoding="utf-8")
        git(fixture.root, "checkout", "--detach")
        self.assertNotEqual(
            fixture.run(env=fixture.post_release_pr_env(event)).returncode,
            0,
        )

    def test_56_post_release_pr_missing_event_rejected(self) -> None:
        fixture = self.fixture()
        fixture.publish_then_close()
        event = fixture.root / "missing-event.json"
        git(fixture.root, "checkout", "--detach")
        self.assertNotEqual(
            fixture.run(env=fixture.post_release_pr_env(event)).returncode,
            0,
        )

    def test_57_post_release_pr_missing_repository_rejected(self) -> None:
        fixture = self.fixture()
        fixture.publish_then_close()
        event = fixture.write_pr_event()
        env = fixture.post_release_pr_env(event)
        env["GITHUB_REPOSITORY"] = ""
        git(fixture.root, "checkout", "--detach")
        self.assertNotEqual(fixture.run(env=env).returncode, 0)

    def test_58_post_release_lightweight_tag_rejected(self) -> None:
        fixture = self.fixture()
        fixture.complete_release()
        fixture.commit()
        git(fixture.root, "tag", "v1.13.10")
        fixture.close_post_release()
        fixture.commit()
        self.assert_rules(
            fixture.run("--state", "post-release-closed"),
            ["CKRS017"],
        )

    def test_59_post_release_exact_target_rejected(self) -> None:
        fixture = self.fixture()
        fixture.complete_release()
        fixture.close_post_release()
        fixture.commit()
        git(fixture.root, "tag", "-a", "v1.13.10", "-m", "tag")
        self.assert_rules(
            fixture.run("--state", "post-release-closed"),
            ["CKRS017"],
        )

    def test_60_post_release_unrelated_tag_rejected(self) -> None:
        fixture = self.fixture()
        fixture.complete_release()
        fixture.commit()
        published = run_process(
            [resolved_executable("git"), "-C", str(fixture.root), "rev-parse", "HEAD"],
            check=True,
        ).stdout.strip()
        git(fixture.root, "checkout", "--orphan", "unrelated")
        git(fixture.root, "rm", "-rf", ".")
        write(fixture.root, "unrelated", "unrelated\n")
        git(fixture.root, "add", ".")
        git(fixture.root, "commit", "-m", "unrelated")
        git(fixture.root, "tag", "-a", "v1.13.10", "-m", "tag")
        git(fixture.root, "checkout", "release/v1.13.10")
        fixture.close_post_release()
        fixture.commit()
        self.assert_rules(
            fixture.run("--state", "post-release-closed"),
            ["CKRS017"],
        )
        self.assertTrue(published)

    def test_61_post_release_gate_status_is_exact(self) -> None:
        fixture = self.fixture()
        fixture.publish_then_close()
        replace(
            fixture.root,
            "docs/release/v1.13/v1.13.10-release-gate.md",
            "Passed and released",
            "Passed — awaiting publication",
        )
        self.assert_rules(
            fixture.run("--state", "post-release-closed"),
            ["CKRS018"],
        )

    def test_62_post_release_pending_verdict_rejected(self) -> None:
        fixture = self.fixture()
        fixture.publish_then_close()
        replace(
            fixture.root,
            "docs/release/v1.13/v1.13.10-release-gate.md",
            "PASS — PUBLISHED AND OPERATIONALLY CLOSED",
            "PENDING",
        )
        self.assert_rules(
            fixture.run("--state", "post-release-closed"),
            ["CKRS018"],
        )

    def test_63_post_release_missing_tag_rejected(self) -> None:
        fixture = self.fixture()
        fixture.complete_release()
        fixture.close_post_release()
        fixture.commit()
        self.assert_rules(
            fixture.run("--state", "post-release-closed"),
            ["CKRS017"],
        )

    def test_64_post_release_wrong_branch_rejected(self) -> None:
        fixture = self.fixture()
        fixture.publish_then_close()
        git(fixture.root, "checkout", "-b", "wrong-branch")
        self.assert_rules(
            fixture.run("--state", "post-release-closed"),
            ["CKRS016"],
        )

    def test_65_post_release_empty_verdict_rejected(self) -> None:
        fixture = self.fixture()
        fixture.publish_then_close()
        replace(
            fixture.root,
            "docs/release/v1.13/v1.13.10-release-gate.md",
            "PASS — PUBLISHED AND OPERATIONALLY CLOSED",
            "",
        )
        self.assert_rules(
            fixture.run("--state", "post-release-closed"),
            ["CKRS018"],
        )

    def test_66_post_release_pr_unreadable_event_rejected(self) -> None:
        fixture = self.fixture()
        fixture.publish_then_close()
        git(fixture.root, "checkout", "--detach")
        self.assertNotEqual(
            fixture.run(env=fixture.post_release_pr_env(fixture.root)).returncode,
            0,
        )

    def test_67_post_release_pr_environment_head_mismatch_rejected(self) -> None:
        fixture = self.fixture()
        fixture.publish_then_close()
        event = fixture.write_pr_event()
        env = fixture.post_release_pr_env(event)
        env["GITHUB_HEAD_REF"] = "release/v1.13.9"
        git(fixture.root, "checkout", "--detach")
        self.assertNotEqual(fixture.run(env=env).returncode, 0)

    def test_68_git_ancestry_internal_error_fails_closed(self) -> None:
        completed = ProcessResult(
            ["git", "merge-base", "--is-ancestor"],
            128,
            "",
            "fixture ancestry failure",
        )
        with mock.patch.object(
            release_state_support,
            "run_git",
            return_value=completed,
        ):
            with self.assertRaisesRegex(
                release_state_support.InternalError,
                "fixture ancestry failure",
            ):
                release_state_support.strict_git_ancestor(
                    Path("."),
                    "ancestor",
                    "descendant",
                )


    def test_69_boundary_aware_pre_release_progression(self) -> None:
        fixture = self.fixture()
        fixture.prepare_boundary_pre_release()
        self.assert_ok(fixture.run("--state", "auto"))
        self.assertIn("state=pre-release", fixture.run().stdout)

    def test_70_merged_not_tagged_publication_phase_next(self) -> None:
        fixture = self.fixture()
        fixture.prepare_boundary_candidate()
        fixture.commit()
        git(fixture.root, "branch", "-M", "main")
        self.assert_ok(fixture.run())
        self.assertIn("state=merged-not-tagged", fixture.run().stdout)

    def test_71_merged_before_boundary_rejected(self) -> None:
        fixture = self.fixture()
        fixture.prepare_boundary_pre_release()
        fixture.commit()
        git(fixture.root, "branch", "-M", "main")
        self.assert_rules(fixture.run(), ["CKRS013", "CKRS018"])

    def test_72_boundary_aware_exact_tag_infers_tagged(self) -> None:
        fixture = self.fixture()
        fixture.prepare_boundary_candidate()
        fixture.commit()
        git(fixture.root, "tag", "-a", "v1.13.10", "-m", "tag")
        git(fixture.root, "checkout", "--detach")
        process = fixture.run()
        self.assert_ok(process)
        self.assertIn("state=tagged", process.stdout)

    def test_73_explicit_tagged_publication_phase_next(self) -> None:
        fixture = self.fixture()
        fixture.prepare_boundary_candidate()
        fixture.commit()
        git(fixture.root, "tag", "-a", "v1.13.10", "-m", "tag")
        self.assert_ok(fixture.run("--state", "tagged"))

    def test_74_boundary_contract_released_state_rejected(self) -> None:
        fixture = self.fixture()
        fixture.prepare_boundary_candidate()
        fixture.commit()
        git(fixture.root, "tag", "-a", "v1.13.10", "-m", "tag")
        self.assert_rules(
            fixture.run("--state", "released"),
            ["CKRS013", "CKRS018"],
        )

    def test_75_legacy_exact_tag_infers_released(self) -> None:
        fixture = self.fixture()
        fixture.complete_release()
        fixture.commit()
        git(fixture.root, "tag", "-a", "v1.13.10", "-m", "tag")
        process = fixture.run()
        self.assert_ok(process)
        self.assertIn("state=released", process.stdout)

    def test_76_legacy_all_complete_merged_state(self) -> None:
        fixture = self.fixture()
        fixture.complete_release()
        fixture.commit()
        git(fixture.root, "branch", "-M", "main")
        self.assert_ok(fixture.run("--state", "merged-not-tagged"))

    def test_77_post_release_pending_closure_progression(self) -> None:
        fixture = self.fixture()
        fixture.publish_boundary_pending()
        git(
            fixture.root,
            "branch",
            "-M",
            "release/v1.13.10-post-publication-closure",
        )
        process = fixture.run()
        self.assert_ok(process)
        self.assertIn("state=post-release-pending-closure", process.stdout)

    def test_78_boundary_post_release_closed_all_complete(self) -> None:
        fixture = self.fixture()
        fixture.close_boundary_release()
        git(fixture.root, "branch", "-M", "main")
        process = fixture.run()
        self.assert_ok(process)
        self.assertIn("state=post-release-closed", process.stdout)

    def test_79_multiple_next_phases_rejected(self) -> None:
        fixture = self.fixture()
        fixture.prepare_boundary_candidate()
        fixture.set_phase_states(["Complete", "Next", "Next"])
        fixture.commit()
        git(fixture.root, "branch", "-M", "main")
        self.assert_rules(fixture.run(), ["CKRS013", "CKRS018"])

    def test_80_status_gap_rejected(self) -> None:
        fixture = self.fixture()
        fixture.prepare_boundary_candidate()
        fixture.set_phase_states(["Complete", "Not started", "Next"])
        fixture.commit()
        git(fixture.root, "branch", "-M", "main")
        self.assert_rules(fixture.run(), ["CKRS013", "CKRS018"])

    def test_81_next_followed_by_complete_rejected(self) -> None:
        fixture = self.fixture()
        fixture.enable_lifecycle_boundaries()
        fixture.set_phase_states(["Next", "Complete", "Not started"])
        self.assert_rules(
            fixture.run("--state", "development"),
            ["CKRS013"],
        )

    def test_82_false_operational_closure_with_next_rejected(self) -> None:
        fixture = self.fixture()
        fixture.publish_boundary_pending()
        for relative in (
            "docs/release/v1.13/v1.13.10-scope.md",
            "docs/release/v1.13/v1.13.10-phase-list.md",
            "docs/release/v1.13/v1.13.10-validation-checklist.md",
        ):
            replace(
                fixture.root,
                relative,
                "**Status:** Published; post-publication closure pending",
                "**Status:** Released and operationally closed",
            )
        self.assert_rules(
            fixture.run("--state", "post-release-closed"),
            ["CKRS013", "CKRS018"],
        )

    def test_83_premature_passed_and_released_gate_rejected(self) -> None:
        fixture = self.fixture()
        fixture.prepare_boundary_candidate()
        write(
            fixture.root,
            "docs/release/v1.13/v1.13.10-release-gate.md",
            "# Gate\n\n**Status:** Passed and released\n\n"
            "## Final verdict\n\nPASS\n",
        )
        fixture.commit()
        git(fixture.root, "branch", "-M", "main")
        self.assert_rules(fixture.run(), ["CKRS018"])

    def test_84_missing_boundary_metadata_rejects_partial_candidate(self) -> None:
        fixture = self.fixture()
        fixture.prepare_boundary_candidate()
        write(
            fixture.root,
            "docs/release/v1.13/v1.13.10-release-state-validator-contract.md",
            "# Contract\n",
        )
        fixture.commit()
        git(fixture.root, "branch", "-M", "main")
        self.assert_rules(fixture.run(), ["CKRS013", "CKRS018"])

    def test_85_duplicate_boundary_metadata_rejected(self) -> None:
        fixture = self.fixture()
        fixture.prepare_boundary_candidate()
        relative = (
            "docs/release/v1.13/"
            "v1.13.10-release-state-validator-contract.md"
        )
        target = fixture.root / relative
        write(
            fixture.root,
            relative,
            target.read_text(encoding="utf-8")
            + "**Merge-complete phase:** 0\n",
        )
        fixture.commit()
        git(fixture.root, "branch", "-M", "main")
        self.assert_rules(
            fixture.run(),
            ["CKRS013", "CKRS018", "CKRS019"],
        )

    def test_86_missing_nonconsecutive_boundary_rejected(self) -> None:
        fixture = self.fixture()
        fixture.prepare_boundary_candidate()
        fixture.enable_lifecycle_boundaries(0, 2, 3)
        fixture.commit()
        git(fixture.root, "branch", "-M", "main")
        self.assert_rules(
            fixture.run(),
            ["CKRS019"],
        )

    def test_87_nonterminal_boundaries_rejected(self) -> None:
        fixture = self.fixture()
        fixture.prepare_boundary_candidate()
        fixture.enable_lifecycle_boundaries(0, 1, 1)
        fixture.commit()
        git(fixture.root, "branch", "-M", "main")
        self.assert_rules(
            fixture.run(),
            ["CKRS019"],
        )

    def test_88_recovery_branch_and_pr_infer_merged_state(self) -> None:
        fixture = self.fixture()
        fixture.prepare_boundary_candidate()
        fixture.commit()
        recovery = "recovery/v1.13.10-phase8-release-state"
        git(fixture.root, "branch", "-M", recovery)
        self.assert_ok(fixture.run())
        event = fixture.write_pr_event(head=recovery)
        git(fixture.root, "checkout", "--detach")
        self.assert_ok(
            fixture.run(env=fixture.post_release_pr_env(event, recovery)),
        )

    def test_89_post_publication_closure_branch_and_pr(self) -> None:
        fixture = self.fixture()
        fixture.publish_boundary_pending()
        closure = "release/v1.13.10-post-publication-closure"
        git(fixture.root, "branch", "-M", closure)
        self.assert_ok(fixture.run())
        event = fixture.write_pr_event(head=closure)
        git(fixture.root, "checkout", "--detach")
        self.assert_ok(
            fixture.run(env=fixture.post_release_pr_env(event, closure)),
        )

    def test_90_closure_pr_wrong_base_rejected(self) -> None:
        fixture = self.fixture()
        fixture.publish_boundary_pending()
        closure = "release/v1.13.10-post-publication-closure"
        event = fixture.write_pr_event(base="develop", head=closure)
        git(fixture.root, "checkout", "--detach")
        self.assertNotEqual(
            fixture.run(
                env=fixture.post_release_pr_env(event, closure),
            ).returncode,
            0,
        )


class ImmutableLifecycleTransitionTests(unittest.TestCase):
    def fixture(self) -> Fixture:
        fixture = Fixture()
        self.addCleanup(fixture.close)
        return fixture

    def assert_ok(self, process: ProcessResult) -> None:
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        self.assertEqual(process.stderr, "")

    def json_result(self, process: ProcessResult) -> dict[str, object]:
        self.assert_ok(process)
        return json.loads(process.stdout)

    def assert_fails_rule(self, process: ProcessResult, rule: str) -> None:
        self.assertEqual(process.returncode, 1, process.stdout + process.stderr)
        if process.stdout.startswith("{"):
            payload = json.loads(process.stdout)
            self.assertIn(rule, [item["rule"] for item in payload["violations"]])
        else:
            self.assertIn(f"[{rule}]", process.stdout)

    def test_v1_pre_release_json_contract(self) -> None:
        fixture = self.fixture()
        fixture.prepare_immutable_pre_release()
        payload = self.json_result(fixture.run("--state", "auto", "--json"))
        self.assertEqual(payload["state"], "pre-release")
        self.assertEqual(payload["artifact_state"], "pre-release")
        self.assertEqual(
            payload["authorization_status"], "NOT_EVALUATED_BY_VALIDATOR"
        )
        self.assertEqual(
            payload["certification_status"], "PENDING_EXTERNAL_EVIDENCE"
        )
        self.assertEqual(payload["evidence_scope"], "LOCAL_RELEASE_BRANCH")
        self.assertEqual(
            payload["outstanding_obligations"],
            list(validate_release_state.OBLIGATION_ORDER),
        )

    def test_v1_release_push_context_is_distinct(self) -> None:
        fixture = self.fixture()
        candidate = fixture.prepare_immutable_pre_release()
        base = fixture.rev_parse(f"{candidate}^")
        event = fixture.write_push_event(
            "refs/heads/release/v1.13.10", base, candidate
        )
        payload = self.json_result(
            fixture.run(
                "--json",
                env=fixture.push_env(
                    "refs/heads/release/v1.13.10", candidate, event
                ),
            )
        )
        self.assertEqual(payload["artifact_state"], "pre-release")
        self.assertEqual(
            payload["evidence_scope"],
            "GITHUB_RELEASE_PUSH_CONTEXT_CONSISTENCY",
        )

    def test_declaration_forms_fail_closed(self) -> None:
        mutations = (
            (
                "**Canonical repository:** fixture/coldkeep\n",
                "",
            ),
            (
                "**Canonical repository:** fixture/coldkeep\n",
                "**Canonical repository:** fixture/coldkeep\n"
                "**Canonical repository:** fixture/coldkeep\n",
            ),
            (
                "immutable-transition-v1",
                "immutable-transition-v2",
            ),
            (
                "fixture/coldkeep",
                "not-a-repository",
            ),
            (
                "**Lifecycle declaration model:** immutable-transition-v1",
                "**Lifecycle declaration model:** ",
            ),
        )
        for old, new in mutations:
            with self.subTest(new=new):
                fixture = self.fixture()
                fixture.prepare_boundary_pre_release()
                fixture.enable_immutable_transition()
                replace(
                    fixture.root,
                    "docs/release/v1.13/v1.13.10-release-state-validator-contract.md",
                    old,
                    new,
                )
                self.assert_fails_rule(fixture.run("--state", "pre-release"), "CKRS019")

    def test_immutable_state_requires_declaration(self) -> None:
        fixture = self.fixture()
        fixture.prepare_boundary_pre_release()
        fixture.commit()
        git(fixture.root, "branch", "-M", "main")
        self.assert_fails_rule(
            fixture.run(
                "--state", "merged-pending-final-main-certification"
            ),
            "CKRS019",
        )

    def test_local_normal_merge_auto_and_explicit_agree(self) -> None:
        fixture = self.fixture()
        _, candidate, merge = fixture.create_immutable_merge()
        auto = self.json_result(fixture.run("--json"))
        explicit = self.json_result(
            fixture.run(
                "--state",
                "merged-pending-final-main-certification",
                "--json",
            )
        )
        self.assertEqual(fixture.rev_parse("HEAD"), merge)
        self.assertEqual(
            auto["artifact_state"],
            "merged-pending-final-main-certification",
        )
        self.assertEqual(auto["artifact_state"], explicit["artifact_state"])
        self.assertEqual(auto["evidence_scope"], "LOCAL_ARTIFACT_ONLY")
        self.assertIn(
            "final-main-certification", auto["outstanding_obligations"]
        )
        unchanged = run_process(
            [
                resolved_executable("git"),
                "-C",
                str(fixture.root),
                "diff",
                "--exit-code",
                candidate,
                merge,
                "--",
                "docs/release/v1.13/v1.13.10-phase-list.md",
                "docs/release/v1.13/v1.13.10-release-gate.md",
            ],
            check=False,
        )
        self.assertEqual(unchanged.returncode, 0, unchanged.stdout + unchanged.stderr)

    def test_hosted_main_push_validates_event_and_topology(self) -> None:
        fixture = self.fixture()
        base, _, merge = fixture.create_immutable_merge()
        event = fixture.write_push_event("refs/heads/main", base, merge)
        payload = self.json_result(
            fixture.run(
                "--json",
                env=fixture.push_env("refs/heads/main", merge, event),
            )
        )
        self.assertEqual(
            payload["artifact_state"],
            "merged-pending-final-main-certification",
        )
        self.assertEqual(
            payload["evidence_scope"],
            "GITHUB_MAIN_PUSH_CONTEXT_CONSISTENCY",
        )

    def test_main_push_impersonation_matrix_fails(self) -> None:
        cases = (
            {"repository": "fork/coldkeep"},
            {"created": True},
            {"deleted": True},
            {"forced": True},
            {"before": "0" * 40},
            {"after": "f" * 40},
        )
        for case in cases:
            with self.subTest(case=case):
                fixture = self.fixture()
                base, _, merge = fixture.create_immutable_merge()
                event = fixture.write_push_event(
                    "refs/heads/main",
                    str(case.get("before", base)),
                    str(case.get("after", merge)),
                    repository=str(case.get("repository", "fixture/coldkeep")),
                    created=bool(case.get("created", False)),
                    deleted=bool(case.get("deleted", False)),
                    forced=bool(case.get("forced", False)),
                )
                env = fixture.push_env("refs/heads/main", merge, event)
                self.assert_fails_rule(fixture.run(env=env), "CKRS016")

    def test_main_push_environment_and_payload_conflicts_fail(self) -> None:
        cases = ("wrong-ref", "wrong-event", "wrong-sha", "missing", "unreadable")
        for case in cases:
            with self.subTest(case=case):
                fixture = self.fixture()
                base, _, merge = fixture.create_immutable_merge()
                event = fixture.write_push_event("refs/heads/main", base, merge)
                env = fixture.push_env("refs/heads/main", merge, event)
                if case == "wrong-ref":
                    env["GITHUB_REF"] = "refs/heads/release/v1.13.10"
                elif case == "wrong-event":
                    env["GITHUB_EVENT_NAME"] = "pull_request"
                elif case == "wrong-sha":
                    env["GITHUB_SHA"] = base
                elif case == "missing":
                    env["GITHUB_EVENT_PATH"] = ""
                else:
                    event.write_text("{", encoding="utf-8")
                self.assert_fails_rule(fixture.run(env=env), "CKRS016")

    def test_main_topology_rejects_wrong_tree_and_parent_order(self) -> None:
        fixture = self.fixture()
        _, candidate, _ = fixture.create_immutable_merge()
        write(fixture.root, "TEST_FIXTURE_ONLY.txt", "wrong merge tree\n")
        git(fixture.root, "add", "TEST_FIXTURE_ONLY.txt")
        git(fixture.root, "commit", "--amend", "--no-edit")
        self.assert_fails_rule(fixture.run(), "CKRS016")

        fixture = self.fixture()
        _, candidate, _ = fixture.create_immutable_merge()
        base = fixture.rev_parse(f"{candidate}^")
        tree = fixture.rev_parse(f"{candidate}^{{tree}}")
        bad = run_process(
            [
                resolved_executable("git"),
                "-C",
                str(fixture.root),
                "commit-tree",
                tree,
                "-p",
                candidate,
                "-p",
                base,
                "-m",
                "TEST_FIXTURE_ONLY reversed parents",
            ],
            check=True,
        ).stdout.strip()
        git(fixture.root, "reset", "--hard", bad)
        self.assert_fails_rule(fixture.run(), "CKRS016")

    def test_nonmerge_main_commit_cannot_self_certify(self) -> None:
        fixture = self.fixture()
        fixture.prepare_immutable_pre_release()
        git(fixture.root, "branch", "-M", "main")
        self.assert_fails_rule(fixture.run(), "CKRS016")

    def test_synthetic_pr_checkout_uses_payload_head_and_base(self) -> None:
        fixture = self.fixture()
        base, candidate, synthetic = fixture.create_immutable_merge()
        git(fixture.root, "checkout", "--detach", synthetic)
        event = fixture.write_strict_pr_event(base, candidate, synthetic)
        payload = self.json_result(
            fixture.run("--json", env=fixture.strict_pr_env(event, synthetic))
        )
        self.assertEqual(payload["artifact_state"], "pre-release")
        self.assertEqual(
            payload["evidence_scope"], "GITHUB_PR_EVENT_CONTEXT_CONSISTENCY"
        )
        self.assertNotEqual(synthetic, candidate)

    def test_synthetic_pr_accepts_captured_advisory_merge_metadata_form(self) -> None:
        fixture = self.fixture()
        base, candidate, synthetic = fixture.create_immutable_merge()
        git(fixture.root, "checkout", "--detach", synthetic)
        captured_metadata_literal = "403cba62924dd85de7ab07c758b4348bd2f97155"
        self.assertNotEqual(captured_metadata_literal, synthetic)
        event = fixture.write_strict_pr_event(
            base,
            candidate,
            captured_metadata_literal,
        )
        for mode in ("auto", "pre-release"):
            with self.subTest(mode=mode):
                destination = fixture.root / f"diagnostic-{mode}.json"
                environment = fixture.strict_pr_env(event, synthetic)
                inactive = fixture.run("--state", mode, "--json", env=environment)
                active = fixture.run(
                    "--state",
                    mode,
                    "--json",
                    "--diagnostic-json",
                    str(destination),
                    env=environment,
                )
                diagnostic = json.loads(destination.read_text(encoding="utf-8"))
                strict_rejectors = [
                    item["first_rejecting_predicate"]
                    for item in diagnostic["evaluations"]
                    if not item["site"].endswith("release-push")
                ]
                self.assertEqual(
                    active.returncode,
                    0,
                    f"strict rejectors={strict_rejectors}\n{active.stdout}{active.stderr}",
                )
                self.assertEqual(active.returncode, inactive.returncode)
                self.assertEqual(active.stdout, inactive.stdout)
                self.assertEqual(active.stderr, inactive.stderr)
                payload = json.loads(active.stdout)
                self.assertEqual(payload["artifact_state"], "pre-release")
                self.assertEqual(
                    payload["evidence_scope"],
                    "GITHUB_PR_EVENT_CONTEXT_CONSISTENCY",
                )
                self.assertEqual(strict_rejectors, [None, None])

    def test_synthetic_pr_merge_identity_value_matrix(self) -> None:
        cases: tuple[tuple[str, object, bool, bool], ...] = (
            ("matching-string", "synthetic", True, True),
            ("present-null", None, True, True),
            ("missing-key", None, False, False),
            ("payload-head-advisory", "candidate", True, True),
            ("payload-base-advisory", "base", True, True),
            ("absent-object-advisory", "d" * 40, True, True),
            ("empty-string", "", True, False),
            ("literal-null-string", "null", True, False),
            ("truncated-sha", "truncated", True, False),
            ("overlong-sha", "overlong", True, False),
            ("nonhex-sha", "g" * 40, True, False),
            ("zero-sha", "0" * 40, True, False),
            ("case-variant", "uppercase", True, False),
            ("whitespace-variant", "whitespace", True, False),
            ("newline-variant", "newline", True, False),
            ("boolean", True, True, False),
            ("number", 7, True, False),
            ("array", ["synthetic"], True, False),
            ("object", {"sha": "synthetic"}, True, False),
        )
        for name, value, include, accepted in cases:
            with self.subTest(case=name):
                fixture = self.fixture()
                base, candidate, synthetic = fixture.create_immutable_merge()
                git(fixture.root, "checkout", "--detach", synthetic)
                actual_value = (
                    {
                        "synthetic": synthetic,
                        "candidate": candidate,
                        "base": base,
                        "truncated": synthetic[:-1],
                        "overlong": f"{synthetic}0",
                        "uppercase": synthetic.upper(),
                        "whitespace": f" {synthetic}",
                        "newline": f"{synthetic}\n",
                    }.get(value, value)
                    if isinstance(value, str)
                    else value
                )
                if name == "absent-object-advisory":
                    self.assertNotEqual(
                        run_process(
                            [resolved_executable("git"), "-C", str(fixture.root), "cat-file", "-e", f"{actual_value}^{{commit}}"],
                            check=False,
                        ).returncode,
                        0,
                    )
                event = fixture.write_strict_pr_event(
                    base,
                    candidate,
                    actual_value,
                    include_merge_sha=include,
                )
                for mode in ("auto", "pre-release"):
                    with self.subTest(mode=mode):
                        process = fixture.run(
                            "--state",
                            mode,
                            "--json",
                            env=fixture.strict_pr_env(event, synthetic),
                        )
                        if accepted:
                            payload = self.json_result(process)
                            self.assertEqual(payload["artifact_state"], "pre-release")
                            self.assertEqual(
                                payload["evidence_scope"],
                                "GITHUB_PR_EVENT_CONTEXT_CONSISTENCY",
                            )
                            self.assertEqual(
                                payload["authorization_status"],
                                "NOT_EVALUATED_BY_VALIDATOR",
                            )
                            self.assertEqual(
                                payload["certification_status"],
                                "PENDING_EXTERNAL_EVIDENCE",
                            )
                        else:
                            self.assert_fails_rule(process, "CKRS016")

    def test_synthetic_pr_advisory_merge_objects_do_not_define_checkout(self) -> None:
        fixture = self.fixture()
        self.assertEqual(
            run_process(
                [resolved_executable("git"), "-C", str(fixture.root), "remote"],
                check=True,
            ).stdout,
            "",
        )
        self.assertFalse((fixture.root / ".git/objects/info/alternates").exists())
        base, candidate, synthetic = fixture.create_immutable_merge()
        candidate_tree = fixture.rev_parse(f"{candidate}^{{tree}}")
        base_tree = fixture.rev_parse(f"{base}^{{tree}}")
        prior_head = run_process(
            [
                resolved_executable("git"),
                "-C",
                str(fixture.root),
                "commit-tree",
                base_tree,
                "-p",
                base,
                "-m",
                "TEST_FIXTURE_ONLY prior release head",
            ],
            check=True,
        ).stdout.strip()
        prior_merge = run_process(
            [
                resolved_executable("git"),
                "-C",
                str(fixture.root),
                "commit-tree",
                base_tree,
                "-p",
                base,
                "-p",
                prior_head,
                "-m",
                "TEST_FIXTURE_ONLY prior-head merge",
            ],
            check=True,
        ).stdout.strip()
        regenerated_equivalent = run_process(
            [
                resolved_executable("git"),
                "-C",
                str(fixture.root),
                "commit-tree",
                candidate_tree,
                "-p",
                base,
                "-p",
                candidate,
                "-m",
                "TEST_FIXTURE_ONLY regenerated equivalent merge",
            ],
            check=True,
        ).stdout.strip()
        self.assertNotEqual(prior_merge, synthetic)
        self.assertNotEqual(
            run_process(
                [resolved_executable("git"), "-C", str(fixture.root), "show", "-s", "--format=%P", prior_merge],
                check=True,
            ).stdout.strip().split(),
            [base, candidate],
        )
        self.assertNotEqual(regenerated_equivalent, synthetic)
        self.assertEqual(
            run_process(
                [resolved_executable("git"), "-C", str(fixture.root), "show", "-s", "--format=%P", regenerated_equivalent],
                check=True,
            ).stdout.strip().split(),
            [base, candidate],
        )
        self.assertEqual(
            fixture.rev_parse(f"{regenerated_equivalent}^{{tree}}"),
            candidate_tree,
        )
        git(fixture.root, "checkout", "--detach", synthetic)
        for name, advisory_sha in (
            ("prior-head-merge", prior_merge),
            ("regenerated-equivalent-merge", regenerated_equivalent),
        ):
            with self.subTest(case=name):
                event = fixture.write_strict_pr_event(base, candidate, advisory_sha)
                for mode in ("auto", "pre-release"):
                    with self.subTest(mode=mode):
                        payload = self.json_result(
                            fixture.run(
                                "--state",
                                mode,
                                "--json",
                                env=fixture.strict_pr_env(event, synthetic),
                            )
                        )
                        self.assertEqual(payload["artifact_state"], "pre-release")

    def test_direct_pr_head_does_not_require_merge_identity(self) -> None:
        fixture = self.fixture()
        candidate = fixture.prepare_immutable_pre_release()
        base = fixture.rev_parse(f"{candidate}^")
        git(fixture.root, "checkout", "--detach", candidate)
        event = fixture.write_strict_pr_event(
            base,
            candidate,
            None,
            include_merge_sha=False,
        )
        for mode in ("auto", "pre-release"):
            with self.subTest(mode=mode):
                payload = self.json_result(
                    fixture.run(
                        "--state",
                        mode,
                        "--json",
                        env=fixture.strict_pr_env(event, candidate),
                    )
                )
                self.assertEqual(
                    payload["evidence_scope"],
                    "GITHUB_PR_EVENT_CONTEXT_CONSISTENCY",
                )

    def test_nullable_synthetic_pr_identity_controls_fail_closed(self) -> None:
        cases = (
            "environment-repository-wrong",
            "environment-repository-missing",
            "payload-repository-wrong",
            "payload-repository-missing",
            "base-repository-fork",
            "head-repository-fork",
            "pr-number-ref-mismatch",
            "pr-number-wrong-type",
            "full-ref-wrong",
            "short-ref-wrong",
            "head-branch-wrong",
            "base-branch-wrong",
            "runtime-sha-wrong",
            "payload-head-ref-wrong",
            "payload-base-ref-wrong",
        )
        for case in cases:
            with self.subTest(case=case):
                fixture = self.fixture()
                base, candidate, synthetic = fixture.create_immutable_merge()
                git(fixture.root, "checkout", "--detach", synthetic)
                event = fixture.write_strict_pr_event(base, candidate, None)
                env = fixture.strict_pr_env(event, synthetic)
                payload = json.loads(event.read_text(encoding="utf-8"))
                if case == "environment-repository-wrong":
                    env["GITHUB_REPOSITORY"] = "fork/coldkeep"
                elif case == "environment-repository-missing":
                    env["GITHUB_REPOSITORY"] = ""
                elif case == "payload-repository-wrong":
                    payload["repository"] = {"full_name": "fork/coldkeep"}
                elif case == "payload-repository-missing":
                    del payload["repository"]
                elif case == "base-repository-fork":
                    payload["pull_request"]["base"]["repo"]["full_name"] = "fork/coldkeep"
                elif case == "head-repository-fork":
                    payload["pull_request"]["head"]["repo"]["full_name"] = "fork/coldkeep"
                elif case == "pr-number-ref-mismatch":
                    payload["number"] = 8
                elif case == "pr-number-wrong-type":
                    payload["number"] = "7"
                elif case == "full-ref-wrong":
                    env["GITHUB_REF"] = "refs/pull/8/merge"
                elif case == "short-ref-wrong":
                    env["GITHUB_REF_NAME"] = "8/merge"
                elif case == "head-branch-wrong":
                    env["GITHUB_HEAD_REF"] = "release/v1.13.9"
                elif case == "base-branch-wrong":
                    env["GITHUB_BASE_REF"] = "develop"
                elif case == "runtime-sha-wrong":
                    env["GITHUB_SHA"] = candidate
                elif case == "payload-head-ref-wrong":
                    payload["pull_request"]["head"]["ref"] = "release/v1.13.9"
                else:
                    payload["pull_request"]["base"]["ref"] = "develop"
                event.write_text(json.dumps(payload), encoding="utf-8")
                for mode in ("auto", "pre-release"):
                    with self.subTest(mode=mode):
                        process = fixture.run("--state", mode, env=env)
                        if case == "head-branch-wrong" and mode == "auto":
                            self.assertEqual(process.returncode, 2)
                            self.assertIn(
                                "unable to infer release lifecycle",
                                process.stderr,
                            )
                        else:
                            self.assert_fails_rule(process, "CKRS016")

    def test_nullable_synthetic_pr_event_shape_controls_fail_closed(self) -> None:
        cases = (
            "missing-event-path",
            "unreadable-event-path",
            "invalid-utf8",
            "malformed-json",
            "non-object-event",
            "missing-pull-request",
            "wrong-type-pull-request",
            "missing-base-object",
            "wrong-type-base-object",
            "missing-head-object",
            "wrong-type-head-object",
        )
        for case in cases:
            with self.subTest(case=case):
                fixture = self.fixture()
                base, candidate, synthetic = fixture.create_immutable_merge()
                git(fixture.root, "checkout", "--detach", synthetic)
                event = fixture.write_strict_pr_event(base, candidate, None)
                env = fixture.strict_pr_env(event, synthetic)
                payload = json.loads(event.read_text(encoding="utf-8"))
                if case == "missing-event-path":
                    env["GITHUB_EVENT_PATH"] = ""
                elif case == "unreadable-event-path":
                    env["GITHUB_EVENT_PATH"] = str(fixture.root / "absent-event.json")
                elif case == "invalid-utf8":
                    event.write_bytes(b"\xff")
                elif case == "malformed-json":
                    event.write_text("{", encoding="utf-8")
                elif case == "non-object-event":
                    event.write_text("[]", encoding="utf-8")
                elif case == "missing-pull-request":
                    del payload["pull_request"]
                    event.write_text(json.dumps(payload), encoding="utf-8")
                elif case == "wrong-type-pull-request":
                    payload["pull_request"] = []
                    event.write_text(json.dumps(payload), encoding="utf-8")
                elif case == "missing-base-object":
                    del payload["pull_request"]["base"]
                    event.write_text(json.dumps(payload), encoding="utf-8")
                elif case == "wrong-type-base-object":
                    payload["pull_request"]["base"] = []
                    event.write_text(json.dumps(payload), encoding="utf-8")
                elif case == "missing-head-object":
                    del payload["pull_request"]["head"]
                    event.write_text(json.dumps(payload), encoding="utf-8")
                else:
                    payload["pull_request"]["head"] = []
                    event.write_text(json.dumps(payload), encoding="utf-8")
                for mode in ("auto", "pre-release"):
                    with self.subTest(mode=mode):
                        self.assert_fails_rule(
                            fixture.run("--state", mode, env=env),
                            "CKRS016",
                        )

    def test_nullable_synthetic_pr_object_and_topology_controls_fail_closed(self) -> None:
        cases = (
            "absent-head-object",
            "absent-base-object",
            "existing-wrong-head-object",
            "existing-wrong-base-object",
            "wrong-parent-count",
            "three-parent-count",
            "reversed-parent-order",
            "wrong-tree",
            "checkout-runtime-conflict",
        )
        for case in cases:
            with self.subTest(case=case):
                fixture = self.fixture()
                base, candidate, synthetic = fixture.create_immutable_merge()
                checked_out = synthetic
                event = fixture.write_strict_pr_event(
                    base,
                    candidate,
                    "403cba62924dd85de7ab07c758b4348bd2f97155",
                )
                payload = json.loads(event.read_text(encoding="utf-8"))
                if case == "absent-head-object":
                    payload["pull_request"]["head"]["sha"] = "f" * 40
                elif case == "absent-base-object":
                    payload["pull_request"]["base"]["sha"] = "e" * 40
                elif case == "existing-wrong-head-object":
                    payload["pull_request"]["head"]["sha"] = base
                elif case == "existing-wrong-base-object":
                    payload["pull_request"]["base"]["sha"] = candidate
                elif case in ("wrong-parent-count", "three-parent-count", "reversed-parent-order", "wrong-tree"):
                    tree = fixture.rev_parse(f"{candidate}^{{tree}}")
                    parents = ["-p", base]
                    if case == "three-parent-count":
                        parents = ["-p", base, "-p", candidate, "-p", synthetic]
                    elif case == "reversed-parent-order":
                        parents = ["-p", candidate, "-p", base]
                    elif case == "wrong-tree":
                        write(fixture.root, "TEST_FIXTURE_ONLY.txt", "wrong tree\n")
                        git(fixture.root, "add", "TEST_FIXTURE_ONLY.txt")
                        tree = run_process(
                            [resolved_executable("git"), "-C", str(fixture.root), "write-tree"],
                            check=True,
                        ).stdout.strip()
                        parents = ["-p", base, "-p", candidate]
                    checked_out = run_process(
                        [
                            resolved_executable("git"),
                            "-C",
                            str(fixture.root),
                            "commit-tree",
                            tree,
                            *parents,
                            "-m",
                            f"TEST_FIXTURE_ONLY {case}",
                        ],
                        check=True,
                    ).stdout.strip()
                else:
                    checked_out = candidate
                event.write_text(json.dumps(payload), encoding="utf-8")
                git(fixture.root, "checkout", "--detach", checked_out)
                env = fixture.strict_pr_env(event, synthetic if case == "checkout-runtime-conflict" else checked_out)
                for mode in ("auto", "pre-release"):
                    with self.subTest(mode=mode):
                        self.assert_fails_rule(
                            fixture.run("--state", mode, env=env),
                            "CKRS016",
                        )

    def test_nullable_synthetic_pr_context_impersonation_is_rejected(self) -> None:
        fixture = self.fixture()
        base, candidate, synthetic = fixture.create_immutable_merge()
        git(fixture.root, "checkout", "--detach", synthetic)
        event = fixture.write_strict_pr_event(base, candidate, None)
        env = fixture.strict_pr_env(event, synthetic)
        env.update(
            {
                "GITHUB_EVENT_NAME": "push",
                "GITHUB_REF": "refs/heads/main",
                "GITHUB_REF_NAME": "main",
                "GITHUB_HEAD_REF": "",
                "GITHUB_BASE_REF": "",
            }
        )
        for mode in ("auto", "merged-pending-final-main-certification"):
            with self.subTest(mode=mode):
                self.assert_fails_rule(
                    fixture.run("--state", mode, env=env),
                    "CKRS016",
                )

    def test_synthetic_pr_fork_and_wrong_head_fail(self) -> None:
        for forked, wrong_head in ((True, False), (False, True)):
            with self.subTest(forked=forked, wrong_head=wrong_head):
                fixture = self.fixture()
                base, candidate, synthetic = fixture.create_immutable_merge()
                git(fixture.root, "checkout", "--detach", synthetic)
                event = fixture.write_strict_pr_event(
                    base,
                    "f" * 40 if wrong_head else candidate,
                    synthetic,
                    head_repository="fork/coldkeep" if forked else None,
                )
                self.assert_fails_rule(
                    fixture.run(env=fixture.strict_pr_env(event, synthetic)),
                    "CKRS016",
                )

    def test_annotated_tag_is_pending_not_published(self) -> None:
        fixture = self.fixture()
        _, _, merge = fixture.create_immutable_merge()
        git(fixture.root, "tag", "-a", "v1.13.10", "-m", "TEST_FIXTURE_ONLY")
        payload = self.json_result(fixture.run("--json"))
        self.assertEqual(
            payload["artifact_state"], "tagged-pending-tag-certification"
        )
        self.assertEqual(payload["evidence_scope"], "LOCAL_TAG_ARTIFACT_ONLY")
        self.assertEqual(fixture.rev_parse("v1.13.10^{}"), merge)
        self.assertIn("tag-certification", payload["outstanding_obligations"])

    def test_tag_creation_event_distinguishes_object_and_commit(self) -> None:
        fixture = self.fixture()
        _, _, merge = fixture.create_immutable_merge()
        git(fixture.root, "tag", "-a", "v1.13.10", "-m", "TEST_FIXTURE_ONLY")
        tag_object = fixture.rev_parse("refs/tags/v1.13.10")
        self.assertNotEqual(tag_object, merge)
        for runtime_sha, event_after in (
            (merge, merge),
            (merge, tag_object),
            (tag_object, merge),
            (tag_object, tag_object),
        ):
            with self.subTest(runtime_sha=runtime_sha, event_after=event_after):
                event = fixture.write_push_event(
                    "refs/tags/v1.13.10",
                    "0" * 40,
                    event_after,
                    created=True,
                )
                process = fixture.run(
                "--json",
                env=fixture.push_env(
                    "refs/tags/v1.13.10",
                    runtime_sha,
                    event,
                    ref_type="tag",
                    ),
                )
                self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
                self.assertEqual(
                    self.json_result(process)["evidence_scope"],
                    "GITHUB_TAG_EVENT_CONTEXT_CONSISTENCY",
                )

    def test_tag_context_rejects_isolation_overrides(self) -> None:
        for case in ("shallow", "alternates", "grafts", "replacements"):
            with self.subTest(case=case):
                fixture = self.fixture()
                base, candidate, merge = fixture.create_immutable_merge()
                git(fixture.root, "tag", "-a", "v1.13.10", "-m", "TEST_FIXTURE_ONLY")
                tag_object = fixture.rev_parse("refs/tags/v1.13.10")
                event = fixture.write_push_event(
                    "refs/tags/v1.13.10", "0" * 40, tag_object, created=True
                )
                if case == "shallow":
                    admin = run_process(
                        [resolved_executable("git"), "-C", str(fixture.root), "rev-parse", "--git-path", "shallow"],
                        check=True,
                    ).stdout.strip()
                    shallow = Path(admin) if Path(admin).is_absolute() else fixture.root / admin
                    shallow.write_text(base + "\n", encoding="ascii")
                elif case == "alternates":
                    alternate_store = fixture.root / "TEST_FIXTURE_ONLY-alternate-objects"
                    alternate_store.mkdir()
                    admin = run_process(
                        [resolved_executable("git"), "-C", str(fixture.root), "rev-parse", "--git-path", "objects/info/alternates"],
                        check=True,
                    ).stdout.strip()
                    alternates = Path(admin) if Path(admin).is_absolute() else fixture.root / admin
                    alternates.parent.mkdir(parents=True, exist_ok=True)
                    alternates.write_text(str(alternate_store) + "\n", encoding="utf-8")
                elif case == "grafts":
                    admin = run_process(
                        [resolved_executable("git"), "-C", str(fixture.root), "rev-parse", "--git-path", "info/grafts"],
                        check=True,
                    ).stdout.strip()
                    grafts = Path(admin) if Path(admin).is_absolute() else fixture.root / admin
                    grafts.parent.mkdir(parents=True, exist_ok=True)
                    grafts.write_text(f"{candidate} {base}\n", encoding="ascii")
                else:
                    git(fixture.root, "replace", candidate, base)
                self.assert_fails_rule(
                    fixture.run(
                        env=fixture.push_env(
                            "refs/tags/v1.13.10", tag_object, event, ref_type="tag"
                        )
                    ),
                    "CKRS016",
                )

    def test_tag_context_rejects_alternate_same_target_tag(self) -> None:
        fixture = self.fixture()
        _, _, merge = fixture.create_immutable_merge()
        git(fixture.root, "tag", "-a", "v1.13.10", "-m", "TEST_FIXTURE_ONLY")
        git(fixture.root, "tag", "-a", "v1.13.11", "-m", "TEST_FIXTURE_ONLY", merge)
        alternate = fixture.rev_parse("refs/tags/v1.13.11")
        event = fixture.write_push_event(
            "refs/tags/v1.13.10", "0" * 40, alternate, created=True
        )
        self.assert_fails_rule(
            fixture.run(
                env=fixture.push_env(
                    "refs/tags/v1.13.10", alternate, event, ref_type="tag"
                )
            ),
            "CKRS016",
        )

    def test_tag_context_rejects_nested_selected_tag(self) -> None:
        fixture = self.fixture()
        _, _, merge = fixture.create_immutable_merge()
        git(fixture.root, "tag", "-a", "v1.13.9", "-m", "TEST_FIXTURE_ONLY", merge)
        git(fixture.root, "tag", "-a", "v1.13.10", "-m", "TEST_FIXTURE_ONLY", "v1.13.9")
        tag_object = fixture.rev_parse("refs/tags/v1.13.10")
        event = fixture.write_push_event(
            "refs/tags/v1.13.10", "0" * 40, tag_object, created=True
        )
        self.assert_fails_rule(
            fixture.run(
                env=fixture.push_env(
                    "refs/tags/v1.13.10", tag_object, event, ref_type="tag"
                )
            ),
            "CKRS016",
        )

    def test_lightweight_tag_rejected_for_pending_state(self) -> None:
        fixture = self.fixture()
        fixture.create_immutable_merge()
        git(fixture.root, "tag", "v1.13.10")
        self.assert_fails_rule(
            fixture.run(
                "--state", "tagged-pending-tag-certification"
            ),
            "CKRS017",
        )

    def test_closure_pending_and_candidate_are_conditional(self) -> None:
        fixture = self.fixture()
        fixture.publish_boundary_pending()
        fixture.enable_immutable_transition()
        fixture.commit()
        pending = self.json_result(fixture.run("--json"))
        self.assertEqual(
            pending["artifact_state"], "post-release-pending-closure"
        )
        fixture.set_phase_states(["Complete", "Complete", "Complete"])
        fixture.commit()
        candidate = self.json_result(fixture.run("--json"))
        self.assertEqual(
            candidate["artifact_state"], "post-release-closure-candidate"
        )
        self.assertEqual(
            candidate["certification_status"], "PENDING_EXTERNAL_EVIDENCE"
        )
        self.assertEqual(
            candidate["outstanding_obligations"],
            ["protected-closure-certification", "phase19t-terminal-audit"],
        )

    def test_first_closure_main_projection_needs_no_future_receipt(self) -> None:
        fixture = self.fixture()
        published = fixture.publish_boundary_pending()
        fixture.enable_immutable_transition()
        fixture.commit()
        git(fixture.root, "branch", "-f", "main", published)
        git(fixture.root, "checkout", "main")
        git(
            fixture.root,
            "merge",
            "--no-ff",
            "release/v1.13.10",
            "-m",
            "TEST_FIXTURE_ONLY closure merge",
        )
        merge = fixture.rev_parse("HEAD")
        event = fixture.write_push_event("refs/heads/main", published, merge)
        payload = self.json_result(
            fixture.run(
                "--json",
                env=fixture.push_env("refs/heads/main", merge, event),
            )
        )
        self.assertEqual(
            payload["artifact_state"], "post-release-pending-closure"
        )
        self.assertEqual(
            payload["evidence_scope"],
            "GITHUB_CLOSURE_MAIN_CONTEXT_CONSISTENCY",
        )
        self.assertEqual(
            payload["certification_status"], "PENDING_EXTERNAL_EVIDENCE"
        )

    def test_v1_terminal_closure_bypass_is_rejected(self) -> None:
        fixture = self.fixture()
        fixture.publish_boundary_pending()
        fixture.enable_immutable_transition()
        fixture.set_phase_states(["Complete", "Complete", "Complete"])
        self.assert_fails_rule(
            fixture.run("--state", "post-release-closed"),
            "CKRS019",
        )

    def test_candidate_gate_literal_cannot_project_closure(self) -> None:
        fixture = self.fixture()
        fixture.publish_boundary_pending()
        fixture.enable_immutable_transition()
        fixture.set_phase_states(["Complete", "Complete", "Complete"])
        replace(
            fixture.root,
            "docs/release/v1.13/v1.13.10-release-gate.md",
            "Passed and released — closure pending",
            "Passed — pre-merge prerequisites complete",
        )
        self.assert_fails_rule(
            fixture.run("--state", "post-release-closure-candidate"),
            "CKRS018",
        )

    def test_failure_json_keeps_non_authorizing_vocabulary(self) -> None:
        fixture = self.fixture()
        fixture.prepare_immutable_pre_release()
        replace(
            fixture.root,
            "docs/release/v1.13/v1.13.10-release-state-validator-contract.md",
            "fixture/coldkeep",
            "malformed",
        )
        process = fixture.run("--state", "pre-release", "--json")
        self.assertEqual(process.returncode, 1)
        payload = json.loads(process.stdout)
        self.assertEqual(payload["artifact_state"], "pre-release")
        self.assertEqual(
            payload["authorization_status"], "NOT_EVALUATED_BY_VALIDATOR"
        )
        self.assertEqual(
            payload["certification_status"], "PENDING_EXTERNAL_EVIDENCE"
        )
        self.assertEqual(
            payload["evidence_scope"], "LEGACY_STRUCTURAL_CONTEXT"
        )


class DiagnosticCaptureTests(unittest.TestCase):
    """Prove opt-in diagnostics observe without changing acceptance."""

    def fixture(self) -> Fixture:
        fixture = Fixture()
        self.addCleanup(fixture.close)
        return fixture

    def output_path(self, name: str = "diagnostic.json") -> Path:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        return Path(directory.name) / name

    @staticmethod
    def load_diagnostic(path: Path) -> dict[str, object]:
        return json.loads(path.read_text(encoding="utf-8"))

    @staticmethod
    def predicate(evaluation: dict[str, object], predicate_id: str) -> dict[str, object]:
        return next(
            item
            for item in evaluation["predicates"]
            if item["id"] == predicate_id
        )

    def test_diagnostic_contract_closes_scalar_and_collection_vocabulary(self) -> None:
        contract = (
            SCRIPT.parents[1]
            / "docs/release/v1.13/v1.13.17-release-state-validator-contract.md"
        ).read_text(encoding="utf-8")
        normalized_contract = " ".join(contract.split())
        required_literals = (
            "Accepted exposed strings are limited to these finite families",
            "No other category, exposed scalar, identity type, or identity key is valid.",
            "`checkout_parents` has cardinality 0 before observation",
            "preserves the complete ordered Git parent list",
            "Records occur uniquely in `base`, `head` order",
            "Complete evaluation predicate cardinality is 32 for release PR, 5",
            "for release push, and 34 for tag push.",
            "original member count, with no producer maximum",
            "up to four snapshots are truthful repeated reads",
            "failed artifact has no producer maximum",
            "`inference-tag-push`, `ckrs016-tag-push`",
            "`explicit-state-tag-push`, `ckrs016-tag-push`",
            "`EXACT_COMMIT_M`",
            "`EXACT_NAMED_TAG_A`",
        )
        for literal in required_literals:
            with self.subTest(literal=literal):
                self.assertIn(literal, normalized_contract)
        expected_pr = (
            "pr.env.actions", "pr.env.event", "pr.env.full_ref",
            "pr.env.short_ref", "pr.env.head_ref", "pr.env.base_ref",
            "pr.env.repository", "pr.env.runtime_sha", "pr.env.event_path",
            "pr.event.read", "pr.event.utf8", "pr.event.json",
            "pr.event.object", "pr.payload.pull_request", "pr.payload.number",
            "pr.payload.number_ref", "pr.payload.repository",
            "pr.payload.base_object", "pr.payload.head_object",
            "pr.payload.base_ref", "pr.payload.head_ref",
            "pr.payload.base_repository", "pr.payload.head_repository",
            "pr.payload.base_sha", "pr.payload.head_sha",
            "pr.git.base_object", "pr.git.head_object", "pr.route",
            "pr.payload.merge_member", "pr.payload.merge_value",
            "pr.git.parents", "pr.git.tree",
        )
        expected_release_push = (
            "release_push.event.read", "release_push.event.utf8",
            "release_push.event.json", "release_push.event.object",
            "release_push.context",
        )
        self.assertEqual(
            tuple(item[0] for item in release_state_support.PR_PREDICATES),
            expected_pr,
        )
        self.assertEqual(
            tuple(item[0] for item in release_state_support.RELEASE_PUSH_PREDICATES),
            expected_release_push,
        )
        for predicate_id in (*expected_pr, *expected_release_push):
            self.assertIn(f"`{predicate_id}`", contract)

    def run_active(
        self,
        fixture: Fixture,
        event: Path,
        checkout: str,
        *,
        mode: str = "auto",
        output: Path | None = None,
        env_updates: dict[str, str] | None = None,
    ) -> tuple[ProcessResult, Path, dict[str, object]]:
        destination = output or self.output_path()
        env = fixture.strict_pr_env(event, checkout)
        env.update({
            "GITHUB_RUN_ID": "7001",
            "GITHUB_RUN_ATTEMPT": "1",
            "GITHUB_JOB": "quality",
            "GITHUB_WORKFLOW": "CI",
        })
        if env_updates:
            env.update(env_updates)
        process = fixture.run(
            "--state",
            mode,
            "--diagnostic-json",
            str(destination),
            env=env,
        )
        return process, destination, self.load_diagnostic(destination)

    def test_tag_v2_inventory_and_site_attribution(self) -> None:
        expected_ids = [
            "tag.env.actions", "tag.env.event", "tag.env.full_ref",
            "tag.env.short_ref", "tag.env.ref_type", "tag.env.repository",
            "tag.env.runtime_sha_format", "tag.env.event_path",
            "tag.event.read", "tag.event.size", "tag.event.utf8",
            "tag.event.json", "tag.event.object", "tag.payload.ref",
            "tag.payload.before", "tag.payload.after_format",
            "tag.payload.created", "tag.payload.deleted",
            "tag.payload.forced", "tag.payload.repository",
            "tag.git.not_shallow", "tag.git.no_alternates",
            "tag.git.no_grafts", "tag.git.no_replacements",
            "tag.git.named_ref", "tag.git.named_ref_type",
            "tag.git.direct_target", "tag.git.direct_target_declared_type",
            "tag.git.direct_target_actual_type", "tag.git.target_equals_head",
            "tag.identity.runtime_sha", "tag.identity.event_after",
            "tag.topology.parents_and_ancestry", "tag.topology.tree",
        ]
        for mode, expected_sites in (
            ("auto", ["inference-tag-push", "ckrs016-tag-push"]),
            (
                "tagged-pending-tag-certification",
                ["explicit-state-tag-push", "ckrs016-tag-push"],
            ),
        ):
            with self.subTest(mode=mode):
                fixture = self.fixture()
                _, _, merge = fixture.create_immutable_merge()
                git(fixture.root, "tag", "-a", "v1.13.10", "-m", "TEST_FIXTURE_ONLY")
                tag_object = fixture.rev_parse("refs/tags/v1.13.10")
                event = fixture.write_push_event(
                    "refs/tags/v1.13.10", "0" * 40, tag_object, created=True
                )
                destination = self.output_path(f"tag-{mode}.json")
                environment = fixture.push_env(
                    "refs/tags/v1.13.10",
                    tag_object,
                    event,
                    ref_type="tag",
                )
                environment.update({
                    "GITHUB_RUN_ID": "7002",
                    "GITHUB_RUN_ATTEMPT": "1",
                    "GITHUB_JOB": "quality",
                    "GITHUB_WORKFLOW": "CI",
                })
                process = fixture.run(
                    "--state", mode,
                    "--diagnostic-json", str(destination),
                    env=environment,
                )
                self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
                diagnostic = self.load_diagnostic(destination)
                self.assertEqual(
                    diagnostic["schema"],
                    "coldkeep-release-state-diagnostic/v3",
                )
                evaluations = diagnostic["evaluations"]
                self.assertEqual([item["site"] for item in evaluations], expected_sites)
                self.assertEqual([item["event_snapshot_id"] for item in evaluations], [1, 2])
                for evaluation in evaluations:
                    self.assertEqual(evaluation["route"], "tag-push")
                    self.assertEqual(evaluation["result"], "PASS")
                    self.assertIsNone(evaluation["first_rejecting_predicate"])
                    self.assertEqual(
                        [item["id"] for item in evaluation["predicates"]],
                        expected_ids,
                    )
                    self.assertTrue(
                        all(item["result"] == "PASS" for item in evaluation["predicates"])
                    )
                    context = evaluation["runtime_context"]
                    self.assertEqual(
                        context["runtime_sha_classification"]["value"],
                        "EXACT_NAMED_TAG_A",
                    )
                    self.assertEqual(
                        context["event_after_classification"]["value"],
                        "EXACT_NAMED_TAG_A",
                    )

    def test_tag_identity_classification_vocabulary_matrix(self) -> None:
        fixture = self.fixture()
        _, _, merge = fixture.create_immutable_merge()
        git(fixture.root, "tag", "-a", "v1.13.10", "-m", "TEST_FIXTURE_ONLY")
        tag_object = fixture.rev_parse("refs/tags/v1.13.10")
        expected = {
            merge: "EXACT_COMMIT_M",
            tag_object: "EXACT_NAMED_TAG_A",
        }
        for runtime_sha, event_after in (
            (merge, merge), (merge, tag_object),
            (tag_object, merge), (tag_object, tag_object),
        ):
            with self.subTest(runtime_sha=runtime_sha, event_after=event_after):
                event = fixture.write_push_event(
                    "refs/tags/v1.13.10", "0" * 40, event_after, created=True
                )
                destination = self.output_path(
                    f"classification-{runtime_sha[:6]}-{event_after[:6]}.json"
                )
                process = fixture.run(
                    "--state", "tagged-pending-tag-certification",
                    "--diagnostic-json", str(destination),
                    env=fixture.push_env(
                        "refs/tags/v1.13.10", runtime_sha, event, ref_type="tag"
                    ),
                )
                self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
                diagnostic = self.load_diagnostic(destination)
                self.assertEqual(diagnostic["validator"]["result"], "ok")
                for evaluation in diagnostic["evaluations"]:
                    context = evaluation["runtime_context"]
                    self.assertEqual(
                        context["runtime_sha_classification"]["value"],
                        expected[runtime_sha],
                    )
                    self.assertEqual(
                        context["event_after_classification"]["value"],
                        expected[event_after],
                    )

    def test_tag_event_size_limit_stops_before_decode(self) -> None:
        fixture = self.fixture()
        fixture.create_immutable_merge()
        git(fixture.root, "tag", "-a", "v1.13.10", "-m", "TEST_FIXTURE_ONLY")
        tag_object = fixture.rev_parse("refs/tags/v1.13.10")
        event = fixture.root / "event.json"
        event.write_bytes(b"{" + b"x" * 1048576)
        destination = self.output_path("oversized-tag.json")
        environment = fixture.push_env(
            "refs/tags/v1.13.10", tag_object, event, ref_type="tag"
        )
        process = fixture.run(
            "--state", "tagged-pending-tag-certification",
            "--diagnostic-json", str(destination),
            env=environment,
        )
        self.assertEqual(process.returncode, 1)
        diagnostic = self.load_diagnostic(destination)
        self.assertEqual(diagnostic["validator"]["result"], "error")
        for evaluation in diagnostic["evaluations"]:
            snapshot = diagnostic["event_snapshots"][
                evaluation["event_snapshot_id"] - 1
            ]
            self.assertIsNone(snapshot["size_bytes"])
            self.assertIsNone(snapshot["content_sha256"])
            self.assertEqual(snapshot["size_limit_status"], "FAIL")
            self.assertEqual(snapshot["utf8_status"], "NOT_EVALUATED")
            self.assertEqual(snapshot["json_status"], "NOT_EVALUATED")
            self.assertEqual(
                set(snapshot["projection"]),
                {"ref", "before", "after", "created", "deleted", "forced", "repository"},
            )
            predicate = self.predicate(evaluation, "tag.event.size")
            self.assertEqual(predicate["result"], "FAIL")
            self.assertEqual(
                self.predicate(evaluation, "tag.event.utf8")["result"],
                "NOT_EVALUATED",
            )

    def test_tag_event_limit_has_truthful_whole_event_measurements(self) -> None:
        limit = 1048576
        common_oversize_prefix = b"x" * (limit + 1)
        cases = (
            ("limit", b"{}" + b" " * (limit - 2), True),
            ("limit-plus-one", b"x" * (limit + 1), False),
            ("limit-plus-two", b"x" * (limit + 2), False),
            ("large-a", common_oversize_prefix + b"a" * (limit + 16), False),
            ("large-b", common_oversize_prefix + b"b" * (limit + 16), False),
        )
        oversized_snapshots = []
        for name, raw, accepted in cases:
            with self.subTest(name=name):
                temporary = tempfile.TemporaryDirectory()
                self.addCleanup(temporary.cleanup)
                event = Path(temporary.name) / "event.json"
                event.write_bytes(raw)
                recorder = release_state_support.DiagnosticRecorder("auto", {})
                evaluation = recorder.begin_evaluation(
                    "TEST_FIXTURE_ONLY-tag-push",
                    {},
                    "1" * 40,
                    "",
                    "1.13.10",
                    "fixture/coldkeep",
                )
                original_open = Path.open
                reads: list[tuple[int, int]] = []

                class ObservedReader:
                    def __init__(self, handle: object) -> None:
                        self.handle = handle

                    def __enter__(self) -> "ObservedReader":
                        self.handle.__enter__()
                        return self

                    def __exit__(self, *args: object) -> object:
                        return self.handle.__exit__(*args)

                    def read(self, requested: int = -1) -> bytes:
                        returned = self.handle.read(requested)
                        reads.append((requested, len(returned)))
                        return returned

                def observed_open(selected: Path, *args: object, **kwargs: object) -> object:
                    handle = original_open(selected, *args, **kwargs)
                    return ObservedReader(handle) if selected == event else handle

                original_loads = json.loads
                with mock.patch.object(Path, "open", new=observed_open), mock.patch.object(
                    release_state_support.json, "loads", wraps=original_loads
                ) as decoder:
                    payload = release_state_support._load_event(
                        str(event),
                        recorder=recorder,
                        evaluation_id=evaluation,
                        version="1.13.10",
                        canonical_repository="fixture/coldkeep",
                        predicate_prefix="tag",
                    )
                self.assertEqual(reads, [(limit + 1, min(len(raw), limit + 1))])
                snapshot = recorder.event_snapshots[0]
                if accepted:
                    decoder.assert_called_once()
                    self.assertIsInstance(payload, dict)
                    self.assertEqual(snapshot["size_bytes"], limit)
                    self.assertEqual(
                        snapshot["content_sha256"],
                        hashlib.sha256(raw).hexdigest(),
                    )
                    self.assertEqual(snapshot["size_limit_status"], "PASS")
                else:
                    decoder.assert_not_called()
                    self.assertIsNone(payload)
                    self.assertIsNone(snapshot["size_bytes"])
                    self.assertIsNone(snapshot["content_sha256"])
                    self.assertEqual(snapshot["size_limit_status"], "FAIL")
                    self.assertEqual(snapshot["utf8_status"], "NOT_EVALUATED")
                    self.assertEqual(snapshot["json_status"], "NOT_EVALUATED")
                    oversized_snapshots.append(snapshot.copy())
        self.assertEqual(len(oversized_snapshots), 4)
        self.assertEqual(
            {
                (item["size_bytes"], item["content_sha256"])
                for item in oversized_snapshots[-2:]
            },
            {(None, None)},
        )

    def test_duplicate_parent_raw_commit_has_truthful_strict_topology_trace(self) -> None:
        fixture = self.fixture()
        _, candidate, _ = fixture.create_immutable_merge()
        tree = fixture.rev_parse(f"{candidate}^{{tree}}")
        raw = (
            f"tree {tree}\n"
            f"parent {candidate}\n"
            f"parent {candidate}\n"
            "author fixture <fixture@example.invalid> 1700000000 +0000\n"
            "committer fixture <fixture@example.invalid> 1700000000 +0000\n"
            "\nTEST_FIXTURE_ONLY duplicate parent\n"
        )
        created = subprocess.run(
            [
                resolved_executable("git"), "-C", str(fixture.root),
                "hash-object", "-t", "commit", "-w", "--stdin",
            ],
            input=raw,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        ).stdout.strip()
        stored = run_process(
            [resolved_executable("git"), "-C", str(fixture.root), "cat-file", "-p", created],
            check=True,
        ).stdout
        self.assertEqual(stored, raw)
        self.assertEqual(
            run_process(
                [resolved_executable("git"), "-C", str(fixture.root), "cat-file", "-t", created],
                check=True,
            ).stdout.strip(),
            "commit",
        )
        parents = run_process(
            [resolved_executable("git"), "-C", str(fixture.root), "rev-list", "--parents", "-n", "1", created],
            check=True,
        ).stdout.strip().split()[1:]
        self.assertEqual(parents, [candidate, candidate])
        self.assertEqual(fixture.rev_parse(f"{created}^{{tree}}"), tree)
        git(fixture.root, "checkout", "--detach", created)
        git(fixture.root, "tag", "-a", "v1.13.10", "-m", "TEST_FIXTURE_ONLY")
        tag_object = fixture.rev_parse("refs/tags/v1.13.10")
        event = fixture.write_push_event(
            "refs/tags/v1.13.10", "0" * 40, tag_object, created=True
        )
        environment = fixture.push_env(
            "refs/tags/v1.13.10", tag_object, event, ref_type="tag"
        )
        for mode in ("auto", "tagged-pending-tag-certification"):
            with self.subTest(mode=mode):
                destination = self.output_path(f"duplicate-{mode}.json")
                process = fixture.run(
                    "--state", mode,
                    "--diagnostic-json", str(destination),
                    env=environment,
                )
                self.assertEqual(process.returncode, 1, process.stdout + process.stderr)
                self.assertIn("[CKRS016]", process.stdout)
                diagnostic = self.load_diagnostic(destination)
                reached = [
                    item for item in diagnostic["evaluations"]
                    if self.predicate(
                        item, "tag.topology.parents_and_ancestry"
                    )["result"] == "FAIL"
                ]
                self.assertTrue(reached)
                for evaluation in reached:
                    self.assertEqual(
                        evaluation["runtime_context"]["checkout_parents"],
                        [candidate, candidate],
                    )
                    self.assertEqual(
                        self.predicate(evaluation, "tag.topology.tree")["result"],
                        "NOT_EVALUATED",
                    )

    def test_diagnostic_pass_equivalence_for_matching_and_null_synthetic_values(self) -> None:
        for case in ("matching", "null"):
            with self.subTest(case=case):
                fixture = self.fixture()
                base, candidate, synthetic = fixture.create_immutable_merge()
                git(fixture.root, "checkout", "--detach", synthetic)
                event = fixture.write_strict_pr_event(
                    base,
                    candidate,
                    synthetic if case == "matching" else None,
                )
                environment = fixture.strict_pr_env(event, synthetic)
                inactive = fixture.run("--state", "auto", env=environment)
                active, path, diagnostic = self.run_active(
                    fixture, event, synthetic
                )
                self.assertEqual(active.returncode, inactive.returncode)
                self.assertEqual(active.stdout, inactive.stdout)
                self.assertEqual(active.stderr, inactive.stderr)
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
                self.assertEqual(diagnostic["validator"]["result"], "ok")
                self.assertEqual(diagnostic["validator"]["exit_code"], 0)
                self.assertEqual(len(diagnostic["evaluations"]), 4)
                self.assertEqual(len(diagnostic["event_snapshots"]), 4)
                self.assertEqual(
                    [item["event_snapshot_id"] for item in diagnostic["evaluations"]],
                    [1, 2, 3, 4],
                )
                self.assertTrue(
                    all(
                        item["result"] == "PASS"
                        for item in diagnostic["evaluations"]
                        if not item["site"].endswith("release-push")
                    )
                )

    def test_different_canonical_merge_values_are_advisory_and_reach_topology(self) -> None:
        alternates = (
            "569fa4e31d031cbd1f6fdffbd44bbbb2e4813b96",
            "403cba62924dd85de7ab07c758b4348bd2f97155",
        )
        for alternate in alternates:
            with self.subTest(alternate=alternate):
                fixture = self.fixture()
                base, candidate, synthetic = fixture.create_immutable_merge()
                git(fixture.root, "checkout", "--detach", synthetic)
                event = fixture.write_strict_pr_event(base, candidate, alternate)
                environment = fixture.strict_pr_env(event, synthetic)
                inactive = fixture.run("--state", "pre-release", env=environment)
                active, _, diagnostic = self.run_active(
                    fixture,
                    event,
                    synthetic,
                    mode="pre-release",
                )
                self.assertEqual(active.returncode, 0, active.stdout + active.stderr)
                self.assertEqual(active.returncode, inactive.returncode)
                self.assertEqual(active.stdout, inactive.stdout)
                self.assertEqual(active.stderr, inactive.stderr)
                for evaluation in diagnostic["evaluations"]:
                    if evaluation["site"].endswith("release-push"):
                        continue
                    self.assertIsNone(evaluation["first_rejecting_predicate"])
                    for predicate_id in (
                        "pr.payload.merge_member",
                        "pr.payload.merge_value",
                        "pr.git.parents",
                        "pr.git.tree",
                    ):
                        self.assertEqual(
                            self.predicate(evaluation, predicate_id)["result"],
                            "PASS",
                        )
                self.assertEqual(diagnostic["validator"]["rule_codes"], [])

    def test_first_rejectors_distinguish_shape_identity_and_topology(self) -> None:
        cases = (
            ("missing-merge", "pr.payload.merge_member"),
            ("wrong-type-merge", "pr.payload.merge_value"),
            ("wrong-repository", "pr.env.repository"),
            ("malformed-json", "pr.event.json"),
        )
        for case, expected in cases:
            with self.subTest(case=case):
                fixture = self.fixture()
                base, candidate, synthetic = fixture.create_immutable_merge()
                git(fixture.root, "checkout", "--detach", synthetic)
                event = fixture.write_strict_pr_event(base, candidate, synthetic)
                env_updates: dict[str, str] = {}
                if case == "missing-merge":
                    payload = json.loads(event.read_text(encoding="utf-8"))
                    del payload["pull_request"]["merge_commit_sha"]
                    event.write_text(json.dumps(payload), encoding="utf-8")
                elif case == "wrong-type-merge":
                    payload = json.loads(event.read_text(encoding="utf-8"))
                    payload["pull_request"]["merge_commit_sha"] = {"not": "a-sha"}
                    event.write_text(json.dumps(payload), encoding="utf-8")
                elif case == "wrong-repository":
                    env_updates["GITHUB_REPOSITORY"] = "fork/coldkeep"
                else:
                    event.write_text("{", encoding="utf-8")
                process, _, diagnostic = self.run_active(
                    fixture,
                    event,
                    synthetic,
                    mode="pre-release",
                    env_updates=env_updates,
                )
                self.assertEqual(process.returncode, 1)
                for evaluation in diagnostic["evaluations"]:
                    if evaluation["site"].endswith("release-push"):
                        continue
                    self.assertEqual(evaluation["first_rejecting_predicate"], expected)
                    predicates = evaluation["predicates"]
                    reject_index = next(
                        index for index, item in enumerate(predicates)
                        if item["id"] == expected
                    )
                    self.assertTrue(
                        all(item["result"] == "PASS" for item in predicates[:reject_index])
                    )
                    self.assertEqual(predicates[reject_index]["result"], "FAIL")
                    self.assertTrue(
                        all(
                            item["result"] == "NOT_EVALUATED"
                            for item in predicates[reject_index + 1 :]
                        )
                    )

    def test_diagnostic_identity_loader_and_topology_matrix_preserves_rejection(self) -> None:
        cases = (
            ("missing-event-path", "pr.env.event_path"),
            ("unreadable-event", "pr.event.read"),
            ("invalid-utf8", "pr.event.utf8"),
            ("wrong-full-ref", "pr.env.full_ref"),
            ("number-ref-mismatch", "pr.payload.number_ref"),
            ("wrong-base-ref", "pr.payload.base_ref"),
            ("runtime-sha-conflict", "pr.env.runtime_sha"),
            ("absent-head-object", "pr.git.head_object"),
            ("reversed-parents", "pr.git.parents"),
            ("wrong-tree", "pr.git.tree"),
        )
        for case, expected in cases:
            with self.subTest(case=case):
                fixture = self.fixture()
                base, candidate, synthetic = fixture.create_immutable_merge()
                checked_out = synthetic
                event = fixture.write_strict_pr_event(base, candidate, synthetic)
                payload = json.loads(event.read_text(encoding="utf-8"))
                env_updates: dict[str, str] = {}
                if case == "missing-event-path":
                    env_updates["GITHUB_EVENT_PATH"] = ""
                elif case == "unreadable-event":
                    env_updates["GITHUB_EVENT_PATH"] = str(
                        fixture.root / "TEST_FIXTURE_ONLY-absent-event.json"
                    )
                elif case == "invalid-utf8":
                    event.write_bytes(b"\xff")
                elif case == "wrong-full-ref":
                    env_updates["GITHUB_REF"] = "refs/heads/main"
                elif case == "number-ref-mismatch":
                    payload["number"] = 8
                    event.write_text(json.dumps(payload), encoding="utf-8")
                elif case == "wrong-base-ref":
                    payload["pull_request"]["base"]["ref"] = "develop"
                    event.write_text(json.dumps(payload), encoding="utf-8")
                elif case == "runtime-sha-conflict":
                    env_updates["GITHUB_SHA"] = candidate
                elif case == "absent-head-object":
                    payload["pull_request"]["head"]["sha"] = "f" * 40
                    event.write_text(json.dumps(payload), encoding="utf-8")
                else:
                    tree = fixture.rev_parse(f"{candidate}^{{tree}}")
                    parents = ["-p", candidate, "-p", base]
                    if case == "wrong-tree":
                        write(fixture.root, "TEST_FIXTURE_ONLY.txt", "wrong tree\n")
                        git(fixture.root, "add", "TEST_FIXTURE_ONLY.txt")
                        tree = run_process(
                            [resolved_executable("git"), "-C", str(fixture.root), "write-tree"],
                            check=True,
                        ).stdout.strip()
                        parents = ["-p", base, "-p", candidate]
                    checked_out = run_process(
                        [
                            resolved_executable("git"),
                            "-C",
                            str(fixture.root),
                            "commit-tree",
                            tree,
                            *parents,
                            "-m",
                            f"TEST_FIXTURE_ONLY {case}",
                        ],
                        check=True,
                    ).stdout.strip()
                    payload["pull_request"]["merge_commit_sha"] = checked_out
                    event.write_text(json.dumps(payload), encoding="utf-8")
                git(fixture.root, "checkout", "--detach", checked_out)
                environment = fixture.strict_pr_env(event, checked_out)
                environment.update(env_updates)
                inactive = fixture.run(
                    "--state", "pre-release", env=environment
                )
                active, _, diagnostic = self.run_active(
                    fixture,
                    event,
                    checked_out,
                    mode="pre-release",
                    env_updates=env_updates,
                )
                self.assertEqual(active.returncode, inactive.returncode)
                self.assertEqual(active.stdout, inactive.stdout)
                self.assertEqual(active.stderr, inactive.stderr)
                self.assertEqual(active.returncode, 1)
                pr_evaluations = [
                    item
                    for item in diagnostic["evaluations"]
                    if not item["site"].endswith("release-push")
                ]
                self.assertTrue(pr_evaluations)
                self.assertTrue(
                    all(
                        item["first_rejecting_predicate"] == expected
                        for item in pr_evaluations
                    )
                )

    def test_direct_head_acceptance_skips_synthetic_predicates(self) -> None:
        fixture = self.fixture()
        candidate = fixture.prepare_immutable_pre_release()
        base = fixture.rev_parse(f"{candidate}^")
        git(fixture.root, "checkout", "--detach", candidate)
        event = fixture.write_strict_pr_event(
            base,
            candidate,
            None,
            include_merge_sha=False,
        )
        process, _, diagnostic = self.run_active(fixture, event, candidate)
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        for evaluation in diagnostic["evaluations"]:
            if evaluation["site"].endswith("release-push"):
                continue
            self.assertEqual(self.predicate(evaluation, "pr.route")["result"], "PASS")
            for predicate_id in (
                "pr.payload.merge_member",
                "pr.payload.merge_value",
                "pr.git.parents",
                "pr.git.tree",
            ):
                self.assertEqual(
                    self.predicate(evaluation, predicate_id)["result"],
                    "NOT_EVALUATED",
                )

    def test_event_snapshots_bind_the_bytes_consumed_by_each_evaluation(self) -> None:
        fixture = self.fixture()
        base, candidate, synthetic = fixture.create_immutable_merge()
        git(fixture.root, "checkout", "--detach", synthetic)
        event = fixture.write_strict_pr_event(base, candidate, synthetic)
        destination = self.output_path()
        environment = os.environ.copy()
        for key in GITHUB_KEYS:
            environment.pop(key, None)
        environment.update(fixture.strict_pr_env(event, synthetic))
        original_loader = release_state_support._load_event
        calls = 0

        def changing_loader(*args: object, **kwargs: object) -> object:
            nonlocal calls
            value = original_loader(*args, **kwargs)
            calls += 1
            if calls == 2:
                payload = json.loads(event.read_text(encoding="utf-8"))
                payload["TEST_FIXTURE_ONLY_EXCLUDED_FIELD"] = "changed-between-evaluations"
                event.write_text(json.dumps(payload), encoding="utf-8")
            return value

        stdout = io.StringIO()
        stderr = io.StringIO()
        with mock.patch.dict(os.environ, environment, clear=True), mock.patch.object(
            release_state_support,
            "_load_event",
            side_effect=changing_loader,
        ), redirect_stderr(stderr), mock.patch("sys.stdout", stdout):
            status = validate_release_state.main(
                [
                    "--repo-root",
                    str(fixture.root),
                    "--state",
                    "auto",
                    "--diagnostic-json",
                    str(destination),
                ]
            )
        self.assertEqual(status, 0, stdout.getvalue() + stderr.getvalue())
        diagnostic = self.load_diagnostic(destination)
        snapshots = diagnostic["event_snapshots"]
        self.assertEqual(len(snapshots), 4)
        evaluation_snapshots = {
            item["site"]: snapshots[item["event_snapshot_id"] - 1]
            for item in diagnostic["evaluations"]
        }
        self.assertNotEqual(
            evaluation_snapshots["inference"]["content_sha256"],
            evaluation_snapshots["ckrs016"]["content_sha256"],
        )
        self.assertEqual(
            [item["event_snapshot_id"] for item in diagnostic["evaluations"]],
            [1, 2, 3, 4],
        )

    def test_closed_schema_privacy_and_no_network_capture(self) -> None:
        # These values are artificial privacy canaries, never real credentials.
        markers = (
            "ARTIFICIAL_EVENT_SECRET_6f91",
            "ARTIFICIAL_ENV_SECRET_702a",
            "ARTIFICIAL_PATH_SECRET_d301",
            "ARTIFICIAL_NESTED_SECRET_49bc",
            "ARTIFICIAL_LABEL_SECRET_20dd",
        )
        fixture = self.fixture()
        base, candidate, synthetic = fixture.create_immutable_merge()
        git(fixture.root, "checkout", "--detach", synthetic)
        event = fixture.root / f"{markers[2]}.json"
        source = fixture.write_strict_pr_event(base, candidate, synthetic)
        payload = json.loads(source.read_text(encoding="utf-8"))
        payload["pull_request"]["body"] = markers[0]
        payload["sender"] = {markers[3]: markers[3]}
        payload[markers[3]] = {"nested": markers[3]}
        event.write_text(json.dumps(payload), encoding="utf-8")
        process, path, diagnostic = self.run_active(
            fixture,
            event,
            synthetic,
            env_updates={
                "GITHUB_TOKEN": markers[1],
                "GITHUB_JOB": markers[4],
                "GITHUB_WORKFLOW": markers[4],
            },
        )
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        encoded = path.read_text(encoding="utf-8")
        for marker in markers:
            self.assertNotIn(marker, encoded)
            self.assertNotIn(marker, process.stderr)
        self.assertEqual(
            set(diagnostic),
            {"schema", "created_at_utc", "validator", "attribution", "event_snapshots", "evaluations", "capture"},
        )
        self.assertEqual(
            set(diagnostic["capture"]),
            {"complete", "incomplete_reasons", "same_process", "network_lookups", "whole_event_included", "whole_environment_included", "non_authorizing"},
        )
        self.assertEqual(
            set(diagnostic["validator"]),
            {"requested_state", "resolved_state", "result", "exit_code", "rule_codes", "authorization_status", "certification_status", "checkout_commit", "checkout_tree", "sources", "security_progression", "security_result"},
        )
        self.assertEqual(
            set(diagnostic["validator"]["sources"][0]),
            {"path", "loaded_path_match", "git_blob", "measured_git_blob", "measured_sha256", "matches_git_blob", "bytes"},
        )
        self.assertEqual(
            set(diagnostic["attribution"]),
            {"repository", "run_id", "run_attempt", "job", "workflow", "event_name"},
        )
        snapshot = diagnostic["event_snapshots"][0]
        self.assertEqual(
            set(snapshot),
            {"id", "route", "selector", "safe_basename", "path_string_sha256", "present", "readable", "size_bytes", "content_sha256", "size_limit_bytes", "size_limit_status", "utf8_status", "json_status", "top_level_shape", "projection"},
        )
        self.assertEqual(
            set(snapshot["projection"]),
            {"repository", "number", "pull_request_shape", "base_shape", "head_shape", "base_ref", "base_sha", "base_repository", "head_ref", "head_sha", "head_repository", "merge_member", "merge_type", "merge_value"},
        )
        evaluation = diagnostic["evaluations"][0]
        self.assertEqual(
            set(evaluation),
            {"id", "site", "route", "sequence", "event_snapshot_id", "runtime_context", "result", "first_rejecting_predicate", "predicates"},
        )
        self.assertEqual(
            set(evaluation["runtime_context"]),
            {"github_actions", "event_name", "full_ref", "short_ref", "head_ref", "base_ref", "repository", "runtime_sha", "actual_checkout_commit", "actual_branch", "checkout_parents", "checkout_tree", "payload_head_tree", "object_availability"},
        )
        predicate = evaluation["predicates"][0]
        self.assertEqual(
            set(predicate),
            {"id", "source_path", "source_sha256", "source_line", "input_source", "required_relation", "result", "observed"},
        )
        self.assertEqual(
            set(predicate["observed"]),
            {"category", "type", "length", "sha256", "value"},
        )
        helper_source = next(
            source
            for source in diagnostic["validator"]["sources"]
            if source["path"] == "scripts/release_state_support.py"
        )
        self.assertEqual(predicate["source_sha256"], helper_source["measured_sha256"])
        self.assertIsInstance(predicate["source_line"], int)
        self.assertEqual(diagnostic["capture"]["network_lookups"], 0)
        self.assertFalse(diagnostic["capture"]["whole_event_included"])
        self.assertFalse(diagnostic["capture"]["whole_environment_included"])

    def test_writer_and_serialization_failures_preserve_original_status(self) -> None:
        fixture = self.fixture()
        ordinary = fixture.run("--state", "auto")

        constructor_destination = self.output_path("constructor.json")
        constructor_stdout = io.StringIO()
        constructor_stderr = io.StringIO()
        clean_environment = os.environ.copy()
        for key in GITHUB_KEYS:
            clean_environment.pop(key, None)
        with mock.patch.dict(os.environ, clean_environment, clear=True), mock.patch.object(
            validate_release_state,
            "DiagnosticRecorder",
            side_effect=RuntimeError("ARTIFICIAL_CONSTRUCTOR_SECRET_4c82"),
        ), redirect_stderr(constructor_stderr), mock.patch(
            "sys.stdout", constructor_stdout
        ):
            constructor_status = validate_release_state.main(
                [
                    "--repo-root",
                    str(fixture.root),
                    "--state",
                    "auto",
                    "--diagnostic-json",
                    str(constructor_destination),
                ]
            )
        self.assertEqual(constructor_status, ordinary.returncode)
        self.assertEqual(constructor_stdout.getvalue(), ordinary.stdout)
        self.assertEqual(
            constructor_stderr.getvalue(),
            "[release-state] WARNING diagnostic capture failed: output unavailable\n",
        )
        self.assertFalse(constructor_destination.exists())

        existing = self.output_path()
        existing.write_text("owned-before-test\n", encoding="utf-8")
        failed_writer = fixture.run(
            "--state",
            "auto",
            "--diagnostic-json",
            str(existing),
        )
        self.assertEqual(failed_writer.returncode, ordinary.returncode)
        self.assertEqual(failed_writer.stdout, ordinary.stdout)
        self.assertEqual(
            failed_writer.stderr,
            "[release-state] WARNING diagnostic capture failed: output unavailable\n",
        )
        self.assertEqual(existing.read_text(encoding="utf-8"), "owned-before-test\n")

        marker = "ARTIFICIAL_EXCEPTION_SECRET_31ef"
        destination = self.output_path("serialization.json")
        stdout = io.StringIO()
        stderr = io.StringIO()
        with mock.patch.dict(os.environ, clean_environment, clear=True), mock.patch.object(
            release_state_support.DiagnosticRecorder,
            "document",
            side_effect=RuntimeError(marker),
        ), redirect_stderr(stderr), mock.patch("sys.stdout", stdout):
            status = validate_release_state.main(
                [
                    "--repo-root",
                    str(fixture.root),
                    "--state",
                    "auto",
                    "--diagnostic-json",
                    str(destination),
                ]
            )
        self.assertEqual(status, ordinary.returncode)
        self.assertEqual(stdout.getvalue(), ordinary.stdout)
        self.assertNotIn(marker, stderr.getvalue())
        self.assertEqual(
            stderr.getvalue(),
            "[release-state] WARNING diagnostic capture failed: output unavailable\n",
        )

    def test_handled_error_writes_exit_two_diagnostic(self) -> None:
        destination = self.output_path()
        missing_root = destination.parent / "missing-repository"
        process = run_validator(
            missing_root,
            "--state",
            "auto",
            "--diagnostic-json",
            str(destination),
        )
        self.assertEqual(process.returncode, 2)
        diagnostic = self.load_diagnostic(destination)
        self.assertEqual(diagnostic["validator"]["result"], "error")
        self.assertEqual(diagnostic["validator"]["exit_code"], 2)
        self.assertIsNone(diagnostic["validator"]["resolved_state"])
        self.assertFalse(diagnostic["capture"]["complete"])

    def test_source_attribution_distinguishes_committed_dirty_and_synthetic(self) -> None:
        fixture = self.fixture()
        scripts = fixture.root / "scripts"
        scripts.mkdir()
        for name in ("release_state_support.py", "validate_release_state.py"):
            shutil.copy2(SCRIPT.with_name(name), scripts / name)
        for relative in release_state_support.DiagnosticRecorder._SOURCE_PATHS[2:]:
            source = SCRIPT.parents[1] / relative
            target = fixture.root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
        git(fixture.root, "add", "scripts", "docs/release/v1.13/v1.13.17-release-state.md", "docs/release/v1.13/v1.13.17-release-state-validator-contract.md", "docs/release/v1.13/v1.13.17-security-toolchain-progression-reference.md")
        git(fixture.root, "commit", "-m", "TEST_FIXTURE_ONLY validator sources")
        committed_head = fixture.rev_parse("HEAD")
        destination = self.output_path("committed.json")
        process = run_validator_script(
            scripts / "validate_release_state.py",
            fixture.root,
            "--state",
            "auto",
            "--diagnostic-json",
            str(destination),
        )
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        committed = self.load_diagnostic(destination)
        self.assertEqual(committed["validator"]["checkout_commit"], committed_head)
        self.assertTrue(committed["capture"]["complete"])
        self.assertTrue(
            all(source["matches_git_blob"] is True for source in committed["validator"]["sources"])
        )

        with (scripts / "release_state_support.py").open("a", encoding="utf-8") as handle:
            handle.write("\n# TEST_FIXTURE_ONLY dirty source attribution\n")
        dirty_destination = self.output_path("dirty.json")
        dirty_process = run_validator_script(
            scripts / "validate_release_state.py",
            fixture.root,
            "--state",
            "auto",
            "--diagnostic-json",
            str(dirty_destination),
        )
        self.assertEqual(dirty_process.returncode, 0, dirty_process.stdout + dirty_process.stderr)
        dirty = self.load_diagnostic(dirty_destination)
        helper = next(
            source for source in dirty["validator"]["sources"]
            if source["path"] == "scripts/release_state_support.py"
        )
        self.assertFalse(helper["matches_git_blob"])
        self.assertFalse(dirty["capture"]["complete"])

        fixture = self.fixture()
        scripts = fixture.root / "scripts"
        scripts.mkdir()
        for name in ("release_state_support.py", "validate_release_state.py"):
            shutil.copy2(SCRIPT.with_name(name), scripts / name)
        for relative in release_state_support.DiagnosticRecorder._SOURCE_PATHS[2:]:
            source = SCRIPT.parents[1] / relative
            target = fixture.root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
        git(fixture.root, "add", "scripts", "docs/release/v1.13/v1.13.17-release-state.md", "docs/release/v1.13/v1.13.17-release-state-validator-contract.md", "docs/release/v1.13/v1.13.17-security-toolchain-progression-reference.md")
        git(fixture.root, "commit", "-m", "TEST_FIXTURE_ONLY validator sources")
        base, candidate, synthetic = fixture.create_immutable_merge()
        git(fixture.root, "checkout", "--detach", synthetic)
        event = fixture.write_strict_pr_event(base, candidate, synthetic)
        synthetic_destination = self.output_path("synthetic.json")
        synthetic_process = run_validator_script(
            scripts / "validate_release_state.py",
            fixture.root,
            "--state",
            "auto",
            "--diagnostic-json",
            str(synthetic_destination),
            env=fixture.strict_pr_env(event, synthetic),
        )
        self.assertEqual(synthetic_process.returncode, 0, synthetic_process.stdout + synthetic_process.stderr)
        synthetic_record = self.load_diagnostic(synthetic_destination)
        self.assertEqual(synthetic_record["validator"]["checkout_commit"], synthetic)
        self.assertNotEqual(synthetic_record["validator"]["checkout_commit"], candidate)
        self.assertTrue(
            all(source["matches_git_blob"] is True for source in synthetic_record["validator"]["sources"])
        )

    def test_workflow_diagnostic_enforcement_positive_and_negative_controls(self) -> None:
        workflow_path = SCRIPT.parents[1] / ".github/workflows/ci.yml"
        audit_path = SCRIPT.with_name("audit_ci_enforcement.sh")
        workflow = workflow_path.read_text(encoding="utf-8")

        def probe(value: str) -> ProcessResult:
            directory = tempfile.TemporaryDirectory()
            self.addCleanup(directory.cleanup)
            path = Path(directory.name) / "ci.yml"
            path.write_text(value, encoding="utf-8")
            return run_process(
                ["bash", str(audit_path), "--diagnostic-workflow-probe", str(path)],
                cwd=SCRIPT.parents[1],
                check=False,
            )

        positive = probe(workflow)
        self.assertEqual(positive.returncode, 0, positive.stdout + positive.stderr)
        upload_pattern = re.compile(
            r"\n      - name: Upload release-state diagnostic\n.*?(?=\n      - name:)",
            re.S,
        )
        cases = (
            (
                "missing-argument",
                workflow.replace('              --diagnostic-json "$COLDKEEP_RELEASE_DIAGNOSTIC_PATH"\n', "", 1),
                "applicable release PR passes the diagnostic argument",
            ),
            (
                "omitted-upload",
                upload_pattern.sub("", workflow, count=1),
                "missing release-state diagnostic upload",
            ),
            (
                "wrong-order",
                workflow.replace(
                    "      - name: Upload release-state diagnostic\n",
                    "      - name: TEST_FIXTURE_ONLY interposed step\n        run: 'true'\n\n      - name: Upload release-state diagnostic\n",
                    1,
                ),
                "upload must immediately follow validation",
            ),
            (
                "success-only-upload",
                workflow.replace("always() && ((github.event_name", "success() && ((github.event_name", 1),
                "diagnostic upload uses always and the release-PR-or-tag scope",
            ),
            (
                "missing-release-scope",
                workflow.replace(
                    "if: ${{ always() && ((github.event_name == 'pull_request' && startsWith(github.head_ref, 'release/')) || (github.event_name == 'push' && github.ref_type == 'tag' && startsWith(github.ref, 'refs/tags/v'))) }}",
                    "if: ${{ always() }}",
                    1,
                ),
                "diagnostic upload uses always and the release-PR-or-tag scope",
            ),
            (
                "wrong-path",
                workflow.replace(
                    "          path: ${{ runner.temp }}/coldkeep-release-state-${{ github.run_id }}-${{ github.run_attempt }}-${{ github.job }}.json",
                    "          path: ${{ runner.temp }}/wrong-diagnostic.json",
                    1,
                ),
                "diagnostic upload uses the exact single-file path",
            ),
            (
                "wrong-name",
                workflow.replace("          name: release-state-diagnostic-", "          name: wrong-diagnostic-", 1),
                "diagnostic artifact name is run/job/checkout attributed",
            ),
            (
                "wrong-retention",
                workflow.replace("          retention-days: 14", "          retention-days: 7", 1),
                "diagnostic artifact retention is fourteen days",
            ),
            (
                "missing-file-warn",
                workflow.replace("          if-no-files-found: error", "          if-no-files-found: warn", 1),
                "diagnostic upload fails on missing current capture",
            ),
            (
                "wildcard-upload",
                workflow.replace(
                    "          path: ${{ runner.temp }}/coldkeep-release-state-${{ github.run_id }}-${{ github.run_attempt }}-${{ github.job }}.json",
                    "          path: ${{ runner.temp }}/*.json",
                    1,
                ),
                "single-file enforcement",
            ),
            (
                "masked-status",
                workflow.replace(
                    "            python3 scripts/validate_release_state.py --state auto\n          fi",
                    "            python3 scripts/validate_release_state.py --state auto || true\n          fi",
                    1,
                ),
                "validator status must remain blocking and unmasked",
            ),
            (
                "continue-on-error",
                workflow.replace(
                    "      - name: Validate repository release state\n",
                    "      - name: Validate repository release state\n        continue-on-error: true\n",
                    1,
                ),
                "validator status must remain blocking and unmasked",
            ),
        )
        for name, mutated, expected in cases:
            with self.subTest(name=name):
                self.assertNotEqual(mutated, workflow)
                result = probe(mutated)
                self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertIn(expected, result.stderr)


class FailedPredecessorContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        source = (
            SCRIPT.parents[1]
            / "docs/release/v1.13/v1.13.17-predecessor-disposition.md"
        )
        self.path = "docs/release/v1.13/v1.13.17-predecessor-disposition.md"
        target = self.root / self.path
        target.parent.mkdir(parents=True)
        target.write_bytes(source.read_bytes())

    def parse(self) -> tuple[object, str | None]:
        document = release_state_support.Document.load(self.root, self.path)
        return release_state_support.parse_failed_predecessor_disposition(document)

    def test_exact_23_key_disposition_passes(self) -> None:
        disposition, detail = self.parse()
        self.assertIsNone(detail)
        self.assertEqual(
            tuple(disposition.values),
            release_state_support.PREDECESSOR_DISPOSITION_KEYS,
        )

    def test_each_missing_key_fails_closed(self) -> None:
        original = (self.root / self.path).read_text(encoding="utf-8")
        for key in release_state_support.PREDECESSOR_DISPOSITION_KEYS:
            with self.subTest(key=key):
                text = re.sub(rf"(?m)^{re.escape(key)}: .*\n", "", original, count=1)
                (self.root / self.path).write_text(text, encoding="utf-8")
                disposition, detail = self.parse()
                self.assertIsNone(disposition)
                self.assertEqual(detail, "schema-v1 key set or order is invalid")
        (self.root / self.path).write_text(original, encoding="utf-8")

    def test_unknown_and_duplicate_keys_fail_closed(self) -> None:
        original = (self.root / self.path).read_text(encoding="utf-8")
        for row in ("UNKNOWN: value\n", "SCHEMA: duplicate\n"):
            with self.subTest(row=row):
                text = original.replace("```text\n", "```text\n" + row, 1)
                (self.root / self.path).write_text(text, encoding="utf-8")
                disposition, detail = self.parse()
                self.assertIsNone(disposition)
                self.assertEqual(detail, "schema-v1 key set or order is invalid")

    def test_selector_is_exact_and_unique(self) -> None:
        contract = release_state_support.Document(
            self.root,
            "contract.md",
            ["**Predecessor disposition model:** failed-publication-predecessor-v1"],
        )
        self.assertEqual(
            release_state_support.parse_predecessor_declaration(contract),
            ("failed-publication-predecessor-v1", None),
        )
        duplicate = release_state_support.Document(
            self.root,
            "contract.md",
            contract.lines * 2,
        )
        self.assertIsNotNone(
            release_state_support.parse_predecessor_declaration(duplicate)[1]
        )


class FailedPredecessorReadBoundaryTests(unittest.TestCase):
    """Exercise the selected predecessor's first-read security boundary."""

    def fixture(self) -> Fixture:
        fixture = Fixture()
        self.addCleanup(fixture.close)
        return fixture

    @staticmethod
    def select(fixture: Fixture) -> Path:
        return ReleaseStateValidatorTests().select_failed_predecessor(fixture)

    def assert_ckrs010(self, fixture: Fixture) -> None:
        process = fixture.run("--state", "development")
        self.assertEqual(process.returncode, 1, process.stdout + process.stderr)
        self.assertIn("[CKRS010]", process.stdout)
        self.assertNotIn("[CKRS019]", process.stdout)
        self.assertNotIn("Traceback", process.stderr)

    def test_selected_regular_and_missing_public_cli_routes(self) -> None:
        fixture = self.fixture()
        path = self.select(fixture)
        process = fixture.run("--state", "development")
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        path.unlink()
        self.assert_ckrs010(fixture)

    def test_selected_symlink_dangling_directory_and_oversize_are_ckrs010(self) -> None:
        for case in ("symlink", "dangling", "directory", "oversize"):
            with self.subTest(case=case):
                fixture = self.fixture()
                path = self.select(fixture)
                original = path.read_bytes()
                path.unlink()
                if case == "symlink":
                    target = fixture.root / "TEST_FIXTURE_ONLY-disposition-target"
                    target.write_bytes(original)
                    path.symlink_to(target)
                elif case == "dangling":
                    path.symlink_to(fixture.root / "TEST_FIXTURE_ONLY-absent")
                elif case == "directory":
                    path.mkdir()
                else:
                    path.write_bytes(b"x" * 65537)
                self.assert_ckrs010(fixture)

    def test_selected_encoding_and_newline_failures_are_ckrs010(self) -> None:
        for prefix in (b"\xff", b"\xef\xbb\xbf", b"\r"):
            with self.subTest(prefix=prefix):
                fixture = self.fixture()
                path = self.select(fixture)
                path.write_bytes(prefix + path.read_bytes())
                self.assert_ckrs010(fixture)

    def test_unselected_dangling_symlink_is_ckrs019_without_loader_call(self) -> None:
        fixture = self.fixture()
        path = (
            fixture.root
            / "docs/release/v1.13/v1.13.10-predecessor-disposition.md"
        )
        path.symlink_to(fixture.root / "TEST_FIXTURE_ONLY-absent")
        with mock.patch.object(
            validate_release_state,
            "load_predecessor_disposition_document",
        ) as loader:
            result = validate_release_state.validate(fixture.root, "development")
        loader.assert_not_called()
        self.assertIn("CKRS019", [item.rule for item in result.violations])
        self.assertNotIn("CKRS010", [item.rule for item in result.violations])

    def test_rejected_file_types_and_known_oversize_are_not_read(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        relative = "docs/release/v1.13/disposition.md"
        path = root / relative
        path.parent.mkdir(parents=True)
        for case in ("symlink", "directory", "oversize"):
            with self.subTest(case=case):
                if path.is_symlink() or path.is_file():
                    path.unlink()
                elif path.exists():
                    path.rmdir()
                if case == "symlink":
                    target = root / "TEST_FIXTURE_ONLY-target"
                    target.write_text("harmless\n", encoding="utf-8")
                    path.symlink_to(target)
                elif case == "directory":
                    path.mkdir()
                else:
                    path.write_bytes(b"x" * 65537)
                with mock.patch.object(
                    release_state_support.os,
                    "read",
                    wraps=os.read,
                ) as reader:
                    document, detail = (
                        release_state_support.load_predecessor_disposition_document(
                            root, relative
                        )
                    )
                self.assertIsNone(document)
                self.assertIsNotNone(detail)
                reader.assert_not_called()

    def test_limit_plus_one_read_and_injected_open_read_type_failures(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        relative = "docs/release/v1.13/disposition.md"
        path = root / relative
        path.parent.mkdir(parents=True)
        path.write_bytes(b"x" * 65536)
        document, detail = release_state_support.load_predecessor_disposition_document(
            root, relative
        )
        self.assertIsNone(detail)
        self.assertEqual(len(document.lines[0]), 65536)

        for primitive in ("open", "read", "fstat"):
            with self.subTest(primitive=primitive):
                patches = []
                if primitive == "open":
                    patches.append(mock.patch.object(os, "open", side_effect=OSError()))
                elif primitive == "read":
                    patches.append(mock.patch.object(os, "read", side_effect=OSError()))
                else:
                    fifo_result = os.stat_result((stat.S_IFIFO, 0, 0, 0, 0, 0, 0, 0, 0, 0))
                    patches.append(mock.patch.object(os, "fstat", return_value=fifo_result))
                    patches.append(mock.patch.object(os, "read", wraps=os.read))
                entered = [patch.start() for patch in patches]
                try:
                    rejected, error = (
                        release_state_support.load_predecessor_disposition_document(
                            root, relative
                        )
                    )
                finally:
                    for patch in reversed(patches):
                        patch.stop()
                self.assertIsNone(rejected)
                self.assertIsNotNone(error)
                if primitive == "fstat":
                    entered[-1].assert_not_called()

    def test_exact_read_budget_and_short_read_accounting(self) -> None:
        limit = release_state_support.PREDECESSOR_DISPOSITION_MAX_BYTES
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        relative = "docs/release/v1.13/disposition.md"
        path = root / relative
        path.parent.mkdir(parents=True)
        original_read = os.read
        original_fstat = os.fstat

        def exercise(
            content: bytes,
            *,
            advertised_size: int | None = None,
            short_read: int | None = None,
        ) -> tuple[release_state_support.Document | None, str | None, list[tuple[int, int]]]:
            path.write_bytes(content)
            calls: list[tuple[int, int]] = []

            def observed_read(descriptor: int, requested: int) -> bytes:
                actual_request = min(requested, short_read) if short_read else requested
                returned = original_read(descriptor, actual_request)
                calls.append((requested, len(returned)))
                return returned

            def observed_fstat(descriptor: int) -> os.stat_result:
                value = original_fstat(descriptor)
                if advertised_size is None:
                    return value
                fields = list(value)
                fields[6] = advertised_size
                return os.stat_result(fields)

            with mock.patch.object(os, "read", side_effect=observed_read), mock.patch.object(
                os, "fstat", side_effect=observed_fstat
            ):
                document, detail = release_state_support.load_predecessor_disposition_document(
                    root, relative
                )
            remaining = limit + 1
            for requested, returned in calls:
                self.assertEqual(requested, min(8192, remaining))
                self.assertLessEqual(returned, requested)
                remaining -= returned
            self.assertGreaterEqual(remaining, 0)
            return document, detail, calls

        exact, detail, calls = exercise(b"x" * limit)
        self.assertIsNone(detail)
        self.assertEqual(len(exact.lines[0]), limit)
        self.assertEqual(sum(returned for _, returned in calls), limit)
        self.assertEqual(calls[-1], (1, 0))

        grown, detail, calls = exercise(
            b"x" * (limit + 1), advertised_size=limit
        )
        self.assertIsNone(grown)
        self.assertEqual(
            detail, "file exceeds the 65536-byte predecessor disposition limit"
        )
        self.assertEqual(sum(returned for _, returned in calls), limit + 1)
        self.assertEqual(calls[-1], (1, 1))

        short, detail, calls = exercise(b"x" * limit, short_read=997)
        self.assertIsNone(detail)
        self.assertEqual(len(short.lines[0]), limit)
        self.assertEqual(sum(returned for _, returned in calls), limit)
        self.assertEqual(calls[-1], (1, 0))
        self.assertGreater(len(calls), 8)

    def test_selected_path_replacement_is_rejected_after_checked_read(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        relative = "docs/release/v1.13/disposition.md"
        path = root / relative
        path.parent.mkdir(parents=True)
        path.write_text("TEST_FIXTURE_ONLY checked object\n", encoding="utf-8")
        target = root / "TEST_FIXTURE_ONLY-replacement-target"
        target.write_text("TEST_FIXTURE_ONLY replacement\n", encoding="utf-8")
        original_open = os.open
        replaced = False

        def replacing_open(
            selected: object,
            flags: int,
            *args: object,
            **kwargs: object,
        ) -> int:
            nonlocal replaced
            descriptor = original_open(selected, flags, *args, **kwargs)
            if selected == path.name and not replaced:
                replaced = True
                path.unlink()
                path.symlink_to(target)
            return descriptor

        with mock.patch.object(os, "open", side_effect=replacing_open):
            document, detail = (
                release_state_support.load_predecessor_disposition_document(
                    root, relative
                )
            )
        self.assertTrue(replaced)
        self.assertIsNone(document)
        self.assertEqual(detail, "canonical path changed during the bounded read")


class SuccessorFixtureSourceCopyTests(unittest.TestCase):
    def make_source(self, root: Path) -> str:
        for relative in SUCCESSOR_HISTORICAL_PATHS:
            write(root, relative.as_posix(), f"TEST_FIXTURE_ONLY {relative}\n")
        git(root, "init")
        git(root, "config", "user.name", "fixture")
        git(root, "config", "user.email", "fixture@example.invalid")
        git(root, "add", ".")
        git(root, "commit", "-m", "TEST_FIXTURE_ONLY historical base")
        return "HEAD"

    def test_current_pair_copy_is_independent_of_index_membership(self) -> None:
        pair = (
            validate_governance.CURRENT_RELEASE_BODY,
            validate_governance.CURRENT_RELEASE_BODY_CHECKSUM,
        )
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source"
            source.mkdir()
            base = self.make_source(source)
            body = b"TEST_FIXTURE_ONLY body\n"
            digest = hashlib.sha256(body).hexdigest()
            for relative, content in (
                (pair[0], body),
                (pair[1], f"{digest}  {pair[0].as_posix()}\n".encode("ascii")),
            ):
                path = source / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(content)
            observed = []
            for index_state in ("untracked", "tracked"):
                if index_state == "tracked":
                    git(source, "add", *(item.as_posix() for item in pair))
                destination = Path(directory) / index_state
                destination.mkdir()
                copy_successor_fixture_source(
                    source, destination, compatibility_base=base
                )
                observed.append(tuple((destination / item).read_bytes() for item in pair))
            self.assertEqual(observed[0], observed[1])
            self.assertEqual(observed[0], (body, f"{digest}  {pair[0].as_posix()}\n".encode("ascii")))

    def test_terminal_reference_is_excluded_regardless_of_index_membership(self) -> None:
        relative = validate_governance.TERMINAL_DISPOSITION_REFERENCE_FILE
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source"
            source.mkdir()
            base = self.make_source(source)
            path = source / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes((SCRIPT.parents[1] / relative).read_bytes())
            for index_state in ("untracked", "tracked"):
                if index_state == "tracked":
                    git(source, "add", relative.as_posix())
                destination = Path(directory) / index_state
                destination.mkdir()
                copy_successor_fixture_source(
                    source, destination, compatibility_base=base
                )
                self.assertFalse((destination / relative).exists())

    def test_missing_current_or_historical_source_fails_setup(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source"
            source.mkdir()
            base = self.make_source(source)
            missing_current = SUCCESSOR_HISTORICAL_PATHS[0]
            (source / missing_current).unlink()
            with self.assertRaises(FileNotFoundError):
                copy_successor_fixture_source(
                    source,
                    Path(directory) / "missing-current",
                    compatibility_base=base,
                )

        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source"
            source.mkdir()
            base = self.make_source(source)
            missing_historical = SUCCESSOR_HISTORICAL_PATHS[0]
            git(source, "rm", missing_historical.as_posix())
            git(source, "commit", "-m", "TEST_FIXTURE_ONLY remove historical path")
            write(source, missing_historical.as_posix(), "TEST_FIXTURE_ONLY current only\n")
            with self.assertRaises(subprocess.CalledProcessError):
                copy_successor_fixture_source(
                    source,
                    Path(directory) / "missing-historical",
                    compatibility_base="HEAD",
                )

    def test_historical_fixture_rejects_injected_terminal_reference(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "historical"
            destination.mkdir()
            source = SCRIPT.parents[1]
            copy_successor_fixture_source(source, destination)
            self.assertEqual(validate_governance.validate(destination), [])
            relative = validate_governance.TERMINAL_DISPOSITION_REFERENCE_FILE
            target = destination / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes((source / relative).read_bytes())
            violations = validate_governance.validate(destination)
            self.assertTrue(
                any(
                    "unexpected terminal disposition material for historical vocabulary"
                    in item
                    for item in violations
                ),
                violations,
            )


class SuccessorLifecycleIntegrationTests(unittest.TestCase):
    """Exercise the complete successor authority through C, M, A and closure."""

    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name) / "repo"
        self.root.mkdir()
        source_root = SCRIPT.parents[1]
        copy_successor_fixture_source(source_root, self.root)
        self.prepare_development_candidate()
        git(self.root, "init")
        git(self.root, "config", "user.name", "fixture")
        git(self.root, "config", "user.email", "fixture@example.invalid")
        git(self.root, "add", ".")
        git(self.root, "commit", "-m", "TEST_FIXTURE_ONLY predecessor base")
        self.predecessor = self.rev_parse("HEAD")
        git(self.root, "tag", "-a", "v1.13.16", "-m", "TEST_FIXTURE_ONLY")
        predecessor_tag = self.rev_parse("refs/tags/v1.13.16")
        disposition = (
            self.root
            / "docs/release/v1.13/v1.13.17-predecessor-disposition.md"
        )
        text = disposition.read_text(encoding="utf-8")
        text = re.sub(r"(?m)^TAG_OBJECT: [0-9a-f]{40}$", f"TAG_OBJECT: {predecessor_tag}", text)
        text = re.sub(r"(?m)^DIRECT_TARGET: [0-9a-f]{40}$", f"DIRECT_TARGET: {self.predecessor}", text)
        text = re.sub(r"(?m)^SUCCESSOR_BASE: [0-9a-f]{40}$", f"SUCCESSOR_BASE: {self.predecessor}", text)
        disposition.write_text(text, encoding="utf-8")
        git(self.root, "add", disposition.relative_to(self.root).as_posix())
        git(self.root, "commit", "-m", "TEST_FIXTURE_ONLY successor development")
        git(self.root, "checkout", "-b", "release/v1.13.17")
        self.release_push_event_number = 0
        self.closure_push_event_number = 0

    def rev_parse(self, value: str) -> str:
        return run_process(
            [resolved_executable("git"), "-C", str(self.root), "rev-parse", value],
            check=True,
        ).stdout.strip()

    def validator(
        self,
        mode: str,
        env: dict[str, str] | None = None,
    ) -> ProcessResult:
        return run_validator(self.root, "--state", mode, "--json", env=env)

    def fixture_validator(
        self,
        mode: str,
        env: dict[str, str] | None = None,
        *extra: str,
    ) -> ProcessResult:
        return run_validator_script(
            self.root / "scripts/validate_release_state.py",
            self.root,
            "--state", mode, "--json", *extra,
            env=env,
        )

    def replace_regex_once(
        self,
        relative: str | Path,
        pattern: str,
        replacement: str,
        *,
        flags: int = 0,
    ) -> None:
        path = self.root / relative
        text, count = re.subn(
            pattern,
            replacement,
            path.read_text(encoding="utf-8"),
            count=1,
            flags=flags,
        )
        self.assertEqual(count, 1, f"{relative}: {pattern}")
        path.write_text(text, encoding="utf-8")

    def set_canonical_value(self, key: str, value: str) -> None:
        self.replace_regex_once(
            validate_governance.CANONICAL_CURRENT_STATE_FILE,
            rf"(?m)^{re.escape(key)}: \S+$",
            f"{key}: {value}",
        )

    def remove_successor_publication_pair(self) -> None:
        for relative in (
            validate_governance.CURRENT_RELEASE_BODY,
            validate_governance.CURRENT_RELEASE_BODY_CHECKSUM,
        ):
            path = self.root / relative
            if path.is_symlink() or path.is_file():
                path.unlink()
            elif path.exists():
                raise AssertionError(f"unexpected non-file fixture path: {relative}")

    def set_lifecycle_prose(self, lifecycle: str) -> None:
        if lifecycle == "development":
            badge = "v1.13.17%20recovery%20development"
            readme_state = (
                "v1.13.17 — Annotated-Tag Certification Recovery is the active Route C\n"
                "development successor. Phases 0-1 are Complete, Phase 2 is Next, and its\n"
                "publication body/checksum are intentionally absent. The work is limited to\n"
                "release-state, tag-identity diagnostics, CI enforcement, and governance; it\n"
                "does not change product storage behavior. v1.13.16 is the immutable\n"
                "failed-publication predecessor: annotated tag A16 exists, its required tag CI\n"
                "failed, the tag certificate was withheld, no GitHub Release exists, and\n"
                "publication and Phase 19 remain unauthorized. V1.x full closure is not\n"
                "established."
            )
            release_state = (
                "v1.13.17 is the active Route C recovery successor in development. Phases 0-1\n"
                "are Complete and Phase 2 is Next. Publication material is absent. v1.13.16 is\n"
                "the immutable failed-publication predecessor; its tag certificate was withheld\n"
                "and no GitHub Release exists. No successor push, PR, merge, tag, publication,\n"
                "or closure is authorized by this tracked state."
            )
            scope_state = (
                "Phases 0-1 are Complete. Phase 2 is Next. The release gate is not passed and\n"
                "publication material is absent."
            )
        elif lifecycle == "pre-release":
            badge = "v1.13.17%20ready"
            readme_state = (
                "v1.13.17 — Annotated-Tag Certification Recovery is the ready Route C\n"
                "successor. Phases 0-4 are Complete, Phase 5 is Next, and its source-only\n"
                "publication body/checksum are frozen. The candidate remains local and does\n"
                "not authorize a push, PR, merge, tag, publication, or closure. v1.13.16 is\n"
                "the immutable failed-publication predecessor. V1.x full closure is not\n"
                "established."
            )
            release_state = (
                "v1.13.17 is the ready Route C recovery successor. Phases 0-4 are Complete and\n"
                "Phase 5 is Next. Publication material is frozen. v1.13.16 is the immutable\n"
                "failed-publication predecessor. No push, PR, merge, tag, publication, or\n"
                "closure is authorized by this tracked state."
            )
            scope_state = (
                "Phases 0-4 are Complete. Phase 5 is Next. The pre-merge prerequisite gate is\n"
                "passed and publication material is frozen."
            )
        else:
            raise AssertionError(f"unsupported fixture lifecycle: {lifecycle}")
        self.replace_regex_once(
            "README.md",
            r"(?m)^!\[Status\]\(https://img\.shields\.io/badge/status-v1\.13\.17%20[^)]*\)$",
            f"![Status](https://img.shields.io/badge/status-{badge})",
        )
        self.replace_regex_once(
            "README.md",
            r"(?ms)^v1\.13\.17 — Annotated-Tag Certification Recovery is .*?(?=\n\nThe latest published stable release remains)",
            readme_state,
        )
        self.replace_regex_once(
            "docs/release/v1.13/README.md",
            r"(?ms)^v1\.13\.17 is .*?(?=\n\n## Historical v1\.13\.16 authority narrative$)",
            release_state,
        )
        self.replace_regex_once(
            "docs/release/v1.13/v1.13.17-scope.md",
            r"(?ms)^Phases 0-.*?(?=\n\n## Boundaries$)",
            scope_state,
        )

    def prepare_development_candidate(self) -> None:
        """Construct the declared development scenario from any valid source phase."""
        self.remove_successor_publication_pair()
        self.set_phase_progression(2)
        self.set_tracker_status("Active")
        self.set_state_values({
            "CURRENT_PHASE": "2_NEXT",
            "TRACKED_PUBLICATION_MATERIAL": "ABSENT",
            "V1_13_17_STATE": "ACTIVE_RECOVERY_SUCCESSOR_DEVELOPMENT",
        })
        self.set_canonical_value("RELEASE_BODY_SHA256", "ABSENT")
        self.replace_regex_once(
            "CHANGELOG.md",
            r"(?m)^## v1\.13\.17 - (?:Unreleased|\d{4}-\d{2}-\d{2}) —",
            "## v1.13.17 - Unreleased —",
        )
        self.set_lifecycle_prose("development")
        gate = self.root / "docs/release/v1.13/v1.13.17-release-gate.md"
        gate.write_text(
            "# v1.13.17 Release Gate\n\n"
            "**Status:** Development — gate not passed\n\n"
            "## Current gate\n\n"
            "Phases 0-1 are Complete and Phase 2 is Next. The successor publication body\n"
            "and checksum are absent. Local implementation does not establish independent\n"
            "acceptance, hosted certification, protected-merge permission, tag authority,\n"
            "publication authority, or closure.\n\n"
            "## Final verdict\n\n"
            "Not passed. Continue only under the separately authorized phase that owns the\n"
            "next transition.\n",
            encoding="utf-8",
        )

    def set_state_values(self, values: dict[str, str]) -> None:
        paths = (validate_governance.CANONICAL_CURRENT_STATE_FILE,) + (
            validate_governance.CURRENT_STATE_MIRROR_FILES
        )
        for relative in paths:
            path = self.root / relative
            text = path.read_text(encoding="utf-8")
            for key, value in values.items():
                text, count = re.subn(
                    rf"(?m)^{re.escape(key)}: \S+$",
                    f"{key}: {value}",
                    text,
                    count=1,
                )
                self.assertEqual(count, 1, f"{relative}: {key}")
            path.write_text(text, encoding="utf-8")

    def set_phase_progression(self, next_phase: int | None) -> None:
        for relative, field in (
            (Path("docs/release/v1.13/v1.13.17-phase-list.md"), "Status"),
            (Path("docs/release/v1.13/v1.13.17-validation-checklist.md"), "Phase status"),
        ):
            path = self.root / relative
            text = path.read_text(encoding="utf-8")
            for number in range(10):
                status = (
                    "Complete" if next_phase is None or number < next_phase else
                    "Next" if number == next_phase else
                    "Not started"
                )
                pattern = rf"(?ms)(^## Phase {number}\b.*?^\*\*{re.escape(field)}:\*\*) (?:Complete|Next|Not started)$"
                text, count = re.subn(pattern, rf"\1 {status}", text, count=1)
                self.assertEqual(count, 1, f"{relative}: Phase {number}")
            path.write_text(text, encoding="utf-8")

    def set_tracker_status(self, status: str) -> None:
        for relative in (
            "docs/release/v1.13/v1.13.17-scope.md",
            "docs/release/v1.13/v1.13.17-phase-list.md",
            "docs/release/v1.13/v1.13.17-validation-checklist.md",
        ):
            path = self.root / relative
            text = path.read_text(encoding="utf-8")
            text, count = re.subn(
                r"(?m)^\*\*Status:\*\* .+$",
                f"**Status:** {status}",
                text,
                count=1,
            )
            self.assertEqual(count, 1, relative)
            path.write_text(text, encoding="utf-8")

    def freeze_candidate(self) -> None:
        body_path = self.root / validate_governance.CURRENT_RELEASE_BODY
        checksum_path = self.root / validate_governance.CURRENT_RELEASE_BODY_CHECKSUM
        body = (
            "# v1.13.17 Annotated-Tag Certification Recovery\n\n"
            "This source-only release has no custom assets. It preserves the "
            "v1.13.15 stable state and the v1.13.16 failed publication while "
            "distinguishing inherited product repairs from release-tool correction.\n"
        ).encode("utf-8")
        body_path.parent.mkdir(parents=True, exist_ok=True)
        body_path.write_bytes(body)
        digest = hashlib.sha256(body).hexdigest()
        checksum_path.write_text(
            f"{digest}  {validate_governance.CURRENT_RELEASE_BODY.as_posix()}\n",
            encoding="ascii",
        )
        self.set_phase_progression(5)
        self.set_tracker_status("Ready for release")
        self.set_state_values({
            "CURRENT_PHASE": "5_NEXT",
            "TRACKED_PUBLICATION_MATERIAL": "FROZEN",
            "V1_13_17_STATE": "READY_PRE_RELEASE",
        })
        self.set_canonical_value("RELEASE_BODY_SHA256", digest)
        self.replace_regex_once(
            "CHANGELOG.md",
            r"(?m)^## v1\.13\.17 - (?:Unreleased|\d{4}-\d{2}-\d{2}) —",
            "## v1.13.17 - 2026-01-02 —",
        )
        self.set_lifecycle_prose("pre-release")
        gate = self.root / "docs/release/v1.13/v1.13.17-release-gate.md"
        gate.write_text(
            "# v1.13.17 Release Gate\n\n"
            "**Status:** Passed — pre-merge prerequisites complete\n\n"
            "## Final verdict\n\nPASS — PRE-MERGE PREREQUISITES COMPLETE\n",
            encoding="utf-8",
        )

    def tag_environment(self, runtime_sha: str, event_after: str) -> dict[str, str]:
        event = self.root / "TEST_FIXTURE_ONLY-tag-event.json"
        event.write_text(
            json.dumps({
                "ref": "refs/tags/v1.13.17",
                "before": "0" * 40,
                "after": event_after,
                "created": True,
                "deleted": False,
                "forced": False,
                "repository": {"full_name": "franchoy/coldkeep"},
            }),
            encoding="utf-8",
        )
        return {
            "GITHUB_ACTIONS": "true",
            "GITHUB_EVENT_NAME": "push",
            "GITHUB_REF": "refs/tags/v1.13.17",
            "GITHUB_REF_NAME": "v1.13.17",
            "GITHUB_REF_TYPE": "tag",
            "GITHUB_EVENT_PATH": str(event),
            "GITHUB_REPOSITORY": "franchoy/coldkeep",
            "GITHUB_SHA": runtime_sha,
        }

    def main_environment(self, before: str, after: str) -> dict[str, str]:
        event = self.root / "TEST_FIXTURE_ONLY-main-event.json"
        event.write_text(
            json.dumps({
                "ref": "refs/heads/main", "before": before, "after": after,
                "created": False, "deleted": False, "forced": False,
                "repository": {"full_name": "franchoy/coldkeep"},
            }),
            encoding="utf-8",
        )
        return {
            "GITHUB_ACTIONS": "true", "GITHUB_EVENT_NAME": "push",
            "GITHUB_REF": "refs/heads/main", "GITHUB_REF_NAME": "main",
            "GITHUB_REF_TYPE": "branch", "GITHUB_EVENT_PATH": str(event),
            "GITHUB_REPOSITORY": "franchoy/coldkeep", "GITHUB_SHA": after,
        }

    def release_push_environment(
        self,
        before: object,
        after: str,
        created: object,
        *,
        payload_updates: dict[str, object] | None = None,
        payload_remove: tuple[str, ...] = (),
        environment_updates: dict[str, str] | None = None,
        raw_event: str | None = None,
    ) -> dict[str, str]:
        self.release_push_event_number += 1
        event = self.root / (
            "TEST_FIXTURE_ONLY-release-push-"
            f"{self.release_push_event_number}.json"
        )
        payload: dict[str, object] = {
            "ref": "refs/heads/release/v1.13.17",
            "before": before,
            "after": after,
            "created": created,
            "deleted": False,
            "forced": False,
            "repository": {"full_name": "franchoy/coldkeep"},
        }
        if payload_updates:
            payload.update(payload_updates)
        for key in payload_remove:
            payload.pop(key, None)
        event.write_text(
            raw_event if raw_event is not None else json.dumps(payload),
            encoding="utf-8",
        )
        environment = {
            "GITHUB_ACTIONS": "true",
            "GITHUB_EVENT_NAME": "push",
            "GITHUB_REF": "refs/heads/release/v1.13.17",
            "GITHUB_REF_NAME": "release/v1.13.17",
            "GITHUB_REF_TYPE": "branch",
            "GITHUB_EVENT_PATH": str(event),
            "GITHUB_REPOSITORY": "franchoy/coldkeep",
            "GITHUB_SHA": after,
        }
        if environment_updates:
            environment.update(environment_updates)
        return environment

    def closure_push_environment(
        self,
        before: object,
        after: str,
        created: object,
        *,
        payload_updates: dict[str, object] | None = None,
        payload_remove: tuple[str, ...] = (),
        environment_updates: dict[str, str] | None = None,
        raw_event: str | None = None,
    ) -> dict[str, str]:
        self.closure_push_event_number += 1
        branch = "release/v1.13.17-post-publication-closure"
        event = self.root / (
            "TEST_FIXTURE_ONLY-closure-push-"
            f"{self.closure_push_event_number}.json"
        )
        payload: dict[str, object] = {
            "ref": f"refs/heads/{branch}",
            "before": before,
            "after": after,
            "created": created,
            "deleted": False,
            "forced": False,
            "repository": {"full_name": "franchoy/coldkeep"},
        }
        if payload_updates:
            payload.update(payload_updates)
        for key in payload_remove:
            payload.pop(key, None)
        event.write_text(
            raw_event if raw_event is not None else json.dumps(payload),
            encoding="utf-8",
        )
        environment = {
            "GITHUB_ACTIONS": "true",
            "GITHUB_EVENT_NAME": "push",
            "GITHUB_REF": f"refs/heads/{branch}",
            "GITHUB_REF_NAME": branch,
            "GITHUB_REF_TYPE": "branch",
            "GITHUB_EVENT_PATH": str(event),
            "GITHUB_REPOSITORY": "franchoy/coldkeep",
            "GITHUB_SHA": after,
        }
        if environment_updates:
            environment.update(environment_updates)
        return environment

    def closure_pr_environment(
        self,
        base_sha: str,
        head_sha: str,
        *,
        number: int = 902,
        checkout_sha: str | None = None,
        merge_commit_sha: str | None = None,
        include_merge_commit_sha: bool = True,
    ) -> dict[str, str]:
        branch = "release/v1.13.17-post-publication-closure"
        event = self.root / f"TEST_FIXTURE_ONLY-closure-pr-{number}.json"
        pull_request: dict[str, object] = {
            "base": {
                "ref": "main", "sha": base_sha,
                "repo": {"full_name": "franchoy/coldkeep"},
            },
            "head": {
                "ref": branch, "sha": head_sha,
                "repo": {"full_name": "franchoy/coldkeep"},
            },
        }
        if include_merge_commit_sha:
            pull_request["merge_commit_sha"] = merge_commit_sha
        event.write_text(
            json.dumps({
                "number": number,
                "repository": {"full_name": "franchoy/coldkeep"},
                "pull_request": pull_request,
            }),
            encoding="utf-8",
        )
        environment = {
            "GITHUB_ACTIONS": "true",
            "GITHUB_EVENT_NAME": "pull_request",
            "GITHUB_REF": f"refs/pull/{number}/merge",
            "GITHUB_REF_NAME": f"{number}/merge",
            "GITHUB_HEAD_REF": branch,
            "GITHUB_BASE_REF": "main",
            "GITHUB_EVENT_PATH": str(event),
            "GITHUB_REPOSITORY": "franchoy/coldkeep",
            "GITHUB_SHA": checkout_sha if checkout_sha is not None else head_sha,
        }
        self.assertEqual(
            environment["GITHUB_REF"],
            f"refs/pull/{json.loads(event.read_text(encoding='utf-8'))['number']}/merge",
        )
        self.assertEqual(
            environment["GITHUB_REF_NAME"],
            environment["GITHUB_REF"].removeprefix("refs/pull/"),
        )
        return environment

    def prepare_synthetic_closure_pr(self) -> tuple[str, str, str]:
        """Build a real ordered [published, closure-head] synthetic checkout."""
        published, closure_head = self.prepare_direct_closure_candidate()
        git(self.root, "checkout", "main")
        git(
            self.root, "merge", "--no-ff",
            "release/v1.13.17-post-publication-closure",
            "-m", "TEST_FIXTURE_ONLY synthetic closure checkout",
        )
        checkout = self.rev_parse("HEAD")
        self.assertEqual(
            run_process(
                [
                    resolved_executable("git"), "-C", str(self.root),
                    "rev-list", "--parents", "-n", "1", checkout,
                ],
                check=True,
            ).stdout.strip().split()[1:],
            [published, closure_head],
        )
        self.assertEqual(
            self.rev_parse(f"{checkout}^{{tree}}"),
            self.rev_parse(f"{closure_head}^{{tree}}"),
        )
        git(self.root, "checkout", "--detach", checkout)
        return published, closure_head, checkout

    def test_closure_pr_advisory_metadata_red_green_contract(self) -> None:
        """CP-01/03: public synthetic advisory values and direct nonconsumption."""
        published, closure_head, checkout = self.prepare_synthetic_closure_pr()
        cases = (
            ("matching", checkout),
            ("present-null", None),
            ("different-existing-object", published),
            ("different-non-object", "1" * 40),
        )
        self.assertNotEqual(published, checkout)
        self.assertNotEqual("1" * 40, checkout)
        self.assertNotEqual(
            run_process(
                [
                    resolved_executable("git"), "-C", str(self.root),
                    "cat-file", "-e", f"{'1' * 40}^{{object}}",
                ],
                check=False,
            ).returncode,
            0,
        )
        for index, (name, metadata) in enumerate(cases, 930):
            environment = self.closure_pr_environment(
                published, closure_head, number=index,
                checkout_sha=checkout, merge_commit_sha=metadata,
            )
            for mode in ("auto", "post-release-closure-candidate"):
                with self.subTest(route="synthetic", case=name, mode=mode):
                    self.assert_projection(
                        self.validator(mode, environment),
                        "post-release-closure-candidate",
                        "GITHUB_CLOSURE_PR_CONTEXT_CONSISTENCY",
                    )
        git(self.root, "checkout", "--detach", closure_head)
        direct = self.closure_pr_environment(
            published, closure_head, number=934,
            include_merge_commit_sha=False,
        )
        for mode in ("auto", "post-release-closure-candidate"):
            with self.subTest(route="direct-head", case="missing", mode=mode):
                self.assert_projection(
                    self.validator(mode, direct),
                    "post-release-closure-candidate",
                    "GITHUB_CLOSURE_PR_CONTEXT_CONSISTENCY",
                )

    def assert_closure_pr_rejected(
        self,
        environment: dict[str, str],
        *,
        explicit_state: str = "post-release-closure-candidate",
    ) -> None:
        for mode in ("auto", explicit_state):
            process = self.validator(mode, environment)
            payload = json.loads(process.stdout)
            with self.subTest(rejected_mode=mode):
                if mode == "auto" and process.returncode == 2:
                    self.assertEqual(payload["evidence_scope"], "UNAVAILABLE")
                    self.assertEqual(payload["error"]["kind"], "git-context")
                else:
                    self.assertEqual(
                        process.returncode, 1, process.stdout + process.stderr
                    )
                    self.assertEqual(
                        payload["evidence_scope"],
                        "UNSUPPORTED_OR_CONFLICTING_CONTEXT",
                    )
                    self.assertIn(
                        "CKRS016",
                        {item["rule"] for item in payload["violations"]},
                    )

    @staticmethod
    def rewrite_fixture_event(
        environment: dict[str, str],
        mutate: object,
    ) -> None:
        event = Path(environment["GITHUB_EVENT_PATH"])
        payload = json.loads(event.read_text(encoding="utf-8"))
        mutate(payload)
        event.write_text(json.dumps(payload), encoding="utf-8")

    def test_closure_pr_metadata_rejection_and_direct_nonconsumption(self) -> None:
        """CP-02/03: malformed synthetic metadata rejects; direct ignores it."""
        published, closure_head, checkout = self.prepare_synthetic_closure_pr()
        malformed = (
            ("absent", None, False),
            ("true", True, True),
            ("false", False, True),
            ("integer", 7, True),
            ("float", 1.5, True),
            ("array", [], True),
            ("object", {}, True),
            ("empty", "", True),
            ("short", "a" * 39, True),
            ("long", "a" * 41, True),
            ("uppercase", "A" * 40, True),
            ("mixed-case", "a" * 39 + "A", True),
            ("non-hex", "g" * 40, True),
            ("leading-space", " " + "a" * 40, True),
            ("trailing-space", "a" * 40 + " ", True),
            ("all-zero", "0" * 40, True),
        )
        for index, (name, metadata, present) in enumerate(malformed, 940):
            with self.subTest(route="synthetic", case=name):
                environment = self.closure_pr_environment(
                    published, closure_head, number=index,
                    checkout_sha=checkout, merge_commit_sha=metadata,
                    include_merge_commit_sha=present,
                )
                self.assert_closure_pr_rejected(environment)

        git(self.root, "checkout", "--detach", closure_head)
        direct_values = (
            ("absent", None, False), ("null", None, True),
            ("matching", closure_head, True), ("differing", published, True),
            ("boolean", True, True), ("object", {}, True),
            ("all-zero", "0" * 40, True),
        )
        for index, (name, metadata, present) in enumerate(direct_values, 960):
            environment = self.closure_pr_environment(
                published, closure_head, number=index,
                merge_commit_sha=metadata,
                include_merge_commit_sha=present,
            )
            for mode in ("auto", "post-release-closure-candidate"):
                with self.subTest(route="direct", case=name, mode=mode):
                    self.assert_projection(
                        self.validator(mode, environment),
                        "post-release-closure-candidate",
                        "GITHUB_CLOSURE_PR_CONTEXT_CONSISTENCY",
                    )
        malformed_event = self.closure_pr_environment(
            published, closure_head, number=967,
            include_merge_commit_sha=False,
        )
        Path(malformed_event["GITHUB_EVENT_PATH"]).write_text("[]", encoding="utf-8")
        self.assert_closure_pr_rejected(malformed_event)
        conflicting = self.closure_pr_environment(
            published, closure_head, number=968,
            include_merge_commit_sha=False,
        )
        conflicting["GITHUB_REPOSITORY"] = "fork/coldkeep"
        self.assert_closure_pr_rejected(conflicting)

    def test_closure_pr_common_identity_and_object_guards(self) -> None:
        """CP-04: accepted differing metadata cannot bypass common guards."""
        published, closure_head, checkout = self.prepare_synthetic_closure_pr()
        blob = self.rev_parse(f"{closure_head}:README.md")

        def baseline(number: int) -> dict[str, str]:
            return self.closure_pr_environment(
                published, closure_head, number=number,
                checkout_sha=checkout, merge_commit_sha=published,
            )

        environment_cases = (
            ("actions", "GITHUB_ACTIONS", "false"),
            ("event", "GITHUB_EVENT_NAME", "push"),
            ("full-ref", "GITHUB_REF", "refs/pull/999/merge"),
            ("short-ref", "GITHUB_REF_NAME", "999/merge"),
            ("base-ref", "GITHUB_BASE_REF", "develop"),
            ("head-ref", "GITHUB_HEAD_REF", "release/v1.13.17"),
            ("repository", "GITHUB_REPOSITORY", "fork/coldkeep"),
            ("runtime", "GITHUB_SHA", closure_head),
            ("event-path", "GITHUB_EVENT_PATH", ""),
        )
        for index, (name, key, value) in enumerate(environment_cases, 970):
            with self.subTest(source="environment", case=name):
                environment = baseline(index)
                environment[key] = value
                self.assert_closure_pr_rejected(environment)

        def remove_number(payload: dict[str, object]) -> None:
            payload.pop("number")
        def wrong_number(payload: dict[str, object]) -> None:
            payload["number"] = 1
        def wrong_repository(payload: dict[str, object]) -> None:
            payload["repository"] = {"full_name": "fork/coldkeep"}
        def missing_pr(payload: dict[str, object]) -> None:
            payload.pop("pull_request")
        def wrong_pr_shape(payload: dict[str, object]) -> None:
            payload["pull_request"] = []
        def wrong_base_ref(payload: dict[str, object]) -> None:
            payload["pull_request"]["base"]["ref"] = "develop"
        def wrong_head_ref(payload: dict[str, object]) -> None:
            payload["pull_request"]["head"]["ref"] = "release/v1.13.17"
        def wrong_base_repo(payload: dict[str, object]) -> None:
            payload["pull_request"]["base"]["repo"]["full_name"] = "fork/coldkeep"
        def wrong_head_repo(payload: dict[str, object]) -> None:
            payload["pull_request"]["head"]["repo"]["full_name"] = "fork/coldkeep"
        def base_shape(payload: dict[str, object]) -> None:
            payload["pull_request"]["base"] = []
        def head_shape(payload: dict[str, object]) -> None:
            payload["pull_request"]["head"] = []
        def base_unavailable(payload: dict[str, object]) -> None:
            payload["pull_request"]["base"]["sha"] = "2" * 40
        def head_unavailable(payload: dict[str, object]) -> None:
            payload["pull_request"]["head"]["sha"] = "3" * 40
        def base_wrong_type(payload: dict[str, object]) -> None:
            payload["pull_request"]["base"]["sha"] = blob
        def head_wrong_type(payload: dict[str, object]) -> None:
            payload["pull_request"]["head"]["sha"] = blob

        event_cases = (
            ("missing-number", remove_number), ("wrong-number", wrong_number),
            ("repository", wrong_repository), ("missing-pr", missing_pr),
            ("pr-shape", wrong_pr_shape), ("base-ref", wrong_base_ref),
            ("head-ref", wrong_head_ref), ("base-repo", wrong_base_repo),
            ("head-repo", wrong_head_repo), ("base-shape", base_shape),
            ("head-shape", head_shape), ("base-unavailable", base_unavailable),
            ("head-unavailable", head_unavailable),
            ("base-wrong-type", base_wrong_type),
            ("head-wrong-type", head_wrong_type),
        )
        for index, (name, mutation) in enumerate(event_cases, 980):
            with self.subTest(source="event", case=name):
                environment = baseline(index)
                self.rewrite_fixture_event(environment, mutation)
                self.assert_closure_pr_rejected(environment)

    def test_closure_pr_real_topology_and_event_negatives(self) -> None:
        """CP-05/06: real raw topology and event loader negatives."""
        published, closure_head, checkout = self.prepare_synthetic_closure_pr()
        head_tree = self.rev_parse(f"{closure_head}^{{tree}}")
        wrong_tree = self.rev_parse(f"{published}^{{tree}}")

        def raw_commit(parents: list[str], tree: str, label: str) -> str:
            raw = (
                f"tree {tree}\n"
                + "".join(f"parent {parent}\n" for parent in parents)
                + "author fixture <fixture@example.invalid> 1700000000 +0000\n"
                + "committer fixture <fixture@example.invalid> 1700000000 +0000\n"
                + f"\nTEST_FIXTURE_ONLY {label}\n"
            )
            created = subprocess.run(
                [resolved_executable("git"), "-C", str(self.root),
                 "hash-object", "-t", "commit", "-w", "--stdin"],
                input=raw, text=True, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, check=True,
            ).stdout.strip()
            self.assertEqual(
                run_process(
                    [resolved_executable("git"), "-C", str(self.root),
                     "cat-file", "-p", created], check=True,
                ).stdout,
                raw,
            )
            self.assertEqual(
                run_process(
                    [resolved_executable("git"), "-C", str(self.root),
                     "cat-file", "-t", created], check=True,
                ).stdout.strip(),
                "commit",
            )
            return created

        topology = (
            ("reversed", [closure_head, published], head_tree),
            ("missing", [published], head_tree),
            ("extra", [published, closure_head, self.predecessor], head_tree),
            ("duplicate", [published, closure_head, closure_head], head_tree),
            ("wrong-tree", [published, closure_head], wrong_tree),
        )
        for index, (name, parents, tree) in enumerate(topology, 1000):
            bad = raw_commit(parents, tree, name)
            stored = run_process(
                [resolved_executable("git"), "-C", str(self.root),
                 "cat-file", "-p", bad], check=True,
            ).stdout
            self.assertEqual(
                re.findall(r"(?m)^parent ([0-9a-f]{40})$", stored), parents
            )
            self.assertEqual(
                re.search(r"(?m)^tree ([0-9a-f]{40})$", stored).group(1), tree
            )
            git(self.root, "checkout", "--detach", bad)
            environment = self.closure_pr_environment(
                published, closure_head, number=index,
                checkout_sha=bad, merge_commit_sha=published,
            )
            with self.subTest(topology=name):
                self.assert_closure_pr_rejected(environment)

        git(self.root, "checkout", "--detach", checkout)
        for index, (name, raw) in enumerate((
            ("malformed", "{"), ("array", "[]"), ("null", "null")
        ), 1010):
            environment = self.closure_pr_environment(
                published, closure_head, number=index,
                checkout_sha=checkout, merge_commit_sha=published,
            )
            Path(environment["GITHUB_EVENT_PATH"]).write_text(raw, encoding="utf-8")
            with self.subTest(event=name):
                self.assert_closure_pr_rejected(environment)
        missing = self.closure_pr_environment(
            published, closure_head, number=1013,
            checkout_sha=checkout, merge_commit_sha=published,
        )
        Path(missing["GITHUB_EVENT_PATH"]).unlink()
        self.assert_closure_pr_rejected(missing)
        # The PR loader is intentionally unbounded. A coherent object just above the
        # tag loader's bound remains valid and proves this repair adds no new limit.
        large = self.closure_pr_environment(
            published, closure_head, number=1014,
            checkout_sha=checkout, merge_commit_sha=published,
        )
        event = Path(large["GITHUB_EVENT_PATH"])
        payload = json.loads(event.read_text(encoding="utf-8"))
        payload["padding"] = "x" * 1048576
        event.write_text(json.dumps(payload), encoding="utf-8")
        self.assertGreater(event.stat().st_size, 1048576)
        for mode in ("auto", "post-release-closure-candidate"):
            self.assert_projection(
                self.validator(mode, large),
                "post-release-closure-candidate",
                "GITHUB_CLOSURE_PR_CONTEXT_CONSISTENCY",
            )

    def test_closure_pr_rechecks_event_for_ckrs016(self) -> None:
        """CP-07: inference acceptance is never cached for final CKRS016."""
        published, closure_head, checkout = self.prepare_synthetic_closure_pr()
        for name, mutate in (
            (
                "authoritative-base",
                lambda payload: payload["pull_request"]["base"].update(
                    {"ref": "develop"}
                ),
            ),
            (
                "metadata-shape",
                lambda payload: payload["pull_request"].update(
                    {"merge_commit_sha": True}
                ),
            ),
        ):
            environment = self.closure_pr_environment(
                published, closure_head, number=1020,
                checkout_sha=checkout, merge_commit_sha=None,
            )
            event = Path(environment["GITHUB_EVENT_PATH"])
            actual_environment = os.environ.copy()
            for key in GITHUB_KEYS:
                actual_environment.pop(key, None)
            actual_environment.update(environment)
            original_context = release_state_support.strict_post_release_pr_context
            evaluations = 0

            def mutate_after_inference(*args: object, **kwargs: object) -> object:
                nonlocal evaluations
                accepted = original_context(*args, **kwargs)
                evaluations += 1
                if evaluations == 1:
                    changed = json.loads(event.read_text(encoding="utf-8"))
                    mutate(changed)
                    event.write_text(json.dumps(changed), encoding="utf-8")
                return accepted

            with self.subTest(case=name), mock.patch.dict(
                os.environ, actual_environment, clear=True
            ), mock.patch.object(
                release_state_support,
                "strict_post_release_pr_context",
                side_effect=mutate_after_inference,
            ):
                result = validate_release_state.validate(self.root, "auto")
                self.assertEqual(evaluations, 2)
                self.assertEqual(result.state, "post-release-closure-candidate")
                self.assertEqual(
                    result.evidence_scope,
                    "GITHUB_CLOSURE_PR_CONTEXT_CONSISTENCY",
                )
                self.assertIn("CKRS016", {item.rule for item in result.violations})

    def test_closure_pr_diagnostic_parity_and_source_binding(self) -> None:
        """CP-08: closure diagnostics stay empty and behavior-neutral."""
        published, closure_head, checkout = self.prepare_synthetic_closure_pr()
        synthetic = (
            ("matching", checkout, True), ("null", None, True),
            ("differing", published, True), ("invalid", True, False),
        )
        for index, (name, metadata, accepted) in enumerate(synthetic, 1030):
            environment = self.closure_pr_environment(
                published, closure_head, number=index,
                checkout_sha=checkout, merge_commit_sha=metadata,
            )
            for mode in ("auto", "post-release-closure-candidate"):
                inactive = self.fixture_validator(mode, environment)
                destination = self.root / f"TEST_FIXTURE_ONLY-pr-{index}-{mode}.json"
                active = self.fixture_validator(
                    mode, environment,
                    "--diagnostic-json", str(destination),
                )
                with self.subTest(route="synthetic", case=name, mode=mode):
                    self.assertEqual(active.returncode, inactive.returncode)
                    self.assertEqual(active.stdout, inactive.stdout)
                    self.assertEqual(active.stderr, inactive.stderr)
                    self.assertEqual(active.returncode, 0 if accepted else 1)
                    diagnostic = json.loads(destination.read_text(encoding="utf-8"))
                    self.assertFalse(diagnostic["capture"]["complete"])
                    self.assertEqual(diagnostic["evaluations"], [])
                    self.assertEqual(diagnostic["event_snapshots"], [])
                    self.assertEqual(
                        diagnostic["validator"]["exit_code"], active.returncode
                    )
                    for source in diagnostic["validator"]["sources"]:
                        source_path = self.root / source["path"]
                        if source_path.is_file():
                            self.assertTrue(source["loaded_path_match"])
                            self.assertEqual(
                                source["measured_sha256"],
                                hashlib.sha256(source_path.read_bytes()).hexdigest(),
                            )
                        else:
                            self.assertIsNone(source["measured_sha256"])
        git(self.root, "checkout", "--detach", closure_head)
        direct = self.closure_pr_environment(
            published, closure_head, number=1034,
            include_merge_commit_sha=False,
        )
        for mode in ("auto", "post-release-closure-candidate"):
            inactive = self.fixture_validator(mode, direct)
            destination = self.root / f"TEST_FIXTURE_ONLY-pr-direct-{mode}.json"
            active = self.fixture_validator(
                mode, direct, "--diagnostic-json", str(destination)
            )
            self.assertEqual(
                (active.returncode, active.stdout, active.stderr),
                (inactive.returncode, inactive.stdout, inactive.stderr),
            )
            diagnostic = json.loads(destination.read_text(encoding="utf-8"))
            self.assertEqual(diagnostic["evaluations"], [])
            self.assertEqual(diagnostic["event_snapshots"], [])

    def test_two_commit_closure_forward_routes_and_terminal_boundary(self) -> None:
        """CP-09: fixture M -> K -> H supports update, PR and first main."""
        support = self.root / "scripts/release_state_support.py"
        repaired = support.read_text(encoding="utf-8")
        old = repaired.replace(
            'merge_present\n        and merge_field_valid',
            'pull_request.get("merge_commit_sha") == head',
            1,
        )
        self.assertNotEqual(old, repaired)
        support.write_text(old, encoding="utf-8")
        published, initial_closure = self.prepare_direct_closure_candidate()
        support.write_text(repaired, encoding="utf-8")
        git(self.root, "add", "scripts/release_state_support.py")
        git(self.root, "commit", "-m", "TEST_FIXTURE_ONLY closure metadata repair H")
        repaired_closure = self.rev_parse("HEAD")
        self.assertEqual(self.rev_parse(f"{repaired_closure}^"), initial_closure)
        self.assertEqual(self.rev_parse("refs/tags/v1.13.17^{}"), published)
        update = self.closure_push_environment(
            initial_closure, repaired_closure, False
        )
        for mode in ("auto", "post-release-closure-candidate"):
            self.assert_projection(
                self.validator(mode, update),
                "post-release-closure-candidate",
                "GITHUB_CLOSURE_PUSH_CONTEXT_CONSISTENCY",
            )
        git(self.root, "checkout", "main")
        git(
            self.root, "merge", "--no-ff",
            "release/v1.13.17-post-publication-closure",
            "-m", "TEST_FIXTURE_ONLY first closure main C",
        )
        first_main = self.rev_parse("HEAD")
        self.assertEqual(
            self.rev_parse(f"{first_main}^{{tree}}"),
            self.rev_parse(f"{repaired_closure}^{{tree}}"),
        )
        self.assertEqual(
            run_process(
                [resolved_executable("git"), "-C", str(self.root),
                 "rev-list", "--parents", "-n", "1", first_main],
                check=True,
            ).stdout.strip().split()[1:],
            [published, repaired_closure],
        )
        git(self.root, "checkout", "--detach", first_main)
        for index, metadata in enumerate((None, initial_closure), 1040):
            environment = self.closure_pr_environment(
                published, repaired_closure, number=index,
                checkout_sha=first_main, merge_commit_sha=metadata,
            )
            for mode in ("auto", "post-release-closure-candidate"):
                self.assert_projection(
                    self.validator(mode, environment),
                    "post-release-closure-candidate",
                    "GITHUB_CLOSURE_PR_CONTEXT_CONSISTENCY",
                )
        main = self.main_environment(published, first_main)
        for mode in ("auto", "post-release-closure-candidate"):
            self.assert_projection(
                self.validator(mode, main),
                "post-release-closure-candidate",
                "GITHUB_CLOSURE_MAIN_CONTEXT_CONSISTENCY",
            )
        terminal = self.validator("post-release-closed", main)
        self.assertEqual(terminal.returncode, 1, terminal.stdout + terminal.stderr)
        self.assertIn(
            "CKRS019",
            {item["rule"] for item in json.loads(terminal.stdout)["violations"]},
        )
        self.assert_closure_pr_rejected(
            self.closure_pr_environment(
                published, repaired_closure, number=1042,
                checkout_sha=repaired_closure,
                merge_commit_sha=initial_closure,
            ) | {"GITHUB_SHA": repaired_closure}
        )
        self.assertEqual(self.rev_parse("refs/tags/v1.13.17^{}"), published)

    def test_pending_closure_pr_accepts_null_and_differing_metadata(self) -> None:
        """CP-10: the shared helper preserves the pending-closure caller."""
        published, pending = self.prepare_direct_closure_candidate(candidate=False)
        git(self.root, "checkout", "main")
        git(
            self.root, "merge", "--no-ff",
            "release/v1.13.17-post-publication-closure",
            "-m", "TEST_FIXTURE_ONLY pending synthetic PR",
        )
        checkout = self.rev_parse("HEAD")
        git(self.root, "checkout", "--detach", checkout)
        for index, metadata in enumerate((None, published), 1050):
            environment = self.closure_pr_environment(
                published, pending, number=index,
                checkout_sha=checkout, merge_commit_sha=metadata,
            )
            for mode in ("auto", "post-release-pending-closure"):
                self.assert_projection(
                    self.validator(mode, environment),
                    "post-release-pending-closure",
                    "GITHUB_CLOSURE_PR_CONTEXT_CONSISTENCY",
                )

    def prepare_direct_closure_candidate(
        self,
        *,
        candidate: bool = True,
    ) -> tuple[str, str]:
        """Create a published merge/tag and one direct-child closure candidate."""
        git(self.root, "branch", "main", self.predecessor)
        self.freeze_candidate()
        git(self.root, "add", ".")
        git(self.root, "commit", "-m", "TEST_FIXTURE_ONLY final candidate C")
        git(self.root, "checkout", "main")
        git(
            self.root, "merge", "--no-ff", "release/v1.13.17",
            "-m", "TEST_FIXTURE_ONLY normal merge M",
        )
        published_merge = self.rev_parse("HEAD")
        git(self.root, "tag", "-a", "v1.13.17", "-m", "TEST_FIXTURE_ONLY")
        git(
            self.root, "checkout", "-b",
            "release/v1.13.17-post-publication-closure",
        )
        self.set_phase_progression(None if candidate else 9)
        self.set_tracker_status("Published; post-publication closure pending")
        self.set_state_values({
            "CURRENT_PHASE": "NONE_CLOSURE_CANDIDATE" if candidate else "9_NEXT",
            "V1_13_17_STATE": (
                "CLOSURE_CANDIDATE_PENDING_TERMINAL_AUDIT"
                if candidate else "PUBLISHED_CLOSURE_PENDING"
            ),
        })
        if not candidate:
            scope = self.root / "docs/release/v1.13/v1.13.17-scope.md"
            scope.write_text(
                scope.read_text(encoding="utf-8")
                + "\nPhase 9 is Next; terminal effectiveness remains external.\n",
                encoding="utf-8",
            )
            release_index = self.root / "docs/release/v1.13/README.md"
            release_index.write_text(
                release_index.read_text(encoding="utf-8").replace(
                    "## Current Release State\n",
                    "## Current Release State\n\n"
                    "Phase 9 is Next; terminal effectiveness remains external.\n",
                    1,
                ),
                encoding="utf-8",
            )
        for relative, old, new in (
            ("README.md", "v1.13.17%20ready", "v1.13.17%20published"),
            ("README.md", "is the ready Route C", "is the published Route C"),
            (
                "docs/release/v1.13/README.md",
                "v1.13.17 is the ready Route C",
                "v1.13.17 is the published Route C",
            ),
        ):
            path = self.root / relative
            path.write_text(
                path.read_text(encoding="utf-8").replace(old, new, 1),
                encoding="utf-8",
            )
        gate = self.root / "docs/release/v1.13/v1.13.17-release-gate.md"
        gate.write_text(
            "# v1.13.17 Release Gate\n\n"
            "**Status:** Passed and released — closure pending\n\n"
            "## Final verdict\n\n"
            "PASS — PUBLICATION COMPLETE; TERMINAL AUDIT PENDING\n",
            encoding="utf-8",
        )
        git(self.root, "add", ".")
        git(self.root, "commit", "-m", "TEST_FIXTURE_ONLY closure candidate")
        closure_candidate = self.rev_parse("HEAD")
        self.assertEqual(
            self.rev_parse(f"{closure_candidate}^"), published_merge
        )
        self.assertEqual(
            self.rev_parse("refs/tags/v1.13.17^{}"), published_merge
        )
        return published_merge, closure_candidate

    def add_development_history(self) -> str:
        git(self.root, "commit", "--allow-empty", "-m", "TEST_FIXTURE_ONLY development step one")
        git(self.root, "commit", "--allow-empty", "-m", "TEST_FIXTURE_ONLY development step two")
        candidate = self.rev_parse("HEAD")
        self.assertEqual(
            self.rev_parse(f"{candidate}^{{commit}}"),
            candidate,
        )
        self.assertEqual(
            self.rev_parse(f"{self.predecessor}^{{commit}}"),
            self.predecessor,
        )
        self.assertEqual(
            run_process(
                [
                    resolved_executable("git"), "-C", str(self.root),
                    "rev-list", "--count", f"{self.predecessor}..{candidate}",
                ],
                check=True,
            ).stdout.strip(),
            "3",
        )
        return candidate

    def assert_release_push_rejected(
        self,
        environment: dict[str, str],
        automatic_state: str | None = None,
    ) -> None:
        automatic = self.validator("auto", environment)
        payload = json.loads(automatic.stdout)
        if automatic_state is None:
            self.assertEqual(
                automatic.returncode, 2, automatic.stdout + automatic.stderr
            )
            self.assertIsNone(payload["state"])
            self.assertEqual(payload["evidence_scope"], "UNAVAILABLE")
            self.assertEqual(payload["violations"], [])
            self.assertEqual(payload["error"]["kind"], "git-context")
        else:
            self.assertEqual(
                automatic.returncode, 1, automatic.stdout + automatic.stderr
            )
            self.assertEqual(payload["state"], automatic_state)
            self.assertEqual(
                payload["evidence_scope"], "UNSUPPORTED_OR_CONFLICTING_CONTEXT"
            )
            self.assertIn(
                "CKRS016", {item["rule"] for item in payload["violations"]}
            )
        explicit = self.validator("development", environment)
        self.assertEqual(explicit.returncode, 1, explicit.stdout + explicit.stderr)
        payload = json.loads(explicit.stdout)
        self.assertIn(
            "CKRS016",
            {item["rule"] for item in payload["violations"]},
        )
        self.assertEqual(
            payload["evidence_scope"], "UNSUPPORTED_OR_CONFLICTING_CONTEXT"
        )

    def assert_projection(
        self,
        process: ProcessResult,
        state: str,
        evidence_scope: str,
    ) -> dict[str, object]:
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        payload = json.loads(process.stdout)
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["state"], state)
        self.assertEqual(payload["artifact_state"], state)
        self.assertEqual(payload["active_version"], "1.13.17")
        self.assertEqual(payload["violations"], [])
        self.assertIsNone(payload["error"])
        self.assertEqual(payload["evidence_scope"], evidence_scope)
        self.assertEqual(
            payload["authorization_status"], "NOT_EVALUATED_BY_VALIDATOR"
        )
        self.assertEqual(
            payload["certification_status"], "PENDING_EXTERNAL_EVIDENCE"
        )
        return payload

    def test_release_branch_push_creation_parent_red_and_repaired_green(self) -> None:
        candidate = self.add_development_history()
        self.assert_projection(
            self.validator("auto"), "development", "LOCAL_RELEASE_BRANCH"
        )
        update = self.release_push_environment(
            self.predecessor, candidate, False
        )
        self.assert_projection(
            self.validator("auto", update),
            "development",
            "GITHUB_RELEASE_PUSH_CONTEXT_CONSISTENCY",
        )
        creation = self.release_push_environment("0" * 40, candidate, True)
        for mode in ("auto", "development"):
            with self.subTest(mode=mode):
                self.assert_projection(
                    self.validator(mode, creation),
                    "development",
                    "GITHUB_RELEASE_PUSH_CONTEXT_CONSISTENCY",
                )
        for name, environment in (("creation", creation), ("update", update)):
            with self.subTest(human=name):
                process = run_validator(
                    self.root, "--state", "auto", env=environment
                )
                self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
                self.assertEqual(
                    process.stdout,
                    "[release-state] OK state=development "
                    "active_version=1.13.17 violations=0 "
                    "security_progression=NOT_SELECTED "
                    "security_result=NOT_EVALUATED_BY_VALIDATOR\n",
                )
                self.assertEqual(process.stderr, "")

    def test_release_branch_push_creation_update_complete_matrix(self) -> None:
        candidate = self.add_development_history()
        for lifecycle, explicit in (
            ("development", "development"),
            ("pre-release", "pre-release"),
        ):
            if lifecycle == "pre-release":
                git(self.root, "checkout", "release/v1.13.17")
                self.freeze_candidate()
                git(self.root, "add", ".")
                git(self.root, "commit", "-m", "TEST_FIXTURE_ONLY final candidate C")
                candidate = self.rev_parse("HEAD")
            self.assertEqual(validate_governance.validate(self.root), [])
            for checkout in ("attached", "detached"):
                if checkout == "attached":
                    git(self.root, "checkout", "release/v1.13.17")
                else:
                    git(self.root, "checkout", "--detach", candidate)
                self.assertEqual(self.rev_parse("HEAD"), candidate)
                for transition, before, created in (
                    ("creation", "0" * 40, True),
                    ("update", self.predecessor, False),
                ):
                    environment = self.release_push_environment(
                        before, candidate, created
                    )
                    for mode in ("auto", explicit):
                        with self.subTest(
                            lifecycle=lifecycle,
                            checkout=checkout,
                            transition=transition,
                            mode=mode,
                        ):
                            self.assert_projection(
                                self.validator(mode, environment),
                                lifecycle,
                                "GITHUB_RELEASE_PUSH_CONTEXT_CONSISTENCY",
                            )
                    if checkout == "detached":
                        with self.subTest(
                            lifecycle=lifecycle,
                            checkout=checkout,
                            transition=transition,
                            mode="auto-human",
                        ):
                            process = run_validator(
                                self.root, "--state", "auto", env=environment
                            )
                            self.assertEqual(
                                process.returncode,
                                0,
                                process.stdout + process.stderr,
                            )
                            self.assertEqual(
                                process.stdout,
                                f"[release-state] OK state={lifecycle} "
                                "active_version=1.13.17 violations=0 "
                                "security_progression=NOT_SELECTED "
                                "security_result=NOT_EVALUATED_BY_VALIDATOR\n",
                            )
                            self.assertEqual(process.stderr, "")
            if lifecycle == "development":
                git(self.root, "checkout", "release/v1.13.17")

    def test_release_branch_push_pairing_types_and_common_guards(self) -> None:
        candidate = self.add_development_history()
        git(self.root, "checkout", "--detach", candidate)
        valid_transitions = (
            ("creation", "0" * 40, True),
            ("update", self.predecessor, False),
        )
        for name, before, created in valid_transitions:
            with self.subTest(control=name):
                self.assert_projection(
                    self.validator(
                        "development",
                        self.release_push_environment(before, candidate, created),
                    ),
                    "development",
                    "GITHUB_RELEASE_PUSH_CONTEXT_CONSISTENCY",
                )

        pairing_cases = (
            ("zero-false", "0" * 40, False, {}, ()),
            ("nonzero-true", self.predecessor, True, {}, ()),
            ("created-missing", "0" * 40, True, {}, ("created",)),
            ("created-null", "0" * 40, None, {}, ()),
            ("created-zero", "0" * 40, 0, {}, ()),
            ("created-one", "0" * 40, 1, {}, ()),
            ("created-string-true", "0" * 40, "true", {}, ()),
            ("created-string-false", self.predecessor, "false", {}, ()),
            ("created-list", "0" * 40, [], {}, ()),
            ("created-object", "0" * 40, {}, {}, ()),
            ("before-missing", "0" * 40, True, {}, ("before",)),
            ("before-null", None, True, {}, ()),
            ("before-number", 0, True, {}, ()),
            ("before-empty", "", True, {}, ()),
            ("before-short", "0" * 39, True, {}, ()),
            ("before-long", "0" * 41, True, {}, ()),
            ("before-nonhex", "g" * 40, True, {}, ()),
            ("before-uppercase", "A" * 40, False, {}, ()),
            ("before-leading-space", " " + "0" * 40, True, {}, ()),
            ("before-trailing-space", "0" * 40 + " ", True, {}, ()),
        )
        for name, before, created, updates, remove in pairing_cases:
            with self.subTest(pairing=name):
                self.assert_release_push_rejected(
                    self.release_push_environment(
                        before,
                        candidate,
                        created,
                        payload_updates=updates,
                        payload_remove=remove,
                    )
                )

        payload_guards = (
            ("deleted-true", {"deleted": True}, ()),
            ("deleted-missing", {}, ("deleted",)),
            ("deleted-string", {"deleted": "false"}, ()),
            ("forced-true", {"forced": True}, ()),
            ("forced-missing", {}, ("forced",)),
            ("forced-number", {"forced": 0}, ()),
            ("repository-wrong", {"repository": {"full_name": "fork/coldkeep"}}, ()),
            ("repository-missing", {}, ("repository",)),
            ("repository-shape", {"repository": "franchoy/coldkeep"}, ()),
            ("event-ref", {"ref": "refs/heads/main"}, ()),
            ("event-after", {"after": self.predecessor}, ()),
        )
        environment_guards = (
            ("actions-missing", {"GITHUB_ACTIONS": ""}),
            ("event-name-create", {"GITHUB_EVENT_NAME": "create"}),
            ("full-ref", {"GITHUB_REF": "refs/heads/main"}),
            ("short-ref", {"GITHUB_REF_NAME": "main"}),
            ("ref-type-tag", {"GITHUB_REF_TYPE": "tag"}),
            ("repository", {"GITHUB_REPOSITORY": "fork/coldkeep"}),
            ("runtime-sha", {"GITHUB_SHA": self.predecessor}),
            ("event-path-missing", {"GITHUB_EVENT_PATH": ""}),
            ("closure-ref", {
                "GITHUB_REF": "refs/heads/release/v1.13.17-post-publication-closure",
                "GITHUB_REF_NAME": "release/v1.13.17-post-publication-closure",
            }),
            ("wrong-release", {
                "GITHUB_REF": "refs/heads/release/v1.13.18",
                "GITHUB_REF_NAME": "release/v1.13.18",
            }),
        )
        for transition, before, created in valid_transitions:
            for name, updates, remove in payload_guards:
                with self.subTest(transition=transition, payload_guard=name):
                    self.assert_release_push_rejected(
                        self.release_push_environment(
                            before,
                            candidate,
                            created,
                            payload_updates=updates,
                            payload_remove=remove,
                        )
                    )
            for name, updates in environment_guards:
                with self.subTest(transition=transition, environment_guard=name):
                    self.assert_release_push_rejected(
                        self.release_push_environment(
                            before,
                            candidate,
                            created,
                            environment_updates=updates,
                        ),
                        "merged-not-tagged" if name == "full-ref" else None,
                    )
            with self.subTest(transition=transition, event="malformed"):
                self.assert_release_push_rejected(
                    self.release_push_environment(
                        before, candidate, created, raw_event="{"
                    )
                )
            with self.subTest(transition=transition, event="wrong-shape"):
                self.assert_release_push_rejected(
                    self.release_push_environment(
                        before, candidate, created, raw_event="[]"
                    )
                )
            missing = self.release_push_environment(before, candidate, created)
            Path(missing["GITHUB_EVENT_PATH"]).unlink()
            with self.subTest(transition=transition, event="missing"):
                self.assert_release_push_rejected(missing)

        for name, before, created in valid_transitions:
            with self.subTest(restored=name):
                self.assert_projection(
                    self.validator(
                        "development",
                        self.release_push_environment(before, candidate, created),
                    ),
                    "development",
                    "GITHUB_RELEASE_PUSH_CONTEXT_CONSISTENCY",
                )

    def test_detached_auto_release_push_inference_boundaries(self) -> None:
        candidate = self.add_development_history()
        valid = self.release_push_environment("0" * 40, candidate, True)

        git(self.root, "checkout", "--detach", candidate)
        self.assert_projection(
            self.validator("auto", valid),
            "development",
            "GITHUB_RELEASE_PUSH_CONTEXT_CONSISTENCY",
        )

        git(self.root, "checkout", "-b", "TEST_FIXTURE_ONLY-wrong", candidate)
        for mode, expected_exit in (("auto", 2), ("development", 1)):
            with self.subTest(actual_branch="wrong", mode=mode):
                process = self.validator(mode, valid)
                self.assertEqual(
                    process.returncode,
                    expected_exit,
                    process.stdout + process.stderr,
                )
                payload = json.loads(process.stdout)
                self.assertNotEqual(
                    payload["evidence_scope"],
                    "GITHUB_RELEASE_PUSH_CONTEXT_CONSISTENCY",
                )
                if mode == "auto":
                    self.assertEqual(payload["error"]["kind"], "git-context")
                else:
                    self.assertIn(
                        "CKRS016",
                        {item["rule"] for item in payload["violations"]},
                    )

        git(self.root, "checkout", "--detach", candidate)
        substitutions = (
            ("main", {
                "GITHUB_REF": "refs/heads/main",
                "GITHUB_REF_NAME": "main",
            }, 1, "merged-not-tagged", "UNSUPPORTED_OR_CONFLICTING_CONTEXT", None, "CKRS016"),
            ("tag", {
                "GITHUB_REF": "refs/tags/v1.13.17",
                "GITHUB_REF_NAME": "v1.13.17",
                "GITHUB_REF_TYPE": "tag",
            }, 2, None, "UNAVAILABLE", "git-context", None),
            ("closure", {
                "GITHUB_REF": "refs/heads/release/v1.13.17-post-publication-closure",
                "GITHUB_REF_NAME": "release/v1.13.17-post-publication-closure",
            }, 2, None, "UNAVAILABLE", "git-context", None),
            ("pr", {
                "GITHUB_EVENT_NAME": "pull_request",
                "GITHUB_REF": "refs/pull/17/merge",
                "GITHUB_REF_NAME": "17/merge",
                "GITHUB_HEAD_REF": "release/v1.13.17",
                "GITHUB_BASE_REF": "main",
            }, 1, "development", "UNSUPPORTED_OR_CONFLICTING_CONTEXT", None, "CKRS016"),
        )
        for name, updates, exit_code, state, scope, error, rule in substitutions:
            with self.subTest(context=name):
                environment = dict(valid)
                environment.update(updates)
                process = self.validator("auto", environment)
                self.assertEqual(
                    process.returncode,
                    exit_code,
                    process.stdout + process.stderr,
                )
                payload = json.loads(process.stdout)
                self.assertEqual(payload["state"], state)
                self.assertEqual(payload["evidence_scope"], scope)
                if error:
                    self.assertEqual(payload["error"]["kind"], error)
                    self.assertEqual(payload["violations"], [])
                else:
                    self.assertIsNone(payload["error"])
                    self.assertIn(
                        rule,
                        {item["rule"] for item in payload["violations"]},
                    )

        git(self.root, "branch", "main", candidate)
        git(self.root, "checkout", "main")
        actual_main = self.validator("auto", valid)
        self.assertEqual(
            actual_main.returncode,
            1,
            actual_main.stdout + actual_main.stderr,
        )
        actual_main_payload = json.loads(actual_main.stdout)
        self.assertEqual(actual_main_payload["state"], "merged-not-tagged")
        self.assertEqual(
            actual_main_payload["evidence_scope"],
            "UNSUPPORTED_OR_CONFLICTING_CONTEXT",
        )
        self.assertIn(
            "CKRS016",
            {item["rule"] for item in actual_main_payload["violations"]},
        )

        git(self.root, "checkout", "--detach", candidate)

        contract = (
            self.root
            / "docs/release/v1.13/v1.13.17-release-state-validator-contract.md"
        )
        original = contract.read_text(encoding="utf-8")
        try:
            for name, replacement in (
                ("missing", ""),
                (
                    "malformed",
                    "**Lifecycle declaration model:** immutable-transition-v2\n",
                ),
            ):
                with self.subTest(declaration=name):
                    contract.write_text(
                        original.replace(
                            "**Lifecycle declaration model:** immutable-transition-v1\n",
                            replacement,
                            1,
                        ),
                        encoding="utf-8",
                    )
                    automatic = self.validator("auto", valid)
                    self.assertEqual(
                        automatic.returncode,
                        2,
                        automatic.stdout + automatic.stderr,
                    )
                    self.assertEqual(
                        json.loads(automatic.stdout)["error"]["kind"],
                        "git-context",
                    )
                    explicit = self.validator("development", valid)
                    self.assertEqual(
                        explicit.returncode,
                        1,
                        explicit.stdout + explicit.stderr,
                    )
                    self.assertIn(
                        "CKRS019",
                        {
                            item["rule"]
                            for item in json.loads(explicit.stdout)["violations"]
                        },
                    )
        finally:
            contract.write_text(original, encoding="utf-8")

    def test_detached_auto_release_push_rechecks_event_for_ckrs016(self) -> None:
        candidate = self.add_development_history()
        environment = self.release_push_environment("0" * 40, candidate, True)
        event = Path(environment["GITHUB_EVENT_PATH"])
        git(self.root, "checkout", "--detach", candidate)
        actual_environment = os.environ.copy()
        for key in GITHUB_KEYS:
            actual_environment.pop(key, None)
        actual_environment.update(environment)
        original_load = release_state_support._load_event
        release_push_reads = 0

        def mutate_after_inference(*args: object, **kwargs: object) -> object:
            nonlocal release_push_reads
            payload = original_load(*args, **kwargs)
            if kwargs.get("predicate_prefix") == "release_push":
                release_push_reads += 1
                if release_push_reads == 1:
                    changed = json.loads(event.read_text(encoding="utf-8"))
                    changed["forced"] = True
                    event.write_text(json.dumps(changed), encoding="utf-8")
            return payload

        with mock.patch.dict(os.environ, actual_environment, clear=True), mock.patch.object(
            release_state_support,
            "_load_event",
            side_effect=mutate_after_inference,
        ):
            result = validate_release_state.validate(self.root, "auto")
        self.assertEqual(release_push_reads, 2)
        self.assertEqual(result.state, "development")
        self.assertEqual(
            result.evidence_scope,
            "GITHUB_RELEASE_PUSH_CONTEXT_CONSISTENCY",
        )
        self.assertIn("CKRS016", {item.rule for item in result.violations})

    def test_release_branch_push_diagnostic_compatibility(self) -> None:
        candidate = self.add_development_history()
        git(self.root, "checkout", "--detach", candidate)
        cases = (
            ("creation", "0" * 40, True, {}, True),
            ("update", self.predecessor, False, {}, True),
            ("crossed", "0" * 40, False, {}, False),
            ("creation-wrong-repository", "0" * 40, True, {
                "GITHUB_REPOSITORY": "fork/coldkeep",
            }, False),
        )
        for mode in ("auto", "development"):
            for name, before, created, env_updates, accepted in cases:
                environment = self.release_push_environment(
                    before,
                    candidate,
                    created,
                    environment_updates=env_updates,
                )
                inactive = self.fixture_validator(mode, environment)
                destination = self.root / (
                    f"TEST_FIXTURE_ONLY-{mode}-{name}-diagnostic.json"
                )
                active = self.fixture_validator(
                    mode,
                    environment,
                    "--diagnostic-json", str(destination),
                )
                with self.subTest(mode=mode, case=name):
                    self.assertEqual(active.returncode, inactive.returncode)
                    self.assertEqual(active.stdout, inactive.stdout)
                    self.assertEqual(active.stderr, inactive.stderr)
                    diagnostic = json.loads(
                        destination.read_text(encoding="utf-8")
                    )
                    self.assertEqual(
                        diagnostic["schema"],
                        "coldkeep-release-state-diagnostic/v3",
                    )
                    self.assertFalse(diagnostic["capture"]["complete"])
                    self.assertEqual(
                        diagnostic["validator"]["exit_code"], active.returncode
                    )
                    self.assertEqual(
                        diagnostic["validator"]["result"],
                        "ok" if accepted else "error",
                    )
                    self.assertEqual(
                        diagnostic["validator"]["resolved_state"],
                        None if mode == "auto" and not accepted else "development",
                    )
                    evaluations = [
                        item for item in diagnostic["evaluations"]
                        if item["route"] == "release-push"
                    ]
                    expected_sites = (
                        ["inference-release-push", "ckrs016-release-push"]
                        if mode == "auto" and accepted else
                        ["inference-release-push"]
                        if mode == "auto" else
                        ["explicit-state-release-push", "ckrs016-release-push"]
                    )
                    self.assertEqual(
                        [item["site"] for item in evaluations], expected_sites
                    )
                    snapshot_ids = [
                        item["event_snapshot_id"] for item in evaluations
                    ]
                    self.assertTrue(all(item is not None for item in snapshot_ids))
                    self.assertEqual(len(set(snapshot_ids)), len(snapshot_ids))
                    for evaluation in evaluations:
                        self.assertEqual(len(evaluation["predicates"]), 5)
                        self.assertEqual(
                            evaluation["result"],
                            "PASS" if accepted else "FAIL",
                        )
                        self.assertEqual(
                            evaluation["first_rejecting_predicate"],
                            None if accepted else "release_push.context",
                        )
                        self.assertTrue(
                            all(
                                item["source_path"]
                                == "scripts/release_state_support.py"
                                and item["source_sha256"] is not None
                                for item in evaluation["predicates"]
                            )
                        )
                    for source in diagnostic["validator"]["sources"]:
                        source_path = self.root / source["path"]
                        if source_path.is_file():
                            self.assertTrue(source["loaded_path_match"])
                            self.assertTrue(source["matches_git_blob"])
                            self.assertEqual(
                                source["git_blob"], source["measured_git_blob"]
                            )
                            self.assertEqual(
                                source["measured_sha256"],
                                hashlib.sha256(source_path.read_bytes()).hexdigest(),
                            )
                        else:
                            self.assertIsNone(source["loaded_path_match"])

    def test_scenario_factory_is_source_phase_independent(self) -> None:
        self.prepare_development_candidate()
        self.assert_projection(
            self.validator("development"), "development", "LOCAL_RELEASE_BRANCH"
        )
        self.assertEqual(validate_governance.validate(self.root), [])
        canonical = self.root / validate_governance.CANONICAL_CURRENT_STATE_FILE
        development = canonical.read_text(encoding="utf-8")
        self.assertIn("CURRENT_PHASE: 2_NEXT", development)
        self.assertIn("TRACKED_PUBLICATION_MATERIAL: ABSENT", development)
        self.assertIn("RELEASE_BODY_SHA256: ABSENT", development)
        self.assertFalse((self.root / validate_governance.CURRENT_RELEASE_BODY).exists())
        self.assertFalse(
            (self.root / validate_governance.CURRENT_RELEASE_BODY_CHECKSUM).exists()
        )

        self.freeze_candidate()
        first_digest = hashlib.sha256(
            (self.root / validate_governance.CURRENT_RELEASE_BODY).read_bytes()
        ).hexdigest()
        self.set_canonical_value("RELEASE_BODY_SHA256", "1" * 64)
        self.freeze_candidate()
        frozen = canonical.read_text(encoding="utf-8")
        self.assertIn(f"RELEASE_BODY_SHA256: {first_digest}", frozen)
        self.assertNotIn(f"RELEASE_BODY_SHA256: {'1' * 64}", frozen)
        self.assert_projection(
            self.validator("pre-release"), "pre-release", "LOCAL_RELEASE_BRANCH"
        )
        self.assertEqual(validate_governance.validate(self.root), [])

        self.prepare_development_candidate()
        self.assert_projection(
            self.validator("development"), "development", "LOCAL_RELEASE_BRANCH"
        )
        self.freeze_candidate()
        self.set_phase_progression(9)
        self.set_tracker_status("Published; post-publication closure pending")
        self.set_state_values({
            "CURRENT_PHASE": "9_NEXT",
            "V1_13_17_STATE": "PUBLISHED_CLOSURE_PENDING",
        })
        phase_nine = canonical.read_text(encoding="utf-8")
        self.assertIn("CURRENT_PHASE: 9_NEXT", phase_nine)
        self.assertIn("TRACKED_PUBLICATION_MATERIAL: FROZEN", phase_nine)
        self.set_phase_progression(None)
        self.set_state_values({
            "CURRENT_PHASE": "NONE_CLOSURE_CANDIDATE",
            "V1_13_17_STATE": "CLOSURE_CANDIDATE_PENDING_TERMINAL_AUDIT",
        })
        self.assertIn(
            "CURRENT_PHASE: NONE_CLOSURE_CANDIDATE",
            canonical.read_text(encoding="utf-8"),
        )

        self.freeze_candidate()
        checksum = self.root / validate_governance.CURRENT_RELEASE_BODY_CHECKSUM
        checksum.write_text(
            "0" * 64
            + f"  {validate_governance.CURRENT_RELEASE_BODY.as_posix()}\n",
            encoding="ascii",
        )
        self.assertTrue(
            any(
                "bytes do not match body digest and exact path" in item
                for item in validate_governance.validate(self.root)
            )
        )

    def test_attached_closure_push_creation_direct_child_red_green(self) -> None:
        published_merge, closure_candidate = self.prepare_direct_closure_candidate()
        for mode in ("auto", "post-release-closure-candidate"):
            self.assert_projection(
                self.validator(mode),
                "post-release-closure-candidate",
                "LOCAL_CLOSURE_ARTIFACT_ONLY",
            )
        self.assertEqual(validate_governance.validate(self.root), [])
        environment = self.closure_push_environment(
            "0" * 40, closure_candidate, True
        )
        for mode in ("auto", "post-release-closure-candidate"):
            with self.subTest(mode=mode):
                self.assert_projection(
                    self.validator(mode, environment),
                    "post-release-closure-candidate",
                    "GITHUB_CLOSURE_PUSH_CONTEXT_CONSISTENCY",
                )
        self.assertTrue(
            run_process(
                [
                    resolved_executable("git"), "-C", str(self.root),
                    "merge-base", "--is-ancestor", published_merge,
                    closure_candidate,
                ],
                check=False,
            ).returncode == 0
        )

    def test_attached_closure_push_eight_positive_matrix_and_controls(self) -> None:
        published_merge, closure_candidate = self.prepare_direct_closure_candidate()
        transitions = (
            ("creation", "0" * 40, True),
            ("update-unavailable-before", "a" * 40, False),
        )
        for name, before, created in transitions:
            environment = self.closure_push_environment(
                before, closure_candidate, created
            )
            for mode in ("auto", "post-release-closure-candidate"):
                with self.subTest(state="candidate", transition=name, mode=mode):
                    self.assert_projection(
                        self.validator(mode, environment),
                        "post-release-closure-candidate",
                        "GITHUB_CLOSURE_PUSH_CONTEXT_CONSISTENCY",
                    )
            human = run_validator(
                self.root,
                "--state", "post-release-closure-candidate",
                env=environment,
            )
            self.assertEqual(human.returncode, 0, human.stdout + human.stderr)
            self.assertIn("state=post-release-closure-candidate", human.stdout)

        pr_environment = self.closure_pr_environment(
            published_merge, closure_candidate
        )
        for mode in ("auto", "post-release-closure-candidate"):
            with self.subTest(state="candidate", context="pr", mode=mode):
                self.assert_projection(
                    self.validator(mode, pr_environment),
                    "post-release-closure-candidate",
                    "GITHUB_CLOSURE_PR_CONTEXT_CONSISTENCY",
                )

        git(self.root, "checkout", "main")
        git(
            self.root, "merge", "--no-ff",
            "release/v1.13.17-post-publication-closure",
            "-m", "TEST_FIXTURE_ONLY closure main",
        )
        closure_merge = self.rev_parse("HEAD")
        self.assertEqual(
            run_process(
                [
                    resolved_executable("git"), "-C", str(self.root),
                    "rev-list", "--parents", "-n", "1", closure_merge,
                ],
                check=True,
            ).stdout.strip().split()[1:],
            [published_merge, closure_candidate],
        )
        self.assertEqual(
            self.rev_parse(f"{closure_merge}^{{tree}}"),
            self.rev_parse(f"{closure_candidate}^{{tree}}"),
        )
        git(self.root, "checkout", "--detach", closure_merge)
        synthetic_pr_environment = self.closure_pr_environment(
            published_merge,
            closure_candidate,
            number=904,
            checkout_sha=closure_merge,
            merge_commit_sha=closure_merge,
        )
        for mode in ("auto", "post-release-closure-candidate"):
            with self.subTest(state="candidate", context="synthetic-pr", mode=mode):
                self.assert_projection(
                    self.validator(mode, synthetic_pr_environment),
                    "post-release-closure-candidate",
                    "GITHUB_CLOSURE_PR_CONTEXT_CONSISTENCY",
                )
        main_environment = self.main_environment(published_merge, closure_merge)
        for mode in ("auto", "post-release-closure-candidate"):
            with self.subTest(state="candidate", context="main", mode=mode):
                self.assert_projection(
                    self.validator(mode, main_environment),
                    "post-release-closure-candidate",
                    "GITHUB_CLOSURE_MAIN_CONTEXT_CONSISTENCY",
                )

    def test_attached_pending_closure_push_matrix_and_controls(self) -> None:
        published_merge, closure_pending = self.prepare_direct_closure_candidate(
            candidate=False
        )
        for mode in ("auto", "post-release-pending-closure"):
            self.assert_projection(
                self.validator(mode),
                "post-release-pending-closure",
                "LOCAL_CLOSURE_ARTIFACT_ONLY",
            )
        self.assertEqual(validate_governance.validate(self.root), [])
        for name, before, created in (
            ("creation", "0" * 40, True),
            ("update-unavailable-before", "b" * 40, False),
        ):
            environment = self.closure_push_environment(
                before, closure_pending, created
            )
            for mode in ("auto", "post-release-pending-closure"):
                with self.subTest(state="pending", transition=name, mode=mode):
                    self.assert_projection(
                        self.validator(mode, environment),
                        "post-release-pending-closure",
                        "GITHUB_CLOSURE_PUSH_CONTEXT_CONSISTENCY",
                    )
        pr_environment = self.closure_pr_environment(
            published_merge, closure_pending, number=903
        )
        for mode in ("auto", "post-release-pending-closure"):
            with self.subTest(state="pending", context="pr", mode=mode):
                self.assert_projection(
                    self.validator(mode, pr_environment),
                    "post-release-pending-closure",
                    "GITHUB_CLOSURE_PR_CONTEXT_CONSISTENCY",
                )
        git(self.root, "checkout", "main")
        git(
            self.root, "merge", "--no-ff",
            "release/v1.13.17-post-publication-closure",
            "-m", "TEST_FIXTURE_ONLY pending closure main",
        )
        closure_merge = self.rev_parse("HEAD")
        self.assertEqual(
            run_process(
                [
                    resolved_executable("git"), "-C", str(self.root),
                    "rev-list", "--parents", "-n", "1", closure_merge,
                ],
                check=True,
            ).stdout.strip().split()[1:],
            [published_merge, closure_pending],
        )
        self.assertEqual(
            self.rev_parse(f"{closure_merge}^{{tree}}"),
            self.rev_parse(f"{closure_pending}^{{tree}}"),
        )
        git(self.root, "checkout", "--detach", closure_merge)
        synthetic_pr_environment = self.closure_pr_environment(
            published_merge,
            closure_pending,
            number=905,
            checkout_sha=closure_merge,
            merge_commit_sha=closure_merge,
        )
        for mode in ("auto", "post-release-pending-closure"):
            with self.subTest(state="pending", context="synthetic-pr", mode=mode):
                self.assert_projection(
                    self.validator(mode, synthetic_pr_environment),
                    "post-release-pending-closure",
                    "GITHUB_CLOSURE_PR_CONTEXT_CONSISTENCY",
                )
        main_environment = self.main_environment(published_merge, closure_merge)
        for mode in ("auto", "post-release-pending-closure"):
            with self.subTest(state="pending", context="main", mode=mode):
                self.assert_projection(
                    self.validator(mode, main_environment),
                    "post-release-pending-closure",
                    "GITHUB_CLOSURE_MAIN_CONTEXT_CONSISTENCY",
                )

    def test_attached_closure_push_field_and_event_negatives(self) -> None:
        _, closure_candidate = self.prepare_direct_closure_candidate()

        def rejected(environment: dict[str, str]) -> None:
            process = self.validator(
                "post-release-closure-candidate", environment
            )
            self.assertEqual(process.returncode, 1, process.stdout + process.stderr)
            payload = json.loads(process.stdout)
            self.assertEqual(
                payload["evidence_scope"], "UNSUPPORTED_OR_CONFLICTING_CONTEXT"
            )
            self.assertIn(
                "CKRS016", {item["rule"] for item in payload["violations"]}
            )

        payload_cases = (
            ("crossed-creation", "0" * 40, False, {}, ()),
            ("crossed-update", "a" * 40, True, {}, ()),
            ("created-string", "0" * 40, "true", {}, ()),
            ("before-number", 0, True, {}, ()),
            ("before-uppercase", "A" * 40, False, {}, ()),
            ("deleted-true", "0" * 40, True, {"deleted": True}, ()),
            ("deleted-string", "0" * 40, True, {"deleted": "false"}, ()),
            ("forced-true", "0" * 40, True, {"forced": True}, ()),
            ("forced-number", "0" * 40, True, {"forced": 0}, ()),
            ("after-short", "0" * 40, True, {"after": "a" * 39}, ()),
            ("after-uppercase", "0" * 40, True, {"after": "A" * 40}, ()),
            ("event-ref", "0" * 40, True, {"ref": "refs/heads/main"}, ()),
            ("payload-repository", "0" * 40, True, {"repository": {"full_name": "fork/coldkeep"}}, ()),
            ("missing-created", "0" * 40, True, {}, ("created",)),
            ("missing-deleted", "0" * 40, True, {}, ("deleted",)),
            ("missing-forced", "0" * 40, True, {}, ("forced",)),
        )
        for name, before, created, updates, remove in payload_cases:
            with self.subTest(payload=name):
                rejected(self.closure_push_environment(
                    before, closure_candidate, created,
                    payload_updates=updates, payload_remove=remove,
                ))
        environment_cases = (
            ("actions", {"GITHUB_ACTIONS": "false"}),
            ("event", {"GITHUB_EVENT_NAME": "create"}),
            ("full-ref", {"GITHUB_REF": "refs/heads/main"}),
            ("short-ref", {"GITHUB_REF_NAME": "main"}),
            ("ref-type", {"GITHUB_REF_TYPE": "tag"}),
            ("repository", {"GITHUB_REPOSITORY": "fork/coldkeep"}),
            ("runtime-short", {"GITHUB_SHA": "a" * 39}),
            ("runtime-uppercase", {"GITHUB_SHA": "A" * 40}),
            ("runtime-other", {"GITHUB_SHA": "a" * 40}),
            ("event-path", {"GITHUB_EVENT_PATH": ""}),
        )
        for name, updates in environment_cases:
            with self.subTest(environment=name):
                rejected(self.closure_push_environment(
                    "0" * 40, closure_candidate, True,
                    environment_updates=updates,
                ))
        for name, raw in (("malformed", "{"), ("non-object", "[]")):
            with self.subTest(event=name):
                rejected(self.closure_push_environment(
                    "0" * 40, closure_candidate, True, raw_event=raw
                ))
        event_limit = 1048576
        boundary_payload = {
            "ref": "refs/heads/release/v1.13.17-post-publication-closure",
            "before": "0" * 40,
            "after": closure_candidate,
            "created": True,
            "deleted": False,
            "forced": False,
            "repository": {"full_name": "franchoy/coldkeep"},
            "padding": "",
        }
        empty_padding = json.dumps(boundary_payload, separators=(",", ":"))
        boundary_payload["padding"] = "x" * (
            event_limit - len(empty_padding.encode("utf-8"))
        )
        boundary_event = json.dumps(boundary_payload, separators=(",", ":"))
        self.assertEqual(len(boundary_event.encode("utf-8")), event_limit)
        self.assert_projection(
            self.validator(
                "post-release-closure-candidate",
                self.closure_push_environment(
                    "0" * 40,
                    closure_candidate,
                    True,
                    raw_event=boundary_event,
                ),
            ),
            "post-release-closure-candidate",
            "GITHUB_CLOSURE_PUSH_CONTEXT_CONSISTENCY",
        )
        oversized = self.closure_push_environment(
            "0" * 40, closure_candidate, True,
            raw_event=f"{boundary_event} ",
        )
        self.assertEqual(
            len(Path(oversized["GITHUB_EVENT_PATH"]).read_bytes()),
            event_limit + 1,
        )
        rejected(oversized)
        missing = self.closure_push_environment(
            "0" * 40, closure_candidate, True
        )
        Path(missing["GITHUB_EVENT_PATH"]).unlink()
        rejected(missing)
        for name, before, created in (
            ("creation", "0" * 40, True),
            ("update-unavailable-before", "c" * 40, False),
        ):
            with self.subTest(restored=name):
                self.assert_projection(
                    self.validator(
                        "post-release-closure-candidate",
                        self.closure_push_environment(
                            before, closure_candidate, created
                        ),
                    ),
                    "post-release-closure-candidate",
                    "GITHUB_CLOSURE_PUSH_CONTEXT_CONSISTENCY",
                )

    def test_attached_closure_push_checkout_tag_and_isolation_negatives(self) -> None:
        published_merge, closure_candidate = self.prepare_direct_closure_candidate()
        environment = self.closure_push_environment(
            "0" * 40, closure_candidate, True
        )

        def rejected(current: dict[str, str] = environment) -> None:
            process = self.validator("post-release-closure-candidate", current)
            self.assertEqual(process.returncode, 1, process.stdout + process.stderr)
            self.assertIn(
                "CKRS016",
                {item["rule"] for item in json.loads(process.stdout)["violations"]},
            )

        git(self.root, "checkout", "-b", "TEST_FIXTURE_ONLY-wrong-branch")
        rejected()
        git(self.root, "checkout", "--detach", closure_candidate)
        rejected()
        git(self.root, "checkout", "--detach", published_merge)
        rejected()
        git(
            self.root, "checkout",
            "release/v1.13.17-post-publication-closure",
        )
        isolated = dict(environment)
        isolated["GIT_ALTERNATE_OBJECT_DIRECTORIES"] = str(
            self.root / "TEST_FIXTURE_ONLY-alternates"
        )
        rejected(isolated)

        git(self.root, "tag", "-d", "v1.13.17")
        rejected()
        git(self.root, "tag", "v1.13.17", published_merge)
        rejected()
        git(self.root, "tag", "-d", "v1.13.17")
        git(self.root, "tag", "-a", "v1.13.17", closure_candidate, "-m", "TEST_FIXTURE_ONLY equal")
        rejected()
        git(self.root, "tag", "-d", "v1.13.17")
        unrelated = run_process(
            [
                resolved_executable("git"), "-C", str(self.root),
                "commit-tree", f"{published_merge}^{{tree}}",
                "-m", "TEST_FIXTURE_ONLY unrelated tag target",
            ],
            check=True,
        ).stdout.strip()
        git(self.root, "tag", "-a", "v1.13.17", unrelated, "-m", "TEST_FIXTURE_ONLY unrelated")
        rejected()
        git(self.root, "tag", "-d", "v1.13.17")
        blob = self.rev_parse("HEAD:README.md")
        git(self.root, "tag", "-a", "v1.13.17", blob, "-m", "TEST_FIXTURE_ONLY blob")
        wrong_type = self.validator("post-release-closure-candidate", environment)
        self.assertEqual(wrong_type.returncode, 2, wrong_type.stdout + wrong_type.stderr)
        self.assertEqual(json.loads(wrong_type.stdout)["error"]["kind"], "git")
        git(self.root, "tag", "-d", "v1.13.17")
        git(self.root, "tag", "-a", "TEST_FIXTURE_ONLY-inner", published_merge, "-m", "TEST_FIXTURE_ONLY inner")
        inner = self.rev_parse("refs/tags/TEST_FIXTURE_ONLY-inner")
        git(self.root, "tag", "-a", "v1.13.17", inner, "-m", "TEST_FIXTURE_ONLY nested")
        rejected()
        git(self.root, "tag", "-d", "v1.13.17")
        git(self.root, "tag", "-a", "v1.13.17", published_merge, "-m", "TEST_FIXTURE_ONLY restored")
        self.assert_projection(
            self.validator("post-release-closure-candidate", environment),
            "post-release-closure-candidate",
            "GITHUB_CLOSURE_PUSH_CONTEXT_CONSISTENCY",
        )

    def test_attached_closure_push_rechecks_event_and_tag_for_ckrs016(self) -> None:
        _, closure_candidate = self.prepare_direct_closure_candidate()
        environment = self.closure_push_environment(
            "0" * 40, closure_candidate, True
        )
        event = Path(environment["GITHUB_EVENT_PATH"])
        actual_environment = os.environ.copy()
        for key in GITHUB_KEYS:
            actual_environment.pop(key, None)
        actual_environment.update(environment)
        original_load = release_state_support._load_event
        reads = 0

        def mutate_event(*args: object, **kwargs: object) -> object:
            nonlocal reads
            payload = original_load(*args, **kwargs)
            if kwargs.get("bounded"):
                reads += 1
                if reads == 1:
                    changed = json.loads(event.read_text(encoding="utf-8"))
                    changed["forced"] = True
                    event.write_text(json.dumps(changed), encoding="utf-8")
            return payload

        with mock.patch.dict(os.environ, actual_environment, clear=True), mock.patch.object(
            release_state_support, "_load_event", side_effect=mutate_event
        ):
            result = validate_release_state.validate(self.root, "auto")
        self.assertEqual(reads, 2)
        self.assertEqual(result.state, "post-release-closure-candidate")
        self.assertEqual(
            result.evidence_scope,
            "GITHUB_CLOSURE_PUSH_CONTEXT_CONSISTENCY",
        )
        self.assertIn("CKRS016", {item.rule for item in result.violations})

        event.write_text(json.dumps({
            "ref": "refs/heads/release/v1.13.17-post-publication-closure",
            "before": "0" * 40,
            "after": closure_candidate,
            "created": True,
            "deleted": False,
            "forced": False,
            "repository": {"full_name": "franchoy/coldkeep"},
        }), encoding="utf-8")
        original_closure = release_state_support.strict_closure_push_context
        evaluations = 0

        def mutate_tag(*args: object, **kwargs: object) -> object:
            nonlocal evaluations
            answer = original_closure(*args, **kwargs)
            evaluations += 1
            if evaluations == 1:
                git(self.root, "tag", "-f", "-a", "v1.13.17", closure_candidate, "-m", "TEST_FIXTURE_ONLY changed")
            return answer

        with mock.patch.dict(os.environ, actual_environment, clear=True), mock.patch.object(
            release_state_support,
            "strict_closure_push_context",
            side_effect=mutate_tag,
        ):
            result = validate_release_state.validate(self.root, "auto")
        self.assertEqual(evaluations, 2)
        self.assertEqual(
            result.evidence_scope,
            "GITHUB_CLOSURE_PUSH_CONTEXT_CONSISTENCY",
        )
        self.assertIn("CKRS016", {item.rule for item in result.violations})

    def test_attached_closure_push_diagnostic_compatibility(self) -> None:
        _, closure_candidate = self.prepare_direct_closure_candidate()
        for transition, before, created in (
            ("creation", "0" * 40, True),
            ("update", "d" * 40, False),
        ):
            environment = self.closure_push_environment(
                before, closure_candidate, created
            )
            for mode in ("auto", "post-release-closure-candidate"):
                inactive = self.fixture_validator(mode, environment)
                destination = self.root / (
                    "TEST_FIXTURE_ONLY-closure-"
                    f"{transition}-{mode}-diagnostic.json"
                )
                active = self.fixture_validator(
                    mode, environment,
                    "--diagnostic-json", str(destination),
                )
                with self.subTest(transition=transition, mode=mode):
                    self.assertEqual(active.returncode, inactive.returncode)
                    self.assertEqual(active.stdout, inactive.stdout)
                    self.assertEqual(active.stderr, inactive.stderr)
                    diagnostic = json.loads(destination.read_text(encoding="utf-8"))
                    self.assertEqual(
                        diagnostic["schema"],
                        "coldkeep-release-state-diagnostic/v3",
                    )
                    self.assertFalse(diagnostic["capture"]["complete"])
                    self.assertEqual(diagnostic["validator"]["exit_code"], 0)
                    self.assertEqual(diagnostic["validator"]["result"], "ok")
                    self.assertEqual(diagnostic["evaluations"], [])
                    self.assertEqual(diagnostic["event_snapshots"], [])
                    for source in diagnostic["validator"]["sources"]:
                        if (self.root / source["path"]).is_file():
                            self.assertTrue(source["loaded_path_match"])
                            self.assertTrue(source["matches_git_blob"])
                        else:
                            self.assertIsNone(source["loaded_path_match"])

    def test_complete_successor_development_candidate_merge_tag_and_closure(self) -> None:
        for mode in ("auto", "development"):
            with self.subTest(checkpoint="development", mode=mode):
                self.assert_projection(
                    self.validator(mode), "development", "LOCAL_RELEASE_BRANCH"
                )
        self.assertEqual(validate_governance.validate(self.root), [])
        self.assertFalse((self.root / validate_governance.CURRENT_RELEASE_BODY).exists())
        self.assertNotEqual(self.rev_parse("refs/tags/v1.13.16^{}"), self.rev_parse("HEAD"))

        git(self.root, "branch", "main", self.predecessor)
        self.freeze_candidate()
        git(self.root, "add", ".")
        git(self.root, "commit", "-m", "TEST_FIXTURE_ONLY final candidate C")
        candidate = self.rev_parse("HEAD")
        for mode in ("auto", "pre-release"):
            with self.subTest(checkpoint="pre-release", mode=mode):
                self.assert_projection(
                    self.validator(mode), "pre-release", "LOCAL_RELEASE_BRANCH"
                )
        self.assertEqual(validate_governance.validate(self.root), [])

        relevant = (
            "CHANGELOG.md", "README.md", "SECURITY.md",
            "docs/release/v1.13/README.md",
            "docs/release/v1.13/v1.13.17-scope.md",
            "docs/release/v1.13/v1.13.17-phase-list.md",
            "docs/release/v1.13/v1.13.17-validation-checklist.md",
            "docs/release/v1.13/v1.13.17-release-state.md",
            "docs/release/v1.13/v1.13.17-release-gate.md",
            validate_governance.CURRENT_RELEASE_BODY.as_posix(),
            validate_governance.CURRENT_RELEASE_BODY_CHECKSUM.as_posix(),
        )
        candidate_blobs = {
            path: self.rev_parse(f"{candidate}:{path}") for path in relevant
        }
        git(self.root, "checkout", "main")
        git(
            self.root, "merge", "--no-ff", "release/v1.13.17",
            "-m", "TEST_FIXTURE_ONLY normal merge M",
        )
        merge = self.rev_parse("HEAD")
        self.assertEqual(
            run_process(
                [resolved_executable("git"), "-C", str(self.root), "rev-list", "--parents", "-n", "1", merge],
                check=True,
            ).stdout.strip().split()[1:],
            [self.predecessor, candidate],
        )
        self.assertEqual(self.rev_parse(f"{merge}^{{tree}}"), self.rev_parse(f"{candidate}^{{tree}}"))
        self.assertEqual(
            {path: self.rev_parse(f"{merge}:{path}") for path in relevant},
            candidate_blobs,
        )
        self.assertFalse(
            run_process(
                [resolved_executable("git"), "-C", str(self.root), "show-ref", "--verify", "--quiet", "refs/tags/v1.13.17"],
                check=False,
            ).returncode == 0
        )
        git(self.root, "checkout", "--detach", merge)
        pending_environment = self.main_environment(self.predecessor, merge)
        for mode in ("auto", "merged-pending-final-main-certification"):
            with self.subTest(checkpoint="pending-main", mode=mode):
                self.assert_projection(
                    self.validator(mode, pending_environment),
                    "merged-pending-final-main-certification",
                    "GITHUB_MAIN_PUSH_CONTEXT_CONSISTENCY",
                )
        git(self.root, "tag", "-a", "v1.13.17", "-m", "TEST_FIXTURE_ONLY")
        tag_object = self.rev_parse("refs/tags/v1.13.17")
        self.assertEqual(self.rev_parse("refs/tags/v1.13.17^{}"), merge)
        self.assertEqual(validate_governance.validate(self.root), [])

        for runtime_sha, event_after in (
            (merge, merge), (merge, tag_object),
            (tag_object, merge), (tag_object, tag_object),
        ):
            for mode in ("auto", "tagged-pending-tag-certification"):
                with self.subTest(runtime_sha=runtime_sha, event_after=event_after, mode=mode):
                    process = self.validator(
                        mode, self.tag_environment(runtime_sha, event_after)
                    )
                    self.assert_projection(
                        process,
                        "tagged-pending-tag-certification",
                        "GITHUB_TAG_EVENT_CONTEXT_CONSISTENCY",
                    )

        git(self.root, "checkout", "-b", "release/v1.13.17-post-publication-closure")
        self.set_phase_progression(None)
        self.set_tracker_status("Published; post-publication closure pending")
        self.set_state_values({
            "CURRENT_PHASE": "NONE_CLOSURE_CANDIDATE",
            "V1_13_17_STATE": "CLOSURE_CANDIDATE_PENDING_TERMINAL_AUDIT",
        })
        for relative, old, new in (
            ("README.md", "v1.13.17%20ready", "v1.13.17%20published"),
            ("README.md", "is the ready Route C", "is the published Route C"),
            ("docs/release/v1.13/README.md", "v1.13.17 is the ready Route C", "v1.13.17 is the published Route C"),
        ):
            path = self.root / relative
            path.write_text(path.read_text(encoding="utf-8").replace(old, new, 1), encoding="utf-8")
        gate = self.root / "docs/release/v1.13/v1.13.17-release-gate.md"
        gate.write_text(
            "# v1.13.17 Release Gate\n\n"
            "**Status:** Passed and released — closure pending\n\n"
            "## Final verdict\n\nPASS — PUBLICATION COMPLETE; TERMINAL AUDIT PENDING\n",
            encoding="utf-8",
        )
        git(self.root, "add", ".")
        git(self.root, "commit", "-m", "TEST_FIXTURE_ONLY closure candidate")
        closure_candidate = self.rev_parse("HEAD")
        for mode in ("auto", "post-release-closure-candidate"):
            with self.subTest(checkpoint="closure-branch", mode=mode):
                self.assert_projection(
                    self.validator(mode),
                    "post-release-closure-candidate",
                    "LOCAL_CLOSURE_ARTIFACT_ONLY",
                )
        self.assertEqual(validate_governance.validate(self.root), [])
        git(self.root, "checkout", "main")
        git(
            self.root, "merge", "--no-ff", "release/v1.13.17-post-publication-closure",
            "-m", "TEST_FIXTURE_ONLY closure main",
        )
        closure_merge = self.rev_parse("HEAD")
        git(self.root, "checkout", "--detach", closure_merge)
        environment = self.main_environment(merge, closure_merge)
        for mode in ("auto", "post-release-closure-candidate"):
            with self.subTest(checkpoint="closure-main", mode=mode):
                self.assert_projection(
                    self.validator(mode, environment),
                    "post-release-closure-candidate",
                    "GITHUB_CLOSURE_MAIN_CONTEXT_CONSISTENCY",
                )
        terminal = self.validator("post-release-closed", environment)
        self.assertEqual(terminal.returncode, 1, terminal.stdout + terminal.stderr)
        terminal_payload = json.loads(terminal.stdout)
        self.assertEqual(terminal_payload["state"], "post-release-closed")
        self.assertIn(
            "CKRS019", {item["rule"] for item in terminal_payload["violations"]}
        )
        self.assertNotEqual(self.rev_parse(f"{closure_candidate}^{{tree}}"), self.rev_parse(f"{merge}^{{tree}}"))
        self.assertEqual(self.rev_parse("refs/tags/v1.13.17^{}"), merge)

    def test_complete_successor_semantic_negatives(self) -> None:
        def rules(process: ProcessResult) -> set[str]:
            self.assertEqual(process.returncode, 1, process.stdout + process.stderr)
            return {item["rule"] for item in json.loads(process.stdout)["violations"]}

        disposition = self.root / "docs/release/v1.13/v1.13.17-predecessor-disposition.md"
        original_disposition = disposition.read_bytes()
        for name, mutation in (
            ("missing", None),
            ("malformed", b"TEST_FIXTURE_ONLY malformed\n"),
            (
                "contradictory",
                re.sub(
                    rb"(?m)^DIRECT_TARGET: [0-9a-f]{40}$",
                    b"DIRECT_TARGET: " + b"0" * 40,
                    original_disposition,
                    count=1,
                ),
            ),
        ):
            with self.subTest(checkpoint="predecessor", case=name):
                if disposition.exists():
                    disposition.unlink()
                if mutation is not None:
                    disposition.write_bytes(mutation)
                self.assertIn("CKRS010", rules(self.validator("development")))
                disposition.write_bytes(original_disposition)

        contract = self.root / "docs/release/v1.13/v1.13.17-release-state-validator-contract.md"
        original_contract = contract.read_bytes()
        contract.write_bytes(
            original_contract.replace(
                b"**Predecessor disposition model:** failed-publication-predecessor-v1\n",
                b"",
                1,
            )
        )
        ordinary_fallback = rules(self.validator("development"))
        self.assertIn("CKRS019", ordinary_fallback)
        self.assertNotIn("CKRS010", ordinary_fallback)
        contract.write_bytes(original_contract)

        git(self.root, "branch", "main", self.predecessor)
        self.freeze_candidate()
        body = self.root / validate_governance.CURRENT_RELEASE_BODY
        checksum = self.root / validate_governance.CURRENT_RELEASE_BODY_CHECKSUM
        state = self.root / validate_governance.CANONICAL_CURRENT_STATE_FILE
        originals = (body.read_bytes(), checksum.read_bytes(), state.read_bytes())

        def restore_body() -> None:
            body.write_bytes(originals[0])
            checksum.write_bytes(originals[1])
            state.write_bytes(originals[2])

        body.write_bytes(body.read_bytes().replace(b"source-only", b"source only", 1))
        body_violations = validate_governance.validate(self.root)
        self.assertTrue(any("digest does not match" in item for item in body_violations))
        self.assertTrue(
            any("bytes do not match body digest and exact path" in item for item in body_violations)
        )
        restore_body()

        checksum.write_text(
            "0" * 64 + f"  {validate_governance.CURRENT_RELEASE_BODY.as_posix()}\n",
            encoding="ascii",
        )
        checksum_violations = validate_governance.validate(self.root)
        self.assertTrue(
            any("bytes do not match body digest and exact path" in item for item in checksum_violations)
        )
        self.assertFalse(any("digest does not match" in item for item in checksum_violations))
        restore_body()

        state.write_text(
            re.sub(
                r"(?m)^RELEASE_BODY_SHA256: [0-9a-f]{64}$",
                f"RELEASE_BODY_SHA256: {'0' * 64}",
                state.read_text(encoding="utf-8"),
                count=1,
            ),
            encoding="utf-8",
        )
        canonical_violations = validate_governance.validate(self.root)
        self.assertTrue(any("digest does not match" in item for item in canonical_violations))
        self.assertFalse(
            any("bytes do not match body digest and exact path" in item for item in canonical_violations)
        )
        restore_body()

        semantic_body = body.read_bytes().replace(b"source-only", b"archival", 1)
        semantic_digest = hashlib.sha256(semantic_body).hexdigest()
        body.write_bytes(semantic_body)
        checksum.write_text(
            f"{semantic_digest}  {validate_governance.CURRENT_RELEASE_BODY.as_posix()}\n",
            encoding="ascii",
        )
        state.write_text(
            re.sub(
                r"(?m)^RELEASE_BODY_SHA256: [0-9a-f]{64}$",
                f"RELEASE_BODY_SHA256: {semantic_digest}",
                state.read_text(encoding="utf-8"),
                count=1,
            ),
            encoding="utf-8",
        )
        semantic_violations = validate_governance.validate(self.root)
        self.assertTrue(
            any("missing semantic marker 'source-only'" in item for item in semantic_violations),
            semantic_violations,
        )
        self.assertFalse(
            any("digest" in item or "checksum" in item for item in semantic_violations),
            semantic_violations,
        )
        restore_body()

        mirror = self.root / "AGENTS.md"
        original_mirror = mirror.read_bytes()
        mirror.write_bytes(original_mirror.replace(b"CURRENT_PHASE: 5_NEXT", b"CURRENT_PHASE: 4_NEXT", 1))
        mirror_violations = validate_governance.validate(self.root)
        self.assertTrue(
            any("AGENTS.md" in item and "mirror" in item for item in mirror_violations),
            mirror_violations,
        )
        mirror.write_bytes(original_mirror)

        gate = self.root / "docs/release/v1.13/v1.13.17-release-gate.md"
        original_gate = gate.read_bytes()
        gate.write_bytes(original_gate.replace(b"Passed", b"Failed", 1))
        self.assertIn("CKRS018", rules(self.validator("pre-release")))
        gate.write_bytes(original_gate)

        git(self.root, "add", ".")
        git(self.root, "commit", "-m", "TEST_FIXTURE_ONLY final candidate C")
        candidate = self.rev_parse("HEAD")
        git(self.root, "checkout", "main")
        git(
            self.root, "merge", "--no-ff", "release/v1.13.17",
            "-m", "TEST_FIXTURE_ONLY normal merge M",
        )
        merge = self.rev_parse("HEAD")
        git(self.root, "checkout", "--detach", merge)
        git(self.root, "tag", "-a", "v1.13.17", "-m", "TEST_FIXTURE_ONLY")
        tag_object = self.rev_parse("refs/tags/v1.13.17")
        self.assert_projection(
            self.validator(
                "tagged-pending-tag-certification",
                self.tag_environment(tag_object, tag_object),
            ),
            "tagged-pending-tag-certification",
            "GITHUB_TAG_EVENT_CONTEXT_CONSISTENCY",
        )

        for name in ("wrong-after", "missing-created"):
            environment = self.tag_environment(
                merge, self.predecessor if name == "wrong-after" else tag_object
            )
            if name == "missing-created":
                event = Path(environment["GITHUB_EVENT_PATH"])
                payload = json.loads(event.read_text(encoding="utf-8"))
                del payload["created"]
                event.write_text(json.dumps(payload), encoding="utf-8")
            with self.subTest(checkpoint="tag-event", case=name):
                self.assertIn(
                    "CKRS016",
                    rules(self.validator("tagged-pending-tag-certification", environment)),
                )
                self.assert_projection(
                    self.validator(
                        "tagged-pending-tag-certification",
                        self.tag_environment(tag_object, tag_object),
                    ),
                    "tagged-pending-tag-certification",
                    "GITHUB_TAG_EVENT_CONTEXT_CONSISTENCY",
                )

        def commit_tree(tree: str, parents: tuple[str, ...], message: str) -> str:
            command = [resolved_executable("git"), "-C", str(self.root), "commit-tree", tree]
            for parent in parents:
                command.extend(("-p", parent))
            command.extend(("-m", message))
            return run_process(command, check=True).stdout.strip()

        candidate_tree = self.rev_parse(f"{candidate}^{{tree}}")
        predecessor_tree = self.rev_parse(f"{self.predecessor}^{{tree}}")
        unrelated = commit_tree(candidate_tree, (), "TEST_FIXTURE_ONLY unrelated")
        bad_commits = (
            ("wrong-order", commit_tree(candidate_tree, (candidate, self.predecessor), "TEST_FIXTURE_ONLY reversed")),
            ("nonancestor", commit_tree(candidate_tree, (self.predecessor, unrelated), "TEST_FIXTURE_ONLY nonancestor")),
            ("wrong-tree", commit_tree(predecessor_tree, (self.predecessor, candidate), "TEST_FIXTURE_ONLY wrong tree")),
        )
        for name, bad_commit in bad_commits:
            with self.subTest(checkpoint="tag-topology", case=name):
                self.assertEqual(
                    run_process(
                        [resolved_executable("git"), "-C", str(self.root), "cat-file", "-t", bad_commit],
                        check=True,
                    ).stdout.strip(),
                    "commit",
                )
                raw_commit = run_process(
                    [resolved_executable("git"), "-C", str(self.root), "cat-file", "-p", bad_commit],
                    check=True,
                ).stdout
                expected_parents = {
                    "wrong-order": [candidate, self.predecessor],
                    "nonancestor": [self.predecessor, unrelated],
                    "wrong-tree": [self.predecessor, candidate],
                }[name]
                self.assertEqual(
                    re.findall(r"(?m)^parent ([0-9a-f]{40})$", raw_commit),
                    expected_parents,
                )
                self.assertEqual(
                    re.search(r"(?m)^tree ([0-9a-f]{40})$", raw_commit).group(1),
                    predecessor_tree if name == "wrong-tree" else candidate_tree,
                )
                git(self.root, "checkout", "--detach", bad_commit)
                git(self.root, "tag", "-f", "-a", "v1.13.17", "-m", "TEST_FIXTURE_ONLY")
                bad_tag = self.rev_parse("refs/tags/v1.13.17")
                self.assertIn(
                    "CKRS016",
                    rules(
                        self.validator(
                            "tagged-pending-tag-certification",
                            self.tag_environment(bad_commit, bad_tag),
                        )
                    ),
                )
                git(self.root, "checkout", "--detach", merge)
                git(self.root, "tag", "-f", "-a", "v1.13.17", "-m", "TEST_FIXTURE_ONLY")
                restored_tag = self.rev_parse("refs/tags/v1.13.17")
                self.assert_projection(
                    self.validator(
                        "tagged-pending-tag-certification",
                        self.tag_environment(restored_tag, restored_tag),
                    ),
                    "tagged-pending-tag-certification",
                    "GITHUB_TAG_EVENT_CONTEXT_CONSISTENCY",
                )


class TerminalSerializationContractTests(unittest.TestCase):
    """Phase 9N-R1 terminal serialization fixtures and boundaries."""

    TERMINAL_BRANCH = "release/v1.13.17-terminal-serialization"
    HISTORICAL_PROGRESSION_SOURCE = (
        "f1adb091d37ded75199f530a84477d44bebb36b7"
    )
    TERMINAL_DOCS = (
        "docs/release/v1.13/v1.13.17-release-state.md",
        "docs/release/v1.13/v1.13.17-release-state-validator-contract.md",
        "docs/release/v1.13/v1.13.17-terminal-disposition-reference.md",
        "docs/release/v1.13/v1.13.17-release-gate.md",
        "docs/release/v1.13/v1.13.17-scope.md",
        "docs/release/v1.13/v1.13.17-phase-list.md",
        "docs/release/v1.13/v1.13.17-validation-checklist.md",
    )

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "repo"
        source = SCRIPT.parent.parent
        self.root.mkdir()
        committed = run_process(
            [
                resolved_executable("git"),
                "-C",
                str(source),
                "ls-tree",
                "-r",
                "--name-only",
                "-z",
                "HEAD",
            ],
            check=True,
        ).stdout.split("\0")
        inventory = {Path(item) for item in committed if item}
        inventory.difference_update(validate_governance.V18_AUTHORITY_FILES)
        inventory.update(Path(item) for item in self.TERMINAL_DOCS)
        for relative in sorted(inventory):
            source_path = source / relative
            if not source_path.is_file():
                raise FileNotFoundError(f"terminal fixture input is unavailable: {relative}")
            target = self.root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            if relative in SUCCESSOR_HISTORICAL_PATHS:
                blob = run_process(
                    [
                        resolved_executable("git"), "-C", str(source), "show",
                        f"HEAD:{relative.as_posix()}",
                    ],
                    check=True,
                ).stdout
                target.write_text(blob, encoding="utf-8")
            else:
                shutil.copy2(source_path, target, follow_symlinks=False)
        # The existing progression methods are immutable historical-v1 tests.
        # Source their three inputs from exact H2, never the current checkout.
        for relative in (
            Path(self.TERMINAL_DOCS[0]),
            Path(self.TERMINAL_DOCS[1]),
            Path("docs/release/v1.13/v1.13.17-security-toolchain-progression-reference.md"),
        ):
            blob = run_process(
                [
                    resolved_executable("git"), "-C", str(source), "show",
                    f"{self.HISTORICAL_PROGRESSION_SOURCE}:{relative.as_posix()}",
                ],
                check=True,
            ).stdout
            target = self.root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(blob, encoding="utf-8")
        self.normalize_progression_material()
        git(self.root, "init")
        git(self.root, "config", "user.name", "fixture")
        git(self.root, "config", "user.email", "fixture@example.invalid")
        git(self.root, "add", ".")
        git(self.root, "commit", "--quiet", "-m", "TEST_FIXTURE_ONLY predecessor")
        predecessor = self.rev_parse("HEAD")
        git(self.root, "tag", "-a", "v1.13.16", "-m", "TEST_FIXTURE_ONLY")
        predecessor_tag = self.rev_parse("refs/tags/v1.13.16")
        disposition = (
            self.root
            / "docs/release/v1.13/v1.13.17-predecessor-disposition.md"
        )
        disposition_text = disposition.read_text(encoding="utf-8")
        for key, value in (
            ("TAG_OBJECT", predecessor_tag),
            ("DIRECT_TARGET", predecessor),
            ("SUCCESSOR_BASE", predecessor),
        ):
            disposition_text, count = re.subn(
                rf"(?m)^{key}: [0-9a-f]{{40}}$",
                f"{key}: {value}",
                disposition_text,
                count=1,
            )
            if count != 1:
                raise AssertionError(f"missing predecessor identity {key}")
        disposition.write_text(disposition_text, encoding="utf-8")
        git(self.root, "add", disposition.relative_to(self.root).as_posix())
        git(self.root, "commit", "--quiet", "-m", "TEST_FIXTURE_ONLY published M17")
        self.M17 = self.rev_parse("HEAD")
        git(self.root, "tag", "-a", "v1.13.17", "-m", "TEST_FIXTURE_ONLY")
        self.A17 = self.rev_parse("refs/tags/v1.13.17")
        (self.root / "TEST_FIXTURE_ONLY-terminal-c").write_text(
            "scratch C tree discriminator\n", encoding="utf-8"
        )
        git(self.root, "add", "TEST_FIXTURE_ONLY-terminal-c")
        git(self.root, "commit", "--quiet", "-m", "TEST_FIXTURE_ONLY accepted C")
        self.C = self.rev_parse("HEAD")
        self.C_TREE = self.rev_parse("HEAD^{tree}")
        identities = {
            "ACCEPTED_C": self.C,
            "ACCEPTED_C_TREE": self.C_TREE,
            "PUBLISHED_M17": self.M17,
            "ANNOTATED_TAG_A17": self.A17,
            "TAG_DIRECT_TARGET": self.M17,
        }
        for relative, prefix in (
            (Path(self.TERMINAL_DOCS[1]), "EXPECTED_"),
            (Path(self.TERMINAL_DOCS[2]), ""),
        ):
            target = self.root / relative
            text = target.read_text(encoding="utf-8")
            for key, value in identities.items():
                text, count = re.subn(
                    rf"(?m)^{prefix}{re.escape(key)}: \S+$",
                    f"{prefix}{key}: {value}",
                    text,
                    count=1,
                )
                if count != 1:
                    raise AssertionError(f"missing terminal identity {prefix}{key}")
            target.write_text(text, encoding="utf-8")
        git(self.root, "checkout", "--quiet", "-b", self.TERMINAL_BRANCH)
        git(self.root, "add", self.TERMINAL_DOCS[1], self.TERMINAL_DOCS[2])
        git(
            self.root,
            "commit",
            "--quiet",
            "-m",
            "TEST_FIXTURE_ONLY terminal serialization candidate",
        )
        self.candidate = self.rev_parse("HEAD")
        completed = run_validator(self.root, "--state", "auto", "--json")
        if completed.returncode != 0:
            raise AssertionError(completed.stdout + completed.stderr)

    def normalize_progression_material(self) -> None:
        """Remove every selector component atomically from the base fixture."""
        reference = self.root / Path(
            "docs/release/v1.13/v1.13.17-security-toolchain-progression-reference.md"
        )
        if reference.exists() or reference.is_symlink():
            reference.unlink()
        state_path = self.root / self.TERMINAL_DOCS[0]
        state = re.sub(
            r"(?m)^SECURITY_TOOLCHAIN_PROGRESSION_(?:SCHEMA|REFERENCE): .*\n",
            "",
            state_path.read_text(encoding="utf-8"),
        )
        state_path.write_text(state, encoding="utf-8")
        contract_path = self.root / self.TERMINAL_DOCS[1]
        contract = contract_path.read_text(encoding="utf-8")
        for suffix in ("", "-v2", "-v3"):
            marker = f"coldkeep-security-toolchain-progression{suffix}-expectations"
            block_pattern = (
                rf"(?ms)^<!-- {marker}:start -->\n.*?"
                + rf"^<!-- {marker}:end -->\n?"
            )
            contract = re.sub(
                block_pattern,
                "",
                contract,
            )
        contract_path.write_text(contract, encoding="utf-8")
        self.assertNotIn("SECURITY_TOOLCHAIN_PROGRESSION_", state)
        self.assertNotIn("security-toolchain-progression-expectations", contract)
        self.assertNotIn("security-toolchain-progression-v2-expectations", contract)
        self.assertNotIn("security-toolchain-progression-v3-expectations", contract)
        self.assertFalse(reference.exists() or reference.is_symlink())

    def rev_parse(self, value: str) -> str:
        return run_process(
            [resolved_executable("git"), "-C", str(self.root), "rev-parse", value],
            check=True,
        ).stdout.strip()

    def test_fixture_graph_is_owned_and_has_required_topology(self) -> None:
        for relative in (
            ".git/objects/info/alternates",
            ".git/info/grafts",
        ):
            self.assertFalse((self.root / relative).exists(), relative)
        replacements = run_process(
            [
                resolved_executable("git"),
                "-C",
                str(self.root),
                "for-each-ref",
                "--format=%(refname)",
                "refs/replace",
            ],
            check=True,
        ).stdout.strip()
        self.assertEqual(replacements, "")
        self.assertEqual(self.rev_parse(f"{self.candidate}^"), self.C)
        ancestry = run_process(
            [
                resolved_executable("git"),
                "-C",
                str(self.root),
                "merge-base",
                "--is-ancestor",
                self.M17,
                self.C,
            ],
            check=False,
        )
        self.assertEqual(ancestry.returncode, 0, ancestry.stderr)
        self.assertEqual(self.rev_parse("v1.13.17^{}"), self.M17)

    def test_executed_validator_is_current_working_image(self) -> None:
        modules = (
            ("scripts/validate_release_state.py", validate_release_state),
            ("scripts/release_state_support.py", release_state_support),
            ("scripts/validate_governance.py", validate_governance),
        )
        for relative, module in modules:
            with self.subTest(relative=relative):
                loaded = Path(module.__file__).resolve()
                current = (SCRIPT.parents[1] / relative).resolve()
                copied = self.root / relative
                self.assertEqual(loaded, current)
                self.assertEqual(copied.read_bytes(), current.read_bytes())
                self.assertEqual(
                    hashlib.sha256(copied.read_bytes()).hexdigest(),
                    hashlib.sha256(loaded.read_bytes()).hexdigest(),
                )

    def test_exact_attached_candidate_reference_and_projection(self) -> None:
        """TS-REF-01/CTX-01/AUTH-01: exact local Hts is structural only."""
        completed = run_validator(
            self.root,
            "--state",
            "auto",
            "--json",
        )
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        payload = json.loads(completed.stdout)
        self.assertEqual(
            payload["state"], "post-release-terminal-serialization-candidate"
        )
        self.assertEqual(
            payload["evidence_scope"], "LOCAL_TERMINAL_SERIALIZATION_CANDIDATE"
        )
        self.assertEqual(
            payload["authorization_status"], "NOT_EVALUATED_BY_VALIDATOR"
        )
        self.assertEqual(
            payload["certification_status"], "PENDING_EXTERNAL_EVIDENCE"
        )

    def run_json(
        self,
        state: str = "auto",
        *,
        env: dict[str, str] | None = None,
        diagnostic: Path | None = None,
    ) -> tuple[ProcessResult, dict[str, object]]:
        arguments = ["--state", state, "--json"]
        if diagnostic is not None:
            arguments.extend(("--diagnostic-json", str(diagnostic)))
        completed = run_validator(self.root, *arguments, env=env)
        return completed, json.loads(completed.stdout)

    def activate_security_progression(self) -> tuple[str, str]:
        """Create one fixture-owned H2 with the exact closed 25-row delta."""
        self.H1 = self.candidate
        h1_tree = self.rev_parse(f"{self.H1}^{{tree}}")
        source_root = SCRIPT.parents[1]
        reference_relative = Path(
            "docs/release/v1.13/v1.13.17-security-toolchain-progression-reference.md"
        )
        reference = run_process(
            [
                resolved_executable("git"), "-C", str(source_root), "show",
                f"{self.HISTORICAL_PROGRESSION_SOURCE}:{reference_relative.as_posix()}",
            ],
            check=True,
        ).stdout
        replacements = {
            "3d32e1662fdc0de251fd87bc5af6a309fcf3b6a6": self.C,
            "34c44fcce146336834e9b4cf38fe9308cb35c3e0": self.C_TREE,
            "3726f1fb034924d390b51abcd0b579e5168958e2": self.H1,
            "e47adabce24895448055e0ebb7f403300ebf9d24": h1_tree,
        }
        for old, new in replacements.items():
            reference = reference.replace(old, new)
        reference_path = self.root / reference_relative
        reference_path.write_text(reference, encoding="utf-8")
        reference_digest = hashlib.sha256(reference.encode("utf-8")).hexdigest()

        source_contract = run_process(
            [
                resolved_executable("git"), "-C", str(source_root), "show",
                f"{self.HISTORICAL_PROGRESSION_SOURCE}:{self.TERMINAL_DOCS[1]}",
            ],
            check=True,
        ).stdout
        block_match = re.search(
            r"(?ms)^<!-- coldkeep-security-toolchain-progression-expectations:start -->\n.*?^<!-- coldkeep-security-toolchain-progression-expectations:end -->$",
            source_contract,
        )
        if block_match is None:
            raise AssertionError("progression expectation block is unavailable")
        block = block_match.group(0)
        for old, new in replacements.items():
            block = block.replace(old, new)
        block = block.replace(
            "b133be6f1cd173523c59911df303c2fa38344fea4a70b4fba0d701a6b2918b57",
            reference_digest,
        )
        contract_path = self.root / self.TERMINAL_DOCS[1]
        contract_path.write_text(
            contract_path.read_text(encoding="utf-8").rstrip() + "\n\n" + block + "\n",
            encoding="utf-8",
        )
        state_path = self.root / self.TERMINAL_DOCS[0]
        terminal_pointer = (
            "TERMINAL_DISPOSITION_REFERENCE: "
            "docs/release/v1.13/v1.13.17-terminal-disposition-reference.md\n"
        )
        progression_pointer = (
            "SECURITY_TOOLCHAIN_PROGRESSION_REFERENCE: "
            f"{reference_relative.as_posix()}\n"
        )
        state = state_path.read_text(encoding="utf-8")
        state_path.write_text(
            state.replace(
                terminal_pointer,
                terminal_pointer + progression_pointer,
                1,
            ),
            encoding="utf-8",
        )

        rows = [
            line.split(" ", 2)
            for line in reference.splitlines()
            if re.fullmatch(r"[MA] 100(?:644|755) \S+", line)
        ]
        self.assertEqual(len(rows), 25)
        for status_value, _, path_value in rows:
            if status_value == "A" or path_value in {
                self.TERMINAL_DOCS[0], self.TERMINAL_DOCS[1]
            }:
                continue
            target = self.root / path_value
            if target.name == "go.mod" or target.suffix == ".go":
                marker = "// TEST_FIXTURE_ONLY progression delta\n"
            elif target.suffix in (".py", ".sh", ".yml") or target.name == "Dockerfile":
                marker = "# TEST_FIXTURE_ONLY progression delta\n"
            else:
                marker = "<!-- TEST_FIXTURE_ONLY progression delta -->\n"
            with target.open("a", encoding="utf-8") as stream:
                stream.write(marker)
        paths = [row[2] for row in rows]
        git(self.root, "add", *paths)
        git(
            self.root,
            "commit",
            "--quiet",
            "-m",
            "TEST_FIXTURE_ONLY security progression H2",
        )
        self.candidate = self.rev_parse("HEAD")
        return self.H1, self.candidate

    def progression_paths(self) -> tuple[Path, Path, Path]:
        return (
            self.root / self.TERMINAL_DOCS[0],
            self.root / self.TERMINAL_DOCS[1],
            self.root
            / "docs/release/v1.13/v1.13.17-security-toolchain-progression-reference.md",
        )

    def synchronize_progression_digest(self) -> None:
        _, contract, reference = self.progression_paths()
        digest = hashlib.sha256(reference.read_bytes()).hexdigest()
        text, count = re.subn(
            r"(?m)^EXPECTED_REFERENCE_SHA256: [0-9a-f]{64}$",
            f"EXPECTED_REFERENCE_SHA256: {digest}",
            contract.read_text(encoding="utf-8"),
            count=1,
        )
        self.assertEqual(count, 1)
        contract.write_text(text, encoding="utf-8")

    def replace_progression_field(
        self, key: str, value: str, *, synchronize_expectation: bool = True
    ) -> None:
        _, contract, reference = self.progression_paths()
        text, count = re.subn(
            rf"(?m)^{re.escape(key)}: \S+$",
            f"{key}: {value}",
            reference.read_text(encoding="utf-8"),
            count=1,
        )
        self.assertEqual(count, 1)
        reference.write_text(text, encoding="utf-8")
        if synchronize_expectation:
            expectation = (
                "EXPECTED_REFERENCE_SCHEMA" if key == "SCHEMA" else f"EXPECTED_{key}"
            )
            contract_text, count = re.subn(
                rf"(?m)^{re.escape(expectation)}: \S+$",
                f"{expectation}: {value}",
                contract.read_text(encoding="utf-8"),
                count=1,
            )
            self.assertEqual(count, 1)
            contract.write_text(contract_text, encoding="utf-8")
        self.synchronize_progression_digest()

    def assert_progression_rejected(self, expected_status: str = "INVALID") -> None:
        completed, payload = self.run_json(
            "post-release-terminal-serialization-candidate"
        )
        self.assertEqual(completed.returncode, 1, completed.stdout + completed.stderr)
        self.assertEqual(payload["security_progression"], expected_status)
        self.assertIn("CKRS020", [item["rule"] for item in payload["violations"]])

    def raw_commit_tree(self, tree: str, *parents: str) -> str:
        command = [
            resolved_executable("git"), "-C", str(self.root),
            "commit-tree", tree,
        ]
        for parent in parents:
            command.extend(("-p", parent))
        command.extend(("-m", "TEST_FIXTURE_ONLY progression topology"))
        return run_process(command, check=True).stdout.strip()

    def variant_tree(
        self,
        base: str,
        changes: tuple[tuple[str, str | None, str | None], ...],
    ) -> str:
        with tempfile.TemporaryDirectory() as directory:
            index = Path(directory) / "index"
            environment = os.environ.copy()
            environment["GIT_INDEX_FILE"] = str(index)
            prefix = [
                resolved_executable("git"),
                *FIXTURE_GIT_CONFIG,
                "-C", str(self.root),
            ]
            run_process(prefix + ["read-tree", base], env=environment, check=True)
            for path, mode, object_id in changes:
                if mode is None:
                    run_process(
                        prefix + ["update-index", "--force-remove", "--", path],
                        env=environment,
                        check=True,
                    )
                else:
                    self.assertIsNotNone(object_id)
                    run_process(
                        prefix
                        + [
                            "update-index", "--add", "--cacheinfo",
                            f"{mode},{object_id},{path}",
                        ],
                        env=environment,
                        check=True,
                    )
            return run_process(
                prefix + ["write-tree"], env=environment, check=True
            ).stdout.strip()

    def hash_fixture_blob(self, content: bytes) -> str:
        path = Path(self.temporary.name) / "progression-blob"
        path.write_bytes(content)
        return run_process(
            [
                resolved_executable("git"), "-C", str(self.root),
                "hash-object", "-w", str(path),
            ],
            check=True,
        ).stdout.strip()

    def install_progression_material(self, h2: str) -> None:
        for path in (
            self.TERMINAL_DOCS[0],
            self.TERMINAL_DOCS[1],
            "docs/release/v1.13/v1.13.17-security-toolchain-progression-reference.md",
        ):
            content = run_process(
                [
                    resolved_executable("git"), "-C", str(self.root),
                    "show", f"{h2}:{path}",
                ],
                check=True,
            ).stdout
            (self.root / path).write_text(content, encoding="utf-8")

    def write_progression_pr(
        self,
        event: Path,
        h2: str,
        *,
        merge_value: object = None,
    ) -> None:
        event.write_text(
            json.dumps(
                {
                    "repository": {
                        "full_name": "franchoy/coldkeep", "id": 1158711822,
                    },
                    "number": 123,
                    "pull_request": {
                        "base": {
                            "ref": "main", "sha": self.C,
                            "repo": {"full_name": "franchoy/coldkeep"},
                        },
                        "head": {
                            "ref": self.TERMINAL_BRANCH, "sha": h2,
                            "repo": {"full_name": "franchoy/coldkeep"},
                        },
                        "merge_commit_sha": merge_value,
                    },
                }
            ),
            encoding="utf-8",
        )

    def progression_pr_environment(self, event: Path, checkout: str) -> dict[str, str]:
        return {
            "GITHUB_ACTIONS": "true",
            "GITHUB_EVENT_NAME": "pull_request",
            "GITHUB_REF": "refs/pull/123/merge",
            "GITHUB_REF_NAME": "123/merge",
            "GITHUB_HEAD_REF": self.TERMINAL_BRANCH,
            "GITHUB_BASE_REF": "main",
            "GITHUB_EVENT_PATH": str(event),
            "GITHUB_REPOSITORY": "franchoy/coldkeep",
            "GITHUB_REPOSITORY_ID": "1158711822",
            "GITHUB_SHA": checkout,
        }

    def test_security_progression_local_h2_h3_and_delta_boundaries(self) -> None:
        """SP-OBJ-01: one exact H1 child passes and H3 cannot inherit it."""
        _, h2 = self.activate_security_progression()
        completed, payload = self.run_json()
        self.assertEqual(completed.returncode, 0, completed.stdout)
        self.assertEqual(payload["security_progression"], "PROGRESSED_H2_STRUCTURAL_ONLY")
        self.assertEqual(payload["security_result"], "NOT_EVALUATED_BY_VALIDATOR")
        self.assertEqual(
            payload["evidence_scope"], "LOCAL_TERMINAL_SERIALIZATION_CANDIDATE"
        )
        git(self.root, "commit", "--quiet", "--allow-empty", "-m", "TEST_FIXTURE_ONLY H3")
        self.assertEqual(self.rev_parse("HEAD^"), h2)
        completed, payload = self.run_json(
            "post-release-terminal-serialization-candidate"
        )
        self.assertEqual(completed.returncode, 1)
        self.assertEqual(payload["security_progression"], "INVALID")
        self.assertIn("CKRS020", [item["rule"] for item in payload["violations"]])

    def test_security_progression_reference_invalid_and_unavailable(self) -> None:
        """SP-REF-01: digest-invalid and missing selected input stay distinct."""
        self.activate_security_progression()
        reference = self.root / (
            "docs/release/v1.13/v1.13.17-security-toolchain-progression-reference.md"
        )
        original = reference.read_bytes()
        reference.write_bytes(original + b"TEST_FIXTURE_ONLY\n")
        completed, payload = self.run_json()
        self.assertEqual(completed.returncode, 1)
        self.assertEqual(payload["security_progression"], "INVALID")
        reference.write_bytes(original)
        reference.unlink()
        completed, payload = self.run_json()
        self.assertEqual(completed.returncode, 1)
        self.assertEqual(payload["security_progression"], "UNAVAILABLE")

    def test_security_progression_push_pr_and_main_contexts(self) -> None:
        """SP-CTX-01: exact repository, PR 123, and [C,H2] contexts pass."""
        h1, h2 = self.activate_security_progression()
        event = self.root / "progression-push.json"
        event.write_text(
            json.dumps({
                "ref": f"refs/heads/{self.TERMINAL_BRANCH}",
                "before": h1,
                "after": h2,
                "created": False,
                "deleted": False,
                "forced": False,
                "repository": {"full_name": "franchoy/coldkeep", "id": 1158711822},
            }),
            encoding="utf-8",
        )
        push_env = {
            "GITHUB_ACTIONS": "true", "GITHUB_EVENT_NAME": "push",
            "GITHUB_REF": f"refs/heads/{self.TERMINAL_BRANCH}",
            "GITHUB_REF_NAME": self.TERMINAL_BRANCH,
            "GITHUB_REF_TYPE": "branch", "GITHUB_EVENT_PATH": str(event),
            "GITHUB_REPOSITORY": "franchoy/coldkeep",
            "GITHUB_REPOSITORY_ID": "1158711822", "GITHUB_SHA": h2,
        }
        completed, payload = self.run_json(env=push_env)
        self.assertEqual(completed.returncode, 0, completed.stdout)
        self.assertEqual(
            payload["evidence_scope"],
            "GITHUB_TERMINAL_SERIALIZATION_PUSH_CONTEXT_CONSISTENCY",
        )

        event.write_text(
            json.dumps({
                "repository": {"full_name": "franchoy/coldkeep", "id": 1158711822},
                "number": 123,
                "pull_request": {
                    "base": {"ref": "main", "sha": self.C, "repo": {"full_name": "franchoy/coldkeep"}},
                    "head": {"ref": self.TERMINAL_BRANCH, "sha": h2, "repo": {"full_name": "franchoy/coldkeep"}},
                    "merge_commit_sha": None,
                },
            }),
            encoding="utf-8",
        )
        pr_env = {
            "GITHUB_ACTIONS": "true", "GITHUB_EVENT_NAME": "pull_request",
            "GITHUB_REF": "refs/pull/123/merge", "GITHUB_REF_NAME": "123/merge",
            "GITHUB_HEAD_REF": self.TERMINAL_BRANCH, "GITHUB_BASE_REF": "main",
            "GITHUB_EVENT_PATH": str(event), "GITHUB_REPOSITORY": "franchoy/coldkeep",
            "GITHUB_REPOSITORY_ID": "1158711822", "GITHUB_SHA": h2,
        }
        completed, payload = self.run_json(env=pr_env)
        self.assertEqual(completed.returncode, 0, completed.stdout)
        self.assertEqual(
            payload["evidence_scope"],
            "GITHUB_TERMINAL_SERIALIZATION_PR_CONTEXT_CONSISTENCY",
        )

        main = self.commit_tree(self.C, h2)
        git(self.root, "checkout", "--quiet", "-B", "main", main)
        completed, payload = self.run_json()
        self.assertEqual(completed.returncode, 0, completed.stdout)
        self.assertEqual(payload["security_progression"], "PROGRESSED_H2_STRUCTURAL_ONLY")
        self.assertEqual(
            payload["evidence_scope"], "LOCAL_TERMINAL_SERIALIZATION_MAIN_ARTIFACT_ONLY"
        )

    def test_security_progression_anchor_and_identity_matrix(self) -> None:
        """SP-REF-02: remapped grammar, type, availability, and anchors fail closed."""
        self.activate_security_progression()
        _, contract, reference = self.progression_paths()
        originals = (contract.read_bytes(), reference.read_bytes())
        h1_tree = self.rev_parse(f"{self.H1}^{{tree}}")
        cases = (
            ("zero-base", "BASE_C", "0" * 40, "INVALID"),
            ("uppercase-base", "BASE_C", self.C.upper(), "INVALID"),
            ("short-base", "BASE_C", self.C[:-1], "INVALID"),
            ("unavailable-base", "BASE_C", "f" * 40, "UNAVAILABLE"),
            ("base-object-is-tree", "BASE_C", self.C_TREE, "INVALID"),
            ("wrong-base-tree", "BASE_C_TREE", h1_tree, "INVALID"),
            ("wrong-h1", "HISTORICAL_H1", self.C, "INVALID"),
            ("unavailable-h1", "HISTORICAL_H1", "e" * 40, "UNAVAILABLE"),
            ("wrong-h1-parent", "HISTORICAL_H1_PARENT", self.M17, "INVALID"),
            ("wrong-h1-tree", "HISTORICAL_H1_TREE", self.C_TREE, "INVALID"),
        )
        for name, key, value, expected in cases:
            with self.subTest(name=name):
                contract.write_bytes(originals[0])
                reference.write_bytes(originals[1])
                self.replace_progression_field(key, value)
                self.assert_progression_rejected(expected)

    def test_security_progression_reference_grammar_and_loader_matrix(self) -> None:
        """SP-REF-03: re-digested grammar errors differ from unavailable input."""
        self.activate_security_progression()
        _, contract, reference = self.progression_paths()
        contract_original = contract.read_bytes()
        original = reference.read_bytes()

        def reject(name: str, content: bytes, expected: str = "INVALID") -> None:
            with self.subTest(name=name):
                contract.write_bytes(contract_original)
                reference.write_bytes(content)
                self.synchronize_progression_digest()
                self.assert_progression_rejected(expected)

        text = original.decode("utf-8")
        lines = text.splitlines(keepends=True)
        base_index = next(
            index for index, line in enumerate(lines)
            if line.startswith("BASE_C: ")
        )
        tree_index = next(
            index for index, line in enumerate(lines)
            if line.startswith("BASE_C_TREE: ")
        )
        swapped = list(lines)
        swapped[base_index], swapped[tree_index] = (
            swapped[tree_index], swapped[base_index]
        )
        duplicate = list(lines)
        duplicate[tree_index] = duplicate[base_index]
        missing = list(lines)
        missing.pop(tree_index)
        first_delta = next(
            line for line in lines if re.fullmatch(r"[MA] 100(?:644|755) \S+\n", line)
        )
        second_delta = next(
            line for line in lines
            if re.fullmatch(r"[MA] 100(?:644|755) \S+\n", line)
            and line != first_delta
        )
        reject("field-order", "".join(swapped).encode("utf-8"))
        reject("duplicate-field", "".join(duplicate).encode("utf-8"))
        reject("missing-field", "".join(missing).encode("utf-8"))
        reject("unknown-field", original.replace(b"BASE_C: ", b"UNKNOWN: ", 1))
        reject("bad-count", original.replace(b"DELTA_COUNT: 25", b"DELTA_COUNT: 24", 1))
        reject("bad-status", original.replace(first_delta.encode(), b"X" + first_delta.encode()[1:], 1))
        reject("bad-mode", original.replace(b" 100644 ", b" 100600 ", 1))
        reject(
            "unsafe-path",
            original.replace(
                first_delta.encode(),
                first_delta.split(" ", 2)[0].encode()
                + b" 100644 ../TEST_FIXTURE_ONLY\n",
                1,
            ),
        )
        reject("duplicate-delta", original.replace(second_delta.encode(), first_delta.encode(), 1))
        reject("marker", original.replace(b"<!-- coldkeep-security-toolchain-progression-reference:start -->", b"<!-- wrong -->", 1))
        reject("fence", original.replace(b"```text", b"```yaml", 1))
        reject("bom", b"\xef\xbb\xbf" + original)
        reject("nul", original.replace(b"SCHEMA:", b"SCHEMA:\x00", 1))
        reject("crlf", original.replace(b"\n", b"\r\n", 1))
        reject("invalid-utf8", original[:-1] + b"\xff\n")
        reject("missing-terminal-lf", original[:-1])
        reject("extra-terminal-lf", original + b"\n")
        reject("over-limit", b"X" * 16385, "UNAVAILABLE")

        contract.write_bytes(contract_original)
        reference.write_bytes(original)
        reference.unlink()
        self.assert_progression_rejected("UNAVAILABLE")
        reference.mkdir()
        self.assert_progression_rejected("UNAVAILABLE")
        reference.rmdir()
        target = self.root / "TEST_FIXTURE_ONLY-progression-reference"
        target.write_bytes(original)
        reference.symlink_to(target)
        self.assert_progression_rejected("UNAVAILABLE")

    def test_security_progression_topology_delta_and_preservation_matrix(self) -> None:
        """SP-OBJ-02: every selected candidate keeps exact parent, delta, mode, and type."""
        h1, h2 = self.activate_security_progression()
        h1_tree = self.rev_parse(f"{h1}^{{tree}}")
        h2_tree = self.rev_parse(f"{h2}^{{tree}}")
        path = "Dockerfile"
        h1_blob = self.rev_parse(f"{h1}:{path}")
        h2_blob = self.rev_parse(f"{h2}:{path}")
        extra_blob = self.hash_fixture_blob(b"TEST_FIXTURE_ONLY extra delta\n")
        symlink_blob = self.hash_fixture_blob(b"TEST_FIXTURE_ONLY-target")
        terminal_reference = self.TERMINAL_DOCS[2]
        terminal_blob = self.hash_fixture_blob(
            (self.root / terminal_reference).read_bytes()
            + b"<!-- TEST_FIXTURE_ONLY changed original reference -->\n"
        )
        release_body = validate_governance.CURRENT_RELEASE_BODY.as_posix()
        release_blob = self.hash_fixture_blob(
            (self.root / release_body).read_bytes()
            + b"<!-- TEST_FIXTURE_ONLY changed publication body -->\n"
        )
        omitted_tree = self.variant_tree(h2, ((path, "100644", h1_blob),))
        extra_tree = self.variant_tree(
            h2, (("TEST_FIXTURE_ONLY-extra", "100644", extra_blob),)
        )
        deleted_tree = self.variant_tree(h2, ((path, None, None),))
        wrong_mode_tree = self.variant_tree(h2, ((path, "100755", h2_blob),))
        renamed_tree = self.variant_tree(
            h2,
            (
                (path, None, None),
                ("TEST_FIXTURE_ONLY-renamed-Dockerfile", "100644", h2_blob),
            ),
        )
        symlink_tree = self.variant_tree(h2, ((path, "120000", symlink_blob),))
        gitlink_tree = self.variant_tree(h2, ((path, "160000", self.C),))
        terminal_tree = self.variant_tree(
            h2, ((terminal_reference, "100644", terminal_blob),)
        )
        publication_tree = self.variant_tree(
            h2, ((release_body, "100644", release_blob),)
        )
        cases = (
            ("zero-parent", self.raw_commit_tree(h2_tree)),
            ("two-parent", self.raw_commit_tree(h2_tree, h1, self.C)),
            ("extra-parent", self.raw_commit_tree(h2_tree, h1, self.C, self.M17)),
            ("unrelated-descendant", self.raw_commit_tree(h2_tree, self.C)),
            ("empty-delta", self.raw_commit_tree(h1_tree, h1)),
            ("omitted-path", self.raw_commit_tree(omitted_tree, h1)),
            ("extra-path", self.raw_commit_tree(extra_tree, h1)),
            ("deleted-path", self.raw_commit_tree(deleted_tree, h1)),
            ("wrong-mode", self.raw_commit_tree(wrong_mode_tree, h1)),
            ("rename", self.raw_commit_tree(renamed_tree, h1)),
            ("symlink", self.raw_commit_tree(symlink_tree, h1)),
            ("gitlink", self.raw_commit_tree(gitlink_tree, h1)),
            ("changed-terminal-reference", self.raw_commit_tree(terminal_tree, h1)),
            ("changed-publication-body", self.raw_commit_tree(publication_tree, h1)),
        )
        for name, candidate in cases:
            with self.subTest(name=name):
                git(self.root, "checkout", "--quiet", "--force", "--detach", candidate)
                self.install_progression_material(h2)
                self.assert_progression_rejected()

        git(self.root, "checkout", "--quiet", "--force", "--detach", h2)
        git(self.root, "commit", "--quiet", "--allow-empty", "-m", "TEST_FIXTURE_ONLY H3")
        self.install_progression_material(h2)
        self.assert_progression_rejected()

    def test_security_progression_detached_push_and_negative_context_matrix(self) -> None:
        """SP-CTX-02: attached/detached H1-to-H2 push is exact in every field."""
        h1, h2 = self.activate_security_progression()
        event = self.root / "progression-push-matrix.json"

        def exact() -> tuple[dict[str, object], dict[str, str]]:
            body: dict[str, object] = {
                "ref": f"refs/heads/{self.TERMINAL_BRANCH}",
                "before": h1,
                "after": h2,
                "created": False,
                "deleted": False,
                "forced": False,
                "repository": {
                    "full_name": "franchoy/coldkeep", "id": 1158711822,
                },
            }
            environment = {
                "GITHUB_ACTIONS": "true", "GITHUB_EVENT_NAME": "push",
                "GITHUB_REF": f"refs/heads/{self.TERMINAL_BRANCH}",
                "GITHUB_REF_NAME": self.TERMINAL_BRANCH,
                "GITHUB_REF_TYPE": "branch", "GITHUB_EVENT_PATH": str(event),
                "GITHUB_REPOSITORY": "franchoy/coldkeep",
                "GITHUB_REPOSITORY_ID": "1158711822", "GITHUB_SHA": h2,
            }
            return body, environment

        for attachment in ("attached", "detached"):
            with self.subTest(positive=attachment):
                git(
                    self.root, "checkout", "--quiet", "--force",
                    self.TERMINAL_BRANCH if attachment == "attached" else "--detach",
                    *(() if attachment == "attached" else (h2,)),
                )
                body, environment = exact()
                event.write_text(json.dumps(body), encoding="utf-8")
                completed, payload = self.run_json(env=environment)
                self.assertEqual(completed.returncode, 0, completed.stdout)
                self.assertEqual(
                    payload["evidence_scope"],
                    "GITHUB_TERMINAL_SERIALIZATION_PUSH_CONTEXT_CONSISTENCY",
                )

        completed, payload = self.run_json(
            "post-release-terminal-serialization-candidate"
        )
        self.assertEqual(completed.returncode, 1)
        self.assertIn("CKRS016", [item["rule"] for item in payload["violations"]])

        event_mutations = (
            ("created", "created", True),
            ("deleted", "deleted", True),
            ("forced", "forced", True),
            ("before-c", "before", self.C),
            ("after", "after", self.C),
            ("payload-ref", "ref", "refs/heads/release/other"),
        )
        for name, key, value in event_mutations:
            with self.subTest(rejection=name):
                body, environment = exact()
                body[key] = value
                event.write_text(json.dumps(body), encoding="utf-8")
                completed, payload = self.run_json(
                    "post-release-terminal-serialization-candidate", env=environment
                )
                self.assertEqual(completed.returncode, 1)
                self.assertIn("CKRS016", [item["rule"] for item in payload["violations"]])

        for name, key, value in (
            ("repository", "GITHUB_REPOSITORY", "fork/coldkeep"),
            ("repository-id", "GITHUB_REPOSITORY_ID", "9"),
            ("full-ref", "GITHUB_REF", "refs/heads/release/other"),
            ("short-ref", "GITHUB_REF_NAME", "release/other"),
            ("sha", "GITHUB_SHA", self.C),
        ):
            with self.subTest(rejection=name):
                body, environment = exact()
                event.write_text(json.dumps(body), encoding="utf-8")
                environment[key] = value
                completed, payload = self.run_json(
                    "post-release-terminal-serialization-candidate", env=environment
                )
                self.assertEqual(completed.returncode, 1)
                self.assertIn("CKRS016", [item["rule"] for item in payload["violations"]])

        body, environment = exact()
        body["repository"] = {"full_name": "franchoy/coldkeep", "id": 9}
        event.write_text(json.dumps(body), encoding="utf-8")
        completed, payload = self.run_json(
            "post-release-terminal-serialization-candidate", env=environment
        )
        self.assertEqual(completed.returncode, 1)
        self.assertIn("CKRS016", [item["rule"] for item in payload["violations"]])
        body, environment = exact()
        body["repository"] = {"full_name": "fork/coldkeep", "id": 1158711822}
        event.write_text(json.dumps(body), encoding="utf-8")
        completed, payload = self.run_json(
            "post-release-terminal-serialization-candidate", env=environment
        )
        self.assertEqual(completed.returncode, 1)
        self.assertIn("CKRS016", [item["rule"] for item in payload["violations"]])

        git(self.root, "checkout", "--quiet", "--force", "-B", "fixture/wrong", h2)
        body, environment = exact()
        event.write_text(json.dumps(body), encoding="utf-8")
        completed, payload = self.run_json(
            "post-release-terminal-serialization-candidate", env=environment
        )
        self.assertEqual(completed.returncode, 1)
        self.assertIn("CKRS016", [item["rule"] for item in payload["violations"]])

    def test_security_progression_direct_and_synthetic_pr_matrix(self) -> None:
        """SP-CTX-03: PR 123 direct and actual [C,H2] forms prove exact context."""
        _, h2 = self.activate_security_progression()
        h2_tree = self.rev_parse(f"{h2}^{{tree}}")
        event = self.root / "progression-pr-matrix.json"
        for merge_value in (None, h2):
            with self.subTest(direct_merge_value=merge_value):
                git(self.root, "checkout", "--quiet", "--force", self.TERMINAL_BRANCH)
                self.write_progression_pr(event, h2, merge_value=merge_value)
                completed, payload = self.run_json(
                    env=self.progression_pr_environment(event, h2)
                )
                self.assertEqual(completed.returncode, 0, completed.stdout)
                self.assertEqual(
                    payload["evidence_scope"],
                    "GITHUB_TERMINAL_SERIALIZATION_PR_CONTEXT_CONSISTENCY",
                )

        synthetic = self.raw_commit_tree(h2_tree, self.C, h2)
        for merge_value in (None, h2, "1" * 40):
            with self.subTest(synthetic_merge_value=merge_value):
                git(self.root, "checkout", "--quiet", "--force", "--detach", synthetic)
                self.write_progression_pr(event, h2, merge_value=merge_value)
                completed, payload = self.run_json(
                    env=self.progression_pr_environment(event, synthetic)
                )
                self.assertEqual(completed.returncode, 0, completed.stdout)
                self.assertEqual(
                    payload["evidence_scope"],
                    "GITHUB_TERMINAL_SERIALIZATION_PR_CONTEXT_CONSISTENCY",
                )

        def rejected(
            name: str,
            checkout: str,
            mutate_body: object = None,
            mutate_env: object = None,
        ) -> None:
            with self.subTest(rejection=name):
                git(self.root, "checkout", "--quiet", "--force", "--detach", checkout)
                self.write_progression_pr(event, h2, merge_value=h2)
                body = json.loads(event.read_text(encoding="utf-8"))
                environment = self.progression_pr_environment(event, checkout)
                if mutate_body is not None:
                    mutate_body(body)
                    event.write_text(json.dumps(body), encoding="utf-8")
                if mutate_env is not None:
                    mutate_env(environment)
                completed, payload = self.run_json(
                    "post-release-terminal-serialization-candidate", env=environment
                )
                self.assertEqual(completed.returncode, 1)
                self.assertIn("CKRS016", [item["rule"] for item in payload["violations"]])

        rejected("wrong-number", synthetic, lambda body: body.update(number=124))
        rejected(
            "wrong-base",
            synthetic,
            lambda body: body["pull_request"]["base"].update(ref="release/other"),
        )
        rejected(
            "wrong-base-sha",
            synthetic,
            lambda body: body["pull_request"]["base"].update(sha=self.M17),
        )
        rejected(
            "wrong-head",
            synthetic,
            lambda body: body["pull_request"]["head"].update(sha=self.C),
        )
        rejected(
            "wrong-head-ref",
            synthetic,
            lambda body: body["pull_request"]["head"].update(ref="release/other"),
        )
        rejected(
            "wrong-base-repository",
            synthetic,
            lambda body: body["pull_request"]["base"]["repo"].update(full_name="fork/coldkeep"),
        )
        rejected(
            "wrong-head-repository",
            synthetic,
            lambda body: body["pull_request"]["head"]["repo"].update(full_name="fork/coldkeep"),
        )
        rejected(
            "wrong-repository",
            synthetic,
            lambda body: body["repository"].update(full_name="fork/coldkeep"),
        )
        rejected(
            "wrong-payload-repository-id",
            synthetic,
            lambda body: body["repository"].update(id=9),
        )
        rejected(
            "wrong-environment-repository-id",
            synthetic,
            mutate_env=lambda environment: environment.update(GITHUB_REPOSITORY_ID="9"),
        )
        rejected(
            "wrong-ref",
            synthetic,
            mutate_env=lambda environment: environment.update(GITHUB_REF="refs/pull/124/merge"),
        )
        rejected(
            "wrong-short-ref",
            synthetic,
            mutate_env=lambda environment: environment.update(GITHUB_REF_NAME="124/merge"),
        )
        rejected(
            "wrong-environment-head-ref",
            synthetic,
            mutate_env=lambda environment: environment.update(GITHUB_HEAD_REF="release/other"),
        )
        rejected(
            "wrong-environment-base-ref",
            synthetic,
            mutate_env=lambda environment: environment.update(GITHUB_BASE_REF="release/other"),
        )
        rejected(
            "wrong-runtime-sha",
            synthetic,
            mutate_env=lambda environment: environment.update(GITHUB_SHA=h2),
        )
        rejected("parent-order", self.raw_commit_tree(h2_tree, h2, self.C))
        rejected("extra-parent", self.raw_commit_tree(h2_tree, self.C, h2, self.M17))
        rejected("tree", self.raw_commit_tree(self.C_TREE, self.C, h2))
        rejected(
            "zero-merge-metadata",
            synthetic,
            lambda body: body["pull_request"].update(merge_commit_sha="0" * 40),
        )
        rejected(
            "malformed-merge-metadata",
            synthetic,
            lambda body: body["pull_request"].update(merge_commit_sha="not-a-sha"),
        )
        rejected(
            "missing-merge-metadata",
            synthetic,
            lambda body: body["pull_request"].pop("merge_commit_sha"),
        )

    def test_security_progression_main_push_and_complete_diagnostic(self) -> None:
        """SP-CTX-04/DIAG-01: first-main S and committed v3 sources are explicit."""
        _, h2 = self.activate_security_progression()
        h2_tree = self.rev_parse(f"{h2}^{{tree}}")
        main = self.raw_commit_tree(h2_tree, self.C, h2)
        git(self.root, "checkout", "--quiet", "--force", "-B", "main", main)
        diagnostic = Path(self.temporary.name) / "progression-v3-diagnostic.json"
        process = run_validator_script(
            self.root / "scripts/validate_release_state.py",
            self.root,
            "--state", "auto", "--json",
            "--diagnostic-json", str(diagnostic),
        )
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)
        payload = json.loads(process.stdout)
        self.assertEqual(payload["security_progression"], "PROGRESSED_H2_STRUCTURAL_ONLY")
        self.assertEqual(payload["security_result"], "NOT_EVALUATED_BY_VALIDATOR")
        record = json.loads(diagnostic.read_text(encoding="utf-8"))
        self.assertEqual(record["schema"], "coldkeep-release-state-diagnostic/v3")
        self.assertEqual(record["validator"]["security_progression"], "PROGRESSED_H2_STRUCTURAL_ONLY")
        self.assertEqual(record["validator"]["security_result"], "NOT_EVALUATED_BY_VALIDATOR")
        self.assertEqual(
            [source["path"] for source in record["validator"]["sources"]],
            list(release_state_support.DiagnosticRecorder._SOURCE_PATHS),
        )
        for source in record["validator"]["sources"]:
            self.assertIs(source["loaded_path_match"], True)
            self.assertIs(source["matches_git_blob"], True)
            self.assertRegex(source["measured_sha256"], r"^[0-9a-f]{64}$")
            self.assertGreater(source["bytes"], 0)
        self.assertIs(record["capture"]["complete"], True)
        self.assertEqual(record["capture"]["incomplete_reasons"], [])
        self.assertIs(record["capture"]["non_authorizing"], True)

        event = self.root / "progression-main-push.json"

        def exact() -> tuple[dict[str, object], dict[str, str]]:
            body: dict[str, object] = {
                "ref": "refs/heads/main", "before": self.C, "after": main,
                "created": False, "deleted": False, "forced": False,
                "repository": {
                    "full_name": "franchoy/coldkeep", "id": 1158711822,
                },
            }
            environment = {
                "GITHUB_ACTIONS": "true", "GITHUB_EVENT_NAME": "push",
                "GITHUB_REF": "refs/heads/main", "GITHUB_REF_NAME": "main",
                "GITHUB_REF_TYPE": "branch", "GITHUB_EVENT_PATH": str(event),
                "GITHUB_REPOSITORY": "franchoy/coldkeep",
                "GITHUB_REPOSITORY_ID": "1158711822", "GITHUB_SHA": main,
            }
            return body, environment

        git(self.root, "checkout", "--quiet", "--force", "--detach", main)
        body, environment = exact()
        event.write_text(json.dumps(body), encoding="utf-8")
        completed, payload = self.run_json(env=environment)
        self.assertEqual(completed.returncode, 0, completed.stdout)
        self.assertEqual(
            payload["evidence_scope"],
            "GITHUB_TERMINAL_SERIALIZATION_MAIN_CONTEXT_CONSISTENCY",
        )
        for name, key, value in (
            ("wrong-before", "before", self.M17),
            ("created", "created", True),
            ("deleted", "deleted", True),
            ("forced", "forced", True),
        ):
            with self.subTest(rejection=name):
                body, environment = exact()
                body[key] = value
                event.write_text(json.dumps(body), encoding="utf-8")
                completed, payload = self.run_json(
                    "post-release-terminal-serialization-main", env=environment
                )
                self.assertEqual(completed.returncode, 1)
                self.assertIn("CKRS016", [item["rule"] for item in payload["violations"]])

        for name, mutate_body, mutate_env in (
            (
                "payload-repository",
                lambda body: body["repository"].update(full_name="fork/coldkeep"),
                lambda environment: None,
            ),
            (
                "payload-repository-id",
                lambda body: body["repository"].update(id=9),
                lambda environment: None,
            ),
            (
                "environment-repository",
                lambda body: None,
                lambda environment: environment.update(GITHUB_REPOSITORY="fork/coldkeep"),
            ),
            (
                "environment-repository-id",
                lambda body: None,
                lambda environment: environment.update(GITHUB_REPOSITORY_ID="9"),
            ),
            (
                "environment-ref",
                lambda body: None,
                lambda environment: environment.update(GITHUB_REF="refs/heads/release/other"),
            ),
        ):
            with self.subTest(rejection=name):
                body, environment = exact()
                mutate_body(body)
                mutate_env(environment)
                event.write_text(json.dumps(body), encoding="utf-8")
                completed, payload = self.run_json(
                    "post-release-terminal-serialization-main", env=environment
                )
                self.assertEqual(completed.returncode, 1)
                self.assertIn("CKRS016", [item["rule"] for item in payload["violations"]])

        for name, bad_main in (
            ("parent-order", self.raw_commit_tree(h2_tree, h2, self.C)),
            ("extra-parent", self.raw_commit_tree(h2_tree, self.C, h2, self.M17)),
            ("wrong-tree", self.raw_commit_tree(self.C_TREE, self.C, h2)),
            ("historical-h1-second-parent", self.raw_commit_tree(h2_tree, self.C, self.H1)),
            ("later-main-transition", self.raw_commit_tree(h2_tree, main, h2)),
        ):
            with self.subTest(rejection=name):
                git(self.root, "checkout", "--quiet", "--force", "--detach", bad_main)
                body, environment = exact()
                body["after"] = bad_main
                environment["GITHUB_SHA"] = bad_main
                event.write_text(json.dumps(body), encoding="utf-8")
                completed, payload = self.run_json(
                    "post-release-terminal-serialization-main", env=environment
                )
                self.assertEqual(completed.returncode, 1)
                self.assertIn("CKRS016", [item["rule"] for item in payload["violations"]])

    def assert_reference_failure(self, expected_class: str) -> None:
        completed, payload = self.run_json(
            "post-release-terminal-serialization-candidate"
        )
        self.assertEqual(completed.returncode, 1, completed.stdout + completed.stderr)
        messages = [
            item["message"]
            for item in payload["violations"]
            if item["rule"] == "CKRS019"
        ]
        self.assertTrue(
            any(
                f"terminal disposition reference {expected_class}:" in message
                for message in messages
            ),
            messages,
        )

    def test_reference_unavailable_boundaries(self) -> None:
        """TS-REF-02: missing/type/ancestor/oversize are unavailable."""
        relative = Path(
            "docs/release/v1.13/v1.13.17-terminal-disposition-reference.md"
        )
        target = self.root / relative
        original = target.read_bytes()

        target.unlink()
        self.assert_reference_failure("unavailable")
        target.write_bytes(original)

        target.unlink()
        target.mkdir()
        self.assert_reference_failure("unavailable")
        target.rmdir()
        target.write_bytes(original)

        parent = target.parent
        real_parent = parent.with_name("v1.13-real")
        parent.rename(real_parent)
        parent.symlink_to(real_parent.name, target_is_directory=True)
        self.assert_reference_failure("unavailable")
        parent.unlink()
        real_parent.rename(parent)

        target.write_bytes(b"X" * 8193)
        self.assert_reference_failure("unavailable")
        target.write_bytes(original)

    def test_reference_invalid_byte_and_shape_boundaries(self) -> None:
        """TS-REF-02: malformed bytes/newlines/markers/fields are invalid."""
        target = self.root / self.TERMINAL_DOCS[2]
        original = target.read_bytes()
        text = original.decode("utf-8")
        lines = text.splitlines(keepends=True)
        accepted_c = next(
            line for line in lines if line.startswith("ACCEPTED_C: ")
        )
        published = next(
            line for line in lines if line.startswith("PUBLISHED_M17: ")
        )
        cases = {
            "bom": b"\xef\xbb\xbf" + original,
            "cr": original.replace(b"\n", b"\r\n", 1),
            "nul": original.replace(b"source-only", b"source\x00-only", 1),
            "utf8": original.replace(b"source-only", b"source-\xff", 1),
            "final-lf-missing": original[:-1],
            "extra-final-lf": original + b"\n",
            "marker": original.replace(
                b"coldkeep-terminal-disposition-reference:start",
                b"coldkeep-terminal-disposition-reference:wrong",
                1,
            ),
            "fence": original.replace(b"```text\n", b"```yaml\n", 1),
            "order": text.replace(
                accepted_c, "TEST_FIXTURE_ONLY_PLACEHOLDER\n", 1
            ).replace(
                published, accepted_c, 1
            ).replace(
                "TEST_FIXTURE_ONLY_PLACEHOLDER\n", published, 1
            ).encode("utf-8"),
            "duplicate": text.replace(accepted_c, accepted_c + accepted_c, 1).encode("utf-8"),
            "extra": text.replace(accepted_c, accepted_c + "EXTRA: VALUE\n", 1).encode("utf-8"),
            "missing": text.replace(accepted_c, "", 1).encode("utf-8"),
        }
        for name, content in cases.items():
            with self.subTest(name=name):
                target.write_bytes(content)
                self.assert_reference_failure("invalid")
        target.write_bytes(original)

    def test_reference_expectation_and_topology_boundaries(self) -> None:
        """TS-REF-03: compared values and pinned local objects fail closed."""
        reference = self.root / self.TERMINAL_DOCS[2]
        contract = self.root / self.TERMINAL_DOCS[1]
        reference_original = reference.read_text(encoding="utf-8")
        contract_original = contract.read_text(encoding="utf-8")
        m17 = self.M17
        m17_tree = self.rev_parse(f"{m17}^{{tree}}")
        cases = (
            ("release-id", "400460267", "400460268", False),
            (
                "manifest-digest",
                "f0f2505a7cfceba71081ff994c859500a47b3da22b5eb949c0f8a7ebbc5961cf",
                "0" * 64,
                False,
            ),
            (
                "owner-record-digest",
                "f9c679196deab62311d785e76c23215ec18c93fd439bd4c25780e245b7a755b3",
                "1" * 64,
                False,
            ),
            (
                "timestamp",
                "2026-10-04T20:07:58.267870Z",
                "2026-10-04T20:07:58.267871Z",
                False,
            ),
            (
                "scope",
                "V1_X_LIFECYCLE_TERMINAL_CLOSURE_QUALIFIED",
                "V1_X_LIFECYCLE_TERMINAL_CLOSURE_OTHER",
                False,
            ),
            ("annotated-tag-object-type", self.A17, self.C, True),
            ("accepted-c", self.C, m17, True),
            (
                "accepted-c-tree",
                self.C_TREE,
                m17_tree,
                True,
            ),
            ("published-m17", m17, self.C, True),
            ("tag-direct-target", m17, self.C, True),
        )
        fields = {
            "release-id": "GITHUB_RELEASE_ID",
            "manifest-digest": "PHASE9M_MANIFEST_SHA256",
            "owner-record-digest": "PHASE9M_OWNER_RECORD_SHA256",
            "timestamp": "OWNER_DISPOSITION_AT_UTC",
            "scope": "SOURCE_SCOPE",
            "annotated-tag-object-type": "ANNOTATED_TAG_A17",
            "accepted-c": "ACCEPTED_C",
            "accepted-c-tree": "ACCEPTED_C_TREE",
            "published-m17": "PUBLISHED_M17",
            "tag-direct-target": "TAG_DIRECT_TARGET",
        }
        for name, old, new, mutate_both in cases:
            with self.subTest(name=name):
                key = fields[name]
                reference.write_text(
                    reference_original.replace(
                        f"{key}: {old}", f"{key}: {new}", 1
                    ),
                    encoding="utf-8",
                )
                if mutate_both:
                    contract.write_text(
                        contract_original.replace(
                            f"EXPECTED_{key}: {old}",
                            f"EXPECTED_{key}: {new}",
                            1,
                        ),
                        encoding="utf-8",
                    )
                self.assert_reference_failure("invalid")
                reference.write_text(reference_original, encoding="utf-8")
                contract.write_text(contract_original, encoding="utf-8")

    def test_terminal_state_selection_never_downgrades(self) -> None:
        """TS-REF-01: missing/mixed/new material cannot select old closure."""
        state_path = self.root / self.TERMINAL_DOCS[0]
        reference = self.root / self.TERMINAL_DOCS[2]
        original = state_path.read_text(encoding="utf-8")

        state_path.write_text(
            original.replace(
                "CURRENT_PHASE: NONE_TERMINAL_DISPOSITION_DECLARED",
                "CURRENT_PHASE: NONE_CLOSURE_CANDIDATE",
                1,
            ),
            encoding="utf-8",
        )
        completed, payload = self.run_json()
        self.assertEqual(completed.returncode, 1)
        self.assertNotEqual(payload["state"], "post-release-closure-candidate")
        self.assertIn("CKRS019", [item["rule"] for item in payload["violations"]])

        old = original
        replacements = {
            "CURRENT_PHASE: NONE_TERMINAL_DISPOSITION_DECLARED": "CURRENT_PHASE: NONE_CLOSURE_CANDIDATE",
            "V1_13_17_STATE: TERMINAL_DISPOSITION_DECLARATION_PRESENT": "V1_13_17_STATE: CLOSURE_CANDIDATE_PENDING_TERMINAL_AUDIT",
            "V1_X_FULL_CLOSURE: ESTABLISHED_AT_C_SOURCE_SCOPED_QUALIFIED": "V1_X_FULL_CLOSURE: NOT_ESTABLISHED",
            "EXTERNAL_TERMINAL_DISPOSITION: ESTABLISHED_AT_C_SOURCE_SCOPED_QUALIFIED\n": "",
            "TRACKED_TERMINAL_SERIALIZATION: DECLARATION_PRESENT_NON_AUTHORIZING\n": "",
            "TERMINAL_DISPOSITION_REFERENCE: docs/release/v1.13/v1.13.17-terminal-disposition-reference.md\n": "",
        }
        for before, after in replacements.items():
            old = old.replace(before, after, 1)
        state_path.write_text(old, encoding="utf-8")
        completed, payload = self.run_json()
        self.assertEqual(completed.returncode, 1)
        self.assertNotEqual(payload["state"], "post-release-closure-candidate")
        self.assertIn("CKRS019", [item["rule"] for item in payload["violations"]])
        state_path.write_text(original, encoding="utf-8")
        self.assertTrue(reference.is_file())

    def push_environment(
        self, event: Path, *, before: str, created: bool
    ) -> dict[str, str]:
        event.write_text(
            json.dumps(
                {
                    "ref": f"refs/heads/{self.TERMINAL_BRANCH}",
                    "before": before,
                    "after": self.candidate,
                    "created": created,
                    "deleted": False,
                    "forced": False,
                    "repository": {"full_name": "franchoy/coldkeep"},
                }
            ),
            encoding="utf-8",
        )
        return {
            "GITHUB_ACTIONS": "true",
            "GITHUB_EVENT_NAME": "push",
            "GITHUB_REF": f"refs/heads/{self.TERMINAL_BRANCH}",
            "GITHUB_REF_NAME": self.TERMINAL_BRANCH,
            "GITHUB_REF_TYPE": "branch",
            "GITHUB_EVENT_PATH": str(event),
            "GITHUB_REPOSITORY": "franchoy/coldkeep",
            "GITHUB_SHA": self.candidate,
        }

    def test_candidate_detachment_and_push_contexts(self) -> None:
        """TS-CTX-01/02: local detachment rejects; exact fixed-C pushes pass."""
        git(self.root, "checkout", "--quiet", "--detach", self.candidate)
        completed, payload = self.run_json(
            "post-release-terminal-serialization-candidate"
        )
        self.assertEqual(completed.returncode, 1)
        self.assertIn("CKRS016", [item["rule"] for item in payload["violations"]])

        event = self.root / "terminal-push.json"
        for name, before, created in (
            ("creation", "0" * 40, True),
            ("fixed-c-update", self.C, False),
        ):
            with self.subTest(name=name):
                env = self.push_environment(event, before=before, created=created)
                completed, payload = self.run_json(env=env)
                self.assertEqual(completed.returncode, 0, completed.stdout)
                self.assertEqual(
                    payload["evidence_scope"],
                    "GITHUB_TERMINAL_SERIALIZATION_PUSH_CONTEXT_CONSISTENCY",
                )
        env = self.push_environment(event, before="1" * 40, created=False)
        completed, payload = self.run_json(
            "post-release-terminal-serialization-candidate", env=env
        )
        self.assertEqual(completed.returncode, 1)
        self.assertIn("CKRS016", [item["rule"] for item in payload["violations"]])
        for name, mutate_event, mutate_env in (
            ("forced", {"forced": True}, {}),
            ("deleted", {"deleted": True}, {}),
            ("wrong-repository", {}, {"GITHUB_REPOSITORY": "fork/coldkeep"}),
            ("wrong-ref", {}, {"GITHUB_REF": "refs/heads/release/other"}),
            ("wrong-sha", {}, {"GITHUB_SHA": self.C}),
        ):
            with self.subTest(rejection=name):
                env = self.push_environment(event, before=self.C, created=False)
                if mutate_event:
                    body = json.loads(event.read_text(encoding="utf-8"))
                    body.update(mutate_event)
                    event.write_text(json.dumps(body), encoding="utf-8")
                env.update(mutate_env)
                completed, payload = self.run_json(
                    "post-release-terminal-serialization-candidate", env=env
                )
                self.assertEqual(completed.returncode, 1)
                self.assertIn(
                    "CKRS016", [item["rule"] for item in payload["violations"]]
                )
        git(self.root, "checkout", "--quiet", "-b", "fixture/wrong", self.candidate)
        env = self.push_environment(event, before=self.C, created=False)
        completed, payload = self.run_json(
            "post-release-terminal-serialization-candidate", env=env
        )
        self.assertEqual(completed.returncode, 1)
        self.assertIn("CKRS016", [item["rule"] for item in payload["violations"]])

    def pr_environment(self, event: Path, checkout: str) -> dict[str, str]:
        return {
            "GITHUB_ACTIONS": "true",
            "GITHUB_EVENT_NAME": "pull_request",
            "GITHUB_REF": "refs/pull/7/merge",
            "GITHUB_REF_NAME": "7/merge",
            "GITHUB_HEAD_REF": self.TERMINAL_BRANCH,
            "GITHUB_BASE_REF": "main",
            "GITHUB_EVENT_PATH": str(event),
            "GITHUB_REPOSITORY": "franchoy/coldkeep",
            "GITHUB_SHA": checkout,
        }

    def write_terminal_pr(self, event: Path, merge_sha: object = None) -> None:
        event.write_text(
            json.dumps(
                {
                    "repository": {"full_name": "franchoy/coldkeep"},
                    "number": 7,
                    "pull_request": {
                        "base": {
                            "ref": "main",
                            "sha": self.C,
                            "repo": {"full_name": "franchoy/coldkeep"},
                        },
                        "head": {
                            "ref": self.TERMINAL_BRANCH,
                            "sha": self.candidate,
                            "repo": {"full_name": "franchoy/coldkeep"},
                        },
                        "merge_commit_sha": merge_sha,
                    },
                }
            ),
            encoding="utf-8",
        )

    def commit_tree(self, *parents: str) -> str:
        tree = self.rev_parse(f"{self.candidate}^{{tree}}")
        command = [
            resolved_executable("git"), "-C", str(self.root), "commit-tree", tree
        ]
        for parent in parents:
            command.extend(("-p", parent))
        command.extend(("-m", "TEST_FIXTURE_ONLY terminal topology"))
        return run_process(
            command,
            check=True,
        ).stdout.strip()

    def test_direct_and_synthetic_pr_contexts(self) -> None:
        """TS-CTX-03: direct and synthetic terminal PR forms are exact."""
        event = self.root / "terminal-pr.json"
        self.write_terminal_pr(event)
        diagnostic = Path(self.temporary.name) / "terminal-pr-diagnostic.json"
        completed, payload = self.run_json(
            env=self.pr_environment(event, self.candidate),
            diagnostic=diagnostic,
        )
        self.assertEqual(completed.returncode, 0, completed.stdout)
        self.assertEqual(
            payload["evidence_scope"],
            "GITHUB_TERMINAL_SERIALIZATION_PR_CONTEXT_CONSISTENCY",
        )
        record = json.loads(diagnostic.read_text(encoding="utf-8"))
        self.assertEqual(len(record["validator"]["sources"]), 5)
        self.assertGreaterEqual(len(record["evaluations"]), 2)
        for evaluation in record["evaluations"]:
            self.assertEqual(
                evaluation["runtime_context"]["head_ref"]["value"],
                self.TERMINAL_BRANCH,
            )

        synthetic = self.commit_tree(self.C, self.candidate)
        git(self.root, "checkout", "--quiet", "--detach", synthetic)
        self.write_terminal_pr(event, "1" * 40)
        completed, payload = self.run_json(
            env=self.pr_environment(event, synthetic)
        )
        self.assertEqual(completed.returncode, 0, completed.stdout)
        self.assertEqual(
            payload["evidence_scope"],
            "GITHUB_TERMINAL_SERIALIZATION_PR_CONTEXT_CONSISTENCY",
        )
        event.write_text(
            event.read_text(encoding="utf-8").replace(
                "franchoy/coldkeep", "fork/coldkeep", 1
            ),
            encoding="utf-8",
        )
        completed, payload = self.run_json(
            "post-release-terminal-serialization-candidate",
            env=self.pr_environment(event, synthetic),
        )
        self.assertEqual(completed.returncode, 1)
        self.assertIn("CKRS016", [item["rule"] for item in payload["violations"]])
        for name, mutation in (
            ("wrong-base", ("base", "sha", "1" * 40)),
            ("wrong-head", ("head", "sha", self.C)),
            ("wrong-head-ref", ("head", "ref", "release/other")),
        ):
            with self.subTest(rejection=name):
                self.write_terminal_pr(event, "1" * 40)
                body = json.loads(event.read_text(encoding="utf-8"))
                side, key, value = mutation
                body["pull_request"][side][key] = value
                event.write_text(json.dumps(body), encoding="utf-8")
                completed, payload = self.run_json(
                    "post-release-terminal-serialization-candidate",
                    env=self.pr_environment(event, synthetic),
                )
                self.assertEqual(completed.returncode, 1)
                self.assertIn(
                    "CKRS016", [item["rule"] for item in payload["violations"]]
                )
        for name, merge_value, remove in (
            ("zero-merge-metadata", "0" * 40, False),
            ("malformed-merge-metadata", "not-a-sha", False),
            ("missing-merge-metadata", None, True),
        ):
            with self.subTest(rejection=name):
                self.write_terminal_pr(event, merge_value)
                if remove:
                    body = json.loads(event.read_text(encoding="utf-8"))
                    del body["pull_request"]["merge_commit_sha"]
                    event.write_text(json.dumps(body), encoding="utf-8")
                completed, payload = self.run_json(
                    "post-release-terminal-serialization-candidate",
                    env=self.pr_environment(event, synthetic),
                )
                self.assertEqual(completed.returncode, 1)
                self.assertIn(
                    "CKRS016", [item["rule"] for item in payload["violations"]]
                )

    def test_terminal_main_local_and_event_contexts(self) -> None:
        """TS-CTX-04: exact S works attached or event-backed detached."""
        main = self.commit_tree(self.C, self.candidate)
        git(self.root, "checkout", "--quiet", "-B", "main", main)
        completed, payload = self.run_json()
        self.assertEqual(completed.returncode, 0, completed.stdout)
        self.assertEqual(
            payload["state"], "post-release-terminal-serialization-main"
        )
        self.assertEqual(
            payload["evidence_scope"],
            "LOCAL_TERMINAL_SERIALIZATION_MAIN_ARTIFACT_ONLY",
        )

        event = self.root / "terminal-main-push.json"
        event.write_text(
            json.dumps(
                {
                    "ref": "refs/heads/main",
                    "before": self.C,
                    "after": main,
                    "created": False,
                    "deleted": False,
                    "forced": False,
                    "repository": {"full_name": "franchoy/coldkeep"},
                }
            ),
            encoding="utf-8",
        )
        git(self.root, "checkout", "--quiet", "--detach", main)
        env = {
            "GITHUB_ACTIONS": "true",
            "GITHUB_EVENT_NAME": "push",
            "GITHUB_REF": "refs/heads/main",
            "GITHUB_REF_NAME": "main",
            "GITHUB_REF_TYPE": "branch",
            "GITHUB_EVENT_PATH": str(event),
            "GITHUB_REPOSITORY": "franchoy/coldkeep",
            "GITHUB_SHA": main,
        }
        completed, payload = self.run_json(env=env)
        self.assertEqual(completed.returncode, 0, completed.stdout)
        self.assertEqual(
            payload["evidence_scope"],
            "GITHUB_TERMINAL_SERIALIZATION_MAIN_CONTEXT_CONSISTENCY",
        )
        completed, payload = self.run_json(
            "post-release-terminal-serialization-main"
        )
        self.assertEqual(completed.returncode, 1)
        self.assertIn("CKRS016", [item["rule"] for item in payload["violations"]])
        for name, key, value in (
            ("wrong-before", "before", "1" * 40),
            ("forced", "forced", True),
            ("deleted", "deleted", True),
        ):
            with self.subTest(rejection=name):
                body = json.loads(event.read_text(encoding="utf-8"))
                body.update(
                    {
                        "before": self.C,
                        "after": main,
                        "forced": False,
                        "deleted": False,
                    }
                )
                body[key] = value
                event.write_text(json.dumps(body), encoding="utf-8")
                completed, payload = self.run_json(
                    "post-release-terminal-serialization-main", env=env
                )
                self.assertEqual(completed.returncode, 1)
                self.assertIn(
                    "CKRS016", [item["rule"] for item in payload["violations"]]
                )

        wrong_parent = self.commit_tree(self.M17, self.candidate)
        git(self.root, "checkout", "--quiet", "--detach", wrong_parent)
        body = json.loads(event.read_text(encoding="utf-8"))
        body.update(
            {
                "before": self.C,
                "after": wrong_parent,
                "forced": False,
                "deleted": False,
            }
        )
        event.write_text(json.dumps(body), encoding="utf-8")
        env["GITHUB_SHA"] = wrong_parent
        completed, payload = self.run_json(
            "post-release-terminal-serialization-main", env=env
        )
        self.assertEqual(completed.returncode, 1)
        self.assertIn("CKRS016", [item["rule"] for item in payload["violations"]])

    def test_terminal_diagnostic_remains_non_authorizing(self) -> None:
        """TS-AUTH-02: diagnostics retain five sources and no authority claim."""
        diagnostic = Path(self.temporary.name) / "terminal-diagnostic.json"
        completed, payload = self.run_json(diagnostic=diagnostic)
        self.assertEqual(completed.returncode, 0, completed.stdout)
        self.assertEqual(payload["authorization_status"], "NOT_EVALUATED_BY_VALIDATOR")
        self.assertEqual(payload["certification_status"], "PENDING_EXTERNAL_EVIDENCE")
        record = json.loads(diagnostic.read_text(encoding="utf-8"))
        self.assertEqual(len(record["validator"]["sources"]), 5)
        rendered = json.dumps(record, sort_keys=True)
        self.assertNotIn("PHASE9M_MANIFEST_SHA256", rendered)
        self.assertNotIn("EFFECTIVE_TRACKED_TERMINAL_SERIALIZATION", rendered)


class LifecycleBoundaryCompatibilityTests(unittest.TestCase):
    boundaries = LifecycleBoundaries(merge=16, publication=18, closure=19)

    @staticmethod
    def progression(next_phase: int) -> list[str]:
        return [
            "Complete" if phase < next_phase else
            "Next" if phase == next_phase else
            "Not started"
            for phase in range(20)
        ]

    def test_consecutive_lifecycle_contract_remains_valid(self) -> None:
        boundaries = LifecycleBoundaries(merge=0, publication=1, closure=2)
        self.assertTrue(
            lifecycle_boundaries_match_topology(boundaries, [0, 1, 2])
        )

    def test_nonconsecutive_16_18_19_topology_is_valid(self) -> None:
        self.assertTrue(
            lifecycle_boundaries_match_topology(
                self.boundaries,
                list(range(20)),
            )
        )

    def test_invalid_boundary_topologies_fail(self) -> None:
        phases = list(range(20))
        invalid = (
            LifecycleBoundaries(18, 16, 19),
            LifecycleBoundaries(16, 16, 19),
            LifecycleBoundaries(16, 18, 20),
            LifecycleBoundaries(16, 18, 18),
        )
        for boundaries in invalid:
            with self.subTest(boundaries=boundaries):
                self.assertFalse(
                    lifecycle_boundaries_match_topology(boundaries, phases)
                )

    def test_development_phase_2_next_is_valid(self) -> None:
        self.assertTrue(
            lifecycle_progression_valid(
                "development", self.progression(2), self.boundaries
            )
        )

    def test_pre_release_phase_16_next_is_valid(self) -> None:
        self.assertTrue(
            lifecycle_progression_valid(
                "pre-release", self.progression(16), self.boundaries
            )
        )

    def test_merged_not_tagged_phase_17_next_is_valid(self) -> None:
        self.assertTrue(
            lifecycle_progression_valid(
                "merged-not-tagged", self.progression(17), self.boundaries
            )
        )

    def test_merged_not_tagged_phase_18_next_is_valid(self) -> None:
        self.assertTrue(
            lifecycle_progression_valid(
                "merged-not-tagged", self.progression(18), self.boundaries
            )
        )

    def test_tagged_uses_same_bounded_next_window(self) -> None:
        for phase in (17, 18):
            with self.subTest(phase=phase):
                self.assertTrue(
                    lifecycle_progression_valid(
                        "tagged", self.progression(phase), self.boundaries
                    )
                )

    def test_merged_next_at_or_before_merge_is_invalid(self) -> None:
        for phase in (15, 16):
            with self.subTest(phase=phase):
                self.assertFalse(
                    lifecycle_progression_valid(
                        "merged-not-tagged",
                        self.progression(phase),
                        self.boundaries,
                    )
                )

    def test_merged_or_tagged_next_after_publication_is_invalid(self) -> None:
        for state in ("merged-not-tagged", "tagged"):
            with self.subTest(state=state):
                self.assertFalse(
                    lifecycle_progression_valid(
                        state, self.progression(19), self.boundaries
                    )
                )

    def test_post_release_pending_requires_phase_19_next(self) -> None:
        self.assertTrue(
            lifecycle_progression_valid(
                "post-release-pending-closure",
                self.progression(19),
                self.boundaries,
            )
        )
        self.assertFalse(
            lifecycle_progression_valid(
                "post-release-pending-closure",
                self.progression(18),
                self.boundaries,
            )
        )

    def test_post_release_closed_requires_all_complete(self) -> None:
        self.assertTrue(
            lifecycle_progression_valid(
                "post-release-closed", ["Complete"] * 20, self.boundaries
            )
        )
        self.assertFalse(
            lifecycle_progression_valid(
                "post-release-closed", self.progression(19), self.boundaries
            )
        )


class _V2ProgressionFixture:
    """Fixture-owned predecessor -> M17 -> C -> H1 -> H2 graph."""

    PATHS = (
        "docs/release/v1.13/v1.13.17-release-state-validator-contract.md",
        "docs/release/v1.13/v1.13.17-release-state.md",
        "docs/release/v1.13/v1.13.17-security-toolchain-progression-reference.md",
        "docs/release/v1.13/v1.13.17-source-test-allowlist.md",
        "scripts/release_state_support.py", "scripts/test_validate_governance.py",
        "scripts/test_validate_release_state.py", "scripts/validate_governance.py",
        "scripts/validate_release_state.py",
    )
    PREDECESSOR_PATH = (
        "docs/release/v1.13/v1.13.17-predecessor-disposition.md"
    )
    TERMINAL_PATH = (
        "docs/release/v1.13/v1.13.17-terminal-disposition-reference.md"
    )
    PUBLISHED_H3_SOURCE = "07667f0bc1a1a089977a41a2bc56ee3ad15f00fd"
    PUBLISHED_H4_SOURCE = "797f22b42f62636bf406081fb8544b0b5474595c"
    PUBLISHED_H5_SOURCE = "8ea4b56614f4e1be0986321b12042dfe4627fd4b"

    def __init__(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "repo"
        self.root.mkdir()
        source = SCRIPT.parents[1]
        tracked = run_process(
            [resolved_executable("git"), "-C", str(source), "ls-tree", "-r", "--name-only", "-z", "HEAD"],
            check=True,
        ).stdout.split("\0")
        for name in tracked:
            if not name:
                continue
            relative = Path(name)
            target = self.root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            if name in self.PATHS[:3]:
                content = run_process(
                    [
                        resolved_executable("git"), "-C", str(source), "show",
                        f"{self.PUBLISHED_H3_SOURCE}:{name}",
                    ],
                    check=True,
                ).stdout
                target.write_text(content, encoding="utf-8")
            elif relative in SUCCESSOR_HISTORICAL_PATHS:
                content = run_process(
                    [
                        resolved_executable("git"), "-C", str(source), "show",
                        f"HEAD:{name}",
                    ],
                    check=True,
                ).stdout
                target.write_text(content, encoding="utf-8")
            else:
                shutil.copy2(source / relative, target, follow_symlinks=False)
        git(self.root, "init")
        git(self.root, "config", "user.name", "fixture")
        git(self.root, "config", "user.email", "fixture@example.invalid")
        git(self.root, "add", ".")
        git(self.root, "commit", "--quiet", "-m", "TEST_FIXTURE_ONLY predecessor P")
        self.P = self.rev("HEAD")
        git(self.root, "tag", "-a", "v1.13.16", "-m", "TEST_FIXTURE_ONLY predecessor")
        self.PREDECESSOR_TAG = self.rev("refs/tags/v1.13.16")
        self.set_fields(
            self.PREDECESSOR_PATH,
            {
                "TAG_OBJECT": self.PREDECESSOR_TAG,
                "DIRECT_TARGET": self.P,
                "SUCCESSOR_BASE": self.P,
            },
        )
        git(self.root, "add", self.PREDECESSOR_PATH)
        git(self.root, "commit", "--quiet", "-m", "TEST_FIXTURE_ONLY published M17")
        self.M17 = self.rev("HEAD")
        git(self.root, "tag", "-a", "v1.13.17", "-m", "TEST_FIXTURE_ONLY publication")
        self.A17 = self.rev("refs/tags/v1.13.17")
        discriminator = self.root / "TEST_FIXTURE_ONLY-v2-c"
        discriminator.write_text("fixture-owned accepted C tree\n", encoding="utf-8")
        git(self.root, "add", discriminator.name)
        git(self.root, "commit", "--quiet", "-m", "TEST_FIXTURE_ONLY accepted C")
        self.C = self.rev("HEAD")
        self.C_TREE = self.rev("HEAD^{tree}")
        self.sync_terminal_identities()
        self.sync_progression_identities(
            {
                "BASE_C": self.C,
                "BASE_C_TREE": self.C_TREE,
            }
        )
        git(self.root, "add", self.PATHS[0], self.PATHS[2], self.TERMINAL_PATH)
        git(self.root, "commit", "--quiet", "-m", "TEST_FIXTURE_ONLY historical H1")
        self.H1 = self.rev("HEAD")
        self.H1_TREE = self.rev("HEAD^{tree}")
        self.sync_progression_identities(
            {
                "HISTORICAL_H1": self.H1,
                "HISTORICAL_H1_PARENT": self.C,
                "HISTORICAL_H1_TREE": self.H1_TREE,
            }
        )
        git(self.root, "add", self.PATHS[0], self.PATHS[2])
        git(self.root, "commit", "--quiet", "-m", "TEST_FIXTURE_ONLY published H2")
        self.H2 = self.rev("HEAD")
        self.H2_TREE = self.rev("HEAD^{tree}")
        self.sync_progression_identities(
            {
                "PUBLISHED_H2": self.H2,
                "PUBLISHED_H2_PARENT": self.H1,
                "PUBLISHED_H2_TREE": self.H2_TREE,
                "SUCCESSOR_PARENT": self.H2,
            }
        )
        self.sync_digest()
        git(self.root, "checkout", "--quiet", "-b", "release/v1.13.17-terminal-serialization")
        if self.object_type(self.PREDECESSOR_TAG) != "tag":
            raise AssertionError("fixture predecessor tag is not annotated")
        if self.object_type(self.A17) != "tag":
            raise AssertionError("fixture publication tag is not annotated")
        if self.rev("refs/tags/v1.13.16^{}") != self.P:
            raise AssertionError("fixture predecessor tag target disagrees")
        if self.rev("refs/tags/v1.13.17^{}") != self.M17:
            raise AssertionError("fixture publication tag target disagrees")
        if self.rev(f"{self.H1}^") != self.C or self.rev(f"{self.H2}^") != self.H1:
            raise AssertionError("fixture C/H1/H2 parent topology disagrees")

    def close(self) -> None:
        self.temporary.cleanup()

    def rev(self, value: str) -> str:
        return run_process([resolved_executable("git"), "-C", str(self.root), "rev-parse", value], check=True).stdout.strip()

    def object_type(self, value: str) -> str:
        return run_process(
            [
                resolved_executable("git"),
                *FIXTURE_GIT_CONFIG,
                "-C",
                str(self.root),
                "cat-file",
                "-t",
                value,
            ],
            check=True,
        ).stdout.strip()

    def set_fields(self, relative: str, values: dict[str, str]) -> None:
        target = self.root / relative
        text = target.read_text(encoding="utf-8")
        for key, value in values.items():
            text, count = re.subn(
                rf"(?m)^{re.escape(key)}: \S+$",
                f"{key}: {value}",
                text,
                count=1,
            )
            if count != 1:
                raise AssertionError(
                    f"fixture field replacement missing or ambiguous in {relative}: {key}"
                )
        target.write_text(text, encoding="utf-8")

    def sync_terminal_identities(self) -> None:
        identities = {
            "ACCEPTED_C": self.C,
            "ACCEPTED_C_TREE": self.C_TREE,
            "PUBLISHED_M17": self.M17,
            "ANNOTATED_TAG_A17": self.A17,
            "TAG_DIRECT_TARGET": self.M17,
        }
        self.set_fields(self.TERMINAL_PATH, identities)
        self.set_fields(
            self.PATHS[0],
            {f"EXPECTED_{key}": value for key, value in identities.items()},
        )

    def sync_progression_identities(self, identities: dict[str, str]) -> None:
        self.set_fields(self.PATHS[2], identities)
        self.set_fields(
            self.PATHS[0],
            {f"EXPECTED_{key}": value for key, value in identities.items()},
        )

    def sync_digest(self) -> None:
        reference = self.root / self.PATHS[2]
        contract = self.root / self.PATHS[0]
        digest = hashlib.sha256(reference.read_bytes()).hexdigest()
        text, count = re.subn(r"(?m)^EXPECTED_REFERENCE_SHA256: [0-9a-f]{64}$", f"EXPECTED_REFERENCE_SHA256: {digest}", contract.read_text(encoding="utf-8"), count=1)
        if count != 1:
            raise AssertionError("missing v2 digest expectation")
        contract.write_text(text, encoding="utf-8")

    def progression(self) -> release_state_support.SecurityToolchainProgression:
        contract_path = self.root / self.PATHS[0]
        contract = release_state_support.Document(self.root, self.PATHS[0], contract_path.read_text(encoding="utf-8").splitlines())
        progression, detail = release_state_support.load_security_toolchain_progression(self.root, contract, self.PATHS[2])
        if progression is None:
            raise AssertionError(detail)
        return progression

    def make_h3(self) -> str:
        dirty = {self.PATHS[0], self.PATHS[2]}
        for relative in self.PATHS:
            if relative in dirty:
                continue
            target = self.root / relative
            marker = "# TEST_FIXTURE_ONLY v2 delta\n" if target.suffix == ".py" else "<!-- TEST_FIXTURE_ONLY v2 delta -->\n"
            with target.open("a", encoding="utf-8") as stream:
                stream.write(marker)
        git(self.root, "add", *self.PATHS)
        git(self.root, "commit", "--quiet", "-m", "TEST_FIXTURE_ONLY H2 successor H3")
        h3 = self.rev("HEAD")
        changed = run_process(
            [
                resolved_executable("git"),
                *FIXTURE_GIT_CONFIG,
                "-C",
                str(self.root),
                "diff-tree",
                "--no-commit-id",
                "--name-status",
                "-r",
                self.H2,
                h3,
            ],
            check=True,
        ).stdout.splitlines()
        if changed != [f"M\t{relative}" for relative in self.PATHS]:
            raise AssertionError(f"fixture successor delta disagrees: {changed!r}")
        for relative in self.PATHS:
            mode = run_process(
                [
                    resolved_executable("git"),
                    *FIXTURE_GIT_CONFIG,
                    "-C",
                    str(self.root),
                    "ls-tree",
                    h3,
                    "--",
                    relative,
                ],
                check=True,
            ).stdout.split()[0]
            expected = "100755" if relative == "scripts/validate_governance.py" else "100644"
            if mode != expected:
                raise AssertionError(
                    f"fixture successor mode disagrees for {relative}: {mode}"
                )
        status = run_process(
            [
                resolved_executable("git"),
                *FIXTURE_GIT_CONFIG,
                "-C",
                str(self.root),
                "status",
                "--porcelain=v2",
                "--untracked-files=all",
                "-z",
            ],
            check=True,
        ).stdout
        if status:
            raise AssertionError(f"fixture successor is not clean: {status!r}")
        return h3

    def documents(
        self,
    ) -> tuple[Path, Path, Path]:
        return (
            self.root / self.PATHS[1],
            self.root / self.PATHS[0],
            self.root / self.PATHS[2],
        )

    def contract_document(self) -> release_state_support.Document:
        contract = self.root / self.PATHS[0]
        return release_state_support.Document(
            self.root,
            self.PATHS[0],
            contract.read_text(encoding="utf-8").splitlines(),
        )

    def state_document(self) -> release_state_support.Document:
        state = self.root / self.PATHS[1]
        return release_state_support.Document(
            self.root,
            self.PATHS[1],
            state.read_text(encoding="utf-8").splitlines(),
        )

    def load_progression(
        self,
    ) -> tuple[release_state_support.SecurityToolchainProgression | None, str | None]:
        return release_state_support.load_security_toolchain_progression(
            self.root, self.contract_document(), self.PATHS[2]
        )

    def select_progression(
        self,
    ) -> tuple[str, release_state_support.SecurityToolchainProgression | None, str | None]:
        return validate_release_state.security_progression_selection(
            self.root,
            "1.13.17",
            self.state_document(),
            self.contract_document(),
        )

    def run_json(
        self,
        state: str = "auto",
        *,
        env: dict[str, str] | None = None,
        diagnostic: Path | None = None,
        copied_validator: bool = False,
    ) -> tuple[ProcessResult, dict[str, object]]:
        arguments = ["--state", state, "--json"]
        if diagnostic is not None:
            arguments.extend(("--diagnostic-json", str(diagnostic)))
        if copied_validator:
            completed = run_validator_script(
                self.root / "scripts/validate_release_state.py",
                self.root,
                *arguments,
                env=env,
            )
        else:
            completed = run_validator(self.root, *arguments, env=env)
        return completed, json.loads(completed.stdout)

    def run_human(
        self,
        state: str = "auto",
        *,
        env: dict[str, str] | None = None,
        copied_validator: bool = False,
    ) -> ProcessResult:
        if copied_validator:
            return run_validator_script(
                self.root / "scripts/validate_release_state.py",
                self.root,
                "--state",
                state,
                env=env,
            )
        return run_validator(self.root, "--state", state, env=env)

    def checkout_detached(self, commit: str) -> None:
        git(self.root, "checkout", "--quiet", "--force", "--detach", commit)

    def checkout_branch(self, branch: str, commit: str) -> None:
        git(self.root, "checkout", "--quiet", "--force", "-B", branch, commit)

    def raw_commit_tree(self, tree: str, *parents: str) -> str:
        command = [
            resolved_executable("git"),
            *FIXTURE_GIT_CONFIG,
            "-C",
            str(self.root),
            "commit-tree",
            tree,
        ]
        for parent in parents:
            command.extend(("-p", parent))
        command.extend(("-m", "TEST_FIXTURE_ONLY v2 topology"))
        return run_process(command, check=True).stdout.strip()

    def synthetic_merge(self, h3: str) -> str:
        return self.raw_commit_tree(self.rev(f"{h3}^{{tree}}"), self.C, h3)

    def hash_blob(self, content: bytes) -> str:
        blob = Path(self.temporary.name) / "TEST_FIXTURE_ONLY-v2-blob"
        blob.write_bytes(content)
        return run_process(
            [
                resolved_executable("git"),
                *FIXTURE_GIT_CONFIG,
                "-C",
                str(self.root),
                "hash-object",
                "-w",
                str(blob),
            ],
            check=True,
        ).stdout.strip()

    def variant_tree(
        self,
        base: str,
        changes: tuple[tuple[str, str | None, str | None], ...],
    ) -> str:
        with tempfile.TemporaryDirectory() as directory:
            environment = os.environ.copy()
            environment["GIT_INDEX_FILE"] = str(Path(directory) / "index")
            prefix = [
                resolved_executable("git"),
                *FIXTURE_GIT_CONFIG,
                "-C",
                str(self.root),
            ]
            run_process(prefix + ["read-tree", base], env=environment, check=True)
            for path, mode, object_id in changes:
                if mode is None:
                    run_process(
                        prefix + ["update-index", "--force-remove", "--", path],
                        env=environment,
                        check=True,
                    )
                else:
                    if object_id is None:
                        raise AssertionError("fixture object ID is required")
                    run_process(
                        prefix
                        + [
                            "update-index",
                            "--add",
                            "--cacheinfo",
                            f"{mode},{object_id},{path}",
                        ],
                        env=environment,
                        check=True,
                    )
            return run_process(
                prefix + ["write-tree"], env=environment, check=True
            ).stdout.strip()

    def push_context(
        self, h3: str
    ) -> tuple[Path, dict[str, str]]:
        event = self.root / "TEST_FIXTURE_ONLY-v2-push.json"
        event.write_text(
            json.dumps(
                {
                    "ref": "refs/heads/release/v1.13.17-terminal-serialization",
                    "before": self.H2,
                    "after": h3,
                    "created": False,
                    "deleted": False,
                    "forced": False,
                    "repository": {
                        "full_name": "franchoy/coldkeep",
                        "id": 1158711822,
                    },
                }
            ),
            encoding="utf-8",
        )
        return event, {
            "GITHUB_ACTIONS": "true",
            "GITHUB_EVENT_NAME": "push",
            "GITHUB_REF": "refs/heads/release/v1.13.17-terminal-serialization",
            "GITHUB_REF_NAME": "release/v1.13.17-terminal-serialization",
            "GITHUB_REF_TYPE": "branch",
            "GITHUB_EVENT_PATH": str(event),
            "GITHUB_REPOSITORY": "franchoy/coldkeep",
            "GITHUB_REPOSITORY_ID": "1158711822",
            "GITHUB_SHA": h3,
        }

    def pr_context(
        self,
        h3: str,
        checkout: str,
        *,
        merge_value: object = None,
    ) -> tuple[Path, dict[str, str]]:
        event = self.root / "TEST_FIXTURE_ONLY-v2-pr.json"
        event.write_text(
            json.dumps(
                {
                    "repository": {
                        "full_name": "franchoy/coldkeep",
                        "id": 1158711822,
                    },
                    "number": 123,
                    "pull_request": {
                        "base": {
                            "ref": "main",
                            "sha": self.C,
                            "repo": {"full_name": "franchoy/coldkeep"},
                        },
                        "head": {
                            "ref": "release/v1.13.17-terminal-serialization",
                            "sha": h3,
                            "repo": {"full_name": "franchoy/coldkeep"},
                        },
                        "merge_commit_sha": merge_value,
                    },
                }
            ),
            encoding="utf-8",
        )
        return event, {
            "GITHUB_ACTIONS": "true",
            "GITHUB_EVENT_NAME": "pull_request",
            "GITHUB_REF": "refs/pull/123/merge",
            "GITHUB_REF_NAME": "123/merge",
            "GITHUB_REF_TYPE": "branch",
            "GITHUB_HEAD_REF": "release/v1.13.17-terminal-serialization",
            "GITHUB_BASE_REF": "main",
            "GITHUB_EVENT_PATH": str(event),
            "GITHUB_REPOSITORY": "franchoy/coldkeep",
            "GITHUB_REPOSITORY_ID": "1158711822",
            "GITHUB_SHA": checkout,
        }

    def main_context(
        self, main: str
    ) -> tuple[Path, dict[str, str]]:
        event = self.root / "TEST_FIXTURE_ONLY-v2-main.json"
        event.write_text(
            json.dumps(
                {
                    "ref": "refs/heads/main",
                    "before": self.C,
                    "after": main,
                    "created": False,
                    "deleted": False,
                    "forced": False,
                    "repository": {
                        "full_name": "franchoy/coldkeep",
                        "id": 1158711822,
                    },
                }
            ),
            encoding="utf-8",
        )
        return event, {
            "GITHUB_ACTIONS": "true",
            "GITHUB_EVENT_NAME": "push",
            "GITHUB_REF": "refs/heads/main",
            "GITHUB_REF_NAME": "main",
            "GITHUB_REF_TYPE": "branch",
            "GITHUB_EVENT_PATH": str(event),
            "GITHUB_REPOSITORY": "franchoy/coldkeep",
            "GITHUB_REPOSITORY_ID": "1158711822",
            "GITHUB_SHA": main,
        }


class _V3ProgressionFixture(_V2ProgressionFixture):
    """Fixture-owned H3 anchor and one metadata-bound nine-path H4 child."""

    def __init__(self) -> None:
        super().__init__()
        self.H3 = super().make_h3()
        self.H3_TREE = self.rev(f"{self.H3}^{{tree}}")
        source = SCRIPT.parents[1]
        for relative in self.PATHS[:3]:
            content = run_process(
                [
                    resolved_executable("git"), "-C", str(source), "show",
                    f"{self.PUBLISHED_H4_SOURCE}:{relative}",
                ],
                check=True,
            ).stdout
            (self.root / relative).write_text(content, encoding="utf-8")
        self.sync_terminal_identities()
        self.sync_progression_identities(
            {
                "BASE_C": self.C,
                "BASE_C_TREE": self.C_TREE,
                "HISTORICAL_H1": self.H1,
                "HISTORICAL_H1_PARENT": self.C,
                "HISTORICAL_H1_TREE": self.H1_TREE,
                "PUBLISHED_H2": self.H2,
                "PUBLISHED_H2_PARENT": self.H1,
                "PUBLISHED_H2_TREE": self.H2_TREE,
                "PUBLISHED_H3": self.H3,
                "PUBLISHED_H3_PARENT": self.H2,
                "PUBLISHED_H3_TREE": self.H3_TREE,
                "SUCCESSOR_PARENT": self.H3,
            }
        )
        self.sync_digest()

    def make_h4(self) -> str:
        dirty = set(self.PATHS[:3])
        for relative in self.PATHS:
            if relative in dirty:
                continue
            target = self.root / relative
            marker = (
                "# TEST_FIXTURE_ONLY v3 delta\n"
                if target.suffix == ".py"
                else "<!-- TEST_FIXTURE_ONLY v3 delta -->\n"
            )
            with target.open("a", encoding="utf-8") as stream:
                stream.write(marker)
        git(self.root, "add", *self.PATHS)
        progression = self.progression()
        environment = os.environ.copy()
        environment.update(
            {
                "GIT_AUTHOR_NAME": progression.values["SUCCESSOR_AUTHOR_NAME"],
                "GIT_AUTHOR_EMAIL": progression.values[
                    "SUCCESSOR_AUTHOR_EMAIL"
                ],
                "GIT_AUTHOR_DATE": progression.values["SUCCESSOR_AUTHOR_DATE"],
                "GIT_COMMITTER_NAME": progression.values[
                    "SUCCESSOR_COMMITTER_NAME"
                ],
                "GIT_COMMITTER_EMAIL": progression.values[
                    "SUCCESSOR_COMMITTER_EMAIL"
                ],
                "GIT_COMMITTER_DATE": progression.values["SUCCESSOR_COMMITTER_DATE"],
            }
        )
        run_process(
            [
                resolved_executable("git"), *FIXTURE_GIT_CONFIG,
                "-C", str(self.root), "commit", "--quiet", "-m",
                progression.values["SUCCESSOR_SUBJECT"],
            ],
            env=environment,
            check=True,
        )
        h4 = self.rev("HEAD")
        changed = run_process(
            [
                resolved_executable("git"), *FIXTURE_GIT_CONFIG,
                "-C", str(self.root), "diff-tree", "--no-commit-id",
                "--name-status", "-r", self.H3, h4,
            ],
            check=True,
        ).stdout.splitlines()
        if changed != [f"M\t{relative}" for relative in self.PATHS]:
            raise AssertionError(f"fixture H4 delta disagrees: {changed!r}")
        return h4

    def push_context(self, h4: str) -> tuple[Path, dict[str, str]]:
        event, environment = super().push_context(h4)
        body = json.loads(event.read_text(encoding="utf-8"))
        body["before"] = self.H3
        event.write_text(json.dumps(body), encoding="utf-8")
        return event, environment


class TerminalSerializationV3ContractTests(unittest.TestCase):
    """Bound the authorized H3-to-H4 topology, delta, identity, and contexts."""

    def setUp(self) -> None:
        self.fixture = _V3ProgressionFixture()
        self.addCleanup(self.fixture.close)

    def test_v3_exact_h3_and_h4_statuses_and_local_scope(self) -> None:
        progression = self.fixture.progression()
        self.assertEqual(
            release_state_support.security_progression_status(
                self.fixture.root, self.fixture.H3, progression
            ),
            "PUBLISHED_H3_STRUCTURAL_ONLY_ORIGINAL_QUALITY_FAILED",
        )
        h4 = self.fixture.make_h4()
        self.assertEqual(self.fixture.rev(f"{h4}^"), self.fixture.H3)
        self.assertTrue(
            release_state_support.progressed_h2_artifact(
                self.fixture.root, h4, progression
            )
        )
        completed, payload = self.fixture.run_json()
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertEqual(payload["security_progression"], "H3_SUCCESSOR_STRUCTURAL_ONLY")
        self.assertEqual(payload["evidence_scope"], "LOCAL_H3_SUCCESSOR_CANDIDATE")

    def test_v3_push_pr_and_future_main_contexts(self) -> None:
        h4 = self.fixture.make_h4()
        _, push_environment = self.fixture.push_context(h4)
        completed, payload = self.fixture.run_json(env=push_environment)
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertEqual(
            payload["evidence_scope"],
            "GITHUB_H3_SUCCESSOR_PUSH_CONTEXT_CONSISTENCY",
        )
        _, direct_environment = self.fixture.pr_context(h4, h4)
        completed, payload = self.fixture.run_json(env=direct_environment)
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertEqual(
            payload["evidence_scope"],
            "GITHUB_H3_SUCCESSOR_PR_DIRECT_CONTEXT_CONSISTENCY",
        )
        synthetic = self.fixture.synthetic_merge(h4)
        self.fixture.checkout_detached(synthetic)
        _, synthetic_environment = self.fixture.pr_context(h4, synthetic)
        completed, payload = self.fixture.run_json(env=synthetic_environment)
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertEqual(
            payload["evidence_scope"],
            "GITHUB_H3_SUCCESSOR_PR_SYNTHETIC_CONTEXT_CONSISTENCY",
        )
        self.fixture.checkout_branch("main", synthetic)
        completed, payload = self.fixture.run_json()
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertEqual(
            payload["evidence_scope"],
            "LOCAL_H3_SUCCESSOR_MAIN_ARTIFACT_ONLY",
        )
        self.fixture.checkout_detached(synthetic)
        _, main_environment = self.fixture.main_context(synthetic)
        completed, payload = self.fixture.run_json(env=main_environment)
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertEqual(
            payload["evidence_scope"],
            "GITHUB_H3_SUCCESSOR_MAIN_CONTEXT_CONSISTENCY",
        )

    def test_v3_h5_siblings_merges_and_outside_delta_reject(self) -> None:
        progression = self.fixture.progression()
        h4 = self.fixture.make_h4()
        h4_tree = self.fixture.rev(f"{h4}^{{tree}}")
        extra_blob = self.fixture.hash_blob(b"TEST_FIXTURE_ONLY outside\n")
        outside_tree = self.fixture.variant_tree(
            h4,
            (("TEST_FIXTURE_ONLY-outside", "100644", extra_blob),),
        )
        cases = {
            "h5": self.fixture.raw_commit_tree(h4_tree, h4),
            "metadata-distinct-sibling": self.fixture.raw_commit_tree(
                h4_tree, self.fixture.H3
            ),
            "merge-child": self.fixture.raw_commit_tree(
                h4_tree, self.fixture.H3, self.fixture.C
            ),
            "outside-delta": self.fixture.raw_commit_tree(
                outside_tree, self.fixture.H3
            ),
        }
        for name, candidate in cases.items():
            with self.subTest(name=name):
                self.assertEqual(
                    release_state_support.security_progression_status(
                        self.fixture.root, candidate, progression
                    ),
                    "INVALID",
                )

    def test_v3_selection_is_atomic_and_rejects_mixed_material(self) -> None:
        state, contract, _ = self.fixture.documents()
        original_state = state.read_text(encoding="utf-8")
        original_contract = contract.read_text(encoding="utf-8")
        state.write_text(
            re.sub(
                r"(?m)^SECURITY_TOOLCHAIN_PROGRESSION_SCHEMA: .*\n",
                "",
                original_state,
                count=1,
            ),
            encoding="utf-8",
        )
        mode, _, detail = self.fixture.select_progression()
        self.assertEqual(mode, "v3-invalid")
        self.assertIn("selection schema", detail or "")
        state.write_text(original_state, encoding="utf-8")
        contract.write_text(
            original_contract
            + "\nSCHEMA: coldkeep-v1-security-toolchain-progression-expectations/v2\n",
            encoding="utf-8",
        )
        mode, _, detail = self.fixture.select_progression()
        self.assertEqual(mode, "v3-invalid")
        self.assertIn("other-version material", detail or "")


class _V4ProgressionFixture(_V3ProgressionFixture):
    """Fixture-owned H4 anchor and one metadata-bound nine-path H5 child."""

    def __init__(self) -> None:
        super().__init__()
        self.H4 = super().make_h4()
        self.H4_TREE = self.rev(f"{self.H4}^{{tree}}")
        source = SCRIPT.parents[1]
        for relative in self.PATHS[:3]:
            content = run_process(
                [
                    resolved_executable("git"), "-C", str(source), "show",
                    f"{self.PUBLISHED_H5_SOURCE}:{relative}",
                ],
                check=True,
            ).stdout
            (self.root / relative).write_text(content, encoding="utf-8")
        self.sync_terminal_identities()
        self.sync_progression_identities(
            {
                "BASE_C": self.C,
                "BASE_C_TREE": self.C_TREE,
                "HISTORICAL_H1": self.H1,
                "HISTORICAL_H1_PARENT": self.C,
                "HISTORICAL_H1_TREE": self.H1_TREE,
                "PUBLISHED_H2": self.H2,
                "PUBLISHED_H2_PARENT": self.H1,
                "PUBLISHED_H2_TREE": self.H2_TREE,
                "PUBLISHED_H3": self.H3,
                "PUBLISHED_H3_PARENT": self.H2,
                "PUBLISHED_H3_TREE": self.H3_TREE,
                "PUBLISHED_H4": self.H4,
                "PUBLISHED_H4_PARENT": self.H3,
                "PUBLISHED_H4_TREE": self.H4_TREE,
                "SUCCESSOR_PARENT": self.H4,
            }
        )
        self.sync_digest()

    def metadata_commit_tree(
        self,
        tree: str,
        parent: str,
        *,
        date: str | None = None,
        subject: str | None = None,
    ) -> str:
        progression = self.progression()
        commit_date = date or progression.values["SUCCESSOR_AUTHOR_DATE"]
        environment = os.environ.copy()
        environment.update(
            {
                "GIT_AUTHOR_NAME": progression.values["SUCCESSOR_AUTHOR_NAME"],
                "GIT_AUTHOR_EMAIL": progression.values[
                    "SUCCESSOR_AUTHOR_EMAIL"
                ],
                "GIT_AUTHOR_DATE": commit_date,
                "GIT_COMMITTER_NAME": progression.values[
                    "SUCCESSOR_COMMITTER_NAME"
                ],
                "GIT_COMMITTER_EMAIL": progression.values[
                    "SUCCESSOR_COMMITTER_EMAIL"
                ],
                "GIT_COMMITTER_DATE": commit_date,
            }
        )
        return run_process(
            [
                resolved_executable("git"), *FIXTURE_GIT_CONFIG,
                "-C", str(self.root), "commit-tree", tree, "-p", parent,
                "-m", subject or progression.values["SUCCESSOR_SUBJECT"],
            ],
            env=environment,
            check=True,
        ).stdout.strip()

    def make_h5(self) -> str:
        dirty = set(self.PATHS[:3])
        for relative in self.PATHS:
            if relative in dirty:
                continue
            target = self.root / relative
            marker = (
                "# TEST_FIXTURE_ONLY v4 delta\n"
                if target.suffix == ".py"
                else "<!-- TEST_FIXTURE_ONLY v4 delta -->\n"
            )
            with target.open("a", encoding="utf-8") as stream:
                stream.write(marker)
        git(self.root, "add", *self.PATHS)
        progression = self.progression()
        environment = os.environ.copy()
        environment.update(
            {
                "GIT_AUTHOR_NAME": progression.values["SUCCESSOR_AUTHOR_NAME"],
                "GIT_AUTHOR_EMAIL": progression.values[
                    "SUCCESSOR_AUTHOR_EMAIL"
                ],
                "GIT_AUTHOR_DATE": progression.values["SUCCESSOR_AUTHOR_DATE"],
                "GIT_COMMITTER_NAME": progression.values[
                    "SUCCESSOR_COMMITTER_NAME"
                ],
                "GIT_COMMITTER_EMAIL": progression.values[
                    "SUCCESSOR_COMMITTER_EMAIL"
                ],
                "GIT_COMMITTER_DATE": progression.values[
                    "SUCCESSOR_COMMITTER_DATE"
                ],
            }
        )
        run_process(
            [
                resolved_executable("git"), *FIXTURE_GIT_CONFIG,
                "-C", str(self.root), "commit", "--quiet", "-m",
                progression.values["SUCCESSOR_SUBJECT"],
            ],
            env=environment,
            check=True,
        )
        h5 = self.rev("HEAD")
        changed = run_process(
            [
                resolved_executable("git"), *FIXTURE_GIT_CONFIG,
                "-C", str(self.root), "diff-tree", "--no-commit-id",
                "--name-status", "-r", self.H4, h5,
            ],
            check=True,
        ).stdout.splitlines()
        if changed != [f"M\t{relative}" for relative in self.PATHS]:
            raise AssertionError(f"fixture H5 delta disagrees: {changed!r}")
        return h5

    def push_context(self, h5: str) -> tuple[Path, dict[str, str]]:
        event, environment = super().push_context(h5)
        body = json.loads(event.read_text(encoding="utf-8"))
        body["before"] = self.H4
        event.write_text(json.dumps(body), encoding="utf-8")
        return event, environment


class TerminalSerializationV4ContractTests(unittest.TestCase):
    """Bound authorized H4-to-H5 topology, delta, identity, and contexts."""

    def setUp(self) -> None:
        self.fixture = _V4ProgressionFixture()
        self.addCleanup(self.fixture.close)

    def test_v4_exact_h4_and_h5_statuses_and_local_scope(self) -> None:
        progression = self.fixture.progression()
        self.assertEqual(
            release_state_support.security_progression_status(
                self.fixture.root, self.fixture.H4, progression
            ),
            "PUBLISHED_H4_STRUCTURAL_ONLY_ORIGINAL_QUALITY_FAILED",
        )
        h5 = self.fixture.make_h5()
        self.assertEqual(self.fixture.rev(f"{h5}^"), self.fixture.H4)
        self.assertTrue(
            release_state_support.progressed_h2_artifact(
                self.fixture.root, h5, progression
            )
        )
        completed, payload = self.fixture.run_json()
        self.assertEqual(
            completed.returncode, 0, completed.stdout + completed.stderr
        )
        self.assertEqual(
            payload["security_progression"], "H4_SUCCESSOR_STRUCTURAL_ONLY"
        )
        self.assertEqual(
            payload["evidence_scope"], "LOCAL_H4_SUCCESSOR_CANDIDATE"
        )

    def test_v4_timestamp_renderings_preserve_instant_and_offset(self) -> None:
        expected = (1792231826, "+0000")
        self.assertEqual(
            release_state_support._git_timestamp_identity(
                "2026-10-17T10:10:26+00:00"
            ),
            expected,
        )
        self.assertEqual(
            release_state_support._git_timestamp_identity(
                "2026-10-17T10:10:26Z"
            ),
            expected,
        )
        self.assertEqual(
            release_state_support._git_timestamp_identity(
                "2026-10-17T12:10:26+02:00"
            ),
            (expected[0], "+0200"),
        )
        self.assertIsNone(
            release_state_support._git_timestamp_identity("invalid")
        )

    def test_v4_push_pr_and_future_main_contexts(self) -> None:
        h5 = self.fixture.make_h5()
        _, push_environment = self.fixture.push_context(h5)
        completed, payload = self.fixture.run_json(env=push_environment)
        self.assertEqual(
            completed.returncode, 0, completed.stdout + completed.stderr
        )
        self.assertEqual(
            payload["evidence_scope"],
            "GITHUB_H4_SUCCESSOR_PUSH_CONTEXT_CONSISTENCY",
        )
        _, direct_environment = self.fixture.pr_context(h5, h5)
        completed, payload = self.fixture.run_json(env=direct_environment)
        self.assertEqual(
            completed.returncode, 0, completed.stdout + completed.stderr
        )
        self.assertEqual(
            payload["evidence_scope"],
            "GITHUB_H4_SUCCESSOR_PR_DIRECT_CONTEXT_CONSISTENCY",
        )
        synthetic = self.fixture.synthetic_merge(h5)
        self.fixture.checkout_detached(synthetic)
        _, synthetic_environment = self.fixture.pr_context(h5, synthetic)
        completed, payload = self.fixture.run_json(env=synthetic_environment)
        self.assertEqual(
            completed.returncode, 0, completed.stdout + completed.stderr
        )
        self.assertEqual(
            payload["evidence_scope"],
            "GITHUB_H4_SUCCESSOR_PR_SYNTHETIC_CONTEXT_CONSISTENCY",
        )
        self.fixture.checkout_branch("main", synthetic)
        completed, payload = self.fixture.run_json()
        self.assertEqual(
            completed.returncode, 0, completed.stdout + completed.stderr
        )
        self.assertEqual(
            payload["evidence_scope"],
            "LOCAL_H4_SUCCESSOR_MAIN_ARTIFACT_ONLY",
        )
        self.fixture.checkout_detached(synthetic)
        _, main_environment = self.fixture.main_context(synthetic)
        completed, payload = self.fixture.run_json(env=main_environment)
        self.assertEqual(
            completed.returncode, 0, completed.stdout + completed.stderr
        )
        self.assertEqual(
            payload["evidence_scope"],
            "GITHUB_H4_SUCCESSOR_MAIN_CONTEXT_CONSISTENCY",
        )

    def test_v4_h6_siblings_merges_identity_and_outside_delta_reject(
        self,
    ) -> None:
        progression = self.fixture.progression()
        h5 = self.fixture.make_h5()
        h5_tree = self.fixture.rev(f"{h5}^{{tree}}")
        outside_blob = self.fixture.hash_blob(b"TEST_FIXTURE_ONLY outside\n")
        outside_tree = self.fixture.variant_tree(
            h5,
            (("TEST_FIXTURE_ONLY-outside", "100644", outside_blob),),
        )
        cases = {
            "h6": self.fixture.raw_commit_tree(h5_tree, h5),
            "metadata-distinct-sibling": self.fixture.raw_commit_tree(
                h5_tree, self.fixture.H4
            ),
            "merge-child": self.fixture.raw_commit_tree(
                h5_tree, self.fixture.H4, self.fixture.C
            ),
            "outside-delta": self.fixture.metadata_commit_tree(
                outside_tree, self.fixture.H4
            ),
            "same-instant-wrong-offset": self.fixture.metadata_commit_tree(
                h5_tree, self.fixture.H4, date="2026-10-17T12:10:26+02:00"
            ),
            "wrong-instant": self.fixture.metadata_commit_tree(
                h5_tree, self.fixture.H4, date="2026-10-17T10:10:27+00:00"
            ),
            "wrong-subject": self.fixture.metadata_commit_tree(
                h5_tree,
                self.fixture.H4,
                subject="TEST_FIXTURE_ONLY wrong subject",
            ),
        }
        for name, candidate in cases.items():
            with self.subTest(name=name):
                self.assertEqual(
                    release_state_support.security_progression_status(
                        self.fixture.root, candidate, progression
                    ),
                    "INVALID",
                )

    def test_v4_selection_is_atomic_and_rejects_mixed_material(self) -> None:
        state, contract, _ = self.fixture.documents()
        original_state = state.read_text(encoding="utf-8")
        original_contract = contract.read_text(encoding="utf-8")
        state.write_text(
            re.sub(
                r"(?m)^SECURITY_TOOLCHAIN_PROGRESSION_SCHEMA: .*\n",
                "",
                original_state,
                count=1,
            ),
            encoding="utf-8",
        )
        mode, _, detail = self.fixture.select_progression()
        self.assertEqual(mode, "v4-invalid")
        self.assertIn("selection schema", detail or "")
        state.write_text(original_state, encoding="utf-8")
        contract.write_text(
            original_contract
            + "\nSCHEMA: coldkeep-v1-security-toolchain-progression-"
            + "expectations/v3\n",
            encoding="utf-8",
        )
        mode, _, detail = self.fixture.select_progression()
        self.assertEqual(mode, "v4-invalid")
        self.assertIn("other-version material", detail or "")


class _V5ProgressionFixture(_V4ProgressionFixture):
    """Fixture-owned H5 anchor and one metadata-bound nine-path H6 child."""

    def __init__(self) -> None:
        super().__init__()
        self.H5 = super().make_h5()
        self.H5_TREE = self.rev(f"{self.H5}^{{tree}}")
        source = SCRIPT.parents[1]
        for relative in self.PATHS[:3]:
            shutil.copy2(source / relative, self.root / relative)
        self.sync_terminal_identities()
        self.sync_progression_identities(
            {
                "BASE_C": self.C,
                "BASE_C_TREE": self.C_TREE,
                "HISTORICAL_H1": self.H1,
                "HISTORICAL_H1_PARENT": self.C,
                "HISTORICAL_H1_TREE": self.H1_TREE,
                "PUBLISHED_H2": self.H2,
                "PUBLISHED_H2_PARENT": self.H1,
                "PUBLISHED_H2_TREE": self.H2_TREE,
                "PUBLISHED_H3": self.H3,
                "PUBLISHED_H3_PARENT": self.H2,
                "PUBLISHED_H3_TREE": self.H3_TREE,
                "PUBLISHED_H4": self.H4,
                "PUBLISHED_H4_PARENT": self.H3,
                "PUBLISHED_H4_TREE": self.H4_TREE,
                "PUBLISHED_H5": self.H5,
                "PUBLISHED_H5_PARENT": self.H4,
                "PUBLISHED_H5_TREE": self.H5_TREE,
                "SUCCESSOR_PARENT": self.H5,
            }
        )
        self.sync_digest()

    def make_h6(self) -> str:
        dirty = set(self.PATHS[:3])
        for relative in self.PATHS:
            if relative in dirty:
                continue
            target = self.root / relative
            marker = (
                "# TEST_FIXTURE_ONLY v5 delta\n"
                if target.suffix == ".py"
                else "<!-- TEST_FIXTURE_ONLY v5 delta -->\n"
            )
            with target.open("a", encoding="utf-8") as stream:
                stream.write(marker)
        git(self.root, "add", *self.PATHS)
        progression = self.progression()
        environment = os.environ.copy()
        environment.update(
            {
                "GIT_AUTHOR_NAME": progression.values["SUCCESSOR_AUTHOR_NAME"],
                "GIT_AUTHOR_EMAIL": progression.values[
                    "SUCCESSOR_AUTHOR_EMAIL"
                ],
                "GIT_AUTHOR_DATE": progression.values["SUCCESSOR_AUTHOR_DATE"],
                "GIT_COMMITTER_NAME": progression.values[
                    "SUCCESSOR_COMMITTER_NAME"
                ],
                "GIT_COMMITTER_EMAIL": progression.values[
                    "SUCCESSOR_COMMITTER_EMAIL"
                ],
                "GIT_COMMITTER_DATE": progression.values[
                    "SUCCESSOR_COMMITTER_DATE"
                ],
            }
        )
        run_process(
            [
                resolved_executable("git"), *FIXTURE_GIT_CONFIG,
                "-C", str(self.root), "commit", "--quiet", "-m",
                progression.values["SUCCESSOR_SUBJECT"],
            ],
            env=environment,
            check=True,
        )
        h6 = self.rev("HEAD")
        changed = run_process(
            [
                resolved_executable("git"), *FIXTURE_GIT_CONFIG,
                "-C", str(self.root), "diff-tree", "--no-commit-id",
                "--name-status", "-r", self.H5, h6,
            ],
            check=True,
        ).stdout.splitlines()
        if changed != [f"M\t{relative}" for relative in self.PATHS]:
            raise AssertionError(f"fixture H6 delta disagrees: {changed!r}")
        return h6

    def push_context(self, h6: str) -> tuple[Path, dict[str, str]]:
        event, environment = super().push_context(h6)
        body = json.loads(event.read_text(encoding="utf-8"))
        body["before"] = self.H5
        event.write_text(json.dumps(body), encoding="utf-8")
        return event, environment


class TerminalSerializationV5ContractTests(unittest.TestCase):
    """Bound authorized H5-to-H6 topology, delta, identity, and contexts."""

    def setUp(self) -> None:
        self.fixture = _V5ProgressionFixture()
        self.addCleanup(self.fixture.close)

    def test_closed_progression_version_preserves_v1_v5_precedence(
        self,
    ) -> None:
        for version in (2, 3, 4, 5):
            with self.subTest(version=version):
                schema = (
                    "coldkeep-v1-security-toolchain-progression-selection/"
                    f"v{version}"
                )
                self.assertEqual(
                    validate_release_state._closed_progression_version(
                        [schema], {version}, version
                    ),
                    version,
                )
        self.assertEqual(
            validate_release_state._closed_progression_version([], {1}, 1),
            0,
        )
        unknown = "coldkeep-v1-security-toolchain-progression-selection/v99"
        self.assertEqual(
            validate_release_state._closed_progression_version(
                [unknown], set(), None
            ),
            2,
        )
        self.assertIsNotNone(
            validate_release_state._progression_selector_error(
                2, [unknown], [self.fixture.PATHS[2]], set()
            )
        )
        self.assertEqual(
            validate_release_state._closed_progression_version(
                [
                    "coldkeep-v1-security-toolchain-progression-selection/v4",
                    "coldkeep-v1-security-toolchain-progression-selection/v5",
                ],
                {4, 5},
                4,
            ),
            5,
        )

    def test_v5_exact_h5_and_h6_statuses_and_local_scope(self) -> None:
        progression = self.fixture.progression()
        self.assertEqual(
            release_state_support.security_progression_status(
                self.fixture.root, self.fixture.H5, progression
            ),
            "PUBLISHED_H5_STRUCTURAL_ONLY_CI_CODEQL_PASSED_CODACY_1_NEW",
        )
        h6 = self.fixture.make_h6()
        self.assertEqual(self.fixture.rev(f"{h6}^"), self.fixture.H5)
        self.assertTrue(
            release_state_support.progressed_h2_artifact(
                self.fixture.root, h6, progression
            )
        )
        completed, payload = self.fixture.run_json()
        self.assertEqual(
            completed.returncode, 0, completed.stdout + completed.stderr
        )
        self.assertEqual(
            payload["security_progression"], "H5_SUCCESSOR_STRUCTURAL_ONLY"
        )
        self.assertEqual(
            payload["evidence_scope"], "LOCAL_H5_SUCCESSOR_CANDIDATE"
        )

    def test_v5_timestamp_renderings_preserve_instant_and_offset(self) -> None:
        expected = (1792236808, "+0000")
        self.assertEqual(
            release_state_support._git_timestamp_identity(
                "2026-10-17T11:33:28+00:00"
            ),
            expected,
        )
        self.assertEqual(
            release_state_support._git_timestamp_identity(
                "2026-10-17T11:33:28Z"
            ),
            expected,
        )
        self.assertEqual(
            release_state_support._git_timestamp_identity(
                "2026-10-17T13:33:28+02:00"
            ),
            (expected[0], "+0200"),
        )

    def test_v5_push_pr_and_future_main_contexts(self) -> None:
        h6 = self.fixture.make_h6()
        _, push_environment = self.fixture.push_context(h6)
        completed, payload = self.fixture.run_json(env=push_environment)
        self.assertEqual(
            completed.returncode, 0, completed.stdout + completed.stderr
        )
        self.assertEqual(
            payload["evidence_scope"],
            "GITHUB_H5_SUCCESSOR_PUSH_CONTEXT_CONSISTENCY",
        )
        _, direct_environment = self.fixture.pr_context(h6, h6)
        completed, payload = self.fixture.run_json(env=direct_environment)
        self.assertEqual(
            completed.returncode, 0, completed.stdout + completed.stderr
        )
        self.assertEqual(
            payload["evidence_scope"],
            "GITHUB_H5_SUCCESSOR_PR_DIRECT_CONTEXT_CONSISTENCY",
        )
        synthetic = self.fixture.synthetic_merge(h6)
        self.fixture.checkout_detached(synthetic)
        _, synthetic_environment = self.fixture.pr_context(h6, synthetic)
        completed, payload = self.fixture.run_json(env=synthetic_environment)
        self.assertEqual(
            completed.returncode, 0, completed.stdout + completed.stderr
        )
        self.assertEqual(
            payload["evidence_scope"],
            "GITHUB_H5_SUCCESSOR_PR_SYNTHETIC_CONTEXT_CONSISTENCY",
        )
        self.fixture.checkout_branch("main", synthetic)
        completed, payload = self.fixture.run_json()
        self.assertEqual(
            completed.returncode, 0, completed.stdout + completed.stderr
        )
        self.assertEqual(
            payload["evidence_scope"], "LOCAL_H5_SUCCESSOR_MAIN_ARTIFACT_ONLY"
        )
        self.fixture.checkout_detached(synthetic)
        _, main_environment = self.fixture.main_context(synthetic)
        completed, payload = self.fixture.run_json(env=main_environment)
        self.assertEqual(
            completed.returncode, 0, completed.stdout + completed.stderr
        )
        self.assertEqual(
            payload["evidence_scope"],
            "GITHUB_H5_SUCCESSOR_MAIN_CONTEXT_CONSISTENCY",
        )

    def test_v5_h7_siblings_merges_identity_types_and_outside_delta_reject(
        self,
    ) -> None:
        progression = self.fixture.progression()
        h6 = self.fixture.make_h6()
        h6_tree = self.fixture.rev(f"{h6}^{{tree}}")
        outside_blob = self.fixture.hash_blob(b"TEST_FIXTURE_ONLY outside\n")
        outside_tree = self.fixture.variant_tree(
            h6,
            (("TEST_FIXTURE_ONLY-outside", "100644", outside_blob),),
        )
        wrong_type_tree = self.fixture.variant_tree(
            h6,
            ((self.fixture.PATHS[3], "120000", outside_blob),),
        )
        cases = {
            "h7": self.fixture.raw_commit_tree(h6_tree, h6),
            "metadata-distinct-sibling": self.fixture.metadata_commit_tree(
                h6_tree, self.fixture.H4
            ),
            "merge-child": self.fixture.raw_commit_tree(
                h6_tree, self.fixture.H5, self.fixture.C
            ),
            "outside-delta": self.fixture.metadata_commit_tree(
                outside_tree, self.fixture.H5
            ),
            "wrong-source-type": self.fixture.metadata_commit_tree(
                wrong_type_tree, self.fixture.H5
            ),
            "same-instant-wrong-offset": self.fixture.metadata_commit_tree(
                h6_tree, self.fixture.H5, date="2026-10-17T13:33:28+02:00"
            ),
            "wrong-instant": self.fixture.metadata_commit_tree(
                h6_tree, self.fixture.H5, date="2026-10-17T11:33:29+00:00"
            ),
            "wrong-subject": self.fixture.metadata_commit_tree(
                h6_tree,
                self.fixture.H5,
                subject="TEST_FIXTURE_ONLY wrong subject",
            ),
        }
        for name, candidate in cases.items():
            with self.subTest(name=name):
                self.assertEqual(
                    release_state_support.security_progression_status(
                        self.fixture.root, candidate, progression
                    ),
                    "INVALID",
                )

    def test_v5_selection_is_atomic_and_rejects_mixed_material(self) -> None:
        state, contract, _ = self.fixture.documents()
        original_state = state.read_text(encoding="utf-8")
        original_contract = contract.read_text(encoding="utf-8")
        state.write_text(
            re.sub(
                r"(?m)^SECURITY_TOOLCHAIN_PROGRESSION_SCHEMA: .*\n",
                "",
                original_state,
                count=1,
            ),
            encoding="utf-8",
        )
        mode, _, detail = self.fixture.select_progression()
        self.assertEqual(mode, "v5-invalid")
        self.assertIn("selection schema", detail or "")
        state.write_text(original_state, encoding="utf-8")
        contract.write_text(
            original_contract
            + "\nSCHEMA: coldkeep-v1-security-toolchain-progression-"
            + "expectations/v4\n",
            encoding="utf-8",
        )
        mode, _, detail = self.fixture.select_progression()
        self.assertEqual(mode, "v5-invalid")
        self.assertIn("other-version material", detail or "")


class TerminalSerializationV2ContractTests(unittest.TestCase):
    """Complete direct matrix for the closed H2-successor contract."""

    def setUp(self) -> None:
        self.fixture = _V2ProgressionFixture()
        self.addCleanup(self.fixture.close)

    def assert_human_json_semantic_parity(
        self,
        human: ProcessResult,
        payload: dict[str, object],
    ) -> dict[str, object]:
        parsed = parse_release_state_human_output(human.stdout)
        expected_status = "OK" if payload["status"] == "ok" else "FAILED"
        self.assertEqual(parsed["status"], expected_status)
        fields = parsed["fields"]
        self.assertIsInstance(fields, dict)
        for key in (
            "state",
            "active_version",
            "security_progression",
            "security_result",
            "progression_contract_schema",
            "authorization_status",
            "certification_status",
            "evidence_scope",
        ):
            with self.subTest(human_json_field=key):
                self.assertIsInstance(payload[key], str)
                self.assertEqual(fields.get(key), payload[key])
        violations = payload["violations"]
        self.assertIsInstance(violations, list)
        self.assertEqual(fields.get("violations"), str(len(violations)))
        self.assertEqual(
            parsed["rules"],
            [violation["rule"] for violation in violations],
        )
        return parsed

    def assert_repository_id_matrix_rejected(
        self,
        *,
        label: str,
        state: str,
        event: Path,
        environment: dict[str, str],
    ) -> None:
        original = event.read_text(encoding="utf-8")
        completed, payload = self.fixture.run_json(state, env=environment)
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertEqual(payload["security_progression"], "H2_SUCCESSOR_STRUCTURAL_ONLY")

        environment_values: tuple[tuple[str, str | None], ...] = (
            ("wrong", "1158711821"),
            ("missing", None),
            ("boolean-like", "true"),
            ("zero", "0"),
            ("signed", "-1158711822"),
            ("whitespace", " 1158711822 "),
            ("partial", "115871182"),
        )
        for name, value in environment_values:
            with self.subTest(context=label, owner="environment", case=name):
                event.write_text(original, encoding="utf-8")
                mutated = dict(environment)
                if value is None:
                    mutated.pop("GITHUB_REPOSITORY_ID")
                else:
                    mutated["GITHUB_REPOSITORY_ID"] = value
                failed, result = self.fixture.run_json(state, env=mutated)
                self.assertEqual(failed.returncode, 1, failed.stdout + failed.stderr)
                self.assertIn(
                    "CKRS016", [item["rule"] for item in result["violations"]]
                )
                self.assertEqual(
                    result["security_progression"], "H2_SUCCESSOR_STRUCTURAL_ONLY"
                )

        payload_values: tuple[tuple[str, object, bool], ...] = (
            ("wrong", 1158711821, False),
            ("missing", None, True),
            ("boolean", True, False),
            ("zero", 0, False),
            ("signed", -1158711822, False),
            ("whitespace", " 1158711822 ", False),
            ("partial", 115871182, False),
        )
        for name, value, remove in payload_values:
            with self.subTest(context=label, owner="payload", case=name):
                body = json.loads(original)
                if remove:
                    body["repository"].pop("id")
                else:
                    body["repository"]["id"] = value
                event.write_text(json.dumps(body), encoding="utf-8")
                failed, result = self.fixture.run_json(state, env=environment)
                self.assertEqual(failed.returncode, 1, failed.stdout + failed.stderr)
                self.assertIn(
                    "CKRS016", [item["rule"] for item in result["violations"]]
                )
                self.assertEqual(
                    result["security_progression"], "H2_SUCCESSOR_STRUCTURAL_ONLY"
                )
        event.write_text(original, encoding="utf-8")

    def test_v2_historical_h1_and_exact_h2_statuses(self) -> None:
        progression = self.fixture.progression()
        self.assertEqual(release_state_support.security_progression_status(self.fixture.root, self.fixture.H1, progression), "HISTORICAL_H1_UNPATCHED")
        self.assertEqual(release_state_support.security_progression_status(self.fixture.root, self.fixture.H2, progression), "PUBLISHED_H2_STRUCTURAL_ONLY_ORIGINAL_CI_FAILED")
        h3 = self.fixture.make_h3()
        completed, payload = self.fixture.run_json()
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(
            payload["evidence_scope"], "LOCAL_H2_SUCCESSOR_CANDIDATE"
        )

        git(self.fixture.root, "tag", "-d", "v1.13.16")
        failed, failed_payload = self.fixture.run_json(
            "post-release-terminal-serialization-candidate"
        )
        self.assertEqual(failed.returncode, 1, failed.stdout + failed.stderr)
        predecessor = [
            item
            for item in failed_payload["violations"]
            if item["rule"] == "CKRS010"
        ]
        self.assertEqual(len(predecessor), 1, failed.stdout)
        self.assertIn("PREDECESSOR_REF", predecessor[0]["message"])
        git(
            self.fixture.root,
            "update-ref",
            "refs/tags/v1.13.16",
            self.fixture.PREDECESSOR_TAG,
        )

        git(self.fixture.root, "tag", "-d", "v1.13.17")
        git(
            self.fixture.root,
            "update-ref",
            "refs/tags/v1.13.17",
            self.fixture.M17,
        )
        failed, failed_payload = self.fixture.run_json(
            "post-release-terminal-serialization-candidate"
        )
        self.assertEqual(failed.returncode, 1, failed.stdout + failed.stderr)
        publication = [
            item
            for item in failed_payload["violations"]
            if item["rule"] == "CKRS017"
        ]
        self.assertEqual(len(publication), 1, failed.stdout)
        self.assertIn("lightweight", publication[0]["message"])
        git(
            self.fixture.root,
            "update-ref",
            "refs/tags/v1.13.17",
            self.fixture.A17,
        )

        terminal = self.fixture.root / self.fixture.TERMINAL_PATH
        original_terminal = terminal.read_bytes()
        self.fixture.set_fields(
            self.fixture.TERMINAL_PATH, {"ACCEPTED_C": self.fixture.H1}
        )
        failed, failed_payload = self.fixture.run_json(
            "post-release-terminal-serialization-candidate"
        )
        self.assertEqual(failed.returncode, 1, failed.stdout + failed.stderr)
        terminal_rules = [
            item["rule"] for item in failed_payload["violations"]
        ]
        self.assertIn("CKRS019", terminal_rules)
        self.assertTrue(set(terminal_rules).issubset({"CKRS016", "CKRS019"}))
        terminal.write_bytes(original_terminal)

        restored, restored_payload = self.fixture.run_json()
        self.assertEqual(
            restored.returncode, 0, restored.stdout + restored.stderr
        )
        self.assertEqual(restored_payload["status"], "ok")
        self.assertEqual(self.fixture.rev("HEAD"), h3)

    def test_v2_h2_successor_topology_and_delta_boundaries(self) -> None:
        progression = self.fixture.progression()
        h3 = self.fixture.make_h3()
        self.assertTrue(release_state_support.progressed_h2_artifact(self.fixture.root, h3, progression))
        self.assertEqual(release_state_support.security_progression_status(self.fixture.root, h3, progression), "H2_SUCCESSOR_STRUCTURAL_ONLY")
        path = self.fixture.PATHS[4]
        h2_blob = self.fixture.rev(f"{self.fixture.H2}:{path}")
        h3_blob = self.fixture.rev(f"{h3}:{path}")
        extra_blob = self.fixture.hash_blob(b"TEST_FIXTURE_ONLY v2 extra\n")
        symlink_blob = self.fixture.hash_blob(b"TEST_FIXTURE_ONLY-target")
        variants = {
            "omitted": self.fixture.variant_tree(h3, ((path, "100644", h2_blob),)),
            "extra": self.fixture.variant_tree(
                h3, (("TEST_FIXTURE_ONLY-v2-extra", "100644", extra_blob),)
            ),
            "copy": self.fixture.variant_tree(
                h3, (("TEST_FIXTURE_ONLY-v2-copy.py", "100644", h3_blob),)
            ),
            "delete": self.fixture.variant_tree(h3, ((path, None, None),)),
            "mode": self.fixture.variant_tree(h3, ((path, "100755", h3_blob),)),
            "rename": self.fixture.variant_tree(
                h3,
                (
                    (path, None, None),
                    ("scripts/TEST_FIXTURE_ONLY-renamed.py", "100644", h3_blob),
                ),
            ),
            "symlink-type": self.fixture.variant_tree(
                h3, ((path, "120000", symlink_blob),)
            ),
            "gitlink-type": self.fixture.variant_tree(
                h3, ((path, "160000", self.fixture.C),)
            ),
        }
        for name, tree in variants.items():
            with self.subTest(structural_delta=name):
                candidate = self.fixture.raw_commit_tree(tree, self.fixture.H2)
                self.assertFalse(
                    release_state_support.progressed_h2_artifact(
                        self.fixture.root, candidate, progression
                    )
                )
                self.assertEqual(
                    release_state_support.security_progression_status(
                        self.fixture.root, candidate, progression
                    ),
                    "INVALID",
                )

        state, contract, reference = self.fixture.documents()
        del state
        original_contract = contract.read_bytes()
        original_reference = reference.read_bytes()
        rows = [
            line
            for line in original_reference.splitlines(keepends=True)
            if re.fullmatch(rb"M 100(?:644|755) \S+\n", line)
        ]
        self.assertEqual(len(rows), 9)
        grammar_variants = {
            "status": original_reference.replace(rows[0], b"A" + rows[0][1:], 1),
            "path-order": original_reference.replace(
                rows[0] + rows[1], rows[1] + rows[0], 1
            ),
        }
        for name, content in grammar_variants.items():
            with self.subTest(reference_delta=name):
                contract.write_bytes(original_contract)
                reference.write_bytes(content)
                self.fixture.sync_digest()
                loaded, detail = self.fixture.load_progression()
                self.assertIsNone(loaded)
                self.assertIn("security progression reference invalid:", detail or "")
        contract.write_bytes(original_contract)
        reference.write_bytes(original_reference)

    def test_v2_unapproved_siblings_extra_parents_and_descendants_reject(self) -> None:
        progression = self.fixture.progression()
        h3 = self.fixture.make_h3()
        valid_before, valid_before_payload = self.fixture.run_json(
            "post-release-terminal-serialization-candidate",
            copied_validator=True,
        )
        self.assertEqual(
            valid_before.returncode,
            0,
            valid_before.stdout + valid_before.stderr,
        )
        self.assertEqual(valid_before_payload["status"], "ok")
        self.assertEqual(
            valid_before_payload["evidence_scope"],
            "LOCAL_H2_SUCCESSOR_CANDIDATE",
        )
        h3_tree = self.fixture.rev(f"{h3}^{{tree}}")
        c_tree = self.fixture.rev(f"{self.fixture.C}^{{tree}}")
        path = self.fixture.PATHS[4]
        h2_blob = self.fixture.rev(f"{self.fixture.H2}:{path}")
        nonconforming_tree = self.fixture.variant_tree(
            h3, ((path, "100644", h2_blob),)
        )
        descendant = self.fixture.raw_commit_tree(h3_tree, h3)
        deeper = self.fixture.raw_commit_tree(h3_tree, descendant)
        structural_rejections = {
            "h1-based-child": self.fixture.raw_commit_tree(h3_tree, self.fixture.H1),
            "zero-parent": self.fixture.raw_commit_tree(h3_tree),
            "extra-parent": self.fixture.raw_commit_tree(
                h3_tree, self.fixture.H2, self.fixture.C
            ),
            "wrong-h2-tree": self.fixture.raw_commit_tree(c_tree, self.fixture.H2),
            "nonconforming-sibling": self.fixture.raw_commit_tree(
                nonconforming_tree, self.fixture.H2
            ),
            "descendant": descendant,
            "deeper-descendant": deeper,
            "merge-reversed": self.fixture.raw_commit_tree(
                h3_tree, h3, self.fixture.C
            ),
            "merge-extra-parent": self.fixture.raw_commit_tree(
                h3_tree, self.fixture.C, h3, self.fixture.H2
            ),
            "merge-wrong-second-parent": self.fixture.raw_commit_tree(
                h3_tree, self.fixture.C, self.fixture.H2
            ),
            "merge-wrong-tree": self.fixture.raw_commit_tree(
                c_tree, self.fixture.C, h3
            ),
        }
        for name, candidate in structural_rejections.items():
            with self.subTest(topology=name):
                self.assertEqual(
                    release_state_support.security_progression_status(
                        self.fixture.root, candidate, progression
                    ),
                    "INVALID",
                )

        equivalent_sibling = self.fixture.raw_commit_tree(
            h3_tree, self.fixture.H2
        )
        self.assertNotEqual(equivalent_sibling, h3)
        self.assertTrue(
            release_state_support.progressed_h2_artifact(
                self.fixture.root, equivalent_sibling, progression
            )
        )
        self.assertEqual(
            release_state_support.security_progression_status(
                self.fixture.root, equivalent_sibling, progression
            ),
            "H2_SUCCESSOR_STRUCTURAL_ONLY",
        )
        _, bound_environment = self.fixture.push_context(h3)
        self.assertFalse(
            release_state_support.strict_terminal_push_context(
                self.fixture.root,
                "1.13.17",
                equivalent_sibling,
                "",
                bound_environment,
                "franchoy/coldkeep",
                self.fixture.C,
                progression,
            )
        )

        values = dict(progression.values)
        values["PUBLISHED_H2"] = self.fixture.H1
        wrong_h2 = release_state_support.SecurityToolchainProgression(
            progression.expectations, values, progression.delta
        )
        self.assertFalse(
            release_state_support.progressed_h2_artifact(
                self.fixture.root, h3, wrong_h2
            )
        )

        _, contract, reference = self.fixture.documents()
        contract_original = contract.read_bytes()
        reference_original = reference.read_bytes()
        reference_start = (
            b"<!-- coldkeep-security-toolchain-progression-v2-reference:start -->"
        )
        reference_end = (
            b"<!-- coldkeep-security-toolchain-progression-v2-reference:end -->"
        )
        expectation_start = (
            b"<!-- coldkeep-security-toolchain-progression-v2-expectations:start -->"
        )
        expectation_end = (
            b"<!-- coldkeep-security-toolchain-progression-v2-expectations:end -->"
        )

        def replace_unique_marked_field(
            content: bytes,
            start_marker: bytes,
            end_marker: bytes,
            original_field: bytes,
            replacement_field: bytes,
        ) -> bytes:
            self.assertEqual(content.count(start_marker), 1)
            self.assertEqual(content.count(end_marker), 1)
            start = content.index(start_marker)
            end = content.index(end_marker, start) + len(end_marker)
            self.assertLess(start, end)
            marked = content[start:end]
            self.assertEqual(marked.count(original_field), 1)
            self.assertEqual(content.count(original_field), 1)
            replaced = marked.replace(original_field, replacement_field, 1)
            result = content[:start] + replaced + content[end:]
            self.assertEqual(result[:start], content[:start])
            self.assertEqual(result[end:], content[end:])
            return result

        original_tree_field = (
            f"PUBLISHED_H2_TREE: {self.fixture.H2_TREE}".encode()
        )
        replacement_tree_field = (
            f"PUBLISHED_H2_TREE: {self.fixture.C_TREE}".encode()
        )
        expected_reference = replace_unique_marked_field(
            reference_original,
            reference_start,
            reference_end,
            original_tree_field,
            replacement_tree_field,
        )
        original_expectation_field = (
            f"EXPECTED_PUBLISHED_H2_TREE: {self.fixture.H2_TREE}".encode()
        )
        replacement_expectation_field = (
            f"EXPECTED_PUBLISHED_H2_TREE: {self.fixture.C_TREE}".encode()
        )
        expected_contract_before_digest = replace_unique_marked_field(
            contract_original,
            expectation_start,
            expectation_end,
            original_expectation_field,
            replacement_expectation_field,
        )
        original_digest = hashlib.sha256(reference_original).hexdigest().encode()
        replacement_digest = hashlib.sha256(expected_reference).hexdigest().encode()
        expected_contract = replace_unique_marked_field(
            expected_contract_before_digest,
            expectation_start,
            expectation_end,
            b"EXPECTED_REFERENCE_SHA256: " + original_digest,
            b"EXPECTED_REFERENCE_SHA256: " + replacement_digest,
        )
        original_delta_rows = [
            line
            for line in reference_original.splitlines(keepends=True)
            if re.fullmatch(rb"M 100(?:644|755) \S+\n", line)
        ]
        expected_delta_rows = [
            line
            for line in expected_reference.splitlines(keepends=True)
            if re.fullmatch(rb"M 100(?:644|755) \S+\n", line)
        ]
        self.assertEqual(expected_delta_rows, original_delta_rows)
        self.assertEqual(len(expected_delta_rows), 9)
        self.assertNotIn(b"\r", expected_reference)
        expected_reference.decode("utf-8", "strict")
        expected_contract.decode("utf-8", "strict")
        graph_before = {
            "head": self.fixture.rev("HEAD"),
            "h3_parent": self.fixture.rev(f"{h3}^"),
            "h3_tree": self.fixture.rev(f"{h3}^{{tree}}"),
            "h2_tree": self.fixture.rev(f"{self.fixture.H2}^{{tree}}"),
            "c_tree": self.fixture.rev(f"{self.fixture.C}^{{tree}}"),
        }
        self.assertNotEqual(self.fixture.C_TREE, self.fixture.H2_TREE)
        self.assertEqual(self.fixture.object_type(self.fixture.C_TREE), "tree")
        try:
            self.fixture.sync_progression_identities(
                {"PUBLISHED_H2_TREE": self.fixture.C_TREE}
            )
            self.fixture.sync_digest()
            self.assertEqual(reference.read_bytes(), expected_reference)
            self.assertEqual(contract.read_bytes(), expected_contract)
            self.assertEqual(
                [
                    line
                    for line in reference.read_bytes().splitlines(keepends=True)
                    if re.fullmatch(rb"M 100(?:644|755) \S+\n", line)
                ],
                original_delta_rows,
            )
            self.assertEqual(
                {
                    "head": self.fixture.rev("HEAD"),
                    "h3_parent": self.fixture.rev(f"{h3}^"),
                    "h3_tree": self.fixture.rev(f"{h3}^{{tree}}"),
                    "h2_tree": self.fixture.rev(f"{self.fixture.H2}^{{tree}}"),
                    "c_tree": self.fixture.rev(f"{self.fixture.C}^{{tree}}"),
                },
                graph_before,
            )
            wrong_tree, wrong_tree_detail = self.fixture.load_progression()
            self.assertIsNone(wrong_tree)
            self.assertEqual(
                wrong_tree_detail,
                "security progression reference invalid: "
                "published H2 tree disagrees",
            )
            wrong_tree_result, wrong_tree_payload = self.fixture.run_json(
                "post-release-terminal-serialization-candidate",
                copied_validator=True,
            )
            self.assertEqual(
                wrong_tree_result.returncode,
                1,
                wrong_tree_result.stdout + wrong_tree_result.stderr,
            )
            self.assertEqual(wrong_tree_payload["status"], "error")
            self.assertEqual(wrong_tree_payload["security_progression"], "INVALID")
            self.assertEqual(
                wrong_tree_payload["progression_contract_schema"], "INVALID"
            )
            self.assertEqual(
                wrong_tree_payload["evidence_scope"],
                "UNSUPPORTED_OR_CONFLICTING_CONTEXT",
            )
            wrong_tree_violations = [
                violation
                for violation in wrong_tree_payload["violations"]
                if violation["rule"] == "CKRS020"
            ]
            self.assertEqual(len(wrong_tree_violations), 1)
            self.assertIn(
                "published H2 tree disagrees",
                wrong_tree_violations[0]["message"],
            )
        finally:
            contract.write_bytes(contract_original)
            reference.write_bytes(reference_original)

        self.assertEqual(contract.read_bytes(), contract_original)
        self.assertEqual(reference.read_bytes(), reference_original)
        self.assertEqual(
            {
                "head": self.fixture.rev("HEAD"),
                "h3_parent": self.fixture.rev(f"{h3}^"),
                "h3_tree": self.fixture.rev(f"{h3}^{{tree}}"),
                "h2_tree": self.fixture.rev(f"{self.fixture.H2}^{{tree}}"),
                "c_tree": self.fixture.rev(f"{self.fixture.C}^{{tree}}"),
            },
            graph_before,
        )

        restored_progression, restored_detail = self.fixture.load_progression()
        self.assertIsNotNone(restored_progression, restored_detail)
        restored, restored_payload = self.fixture.run_json(
            "post-release-terminal-serialization-candidate",
            copied_validator=True,
        )
        self.assertEqual(restored.returncode, 0, restored.stdout + restored.stderr)
        self.assertEqual(restored_payload["status"], "ok")
        self.assertEqual(
            restored_payload["evidence_scope"],
            "LOCAL_H2_SUCCESSOR_CANDIDATE",
        )
        self.assertNotEqual(
            release_state_support.commit_tree(
                self.fixture.root, self.fixture.H2
            ),
            self.fixture.C_TREE,
        )
        self.assertEqual(self.fixture.rev("HEAD"), h3)
        self.assertEqual(self.fixture.rev(f"{h3}^"), self.fixture.H2)
        self.assertEqual(self.fixture.rev(f"{h3}^{{tree}}"), h3_tree)

    def test_v2_invalid_grammar_with_coherent_digest_reaches_grammar_gate(self) -> None:
        _, contract, reference = self.fixture.documents()
        contract_original = contract.read_bytes()
        original = reference.read_bytes()

        def reject(
            name: str,
            content: bytes,
            expected_detail: str,
            *,
            synchronize_digest: bool = True,
        ) -> None:
            with self.subTest(grammar=name):
                contract.write_bytes(contract_original)
                reference.write_bytes(content)
                if synchronize_digest:
                    self.fixture.sync_digest()
                progression, detail = self.fixture.load_progression()
                self.assertIsNone(progression)
                self.assertIn(expected_detail, detail or "")

        lines = original.splitlines(keepends=True)
        base_index = next(
            index for index, line in enumerate(lines) if line.startswith(b"BASE_C: ")
        )
        tree_index = next(
            index
            for index, line in enumerate(lines)
            if line.startswith(b"BASE_C_TREE: ")
        )
        swapped_fields = list(lines)
        swapped_fields[base_index], swapped_fields[tree_index] = (
            swapped_fields[tree_index],
            swapped_fields[base_index],
        )
        duplicate_field = list(lines)
        duplicate_field[tree_index] = duplicate_field[base_index]
        missing_field = list(lines)
        missing_field.pop(tree_index)
        rows = [
            line
            for line in lines
            if re.fullmatch(rb"M 100(?:644|755) \S+\n", line)
        ]
        self.assertEqual(len(rows), 9)
        swapped_rows = original.replace(rows[0] + rows[1], rows[1] + rows[0], 1)
        grammar_cases = (
            ("field-order", b"".join(swapped_fields), "field order or grammar"),
            ("duplicate-field", b"".join(duplicate_field), "field order or grammar"),
            ("missing-field", b"".join(missing_field), "field or delta count"),
            ("unknown-field", original.replace(b"BASE_C: ", b"UNKNOWN: ", 1), "field order or grammar"),
            ("bad-count", original.replace(b"DELTA_COUNT: 9", b"DELTA_COUNT: 8", 1), "delta count"),
            ("bad-status", original.replace(rows[0], b"X" + rows[0][1:], 1), "delta row grammar"),
            ("bad-mode", original.replace(b" 100644 ", b" 100600 ", 1), "delta row grammar"),
            ("unsafe-path", original.replace(rows[0], b"M 100644 ../TEST_FIXTURE_ONLY\n", 1), "non-canonical"),
            ("duplicate-path", original.replace(rows[1], rows[0], 1), "duplicated or out of order"),
            ("path-order", swapped_rows, "duplicated or out of order"),
            ("wrong-path", original.replace(rows[0], b"M 100644 TEST_FIXTURE_ONLY-wrong\n", 1), "v2 delta inventory"),
            ("unknown-schema", original.replace(b"SCHEMA: coldkeep-v1-security-toolchain-progression-reference/v2", b"SCHEMA: unknown/v2", 1), "reference schema"),
            ("zero-repository-id", original.replace(b"REPOSITORY_ID: 1158711822", b"REPOSITORY_ID: 0", 1), "REPOSITORY_ID grammar"),
            ("signed-pr-number", original.replace(b"PR_NUMBER: 123", b"PR_NUMBER: +123", 1), "PR_NUMBER grammar"),
            ("whitespace-numeric", original.replace(b"DELTA_COUNT: 9", b"DELTA_COUNT:  9", 1), "field order or grammar"),
            ("zero-object", original.replace(f"BASE_C: {self.fixture.C}".encode(), b"BASE_C: " + b"0" * 40, 1), "BASE_C grammar"),
            ("unsupported-enum", original.replace(b"PURPOSE: FIXTURE_ISOLATION_AND_CLOSED_H2_SUCCESSOR_CONTRACT", b"PURPOSE: TEST_FIXTURE_ONLY", 1), "PURPOSE is unsupported"),
            ("marker", original.replace(b"<!-- coldkeep-security-toolchain-progression-v2-reference:start -->", b"<!-- TEST_FIXTURE_ONLY-wrong -->", 1), "marker cardinality or order"),
            ("duplicate-marker", original.replace(b"<!-- coldkeep-security-toolchain-progression-v2-reference:start -->", b"<!-- coldkeep-security-toolchain-progression-v2-reference:start -->\n<!-- coldkeep-security-toolchain-progression-v2-reference:start -->", 1), "marker cardinality or order"),
            ("fence", original.replace(b"```text", b"```yaml", 1), "fence"),
            ("bom", b"\xef\xbb\xbf" + original, "encoding or newline form"),
            ("nul", original.replace(b"SCHEMA:", b"SCHEMA:\x00", 1), "encoding or newline form"),
            ("crlf", original.replace(b"\n", b"\r\n", 1), "encoding or newline form"),
            ("invalid-utf8", original[:-1] + b"\xff\n", "encoding is invalid"),
            ("missing-terminal-lf", original[:-1], "exactly one terminal LF"),
            ("extra-terminal-lf", original + b"\n", "exactly one terminal LF"),
            ("over-limit", b"X" * 16385, "reference unavailable"),
        )
        for name, content, expected_detail in grammar_cases:
            reject(name, content, expected_detail)
        reject(
            "digest-mismatch",
            original.replace(b"DELTA_COUNT: 9", b"DELTA_COUNT: 8", 1),
            "digest disagrees",
            synchronize_digest=False,
        )
        contract.write_bytes(contract_original)
        reference.write_bytes(original)

    def test_v2_selection_partial_mixed_unknown_duplicate_no_fallback(self) -> None:
        state, contract, reference = self.fixture.documents()
        originals = (state.read_bytes(), contract.read_bytes(), reference.read_bytes())

        def restore() -> None:
            if reference.is_symlink() or reference.is_file():
                reference.unlink()
            elif reference.exists():
                reference.rmdir()
            state.write_bytes(originals[0])
            contract.write_bytes(originals[1])
            reference.write_bytes(originals[2])

        def rejected(name: str, mutate: object, expected: str = "v2-invalid") -> None:
            with self.subTest(selection=name):
                restore()
                mutate()
                mode, progression, detail = self.fixture.select_progression()
                self.assertEqual(mode, expected, detail)
                self.assertIsNone(progression)
                self.assertNotIn(mode, ("legacy", "progression"))

        selection_line = (
            b"SECURITY_TOOLCHAIN_PROGRESSION_SCHEMA: "
            b"coldkeep-v1-security-toolchain-progression-selection/v2\n"
        )
        pointer_line = (
            b"SECURITY_TOOLCHAIN_PROGRESSION_REFERENCE: "
            + self.fixture.PATHS[2].encode()
            + b"\n"
        )
        expectation_pattern = re.compile(
            rb"(?ms)^<!-- coldkeep-security-toolchain-progression-v2-expectations:start -->\n.*?^<!-- coldkeep-security-toolchain-progression-v2-expectations:end -->\n?"
        )
        start_marker = b"<!-- coldkeep-security-toolchain-progression-v2-reference:start -->"
        cases = (
            ("missing-schema", lambda: state.write_bytes(state.read_bytes().replace(selection_line, b"", 1))),
            ("missing-pointer", lambda: state.write_bytes(state.read_bytes().replace(pointer_line, b"", 1))),
            ("missing-expectations", lambda: contract.write_bytes(expectation_pattern.sub(b"", contract.read_bytes(), count=1))),
            ("mixed-v1-v2", lambda: contract.write_bytes(contract.read_bytes() + b"\nSCHEMA: coldkeep-v1-security-toolchain-progression-expectations/v1\n")),
            ("unknown-selection-schema", lambda: state.write_bytes(state.read_bytes().replace(b"coldkeep-v1-security-toolchain-progression-selection/v2", b"unknown/v2", 1))),
            ("unknown-expectation-schema", lambda: contract.write_bytes(contract.read_bytes().replace(b"SCHEMA: coldkeep-v1-security-toolchain-progression-expectations/v2", b"SCHEMA: unknown/v2", 1))),
            ("duplicate-schema", lambda: state.write_bytes(state.read_bytes().replace(selection_line, selection_line * 2, 1))),
            ("duplicate-pointer", lambda: state.write_bytes(state.read_bytes().replace(pointer_line, pointer_line * 2, 1))),
            ("duplicate-expectation-marker", lambda: contract.write_bytes(contract.read_bytes().replace(b"<!-- coldkeep-security-toolchain-progression-v2-expectations:start -->", b"<!-- coldkeep-security-toolchain-progression-v2-expectations:start -->\n<!-- coldkeep-security-toolchain-progression-v2-expectations:start -->", 1))),
            ("duplicate-reference-marker", lambda: (reference.write_bytes(reference.read_bytes().replace(start_marker, start_marker + b"\n" + start_marker, 1)), self.fixture.sync_digest())),
            ("malformed-selected-v2", lambda: (reference.write_bytes(reference.read_bytes().replace(b"DELTA_COUNT: 9", b"DELTA_COUNT: 8", 1)), self.fixture.sync_digest())),
            ("unknown-reference-schema", lambda: (reference.write_bytes(reference.read_bytes().replace(b"SCHEMA: coldkeep-v1-security-toolchain-progression-reference/v2", b"SCHEMA: unknown/v2", 1)), self.fixture.sync_digest())),
        )
        for name, mutation in cases:
            rejected(name, mutation)

        rejected("missing-reference", lambda: reference.unlink(), "v2-unavailable")
        rejected("directory-reference", lambda: (reference.unlink(), reference.mkdir()), "v2-unavailable")
        target = self.fixture.root / "TEST_FIXTURE_ONLY-v2-reference"

        def unsafe_symlink() -> None:
            target.write_bytes(reference.read_bytes())
            reference.unlink()
            reference.symlink_to(target)

        rejected("symlink-reference", unsafe_symlink, "v2-unavailable")

        restore()
        state.write_bytes(
            state.read_bytes().replace(selection_line, b"", 1).replace(pointer_line, b"", 1)
        )
        contract.write_bytes(expectation_pattern.sub(b"", contract.read_bytes(), count=1))
        reference.unlink()
        mode, progression, detail = self.fixture.select_progression()
        self.assertEqual((mode, progression, detail), ("legacy", None, None))
        restore()

    def test_v2_wrong_missing_malformed_partial_repository_id_rejects(self) -> None:
        h3 = self.fixture.make_h3()
        push_event, push_environment = self.fixture.push_context(h3)
        self.fixture.checkout_detached(h3)
        self.assert_repository_id_matrix_rejected(
            label="push",
            state="post-release-terminal-serialization-candidate",
            event=push_event,
            environment=push_environment,
        )

        self.fixture.checkout_branch(
            "release/v1.13.17-terminal-serialization", h3
        )
        pr_event, pr_environment = self.fixture.pr_context(h3, h3)
        self.assert_repository_id_matrix_rejected(
            label="direct-pr",
            state="post-release-terminal-serialization-candidate",
            event=pr_event,
            environment=pr_environment,
        )

        main = self.fixture.synthetic_merge(h3)
        self.fixture.checkout_detached(main)
        main_event, main_environment = self.fixture.main_context(main)
        self.assert_repository_id_matrix_rejected(
            label="main",
            state="post-release-terminal-serialization-main",
            event=main_event,
            environment=main_environment,
        )

    def test_v2_supported_explicit_push_pr_main_contexts(self) -> None:
        h3 = self.fixture.make_h3()

        def accepted(
            payload: dict[str, object], state: str, scope: str
        ) -> None:
            self.assertEqual(payload["status"], "ok")
            self.assertEqual(payload["state"], state)
            self.assertEqual(payload["evidence_scope"], scope)
            self.assertEqual(
                payload["security_progression"],
                "H2_SUCCESSOR_STRUCTURAL_ONLY",
            )

        completed, payload = self.fixture.run_json()
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        accepted(
            payload,
            "post-release-terminal-serialization-candidate",
            "LOCAL_H2_SUCCESSOR_CANDIDATE",
        )

        _, push_environment = self.fixture.push_context(h3)
        completed, payload = self.fixture.run_json(env=push_environment)
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        accepted(
            payload,
            "post-release-terminal-serialization-candidate",
            "GITHUB_H2_SUCCESSOR_PUSH_CONTEXT_CONSISTENCY",
        )

        self.fixture.checkout_detached(h3)
        bare, bare_payload = self.fixture.run_json()
        self.assertNotEqual(bare.returncode, 0)
        self.assertEqual(bare_payload["evidence_scope"], "UNAVAILABLE")

        completed, payload = self.fixture.run_json(env=push_environment)
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        accepted(
            payload,
            "post-release-terminal-serialization-candidate",
            "GITHUB_H2_SUCCESSOR_PUSH_CONTEXT_CONSISTENCY",
        )

        self.fixture.checkout_branch(
            "release/v1.13.17-terminal-serialization", h3
        )
        _, direct_environment = self.fixture.pr_context(h3, h3)
        completed, payload = self.fixture.run_json(env=direct_environment)
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        accepted(
            payload,
            "post-release-terminal-serialization-candidate",
            "GITHUB_H2_SUCCESSOR_PR_DIRECT_CONTEXT_CONSISTENCY",
        )

        synthetic = self.fixture.synthetic_merge(h3)
        self.fixture.checkout_detached(synthetic)
        _, synthetic_environment = self.fixture.pr_context(
            h3, synthetic, merge_value="1" * 40
        )
        completed, payload = self.fixture.run_json(env=synthetic_environment)
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        accepted(
            payload,
            "post-release-terminal-serialization-candidate",
            "GITHUB_H2_SUCCESSOR_PR_SYNTHETIC_CONTEXT_CONSISTENCY",
        )

        self.fixture.checkout_branch("main", synthetic)
        completed, payload = self.fixture.run_json()
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        accepted(
            payload,
            "post-release-terminal-serialization-main",
            "LOCAL_H2_SUCCESSOR_MAIN_ARTIFACT_ONLY",
        )

        self.fixture.checkout_detached(synthetic)
        _, main_environment = self.fixture.main_context(synthetic)
        completed, payload = self.fixture.run_json(env=main_environment)
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        accepted(
            payload,
            "post-release-terminal-serialization-main",
            "GITHUB_H2_SUCCESSOR_MAIN_CONTEXT_CONSISTENCY",
        )

    def test_v2_diagnostic_v4_sources_statuses_and_human_json_parity(self) -> None:
        progression = self.fixture.progression()
        h3 = self.fixture.make_h3()

        def assert_context_parity(
            *,
            expected_scope: str,
            state: str = "auto",
            environment: dict[str, str] | None = None,
            expected_returncode: int = 0,
            expected_rule: str | None = None,
        ) -> dict[str, object]:
            completed, context_payload = self.fixture.run_json(
                state,
                env=environment,
                copied_validator=True,
            )
            self.assertEqual(
                completed.returncode,
                expected_returncode,
                completed.stdout + completed.stderr,
            )
            self.assertEqual(context_payload["evidence_scope"], expected_scope)
            human = self.fixture.run_human(
                state,
                env=environment,
                copied_validator=True,
            )
            self.assertEqual(
                human.returncode,
                completed.returncode,
                human.stdout + human.stderr,
            )
            parsed = self.assert_human_json_semantic_parity(
                human, context_payload
            )
            if expected_rule is not None:
                self.assertIn(expected_rule, parsed["rules"])
            return context_payload

        assert_context_parity(
            expected_scope="LOCAL_H2_SUCCESSOR_CANDIDATE"
        )
        _, push_environment = self.fixture.push_context(h3)
        assert_context_parity(
            expected_scope="GITHUB_H2_SUCCESSOR_PUSH_CONTEXT_CONSISTENCY",
            environment=push_environment,
        )
        failed_push_environment = dict(push_environment)
        failed_push_environment["GITHUB_REPOSITORY_ID"] = "1158711821"
        assert_context_parity(
            expected_scope="UNSUPPORTED_OR_CONFLICTING_CONTEXT",
            state="post-release-terminal-serialization-candidate",
            environment=failed_push_environment,
            expected_returncode=1,
            expected_rule="CKRS016",
        )
        missing = "f" * 40
        descendant = self.fixture.raw_commit_tree(
            self.fixture.rev(f"{h3}^{{tree}}"), h3
        )
        statuses = {
            release_state_support.security_progression_status(
                self.fixture.root, h3, None
            ),
            release_state_support.security_progression_status(
                self.fixture.root, self.fixture.H1, progression
            ),
            release_state_support.security_progression_status(
                self.fixture.root, self.fixture.H2, progression
            ),
            release_state_support.security_progression_status(
                self.fixture.root, h3, progression
            ),
            release_state_support.security_progression_status(
                self.fixture.root, descendant, progression
            ),
            release_state_support.security_progression_status(
                self.fixture.root, missing, progression
            ),
        }
        self.assertEqual(
            statuses,
            {
                "NOT_SELECTED",
                "HISTORICAL_H1_UNPATCHED",
                "PUBLISHED_H2_STRUCTURAL_ONLY_ORIGINAL_CI_FAILED",
                "H2_SUCCESSOR_STRUCTURAL_ONLY",
                "INVALID",
                "UNAVAILABLE",
            },
        )

        _, environment = self.fixture.pr_context(h3, h3)
        environment.update(
            {
                "GITHUB_RUN_ID": "123456",
                "GITHUB_RUN_ATTEMPT": "1",
                "GITHUB_JOB": "quality",
                "GITHUB_WORKFLOW": "CI",
            }
        )
        diagnostic = Path(self.fixture.temporary.name) / "v2-diagnostic.json"
        completed, payload = self.fixture.run_json(
            env=environment,
            diagnostic=diagnostic,
            copied_validator=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertEqual(
            set(payload),
            {
                "status", "validator", "state", "artifact_state",
                "active_version", "authorization_status",
                "certification_status", "outstanding_obligations",
                "evidence_scope", "security_progression", "security_result",
                "violations", "error", "progression_contract_schema",
            },
        )
        self.assertEqual(payload["security_progression"], "H2_SUCCESSOR_STRUCTURAL_ONLY")
        self.assertEqual(
            payload["progression_contract_schema"],
            "coldkeep-v1-security-toolchain-progression-reference/v2",
        )
        self.assertEqual(
            payload["evidence_scope"],
            "GITHUB_H2_SUCCESSOR_PR_DIRECT_CONTEXT_CONSISTENCY",
        )
        self.assertEqual(payload["authorization_status"], "NOT_EVALUATED_BY_VALIDATOR")
        self.assertEqual(payload["certification_status"], "PENDING_EXTERNAL_EVIDENCE")
        self.assertEqual(payload["security_result"], "NOT_EVALUATED_BY_VALIDATOR")

        record = json.loads(diagnostic.read_text(encoding="utf-8"))
        self.assertEqual(
            set(record),
            {
                "schema", "created_at_utc", "validator", "attribution",
                "event_snapshots", "evaluations", "capture",
            },
        )
        self.assertEqual(record["schema"], "coldkeep-release-state-diagnostic/v4")
        self.assertEqual(
            set(record["validator"]),
            {
                "requested_state", "resolved_state", "result", "exit_code",
                "rule_codes", "authorization_status", "certification_status",
                "checkout_commit", "checkout_tree", "sources",
                "security_progression", "security_result",
                "progression_contract_schema",
            },
        )
        self.assertEqual(record["validator"]["security_progression"], payload["security_progression"])
        self.assertEqual(record["validator"]["security_result"], payload["security_result"])
        self.assertEqual(record["validator"]["resolved_state"], payload["state"])
        self.assertEqual(record["validator"]["authorization_status"], payload["authorization_status"])
        self.assertEqual(record["validator"]["certification_status"], payload["certification_status"])
        self.assertEqual(record["validator"]["progression_contract_schema"], payload["progression_contract_schema"])
        sources = record["validator"]["sources"]
        self.assertEqual(
            [source["path"] for source in sources],
            list(release_state_support.DiagnosticRecorder._SOURCE_PATHS),
        )
        self.assertEqual(len({source["path"] for source in sources}), 5)
        for source in sources:
            self.assertEqual(
                set(source),
                {
                    "path", "loaded_path_match", "git_blob",
                    "measured_git_blob", "measured_sha256",
                    "matches_git_blob", "bytes",
                },
            )
            self.assertIs(source["loaded_path_match"], True)
            self.assertIs(source["matches_git_blob"], True)
            self.assertEqual(source["git_blob"], source["measured_git_blob"])
            self.assertRegex(source["measured_sha256"], r"^[0-9a-f]{64}$")
            self.assertGreater(source["bytes"], 0)
        self.assertEqual(
            set(record["attribution"]),
            {"repository", "run_id", "run_attempt", "job", "workflow", "event_name"},
        )
        self.assertTrue(
            all(
                record["attribution"][key]["category"] == "validated"
                for key in record["attribution"]
            )
        )
        self.assertGreaterEqual(len(record["evaluations"]), 1)
        self.assertTrue(all(evaluation["result"] == "PASS" for evaluation in record["evaluations"]))
        self.assertIs(record["capture"]["complete"], True)
        self.assertEqual(record["capture"]["incomplete_reasons"], [])
        self.assertIs(record["capture"]["non_authorizing"], True)

        human = self.fixture.run_human(env=environment, copied_validator=True)
        self.assertEqual(human.returncode, completed.returncode, human.stdout + human.stderr)
        self.assert_human_json_semantic_parity(human, payload)

        synthetic = self.fixture.synthetic_merge(h3)
        self.fixture.checkout_detached(synthetic)
        _, synthetic_environment = self.fixture.pr_context(
            h3, synthetic, merge_value="1" * 40
        )
        assert_context_parity(
            expected_scope="GITHUB_H2_SUCCESSOR_PR_SYNTHETIC_CONTEXT_CONSISTENCY",
            environment=synthetic_environment,
        )
        self.fixture.checkout_branch("main", synthetic)
        assert_context_parity(
            expected_scope="LOCAL_H2_SUCCESSOR_MAIN_ARTIFACT_ONLY"
        )
        self.fixture.checkout_detached(synthetic)
        _, main_environment = self.fixture.main_context(synthetic)
        assert_context_parity(
            expected_scope="GITHUB_H2_SUCCESSOR_MAIN_CONTEXT_CONSISTENCY",
            environment=main_environment,
        )
        self.fixture.checkout_branch(
            "release/v1.13.17-terminal-serialization", h3
        )

        _, contract, reference = self.fixture.documents()
        contract_original = contract.read_bytes()
        reference_original = reference.read_bytes()
        restored_graph = {
            "head": self.fixture.rev("HEAD"),
            "parent": self.fixture.rev(f"{h3}^"),
            "tree": self.fixture.rev(f"{h3}^{{tree}}"),
        }

        def assert_restored_valid() -> None:
            self.assertEqual(contract.read_bytes(), contract_original)
            self.assertEqual(reference.read_bytes(), reference_original)
            loaded, detail = self.fixture.load_progression()
            self.assertIsNotNone(loaded, detail)
            restored, restored_payload = self.fixture.run_json(
                "post-release-terminal-serialization-candidate",
                copied_validator=True,
            )
            self.assertEqual(
                restored.returncode,
                0,
                restored.stdout + restored.stderr,
            )
            self.assertEqual(restored_payload["status"], "ok")
            self.assertEqual(
                restored_payload["evidence_scope"],
                "LOCAL_H2_SUCCESSOR_CANDIDATE",
            )
            self.assertEqual(
                restored_payload["authorization_status"],
                "NOT_EVALUATED_BY_VALIDATOR",
            )
            self.assertEqual(
                restored_payload["certification_status"],
                "PENDING_EXTERNAL_EVIDENCE",
            )
            self.assertEqual(
                {
                    "head": self.fixture.rev("HEAD"),
                    "parent": self.fixture.rev(f"{h3}^"),
                    "tree": self.fixture.rev(f"{h3}^{{tree}}"),
                },
                restored_graph,
            )

        try:
            mutated_reference = reference_original.replace(
                b"DELTA_COUNT: 9", b"DELTA_COUNT: 8", 1
            )
            self.assertEqual(
                reference_original.count(b"DELTA_COUNT: 9"), 1
            )
            reference.write_bytes(mutated_reference)
            self.fixture.sync_digest()
            invalid_diagnostic = (
                Path(self.fixture.temporary.name) / "v2-invalid-diagnostic.json"
            )
            invalid, invalid_payload = self.fixture.run_json(
                "post-release-terminal-serialization-candidate",
                diagnostic=invalid_diagnostic,
                copied_validator=True,
            )
            self.assertEqual(invalid.returncode, 1)
            self.assertEqual(invalid_payload["status"], "error")
            self.assertEqual(invalid_payload["security_progression"], "INVALID")
            self.assertEqual(
                invalid_payload["progression_contract_schema"], "INVALID"
            )
            self.assertEqual(
                invalid_payload["evidence_scope"],
                "UNSUPPORTED_OR_CONFLICTING_CONTEXT",
            )
            invalid_ckrs020 = [
                violation["message"]
                for violation in invalid_payload["violations"]
                if violation["rule"] == "CKRS020"
            ]
            self.assertEqual(
                invalid_ckrs020,
                [
                    "security progression reference invalid: "
                    "delta count is invalid"
                ],
            )
            invalid_human = self.fixture.run_human(
                "post-release-terminal-serialization-candidate",
                copied_validator=True,
            )
            self.assertEqual(invalid_human.returncode, invalid.returncode)
            invalid_parsed = self.assert_human_json_semantic_parity(
                invalid_human, invalid_payload
            )
            self.assertIn("CKRS020", invalid_parsed["rules"])
            invalid_record = json.loads(
                invalid_diagnostic.read_text(encoding="utf-8")
            )
            self.assertEqual(
                invalid_record["schema"],
                "coldkeep-release-state-diagnostic/v4",
            )
            self.assertEqual(len(invalid_record["validator"]["sources"]), 5)
            self.assertIs(invalid_record["capture"]["complete"], False)
            self.assertTrue(invalid_record["capture"]["incomplete_reasons"])
        finally:
            contract.write_bytes(contract_original)
            reference.write_bytes(reference_original)

        assert_restored_valid()

        direct_environment = os.environ.copy()
        for key in GITHUB_KEYS:
            direct_environment.pop(key, None)
        direct_environment.update(environment)
        with mock.patch.dict(os.environ, direct_environment, clear=True):
            evaluated = validate_release_state.validate(self.fixture.root, "auto")
        self.assertEqual(evaluated.ordered(), [])
        before_render = repr(evaluated)
        human_output = io.StringIO()
        json_output = io.StringIO()
        render_sentinel = AssertionError("renderer attempted evaluation or I/O")
        with (
            mock.patch.object(
                validate_release_state, "validate", side_effect=render_sentinel
            ),
            mock.patch.object(
                validate_release_state,
                "security_progression_selection",
                side_effect=render_sentinel,
            ),
            mock.patch.object(
                validate_release_state, "run_git", side_effect=render_sentinel
            ),
            mock.patch.object(
                validate_release_state,
                "github_context",
                side_effect=render_sentinel,
            ),
            mock.patch.object(
                validate_release_state,
                "load_terminal_disposition",
                side_effect=render_sentinel,
            ),
            mock.patch.object(
                validate_release_state,
                "load_security_toolchain_progression",
                side_effect=render_sentinel,
            ),
            mock.patch.object(
                release_state_support, "run_process", side_effect=render_sentinel
            ),
            mock.patch.object(Path, "read_text", side_effect=render_sentinel),
            mock.patch.object(Path, "read_bytes", side_effect=render_sentinel),
        ):
            with redirect_stdout(human_output):
                human_returncode = validate_release_state.emit(evaluated, False)
            with redirect_stdout(json_output):
                json_returncode = validate_release_state.emit(evaluated, True)
        self.assertEqual(repr(evaluated), before_render)
        self.assertEqual(human_returncode, 0)
        self.assertEqual(json_returncode, 0)
        direct_payload = json.loads(json_output.getvalue())
        self.assert_human_json_semantic_parity(
            ProcessResult([], human_returncode, human_output.getvalue(), ""),
            direct_payload,
        )
        for key in (
            "state",
            "active_version",
            "security_progression",
            "security_result",
            "progression_contract_schema",
            "authorization_status",
            "certification_status",
            "evidence_scope",
            "violations",
        ):
            self.assertEqual(direct_payload[key], payload[key])

        try:
            reference.unlink()
            unavailable_diagnostic = (
                Path(self.fixture.temporary.name) / "v2-unavailable-diagnostic.json"
            )
            unavailable, unavailable_payload = self.fixture.run_json(
                "post-release-terminal-serialization-candidate",
                diagnostic=unavailable_diagnostic,
                copied_validator=True,
            )
            self.assertEqual(unavailable.returncode, 1)
            self.assertEqual(unavailable_payload["status"], "error")
            self.assertEqual(
                unavailable_payload["security_progression"], "UNAVAILABLE"
            )
            self.assertEqual(
                unavailable_payload["progression_contract_schema"],
                "UNAVAILABLE",
            )
            self.assertEqual(
                unavailable_payload["evidence_scope"],
                "UNSUPPORTED_OR_CONFLICTING_CONTEXT",
            )
            unavailable_ckrs020 = [
                violation["message"]
                for violation in unavailable_payload["violations"]
                if violation["rule"] == "CKRS020"
            ]
            self.assertEqual(
                unavailable_ckrs020,
                ["security progression reference unavailable: file is missing"],
            )
            unavailable_human = self.fixture.run_human(
                "post-release-terminal-serialization-candidate",
                copied_validator=True,
            )
            self.assertEqual(unavailable_human.returncode, unavailable.returncode)
            unavailable_parsed = self.assert_human_json_semantic_parity(
                unavailable_human, unavailable_payload
            )
            self.assertIn("CKRS020", unavailable_parsed["rules"])
            unavailable_record = json.loads(
                unavailable_diagnostic.read_text(encoding="utf-8")
            )
            self.assertEqual(
                unavailable_record["schema"],
                "coldkeep-release-state-diagnostic/v4",
            )
            reference_source = next(
                source
                for source in unavailable_record["validator"]["sources"]
                if source["path"] == self.fixture.PATHS[2]
            )
            self.assertIsNone(reference_source["bytes"])
            self.assertIs(unavailable_record["capture"]["complete"], False)
            self.assertTrue(unavailable_record["capture"]["incomplete_reasons"])
        finally:
            contract.write_bytes(contract_original)
            reference.write_bytes(reference_original)

        assert_restored_valid()


FINAL_SNAPSHOT_TEST_ROOT = Path(__file__).resolve().parent.parent


def load_final_snapshot_for_test() -> release_state_support.FinalSourceSnapshot:
    contract_path = (
        "docs/release/v1.13/v1.13.18-release-state-validator-contract.md"
    )
    reference_path = (
        "docs/release/v1.13/v1.13.18-final-source-snapshot-reference.md"
    )
    contract = release_state_support.Document.load(
        FINAL_SNAPSHOT_TEST_ROOT, contract_path
    )
    snapshot, detail = release_state_support.load_final_source_snapshot(
        FINAL_SNAPSHOT_TEST_ROOT, contract, reference_path
    )
    if detail or snapshot is None:
        raise AssertionError(detail or "snapshot was not loaded")
    return snapshot


class FinalSnapshotReferenceGovernanceTests(unittest.TestCase):
    def test_reference_and_expectations_load_atomically(self) -> None:
        snapshot = load_final_snapshot_for_test()
        self.assertEqual(snapshot.values["SOURCE_VERSION"], "1.13.18")
        self.assertEqual(len(snapshot.delta), 33)

    def test_wrong_pointer_has_no_fallback(self) -> None:
        contract = release_state_support.Document.load(
            FINAL_SNAPSHOT_TEST_ROOT,
            "docs/release/v1.13/v1.13.18-release-state-validator-contract.md",
        )
        snapshot, detail = release_state_support.load_final_source_snapshot(
            FINAL_SNAPSHOT_TEST_ROOT, contract, "wrong.md"
        )
        self.assertIsNone(snapshot)
        self.assertIn("pointer disagrees", detail or "")


class FinalSnapshotSelectionTests(unittest.TestCase):
    def test_exact_selector_is_selected(self) -> None:
        state = release_state_support.Document.load(
            FINAL_SNAPSHOT_TEST_ROOT,
            "docs/release/v1.13/v1.13.18-release-state.md",
        )
        contract = release_state_support.Document.load(
            FINAL_SNAPSHOT_TEST_ROOT,
            "docs/release/v1.13/v1.13.18-release-state-validator-contract.md",
        )
        mode, snapshot, detail = validate_release_state.final_snapshot_selection(
            FINAL_SNAPSHOT_TEST_ROOT, "1.13.18", state, contract
        )
        self.assertEqual((mode, detail), ("snapshot", None))
        self.assertIsNotNone(snapshot)

    def test_partial_selector_fails_closed(self) -> None:
        state = release_state_support.Document.load(
            FINAL_SNAPSHOT_TEST_ROOT,
            "docs/release/v1.13/v1.13.18-release-state.md",
        )
        state = release_state_support.Document(
            state.root,
            state.path,
            [
                line
                for line in state.lines
                if not line.startswith("FINAL_SOURCE_SNAPSHOT_SCHEMA:")
            ],
        )
        contract = release_state_support.Document.load(
            FINAL_SNAPSHOT_TEST_ROOT,
            "docs/release/v1.13/v1.13.18-release-state-validator-contract.md",
        )
        mode, snapshot, detail = validate_release_state.final_snapshot_selection(
            FINAL_SNAPSHOT_TEST_ROOT, "1.13.18", state, contract
        )
        self.assertEqual(mode, "invalid")
        self.assertIsNone(snapshot)
        self.assertIn("selection schema", detail or "")

    def test_selected_route_emits_diagnostic_v5_fields(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            result = validate_release_state.validate(
                FINAL_SNAPSHOT_TEST_ROOT, "auto"
            )
        self.assertEqual(
            result.diagnostic_schema, "coldkeep-release-state-diagnostic/v5"
        )
        self.assertIn(
            result.final_snapshot,
            {"CANDIDATE_SUBSET_VALID", "CANDIDATE_COMPLETE_VALID"},
        )
        output = io.StringIO()
        with redirect_stdout(output):
            status = validate_release_state.emit(result, True)
        payload = json.loads(output.getvalue())
        self.assertEqual(status, 0)
        self.assertEqual(payload["final_snapshot"], result.final_snapshot)
        self.assertEqual(
            payload["final_snapshot_contract_schema"],
            "coldkeep-v1-final-source-snapshot-reference/v1",
        )


class FinalSnapshotDeltaContractTests(unittest.TestCase):
    def test_baseline_is_a_valid_development_subset(self) -> None:
        snapshot = load_final_snapshot_for_test()
        self.assertTrue(
            release_state_support.final_snapshot_artifact(
                FINAL_SNAPSHOT_TEST_ROOT,
                snapshot.values["BASE_M"],
                snapshot,
                require_complete=False,
            )
        )

    def test_one_wrong_status_is_rejected(self) -> None:
        snapshot = load_final_snapshot_for_test()
        changed = list(snapshot.delta)
        changed[0] = ("A", changed[0][1], changed[0][2])
        detail = release_state_support._final_snapshot_grammar(
            snapshot.values, tuple(changed)
        )
        self.assertEqual(detail, "delta inventory is unsupported")

    def test_committed_complete_inventory_accepts_git_tree_order(self) -> None:
        snapshot = load_final_snapshot_for_test()
        head = run_process(
            [
                resolved_executable("git"),
                "-C",
                str(FINAL_SNAPSHOT_TEST_ROOT),
                "rev-parse",
                "HEAD",
            ],
            check=True,
        ).stdout.strip()
        self.assertTrue(
            release_state_support.final_snapshot_artifact(
                FINAL_SNAPSHOT_TEST_ROOT,
                head,
                snapshot,
                require_complete=True,
            )
        )


class FinalSnapshotReleasePushContextTests(unittest.TestCase):
    def test_local_release_branch_is_bounded_by_snapshot(self) -> None:
        snapshot = load_final_snapshot_for_test()
        declaration = release_state_support.LifecycleDeclaration(
            "immutable-transition-v1", "franchoy/coldkeep"
        )
        context = (
            snapshot.values["BASE_M"], "release/v1.13.18", False, False, None
        )
        env = {key: "" for key in release_state_support.github_context()}
        scope = release_state_support.v1_context_scope(
            FINAL_SNAPSHOT_TEST_ROOT,
            "1.13.18",
            "development",
            context,
            env,
            declaration,
            final_snapshot=snapshot,
        )
        self.assertEqual(scope, "LOCAL_RELEASE_BRANCH")


class FinalSnapshotPullRequestContextTests(unittest.TestCase):
    def test_pr_requires_exact_m_base(self) -> None:
        snapshot = load_final_snapshot_for_test()
        declaration = release_state_support.LifecycleDeclaration(
            "immutable-transition-v1", "franchoy/coldkeep"
        )
        context = (snapshot.values["BASE_M"], "", False, False, None)
        env = {key: "TEST" for key in release_state_support.github_context()}
        with (
            mock.patch.object(
                release_state_support,
                "strict_release_pr_context",
                return_value=True,
            ),
            mock.patch.object(
                release_state_support,
                "final_snapshot_artifact",
                return_value=True,
            ),
            mock.patch.object(
                release_state_support,
                "final_snapshot_pr_base_matches",
                side_effect=(True, False),
            ),
        ):
            accepted = release_state_support.v1_context_scope(
                FINAL_SNAPSHOT_TEST_ROOT,
                "1.13.18",
                "development",
                context,
                env,
                declaration,
                final_snapshot=snapshot,
            )
            rejected = release_state_support.v1_context_scope(
                FINAL_SNAPSHOT_TEST_ROOT,
                "1.13.18",
                "development",
                context,
                env,
                declaration,
                final_snapshot=snapshot,
            )
        self.assertEqual(accepted, "GITHUB_PR_EVENT_CONTEXT_CONSISTENCY")
        self.assertIsNone(rejected)


class FinalSnapshotMainContextTests(unittest.TestCase):
    def test_main_requires_exact_snapshot_merge_artifact(self) -> None:
        snapshot = load_final_snapshot_for_test()
        declaration = release_state_support.LifecycleDeclaration(
            "immutable-transition-v1", "franchoy/coldkeep"
        )
        context = ("f" * 40, "main", False, False, None)
        env = {key: "" for key in release_state_support.github_context()}
        with (
            mock.patch.object(
                release_state_support, "normal_merge_artifact", return_value=True
            ),
            mock.patch.object(
                release_state_support,
                "final_snapshot_main_artifact",
                side_effect=(True, False),
            ),
        ):
            accepted = release_state_support.v1_context_scope(
                FINAL_SNAPSHOT_TEST_ROOT,
                "1.13.18",
                "merged-pending-final-main-certification",
                context,
                env,
                declaration,
                final_snapshot=snapshot,
            )
            rejected = release_state_support.v1_context_scope(
                FINAL_SNAPSHOT_TEST_ROOT,
                "1.13.18",
                "merged-pending-final-main-certification",
                context,
                env,
                declaration,
                final_snapshot=snapshot,
            )
        self.assertEqual(accepted, "LOCAL_ARTIFACT_ONLY")
        self.assertIsNone(rejected)


class FinalSnapshotTagSimulationTests(unittest.TestCase):
    def test_tag_simulation_requires_snapshot_main_artifact(self) -> None:
        snapshot = load_final_snapshot_for_test()
        declaration = release_state_support.LifecycleDeclaration(
            "immutable-transition-v1", "franchoy/coldkeep"
        )
        env = {key: "" for key in release_state_support.github_context()}
        with mock.patch.object(
            release_state_support,
            "final_snapshot_main_artifact",
            return_value=True,
        ):
            scope = release_state_support.v1_context_scope(
                FINAL_SNAPSHOT_TEST_ROOT,
                "1.13.18",
                "tagged",
                ("f" * 40, "", True, True, "f" * 40),
                env,
                declaration,
                final_snapshot=snapshot,
            )
        self.assertEqual(scope, "LOCAL_TAG_ARTIFACT_ONLY")


class FinalSnapshotAuthorityBoundaryTests(unittest.TestCase):
    def test_reference_is_explicitly_non_authorizing(self) -> None:
        snapshot = load_final_snapshot_for_test()
        self.assertEqual(snapshot.values["STATUS"], "STRUCTURAL_NON_AUTHORIZING")
        self.assertEqual(
            snapshot.expectations["EXPECTED_VALIDATOR_EFFECT"],
            "STRUCTURAL_ONLY_NO_PROVIDER_OR_OWNER_AUTHORITY",
        )


class HistoricalV11317ImmutabilityTests(unittest.TestCase):
    def test_every_frozen_release_document_matches_phase01_digest(self) -> None:
        self.assertEqual(validate_governance._validate_v11317_frozen(
            FINAL_SNAPSHOT_TEST_ROOT
        ), [])


class FinalSnapshotErroneousContextTests(unittest.TestCase):
    def test_wrong_source_version_cannot_select_snapshot(self) -> None:
        state = release_state_support.Document.load(
            FINAL_SNAPSHOT_TEST_ROOT,
            "docs/release/v1.13/v1.13.18-release-state.md",
        )
        contract = release_state_support.Document.load(
            FINAL_SNAPSHOT_TEST_ROOT,
            "docs/release/v1.13/v1.13.18-release-state-validator-contract.md",
        )
        mode, snapshot, detail = validate_release_state.final_snapshot_selection(
            FINAL_SNAPSHOT_TEST_ROOT, "1.13.19", state, contract
        )
        self.assertEqual(mode, "invalid")
        self.assertIsNone(snapshot)
        self.assertIn("unsupported source version", detail or "")


if __name__ == "__main__":
    unittest.main(verbosity=2)
