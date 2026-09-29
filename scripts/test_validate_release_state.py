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
from contextlib import redirect_stderr
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


SCRIPT = Path(__file__).with_name("validate_release_state.py").resolve()
GITHUB_KEYS = (
    "GITHUB_EVENT_NAME",
    "GITHUB_REF",
    "GITHUB_REF_NAME",
    "GITHUB_HEAD_REF",
    "GITHUB_BASE_REF",
    "GITHUB_EVENT_PATH",
    "GITHUB_REPOSITORY",
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


def copy_successor_fixture_source(source_root: Path, destination_root: Path) -> None:
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
    for relative in sorted(paths):
        source = source_root / relative
        destination = destination_root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination, follow_symlinks=False)


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
                    "coldkeep-release-state-diagnostic/v2",
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
            {"requested_state", "resolved_state", "result", "exit_code", "rule_codes", "authorization_status", "certification_status", "checkout_commit", "checkout_tree", "sources"},
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
        git(fixture.root, "add", "scripts")
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
        git(fixture.root, "add", "scripts")
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
    def test_current_pair_copy_is_independent_of_index_membership(self) -> None:
        pair = (
            validate_governance.CURRENT_RELEASE_BODY,
            validate_governance.CURRENT_RELEASE_BODY_CHECKSUM,
        )
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source"
            source.mkdir()
            body = b"TEST_FIXTURE_ONLY body\n"
            digest = hashlib.sha256(body).hexdigest()
            for relative, content in (
                (pair[0], body),
                (pair[1], f"{digest}  {pair[0].as_posix()}\n".encode("ascii")),
            ):
                path = source / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(content)
            git(source, "init")
            observed = []
            for index_state in ("untracked", "tracked"):
                if index_state == "tracked":
                    git(source, "add", *(item.as_posix() for item in pair))
                destination = Path(directory) / index_state
                destination.mkdir()
                copy_successor_fixture_source(source, destination)
                observed.append(tuple((destination / item).read_bytes() for item in pair))
            self.assertEqual(observed[0], observed[1])
            self.assertEqual(observed[0], (body, f"{digest}  {pair[0].as_posix()}\n".encode("ascii")))


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
            r"(?ms)^v1\.13\.17 — Annotated-Tag Certification Recovery is the .*?^established\.$",
            readme_state,
        )
        self.replace_regex_once(
            "docs/release/v1.13/README.md",
            r"(?ms)^v1\.13\.17 is the .*?^[^\n]*closure is authorized by this tracked state\.$",
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
                    "active_version=1.13.17 violations=0\n",
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
                                "active_version=1.13.17 violations=0\n",
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
                        "coldkeep-release-state-diagnostic/v2",
                    )
                    self.assertTrue(diagnostic["capture"]["complete"])
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
                        self.assertTrue(source["loaded_path_match"])
                        self.assertTrue(source["matches_git_blob"])
                        self.assertEqual(
                            source["git_blob"], source["measured_git_blob"]
                        )
                        self.assertEqual(
                            source["measured_sha256"],
                            hashlib.sha256(
                                (self.root / source["path"]).read_bytes()
                            ).hexdigest(),
                        )

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


if __name__ == "__main__":
    unittest.main(verbosity=2)
