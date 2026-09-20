#!/usr/bin/env python3
"""Isolated fixture tests for the release-state validator."""

from __future__ import annotations

import io
import json
import os
import re
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest import mock

import release_state_support
import validate_release_state
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
)


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
        merge_sha: str,
        *,
        repository: str = "fixture/coldkeep",
        base_repository: str | None = None,
        head_repository: str | None = None,
    ) -> Path:
        path = self.root / "strict-pr-event.json"
        path.write_text(
            json.dumps(
                {
                    "repository": {"full_name": repository},
                    "number": 7,
                    "pull_request": {
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
                        "merge_commit_sha": merge_sha,
                    },
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
        event = fixture.write_push_event(
            "refs/tags/v1.13.10",
            "0" * 40,
            merge,
            created=True,
        )
        payload = self.json_result(
            fixture.run(
                "--json",
                env=fixture.push_env(
                    "refs/tags/v1.13.10",
                    merge,
                    event,
                    ref_type="tag",
                ),
            )
        )
        self.assertEqual(
            payload["evidence_scope"], "GITHUB_TAG_EVENT_CONTEXT_CONSISTENCY"
        )
        wrong = fixture.write_push_event(
            "refs/tags/v1.13.10",
            "0" * 40,
            tag_object,
            created=True,
        )
        self.assert_fails_rule(
            fixture.run(
                env=fixture.push_env(
                    "refs/tags/v1.13.10",
                    merge,
                    wrong,
                    ref_type="tag",
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
