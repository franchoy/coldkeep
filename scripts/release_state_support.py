"""Private support primitives for the Coldkeep release-state validator."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import tempfile
from datetime import datetime, timezone
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Optional


SEMVER = re.compile(r"^\d+\.\d+\.\d+$")
CHANGELOG_HEADING = re.compile(r"^## (?:v|\[)(\d+\.\d+\.\d+)(?:\]|\b)(.*)$")
PHASE_HEADING = re.compile(r"^## Phase (\d+)\b")
METADATA = re.compile(r"^\*\*(Release|Status|Branch|Phase status):\*\*\s*(.*)$")
LIFECYCLE_BOUNDARY = re.compile(
    r"^\*\*(Merge-complete phase|Tag/publication phase|Post-publication closure phase):\*\*\s*(.*)$",
)
LIFECYCLE_DECLARATION = re.compile(
    r"^\*\*(Lifecycle declaration model|Canonical repository):\*\*\s*(.*)$",
)
PREDECESSOR_DECLARATION = re.compile(
    r"^\*\*Predecessor disposition model:\*\*\s*(.*)$",
)
SUPPORTED_LIFECYCLE_DECLARATION = "immutable-transition-v1"
SUPPORTED_PREDECESSOR_DECLARATION = "failed-publication-predecessor-v1"
PREDECESSOR_DISPOSITION_KEYS = (
    "SCHEMA", "SUCCESSOR_VERSION", "PREDECESSOR_VERSION",
    "CANONICAL_REPOSITORY", "PREDECESSOR_REF", "TAG_OBJECT",
    "TAG_OBJECT_TYPE", "DIRECT_TARGET", "DIRECT_TARGET_DECLARED_TYPE",
    "DIRECT_TARGET_ACTUAL_TYPE", "SUCCESSOR_BASE", "ORIGINAL_TAG_CI_RUN",
    "ORIGINAL_TAG_CI_ATTEMPT", "ORIGINAL_TAG_CI_CONCLUSION",
    "TAG_CERTIFICATE", "GITHUB_RELEASE_OBSERVATION",
    "GITHUB_RELEASE_OBSERVED_AT_UTC", "PUBLICATION_AUTHORITY",
    "TERMINAL_CLOSURE", "PREDECESSOR_FINAL_PHASE",
    "DIAGNOSIS_ARCHIVE_BYTES", "DIAGNOSIS_ARCHIVE_SHA256",
    "DIAGNOSIS_MANIFEST_SHA256",
)
PREDECESSOR_DISPOSITION_MAX_BYTES = 65536
TERMINAL_DISPOSITION_MAX_BYTES = 8192
TERMINAL_EXPECTATION_KEYS = (
    "SCHEMA", "REFERENCE_PATH", "REFERENCE_MAX_BYTES",
    "EXPECTED_REFERENCE_SCHEMA", "EXPECTED_SOURCE_VERSION",
    "EXPECTED_CANONICAL_REPOSITORY", "EXPECTED_OWNER_DISPOSITION_PHASE",
    "EXPECTED_OWNER_DISPOSITION_STATUS", "EXPECTED_OWNER_DISPOSITION_AT_UTC",
    "EXPECTED_SOURCE_SCOPE", "EXPECTED_ACCEPTED_C", "EXPECTED_ACCEPTED_C_TREE",
    "EXPECTED_PUBLISHED_M17", "EXPECTED_ANNOTATED_TAG_A17",
    "EXPECTED_TAG_DIRECT_TARGET", "EXPECTED_GITHUB_RELEASE_ID",
    "EXPECTED_PHASE9M_MANIFEST_SHA256", "EXPECTED_PHASE9M_OWNER_RECORD_SHA256",
    "EXPECTED_ORIGINAL_ATTEMPT_ONLY_ALL_SUCCESS_CERTIFICATE",
    "EXPECTED_RETROSPECTIVE_PROCEDURAL_EXCEPTION",
    "EXPECTED_PRIVATE_EVIDENCE_LOCATION",
    "EXPECTED_TRACKED_SERIALIZATION_AUTHORITY", "EXPECTED_VALIDATOR_EFFECT",
)
TERMINAL_REFERENCE_KEYS = (
    "SCHEMA", "SOURCE_VERSION", "CANONICAL_REPOSITORY",
    "OWNER_DISPOSITION_PHASE", "OWNER_DISPOSITION_STATUS",
    "OWNER_DISPOSITION_AT_UTC", "SOURCE_SCOPE", "ACCEPTED_C",
    "ACCEPTED_C_TREE", "PUBLISHED_M17", "ANNOTATED_TAG_A17",
    "TAG_DIRECT_TARGET", "GITHUB_RELEASE_ID", "PHASE9M_MANIFEST_SHA256",
    "PHASE9M_OWNER_RECORD_SHA256",
    "ORIGINAL_ATTEMPT_ONLY_ALL_SUCCESS_CERTIFICATE",
    "RETROSPECTIVE_PROCEDURAL_EXCEPTION", "PRIVATE_EVIDENCE_LOCATION",
    "TRACKED_SERIALIZATION_AUTHORITY", "VALIDATOR_EFFECT",
)
SECURITY_PROGRESSION_MAX_BYTES = 16384
SECURITY_PROGRESSION_V1_EXPECTATION_KEYS = (
    "SCHEMA", "REFERENCE_PATH", "REFERENCE_MAX_BYTES",
    "EXPECTED_REFERENCE_SHA256", "EXPECTED_REFERENCE_SCHEMA",
    "EXPECTED_SOURCE_VERSION", "EXPECTED_CANONICAL_REPOSITORY",
    "EXPECTED_REPOSITORY_ID", "EXPECTED_BRANCH_REF", "EXPECTED_BASE_REF",
    "EXPECTED_PR_NUMBER", "EXPECTED_BASE_C", "EXPECTED_BASE_C_TREE",
    "EXPECTED_HISTORICAL_H1", "EXPECTED_HISTORICAL_H1_PARENT",
    "EXPECTED_HISTORICAL_H1_TREE", "EXPECTED_OLD_TOOLCHAIN",
    "EXPECTED_NEW_TOOLCHAIN", "EXPECTED_LANGUAGE_FLOOR", "EXPECTED_PURPOSE",
    "EXPECTED_DELTA_SCHEMA", "EXPECTED_DELTA_COUNT", "EXPECTED_STATUS",
    "EXPECTED_VALIDATOR_EFFECT", "EXPECTED_OWNER_AUTHORITY",
    "EXPECTED_SECURITY_RESULT", "EXPECTED_PROVENANCE",
)
SECURITY_PROGRESSION_V1_REFERENCE_KEYS = (
    "SCHEMA", "SOURCE_VERSION", "CANONICAL_REPOSITORY", "REPOSITORY_ID",
    "BRANCH_REF", "BASE_REF", "PR_NUMBER", "BASE_C", "BASE_C_TREE",
    "HISTORICAL_H1", "HISTORICAL_H1_PARENT", "HISTORICAL_H1_TREE",
    "OLD_TOOLCHAIN", "NEW_TOOLCHAIN", "LANGUAGE_FLOOR", "PURPOSE",
    "DELTA_SCHEMA", "DELTA_COUNT",
)
SECURITY_PROGRESSION_V2_EXPECTATION_KEYS = (
    "SCHEMA", "SELECTION_SCHEMA", "REFERENCE_PATH", "REFERENCE_MAX_BYTES",
    "EXPECTED_REFERENCE_SHA256", "EXPECTED_REFERENCE_SCHEMA",
    "EXPECTED_SOURCE_VERSION", "EXPECTED_CANONICAL_REPOSITORY",
    "EXPECTED_REPOSITORY_ID", "EXPECTED_BRANCH_REF", "EXPECTED_BASE_REF",
    "EXPECTED_PR_NUMBER", "EXPECTED_BASE_C", "EXPECTED_BASE_C_TREE",
    "EXPECTED_HISTORICAL_H1", "EXPECTED_HISTORICAL_H1_PARENT",
    "EXPECTED_HISTORICAL_H1_TREE", "EXPECTED_PUBLISHED_H2",
    "EXPECTED_PUBLISHED_H2_PARENT", "EXPECTED_PUBLISHED_H2_TREE",
    "EXPECTED_PUBLISHED_H2_CI_STATUS", "EXPECTED_SUCCESSOR_PARENT",
    "EXPECTED_OLD_TOOLCHAIN", "EXPECTED_NEW_TOOLCHAIN",
    "EXPECTED_LANGUAGE_FLOOR", "EXPECTED_PURPOSE", "EXPECTED_DELTA_SCHEMA",
    "EXPECTED_DELTA_COUNT", "EXPECTED_STATUS", "EXPECTED_VALIDATOR_EFFECT",
    "EXPECTED_OWNER_AUTHORITY", "EXPECTED_SECURITY_RESULT",
    "EXPECTED_PROVENANCE",
)
SECURITY_PROGRESSION_V2_REFERENCE_KEYS = (
    "SCHEMA", "SOURCE_VERSION", "CANONICAL_REPOSITORY", "REPOSITORY_ID",
    "BRANCH_REF", "BASE_REF", "PR_NUMBER", "BASE_C", "BASE_C_TREE",
    "HISTORICAL_H1", "HISTORICAL_H1_PARENT", "HISTORICAL_H1_TREE",
    "PUBLISHED_H2", "PUBLISHED_H2_PARENT", "PUBLISHED_H2_TREE",
    "PUBLISHED_H2_CI_STATUS", "SUCCESSOR_PARENT", "OLD_TOOLCHAIN",
    "NEW_TOOLCHAIN", "LANGUAGE_FLOOR", "PURPOSE", "DELTA_SCHEMA",
    "DELTA_COUNT",
)
SECURITY_PROGRESSION_V3_EXPECTATION_KEYS = (
    "SCHEMA", "SELECTION_SCHEMA", "REFERENCE_PATH", "REFERENCE_MAX_BYTES",
    "EXPECTED_REFERENCE_SHA256", "EXPECTED_REFERENCE_SCHEMA",
    "EXPECTED_SOURCE_VERSION", "EXPECTED_CANONICAL_REPOSITORY",
    "EXPECTED_REPOSITORY_ID", "EXPECTED_BRANCH_REF", "EXPECTED_BASE_REF",
    "EXPECTED_PR_NUMBER", "EXPECTED_BASE_C", "EXPECTED_BASE_C_TREE",
    "EXPECTED_HISTORICAL_H1", "EXPECTED_HISTORICAL_H1_PARENT",
    "EXPECTED_HISTORICAL_H1_TREE", "EXPECTED_PUBLISHED_H2",
    "EXPECTED_PUBLISHED_H2_PARENT", "EXPECTED_PUBLISHED_H2_TREE",
    "EXPECTED_PUBLISHED_H2_CI_STATUS", "EXPECTED_PUBLISHED_H3",
    "EXPECTED_PUBLISHED_H3_PARENT", "EXPECTED_PUBLISHED_H3_TREE",
    "EXPECTED_PUBLISHED_H3_CI_STATUS", "EXPECTED_SUCCESSOR_PARENT",
    "EXPECTED_SUCCESSOR_AUTHOR_NAME", "EXPECTED_SUCCESSOR_AUTHOR_EMAIL",
    "EXPECTED_SUCCESSOR_AUTHOR_DATE", "EXPECTED_SUCCESSOR_COMMITTER_NAME",
    "EXPECTED_SUCCESSOR_COMMITTER_EMAIL", "EXPECTED_SUCCESSOR_COMMITTER_DATE",
    "EXPECTED_SUCCESSOR_SUBJECT",
    "EXPECTED_OLD_TOOLCHAIN", "EXPECTED_NEW_TOOLCHAIN",
    "EXPECTED_LANGUAGE_FLOOR", "EXPECTED_PURPOSE", "EXPECTED_DELTA_SCHEMA",
    "EXPECTED_DELTA_COUNT", "EXPECTED_STATUS", "EXPECTED_VALIDATOR_EFFECT",
    "EXPECTED_OWNER_AUTHORITY", "EXPECTED_SECURITY_RESULT",
    "EXPECTED_PROVENANCE",
)
SECURITY_PROGRESSION_V3_REFERENCE_KEYS = (
    "SCHEMA", "SOURCE_VERSION", "CANONICAL_REPOSITORY", "REPOSITORY_ID",
    "BRANCH_REF", "BASE_REF", "PR_NUMBER", "BASE_C", "BASE_C_TREE",
    "HISTORICAL_H1", "HISTORICAL_H1_PARENT", "HISTORICAL_H1_TREE",
    "PUBLISHED_H2", "PUBLISHED_H2_PARENT", "PUBLISHED_H2_TREE",
    "PUBLISHED_H2_CI_STATUS", "PUBLISHED_H3", "PUBLISHED_H3_PARENT",
    "PUBLISHED_H3_TREE", "PUBLISHED_H3_CI_STATUS", "SUCCESSOR_PARENT",
    "SUCCESSOR_AUTHOR_NAME", "SUCCESSOR_AUTHOR_EMAIL", "SUCCESSOR_AUTHOR_DATE",
    "SUCCESSOR_COMMITTER_NAME", "SUCCESSOR_COMMITTER_EMAIL",
    "SUCCESSOR_COMMITTER_DATE", "SUCCESSOR_SUBJECT",
    "OLD_TOOLCHAIN", "NEW_TOOLCHAIN", "LANGUAGE_FLOOR", "PURPOSE",
    "DELTA_SCHEMA", "DELTA_COUNT",
)
SECURITY_PROGRESSION_V4_EXPECTATION_KEYS = (
    "SCHEMA", "SELECTION_SCHEMA", "REFERENCE_PATH", "REFERENCE_MAX_BYTES",
    "EXPECTED_REFERENCE_SHA256", "EXPECTED_REFERENCE_SCHEMA",
    "EXPECTED_SOURCE_VERSION", "EXPECTED_CANONICAL_REPOSITORY",
    "EXPECTED_REPOSITORY_ID", "EXPECTED_BRANCH_REF", "EXPECTED_BASE_REF",
    "EXPECTED_PR_NUMBER", "EXPECTED_BASE_C", "EXPECTED_BASE_C_TREE",
    "EXPECTED_HISTORICAL_H1", "EXPECTED_HISTORICAL_H1_PARENT",
    "EXPECTED_HISTORICAL_H1_TREE", "EXPECTED_PUBLISHED_H2",
    "EXPECTED_PUBLISHED_H2_PARENT", "EXPECTED_PUBLISHED_H2_TREE",
    "EXPECTED_PUBLISHED_H2_CI_STATUS", "EXPECTED_PUBLISHED_H3",
    "EXPECTED_PUBLISHED_H3_PARENT", "EXPECTED_PUBLISHED_H3_TREE",
    "EXPECTED_PUBLISHED_H3_CI_STATUS", "EXPECTED_PUBLISHED_H4",
    "EXPECTED_PUBLISHED_H4_PARENT", "EXPECTED_PUBLISHED_H4_TREE",
    "EXPECTED_PUBLISHED_H4_CI_STATUS", "EXPECTED_SUCCESSOR_PARENT",
    "EXPECTED_SUCCESSOR_AUTHOR_NAME", "EXPECTED_SUCCESSOR_AUTHOR_EMAIL",
    "EXPECTED_SUCCESSOR_AUTHOR_DATE", "EXPECTED_SUCCESSOR_COMMITTER_NAME",
    "EXPECTED_SUCCESSOR_COMMITTER_EMAIL", "EXPECTED_SUCCESSOR_COMMITTER_DATE",
    "EXPECTED_SUCCESSOR_SUBJECT",
    "EXPECTED_OLD_TOOLCHAIN", "EXPECTED_NEW_TOOLCHAIN",
    "EXPECTED_LANGUAGE_FLOOR", "EXPECTED_PURPOSE", "EXPECTED_DELTA_SCHEMA",
    "EXPECTED_DELTA_COUNT", "EXPECTED_STATUS", "EXPECTED_VALIDATOR_EFFECT",
    "EXPECTED_OWNER_AUTHORITY", "EXPECTED_SECURITY_RESULT",
    "EXPECTED_PROVENANCE",
)
SECURITY_PROGRESSION_V4_REFERENCE_KEYS = (
    "SCHEMA", "SOURCE_VERSION", "CANONICAL_REPOSITORY", "REPOSITORY_ID",
    "BRANCH_REF", "BASE_REF", "PR_NUMBER", "BASE_C", "BASE_C_TREE",
    "HISTORICAL_H1", "HISTORICAL_H1_PARENT", "HISTORICAL_H1_TREE",
    "PUBLISHED_H2", "PUBLISHED_H2_PARENT", "PUBLISHED_H2_TREE",
    "PUBLISHED_H2_CI_STATUS", "PUBLISHED_H3", "PUBLISHED_H3_PARENT",
    "PUBLISHED_H3_TREE", "PUBLISHED_H3_CI_STATUS", "PUBLISHED_H4",
    "PUBLISHED_H4_PARENT", "PUBLISHED_H4_TREE", "PUBLISHED_H4_CI_STATUS",
    "SUCCESSOR_PARENT", "SUCCESSOR_AUTHOR_NAME", "SUCCESSOR_AUTHOR_EMAIL",
    "SUCCESSOR_AUTHOR_DATE", "SUCCESSOR_COMMITTER_NAME",
    "SUCCESSOR_COMMITTER_EMAIL", "SUCCESSOR_COMMITTER_DATE",
    "SUCCESSOR_SUBJECT", "OLD_TOOLCHAIN", "NEW_TOOLCHAIN", "LANGUAGE_FLOOR",
    "PURPOSE", "DELTA_SCHEMA", "DELTA_COUNT",
)
SECURITY_PROGRESSION_V5_EXPECTATION_KEYS = (
    "SCHEMA", "SELECTION_SCHEMA", "REFERENCE_PATH", "REFERENCE_MAX_BYTES",
    "EXPECTED_REFERENCE_SHA256", "EXPECTED_REFERENCE_SCHEMA",
    "EXPECTED_SOURCE_VERSION", "EXPECTED_CANONICAL_REPOSITORY",
    "EXPECTED_REPOSITORY_ID", "EXPECTED_BRANCH_REF", "EXPECTED_BASE_REF",
    "EXPECTED_PR_NUMBER", "EXPECTED_BASE_C", "EXPECTED_BASE_C_TREE",
    "EXPECTED_HISTORICAL_H1", "EXPECTED_HISTORICAL_H1_PARENT",
    "EXPECTED_HISTORICAL_H1_TREE", "EXPECTED_PUBLISHED_H2",
    "EXPECTED_PUBLISHED_H2_PARENT", "EXPECTED_PUBLISHED_H2_TREE",
    "EXPECTED_PUBLISHED_H2_CI_STATUS", "EXPECTED_PUBLISHED_H3",
    "EXPECTED_PUBLISHED_H3_PARENT", "EXPECTED_PUBLISHED_H3_TREE",
    "EXPECTED_PUBLISHED_H3_CI_STATUS", "EXPECTED_PUBLISHED_H4",
    "EXPECTED_PUBLISHED_H4_PARENT", "EXPECTED_PUBLISHED_H4_TREE",
    "EXPECTED_PUBLISHED_H4_CI_STATUS", "EXPECTED_PUBLISHED_H5",
    "EXPECTED_PUBLISHED_H5_PARENT", "EXPECTED_PUBLISHED_H5_TREE",
    "EXPECTED_PUBLISHED_H5_CI_STATUS", "EXPECTED_SUCCESSOR_PARENT",
    "EXPECTED_SUCCESSOR_AUTHOR_NAME", "EXPECTED_SUCCESSOR_AUTHOR_EMAIL",
    "EXPECTED_SUCCESSOR_AUTHOR_DATE", "EXPECTED_SUCCESSOR_COMMITTER_NAME",
    "EXPECTED_SUCCESSOR_COMMITTER_EMAIL", "EXPECTED_SUCCESSOR_COMMITTER_DATE",
    "EXPECTED_SUCCESSOR_SUBJECT",
    "EXPECTED_OLD_TOOLCHAIN", "EXPECTED_NEW_TOOLCHAIN",
    "EXPECTED_LANGUAGE_FLOOR", "EXPECTED_PURPOSE", "EXPECTED_DELTA_SCHEMA",
    "EXPECTED_DELTA_COUNT", "EXPECTED_STATUS", "EXPECTED_VALIDATOR_EFFECT",
    "EXPECTED_OWNER_AUTHORITY", "EXPECTED_SECURITY_RESULT",
    "EXPECTED_PROVENANCE",
)
SECURITY_PROGRESSION_V5_REFERENCE_KEYS = (
    "SCHEMA", "SOURCE_VERSION", "CANONICAL_REPOSITORY", "REPOSITORY_ID",
    "BRANCH_REF", "BASE_REF", "PR_NUMBER", "BASE_C", "BASE_C_TREE",
    "HISTORICAL_H1", "HISTORICAL_H1_PARENT", "HISTORICAL_H1_TREE",
    "PUBLISHED_H2", "PUBLISHED_H2_PARENT", "PUBLISHED_H2_TREE",
    "PUBLISHED_H2_CI_STATUS", "PUBLISHED_H3", "PUBLISHED_H3_PARENT",
    "PUBLISHED_H3_TREE", "PUBLISHED_H3_CI_STATUS", "PUBLISHED_H4",
    "PUBLISHED_H4_PARENT", "PUBLISHED_H4_TREE", "PUBLISHED_H4_CI_STATUS",
    "PUBLISHED_H5", "PUBLISHED_H5_PARENT", "PUBLISHED_H5_TREE",
    "PUBLISHED_H5_CI_STATUS", "SUCCESSOR_PARENT", "SUCCESSOR_AUTHOR_NAME",
    "SUCCESSOR_AUTHOR_EMAIL", "SUCCESSOR_AUTHOR_DATE",
    "SUCCESSOR_COMMITTER_NAME", "SUCCESSOR_COMMITTER_EMAIL",
    "SUCCESSOR_COMMITTER_DATE", "SUCCESSOR_SUBJECT", "OLD_TOOLCHAIN",
    "NEW_TOOLCHAIN", "LANGUAGE_FLOOR", "PURPOSE", "DELTA_SCHEMA",
    "DELTA_COUNT",
)
SECURITY_PROGRESSION_TRAILER_KEYS = (
    "STATUS", "VALIDATOR_EFFECT", "OWNER_AUTHORITY", "SECURITY_RESULT",
    "PROVENANCE",
)
FINAL_SNAPSHOT_MAX_BYTES = 32768
FINAL_SNAPSHOT_EXPECTATION_KEYS = (
    "SCHEMA", "SELECTION_SCHEMA", "REFERENCE_PATH", "REFERENCE_MAX_BYTES",
    "EXPECTED_REFERENCE_SHA256", "EXPECTED_REFERENCE_SCHEMA",
    "EXPECTED_SOURCE_VERSION", "EXPECTED_CANONICAL_REPOSITORY",
    "EXPECTED_REPOSITORY_ID", "EXPECTED_BRANCH_REF", "EXPECTED_BASE_REF",
    "EXPECTED_TAG_REF", "EXPECTED_BASE_M", "EXPECTED_BASE_M_TREE",
    "EXPECTED_BASE_M_PARENTS", "EXPECTED_LANGUAGE_FLOOR",
    "EXPECTED_TOOLCHAIN", "EXPECTED_PUBLISHED_M17",
    "EXPECTED_ANNOTATED_TAG_A17", "EXPECTED_GITHUB_RELEASE_ID",
    "EXPECTED_TS_ACCEPTANCE_STATUS", "EXPECTED_TS_ACCEPTANCE_SHA256",
    "EXPECTED_ORIGINAL_ATTEMPT_ONLY_ALL_SUCCESS_CERTIFICATE",
    "EXPECTED_RETROSPECTIVE_PROCEDURAL_EXCEPTION",
    "EXPECTED_DELTA_SCHEMA", "EXPECTED_DELTA_COUNT", "EXPECTED_STATUS",
    "EXPECTED_VALIDATOR_EFFECT",
)
FINAL_SNAPSHOT_REFERENCE_KEYS = (
    "SCHEMA", "SOURCE_VERSION", "CANONICAL_REPOSITORY", "REPOSITORY_ID",
    "BRANCH_REF", "BASE_REF", "TAG_REF", "BASE_M", "BASE_M_TREE",
    "BASE_M_PARENTS", "LANGUAGE_FLOOR", "TOOLCHAIN", "PUBLISHED_M17",
    "ANNOTATED_TAG_A17", "GITHUB_RELEASE_ID", "TS_ACCEPTANCE_STATUS",
    "TS_ACCEPTANCE_SHA256",
    "ORIGINAL_ATTEMPT_ONLY_ALL_SUCCESS_CERTIFICATE",
    "RETROSPECTIVE_PROCEDURAL_EXCEPTION", "DELTA_SCHEMA", "DELTA_COUNT",
    "STATUS",
)
FINAL_SNAPSHOT_DELTA = (
    ("M", "100644", ".devcontainer/Dockerfile"),
    ("M", "100755", ".devcontainer/post-create.sh"),
    ("M", "100644", ".github/copilot-instructions.md"),
    ("M", "100644", ".github/instructions/ci.instructions.md"),
    ("M", "100644", ".github/prompts/critical-path-coverage.prompt.md"),
    ("M", "100644", ".github/prompts/regression-fix.prompt.md"),
    ("M", "100644", ".github/workflows/benchmark-baseline.yml"),
    ("M", "100644", ".github/workflows/ci.yml"),
    ("M", "100644", ".github/workflows/codeql.yml"),
    ("M", "100644", "AGENTS.md"),
    ("M", "100644", "CHANGELOG.md"),
    ("M", "100644", "CONTRIBUTING.md"),
    ("M", "100644", "Dockerfile"),
    ("M", "100644", "PRE_RELEASE_CHECKLIST.md"),
    ("M", "100644", "README.md"),
    ("M", "100644", "SECURITY.md"),
    ("M", "100644", "docs/release/v1.13/README.md"),
    ("M", "100644", "docs/release/v1.13/v1.13.x-release-train.md"),
    ("M", "100644", "go.mod"),
    ("M", "100644", "internal/version/version.go"),
    ("M", "100644", "internal/version/version_test.go"),
    ("M", "100755", "scripts/audit_ci_enforcement.sh"),
    ("M", "100644", "scripts/audit_ci_enforcement_test.go"),
    ("M", "100644", "scripts/release_state_support.py"),
    ("M", "100644", "scripts/test_validate_governance.py"),
    ("M", "100644", "scripts/test_validate_release_state.py"),
    ("M", "100755", "scripts/validate_governance.py"),
    ("M", "100755", "scripts/validate_phase3_contracts.py"),
    ("M", "100644", "scripts/validate_release_state.py"),
    ("A", "100644", "docs/release/v1.13/v1.13.18-final-source-snapshot-reference.md"),
    ("A", "100644", "docs/release/v1.13/v1.13.18-phase-list.md"),
    ("A", "100644", "docs/release/v1.13/v1.13.18-release-body.md"),
    ("A", "100644", "docs/release/v1.13/v1.13.18-release-body.sha256"),
    ("A", "100644", "docs/release/v1.13/v1.13.18-release-gate.md"),
    ("A", "100644", "docs/release/v1.13/v1.13.18-release-state-validator-contract.md"),
    ("A", "100644", "docs/release/v1.13/v1.13.18-release-state.md"),
    ("A", "100644", "docs/release/v1.13/v1.13.18-release-train-reconciliation.md"),
    ("A", "100644", "docs/release/v1.13/v1.13.18-scope.md"),
    ("A", "100644", "docs/release/v1.13/v1.13.18-source-test-allowlist.md"),
    ("A", "100644", "docs/release/v1.13/v1.13.18-validation-checklist.md"),
)
SECURITY_PROGRESSION_V2_DELTA = (
    ("M", "100644", "docs/release/v1.13/v1.13.17-release-state-validator-contract.md"),
    ("M", "100644", "docs/release/v1.13/v1.13.17-release-state.md"),
    ("M", "100644", "docs/release/v1.13/v1.13.17-security-toolchain-progression-reference.md"),
    ("M", "100644", "docs/release/v1.13/v1.13.17-source-test-allowlist.md"),
    ("M", "100644", "scripts/release_state_support.py"),
    ("M", "100644", "scripts/test_validate_governance.py"),
    ("M", "100644", "scripts/test_validate_release_state.py"),
    ("M", "100755", "scripts/validate_governance.py"),
    ("M", "100644", "scripts/validate_release_state.py"),
)
SECURITY_PROGRESSION_V3_DELTA = SECURITY_PROGRESSION_V2_DELTA
SECURITY_PROGRESSION_V4_DELTA = SECURITY_PROGRESSION_V2_DELTA
SECURITY_PROGRESSION_V5_DELTA = SECURITY_PROGRESSION_V2_DELTA
GIT_SHA = re.compile(r"^[0-9a-f]{40}$")
NONZERO_GIT_SHA = re.compile(r"^(?!0{40}$)[0-9a-f]{40}$")
ProcessResult = subprocess.CompletedProcess[str]


@dataclass(frozen=True)
class Violation:
    rule: str
    path: str
    line: int
    message: str


@dataclass
class ValidationResult:
    state: Optional[str]
    active_version: Optional[str]
    violations: list[Violation] = field(default_factory=list)
    artifact_state: Optional[str] = None
    authorization_status: str = "NOT_EVALUATED_BY_VALIDATOR"
    certification_status: str = "PENDING_EXTERNAL_EVIDENCE"
    outstanding_obligations: list[str] = field(default_factory=list)
    evidence_scope: str = "UNAVAILABLE"
    security_progression: str = "NOT_SELECTED"
    security_result: str = "NOT_EVALUATED_BY_VALIDATOR"
    diagnostic_schema: str = "coldkeep-release-state-diagnostic/v3"
    progression_contract_schema: Optional[str] = None
    final_snapshot: str = "NOT_SELECTED"
    final_snapshot_contract_schema: Optional[str] = None

    def add(self, rule: str, path: str, line: int, message: str) -> None:
        self.violations.append(Violation(rule, path, line, message))

    def ordered(self) -> list[Violation]:
        return sorted(
            self.violations,
            key=lambda item: (item.rule, item.path, item.line, item.message),
        )


class InternalError(Exception):
    def __init__(self, kind: str, message: str) -> None:
        """Create an internal error with a stable machine-readable kind."""
        super().__init__(message)
        self.kind = kind
        self.message = message


DIAGNOSTIC_SCHEMA = "coldkeep-release-state-diagnostic/v3"
DIAGNOSTIC_SCHEMA_V4 = "coldkeep-release-state-diagnostic/v4"
DIAGNOSTIC_SCHEMA_V5 = "coldkeep-release-state-diagnostic/v5"
DIAGNOSTIC_RESULTS = {"PASS", "FAIL", "NOT_EVALUATED", "UNAVAILABLE"}
PR_PREDICATES = (
    ("pr.env.actions", "environment", "GITHUB_ACTIONS equals true"),
    ("pr.env.event", "environment", "GITHUB_EVENT_NAME equals pull_request"),
    ("pr.env.full_ref", "environment", "GITHUB_REF is refs/pull/<positive>/merge"),
    ("pr.env.short_ref", "environment", "GITHUB_REF_NAME equals the full-ref suffix"),
    ("pr.env.head_ref", "environment", "GITHUB_HEAD_REF equals the active release branch"),
    ("pr.env.base_ref", "environment", "GITHUB_BASE_REF equals main"),
    ("pr.env.repository", "environment", "GITHUB_REPOSITORY equals the canonical repository"),
    ("pr.env.runtime_sha", "environment", "GITHUB_SHA equals the actual checkout commit"),
    ("pr.env.event_path", "environment", "GITHUB_EVENT_PATH is present"),
    ("pr.event.read", "event-file", "the selected event file is readable"),
    ("pr.event.utf8", "event-file", "the consumed event bytes decode as UTF-8"),
    ("pr.event.json", "event-file", "the consumed event text parses as JSON"),
    ("pr.event.object", "event-file", "the parsed event is an object"),
    ("pr.payload.pull_request", "event-payload", "pull_request is an object"),
    ("pr.payload.number", "event-payload", "number is a positive integer"),
    ("pr.payload.number_ref", "event-and-environment", "the PR number agrees with GITHUB_REF"),
    ("pr.payload.repository", "event-payload", "the payload repository is canonical"),
    ("pr.payload.base_object", "event-payload", "pull_request.base is an object"),
    ("pr.payload.head_object", "event-payload", "pull_request.head is an object"),
    ("pr.payload.base_ref", "event-payload", "the payload base ref equals main"),
    ("pr.payload.head_ref", "event-payload", "the payload head ref equals the active release branch"),
    ("pr.payload.base_repository", "event-payload", "the payload base repository is canonical"),
    ("pr.payload.head_repository", "event-payload", "the payload head repository is canonical"),
    ("pr.payload.base_sha", "event-payload", "the payload base SHA is canonical"),
    ("pr.payload.head_sha", "event-payload", "the payload head SHA is canonical"),
    ("pr.git.base_object", "git-object", "the payload base commit object is available"),
    ("pr.git.head_object", "git-object", "the payload head commit object is available"),
    ("pr.route", "runtime-and-payload", "the checkout is classified as direct-head or synthetic"),
    ("pr.payload.merge_member", "event-payload", "synthetic checkout has a merge_commit_sha member"),
    ("pr.payload.merge_value", "event-payload", "merge_commit_sha is null or a canonical nonzero full lowercase SHA and is advisory"),
    ("pr.git.parents", "git-object", "synthetic checkout parents equal ordered base then head"),
    ("pr.git.tree", "git-object", "synthetic checkout tree equals the payload-head tree"),
)
RELEASE_PUSH_PREDICATES = (
    ("release_push.event.read", "event-file", "the selected event file is readable"),
    ("release_push.event.utf8", "event-file", "the consumed event bytes decode as UTF-8"),
    ("release_push.event.json", "event-file", "the consumed event text parses as JSON"),
    ("release_push.event.object", "event-file", "the parsed event is an object"),
    ("release_push.context", "environment-and-event", "the release-push context compound predicate matches"),
)
TAG_PREDICATES = (
    ("tag.env.actions", "environment", "GITHUB_ACTIONS equals true"),
    ("tag.env.event", "environment", "GITHUB_EVENT_NAME equals push"),
    ("tag.env.full_ref", "environment", "GITHUB_REF equals the active version tag ref"),
    ("tag.env.short_ref", "environment", "GITHUB_REF_NAME equals the active version tag"),
    ("tag.env.ref_type", "environment", "GITHUB_REF_TYPE equals tag"),
    ("tag.env.repository", "environment", "GITHUB_REPOSITORY equals the canonical repository"),
    ("tag.env.runtime_sha_format", "environment", "GITHUB_SHA is a canonical full lowercase SHA"),
    ("tag.env.event_path", "environment", "GITHUB_EVENT_PATH is present"),
    ("tag.event.read", "event-file", "the selected event file is readable"),
    ("tag.event.size", "event-file", "the selected event is at most 1048576 bytes"),
    ("tag.event.utf8", "event-file", "the consumed event bytes decode as UTF-8"),
    ("tag.event.json", "event-file", "the consumed event text parses as JSON"),
    ("tag.event.object", "event-file", "the parsed event is an object"),
    ("tag.payload.ref", "event-payload", "event ref equals the active version tag ref"),
    ("tag.payload.before", "event-payload", "event before is the all-zero creation value"),
    ("tag.payload.after_format", "event-payload", "event after is a canonical full lowercase SHA"),
    ("tag.payload.created", "event-payload", "event created is boolean true"),
    ("tag.payload.deleted", "event-payload", "event deleted is boolean false"),
    ("tag.payload.forced", "event-payload", "event forced is boolean false"),
    ("tag.payload.repository", "event-payload", "event repository is canonical"),
    ("tag.git.not_shallow", "git-administration", "the repository is not shallow"),
    ("tag.git.no_alternates", "git-administration", "no object alternates are configured"),
    ("tag.git.no_grafts", "git-administration", "no grafts file is present"),
    ("tag.git.no_replacements", "git-administration", "no replacement refs or namespace are active"),
    ("tag.git.named_ref", "git-object", "the exact named tag ref resolves to one canonical object"),
    ("tag.git.named_ref_type", "git-object", "the exact named object has raw type tag"),
    ("tag.git.direct_target", "git-object", "the tag has exactly one direct object target"),
    ("tag.git.direct_target_declared_type", "git-object", "the tag declares direct target type commit"),
    ("tag.git.direct_target_actual_type", "git-object", "the direct target actual type is commit"),
    ("tag.git.target_equals_head", "git-object", "the direct target equals checkout HEAD"),
    ("tag.identity.runtime_sha", "environment-and-git", "runtime SHA is exact A or M and resolves to M"),
    ("tag.identity.event_after", "event-and-git", "event after is exact A or M and resolves to M"),
    ("tag.topology.parents_and_ancestry", "git-object", "HEAD is a normal ordered two-parent merge with first parent ancestral to second"),
    ("tag.topology.tree", "git-object", "HEAD tree equals the second-parent candidate tree"),
)


def _type_name(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return "other"


def _bounded_identity(
    value: object,
    *,
    category: str,
    allow: Optional[re.Pattern[str]] = None,
    expected: Optional[str] = None,
    allow_null: bool = False,
    allow_positive_integer: bool = False,
) -> dict[str, object]:
    """Return one fixed-shape, shareable representation of consumed input."""
    if value is None and allow_null:
        return {"category": "validated-null", "type": "null", "length": None, "sha256": None, "value": None}
    if allow_positive_integer and isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return {"category": "validated", "type": "integer", "length": None, "sha256": None, "value": value}
    if isinstance(value, str) and len(value) <= 256 and (
        (expected is not None and value == expected)
        or (allow is not None and allow.fullmatch(value) is not None)
    ):
        return {"category": "validated", "type": "string", "length": len(value), "sha256": None, "value": value}
    value_type = _type_name(value)
    if isinstance(value, str):
        encoded = value.encode("utf-8", errors="surrogatepass")
        length: Optional[int] = len(value)
    else:
        encoded = value_type.encode("ascii")
        length = len(value) if isinstance(value, (list, dict)) else None
    return {
        "category": category,
        "type": value_type,
        "length": length,
        "sha256": hashlib.sha256(encoded).hexdigest(),
        "value": None,
    }


def _unavailable_identity(category: str = "unavailable") -> dict[str, object]:
    return {"category": category, "type": "unavailable", "length": None, "sha256": None, "value": None}


class DiagnosticRecorder:
    """Best-effort, closed-schema tracing for the actual validation path."""

    _SOURCE_PATHS = (
        "scripts/release_state_support.py",
        "scripts/validate_release_state.py",
        "docs/release/v1.13/v1.13.17-release-state.md",
        "docs/release/v1.13/v1.13.17-release-state-validator-contract.md",
        "docs/release/v1.13/v1.13.17-security-toolchain-progression-reference.md",
    )
    _FINAL_SNAPSHOT_SOURCE_PATHS = (
        "scripts/release_state_support.py",
        "scripts/validate_release_state.py",
        "docs/release/v1.13/v1.13.18-release-state.md",
        "docs/release/v1.13/v1.13.18-release-state-validator-contract.md",
        "docs/release/v1.13/v1.13.18-final-source-snapshot-reference.md",
    )

    def __init__(self, requested_state: str, environment: dict[str, str]) -> None:
        self.requested_state = requested_state
        self.environment = dict(environment)
        self.event_snapshots: list[dict[str, object]] = []
        self.evaluations: list[dict[str, object]] = []
        self.incomplete_reasons: list[str] = ["source-attribution-unavailable"]
        self.root: Optional[Path] = None
        self.checkout_commit: Optional[str] = None
        self.checkout_tree: Optional[str] = None
        self.source_paths = self._SOURCE_PATHS
        self.source_records: list[dict[str, object]] = [
            {
                "path": relative,
                "loaded_path_match": None,
                "git_blob": None,
                "measured_git_blob": None,
                "measured_sha256": None,
                "matches_git_blob": None,
                "bytes": None,
            }
            for relative in self._SOURCE_PATHS
        ]
        self.source_hashes: dict[str, str] = {}
        self.predicate_lines: dict[str, int] = {}

    def mark_incomplete(self, reason: str) -> None:
        if reason in {
            "source-attribution-unavailable",
            "event-snapshot-unavailable",
            "predicate-recording-unavailable",
            "diagnostic-construction-unavailable",
            "output-size-limit",
            "tag-observation-unavailable",
        } and reason not in self.incomplete_reasons:
            self.incomplete_reasons.append(reason)

    def bind_sources(self, root: Path, validator_path: Path) -> None:
        """Measure loaded implementation bytes once at startup."""
        self.root = root
        try:
            version_source = (root / "internal/version/version.go").read_text(
                encoding="utf-8"
            )
            if re.search(r"(?m)^\s*Patch\s*=\s*18\s*$", version_source):
                self.source_paths = self._FINAL_SNAPSHOT_SOURCE_PATHS
                self.source_records = [
                    {
                        "path": relative,
                        "loaded_path_match": None,
                        "git_blob": None,
                        "measured_git_blob": None,
                        "measured_sha256": None,
                        "matches_git_blob": None,
                        "bytes": None,
                    }
                    for relative in self.source_paths
                ]
            self.checkout_commit = run_git(root, ["rev-parse", "HEAD"]).stdout.strip()
            self.checkout_tree = run_git(root, ["rev-parse", "HEAD^{tree}"]).stdout.strip()
            loaded = {
                "scripts/release_state_support.py": Path(__file__).resolve(),
                "scripts/validate_release_state.py": validator_path.resolve(),
            }
            records = []
            attribution_complete = True
            for relative in self.source_paths:
                expected_path = (root / relative).resolve()
                actual_path = loaded.get(relative, expected_path)
                if not actual_path.is_file() or actual_path.is_symlink():
                    attribution_complete = False
                    records.append({
                        "path": relative,
                        "loaded_path_match": None,
                        "git_blob": None,
                        "measured_git_blob": None,
                        "measured_sha256": None,
                        "matches_git_blob": None,
                        "bytes": None,
                    })
                    continue
                loaded_path_match = actual_path == expected_path
                content = actual_path.read_bytes()
                measured_sha256 = hashlib.sha256(content).hexdigest()
                measured_blob = hashlib.sha1(
                    f"blob {len(content)}\0".encode("ascii") + content
                ).hexdigest()
                git_blob_result = run_git(
                    root,
                    ["rev-parse", f"HEAD:{relative}"],
                    allow_failure=True,
                )
                git_blob = (
                    git_blob_result.stdout.strip()
                    if git_blob_result.returncode == 0
                    and GIT_SHA.fullmatch(git_blob_result.stdout.strip())
                    else None
                )
                matches_git_blob = git_blob == measured_blob if git_blob else None
                if not loaded_path_match or matches_git_blob is not True:
                    attribution_complete = False
                records.append({
                    "path": relative,
                    "loaded_path_match": loaded_path_match,
                    "git_blob": git_blob,
                    "measured_git_blob": measured_blob,
                    "measured_sha256": measured_sha256,
                    "matches_git_blob": matches_git_blob,
                    "bytes": len(content),
                })
                self.source_hashes[relative] = measured_sha256
                if relative == "scripts/release_state_support.py":
                    for number, line in enumerate(content.decode("utf-8").splitlines(), 1):
                        marker = re.search(r"# diagnostic-predicate: ([a-z0-9_.-]+)$", line)
                        if marker:
                            self.predicate_lines[marker.group(1)] = number
            self.source_records = records
            if attribution_complete:
                self.incomplete_reasons = [
                    reason
                    for reason in self.incomplete_reasons
                    if reason != "source-attribution-unavailable"
                ]
        except Exception:
            self.source_records = [
                {
                    "path": relative,
                    "loaded_path_match": None,
                    "git_blob": None,
                    "measured_git_blob": None,
                    "measured_sha256": None,
                    "matches_git_blob": None,
                    "bytes": None,
                }
                for relative in self.source_paths
            ]
            self.mark_incomplete("source-attribution-unavailable")

    def _runtime_context(
        self,
        env: dict[str, str],
        head: str,
        branch: str,
        version: str,
        canonical_repository: str,
        route: str,
        selected_head_branch: Optional[str] = None,
    ) -> dict[str, object]:
        sha_pattern = GIT_SHA
        if route == "tag-push":
            tag_ref = f"refs/tags/v{version}"
            return {
                "github_actions": _bounded_identity(env.get("GITHUB_ACTIONS"), category="unexpected-actions", expected="true"),
                "event_name": _bounded_identity(env.get("GITHUB_EVENT_NAME"), category="unexpected-event", expected="push"),
                "full_ref": _bounded_identity(env.get("GITHUB_REF"), category="unexpected-full-ref", expected=tag_ref),
                "short_ref": _bounded_identity(env.get("GITHUB_REF_NAME"), category="unexpected-short-ref", expected=f"v{version}"),
                "ref_type": _bounded_identity(env.get("GITHUB_REF_TYPE"), category="unexpected-ref-type", expected="tag"),
                "repository": _bounded_identity(env.get("GITHUB_REPOSITORY"), category="unexpected-repository", expected=canonical_repository),
                "runtime_sha": _bounded_identity(env.get("GITHUB_SHA"), category="unexpected-runtime-sha", allow=sha_pattern),
                "actual_checkout_commit": _bounded_identity(head, category="unexpected-checkout", allow=sha_pattern),
                "actual_branch": _bounded_identity(branch, category="unexpected-branch", expected=""),
                "checkout_tree": _unavailable_identity(),
                "selected_ref": _bounded_identity(tag_ref, category="unexpected-tag-ref", expected=tag_ref),
                "tag_object": _unavailable_identity(),
                "tag_object_type": _unavailable_identity(),
                "direct_target": _unavailable_identity(),
                "direct_target_declared_type": _unavailable_identity(),
                "direct_target_actual_type": _unavailable_identity(),
                "runtime_sha_classification": _unavailable_identity(),
                "runtime_sha_resolved_commit": _unavailable_identity(),
                "event_after": _unavailable_identity(),
                "event_after_classification": _unavailable_identity(),
                "event_after_resolved_commit": _unavailable_identity(),
                "isolation": {"shallow": None, "alternates": None, "grafts": None, "replacements": None},
                "checkout_parents": [],
                "first_parent_tree": _unavailable_identity(),
                "second_parent_tree": _unavailable_identity(),
            }
        ref_pattern = re.compile(r"refs/pull/[1-9][0-9]*/merge")
        short_ref_pattern = re.compile(r"[1-9][0-9]*/merge")
        expected_head_branch = selected_head_branch or f"release/v{version}"
        return {
            "github_actions": _bounded_identity(env.get("GITHUB_ACTIONS"), category="unexpected-actions", expected="true"),
            "event_name": _bounded_identity(env.get("GITHUB_EVENT_NAME"), category="unexpected-event", expected="pull_request"),
            "full_ref": _bounded_identity(env.get("GITHUB_REF"), category="unexpected-full-ref", allow=ref_pattern),
            "short_ref": _bounded_identity(env.get("GITHUB_REF_NAME"), category="unexpected-short-ref", allow=short_ref_pattern),
            "head_ref": _bounded_identity(env.get("GITHUB_HEAD_REF"), category="unexpected-head-ref", expected=expected_head_branch),
            "base_ref": _bounded_identity(env.get("GITHUB_BASE_REF"), category="unexpected-base-ref", expected="main"),
            "repository": _bounded_identity(env.get("GITHUB_REPOSITORY"), category="unexpected-repository", expected=canonical_repository),
            "runtime_sha": _bounded_identity(env.get("GITHUB_SHA"), category="unexpected-runtime-sha", allow=sha_pattern),
            "actual_checkout_commit": _bounded_identity(head, category="unexpected-checkout", allow=sha_pattern),
            "actual_branch": _bounded_identity(branch, category="unexpected-branch", expected="") if branch == "" else _bounded_identity(branch, category="unexpected-branch", expected=expected_head_branch),
            "checkout_parents": [],
            "checkout_tree": _unavailable_identity(),
            "payload_head_tree": _unavailable_identity(),
            "object_availability": [],
        }

    def begin_evaluation(
        self,
        site: str,
        env: dict[str, str],
        head: str,
        branch: str,
        version: str,
        canonical_repository: str,
        selected_head_branch: Optional[str] = None,
    ) -> int:
        evaluation_id = len(self.evaluations) + 1
        route = (
            "tag-push" if site.endswith("tag-push") else
            "release-push" if site.endswith("release-push") else
            "release-pr"
        )
        self.evaluations.append({
            "id": evaluation_id,
            "site": site,
            "route": route,
            "sequence": evaluation_id,
            "event_snapshot_id": None,
            "runtime_context": self._runtime_context(
                env, head, branch, version, canonical_repository, route,
                selected_head_branch,
            ),
            "result": "UNAVAILABLE",
            "first_rejecting_predicate": None,
            "predicates": [],
        })
        return evaluation_id

    def predicate(
        self,
        evaluation_id: int,
        predicate_id: str,
        passed: Optional[bool],
        observed: dict[str, object],
    ) -> bool:
        try:
            evaluation = self.evaluations[evaluation_id - 1]
            spec = next(
                item
                for item in (*PR_PREDICATES, *RELEASE_PUSH_PREDICATES, *TAG_PREDICATES)
                if item[0] == predicate_id
            )
            status = "PASS" if passed is True else "FAIL" if passed is False else "UNAVAILABLE"
            evaluation["predicates"].append({
                "id": predicate_id,
                "source_path": "scripts/release_state_support.py",
                "source_sha256": self.source_hashes.get("scripts/release_state_support.py"),
                "source_line": self.predicate_lines.get(predicate_id),
                "input_source": spec[1],
                "required_relation": spec[2],
                "result": status,
                "observed": observed,
            })
            if passed is False and evaluation["first_rejecting_predicate"] is None:
                evaluation["first_rejecting_predicate"] = predicate_id
            return passed is True
        except Exception:
            self.mark_incomplete("predicate-recording-unavailable")
            return passed

    def attach_snapshot(self, evaluation_id: int, snapshot: dict[str, object]) -> None:
        try:
            snapshot_id = len(self.event_snapshots) + 1
            snapshot["id"] = snapshot_id
            self.event_snapshots.append(snapshot)
            self.evaluations[evaluation_id - 1]["event_snapshot_id"] = snapshot_id
        except Exception:
            self.mark_incomplete("event-snapshot-unavailable")

    def context_update(self, evaluation_id: int, **values: object) -> None:
        try:
            context = self.evaluations[evaluation_id - 1]["runtime_context"]
            for key, value in values.items():
                if key in context:
                    context[key] = value
        except Exception:
            self.mark_incomplete("predicate-recording-unavailable")

    def finish_evaluation(self, evaluation_id: int, accepted: bool) -> bool:
        try:
            evaluation = self.evaluations[evaluation_id - 1]
            seen = {item["id"] for item in evaluation["predicates"]}
            specs = (
                TAG_PREDICATES if evaluation["route"] == "tag-push" else
                RELEASE_PUSH_PREDICATES if evaluation["route"] == "release-push" else
                PR_PREDICATES
            )
            for predicate_id, input_source, required_relation in specs:
                if predicate_id in seen:
                    continue
                evaluation["predicates"].append({
                    "id": predicate_id,
                    "source_path": "scripts/release_state_support.py",
                    "source_sha256": self.source_hashes.get("scripts/release_state_support.py"),
                    "source_line": self.predicate_lines.get(predicate_id),
                    "input_source": input_source,
                    "required_relation": required_relation,
                    "result": "NOT_EVALUATED",
                    "observed": _unavailable_identity("not-evaluated"),
                })
            evaluation["result"] = (
                "PASS" if accepted else
                "UNAVAILABLE" if evaluation["first_rejecting_predicate"] is None
                and any(item["result"] == "UNAVAILABLE" for item in evaluation["predicates"])
                else "FAIL"
            )
        except Exception:
            self.mark_incomplete("predicate-recording-unavailable")
        return accepted

    def document(
        self,
        *,
        resolved_state: Optional[str],
        result_status: str,
        exit_code: int,
        rule_codes: list[str],
        security_progression: str,
        security_result: str,
        diagnostic_schema: str = DIAGNOSTIC_SCHEMA,
        progression_contract_schema: Optional[str] = None,
        final_snapshot: str = "NOT_SELECTED",
        final_snapshot_contract_schema: Optional[str] = None,
    ) -> dict[str, object]:
        expected_repository: Optional[str] = None
        for evaluation in self.evaluations:
            runtime_repository = evaluation["runtime_context"]["repository"]
            if runtime_repository.get("category") == "validated":
                expected_repository = runtime_repository.get("value")
                break
        repository = _bounded_identity(
            self.environment.get("GITHUB_REPOSITORY"),
            category="unexpected-repository",
            expected=expected_repository,
        )
        digits = re.compile(r"[0-9]+")
        document = {
            "schema": diagnostic_schema,
            "created_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            "validator": {
                "requested_state": self.requested_state,
                "resolved_state": resolved_state,
                "result": result_status,
                "exit_code": exit_code,
                "rule_codes": sorted(set(rule_codes)),
                "authorization_status": "NOT_EVALUATED_BY_VALIDATOR",
                "certification_status": "PENDING_EXTERNAL_EVIDENCE",
                "checkout_commit": self.checkout_commit,
                "checkout_tree": self.checkout_tree,
                "sources": self.source_records,
                "security_progression": security_progression,
                "security_result": security_result,
            },
            "attribution": {
                "repository": repository,
                "run_id": _bounded_identity(self.environment.get("GITHUB_RUN_ID"), category="unexpected-run-id", allow=digits),
                "run_attempt": _bounded_identity(self.environment.get("GITHUB_RUN_ATTEMPT"), category="unexpected-run-attempt", allow=digits),
                "job": _bounded_identity(self.environment.get("GITHUB_JOB"), category="unexpected-job", expected="quality"),
                "workflow": _bounded_identity(self.environment.get("GITHUB_WORKFLOW"), category="unexpected-workflow", expected="CI"),
                "event_name": _bounded_identity(self.environment.get("GITHUB_EVENT_NAME"), category="unexpected-event", allow=re.compile(r"(?:pull_request|push)")),
            },
            "event_snapshots": self.event_snapshots,
            "evaluations": self.evaluations,
            "capture": {
                "complete": not self.incomplete_reasons,
                "incomplete_reasons": self.incomplete_reasons,
                "same_process": True,
                "network_lookups": 0,
                "whole_event_included": False,
                "whole_environment_included": False,
                "non_authorizing": True,
            },
        }
        if diagnostic_schema == DIAGNOSTIC_SCHEMA_V4:
            document["validator"]["progression_contract_schema"] = progression_contract_schema
        if diagnostic_schema == DIAGNOSTIC_SCHEMA_V5:
            document["validator"]["final_snapshot"] = final_snapshot
            document["validator"]["final_snapshot_contract_schema"] = (
                final_snapshot_contract_schema
            )
        return document

    def write(self, destination: str, document: dict[str, object]) -> None:
        """Publish one fresh mode-0600 diagnostic without replacing any file."""
        target = Path(os.path.abspath(destination))
        parent = target.parent
        if (
            not target.name
            or not parent.is_dir()
            or parent.resolve() != parent
            or target.exists()
            or target.is_symlink()
        ):
            raise OSError("diagnostic destination unavailable")
        protected = {Path(__file__).resolve()}
        if self.root is not None:
            protected.add((self.root / "scripts/validate_release_state.py").resolve())
        event_path = self.environment.get("GITHUB_EVENT_PATH", "")
        if event_path:
            protected.add(Path(event_path).resolve())
        if target.resolve() in protected:
            raise OSError("diagnostic destination unavailable")
        encoded = (json.dumps(document, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
        if len(encoded) > 262144:
            self.mark_incomplete("output-size-limit")
            raise OSError("diagnostic output exceeds size limit")
        temporary_name: Optional[str] = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                dir=parent,
                prefix=f".{target.name}.",
                suffix=".tmp",
                delete=False,
            ) as handle:
                temporary_name = handle.name
                os.chmod(temporary_name, 0o600)
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            os.link(temporary_name, target, follow_symlinks=False)
        finally:
            if temporary_name:
                try:
                    Path(temporary_name).unlink()
                except FileNotFoundError:
                    pass


@dataclass
class Document:
    root: Path
    path: str
    lines: list[str]

    @classmethod
    def load(cls, root: Path, path: str) -> "Document":
        target = root / path
        if not target.is_file():
            raise FileNotFoundError(path)
        return cls(root, path, target.read_text(encoding="utf-8").splitlines())

    def section(self, heading: str) -> Optional[tuple[int, list[str]]]:
        marker = next(
            (index for index, line in enumerate(self.lines) if line == heading),
            None,
        )
        if marker is None:
            return None
        level = len(heading) - len(heading.lstrip("#"))
        end = next(
            (
                index
                for index in range(marker + 1, len(self.lines))
                if heading_closes_section(self.lines[index], level)
            ),
            len(self.lines),
        )
        return marker + 1, self.lines[marker:end]

    def metadata(self, name: str) -> list[tuple[int, str]]:
        values = []
        for index, line in enumerate(self.lines, 1):
            match = METADATA.match(line)
            if match and match.group(1) == name:
                values.append((index, match.group(2).strip()))
        return values


def load_predecessor_disposition_document(
    root: Path,
    path: str,
) -> tuple[Optional[Document], Optional[str]]:
    """Load the selected disposition through checked, bounded POSIX I/O."""
    relative = PurePosixPath(path)
    if relative.is_absolute() or not relative.parts or any(
        part in ("", ".", "..") for part in relative.parts
    ):
        return None, "canonical path is invalid"
    required_flags = ("O_CLOEXEC", "O_DIRECTORY", "O_NOFOLLOW", "O_NONBLOCK")
    if any(not hasattr(os, name) for name in required_flags):
        return None, "safe no-follow loading is unavailable on this platform"

    directory_fds: list[int] = []
    file_fd: Optional[int] = None
    try:
        directory_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY | os.O_NOFOLLOW
        current_fd = os.open(root, directory_flags)
        directory_fds.append(current_fd)
        for component in relative.parts[:-1]:
            current_fd = os.open(component, directory_flags, dir_fd=current_fd)
            directory_fds.append(current_fd)
        file_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK
        file_fd = os.open(relative.parts[-1], file_flags, dir_fd=current_fd)
        opened = os.fstat(file_fd)
        if not stat.S_ISREG(opened.st_mode):
            return None, "file must be regular and non-symlink"
        if opened.st_size > PREDECESSOR_DISPOSITION_MAX_BYTES:
            return None, "file exceeds the 65536-byte predecessor disposition limit"
        chunks: list[bytes] = []
        remaining = PREDECESSOR_DISPOSITION_MAX_BYTES + 1
        while remaining:
            chunk = os.read(file_fd, min(8192, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        if len(raw) > PREDECESSOR_DISPOSITION_MAX_BYTES:
            return None, "file exceeds the 65536-byte predecessor disposition limit"
        current = os.stat(
            relative.parts[-1],
            dir_fd=current_fd,
            follow_symlinks=False,
        )
        if (
            not stat.S_ISREG(current.st_mode)
            or (current.st_dev, current.st_ino) != (opened.st_dev, opened.st_ino)
        ):
            return None, "canonical path changed during the bounded read"
    except FileNotFoundError:
        return None, "file is missing"
    except (NotADirectoryError, IsADirectoryError):
        return None, "file must be regular and non-symlink"
    except OSError:
        return None, "file could not be safely opened or read"
    finally:
        if file_fd is not None:
            os.close(file_fd)
        for descriptor in reversed(directory_fds):
            os.close(descriptor)

    if raw.startswith(b"\xef\xbb\xbf") or b"\r" in raw:
        return None, "encoding or newline form is invalid"
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return None, "encoding or newline form is invalid"
    return Document(root, path, text.splitlines()), None


@dataclass(frozen=True)
class LifecycleBoundaries:
    """Numeric release phases that own merge, publication, and closure."""

    merge: int
    publication: int
    closure: int


@dataclass(frozen=True)
class LifecycleDeclaration:
    """Versioned immutable-transition opt-in and canonical repository."""

    model: str
    canonical_repository: str


@dataclass(frozen=True)
class FailedPredecessorDisposition:
    """Strict successor-owned record for a public but unpublished predecessor."""

    values: dict[str, str]


@dataclass(frozen=True)
class TerminalDisposition:
    """Parsed structural terminal-disposition contract and public reference."""

    expectations: dict[str, str]
    values: dict[str, str]


@dataclass(frozen=True)
class SecurityToolchainProgression:
    """Parsed non-authorizing closed progression contract."""

    expectations: dict[str, str]
    values: dict[str, str]
    delta: tuple[tuple[str, str, str], ...]


@dataclass(frozen=True)
class FinalSourceSnapshot:
    """Parsed exact final-v1 source-snapshot selection."""

    expectations: dict[str, str]
    values: dict[str, str]
    delta: tuple[tuple[str, str, str], ...]


def _ordered_marked_values(
    lines: list[str],
    start_marker: str,
    end_marker: str,
    keys: tuple[str, ...],
) -> tuple[Optional[dict[str, str]], Optional[str]]:
    """Parse one exact marker/fence/key block without accepting extensions."""
    starts = [index for index, line in enumerate(lines) if line == start_marker]
    ends = [index for index, line in enumerate(lines) if line == end_marker]
    if not starts and not ends:
        return None, None
    if len(starts) != 1 or len(ends) != 1 or starts[0] >= ends[0]:
        return None, "marker cardinality or order is invalid"
    start, end = starts[0], ends[0]
    if start + 2 > end or lines[start + 1] != "```text" or lines[end - 1] != "```":
        return None, "fence placement is invalid"
    body = lines[start + 2 : end - 1]
    if len(body) != len(keys):
        return None, "field count is invalid"
    values: dict[str, str] = {}
    for expected, line in zip(keys, body):
        match = re.fullmatch(r"([A-Z0-9_]+): ([^\s].*[^\s]|[^\s])", line)
        if not match or match.group(1) != expected:
            return None, "field order or grammar is invalid"
        values[expected] = match.group(2)
    return values, None


def _safe_terminal_reference_bytes(
    root: Path,
    path: str,
    maximum: int,
) -> tuple[Optional[bytes], Optional[str]]:
    """Read one bounded repository-relative regular file through no-follow FDs."""
    if "\\" in path or "\x00" in path or "//" in path:
        return None, "canonical path is invalid"
    relative = PurePosixPath(path)
    if (
        relative.is_absolute()
        or not relative.parts
        or relative.as_posix() != path
        or any(part in ("", ".", "..") for part in relative.parts)
    ):
        return None, "canonical path is invalid"
    required_flags = ("O_CLOEXEC", "O_DIRECTORY", "O_NOFOLLOW", "O_NONBLOCK")
    if any(not hasattr(os, name) for name in required_flags):
        return None, "safe no-follow loading is unavailable on this platform"
    directory_fds: list[int] = []
    file_fd: Optional[int] = None
    try:
        directory_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY | os.O_NOFOLLOW
        current_fd = os.open(root, directory_flags)
        directory_fds.append(current_fd)
        for component in relative.parts[:-1]:
            current_fd = os.open(component, directory_flags, dir_fd=current_fd)
            directory_fds.append(current_fd)
        file_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK
        file_fd = os.open(relative.parts[-1], file_flags, dir_fd=current_fd)
        opened = os.fstat(file_fd)
        if not stat.S_ISREG(opened.st_mode):
            return None, "file must be regular and non-symlink"
        if opened.st_size > maximum:
            return None, "file exceeds the configured byte limit"
        chunks: list[bytes] = []
        remaining = maximum + 1
        while remaining:
            chunk = os.read(file_fd, min(8192, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        if len(raw) > maximum:
            return None, "file exceeds the configured byte limit"
        current = os.stat(relative.parts[-1], dir_fd=current_fd, follow_symlinks=False)
        if (
            not stat.S_ISREG(current.st_mode)
            or (current.st_dev, current.st_ino, current.st_mode)
            != (opened.st_dev, opened.st_ino, opened.st_mode)
        ):
            return None, "canonical path changed during the bounded read"
        return raw, None
    except FileNotFoundError:
        return None, "file is missing"
    except (NotADirectoryError, IsADirectoryError):
        return None, "file must be regular and non-symlink"
    except OSError:
        return None, "file could not be safely opened or read"
    finally:
        if file_fd is not None:
            os.close(file_fd)
        for descriptor in reversed(directory_fds):
            os.close(descriptor)


def _terminal_reference_grammar(values: dict[str, str]) -> Optional[str]:
    semver = re.compile(r"^(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)$")
    repository = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
    digest = re.compile(r"^[0-9a-f]{64}$")
    timestamp = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$")
    if values["SCHEMA"] != "coldkeep-v1-terminal-disposition-reference/v1":
        return "reference schema is unsupported"
    if semver.fullmatch(values["SOURCE_VERSION"]) is None:
        return "source version grammar is invalid"
    if repository.fullmatch(values["CANONICAL_REPOSITORY"]) is None:
        return "canonical repository grammar is invalid"
    if values["OWNER_DISPOSITION_PHASE"] != "9M":
        return "owner disposition phase is unsupported"
    if values["OWNER_DISPOSITION_STATUS"] != "ESTABLISHED_AT_C_ON_SOURCE_SCOPED_QUALIFIED_BASIS_BY_PHASE9M_OWNER_DISPOSITION":
        return "owner disposition status is unsupported"
    if timestamp.fullmatch(values["OWNER_DISPOSITION_AT_UTC"]) is None:
        return "owner disposition timestamp grammar is invalid"
    try:
        datetime.strptime(
            values["OWNER_DISPOSITION_AT_UTC"], "%Y-%m-%dT%H:%M:%S.%fZ"
        )
    except ValueError:
        return "owner disposition timestamp is invalid"
    if values["SOURCE_SCOPE"] != "V1_X_LIFECYCLE_TERMINAL_CLOSURE_QUALIFIED":
        return "source scope is unsupported"
    for key in (
        "ACCEPTED_C", "ACCEPTED_C_TREE", "PUBLISHED_M17",
        "ANNOTATED_TAG_A17", "TAG_DIRECT_TARGET",
    ):
        if NONZERO_GIT_SHA.fullmatch(values[key]) is None:
            return f"{key} grammar is invalid"
    if re.fullmatch(r"[1-9][0-9]*", values["GITHUB_RELEASE_ID"]) is None:
        return "GitHub Release ID grammar is invalid"
    for key in ("PHASE9M_MANIFEST_SHA256", "PHASE9M_OWNER_RECORD_SHA256"):
        if digest.fullmatch(values[key]) is None:
            return f"{key} grammar is invalid"
    enums = {
        "ORIGINAL_ATTEMPT_ONLY_ALL_SUCCESS_CERTIFICATE": "NOT_SATISFIED",
        "RETROSPECTIVE_PROCEDURAL_EXCEPTION": "NOT_GRANTED",
        "PRIVATE_EVIDENCE_LOCATION": "EXTERNAL_NOT_REQUIRED_BY_HOSTED_VALIDATION",
        "TRACKED_SERIALIZATION_AUTHORITY": "NONE_STRUCTURAL_REFERENCE_ONLY",
        "VALIDATOR_EFFECT": "STRUCTURAL_NON_AUTHORIZING",
    }
    for key, expected in enums.items():
        if values[key] != expected:
            return f"{key} is unsupported"
    return None


def load_terminal_disposition(
    root: Path,
    contract: Optional[Document],
    pointer: Optional[str],
) -> tuple[Optional[TerminalDisposition], Optional[str]]:
    """Load, compare, and locally validate the terminal disposition record."""
    if contract is None:
        return None, None
    start = "<!-- coldkeep-terminal-disposition-expectations:start -->"
    end = "<!-- coldkeep-terminal-disposition-expectations:end -->"
    expectations, detail = _ordered_marked_values(
        contract.lines, start, end, TERMINAL_EXPECTATION_KEYS
    )
    if expectations is None:
        if detail is None:
            return None, None
        return None, f"terminal disposition reference invalid: {detail}"
    if expectations["SCHEMA"] != "coldkeep-v1-terminal-disposition-expectations/v1":
        return None, "terminal disposition reference invalid: expectation schema is unsupported"
    if expectations["REFERENCE_MAX_BYTES"] != str(TERMINAL_DISPOSITION_MAX_BYTES):
        return None, "terminal disposition reference invalid: byte limit is unsupported"
    expected_path = expectations["REFERENCE_PATH"]
    if pointer != expected_path:
        return None, "terminal disposition reference invalid: canonical pointer disagrees with expectations"
    raw, unavailable = _safe_terminal_reference_bytes(
        root, expected_path, TERMINAL_DISPOSITION_MAX_BYTES
    )
    if unavailable:
        return None, f"terminal disposition reference unavailable: {unavailable}"
    assert raw is not None
    if raw.startswith(b"\xef\xbb\xbf") or b"\r" in raw or b"\x00" in raw:
        return None, "terminal disposition reference invalid: encoding or newline form is invalid"
    if not raw.endswith(b"\n") or raw.endswith(b"\n\n"):
        return None, "terminal disposition reference invalid: exactly one terminal LF is required"
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return None, "terminal disposition reference invalid: encoding is invalid"
    lines = text[:-1].split("\n")
    reference_start = "<!-- coldkeep-terminal-disposition-reference:start -->"
    reference_end = "<!-- coldkeep-terminal-disposition-reference:end -->"
    if lines.count("```text") != 1 or lines.count("```") != 1:
        return None, "terminal disposition reference invalid: fence cardinality is invalid"
    values, detail = _ordered_marked_values(
        lines, reference_start, reference_end, TERMINAL_REFERENCE_KEYS
    )
    if values is None:
        return None, f"terminal disposition reference invalid: {detail or 'reference block is missing'}"
    grammar_detail = _terminal_reference_grammar(values)
    if grammar_detail:
        return None, f"terminal disposition reference invalid: {grammar_detail}"
    for key in TERMINAL_REFERENCE_KEYS:
        expectation_key = "EXPECTED_REFERENCE_SCHEMA" if key == "SCHEMA" else f"EXPECTED_{key}"
        if expectations.get(expectation_key) != values[key]:
            return None, f"terminal disposition reference invalid: {key} disagrees with expectations"

    def object_type(identity: str) -> Optional[str]:
        result = run_git(root, ["cat-file", "-t", identity], allow_failure=True)
        return result.stdout.strip() if result.returncode == 0 else None

    for key, expected_type in (
        ("ACCEPTED_C", "commit"),
        ("ACCEPTED_C_TREE", "tree"),
        ("PUBLISHED_M17", "commit"),
        ("ANNOTATED_TAG_A17", "tag"),
        ("TAG_DIRECT_TARGET", "commit"),
    ):
        actual_type = object_type(values[key])
        if actual_type is None:
            return None, f"terminal disposition reference unavailable: required {key} object is unavailable"
        if actual_type != expected_type:
            return None, f"terminal disposition reference invalid: {key} object type is invalid"
    accepted_tree = commit_tree(root, values["ACCEPTED_C"])
    if accepted_tree != values["ACCEPTED_C_TREE"]:
        return None, "terminal disposition reference invalid: ACCEPTED_C tree disagrees"
    tag = run_git(
        root, ["cat-file", "-p", values["ANNOTATED_TAG_A17"]], allow_failure=True
    )
    if tag.returncode:
        return None, "terminal disposition reference unavailable: annotated tag body is unavailable"
    headers = tag.stdout.split("\n\n", 1)[0].splitlines()
    objects = [line[7:] for line in headers if line.startswith("object ")]
    types = [line[5:] for line in headers if line.startswith("type ")]
    if objects != [values["TAG_DIRECT_TARGET"]] or types != ["commit"]:
        return None, "terminal disposition reference invalid: annotated tag direct target disagrees"
    if values["TAG_DIRECT_TARGET"] != values["PUBLISHED_M17"]:
        return None, "terminal disposition reference invalid: tag target is not the published commit"
    return TerminalDisposition(expectations, values), None


def _final_snapshot_grammar(
    values: dict[str, str],
    delta: tuple[tuple[str, str, str], ...],
) -> Optional[str]:
    """Validate the closed scalar and delta grammar for v1.13.18."""
    exact = {
        "SCHEMA": "coldkeep-v1-final-source-snapshot-reference/v1",
        "SOURCE_VERSION": "1.13.18",
        "CANONICAL_REPOSITORY": "franchoy/coldkeep",
        "REPOSITORY_ID": "1158711822",
        "BRANCH_REF": "refs/heads/release/v1.13.18",
        "BASE_REF": "refs/heads/main",
        "TAG_REF": "refs/tags/v1.13.18",
        "LANGUAGE_FLOOR": "go1.25.0",
        "TOOLCHAIN": "go1.26.10",
        "GITHUB_RELEASE_ID": "400460267",
        "TS_ACCEPTANCE_STATUS": (
            "TS_READBACK_01_OWNER_ACCEPTED_PROTECTED_MAIN_"
            "EFFECTIVE_QUALIFIED_AT_M"
        ),
        "ORIGINAL_ATTEMPT_ONLY_ALL_SUCCESS_CERTIFICATE": "NOT_SATISFIED",
        "RETROSPECTIVE_PROCEDURAL_EXCEPTION": "NOT_GRANTED",
        "DELTA_SCHEMA": "coldkeep-v1-final-source-snapshot-delta/v1",
        "DELTA_COUNT": "40",
        "STATUS": "STRUCTURAL_NON_AUTHORIZING",
    }
    for key, expected in exact.items():
        if values[key] != expected:
            return f"{key} is unsupported"
    for key in (
        "BASE_M", "BASE_M_TREE", "PUBLISHED_M17", "ANNOTATED_TAG_A17",
    ):
        if NONZERO_GIT_SHA.fullmatch(values[key]) is None:
            return f"{key} grammar is invalid"
    parents = values["BASE_M_PARENTS"].split(",")
    if (
        len(parents) != 2
        or parents[0] == parents[1]
        or any(NONZERO_GIT_SHA.fullmatch(value) is None for value in parents)
    ):
        return "BASE_M_PARENTS grammar is invalid"
    if re.fullmatch(r"[0-9a-f]{64}", values["TS_ACCEPTANCE_SHA256"]) is None:
        return "TS_ACCEPTANCE_SHA256 grammar is invalid"
    if delta != FINAL_SNAPSHOT_DELTA:
        return "delta inventory is unsupported"
    if len(delta) != int(values["DELTA_COUNT"]):
        return "delta count is invalid"
    paths = [path for _, _, path in delta]
    if len(paths) != len(set(paths)):
        return "delta paths are duplicated"
    for status_value, mode, path in delta:
        if status_value not in ("M", "A") or mode not in ("100644", "100755"):
            return "delta status or mode is invalid"
        relative = PurePosixPath(path)
        if (
            "\\" in path
            or "\x00" in path
            or "//" in path
            or relative.is_absolute()
            or not relative.parts
            or relative.as_posix() != path
            or any(part in ("", ".", "..") for part in relative.parts)
        ):
            return "delta path is non-canonical"
    return None


def load_final_source_snapshot(
    root: Path,
    contract: Optional[Document],
    pointer: Optional[str],
) -> tuple[Optional[FinalSourceSnapshot], Optional[str]]:
    """Load and verify the isolated final-source-snapshot reference."""
    if contract is None:
        return None, "final snapshot reference invalid: expectation owner is missing"
    start = "<!-- coldkeep-v1-final-source-snapshot-expectations:start -->"
    end = "<!-- coldkeep-v1-final-source-snapshot-expectations:end -->"
    expectations, detail = _ordered_marked_values(
        contract.lines, start, end, FINAL_SNAPSHOT_EXPECTATION_KEYS
    )
    if expectations is None:
        return None, (
            "final snapshot reference invalid: "
            + (detail or "expectation block is missing")
        )
    if expectations["SCHEMA"] != "coldkeep-v1-final-source-snapshot-expectations/v1":
        return None, "final snapshot reference invalid: expectation schema is unsupported"
    if expectations["SELECTION_SCHEMA"] != "coldkeep-v1-final-source-snapshot-selection/v1":
        return None, "final snapshot reference invalid: selection schema is unsupported"
    if expectations["REFERENCE_MAX_BYTES"] != str(FINAL_SNAPSHOT_MAX_BYTES):
        return None, "final snapshot reference invalid: byte limit is unsupported"
    expected_path = expectations["REFERENCE_PATH"]
    if pointer != expected_path:
        return None, "final snapshot reference invalid: canonical pointer disagrees with expectations"
    raw, unavailable = _safe_terminal_reference_bytes(
        root, expected_path, FINAL_SNAPSHOT_MAX_BYTES
    )
    if unavailable:
        return None, f"final snapshot reference unavailable: {unavailable}"
    assert raw is not None
    if hashlib.sha256(raw).hexdigest() != expectations["EXPECTED_REFERENCE_SHA256"]:
        return None, "final snapshot reference invalid: digest disagrees with expectations"
    if raw.startswith(b"\xef\xbb\xbf") or b"\r" in raw or b"\x00" in raw:
        return None, "final snapshot reference invalid: encoding or newline form is invalid"
    if not raw.endswith(b"\n") or raw.endswith(b"\n\n"):
        return None, "final snapshot reference invalid: exactly one terminal LF is required"
    try:
        lines = raw[:-1].decode("utf-8").split("\n")
    except UnicodeDecodeError:
        return None, "final snapshot reference invalid: encoding is invalid"
    values, detail = _ordered_marked_values(
        lines,
        "<!-- coldkeep-v1-final-source-snapshot-reference:start -->",
        "<!-- coldkeep-v1-final-source-snapshot-reference:end -->",
        FINAL_SNAPSHOT_REFERENCE_KEYS,
    )
    if values is None:
        return None, (
            "final snapshot reference invalid: scalar block "
            + (detail or "is missing")
        )
    delta_start = "<!-- coldkeep-v1-final-source-snapshot-delta:start -->"
    delta_end = "<!-- coldkeep-v1-final-source-snapshot-delta:end -->"
    starts = [index for index, line in enumerate(lines) if line == delta_start]
    ends = [index for index, line in enumerate(lines) if line == delta_end]
    if len(starts) != 1 or len(ends) != 1 or starts[0] >= ends[0]:
        return None, "final snapshot reference invalid: delta marker cardinality or order is invalid"
    start_index, end_index = starts[0], ends[0]
    if (
        lines[start_index + 1 : start_index + 2] != ["```text"]
        or lines[end_index - 1 : end_index] != ["```"]
    ):
        return None, "final snapshot reference invalid: delta fence placement is invalid"
    delta: list[tuple[str, str, str]] = []
    for line in lines[start_index + 2 : end_index - 1]:
        match = re.fullmatch(r"([MA]) (100644|100755) (\S+)", line)
        if match is None:
            return None, "final snapshot reference invalid: delta row grammar is invalid"
        delta.append((match.group(1), match.group(2), match.group(3)))
    frozen_delta = tuple(delta)
    grammar_detail = _final_snapshot_grammar(values, frozen_delta)
    if grammar_detail:
        return None, f"final snapshot reference invalid: {grammar_detail}"
    for key in FINAL_SNAPSHOT_REFERENCE_KEYS:
        expectation_key = (
            "EXPECTED_REFERENCE_SCHEMA" if key == "SCHEMA" else f"EXPECTED_{key}"
        )
        if expectations.get(expectation_key) != values[key]:
            return None, f"final snapshot reference invalid: {key} disagrees with expectations"
    if expectations["EXPECTED_VALIDATOR_EFFECT"] != (
        "STRUCTURAL_ONLY_NO_PROVIDER_OR_OWNER_AUTHORITY"
    ):
        return None, "final snapshot reference invalid: validator effect is unsupported"

    def object_type(key: str, expected: str) -> Optional[str]:
        completed = run_git(root, ["cat-file", "-t", values[key]], allow_failure=True)
        if completed.returncode:
            return f"final snapshot reference unavailable: required {key} object is unavailable"
        if completed.stdout.strip() != expected:
            return f"final snapshot reference invalid: {key} object type is invalid"
        return None

    for key, expected in (
        ("BASE_M", "commit"), ("BASE_M_TREE", "tree"),
        ("PUBLISHED_M17", "commit"), ("ANNOTATED_TAG_A17", "tag"),
    ):
        object_detail = object_type(key, expected)
        if object_detail:
            return None, object_detail
    if commit_tree(root, values["BASE_M"]) != values["BASE_M_TREE"]:
        return None, "final snapshot reference invalid: BASE_M tree disagrees"
    if commit_parents(root, values["BASE_M"]) != values["BASE_M_PARENTS"].split(","):
        return None, "final snapshot reference invalid: BASE_M parents disagree"
    tag_ref = run_git(
        root, ["show-ref", "--verify", "--hash", "refs/tags/v1.13.17"],
        allow_failure=True,
    )
    if tag_ref.returncode or tag_ref.stdout.strip() != values["ANNOTATED_TAG_A17"]:
        return None, "final snapshot reference invalid: historical annotated tag ref disagrees"
    tag_body = run_git(
        root, ["cat-file", "-p", values["ANNOTATED_TAG_A17"]],
        allow_failure=True,
    )
    headers = tag_body.stdout.split("\n\n", 1)[0].splitlines() if not tag_body.returncode else []
    if (
        [line[7:] for line in headers if line.startswith("object ")]
        != [values["PUBLISHED_M17"]]
        or [line[5:] for line in headers if line.startswith("type ")] != ["commit"]
    ):
        return None, "final snapshot reference invalid: historical tag direct target disagrees"
    return FinalSourceSnapshot(expectations, values, frozen_delta), None


def _security_progression_grammar(
    values: dict[str, str],
    delta: tuple[tuple[str, str, str], ...],
) -> Optional[str]:
    """Validate the closed scalar and delta grammar of the progression record."""
    schema = values["SCHEMA"]
    if schema not in (
        "coldkeep-v1-security-toolchain-progression-reference/v1",
        "coldkeep-v1-security-toolchain-progression-reference/v2",
        "coldkeep-v1-security-toolchain-progression-reference/v3",
        "coldkeep-v1-security-toolchain-progression-reference/v4",
        "coldkeep-v1-security-toolchain-progression-reference/v5",
    ):
        return "reference schema is unsupported"
    version = int(schema.rsplit("/v", 1)[1])
    if re.fullmatch(r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)", values["SOURCE_VERSION"]) is None:
        return "source version grammar is invalid"
    if re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", values["CANONICAL_REPOSITORY"]) is None:
        return "canonical repository grammar is invalid"
    for key in ("REPOSITORY_ID", "PR_NUMBER", "DELTA_COUNT"):
        if re.fullmatch(r"[1-9][0-9]*", values[key]) is None:
            return f"{key} grammar is invalid"
    for key in (
        "BASE_C", "BASE_C_TREE", "HISTORICAL_H1",
        "HISTORICAL_H1_PARENT", "HISTORICAL_H1_TREE",
    ):
        if NONZERO_GIT_SHA.fullmatch(values[key]) is None:
            return f"{key} grammar is invalid"
    if version >= 2:
        for key in (
            "PUBLISHED_H2", "PUBLISHED_H2_PARENT", "PUBLISHED_H2_TREE",
            "SUCCESSOR_PARENT",
        ):
            if NONZERO_GIT_SHA.fullmatch(values[key]) is None:
                return f"{key} grammar is invalid"
    exact = {
        "BRANCH_REF": "refs/heads/release/v1.13.17-terminal-serialization",
        "BASE_REF": "refs/heads/main",
        "OLD_TOOLCHAIN": "go1.26.7",
        "NEW_TOOLCHAIN": "go1.26.9",
        "LANGUAGE_FLOOR": "go1.25.0",
        "DELTA_SCHEMA": "STATUS MODE PATH",
    }
    if version >= 3:
        for key in (
            "PUBLISHED_H3", "PUBLISHED_H3_PARENT", "PUBLISHED_H3_TREE",
        ):
            if NONZERO_GIT_SHA.fullmatch(values[key]) is None:
                return f"{key} grammar is invalid"
    if version >= 4:
        for key in (
            "PUBLISHED_H4", "PUBLISHED_H4_PARENT", "PUBLISHED_H4_TREE",
        ):
            if NONZERO_GIT_SHA.fullmatch(values[key]) is None:
                return f"{key} grammar is invalid"
    if version == 5:
        for key in (
            "PUBLISHED_H5", "PUBLISHED_H5_PARENT", "PUBLISHED_H5_TREE",
        ):
            if NONZERO_GIT_SHA.fullmatch(values[key]) is None:
                return f"{key} grammar is invalid"
        exact.update({
            "PURPOSE": (
                "FINAL_CODACY_COMPLEXITY_REMEDIATION_AND_"
                + "CLOSED_H5_SUCCESSOR_CONTRACT"
            ),
            "PUBLISHED_H2_CI_STATUS": "ORIGINAL_BRANCH_AND_PR_CI_FAILED",
            "PUBLISHED_H3_CI_STATUS": (
                "ORIGINAL_BRANCH_AND_PR_QUALITY_AND_CODACY_FAILED"
            ),
            "PUBLISHED_H4_CI_STATUS": (
                "ORIGINAL_BRANCH_AND_PR_QUALITY_FAILED_CODEQL_PASSED_"
                + "CODACY_3_NEW"
            ),
            "PUBLISHED_H5_CI_STATUS": (
                "BRANCH_AND_PR_CI_AND_CODEQL_PASSED_CODACY_1_NEW"
            ),
            "SUCCESSOR_AUTHOR_NAME": "franchoy",
            "SUCCESSOR_AUTHOR_EMAIL": "franchoy@gmail.com",
            "SUCCESSOR_AUTHOR_DATE": "2026-10-17T11:33:28+00:00",
            "SUCCESSOR_COMMITTER_NAME": "franchoy",
            "SUCCESSOR_COMMITTER_EMAIL": "franchoy@gmail.com",
            "SUCCESSOR_COMMITTER_DATE": "2026-10-17T11:33:28+00:00",
            "SUCCESSOR_SUBJECT": (
                "fix(release): clear final Codacy complexity finding"
            ),
            "STATUS": (
                "STRUCTURAL_NON_AUTHORIZING_CLOSED_H5_SUCCESSOR_CONTRACT"
            ),
            "VALIDATOR_EFFECT": "ONE_EXACT_DIRECT_H5_CHILD_DELTA_ONLY",
            "OWNER_AUTHORITY": (
                "LOCAL_IMPLEMENTATION_ONLY_PUBLICATION_AND_MERGE_"
                + "SEPARATELY_AUTHORIZED"
            ),
            "SECURITY_RESULT": (
                "H5_CI_AND_CODEQL_PASSED_CODACY_1_NEW_H6_NOT_EVALUATED"
            ),
            "PROVENANCE": (
                "OWNER_ADOPTED_H5_TO_H6_FINAL_CODACY_REMEDIATION_20261017"
            ),
        })
    elif version == 4:
        exact.update({
            "PURPOSE": (
                "GIT_IDENTITY_AND_QUALITY_REMEDIATION_AND_"
                + "CLOSED_H4_SUCCESSOR_CONTRACT"
            ),
            "PUBLISHED_H2_CI_STATUS": "ORIGINAL_BRANCH_AND_PR_CI_FAILED",
            "PUBLISHED_H3_CI_STATUS": (
                "ORIGINAL_BRANCH_AND_PR_QUALITY_AND_CODACY_FAILED"
            ),
            "PUBLISHED_H4_CI_STATUS": (
                "ORIGINAL_BRANCH_AND_PR_QUALITY_FAILED_CODEQL_PASSED_"
                + "CODACY_3_NEW"
            ),
            "SUCCESSOR_AUTHOR_NAME": "franchoy",
            "SUCCESSOR_AUTHOR_EMAIL": "franchoy@gmail.com",
            "SUCCESSOR_AUTHOR_DATE": "2026-10-17T10:10:26+00:00",
            "SUCCESSOR_COMMITTER_NAME": "franchoy",
            "SUCCESSOR_COMMITTER_EMAIL": "franchoy@gmail.com",
            "SUCCESSOR_COMMITTER_DATE": "2026-10-17T10:10:26+00:00",
            "SUCCESSOR_SUBJECT": (
                "fix(release): stabilize Git identity and clear remaining "
                + "quality findings"
            ),
            "STATUS": (
                "STRUCTURAL_NON_AUTHORIZING_CLOSED_H4_SUCCESSOR_CONTRACT"
            ),
            "VALIDATOR_EFFECT": "ONE_EXACT_DIRECT_H4_CHILD_DELTA_ONLY",
            "OWNER_AUTHORITY": (
                "LOCAL_IMPLEMENTATION_ONLY_PUBLICATION_AND_MERGE_"
                + "SEPARATELY_AUTHORIZED"
            ),
            "SECURITY_RESULT": (
                "H4_CODEQL_PASSED_CODACY_3_NEW_H5_NOT_EVALUATED"
            ),
            "PROVENANCE": (
                "OWNER_ADOPTED_H4_TO_H5_CI_CODACY_REMEDIATION_20261017"
            ),
        })
    elif version == 3:
        exact.update({
            "PURPOSE": "QUALITY_REMEDIATION_AND_CLOSED_H3_SUCCESSOR_CONTRACT",
            "PUBLISHED_H2_CI_STATUS": "ORIGINAL_BRANCH_AND_PR_CI_FAILED",
            "PUBLISHED_H3_CI_STATUS": "ORIGINAL_BRANCH_AND_PR_QUALITY_AND_CODACY_FAILED",
            "SUCCESSOR_AUTHOR_NAME": "franchoy",
            "SUCCESSOR_AUTHOR_EMAIL": "franchoy@gmail.com",
            "SUCCESSOR_AUTHOR_DATE": "2026-10-17T08:16:45+00:00",
            "SUCCESSOR_COMMITTER_NAME": "franchoy",
            "SUCCESSOR_COMMITTER_EMAIL": "franchoy@gmail.com",
            "SUCCESSOR_COMMITTER_DATE": "2026-10-17T08:16:45+00:00",
            "SUCCESSOR_SUBJECT": "fix(release): restore terminal quality gates and Codacy compliance",
            "STATUS": "STRUCTURAL_NON_AUTHORIZING_CLOSED_H3_SUCCESSOR_CONTRACT",
            "VALIDATOR_EFFECT": "ONE_EXACT_DIRECT_H3_CHILD_DELTA_ONLY",
            "OWNER_AUTHORITY": "LOCAL_IMPLEMENTATION_ONLY_PUBLICATION_AND_MERGE_SEPARATELY_AUTHORIZED",
            "SECURITY_RESULT": "H3_CODEQL_PASSED_CODACY_16_NEW_H4_NOT_EVALUATED",
            "PROVENANCE": "OWNER_ADOPTED_H3_TO_H4_QUALITY_REMEDIATION_20261017",
        })
    elif version == 2:
        exact.update({
            "PURPOSE": "FIXTURE_ISOLATION_AND_CLOSED_H2_SUCCESSOR_CONTRACT",
            "PUBLISHED_H2_CI_STATUS": "ORIGINAL_BRANCH_AND_PR_CI_FAILED",
            "STATUS": "STRUCTURAL_NON_AUTHORIZING_CLOSED_H2_SUCCESSOR_CONTRACT",
            "VALIDATOR_EFFECT": "ONE_EXACT_DIRECT_H2_CHILD_DELTA_ONLY",
            "OWNER_AUTHORITY": "NONE_SEPARATE_IMPLEMENTATION_AND_PUBLICATION_AUTHORIZATION_REQUIRED",
            "SECURITY_RESULT": "H2_DATED_RESULTS_ONLY_SUCCESSOR_NOT_EVALUATED",
            "PROVENANCE": "PHASE9AE_OWNER_ADOPTED_DESIGN_SPECIFICATION",
        })
    else:
        exact.update({
            "PURPOSE": "GO-2026-6607_SAME_SERIES_TOOLCHAIN_AND_ONE_STEP_PROGRESSION",
            "STATUS": "STRUCTURAL_NON_AUTHORIZING_UNEXECUTED_PROPOSAL",
            "VALIDATOR_EFFECT": "ONE_EXACT_H1_CHILD_ONLY",
            "OWNER_AUTHORITY": "NONE_REFERENCE_REQUIRES_INDEPENDENT_EXACT_DIFF_REVIEW",
            "SECURITY_RESULT": "NOT_EXECUTED_NOT_REMEDIATED",
            "PROVENANCE": "PHASE9V_MANIFEST_SHA256_254b72f12113f4f53ca73b40ae4c423e6a873ae98cf3ff02cbee1738440cdcb0",
        })
    for key, expected in exact.items():
        if values[key] != expected:
            return f"{key} is unsupported"
    expected_count = 9 if version >= 2 else 25
    if int(values["DELTA_COUNT"]) != len(delta) or len(delta) != expected_count:
        return "delta count is invalid"
    paths: list[str] = []
    for status_value, mode, path in delta:
        if status_value not in ("M", "A") or mode not in ("100644", "100755"):
            return "delta status or mode is invalid"
        if "\\" in path or "\x00" in path or "//" in path:
            return "delta path is non-canonical"
        relative = PurePosixPath(path)
        if (
            relative.is_absolute()
            or not relative.parts
            or relative.as_posix() != path
            or any(part in ("", ".", "..") for part in relative.parts)
        ):
            return "delta path is non-canonical"
        paths.append(path)
    if paths != sorted(paths) or len(set(paths)) != len(paths):
        return "delta paths are duplicated or out of order"
    expected_additions = 0 if version >= 2 else 1
    if sum(1 for status_value, _, _ in delta if status_value == "A") != expected_additions:
        return "delta addition cardinality is invalid"
    if version >= 2 and delta != SECURITY_PROGRESSION_V2_DELTA:
        return f"v{version} delta inventory is unsupported"
    return None


def load_security_toolchain_progression(
    root: Path,
    contract: Optional[Document],
    pointer: Optional[str],
    loaded_reference: Optional[tuple[Optional[bytes], Optional[str]]] = None,
) -> tuple[Optional[SecurityToolchainProgression], Optional[str]]:
    """Load and validate the exact digest-bound security progression record."""
    if contract is None:
        return None, "security progression reference invalid: expectation owner is missing"
    markers = {
        5: (
            "<!-- coldkeep-security-toolchain-progression-v5-"
            + "expectations:start -->"
        ),
        4: (
            "<!-- coldkeep-security-toolchain-progression-v4-"
            + "expectations:start -->"
        ),
        3: "<!-- coldkeep-security-toolchain-progression-v3-expectations:start -->",
        2: "<!-- coldkeep-security-toolchain-progression-v2-expectations:start -->",
        1: "<!-- coldkeep-security-toolchain-progression-expectations:start -->",
    }
    selected_versions = [
        version for version, marker in markers.items() if marker in contract.lines
    ]
    version = selected_versions[0] if len(selected_versions) == 1 else 1
    expectation_keys = {
        1: SECURITY_PROGRESSION_V1_EXPECTATION_KEYS,
        2: SECURITY_PROGRESSION_V2_EXPECTATION_KEYS,
        3: SECURITY_PROGRESSION_V3_EXPECTATION_KEYS,
        4: SECURITY_PROGRESSION_V4_EXPECTATION_KEYS,
        5: SECURITY_PROGRESSION_V5_EXPECTATION_KEYS,
    }[version]
    expectation_start = markers[version]
    expectation_end = expectation_start.replace(":start -->", ":end -->")
    expectations, detail = _ordered_marked_values(
        contract.lines, expectation_start, expectation_end, expectation_keys,
    )
    if expectations is None:
        return None, f"security progression reference invalid: {detail or 'expectation block is missing'}"
    expected_expectation_schema = (
        f"coldkeep-v1-security-toolchain-progression-expectations/v{version}"
    )
    if expectations["SCHEMA"] != expected_expectation_schema:
        return None, "security progression reference invalid: expectation schema is unsupported"
    if version >= 2 and expectations["SELECTION_SCHEMA"] != (
        f"coldkeep-v1-security-toolchain-progression-selection/v{version}"
    ):
        return None, "security progression reference invalid: selection schema is unsupported"
    if expectations["REFERENCE_MAX_BYTES"] != str(SECURITY_PROGRESSION_MAX_BYTES):
        return None, "security progression reference invalid: byte limit is unsupported"
    expected_path = expectations["REFERENCE_PATH"]
    if pointer != expected_path:
        return None, "security progression reference invalid: canonical pointer disagrees with expectations"
    raw, unavailable = (
        loaded_reference if loaded_reference is not None else
        _safe_terminal_reference_bytes(root, expected_path, SECURITY_PROGRESSION_MAX_BYTES)
    )
    if unavailable:
        return None, f"security progression reference unavailable: {unavailable}"
    assert raw is not None
    measured_digest = hashlib.sha256(raw).hexdigest()
    if measured_digest != expectations["EXPECTED_REFERENCE_SHA256"]:
        return None, "security progression reference invalid: digest disagrees with expectations"
    if raw.startswith(b"\xef\xbb\xbf") or b"\r" in raw or b"\x00" in raw:
        return None, "security progression reference invalid: encoding or newline form is invalid"
    if not raw.endswith(b"\n") or raw.endswith(b"\n\n"):
        return None, "security progression reference invalid: exactly one terminal LF is required"
    try:
        lines = raw[:-1].decode("utf-8").split("\n")
    except UnicodeDecodeError:
        return None, "security progression reference invalid: encoding is invalid"
    start_marker = (
        f"<!-- coldkeep-security-toolchain-progression-v{version}-reference:start -->"
        if version >= 2
        else "<!-- coldkeep-security-toolchain-progression-reference:start -->"
    )
    end_marker = start_marker.replace(":start -->", ":end -->")
    starts = [index for index, line in enumerate(lines) if line == start_marker]
    ends = [index for index, line in enumerate(lines) if line == end_marker]
    if len(starts) != 1 or len(ends) != 1 or starts[0] >= ends[0]:
        return None, "security progression reference invalid: marker cardinality or order is invalid"
    start, end = starts[0], ends[0]
    if lines.count("```text") != 1 or lines.count("```") != 1:
        return None, "security progression reference invalid: fence cardinality is invalid"
    if lines[start + 1 : start + 2] != ["```text"] or lines[end - 1 : end] != ["```"]:
        return None, "security progression reference invalid: fence placement is invalid"
    body = lines[start + 2 : end - 1]
    reference_keys = {
        1: SECURITY_PROGRESSION_V1_REFERENCE_KEYS,
        2: SECURITY_PROGRESSION_V2_REFERENCE_KEYS,
        3: SECURITY_PROGRESSION_V3_REFERENCE_KEYS,
        4: SECURITY_PROGRESSION_V4_REFERENCE_KEYS,
        5: SECURITY_PROGRESSION_V5_REFERENCE_KEYS,
    }[version]
    delta_count = 9 if version >= 2 else 25
    required_body_length = (
        len(reference_keys) + delta_count
        + len(SECURITY_PROGRESSION_TRAILER_KEYS)
    )
    if len(body) != required_body_length:
        return None, "security progression reference invalid: field or delta count is invalid"
    values: dict[str, str] = {}
    for key, line in zip(reference_keys, body):
        match = re.fullmatch(r"([A-Z0-9_]+): ([^\s].*[^\s]|[^\s])", line)
        if not match or match.group(1) != key:
            return None, "security progression reference invalid: field order or grammar is invalid"
        values[key] = match.group(2)
    delta_lines = body[
        len(reference_keys):
        len(reference_keys) + delta_count
    ]
    delta: list[tuple[str, str, str]] = []
    for line in delta_lines:
        match = re.fullmatch(r"([MA]) (100644|100755) (\S+)", line)
        if match is None:
            return None, "security progression reference invalid: delta row grammar is invalid"
        delta.append((match.group(1), match.group(2), match.group(3)))
    trailer = body[-len(SECURITY_PROGRESSION_TRAILER_KEYS):]
    for key, line in zip(SECURITY_PROGRESSION_TRAILER_KEYS, trailer):
        match = re.fullmatch(r"([A-Z0-9_]+): ([^\s].*[^\s]|[^\s])", line)
        if not match or match.group(1) != key:
            return None, "security progression reference invalid: trailer order or grammar is invalid"
        values[key] = match.group(2)
    frozen_delta = tuple(delta)
    grammar_detail = _security_progression_grammar(values, frozen_delta)
    if grammar_detail:
        return None, f"security progression reference invalid: {grammar_detail}"
    for key in reference_keys + SECURITY_PROGRESSION_TRAILER_KEYS:
        expectation_key = "EXPECTED_REFERENCE_SCHEMA" if key == "SCHEMA" else f"EXPECTED_{key}"
        if expectations.get(expectation_key) != values[key]:
            return None, f"security progression reference invalid: {key} disagrees with expectations"

    def required_object(key: str, kind: str) -> Optional[str]:
        query = run_git(root, ["cat-file", "-t", values[key]], allow_failure=True)
        if query.returncode:
            return f"security progression reference unavailable: required {key} object is unavailable"
        if query.stdout.strip() != kind:
            return f"security progression reference invalid: {key} object type is invalid"
        return None

    for key, kind in (
        ("BASE_C", "commit"), ("BASE_C_TREE", "tree"),
        ("HISTORICAL_H1", "commit"), ("HISTORICAL_H1_PARENT", "commit"),
        ("HISTORICAL_H1_TREE", "tree"),
    ):
        object_detail = required_object(key, kind)
        if object_detail:
            return None, object_detail
    if values["HISTORICAL_H1_PARENT"] != values["BASE_C"]:
        return None, "security progression reference invalid: historical H1 parent anchor disagrees"
    if commit_tree(root, values["BASE_C"]) != values["BASE_C_TREE"]:
        return None, "security progression reference invalid: BASE_C tree disagrees"
    if commit_parents(root, values["HISTORICAL_H1"]) != [values["BASE_C"]]:
        return None, "security progression reference invalid: historical H1 parents disagree"
    if commit_tree(root, values["HISTORICAL_H1"]) != values["HISTORICAL_H1_TREE"]:
        return None, "security progression reference invalid: historical H1 tree disagrees"
    if version >= 2:
        for key, kind in (
            ("PUBLISHED_H2", "commit"), ("PUBLISHED_H2_PARENT", "commit"),
            ("PUBLISHED_H2_TREE", "tree"), ("SUCCESSOR_PARENT", "commit"),
        ):
            object_detail = required_object(key, kind)
            if object_detail:
                return None, object_detail
        if values["PUBLISHED_H2_PARENT"] != values["HISTORICAL_H1"]:
            return None, "security progression reference invalid: published H2 parent anchor disagrees"
        if commit_parents(root, values["PUBLISHED_H2"]) != [values["HISTORICAL_H1"]]:
            return None, "security progression reference invalid: published H2 parents disagree"
        if commit_tree(root, values["PUBLISHED_H2"]) != values["PUBLISHED_H2_TREE"]:
            return None, "security progression reference invalid: published H2 tree disagrees"
        if version == 2 and values["SUCCESSOR_PARENT"] != values["PUBLISHED_H2"]:
            return None, "security progression reference invalid: successor parent anchor disagrees"
    if version >= 3:
        for key, kind in (
            ("PUBLISHED_H3", "commit"), ("PUBLISHED_H3_PARENT", "commit"),
            ("PUBLISHED_H3_TREE", "tree"),
        ):
            object_detail = required_object(key, kind)
            if object_detail:
                return None, object_detail
        if values["PUBLISHED_H3_PARENT"] != values["PUBLISHED_H2"]:
            return None, "security progression reference invalid: published H3 parent anchor disagrees"
        if commit_parents(root, values["PUBLISHED_H3"]) != [values["PUBLISHED_H2"]]:
            return None, "security progression reference invalid: published H3 parents disagree"
        if commit_tree(root, values["PUBLISHED_H3"]) != values["PUBLISHED_H3_TREE"]:
            return None, "security progression reference invalid: published H3 tree disagrees"
        if (
            version == 3
            and values["SUCCESSOR_PARENT"] != values["PUBLISHED_H3"]
        ):
            return None, (
                "security progression reference invalid: "
                + "successor parent anchor disagrees"
            )
    if version >= 4:
        for key, kind in (
            ("PUBLISHED_H4", "commit"), ("PUBLISHED_H4_PARENT", "commit"),
            ("PUBLISHED_H4_TREE", "tree"),
        ):
            object_detail = required_object(key, kind)
            if object_detail:
                return None, object_detail
        if values["PUBLISHED_H4_PARENT"] != values["PUBLISHED_H3"]:
            return None, (
                "security progression reference invalid: "
                + "published H4 parent anchor disagrees"
            )
        if commit_parents(root, values["PUBLISHED_H4"]) != [
            values["PUBLISHED_H3"]
        ]:
            return None, (
                "security progression reference invalid: "
                + "published H4 parents disagree"
            )
        if (
            commit_tree(root, values["PUBLISHED_H4"])
            != values["PUBLISHED_H4_TREE"]
        ):
            return None, (
                "security progression reference invalid: "
                + "published H4 tree disagrees"
            )
        if version == 4 and values["SUCCESSOR_PARENT"] != values["PUBLISHED_H4"]:
            return None, (
                "security progression reference invalid: "
                + "successor parent anchor disagrees"
            )
    if version == 5:
        for key, kind in (
            ("PUBLISHED_H5", "commit"), ("PUBLISHED_H5_PARENT", "commit"),
            ("PUBLISHED_H5_TREE", "tree"),
        ):
            object_detail = required_object(key, kind)
            if object_detail:
                return None, object_detail
        if values["PUBLISHED_H5_PARENT"] != values["PUBLISHED_H4"]:
            return None, (
                "security progression reference invalid: "
                + "published H5 parent anchor disagrees"
            )
        if commit_parents(root, values["PUBLISHED_H5"]) != [
            values["PUBLISHED_H4"]
        ]:
            return None, (
                "security progression reference invalid: "
                + "published H5 parents disagree"
            )
        if (
            commit_tree(root, values["PUBLISHED_H5"])
            != values["PUBLISHED_H5_TREE"]
        ):
            return None, (
                "security progression reference invalid: "
                + "published H5 tree disagrees"
            )
        if values["SUCCESSOR_PARENT"] != values["PUBLISHED_H5"]:
            return None, (
                "security progression reference invalid: "
                + "successor parent anchor disagrees"
            )
    return SecurityToolchainProgression(expectations, values, frozen_delta), None


def heading_closes_section(line: str, level: int) -> bool:
    """Return whether a Markdown heading closes a bounded section."""
    if not line.startswith("#"):
        return False
    candidate_level = len(line) - len(line.lstrip("#"))
    return candidate_level <= level and line.startswith("#" * candidate_level + " ")


def metadata_named(
    header: list[tuple[int, str, str]],
    name: str,
) -> list[tuple[int, str]]:
    """Select one kind of bounded tracker metadata."""
    return [(index, value) for index, field_name, value in header if field_name == name]


def present_tracker_values(
    values: list[Optional[tuple[str, str, str]]],
) -> Optional[list[tuple[str, str, str]]]:
    """Narrow optional tracker replicas after proving all are present."""
    if any(value is None for value in values):
        return None
    return [value for value in values if value is not None]


def resolved_executable(name: str) -> str:
    """Resolve an executable to an absolute path or fail deterministically."""
    executable = shutil.which(name)
    if executable is None:
        raise InternalError("git", f"unable to locate executable: {name}")
    return str(Path(executable).resolve())


def run_process(
    argv: list[str],
    *,
    cwd: Optional[Path] = None,
    env: Optional[dict[str, str]] = None,
    check: bool = False,
    timeout: Optional[float] = None,
) -> ProcessResult:
    """Run reviewed list-form argv without shell interpretation."""
    return subprocess.run(
        argv,
        cwd=str(cwd) if cwd else None,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=check,
        shell=False,
        timeout=timeout,
    )


def run_git(
    root: Path,
    args: list[str],
    allow_failure: bool = False,
) -> ProcessResult:
    """Run a read-only Git inspection command with stable error handling."""
    try:
        completed = run_process(
            [resolved_executable("git"), "-C", str(root), *args],
            check=False,
        )
    except OSError as exc:
        raise InternalError("git", f"unable to execute git: {exc}") from exc
    if completed.returncode and not allow_failure:
        detail = (
            completed.stderr.strip()
            or completed.stdout.strip()
            or "git command failed"
        )
        raise InternalError("git", detail)
    return completed


def validate_root(value: Optional[str]) -> Path:
    """Validate repository anchors and return the canonical worktree root."""
    root = Path(value).resolve() if value else Path(__file__).resolve().parents[1]
    anchors_exist = (
        root.is_dir()
        and (root / ".git").exists()
        and (root / "internal/version/version.go").is_file()
        and (root / "CHANGELOG.md").is_file()
    )
    if not anchors_exist:
        raise InternalError(
            "repository-layout",
            "repository root is missing required anchors",
        )
    top = run_git(root, ["rev-parse", "--show-toplevel"]).stdout.strip()
    if Path(top).resolve() != root:
        raise InternalError(
            "repository-layout",
            "repository root is not the Git worktree root",
        )
    return root


def parse_source_version(root: Path, result: ValidationResult) -> Optional[str]:
    """Parse the three authoritative Go version constants."""
    path = "internal/version/version.go"
    text = (root / path).read_text(encoding="utf-8")
    parts: list[str] = []
    for name in ("Major", "Minor", "Patch"):
        matches = re.findall(rf"(?m)^\s*{name}\s*=\s*([^\s/]+)", text)
        valid = (
            len(matches) == 1
            and re.fullmatch(r"\d+", matches[0]) is not None
            and int(matches[0]) >= 0
        )
        if not valid:
            result.add(
                "CKRS001",
                path,
                0,
                "source version must contain exactly one integer Major, Minor, and Patch declaration",
            )
            return None
        parts.append(matches[0])
    version = ".".join(parts)
    if not SEMVER.fullmatch(version):
        result.add(
            "CKRS001",
            path,
            0,
            "source version must contain exactly one integer Major, Minor, and Patch declaration",
        )
        return None
    return version


def version_from_release(value: str) -> Optional[tuple[str, str]]:
    """Parse version and title from a release metadata field."""
    match = re.search(r"`?(v\d+\.\d+\.\d+)\s+—\s+([^`]+?)`?$", value)
    if not match:
        return None
    return match.group(1)[1:], match.group(2).strip()


def phase_blocks(doc: Document) -> list[tuple[int, int, list[str]]]:
    """Split a tracker into Phase N blocks."""
    starts = [
        (index, int(match.group(1)))
        for index, line in enumerate(doc.lines)
        if (match := PHASE_HEADING.match(line))
    ]
    blocks = []
    for offset, (start, number) in enumerate(starts):
        end = starts[offset + 1][0] if offset + 1 < len(starts) else len(doc.lines)
        blocks.append((number, start, doc.lines[start:end]))
    return blocks


def field_from_lines(lines: Iterable[str], name: str) -> Optional[str]:
    """Read one anchored metadata field from bounded lines."""
    pattern = re.compile(rf"^\*\*{re.escape(name)}:\*\*\s*(.+)$")
    values = [
        match.group(1).strip()
        for line in lines
        if (match := pattern.match(line))
    ]
    return values[0] if len(values) == 1 else None


def parse_phase_states(
    doc: Document,
    metadata_field: str,
) -> tuple[list[int], dict[int, tuple[str, int]]]:
    """Read phase numbers and one aggregate status per phase."""
    numbers: list[int] = []
    states: dict[int, tuple[str, int]] = {}
    for number, start, block in phase_blocks(doc):
        numbers.append(number)
        values = []
        pattern = re.compile(
            rf"^\*\*{re.escape(metadata_field)}:\*\*\s*(.+)$",
        )
        for relative, line in enumerate(block, 1):
            match = pattern.match(line)
            if match:
                values.append((match.group(1).strip(), start + relative + 1))
        if len(values) == 1:
            states[number] = values[0]
    return numbers, states


def parse_lifecycle_boundaries(
    doc: Optional[Document],
) -> tuple[Optional[LifecycleBoundaries], Optional[str]]:
    """Parse optional generic lifecycle-boundary metadata fail closed."""
    if doc is None:
        return None, None
    expected = (
        "Merge-complete phase",
        "Tag/publication phase",
        "Post-publication closure phase",
    )
    values: dict[str, list[str]] = {name: [] for name in expected}
    for line in doc.lines:
        match = LIFECYCLE_BOUNDARY.match(line)
        if match:
            values[match.group(1)].append(match.group(2).strip())
    if not any(values.values()):
        return None, None
    if any(len(values[name]) != 1 for name in expected):
        return None, "lifecycle boundaries are missing or ambiguous"
    parsed: list[int] = []
    for name in expected:
        value = values[name][0]
        if re.fullmatch(r"\d+", value) is None:
            return None, f"{name} is not a nonnegative integer"
        parsed.append(int(value))
    return LifecycleBoundaries(
        merge=parsed[0],
        publication=parsed[1],
        closure=parsed[2],
    ), None


def parse_lifecycle_declaration(
    doc: Optional[Document],
) -> tuple[Optional[LifecycleDeclaration], Optional[str]]:
    """Parse an optional declaration without falling back on partial opt-in."""
    if doc is None:
        return None, None
    expected = ("Lifecycle declaration model", "Canonical repository")
    values: dict[str, list[str]] = {name: [] for name in expected}
    for line in doc.lines:
        match = LIFECYCLE_DECLARATION.match(line)
        if match:
            values[match.group(1)].append(match.group(2).strip())
    if not any(values.values()):
        return None, None
    if any(len(values[name]) != 1 for name in expected):
        return None, "lifecycle declaration is partial, empty, or ambiguous"
    model = values["Lifecycle declaration model"][0]
    repository = values["Canonical repository"][0]
    if model != SUPPORTED_LIFECYCLE_DECLARATION:
        return None, "lifecycle declaration model is unsupported"
    repository_pattern = r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+"
    if re.fullmatch(repository_pattern, repository) is None:
        return None, "canonical repository is malformed"
    return LifecycleDeclaration(model, repository), None


def parse_predecessor_declaration(
    doc: Optional[Document],
) -> tuple[Optional[str], Optional[str]]:
    """Parse the exact failed-publication selector independently."""
    if doc is None:
        return None, None
    values = [
        match.group(1).strip()
        for line in doc.lines
        if (match := PREDECESSOR_DECLARATION.match(line))
    ]
    if not values:
        return None, None
    if len(values) != 1 or not values[0]:
        return None, "predecessor disposition declaration is empty or ambiguous"
    if values[0] != SUPPORTED_PREDECESSOR_DECLARATION:
        return None, "predecessor disposition declaration is unsupported"
    return values[0], None


def parse_failed_predecessor_disposition(
    doc: Optional[Document],
) -> tuple[Optional[FailedPredecessorDisposition], Optional[str]]:
    """Parse one closed ordered predecessor-disposition block."""
    if doc is None:
        return None, "file is missing"
    start_marker = "<!-- coldkeep-predecessor-disposition:start -->"
    end_marker = "<!-- coldkeep-predecessor-disposition:end -->"
    starts = [index for index, line in enumerate(doc.lines) if line == start_marker]
    ends = [index for index, line in enumerate(doc.lines) if line == end_marker]
    if len(starts) != 1 or len(ends) != 1 or starts[0] >= ends[0]:
        return None, "marker region is missing or ambiguous"
    region = doc.lines[starts[0] + 1 : ends[0]]
    if len(region) < 2 or region[0] != "```text" or region[-1] != "```":
        return None, "marker region must contain one exact text fence"
    if any(line.startswith("```") for line in region[1:-1]):
        return None, "marker region contains an unexpected fence"
    pairs: list[tuple[str, str]] = []
    for line in region[1:-1]:
        match = re.fullmatch(r"([A-Z0-9_]+): (\S(?:.*\S)?)", line)
        if match is None:
            return None, "machine row syntax is invalid"
        pairs.append((match.group(1), match.group(2)))
    keys = tuple(key for key, _ in pairs)
    if keys != PREDECESSOR_DISPOSITION_KEYS:
        return None, "schema-v1 key set or order is invalid"
    values = dict(pairs)
    if values["SCHEMA"] != "coldkeep-failed-publication-predecessor/v1":
        return None, "SCHEMA is unsupported"
    if any(
        re.fullmatch(r"\d+\.\d+\.\d+", values[key]) is None
        for key in ("SUCCESSOR_VERSION", "PREDECESSOR_VERSION")
    ):
        return None, "version field is malformed"
    if re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", values["CANONICAL_REPOSITORY"]) is None:
        return None, "CANONICAL_REPOSITORY is malformed"
    if values["PREDECESSOR_REF"] != f"refs/tags/v{values['PREDECESSOR_VERSION']}":
        return None, "PREDECESSOR_REF conflicts with PREDECESSOR_VERSION"
    for key in ("TAG_OBJECT", "DIRECT_TARGET", "SUCCESSOR_BASE"):
        if NONZERO_GIT_SHA.fullmatch(values[key]) is None:
            return None, f"{key} is malformed"
    for key in ("DIAGNOSIS_ARCHIVE_SHA256", "DIAGNOSIS_MANIFEST_SHA256"):
        if re.fullmatch(r"[0-9a-f]{64}", values[key]) is None:
            return None, f"{key} is malformed"
    for key in ("ORIGINAL_TAG_CI_RUN", "ORIGINAL_TAG_CI_ATTEMPT", "DIAGNOSIS_ARCHIVE_BYTES"):
        if re.fullmatch(r"[1-9][0-9]*", values[key]) is None:
            return None, f"{key} must be a positive integer"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", values["GITHUB_RELEASE_OBSERVED_AT_UTC"]) is None:
        return None, "GITHUB_RELEASE_OBSERVED_AT_UTC is malformed"
    expected_enums = {
        "TAG_OBJECT_TYPE": "tag",
        "DIRECT_TARGET_DECLARED_TYPE": "commit",
        "DIRECT_TARGET_ACTUAL_TYPE": "commit",
        "ORIGINAL_TAG_CI_CONCLUSION": "failure",
        "TAG_CERTIFICATE": "withheld",
        "GITHUB_RELEASE_OBSERVATION": "absent-including-drafts",
        "PUBLICATION_AUTHORITY": "not-granted",
        "TERMINAL_CLOSURE": "not-established",
        "PREDECESSOR_FINAL_PHASE": "not-started-not-authorized",
    }
    if any(values[key] != expected for key, expected in expected_enums.items()):
        return None, "failed-publication disposition claims an unsupported publication or closure state"
    return FailedPredecessorDisposition(values), None


def lifecycle_boundaries_match_topology(
    boundaries: LifecycleBoundaries,
    phase_numbers: list[int],
) -> bool:
    """Require ordered lifecycle boundaries ending at the final phase."""
    if not phase_numbers:
        return False
    return (
        boundaries.merge < boundaries.publication < boundaries.closure
        and boundaries.closure == phase_numbers[-1]
        and boundaries.merge in phase_numbers
        and boundaries.publication in phase_numbers
        and boundaries.closure in phase_numbers
    )


def safe_relative_artifact(root: Path, containing: str, value: str) -> str:
    """Resolve one relative Markdown artifact without escaping the root."""
    first = PurePosixPath(value).parts[0] if PurePosixPath(value).parts else ""
    root_names = {item.name for item in root.iterdir()}
    candidate = root / value if first in root_names else (root / containing).parent / value
    try:
        return candidate.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return "../" + value


def artifact_targets(root: Path, containing: str, lines: Iterable[str]) -> list[str]:
    """Return unique repository-relative Markdown artifacts from bounded lines."""
    raw: list[str] = []
    for line in lines:
        raw.extend(re.findall(r"\[[^\]]+\]\(([^)]+\.md(?:#[^)]+)?)\)", line))
        raw.extend(re.findall(r"`([^`\n]+\.md(?:#[^`\n]+)?)`", line))
    values = (value.split("#", 1)[0] for value in raw)
    relevant = (
        value
        for value in values
        if value and not re.match(r"https?://", value) and not value.startswith("/")
    )
    return sorted(
        {safe_relative_artifact(root, containing, value) for value in relevant},
    )


def changelog_entry_valid(
    found: str,
    suffix: str,
    version: str,
    state: Optional[str],
) -> bool:
    """Return whether the active changelog entry matches its lifecycle."""
    expected_dated = state in (
        "pre-release",
        "merged-pending-final-main-certification",
        "merged-not-tagged",
        "tagged-pending-tag-certification",
        "tagged",
        "released",
        "post-release-pending-closure",
        "post-release-closure-candidate",
        "post-release-terminal-serialization-candidate",
        "post-release-terminal-serialization-main",
        "post-release-closed",
    )
    dated = bool(re.search(r"-\s+\d{4}-\d{2}-\d{2}\b", suffix))
    if found != version:
        return False
    if expected_dated:
        return dated and "Unreleased" not in suffix
    return "Unreleased" in suffix


def readme_current_state_valid(
    version: str,
    state: Optional[str],
    badge: list[str],
    content: str,
) -> bool:
    """Return whether README badge and current block agree on lifecycle."""
    expected_word = (
        "active"
        if state == "development"
        else "published"
        if state in (
            "post-release-pending-closure",
            "post-release-closure-candidate",
            "post-release-terminal-serialization-candidate",
            "post-release-terminal-serialization-main",
            "post-release-closed",
        )
        else "ready"
    )
    return (
        len(badge) == 1
        and f"v{version}" in badge[0]
        and f"v{version}" in content
        and expected_word in content.lower()
    )


def tracker_header(doc: Document) -> list[tuple[int, str, str]]:
    """Return bounded metadata found before a tracker's first level-two heading."""
    header = []
    for index, line in enumerate(doc.lines, 1):
        if line.startswith("## "):
            break
        match = METADATA.match(line)
        if match:
            header.append((index, match.group(1), match.group(2).strip()))
    return header


def tracker_values_disagree(
    values: list[tuple[str, str, str]],
    version: str,
    expected_status: str,
) -> bool:
    """Return whether active tracker identity replicas disagree."""
    return (
        any(value != values[0] for value in values)
        or values[0][0] != version
        or values[0][2] != expected_status
    )


def gate_verdict_missing(verdict: Optional[tuple[int, list[str]]]) -> bool:
    """Return whether a bounded verdict is empty or pending."""
    if verdict is None:
        return True
    lines = verdict[1]
    if lines and lines[0] == "## Final verdict":
        lines = lines[1:]
    text = "\n".join(lines)
    return not text.strip() or re.search(r"\bpending\b", text, re.I) is not None


def previous_closure_detail(
    previous: str,
    scope: Optional[Document],
    gate: Optional[Document],
    train: Optional[Document],
) -> Optional[str]:
    """Return the first missing prior-release closure fact."""
    if scope is None:
        return "scope is missing"
    statuses = [value for _, value in scope.metadata("Status")]
    if "Released and operationally closed" not in statuses:
        return "scope is not operationally closed"
    if gate is None:
        return "canonical gate is missing"
    if train is None:
        return "release train is missing"
    pattern = rf"^### `v{re.escape(previous)}\s+—.*?`$[\s\S]*?^\*\*Status:\*\* Released and operationally closed$"
    if not re.search(pattern, "\n".join(train.lines), re.M):
        return "release train is not operationally closed"
    return gate_closure_detail(gate)


def gate_closure_detail(gate: Document) -> Optional[str]:
    """Return the first missing canonical gate closure fact."""
    statuses = [value for _, value in gate.metadata("Status")]
    if "Passed and released" not in statuses:
        return "gate is not passed and released"
    if gate_verdict_missing(gate.section("## Final verdict")):
        return "gate verdict is missing or pending"
    return None


def current_train_section(train: Document) -> tuple[int, list[str]]:
    """Return the nonhistorical release-train prefix."""
    marker = "## Historical proposed continuation and final disposition"
    end = next(
        (index for index, line in enumerate(train.lines) if line == marker),
        len(train.lines),
    )
    return 1, train.lines[:end]


def prior_is_active(previous: str, branch: str, lines: list[str]) -> bool:
    """Return whether bounded current text calls the prior release active."""
    text = "\n".join(lines)
    version_pattern = rf"v{re.escape(previous)}[^\n]*(?:is\s+)?active"
    branch_pattern = rf"{re.escape(branch)}[^\n]*(?:is\s+)?active"
    return bool(
        re.search(version_pattern, text, re.I)
        or re.search(branch_pattern, text, re.I)
    )


def topology_valid(phase_numbers: list[int], checklist_numbers: list[int]) -> bool:
    """Return whether phase replicas share one contiguous ordered topology."""
    expected = list(range(len(phase_numbers)))
    return (
        phase_numbers == expected
        and len(set(phase_numbers)) == len(phase_numbers)
        and checklist_numbers == expected
        and len(set(checklist_numbers)) == len(checklist_numbers)
    )


def first_invalid_phase(
    phase_numbers: list[int],
    phase_states: dict[int, tuple[str, int]],
) -> Optional[int]:
    """Return the first phase whose aggregate status is unsupported."""
    allowed = {"Complete", "Next", "Not started"}
    return next(
        (
            number
            for number in phase_numbers
            if phase_states.get(number, ("missing", 0))[0] not in allowed
        ),
        None,
    )


def development_progression_valid(ordered: list[str]) -> bool:
    """Return whether statuses follow Complete*, Next, Not-started*."""
    next_positions = [index for index, value in enumerate(ordered) if value == "Next"]
    if len(next_positions) != 1:
        return False
    position = next_positions[0]
    return (
        all(value == "Complete" for value in ordered[:position])
        and all(value == "Not started" for value in ordered[position + 1 :])
    )


def progression_at_phase(ordered: list[str], phase: int) -> bool:
    """Return whether one exact phase is Next in a contiguous progression."""
    return (
        0 <= phase < len(ordered)
        and all(value == "Complete" for value in ordered[:phase])
        and ordered[phase] == "Next"
        and all(value == "Not started" for value in ordered[phase + 1 :])
    )


def lifecycle_progression_valid(
    state: str,
    ordered: list[str],
    boundaries: Optional[LifecycleBoundaries],
    declaration: Optional[LifecycleDeclaration] = None,
) -> bool:
    """Validate legacy or boundary-aware phase progression for one state."""
    complete = bool(ordered) and all(value == "Complete" for value in ordered)
    if boundaries is None:
        if state == "development":
            return development_progression_valid(ordered)
        return state in (
            "pre-release",
            "merged-not-tagged",
            "released",
            "post-release-closed",
        ) and complete
    if state == "development":
        if not development_progression_valid(ordered):
            return False
        return ordered.index("Next") < boundaries.merge
    if state == "pre-release":
        return progression_at_phase(ordered, boundaries.merge)
    if state in (
        "merged-pending-final-main-certification",
        "tagged-pending-tag-certification",
    ):
        return bool(
            declaration
            and progression_at_phase(ordered, boundaries.merge)
        )
    if state in ("merged-not-tagged", "tagged"):
        next_positions = [
            index for index, value in enumerate(ordered) if value == "Next"
        ]
        if len(next_positions) != 1:
            return False
        next_phase = next_positions[0]
        return (
            boundaries.merge < next_phase <= boundaries.publication
            and progression_at_phase(ordered, next_phase)
        )
    if state == "post-release-pending-closure":
        return progression_at_phase(ordered, boundaries.closure)
    if state == "post-release-closure-candidate":
        return bool(declaration and complete)
    if state in (
        "post-release-terminal-serialization-candidate",
        "post-release-terminal-serialization-main",
    ):
        return bool(declaration and complete)
    if state == "post-release-closed":
        return declaration is None and complete
    return False


def github_context() -> dict[str, str]:
    """Return only GitHub environment fields used by lifecycle inference."""
    keys = (
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
    )
    return {key: os.environ.get(key, "") for key in keys}


def accepted_release_pr(version: str, env: dict[str, str]) -> bool:
    """Return whether GitHub describes a PR merge ref for this release."""
    return (
        env["GITHUB_EVENT_NAME"] == "pull_request"
        and env["GITHUB_REF"].startswith("refs/pull/")
        and env["GITHUB_HEAD_REF"] == f"release/v{version}"
    )


def _empty_event_projection() -> dict[str, object]:
    """Return the fixed projection used before a payload object is available."""
    return {
        "repository": _unavailable_identity(),
        "number": _unavailable_identity(),
        "pull_request_shape": "unavailable",
        "base_shape": "unavailable",
        "head_shape": "unavailable",
        "base_ref": _unavailable_identity(),
        "base_sha": _unavailable_identity(),
        "base_repository": _unavailable_identity(),
        "head_ref": _unavailable_identity(),
        "head_sha": _unavailable_identity(),
        "head_repository": _unavailable_identity(),
        "merge_member": "unavailable",
        "merge_type": "unavailable",
        "merge_value": _unavailable_identity(),
    }


def _empty_tag_event_projection() -> dict[str, object]:
    """Return the fixed tag projection before a tag payload is available."""
    return {
        "ref": _unavailable_identity(),
        "before": _unavailable_identity(),
        "after": _unavailable_identity(),
        "created": _unavailable_identity(),
        "deleted": _unavailable_identity(),
        "forced": _unavailable_identity(),
        "repository": _unavailable_identity(),
    }


def _event_projection(
    payload: dict[str, object],
    version: str,
    canonical_repository: str,
) -> dict[str, object]:
    pull_request = payload.get("pull_request")
    base = pull_request.get("base") if isinstance(pull_request, dict) else None
    head = pull_request.get("head") if isinstance(pull_request, dict) else None
    merge_present = isinstance(pull_request, dict) and "merge_commit_sha" in pull_request
    merge_value = pull_request.get("merge_commit_sha") if merge_present else None
    return {
        "repository": _bounded_identity(
            _repository_name(payload.get("repository")),
            category="unexpected-repository",
            expected=canonical_repository,
        ),
        "number": _bounded_identity(
            payload.get("number"),
            category="unexpected-pr-number",
            allow_positive_integer=True,
        ),
        "pull_request_shape": _type_name(pull_request),
        "base_shape": _type_name(base),
        "head_shape": _type_name(head),
        "base_ref": _bounded_identity(
            base.get("ref") if isinstance(base, dict) else None,
            category="unexpected-base-ref",
            expected="main",
        ),
        "base_sha": _bounded_identity(
            base.get("sha") if isinstance(base, dict) else None,
            category="unexpected-base-sha",
            allow=GIT_SHA,
        ),
        "base_repository": _bounded_identity(
            _repository_name(base.get("repo")) if isinstance(base, dict) else None,
            category="unexpected-base-repository",
            expected=canonical_repository,
        ),
        "head_ref": _bounded_identity(
            head.get("ref") if isinstance(head, dict) else None,
            category="unexpected-head-ref",
            expected=f"release/v{version}",
        ),
        "head_sha": _bounded_identity(
            head.get("sha") if isinstance(head, dict) else None,
            category="unexpected-head-sha",
            allow=GIT_SHA,
        ),
        "head_repository": _bounded_identity(
            _repository_name(head.get("repo")) if isinstance(head, dict) else None,
            category="unexpected-head-repository",
            expected=canonical_repository,
        ),
        "merge_member": "present" if merge_present else "missing",
        "merge_type": _type_name(merge_value) if merge_present else "absent",
        "merge_value": _bounded_identity(
            merge_value,
            category="unexpected-merge-value",
            allow=NONZERO_GIT_SHA,
            allow_null=merge_present,
        ) if merge_present else _unavailable_identity("absent"),
    }


def _diagnostic_predicate(
    recorder: Optional[DiagnosticRecorder],
    evaluation_id: Optional[int],
    predicate_id: str,
    passed: Optional[bool],
    observed: dict[str, object],
) -> bool:
    """Record a predicate without permitting diagnostics to alter its value."""
    if recorder is not None and evaluation_id is not None:
        try:
            recorder.predicate(evaluation_id, predicate_id, passed, observed)
        except Exception:
            recorder.mark_incomplete("predicate-recording-unavailable")
    return passed is True


def _load_event(
    path: str,
    *,
    recorder: Optional[DiagnosticRecorder] = None,
    evaluation_id: Optional[int] = None,
    version: str = "",
    canonical_repository: str = "",
    predicate_prefix: str = "pr",
    bounded: bool = False,
) -> Optional[dict[str, object]]:
    """Load the exact event byte snapshot consumed by one evaluation."""
    path_bytes = path.encode("utf-8", errors="surrogatepass")
    basename = Path(path).name
    route = (
        "tag-push" if predicate_prefix == "tag" else
        "release-push" if predicate_prefix == "release_push" else
        "release-pr"
    )
    snapshot: dict[str, object] = {
        "id": 0,
        "route": route,
        "selector": "GITHUB_EVENT_PATH",
        "safe_basename": _bounded_identity(
            basename,
            category="unexpected-event-basename",
            expected="event.json",
        ),
        "path_string_sha256": hashlib.sha256(path_bytes).hexdigest(),
        "present": None,
        "readable": False,
        "size_bytes": None,
        "content_sha256": None,
        "size_limit_bytes": 1048576 if predicate_prefix == "tag" else None,
        "size_limit_status": "NOT_EVALUATED",
        "utf8_status": "NOT_EVALUATED",
        "json_status": "NOT_EVALUATED",
        "top_level_shape": "unavailable",
        "projection": (
            _empty_tag_event_projection()
            if predicate_prefix == "tag"
            else _empty_event_projection()
        ),
    }
    if recorder is not None and evaluation_id is not None:
        recorder.attach_snapshot(evaluation_id, snapshot)
    # diagnostic-predicate: pr.event.read
    # diagnostic-predicate: release_push.event.read
    # diagnostic-predicate: tag.event.read
    try:
        if predicate_prefix == "tag" or bounded:
            with Path(path).open("rb") as handle:
                raw = handle.read(1048577)
        else:
            raw = Path(path).read_bytes()
    except FileNotFoundError:
        snapshot["present"] = False
        _diagnostic_predicate(recorder, evaluation_id, f"{predicate_prefix}.event.read", False, _unavailable_identity("not-found"))
        return None
    except OSError:
        snapshot["present"] = None
        _diagnostic_predicate(recorder, evaluation_id, f"{predicate_prefix}.event.read", False, _unavailable_identity("unreadable"))
        return None
    snapshot["present"] = True
    snapshot["readable"] = True
    _diagnostic_predicate(recorder, evaluation_id, f"{predicate_prefix}.event.read", True, _bounded_identity("readable", category="unexpected-read-status", expected="readable"))
    if predicate_prefix == "tag" or bounded:
        within_limit = len(raw) <= 1048576
        snapshot["size_limit_status"] = "PASS" if within_limit else "FAIL"
        # diagnostic-predicate: tag.event.size
        observed_size = (
            _bounded_identity(len(raw), category="unexpected-size", allow_positive_integer=True)
            if within_limit
            else _bounded_identity("oversize", category="unexpected-size", expected="oversize")
        )
        if not _diagnostic_predicate(recorder, evaluation_id, "tag.event.size", within_limit, observed_size):
            return None
    snapshot["size_bytes"] = len(raw)
    snapshot["content_sha256"] = hashlib.sha256(raw).hexdigest()
    # diagnostic-predicate: pr.event.utf8
    # diagnostic-predicate: release_push.event.utf8
    # diagnostic-predicate: tag.event.utf8
    try:
        text = raw.decode("utf-8").replace("\r\n", "\n").replace("\r", "\n")
    except UnicodeDecodeError:
        snapshot["utf8_status"] = "FAIL"
        _diagnostic_predicate(recorder, evaluation_id, f"{predicate_prefix}.event.utf8", False, _unavailable_identity("invalid-utf8"))
        return None
    snapshot["utf8_status"] = "PASS"
    _diagnostic_predicate(recorder, evaluation_id, f"{predicate_prefix}.event.utf8", True, _bounded_identity("utf-8", category="unexpected-encoding", expected="utf-8"))
    # diagnostic-predicate: pr.event.json
    # diagnostic-predicate: release_push.event.json
    # diagnostic-predicate: tag.event.json
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        snapshot["json_status"] = "FAIL"
        _diagnostic_predicate(recorder, evaluation_id, f"{predicate_prefix}.event.json", False, _unavailable_identity("malformed-json"))
        return None
    snapshot["json_status"] = "PASS"
    _diagnostic_predicate(recorder, evaluation_id, f"{predicate_prefix}.event.json", True, _bounded_identity("parsed", category="unexpected-json-status", expected="parsed"))
    snapshot["top_level_shape"] = _type_name(payload)
    is_object = isinstance(payload, dict)
    # diagnostic-predicate: pr.event.object
    # diagnostic-predicate: release_push.event.object
    # diagnostic-predicate: tag.event.object
    _diagnostic_predicate(recorder, evaluation_id, f"{predicate_prefix}.event.object", is_object, _bounded_identity(_type_name(payload), category="unexpected-event-shape", expected="object"))
    if not is_object:
        return None
    if predicate_prefix == "tag":
        snapshot["projection"] = {
            "ref": _bounded_identity(payload.get("ref"), category="unexpected-tag-ref", expected=f"refs/tags/v{version}"),
            "before": _bounded_identity(payload.get("before"), category="unexpected-before", expected="0" * 40),
            "after": _bounded_identity(payload.get("after"), category="unexpected-after", allow=GIT_SHA),
            "created": _bounded_identity(payload.get("created"), category="unexpected-created", expected=None),
            "deleted": _bounded_identity(payload.get("deleted"), category="unexpected-deleted", expected=None),
            "forced": _bounded_identity(payload.get("forced"), category="unexpected-forced", expected=None),
            "repository": _bounded_identity(_repository_name(payload.get("repository")), category="unexpected-repository", expected=canonical_repository),
        }
    else:
        snapshot["projection"] = _event_projection(payload, version, canonical_repository)
    return payload


def github_context_present(env: dict[str, str]) -> bool:
    """Return whether any reviewed GitHub context field is populated."""
    return any(env.values())


def _repository_name(value: object) -> Optional[str]:
    return value.get("full_name") if isinstance(value, dict) else None


def _repository_id(value: object) -> Optional[int]:
    identity = value.get("id") if isinstance(value, dict) else None
    return identity if isinstance(identity, int) and not isinstance(identity, bool) else None


def _pr_ref_identity(value: object) -> Optional[tuple[object, object, object]]:
    if not isinstance(value, dict):
        return None
    return value.get("ref"), value.get("sha"), _repository_name(value.get("repo"))


def strict_release_pr_context(
    root: Path,
    version: str,
    head: str,
    env: dict[str, str],
    canonical_repository: str,
    *,
    branch: str = "",
    diagnostic: Optional[DiagnosticRecorder] = None,
    diagnostic_site: str = "strict-release-pr",
    selected_head_branch: Optional[str] = None,
) -> bool:
    """Validate a same-repository PR head or synthetic merge checkout."""
    evaluation_id: Optional[int] = None
    if diagnostic is not None:
        try:
            evaluation_id = diagnostic.begin_evaluation(
                diagnostic_site,
                env,
                head,
                branch,
                version,
                canonical_repository,
                selected_head_branch,
            )
        except Exception:
            diagnostic.mark_incomplete("diagnostic-construction-unavailable")

    def observed(value: object, category: str, *, expected: Optional[str] = None, allow: Optional[re.Pattern[str]] = None, allow_null: bool = False, positive: bool = False) -> dict[str, object]:
        return _bounded_identity(value, category=category, expected=expected, allow=allow, allow_null=allow_null, allow_positive_integer=positive)

    def check(predicate_id: str, condition: bool, value: dict[str, object]) -> bool:
        return _diagnostic_predicate(diagnostic, evaluation_id, predicate_id, condition, value)

    def finish(accepted: bool) -> bool:
        if diagnostic is not None and evaluation_id is not None:
            return diagnostic.finish_evaluation(evaluation_id, accepted)
        return accepted

    expected_head_branch = selected_head_branch or f"release/v{version}"
    # The sequential checks below are the prior compound expression in the same order.
    # diagnostic-predicate: pr.env.actions
    if not check("pr.env.actions", env["GITHUB_ACTIONS"] == "true", observed(env["GITHUB_ACTIONS"], "unexpected-actions", expected="true")):
        return finish(False)
    # diagnostic-predicate: pr.env.event
    if not check("pr.env.event", env["GITHUB_EVENT_NAME"] == "pull_request", observed(env["GITHUB_EVENT_NAME"], "unexpected-event", expected="pull_request")):
        return finish(False)
    # diagnostic-predicate: pr.env.full_ref
    if not check("pr.env.full_ref", re.fullmatch(r"refs/pull/\d+/merge", env["GITHUB_REF"]) is not None, observed(env["GITHUB_REF"], "unexpected-full-ref", allow=re.compile(r"refs/pull/[1-9][0-9]*/merge"))):
        return finish(False)
    # diagnostic-predicate: pr.env.short_ref
    if not check("pr.env.short_ref", env["GITHUB_REF_NAME"] == env["GITHUB_REF"].removeprefix("refs/pull/"), observed(env["GITHUB_REF_NAME"], "unexpected-short-ref", expected=env["GITHUB_REF"].removeprefix("refs/pull/"))):
        return finish(False)
    # diagnostic-predicate: pr.env.head_ref
    if not check("pr.env.head_ref", env["GITHUB_HEAD_REF"] == expected_head_branch, observed(env["GITHUB_HEAD_REF"], "unexpected-head-ref", expected=expected_head_branch)):
        return finish(False)
    # diagnostic-predicate: pr.env.base_ref
    if not check("pr.env.base_ref", env["GITHUB_BASE_REF"] == "main", observed(env["GITHUB_BASE_REF"], "unexpected-base-ref", expected="main")):
        return finish(False)
    # diagnostic-predicate: pr.env.repository
    if not check("pr.env.repository", env["GITHUB_REPOSITORY"] == canonical_repository, observed(env["GITHUB_REPOSITORY"], "unexpected-repository", expected=canonical_repository)):
        return finish(False)
    # diagnostic-predicate: pr.env.runtime_sha
    if not check("pr.env.runtime_sha", env["GITHUB_SHA"] == head, observed(env["GITHUB_SHA"], "unexpected-runtime-sha", allow=GIT_SHA)):
        return finish(False)
    # diagnostic-predicate: pr.env.event_path
    if not check("pr.env.event_path", bool(env["GITHUB_EVENT_PATH"]), observed("present" if env["GITHUB_EVENT_PATH"] else "absent", "unexpected-event-path-state", expected="present")):
        return finish(False)

    payload = _load_event(
        env["GITHUB_EVENT_PATH"],
        recorder=diagnostic,
        evaluation_id=evaluation_id,
        version=version,
        canonical_repository=canonical_repository,
    )
    if payload is None:
        return finish(False)
    pull_request = payload.get("pull_request")
    # diagnostic-predicate: pr.payload.pull_request
    if not check("pr.payload.pull_request", isinstance(pull_request, dict), observed(_type_name(pull_request), "unexpected-pull-request-shape", expected="object")):
        return finish(False)
    number = payload.get("number")
    number_valid = isinstance(number, int) and number > 0
    # diagnostic-predicate: pr.payload.number
    if not check("pr.payload.number", number_valid, observed(number, "unexpected-pr-number", positive=True)):
        return finish(False)
    # diagnostic-predicate: pr.payload.number_ref
    if not check("pr.payload.number_ref", env["GITHUB_REF"] == f"refs/pull/{number}/merge", observed(number, "unexpected-pr-number", positive=True)):
        return finish(False)
    repository = _repository_name(payload.get("repository"))
    # diagnostic-predicate: pr.payload.repository
    if not check("pr.payload.repository", repository == canonical_repository, observed(repository, "unexpected-repository", expected=canonical_repository)):
        return finish(False)
    base_object = pull_request.get("base")
    # diagnostic-predicate: pr.payload.base_object
    if not check("pr.payload.base_object", isinstance(base_object, dict), observed(_type_name(base_object), "unexpected-base-shape", expected="object")):
        return finish(False)
    head_object = pull_request.get("head")
    # diagnostic-predicate: pr.payload.head_object
    if not check("pr.payload.head_object", isinstance(head_object, dict), observed(_type_name(head_object), "unexpected-head-shape", expected="object")):
        return finish(False)
    base = _pr_ref_identity(base_object)
    pr_head = _pr_ref_identity(head_object)
    if base is None or pr_head is None:
        return finish(False)
    base_ref, base_sha, base_repo = base
    head_ref, head_sha, head_repo = pr_head
    # diagnostic-predicate: pr.payload.base_ref
    if not check("pr.payload.base_ref", base_ref == "main", observed(base_ref, "unexpected-base-ref", expected="main")):
        return finish(False)
    # diagnostic-predicate: pr.payload.head_ref
    if not check("pr.payload.head_ref", head_ref == expected_head_branch, observed(head_ref, "unexpected-head-ref", expected=expected_head_branch)):
        return finish(False)
    # diagnostic-predicate: pr.payload.base_repository
    if not check("pr.payload.base_repository", base_repo == canonical_repository, observed(base_repo, "unexpected-base-repository", expected=canonical_repository)):
        return finish(False)
    # diagnostic-predicate: pr.payload.head_repository
    if not check("pr.payload.head_repository", head_repo == canonical_repository, observed(head_repo, "unexpected-head-repository", expected=canonical_repository)):
        return finish(False)
    base_sha_valid = isinstance(base_sha, str) and GIT_SHA.fullmatch(base_sha) is not None
    # diagnostic-predicate: pr.payload.base_sha
    if not check("pr.payload.base_sha", base_sha_valid, observed(base_sha, "unexpected-base-sha", allow=GIT_SHA)):
        return finish(False)
    head_sha_valid = isinstance(head_sha, str) and GIT_SHA.fullmatch(head_sha) is not None
    # diagnostic-predicate: pr.payload.head_sha
    if not check("pr.payload.head_sha", head_sha_valid, observed(head_sha, "unexpected-head-sha", allow=GIT_SHA)):
        return finish(False)
    base_tree = commit_tree(root, base_sha)
    if diagnostic is not None and evaluation_id is not None:
        diagnostic.context_update(evaluation_id, object_availability=[{"role": "base", "sha": base_sha, "available": base_tree is not None}])
    # diagnostic-predicate: pr.git.base_object
    if not check("pr.git.base_object", base_tree is not None, observed("available" if base_tree is not None else "unavailable", "unexpected-object-state", expected="available")):
        return finish(False)
    payload_head_tree = commit_tree(root, head_sha)
    if diagnostic is not None and evaluation_id is not None:
        diagnostic.context_update(evaluation_id, object_availability=[{"role": "base", "sha": base_sha, "available": True}, {"role": "head", "sha": head_sha, "available": payload_head_tree is not None}], payload_head_tree=observed(payload_head_tree, "unexpected-tree", allow=GIT_SHA))
    # diagnostic-predicate: pr.git.head_object
    if not check("pr.git.head_object", payload_head_tree is not None, observed("available" if payload_head_tree is not None else "unavailable", "unexpected-object-state", expected="available")):
        return finish(False)
    direct = head == head_sha
    # diagnostic-predicate: pr.route
    check("pr.route", True, observed("direct-head" if direct else "synthetic", "unexpected-route", expected="direct-head" if direct else "synthetic"))
    if direct:
        return finish(True)
    merge_present = "merge_commit_sha" in pull_request
    # diagnostic-predicate: pr.payload.merge_member
    if not check("pr.payload.merge_member", merge_present, observed("present" if merge_present else "missing", "unexpected-member-state", expected="present")):
        return finish(False)
    merge_sha = pull_request["merge_commit_sha"]
    merge_field_valid = merge_sha is None or (
        isinstance(merge_sha, str)
        and GIT_SHA.fullmatch(merge_sha) is not None
        and merge_sha != "0" * 40
    )
    # Preserve the pre-instrumentation probe order: parents are read before the
    # merge-field Boolean participates in the final short-circuit expression.
    parents = commit_parents(root, head)
    if diagnostic is not None and evaluation_id is not None:
        diagnostic.context_update(evaluation_id, checkout_parents=parents or [])
    # diagnostic-predicate: pr.payload.merge_value
    if not check("pr.payload.merge_value", merge_field_valid, observed(merge_sha, "unexpected-merge-value", allow=NONZERO_GIT_SHA, allow_null=True)):
        return finish(False)
    parents_match = parents == [base_sha, head_sha]
    # diagnostic-predicate: pr.git.parents
    if not check("pr.git.parents", parents_match, observed("ordered-base-head" if parents_match else "different", "unexpected-parent-relation", expected="ordered-base-head")):
        return finish(False)
    checkout_tree = commit_tree(root, head)
    comparison_head_tree = commit_tree(root, head_sha)
    if diagnostic is not None and evaluation_id is not None:
        diagnostic.context_update(evaluation_id, checkout_tree=observed(checkout_tree, "unexpected-tree", allow=GIT_SHA), payload_head_tree=observed(comparison_head_tree, "unexpected-tree", allow=GIT_SHA))
    trees_match = checkout_tree == comparison_head_tree
    # diagnostic-predicate: pr.git.tree
    check("pr.git.tree", trees_match, observed("equal" if trees_match else "different", "unexpected-tree-relation", expected="equal"))
    return finish(trees_match)


def strict_post_release_pr_context(
    root: Path,
    version: str,
    head: str,
    env: dict[str, str],
    canonical_repository: str,
) -> bool:
    """Validate a same-repository recovery or closure PR checkout."""
    if not (
        env["GITHUB_ACTIONS"] == "true"
        and env["GITHUB_EVENT_NAME"] == "pull_request"
        and re.fullmatch(r"refs/pull/\d+/merge", env["GITHUB_REF"])
        and env["GITHUB_REF_NAME"]
        == env["GITHUB_REF"].removeprefix("refs/pull/")
        and env["GITHUB_BASE_REF"] == "main"
        and env["GITHUB_REPOSITORY"] == canonical_repository
        and env["GITHUB_SHA"] == head
        and env["GITHUB_EVENT_PATH"]
    ):
        return False
    payload = _load_event(env["GITHUB_EVENT_PATH"])
    pull_request = payload.get("pull_request") if payload else None
    if not isinstance(pull_request, dict):
        return False
    number = payload.get("number") if payload else None
    if not (
        isinstance(number, int)
        and number > 0
        and env["GITHUB_REF"] == f"refs/pull/{number}/merge"
    ):
        return False
    if _repository_name(payload.get("repository")) != canonical_repository:
        return False
    base = _pr_ref_identity(pull_request.get("base"))
    pr_head = _pr_ref_identity(pull_request.get("head"))
    if not base or not pr_head:
        return False
    base_ref, base_sha, base_repo = base
    head_ref, head_sha, head_repo = pr_head
    allowed_heads = {
        f"release/v{version}",
        f"release/v{version}-post-publication-closure",
    }
    if not (
        base_ref == "main"
        and head_ref in allowed_heads
        and env["GITHUB_HEAD_REF"] == head_ref
        and base_repo == canonical_repository
        and head_repo == canonical_repository
        and isinstance(base_sha, str)
        and GIT_SHA.fullmatch(base_sha)
        and isinstance(head_sha, str)
        and GIT_SHA.fullmatch(head_sha)
        and commit_tree(root, base_sha) is not None
        and commit_tree(root, head_sha) is not None
    ):
        return False
    if head == head_sha:
        return True
    parents = commit_parents(root, head)
    merge_present = "merge_commit_sha" in pull_request
    merge_sha = pull_request.get("merge_commit_sha")
    merge_field_valid = merge_sha is None or (
        isinstance(merge_sha, str)
        and GIT_SHA.fullmatch(merge_sha) is not None
        and merge_sha != "0" * 40
    )
    return bool(
        merge_present
        and merge_field_valid
        and parents == [base_sha, head_sha]
        and commit_tree(root, head) == commit_tree(root, head_sha)
    )


def strict_release_push_context(
    version: str,
    head: str,
    env: dict[str, str],
    canonical_repository: str,
    *,
    branch: str = "",
    diagnostic: Optional[DiagnosticRecorder] = None,
    diagnostic_site: str = "release-push",
) -> bool:
    """Validate a release-branch push event without treating it as a merge."""
    expected_ref = f"refs/heads/release/v{version}"
    evaluation_id: Optional[int] = None
    if diagnostic is not None:
        try:
            evaluation_id = diagnostic.begin_evaluation(
                diagnostic_site,
                env,
                head,
                branch,
                version,
                canonical_repository,
            )
        except Exception:
            diagnostic.mark_incomplete("diagnostic-construction-unavailable")
    payload = (
        _load_event(
            env["GITHUB_EVENT_PATH"],
            recorder=diagnostic,
            evaluation_id=evaluation_id,
            version=version,
            canonical_repository=canonical_repository,
            predicate_prefix="release_push",
        )
        if env["GITHUB_EVENT_PATH"]
        else None
    )
    before = payload.get("before") if payload else None
    created = payload.get("created") if payload else None
    zero_sha = "0" * 40
    accepted = bool(
        env["GITHUB_ACTIONS"] == "true"
        and env["GITHUB_EVENT_NAME"] == "push"
        and env["GITHUB_REF"] == expected_ref
        and env["GITHUB_REF_NAME"] == f"release/v{version}"
        and env["GITHUB_REF_TYPE"] == "branch"
        and env["GITHUB_REPOSITORY"] == canonical_repository
        and env["GITHUB_SHA"] == head
        and branch in ("", f"release/v{version}")
        and payload
        and payload.get("ref") == expected_ref
        and payload.get("after") == head
        and isinstance(before, str)
        and GIT_SHA.fullmatch(before)
        and (
            (created is True and before == zero_sha)
            or (created is False and before != zero_sha)
        )
        and payload.get("deleted") is False
        and payload.get("forced") is False
        and _repository_name(payload.get("repository")) == canonical_repository
    )
    # diagnostic-predicate: release_push.context
    _diagnostic_predicate(
        diagnostic,
        evaluation_id,
        "release_push.context",
        accepted,
        _bounded_identity(
            env["GITHUB_EVENT_NAME"],
            category="unexpected-release-push-context",
            expected="push",
        ),
    )
    if diagnostic is not None and evaluation_id is not None:
        diagnostic.finish_evaluation(evaluation_id, accepted)
    return accepted


def strict_closure_push_context(
    root: Path,
    version: str,
    head: str,
    context: tuple[str, str, bool, bool, Optional[str]],
    env: dict[str, str],
    canonical_repository: str,
) -> bool:
    """Validate one attached post-publication closure-branch push."""
    closure_branch = f"release/v{version}-post-publication-closure"
    expected_ref = f"refs/heads/{closure_branch}"
    if not (
        env["GITHUB_ACTIONS"] == "true"
        and env["GITHUB_EVENT_NAME"] == "push"
        and env["GITHUB_REF"] == expected_ref
        and env["GITHUB_REF_NAME"] == closure_branch
        and env["GITHUB_REF_TYPE"] == "branch"
        and env["GITHUB_REPOSITORY"] == canonical_repository
        and NONZERO_GIT_SHA.fullmatch(env["GITHUB_SHA"])
        and env["GITHUB_SHA"] == head
        and env["GITHUB_EVENT_PATH"]
    ):
        return False
    payload = _load_event(env["GITHUB_EVENT_PATH"], bounded=True)
    if payload is None:
        return False
    before = payload.get("before")
    created = payload.get("created")
    if not (
        payload.get("ref") == expected_ref
        and isinstance(payload.get("after"), str)
        and NONZERO_GIT_SHA.fullmatch(payload["after"])
        and payload["after"] == head
        and isinstance(before, str)
        and GIT_SHA.fullmatch(before)
        and (
            (created is True and before == "0" * 40)
            or (created is False and before != "0" * 40)
        )
        and payload.get("deleted") is False
        and payload.get("forced") is False
        and _repository_name(payload.get("repository")) == canonical_repository
    ):
        return False
    if any(
        os.environ.get(name)
        for name in (
            "GIT_ALTERNATE_OBJECT_DIRECTORIES",
            "GIT_OBJECT_DIRECTORY",
            "GIT_REPLACE_REF_BASE",
            "GIT_NAMESPACE",
        )
    ):
        return False

    child_env = dict(os.environ)
    child_env["GIT_NO_REPLACE_OBJECTS"] = "1"

    def closure_git(args: list[str]) -> Optional[ProcessResult]:
        try:
            return run_process(
                [resolved_executable("git"), "-C", str(root), *args],
                env=child_env,
            )
        except OSError:
            return None

    fresh_head_result = closure_git(["rev-parse", "--verify", "HEAD^{commit}"])
    fresh_head = (
        fresh_head_result.stdout.strip()
        if fresh_head_result and fresh_head_result.returncode == 0
        else None
    )
    fresh_branch_result = closure_git(
        ["symbolic-ref", "--quiet", "--short", "HEAD"]
    )
    fresh_branch = (
        fresh_branch_result.stdout.strip()
        if fresh_branch_result and fresh_branch_result.returncode == 0
        else None
    )
    if fresh_head != head or fresh_branch != closure_branch:
        return False
    if context[0] != fresh_head or context[1] != fresh_branch:
        return False

    shallow = closure_git(["rev-parse", "--is-shallow-repository"])
    if not shallow or shallow.returncode != 0 or shallow.stdout.strip() != "false":
        return False
    alternate_path_result = closure_git(
        ["rev-parse", "--git-path", "objects/info/alternates"]
    )
    if not alternate_path_result or alternate_path_result.returncode != 0:
        return False
    alternate_path = Path(alternate_path_result.stdout.strip())
    if not alternate_path.is_absolute():
        alternate_path = root / alternate_path
    if alternate_path.exists():
        return False
    graft_path_result = closure_git(["rev-parse", "--git-path", "info/grafts"])
    if not graft_path_result or graft_path_result.returncode != 0:
        return False
    graft_path = Path(graft_path_result.stdout.strip())
    if not graft_path.is_absolute():
        graft_path = root / graft_path
    if graft_path.exists() and graft_path.stat().st_size != 0:
        return False
    replacements = closure_git(
        ["for-each-ref", "--format=%(refname)", "refs/replace"]
    )
    if not replacements or replacements.returncode != 0 or replacements.stdout.strip():
        return False

    tag_ref = f"refs/tags/v{version}"
    named = closure_git(["show-ref", "--verify", "--hash", tag_ref])
    named_lines = named.stdout.splitlines() if named and named.returncode == 0 else []
    tag_object = (
        named_lines[0]
        if len(named_lines) == 1 and GIT_SHA.fullmatch(named_lines[0])
        else None
    )
    tag_type = closure_git(["cat-file", "-t", tag_object or ""])
    tag_size = closure_git(["cat-file", "-s", tag_object or ""])
    if not (
        tag_object
        and tag_type
        and tag_type.returncode == 0
        and tag_type.stdout.strip() == "tag"
        and tag_size
        and tag_size.returncode == 0
        and re.fullmatch(r"[0-9]+", tag_size.stdout.strip())
        and int(tag_size.stdout.strip()) <= 65536
    ):
        return False
    tag_body = closure_git(["cat-file", "-p", tag_object])
    if not tag_body or tag_body.returncode != 0:
        return False
    headers = tag_body.stdout.split("\n\n", 1)[0].splitlines()
    object_headers = [
        line.removeprefix("object ")
        for line in headers
        if line.startswith("object ")
    ]
    type_headers = [
        line.removeprefix("type ")
        for line in headers
        if line.startswith("type ")
    ]
    direct_target = (
        object_headers[0]
        if len(object_headers) == 1 and GIT_SHA.fullmatch(object_headers[0])
        else None
    )
    if not direct_target or type_headers != ["commit"]:
        return False
    direct_type = closure_git(["cat-file", "-t", direct_target])
    if not direct_type or direct_type.returncode != 0 or direct_type.stdout.strip() != "commit":
        return False
    ancestry = closure_git(["merge-base", "--is-ancestor", direct_target, head])
    if not ancestry or ancestry.returncode != 0 or direct_target == head:
        return False
    return context[2:] == (True, True, direct_target)


def commit_parents(root: Path, commit: str) -> list[str]:
    """Return ordered commit parents, or an empty list for an invalid object."""
    completed = run_git(
        root, ["rev-list", "--parents", "-n", "1", commit], allow_failure=True
    )
    if completed.returncode:
        return []
    parts = completed.stdout.strip().split()
    return parts[1:] if parts and parts[0] == commit else []


def commit_tree(root: Path, commit: str) -> Optional[str]:
    """Return one commit tree, or none for an invalid object."""
    completed = run_git(
        root, ["rev-parse", f"{commit}^{{tree}}"], allow_failure=True
    )
    value = completed.stdout.strip()
    return value if completed.returncode == 0 and GIT_SHA.fullmatch(value) else None


def normal_merge_artifact(
    root: Path,
    head: str,
    expected_first_parent: Optional[str] = None,
) -> bool:
    """Validate a normal two-parent merge preserving the second-parent tree."""
    parents = commit_parents(root, head)
    if len(parents) != 2:
        return False
    first, second = parents
    return bool(
        (expected_first_parent is None or first == expected_first_parent)
        and strict_git_ancestor(root, first, second)
        and commit_tree(root, head) == commit_tree(root, second)
    )


def strict_main_push_context(
    root: Path,
    head: str,
    env: dict[str, str],
    canonical_repository: str,
) -> bool:
    """Validate event/Git consistency for one normal main push merge."""
    payload = _load_event(env["GITHUB_EVENT_PATH"]) if env["GITHUB_EVENT_PATH"] else None
    before = payload.get("before") if payload else None
    return bool(
        env["GITHUB_ACTIONS"] == "true"
        and env["GITHUB_EVENT_NAME"] == "push"
        and env["GITHUB_REF"] == "refs/heads/main"
        and env["GITHUB_REF_NAME"] == "main"
        and env["GITHUB_REF_TYPE"] == "branch"
        and env["GITHUB_REPOSITORY"] == canonical_repository
        and env["GITHUB_SHA"] == head
        and payload
        and payload.get("ref") == "refs/heads/main"
        and payload.get("after") == head
        and isinstance(before, str)
        and GIT_SHA.fullmatch(before)
        and before != "0" * 40
        and payload.get("created") is False
        and payload.get("deleted") is False
        and payload.get("forced") is False
        and _repository_name(payload.get("repository")) == canonical_repository
        and normal_merge_artifact(root, head, before)
    )


def terminal_candidate_artifact(
    root: Path,
    head: str,
    accepted_c: str,
    progression: Optional[SecurityToolchainProgression] = None,
) -> bool:
    """Validate the fixed one-commit terminal serialization candidate."""
    if progression is not None:
        historical_h1 = progression.values["HISTORICAL_H1"]
        version = int(progression.values["SCHEMA"].rsplit("/v", 1)[1])
        if version >= 2:
            predecessor = progression.values[
                _progression_predecessor_key(version)
            ]
            return head == predecessor or progressed_h2_artifact(
                root, head, progression
            )
        return head == historical_h1 or progressed_h2_artifact(root, head, progression)
    return bool(
        commit_tree(root, accepted_c) is not None
        and commit_tree(root, head) is not None
        and commit_parents(root, head) == [accepted_c]
    )


def _tree_inventory(root: Path, commit: str) -> Optional[dict[str, tuple[str, str]]]:
    """Return path to mode/type from one recursively listed commit tree."""
    completed = run_git(root, ["ls-tree", "-r", "-z", commit], allow_failure=True)
    if completed.returncode:
        return None
    inventory: dict[str, tuple[str, str]] = {}
    for record in completed.stdout.split("\0"):
        if not record:
            continue
        match = re.fullmatch(r"([0-7]{6}) ([a-z]+) [0-9a-f]{40}\t(.+)", record)
        if match is None or match.group(3) in inventory:
            return None
        inventory[match.group(3)] = (match.group(1), match.group(2))
    return inventory


def final_snapshot_artifact(
    root: Path,
    head: str,
    snapshot: FinalSourceSnapshot,
    *,
    require_complete: bool,
) -> bool:
    """Validate a merge-free candidate against exact M and the frozen delta."""
    base = snapshot.values["BASE_M"]
    if (
        commit_tree(root, base) != snapshot.values["BASE_M_TREE"]
        or commit_parents(root, base)
        != snapshot.values["BASE_M_PARENTS"].split(",")
    ):
        return False
    if head != base:
        ancestry = run_git(
            root, ["merge-base", "--is-ancestor", base, head], allow_failure=True
        )
        if ancestry.returncode != 0:
            return False
    merges = run_git(
        root, ["rev-list", "--merges", f"{base}..{head}"], allow_failure=True
    )
    if merges.returncode or merges.stdout.strip():
        return False
    changed = run_git(
        root,
        [
            "diff-tree", "--no-commit-id", "--name-status", "-r", "-z",
            "--no-renames", base, head,
        ],
        allow_failure=True,
    )
    if changed.returncode:
        return False
    parts = changed.stdout.split("\0")
    if parts and parts[-1] == "":
        parts.pop()
    if len(parts) % 2:
        return False
    actual = tuple(
        (parts[index], parts[index + 1])
        for index in range(0, len(parts), 2)
    )
    expected = tuple((status, path) for status, _, path in snapshot.delta)
    if len(actual) != len(set(actual)):
        return False
    if require_complete:
        if len(actual) != len(expected) or set(actual) != set(expected):
            return False
    elif any(item not in expected for item in actual):
        return False
    old_inventory = _tree_inventory(root, base)
    new_inventory = _tree_inventory(root, head)
    if old_inventory is None or new_inventory is None:
        return False
    expected_by_path = {
        path: (status, mode) for status, mode, path in snapshot.delta
    }
    for status_value, path in actual:
        expected_status, mode = expected_by_path[path]
        if status_value != expected_status or new_inventory.get(path) != (mode, "blob"):
            return False
        if expected_status == "M" and old_inventory.get(path) != (mode, "blob"):
            return False
        if expected_status == "A" and path in old_inventory:
            return False
    return True


def final_snapshot_main_artifact(
    root: Path,
    head: str,
    snapshot: FinalSourceSnapshot,
) -> bool:
    """Validate exact protected-main topology `[M,R18]` and candidate tree."""
    parents = commit_parents(root, head)
    if len(parents) != 2 or parents[0] != snapshot.values["BASE_M"]:
        return False
    candidate = parents[1]
    return bool(
        commit_tree(root, head) is not None
        and commit_tree(root, head) == commit_tree(root, candidate)
        and final_snapshot_artifact(
            root, candidate, snapshot, require_complete=True
        )
    )


def final_snapshot_pr_base_matches(
    env: dict[str, str], snapshot: FinalSourceSnapshot
) -> bool:
    """Require the PR payload base to remain exact M."""
    payload = _load_event(env.get("GITHUB_EVENT_PATH", ""))
    pull_request = payload.get("pull_request") if payload else None
    base = pull_request.get("base") if isinstance(pull_request, dict) else None
    return bool(
        isinstance(base, dict)
        and base.get("ref") == "main"
        and base.get("sha") == snapshot.values["BASE_M"]
    )


def final_snapshot_pr_artifact_status(
    root: Path,
    checkout: str,
    env: dict[str, str],
    snapshot: FinalSourceSnapshot,
) -> Optional[str]:
    """Validate the selected PR candidate and identify its checkout route."""
    if not final_snapshot_pr_base_matches(env, snapshot):
        return None
    payload = _load_event(env.get("GITHUB_EVENT_PATH", ""))
    pull_request = payload.get("pull_request") if payload else None
    head = pull_request.get("head") if isinstance(pull_request, dict) else None
    candidate = head.get("sha") if isinstance(head, dict) else None
    if not isinstance(candidate, str) or not final_snapshot_artifact(
        root,
        candidate,
        snapshot,
        require_complete=True,
    ):
        return None
    return (
        "PR_DIRECT_COMPLETE_VALID"
        if checkout == candidate
        else "PR_SYNTHETIC_COMPLETE_VALID"
    )


def final_snapshot_context_status(
    root: Path,
    version: str,
    state: str,
    context: tuple[str, str, bool, bool, Optional[str]],
    env: dict[str, str],
    snapshot: FinalSourceSnapshot,
) -> str:
    """Return the structural snapshot status for the active artifact role."""
    head, branch, _, _, _ = context
    if state in ("development", "pre-release"):
        if env.get("GITHUB_EVENT_NAME") == "pull_request":
            if not strict_release_pr_context(
                root,
                version,
                head,
                env,
                snapshot.values["CANONICAL_REPOSITORY"],
                branch=branch,
            ):
                return "INVALID"
            return final_snapshot_pr_artifact_status(
                root, head, env, snapshot
            ) or "INVALID"
        complete = final_snapshot_artifact(
            root, head, snapshot, require_complete=True
        )
        if complete:
            return "CANDIDATE_COMPLETE_VALID"
        if state == "development" and final_snapshot_artifact(
            root, head, snapshot, require_complete=False
        ):
            return "CANDIDATE_SUBSET_VALID"
        return "INVALID"
    if state == "merged-pending-final-main-certification":
        return (
            "MAIN_MERGE_VALID"
            if final_snapshot_main_artifact(root, head, snapshot)
            else "INVALID"
        )
    if state in ("tagged-pending-tag-certification", "tagged"):
        return (
            "TAGGED_MAIN_VALID"
            if final_snapshot_main_artifact(root, head, snapshot)
            else "INVALID"
        )
    return "INVALID"


def _successor_commit_identity_matches(
    root: Path,
    head: str,
    progression: SecurityToolchainProgression,
) -> bool:
    """Bind a closed successor to raw Git identity, instant, and offset."""
    version = int(progression.values["SCHEMA"].rsplit("/v", 1)[1])
    if version < 3:
        return True
    completed = run_git(
        root, ["cat-file", "commit", head],
        allow_failure=True,
    )
    if completed.returncode:
        return False
    headers, separator, message = completed.stdout.partition("\n\n")
    if not separator:
        return False
    parsed: dict[str, tuple[str, str, int, str]] = {}
    for role in ("author", "committer"):
        matching = [
            line
            for line in headers.splitlines()
            if line.startswith(f"{role} ")
        ]
        if len(matching) != 1:
            return False
        match = re.fullmatch(
            rf"{role} (.+) <([^<>]+)> ([0-9]+) ([+-])([0-9]{{2}})([0-9]{{2}})",
            matching[0],
        )
        if match is None:
            return False
        offset_hours, offset_minutes = int(match.group(5)), int(match.group(6))
        if offset_hours > 23 or offset_minutes > 59:
            return False
        offset = f"{match.group(4)}{match.group(5)}{match.group(6)}"
        parsed[role] = (
            match.group(1), match.group(2), int(match.group(3)), offset
        )

    def expected_identity(role: str) -> Optional[tuple[str, str, int, str]]:
        prefix = f"SUCCESSOR_{role.upper()}"
        timestamp = _git_timestamp_identity(
            progression.values[f"{prefix}_DATE"]
        )
        if timestamp is None:
            return None
        return (
            progression.values[f"{prefix}_NAME"],
            progression.values[f"{prefix}_EMAIL"],
            timestamp[0],
            timestamp[1],
        )

    expected_author = expected_identity("author")
    expected_committer = expected_identity("committer")
    subject = message.splitlines()[0] if message.splitlines() else ""
    return bool(
        expected_author is not None
        and expected_committer is not None
        and parsed["author"] == expected_author
        and parsed["committer"] == expected_committer
        and subject == progression.values["SUCCESSOR_SUBJECT"]
    )


def _git_timestamp_identity(value: str) -> Optional[tuple[int, str]]:
    """Normalize strict ISO Git dates to stored epoch and numeric offset."""
    match = re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:Z|[+-]\d{2}:\d{2})",
        value,
    )
    if match is None:
        return None
    try:
        parsed_date = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    offset = parsed_date.utcoffset()
    if offset is None or offset.total_seconds() % 60:
        return None
    suffix = value[-6:] if value[-1] != "Z" else "+00:00"
    offset_hours, offset_minutes = int(suffix[1:3]), int(suffix[4:6])
    if offset_hours > 23 or offset_minutes > 59:
        return None
    return int(parsed_date.timestamp()), suffix[0] + suffix[1:3] + suffix[4:6]


def _progression_predecessor_key(version: int) -> str:
    """Return the exact predecessor identity owned by a progression version."""
    return {
        1: "HISTORICAL_H1",
        2: "PUBLISHED_H2",
        3: "PUBLISHED_H3",
        4: "PUBLISHED_H4",
        5: "PUBLISHED_H5",
    }[version]


def progressed_h2_artifact(
    root: Path,
    head: str,
    progression: SecurityToolchainProgression,
) -> bool:
    """Require one exact child of the schema-selected predecessor and delta."""
    version = int(progression.values["SCHEMA"].rsplit("/v", 1)[1])
    predecessor = progression.values[_progression_predecessor_key(version)]
    if (
        head == predecessor
        or commit_parents(root, head) != [predecessor]
        or not _successor_commit_identity_matches(root, head, progression)
    ):
        return False
    completed = run_git(
        root,
        [
            "diff-tree", "--no-commit-id", "--name-status", "-r", "-z",
            "--no-renames", predecessor, head,
        ],
        allow_failure=True,
    )
    if completed.returncode:
        return False
    parts = completed.stdout.split("\0")
    if parts and parts[-1] == "":
        parts.pop()
    if len(parts) % 2:
        return False
    status_paths = tuple(
        (parts[index], parts[index + 1])
        for index in range(0, len(parts), 2)
    )
    expected_status_paths = tuple(
        (status_value, path)
        for status_value, _, path in progression.delta
    )
    if status_paths != expected_status_paths:
        return False
    old_inventory = _tree_inventory(root, predecessor)
    new_inventory = _tree_inventory(root, head)
    if old_inventory is None or new_inventory is None:
        return False
    for status_value, mode, path in progression.delta:
        new_entry = new_inventory.get(path)
        if new_entry != (mode, "blob"):
            return False
        if status_value == "M" and old_inventory.get(path) != (mode, "blob"):
            return False
        if status_value == "A" and path in old_inventory:
            return False
    return True


def security_progression_status(
    root: Path,
    head: str,
    progression: Optional[SecurityToolchainProgression],
) -> str:
    """Classify only the exact historical or progressed structural artifact."""
    if progression is None:
        return "NOT_SELECTED"
    historical_h1 = progression.values["HISTORICAL_H1"]
    if head == historical_h1:
        return "HISTORICAL_H1_UNPATCHED"
    version = int(progression.values["SCHEMA"].rsplit("/v", 1)[1])
    if version >= 2 and head == progression.values["PUBLISHED_H2"]:
        return "PUBLISHED_H2_STRUCTURAL_ONLY_ORIGINAL_CI_FAILED"
    if version >= 3 and head == progression.values["PUBLISHED_H3"]:
        return "PUBLISHED_H3_STRUCTURAL_ONLY_ORIGINAL_QUALITY_FAILED"
    if version >= 4 and head == progression.values["PUBLISHED_H4"]:
        return "PUBLISHED_H4_STRUCTURAL_ONLY_ORIGINAL_QUALITY_FAILED"
    if version >= 5 and head == progression.values["PUBLISHED_H5"]:
        return "PUBLISHED_H5_STRUCTURAL_ONLY_CI_CODEQL_PASSED_CODACY_1_NEW"
    if progressed_h2_artifact(root, head, progression):
        return {
            1: "PROGRESSED_H2_STRUCTURAL_ONLY",
            2: "H2_SUCCESSOR_STRUCTURAL_ONLY",
            3: "H3_SUCCESSOR_STRUCTURAL_ONLY",
            4: "H4_SUCCESSOR_STRUCTURAL_ONLY",
            5: "H5_SUCCESSOR_STRUCTURAL_ONLY",
        }[version]
    parents = commit_parents(root, head)
    if (
        len(parents) == 2
        and parents[0] == progression.values["BASE_C"]
        and progressed_h2_artifact(root, parents[1], progression)
        and commit_tree(root, head) == commit_tree(root, parents[1])
    ):
        return {
            1: "PROGRESSED_H2_STRUCTURAL_ONLY",
            2: "H2_SUCCESSOR_STRUCTURAL_ONLY",
            3: "H3_SUCCESSOR_STRUCTURAL_ONLY",
            4: "H4_SUCCESSOR_STRUCTURAL_ONLY",
            5: "H5_SUCCESSOR_STRUCTURAL_ONLY",
        }[version]
    available = run_git(
        root, ["cat-file", "-e", f"{head}^{{commit}}"], allow_failure=True
    ).returncode == 0
    return "INVALID" if available else "UNAVAILABLE"


def strict_terminal_push_context(
    root: Path,
    version: str,
    head: str,
    branch: str,
    env: dict[str, str],
    canonical_repository: str,
    accepted_c: str,
    progression: Optional[SecurityToolchainProgression] = None,
) -> bool:
    """Validate the fixed-C terminal branch creation or update push."""
    terminal_branch = f"release/v{version}-terminal-serialization"
    expected_ref = f"refs/heads/{terminal_branch}"
    payload = _load_event(env["GITHUB_EVENT_PATH"]) if env["GITHUB_EVENT_PATH"] else None
    before = payload.get("before") if payload else None
    created = payload.get("created") if payload else None
    if progression is None:
        expected_before = accepted_c
    else:
        version_number = int(progression.values["SCHEMA"].rsplit("/v", 1)[1])
        predecessor_key = _progression_predecessor_key(version_number)
        expected_before = progression.values[predecessor_key]
    expected_repository_id = (
        progression.values["REPOSITORY_ID"] if progression is not None else None
    )
    transition_matches = (
        before == expected_before and created is False
        if progression is not None
        else (before == "0" * 40 and created is True)
        or (before == accepted_c and created is False)
    )
    return bool(
        env["GITHUB_ACTIONS"] == "true"
        and env["GITHUB_EVENT_NAME"] == "push"
        and env["GITHUB_REF"] == expected_ref
        and env["GITHUB_REF_NAME"] == terminal_branch
        and env["GITHUB_REF_TYPE"] == "branch"
        and env["GITHUB_REPOSITORY"] == canonical_repository
        and (
            expected_repository_id is None
            or env.get("GITHUB_REPOSITORY_ID") == expected_repository_id
        )
        and env["GITHUB_SHA"] == head
        and branch in ("", terminal_branch)
        and payload
        and payload.get("ref") == expected_ref
        and payload.get("after") == head
        and transition_matches
        and payload.get("deleted") is False
        and payload.get("forced") is False
        and _repository_name(payload.get("repository")) == canonical_repository
        and (
            expected_repository_id is None
            or _repository_id(payload.get("repository")) == int(expected_repository_id)
        )
        and terminal_candidate_artifact(root, head, accepted_c, progression)
        and (progression is None or progressed_h2_artifact(root, head, progression))
    )


def strict_terminal_pr_context(
    root: Path,
    version: str,
    head: str,
    branch: str,
    env: dict[str, str],
    canonical_repository: str,
    accepted_c: str,
    progression: Optional[SecurityToolchainProgression] = None,
    diagnostic: Optional[DiagnosticRecorder] = None,
    diagnostic_site: str = "terminal-pr",
) -> bool:
    """Validate direct-head or synthetic same-repository terminal PR checkout."""
    terminal_branch = f"release/v{version}-terminal-serialization"
    if not strict_release_pr_context(
        root,
        version,
        head,
        env,
        canonical_repository,
        branch=branch,
        diagnostic=diagnostic,
        diagnostic_site=diagnostic_site,
        selected_head_branch=terminal_branch,
    ):
        return False
    if not (
        env["GITHUB_ACTIONS"] == "true"
        and env["GITHUB_EVENT_NAME"] == "pull_request"
        and re.fullmatch(r"refs/pull/[1-9][0-9]*/merge", env["GITHUB_REF"])
        and env["GITHUB_REF_NAME"] == env["GITHUB_REF"].removeprefix("refs/pull/")
        and env["GITHUB_HEAD_REF"] == terminal_branch
        and env["GITHUB_BASE_REF"] == "main"
        and env["GITHUB_REPOSITORY"] == canonical_repository
        and env["GITHUB_SHA"] == head
        and env["GITHUB_EVENT_PATH"]
    ):
        return False
    payload = _load_event(env["GITHUB_EVENT_PATH"])
    pull_request = payload.get("pull_request") if payload else None
    number = payload.get("number") if payload else None
    if not isinstance(pull_request, dict) or not isinstance(number, int) or number <= 0:
        return False
    if progression is not None and (
        number != int(progression.values["PR_NUMBER"])
        or env.get("GITHUB_REPOSITORY_ID") != progression.values["REPOSITORY_ID"]
        or _repository_id(payload.get("repository"))
        != int(progression.values["REPOSITORY_ID"])
    ):
        return False
    if env["GITHUB_REF"] != f"refs/pull/{number}/merge":
        return False
    if _repository_name(payload.get("repository")) != canonical_repository:
        return False
    base = _pr_ref_identity(pull_request.get("base"))
    pr_head = _pr_ref_identity(pull_request.get("head"))
    if not base or not pr_head:
        return False
    base_ref, base_sha, base_repo = base
    head_ref, head_sha, head_repo = pr_head
    if not (
        base == ("main", accepted_c, canonical_repository)
        and head_ref == terminal_branch
        and head_repo == canonical_repository
        and isinstance(head_sha, str)
        and NONZERO_GIT_SHA.fullmatch(head_sha)
        and terminal_candidate_artifact(root, head_sha, accepted_c, progression)
        and (progression is None or progressed_h2_artifact(root, head_sha, progression))
    ):
        return False
    if head == head_sha:
        return branch in ("", terminal_branch)
    merge_sha = pull_request.get("merge_commit_sha")
    return bool(
        branch == ""
        and "merge_commit_sha" in pull_request
        and (
            merge_sha is None
            or (
                isinstance(merge_sha, str)
                and NONZERO_GIT_SHA.fullmatch(merge_sha)
            )
        )
        and commit_parents(root, head) == [accepted_c, head_sha]
        and commit_tree(root, head) == commit_tree(root, head_sha)
    )


def terminal_main_artifact(
    root: Path,
    head: str,
    accepted_c: str,
    progression: Optional[SecurityToolchainProgression] = None,
) -> bool:
    """Validate S with ordered parents [C,Hts] and candidate tree equality."""
    parents = commit_parents(root, head)
    if len(parents) != 2 or parents[0] != accepted_c:
        return False
    candidate = parents[1]
    return bool(
        terminal_candidate_artifact(root, candidate, accepted_c, progression)
        and (progression is None or progressed_h2_artifact(root, candidate, progression))
        and commit_tree(root, head) == commit_tree(root, candidate)
    )


def strict_terminal_main_push_context(
    root: Path,
    head: str,
    branch: str,
    env: dict[str, str],
    canonical_repository: str,
    accepted_c: str,
    progression: Optional[SecurityToolchainProgression] = None,
) -> bool:
    """Validate the first fixed-C protected-main serialization push."""
    payload = _load_event(env["GITHUB_EVENT_PATH"]) if env["GITHUB_EVENT_PATH"] else None
    return bool(
        env["GITHUB_ACTIONS"] == "true"
        and env["GITHUB_EVENT_NAME"] == "push"
        and env["GITHUB_REF"] == "refs/heads/main"
        and env["GITHUB_REF_NAME"] == "main"
        and env["GITHUB_REF_TYPE"] == "branch"
        and env["GITHUB_REPOSITORY"] == canonical_repository
        and (
            progression is None
            or env.get("GITHUB_REPOSITORY_ID") == progression.values["REPOSITORY_ID"]
        )
        and env["GITHUB_SHA"] == head
        and branch in ("", "main")
        and payload
        and payload.get("ref") == "refs/heads/main"
        and payload.get("before") == accepted_c
        and payload.get("after") == head
        and payload.get("created") is False
        and payload.get("deleted") is False
        and payload.get("forced") is False
        and _repository_name(payload.get("repository")) == canonical_repository
        and (
            progression is None
            or _repository_id(payload.get("repository"))
            == int(progression.values["REPOSITORY_ID"])
        )
        and terminal_main_artifact(root, head, accepted_c, progression)
    )


def strict_tag_push_context(
    root: Path,
    version: str,
    head: str,
    env: dict[str, str],
    canonical_repository: str,
    *,
    branch: str = "",
    diagnostic: Optional[DiagnosticRecorder] = None,
    diagnostic_site: str = "tag-push",
) -> bool:
    """Validate the selected annotated-tag creation through one ordered path."""
    expected_ref = f"refs/tags/v{version}"
    evaluation_id: Optional[int] = None
    if diagnostic is not None:
        try:
            evaluation_id = diagnostic.begin_evaluation(
                diagnostic_site,
                env,
                head,
                branch,
                version,
                canonical_repository,
            )
        except Exception:
            diagnostic.mark_incomplete("diagnostic-construction-unavailable")

    def observed(
        value: object,
        category: str,
        *,
        expected: Optional[str] = None,
        allow: Optional[re.Pattern[str]] = None,
    ) -> dict[str, object]:
        return _bounded_identity(
            value,
            category=category,
            expected=expected,
            allow=allow,
        )

    def check(
        predicate_id: str,
        condition: Optional[bool],
        value: dict[str, object],
    ) -> bool:
        return _diagnostic_predicate(
            diagnostic, evaluation_id, predicate_id, condition, value
        )

    def finish(accepted: bool) -> bool:
        if diagnostic is not None and evaluation_id is not None:
            return diagnostic.finish_evaluation(evaluation_id, accepted)
        return accepted

    def tag_git(args: list[str]) -> Optional[ProcessResult]:
        child_env = dict(os.environ)
        child_env["GIT_NO_REPLACE_OBJECTS"] = "1"
        try:
            return run_process(
                [resolved_executable("git"), "-C", str(root), *args],
                env=child_env,
            )
        except OSError:
            if diagnostic is not None:
                diagnostic.mark_incomplete("tag-observation-unavailable")
            return None

    # diagnostic-predicate: tag.env.actions
    if not check("tag.env.actions", env["GITHUB_ACTIONS"] == "true", observed(env["GITHUB_ACTIONS"], "unexpected-actions", expected="true")):
        return finish(False)
    # diagnostic-predicate: tag.env.event
    if not check("tag.env.event", env["GITHUB_EVENT_NAME"] == "push", observed(env["GITHUB_EVENT_NAME"], "unexpected-event", expected="push")):
        return finish(False)
    # diagnostic-predicate: tag.env.full_ref
    if not check("tag.env.full_ref", env["GITHUB_REF"] == expected_ref, observed(env["GITHUB_REF"], "unexpected-full-ref", expected=expected_ref)):
        return finish(False)
    # diagnostic-predicate: tag.env.short_ref
    if not check("tag.env.short_ref", env["GITHUB_REF_NAME"] == f"v{version}", observed(env["GITHUB_REF_NAME"], "unexpected-short-ref", expected=f"v{version}")):
        return finish(False)
    # diagnostic-predicate: tag.env.ref_type
    if not check("tag.env.ref_type", env["GITHUB_REF_TYPE"] == "tag", observed(env["GITHUB_REF_TYPE"], "unexpected-ref-type", expected="tag")):
        return finish(False)
    # diagnostic-predicate: tag.env.repository
    if not check("tag.env.repository", env["GITHUB_REPOSITORY"] == canonical_repository, observed(env["GITHUB_REPOSITORY"], "unexpected-repository", expected=canonical_repository)):
        return finish(False)
    runtime_sha_valid = GIT_SHA.fullmatch(env["GITHUB_SHA"]) is not None
    # diagnostic-predicate: tag.env.runtime_sha_format
    if not check("tag.env.runtime_sha_format", runtime_sha_valid, observed(env["GITHUB_SHA"], "unexpected-runtime-sha", allow=GIT_SHA)):
        return finish(False)
    # diagnostic-predicate: tag.env.event_path
    if not check("tag.env.event_path", bool(env["GITHUB_EVENT_PATH"]), observed("present" if env["GITHUB_EVENT_PATH"] else "absent", "unexpected-event-path-state", expected="present")):
        return finish(False)

    payload = _load_event(
        env["GITHUB_EVENT_PATH"],
        recorder=diagnostic,
        evaluation_id=evaluation_id,
        version=version,
        canonical_repository=canonical_repository,
        predicate_prefix="tag",
    )
    if payload is None:
        return finish(False)

    # diagnostic-predicate: tag.payload.ref
    if not check("tag.payload.ref", payload.get("ref") == expected_ref, observed(payload.get("ref"), "unexpected-tag-ref", expected=expected_ref)):
        return finish(False)
    # diagnostic-predicate: tag.payload.before
    if not check("tag.payload.before", payload.get("before") == "0" * 40, observed(payload.get("before"), "unexpected-before", expected="0" * 40)):
        return finish(False)
    after = payload.get("after")
    after_valid = isinstance(after, str) and GIT_SHA.fullmatch(after) is not None
    # diagnostic-predicate: tag.payload.after_format
    if not check("tag.payload.after_format", after_valid, observed(after, "unexpected-after", allow=GIT_SHA)):
        return finish(False)
    # diagnostic-predicate: tag.payload.created
    if not check("tag.payload.created", payload.get("created") is True, observed(_type_name(payload.get("created")), "unexpected-created", expected="boolean")):
        return finish(False)
    # diagnostic-predicate: tag.payload.deleted
    if not check("tag.payload.deleted", payload.get("deleted") is False, observed(_type_name(payload.get("deleted")), "unexpected-deleted", expected="boolean")):
        return finish(False)
    # diagnostic-predicate: tag.payload.forced
    if not check("tag.payload.forced", payload.get("forced") is False, observed(_type_name(payload.get("forced")), "unexpected-forced", expected="boolean")):
        return finish(False)
    repository = _repository_name(payload.get("repository"))
    # diagnostic-predicate: tag.payload.repository
    if not check("tag.payload.repository", repository == canonical_repository, observed(repository, "unexpected-repository", expected=canonical_repository)):
        return finish(False)

    shallow = tag_git(["rev-parse", "--is-shallow-repository"])
    shallow_value = shallow.stdout.strip() if shallow and shallow.returncode == 0 else None
    shallow_ok: Optional[bool] = shallow_value == "false" if shallow_value is not None else None
    # diagnostic-predicate: tag.git.not_shallow
    if not check("tag.git.not_shallow", shallow_ok, observed(shallow_value, "unexpected-shallow-state", expected="false")):
        return finish(False)

    alternate_path_result = tag_git(["rev-parse", "--git-path", "objects/info/alternates"])
    alternate_path = Path(alternate_path_result.stdout.strip()) if alternate_path_result and alternate_path_result.returncode == 0 and alternate_path_result.stdout.strip() else None
    if alternate_path is not None and not alternate_path.is_absolute():
        alternate_path = root / alternate_path
    alternate_file_absent = alternate_path is not None and not alternate_path.exists()
    alternate_env_absent = not os.environ.get("GIT_ALTERNATE_OBJECT_DIRECTORIES") and not os.environ.get("GIT_OBJECT_DIRECTORY")
    alternates_ok: Optional[bool] = alternate_file_absent and alternate_env_absent if alternate_path is not None else None
    # diagnostic-predicate: tag.git.no_alternates
    if not check("tag.git.no_alternates", alternates_ok, observed("absent" if alternates_ok else "present" if alternates_ok is False else None, "unexpected-alternates-state", expected="absent")):
        return finish(False)

    graft_path_result = tag_git(["rev-parse", "--git-path", "info/grafts"])
    graft_path = Path(graft_path_result.stdout.strip()) if graft_path_result and graft_path_result.returncode == 0 and graft_path_result.stdout.strip() else None
    if graft_path is not None and not graft_path.is_absolute():
        graft_path = root / graft_path
    grafts_ok: Optional[bool] = (not graft_path.exists() or graft_path.stat().st_size == 0) if graft_path is not None else None
    # diagnostic-predicate: tag.git.no_grafts
    if not check("tag.git.no_grafts", grafts_ok, observed("absent" if grafts_ok else "present" if grafts_ok is False else None, "unexpected-grafts-state", expected="absent")):
        return finish(False)

    replacements = tag_git(["for-each-ref", "--format=%(refname)", "refs/replace"])
    replacement_env_absent = not os.environ.get("GIT_REPLACE_REF_BASE") and not os.environ.get("GIT_NAMESPACE")
    replacements_ok: Optional[bool] = (not replacements.stdout.strip() and replacement_env_absent) if replacements and replacements.returncode == 0 else None
    # diagnostic-predicate: tag.git.no_replacements
    if not check("tag.git.no_replacements", replacements_ok, observed("absent" if replacements_ok else "present" if replacements_ok is False else None, "unexpected-replacement-state", expected="absent")):
        return finish(False)

    named = tag_git(["show-ref", "--verify", "--hash", expected_ref])
    named_lines = named.stdout.splitlines() if named and named.returncode == 0 else []
    tag_object = named_lines[0] if len(named_lines) == 1 and GIT_SHA.fullmatch(named_lines[0]) else None
    named_ok: Optional[bool] = tag_object is not None if named is not None and named.returncode in (0, 1) else None
    if diagnostic is not None and evaluation_id is not None:
        diagnostic.context_update(evaluation_id, tag_object=observed(tag_object, "unexpected-tag-object", allow=GIT_SHA))
    # diagnostic-predicate: tag.git.named_ref
    if not check("tag.git.named_ref", named_ok, observed(tag_object, "unexpected-tag-object", allow=GIT_SHA)):
        return finish(False)

    tag_type_result = tag_git(["cat-file", "-t", tag_object])
    tag_type = tag_type_result.stdout.strip() if tag_type_result and tag_type_result.returncode == 0 else None
    tag_type_ok: Optional[bool] = tag_type == "tag" if tag_type is not None else None
    if diagnostic is not None and evaluation_id is not None:
        diagnostic.context_update(evaluation_id, tag_object_type=observed(tag_type, "unexpected-tag-object-type", expected="tag"))
    # diagnostic-predicate: tag.git.named_ref_type
    if not check("tag.git.named_ref_type", tag_type_ok, observed(tag_type, "unexpected-tag-object-type", expected="tag")):
        return finish(False)

    tag_size_result = tag_git(["cat-file", "-s", tag_object])
    tag_size = (
        int(tag_size_result.stdout.strip())
        if tag_size_result
        and tag_size_result.returncode == 0
        and re.fullmatch(r"[0-9]+", tag_size_result.stdout.strip())
        else None
    )
    if tag_size is None:
        check("tag.git.direct_target", None, _unavailable_identity("tag-size-unavailable"))
        return finish(False)
    if tag_size > 65536:
        check("tag.git.direct_target", False, observed("oversized", "unexpected-tag-size", expected="bounded"))
        return finish(False)
    tag_body_result = tag_git(["cat-file", "-p", tag_object])
    if tag_body_result is None or tag_body_result.returncode != 0:
        check("tag.git.direct_target", None, _unavailable_identity("tag-object-unavailable"))
        return finish(False)
    headers = tag_body_result.stdout.split("\n\n", 1)[0].splitlines()
    object_headers = [line.removeprefix("object ") for line in headers if line.startswith("object ")]
    type_headers = [line.removeprefix("type ") for line in headers if line.startswith("type ")]
    direct_target = object_headers[0] if len(object_headers) == 1 and GIT_SHA.fullmatch(object_headers[0]) else None
    direct_ok = direct_target is not None
    if diagnostic is not None and evaluation_id is not None:
        diagnostic.context_update(evaluation_id, direct_target=observed(direct_target, "unexpected-direct-target", allow=GIT_SHA))
    # diagnostic-predicate: tag.git.direct_target
    if not check("tag.git.direct_target", direct_ok, observed(direct_target, "unexpected-direct-target", allow=GIT_SHA)):
        return finish(False)
    declared_type = type_headers[0] if len(type_headers) == 1 else None
    declared_ok = declared_type == "commit"
    if diagnostic is not None and evaluation_id is not None:
        diagnostic.context_update(evaluation_id, direct_target_declared_type=observed(declared_type, "unexpected-declared-type", expected="commit"))
    # diagnostic-predicate: tag.git.direct_target_declared_type
    if not check("tag.git.direct_target_declared_type", declared_ok, observed(declared_type, "unexpected-declared-type", expected="commit")):
        return finish(False)

    direct_type_result = tag_git(["cat-file", "-t", direct_target])
    direct_type = direct_type_result.stdout.strip() if direct_type_result and direct_type_result.returncode == 0 else None
    direct_type_ok: Optional[bool] = direct_type == "commit" if direct_type is not None else None
    if diagnostic is not None and evaluation_id is not None:
        diagnostic.context_update(evaluation_id, direct_target_actual_type=observed(direct_type, "unexpected-direct-target-type", expected="commit"))
    # diagnostic-predicate: tag.git.direct_target_actual_type
    if not check("tag.git.direct_target_actual_type", direct_type_ok, observed(direct_type, "unexpected-direct-target-type", expected="commit")):
        return finish(False)
    # diagnostic-predicate: tag.git.target_equals_head
    if not check("tag.git.target_equals_head", direct_target == head, observed("equal" if direct_target == head else "different", "unexpected-target-relation", expected="equal")):
        return finish(False)

    def classify(raw: object) -> tuple[Optional[str], Optional[str]]:
        if raw == head:
            return "EXACT_COMMIT_M", head
        if raw == tag_object:
            return "EXACT_NAMED_TAG_A", direct_target
        return None, None

    runtime_class, runtime_resolved = classify(env["GITHUB_SHA"])
    if diagnostic is not None and evaluation_id is not None:
        diagnostic.context_update(
            evaluation_id,
            runtime_sha_classification=observed(runtime_class, "unexpected-runtime-class", allow=re.compile(r"(?:EXACT_COMMIT_M|EXACT_NAMED_TAG_A)")),
            runtime_sha_resolved_commit=observed(runtime_resolved, "unexpected-runtime-resolution", allow=GIT_SHA),
            event_after=observed(after, "unexpected-after", allow=GIT_SHA),
        )
    runtime_ok = runtime_resolved == head
    # diagnostic-predicate: tag.identity.runtime_sha
    if not check("tag.identity.runtime_sha", runtime_ok, observed(runtime_class, "unexpected-runtime-class", allow=re.compile(r"(?:EXACT_COMMIT_M|EXACT_NAMED_TAG_A)"))):
        return finish(False)
    after_class, after_resolved = classify(after)
    if diagnostic is not None and evaluation_id is not None:
        diagnostic.context_update(
            evaluation_id,
            event_after_classification=observed(after_class, "unexpected-after-class", allow=re.compile(r"(?:EXACT_COMMIT_M|EXACT_NAMED_TAG_A)")),
            event_after_resolved_commit=observed(after_resolved, "unexpected-after-resolution", allow=GIT_SHA),
        )
    after_ok = after_resolved == head
    # diagnostic-predicate: tag.identity.event_after
    if not check("tag.identity.event_after", after_ok, observed(after_class, "unexpected-after-class", allow=re.compile(r"(?:EXACT_COMMIT_M|EXACT_NAMED_TAG_A)"))):
        return finish(False)

    parents_result = tag_git(["rev-list", "--parents", "-n", "1", head])
    parent_parts = parents_result.stdout.strip().split() if parents_result and parents_result.returncode == 0 else []
    parents = parent_parts[1:] if parent_parts and parent_parts[0] == head else []
    parents_distinct = len(parents) == 2 and parents[0] != parents[1]
    ancestry_result = tag_git(["merge-base", "--is-ancestor", parents[0], parents[1]]) if parents_distinct else None
    ancestry_ok: Optional[bool] = ancestry_result.returncode == 0 if ancestry_result and ancestry_result.returncode in (0, 1) else None
    topology_ok: Optional[bool] = (
        parents_distinct and ancestry_ok
        if parents_result and parents_result.returncode == 0
        else None
    )
    if diagnostic is not None and evaluation_id is not None:
        diagnostic.context_update(evaluation_id, checkout_parents=parents[:2])
    # diagnostic-predicate: tag.topology.parents_and_ancestry
    if not check("tag.topology.parents_and_ancestry", topology_ok, observed("ordered-normal-merge" if topology_ok else "different" if topology_ok is False else None, "unexpected-topology", expected="ordered-normal-merge")):
        return finish(False)

    head_tree_result = tag_git(["rev-parse", f"{head}^{{tree}}"])
    second_tree_result = tag_git(["rev-parse", f"{parents[1]}^{{tree}}"])
    first_tree_result = tag_git(["rev-parse", f"{parents[0]}^{{tree}}"])
    head_tree = head_tree_result.stdout.strip() if head_tree_result and head_tree_result.returncode == 0 and GIT_SHA.fullmatch(head_tree_result.stdout.strip()) else None
    second_tree = second_tree_result.stdout.strip() if second_tree_result and second_tree_result.returncode == 0 and GIT_SHA.fullmatch(second_tree_result.stdout.strip()) else None
    first_tree = first_tree_result.stdout.strip() if first_tree_result and first_tree_result.returncode == 0 and GIT_SHA.fullmatch(first_tree_result.stdout.strip()) else None
    trees_ok: Optional[bool] = head_tree == second_tree if head_tree is not None and second_tree is not None else None
    if diagnostic is not None and evaluation_id is not None:
        diagnostic.context_update(
            evaluation_id,
            checkout_tree=observed(head_tree, "unexpected-tree", allow=GIT_SHA),
            first_parent_tree=observed(first_tree, "unexpected-tree", allow=GIT_SHA),
            second_parent_tree=observed(second_tree, "unexpected-tree", allow=GIT_SHA),
        )
    # diagnostic-predicate: tag.topology.tree
    check("tag.topology.tree", trees_ok, observed("equal" if trees_ok else "different" if trees_ok is False else None, "unexpected-tree-relation", expected="equal"))
    return finish(trees_ok is True)


def _same_repository_pr(
    env: dict[str, str],
) -> Optional[tuple[object, object, object]]:
    """Return validated base, head, and repository identity for one PR."""
    if env["GITHUB_EVENT_NAME"] != "pull_request":
        return None
    if not env["GITHUB_REF"].startswith("refs/pull/"):
        return None
    event_path = env["GITHUB_EVENT_PATH"]
    repository = env["GITHUB_REPOSITORY"]
    head = env["GITHUB_HEAD_REF"]
    if not event_path or not repository or not head:
        return None
    identity = _post_release_pr_identity(event_path)
    if identity is None or identity[1] != head or identity[2] != repository:
        return None
    return identity


def accepted_recovery_pr(version: str, env: dict[str, str]) -> bool:
    """Accept a same-repository merged-state recovery PR to main."""
    identity = _same_repository_pr(env)
    prefix = f"recovery/v{version}-"
    return bool(
        identity
        and identity[0] == "main"
        and str(identity[1]).startswith(prefix)
    )


def _post_release_pr_identity(event_path: str) -> Optional[tuple[object, object, object]]:
    """Load one bounded PR identity tuple, returning none on invalid input."""
    try:
        payload = json.loads(Path(event_path).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    pull_request = payload.get("pull_request")
    if not isinstance(pull_request, dict):
        return None
    base = pull_request.get("base")
    if not isinstance(base, dict):
        return None
    head = pull_request.get("head")
    if not isinstance(head, dict):
        return None
    head_repository = head.get("repo")
    if not isinstance(head_repository, dict):
        return None
    return base.get("ref"), head.get("ref"), head_repository.get("full_name")


def accepted_post_release_pr(version: str, env: dict[str, str]) -> bool:
    """Validate a same-repository release or closure PR."""
    identity = _same_repository_pr(env)
    allowed = {
        f"release/v{version}",
        f"release/v{version}-post-publication-closure",
    }
    return bool(identity and identity[0] == "main" and identity[1] in allowed)


def phases_complete(phase_doc: Optional[Document]) -> bool:
    """Return whether all recognized phases are explicitly complete."""
    if phase_doc is None:
        return False
    numbers, states = parse_phase_states(phase_doc, "Status")
    return bool(numbers) and all(
        states.get(number, (None, 0))[0] == "Complete" for number in numbers
    )


def v1_context_scope(
    root: Path,
    version: str,
    state: str,
    context: tuple[str, str, bool, bool, Optional[str]],
    env: dict[str, str],
    declaration: LifecycleDeclaration,
    diagnostic: Optional[DiagnosticRecorder] = None,
    diagnostic_site: str = "context",
    terminal_disposition: Optional[TerminalDisposition] = None,
    security_progression: Optional[SecurityToolchainProgression] = None,
    final_snapshot: Optional[FinalSourceSnapshot] = None,
) -> Optional[str]:
    """Return bounded structural evidence scope for one v1 state."""
    head, branch, _, _, _ = context
    release_branch = f"release/v{version}"
    closure_branch = f"release/v{version}-post-publication-closure"
    any_github = github_context_present(env)
    if state == "post-release-terminal-serialization-candidate":
        if terminal_disposition is None:
            return None
        accepted_c = terminal_disposition.values["ACCEPTED_C"]
        terminal_branch = f"release/v{version}-terminal-serialization"
        progression_version = (
            int(security_progression.values["SCHEMA"].rsplit("/v", 1)[1])
            if security_progression is not None else 0
        )
        closed_successor = progression_version >= 2
        successor_name = {2: "H2", 3: "H3", 4: "H4", 5: "H5"}.get(
            progression_version, "H2"
        )
        if (
            not any_github
            and branch == terminal_branch
            and terminal_candidate_artifact(
                root, head, accepted_c, security_progression
            )
            and (
                not closed_successor
                or progressed_h2_artifact(root, head, security_progression)
            )
        ):
            return (
                f"LOCAL_{successor_name}_SUCCESSOR_CANDIDATE" if closed_successor
                else "LOCAL_TERMINAL_SERIALIZATION_CANDIDATE"
            )
        if strict_terminal_push_context(
            root, version, head, branch, env,
            declaration.canonical_repository, accepted_c, security_progression,
        ):
            return (
                f"GITHUB_{successor_name}_SUCCESSOR_PUSH_CONTEXT_CONSISTENCY"
                if closed_successor
                else "GITHUB_TERMINAL_SERIALIZATION_PUSH_CONTEXT_CONSISTENCY"
            )
        if strict_terminal_pr_context(
            root, version, head, branch, env,
            declaration.canonical_repository, accepted_c,
            security_progression, diagnostic, diagnostic_site,
        ):
            if closed_successor:
                payload = _load_event(env["GITHUB_EVENT_PATH"])
                pull_request = payload.get("pull_request") if payload else None
                pr_head = _pr_ref_identity(pull_request.get("head")) if isinstance(pull_request, dict) else None
                return (
                    f"GITHUB_{successor_name}_SUCCESSOR_PR_DIRECT_CONTEXT_CONSISTENCY"
                    if pr_head is not None and head == pr_head[1]
                    else f"GITHUB_{successor_name}_SUCCESSOR_PR_SYNTHETIC_CONTEXT_CONSISTENCY"
                )
            return "GITHUB_TERMINAL_SERIALIZATION_PR_CONTEXT_CONSISTENCY"
        return None
    if state == "post-release-terminal-serialization-main":
        if terminal_disposition is None:
            return None
        accepted_c = terminal_disposition.values["ACCEPTED_C"]
        progression_version = (
            int(security_progression.values["SCHEMA"].rsplit("/v", 1)[1])
            if security_progression is not None else 0
        )
        closed_successor = progression_version >= 2
        successor_name = {2: "H2", 3: "H3", 4: "H4", 5: "H5"}.get(
            progression_version, "H2"
        )
        if (
            not any_github
            and branch == "main"
            and terminal_main_artifact(
                root, head, accepted_c, security_progression
            )
        ):
            return (
                f"LOCAL_{successor_name}_SUCCESSOR_MAIN_ARTIFACT_ONLY"
                if closed_successor
                else "LOCAL_TERMINAL_SERIALIZATION_MAIN_ARTIFACT_ONLY"
            )
        if strict_terminal_main_push_context(
            root, head, branch, env,
            declaration.canonical_repository, accepted_c, security_progression,
        ):
            return (
                f"GITHUB_{successor_name}_SUCCESSOR_MAIN_CONTEXT_CONSISTENCY"
                if closed_successor
                else "GITHUB_TERMINAL_SERIALIZATION_MAIN_CONTEXT_CONSISTENCY"
            )
        return None
    if state in ("development", "pre-release"):
        snapshot_ok = bool(
            final_snapshot is None
            or final_snapshot_artifact(
                root,
                head,
                final_snapshot,
                require_complete=state == "pre-release",
            )
        )
        if not any_github and branch == release_branch and snapshot_ok:
            return "LOCAL_RELEASE_BRANCH"
        if snapshot_ok and strict_release_push_context(
            version,
            head,
            env,
            declaration.canonical_repository,
            branch=branch,
            diagnostic=diagnostic,
            diagnostic_site=f"{diagnostic_site}-release-push",
        ):
            return "GITHUB_RELEASE_PUSH_CONTEXT_CONSISTENCY"
        if (
            strict_release_pr_context(
                root,
                version,
                head,
                env,
                declaration.canonical_repository,
                branch=branch,
                diagnostic=diagnostic,
                diagnostic_site=diagnostic_site,
            )
            and (
                final_snapshot is None
                or final_snapshot_pr_artifact_status(
                    root, head, env, final_snapshot
                ) is not None
            )
        ):
            return "GITHUB_PR_EVENT_CONTEXT_CONSISTENCY"
        return None
    if state == "merged-pending-final-main-certification":
        snapshot_main_ok = bool(
            final_snapshot is None
            or final_snapshot_main_artifact(root, head, final_snapshot)
        )
        if (
            not any_github
            and branch == "main"
            and normal_merge_artifact(root, head)
            and snapshot_main_ok
        ):
            return "LOCAL_ARTIFACT_ONLY"
        if snapshot_main_ok and strict_main_push_context(
            root, head, env, declaration.canonical_repository
        ):
            return "GITHUB_MAIN_PUSH_CONTEXT_CONSISTENCY"
        return None
    if state == "merged-not-tagged":
        if not any_github and (
            branch == "main" or branch.startswith(f"recovery/v{version}-")
        ):
            return "LOCAL_RECONCILED_ARTIFACT"
        if strict_main_push_context(
            root, head, env, declaration.canonical_repository
        ):
            return "GITHUB_MAIN_PUSH_CONTEXT_CONSISTENCY"
        if accepted_recovery_pr(version, env):
            return "GITHUB_RECOVERY_PR_CONTEXT_CONSISTENCY"
        return None
    if state in ("tagged-pending-tag-certification", "tagged"):
        snapshot_main_ok = bool(
            final_snapshot is None
            or final_snapshot_main_artifact(root, head, final_snapshot)
        )
        if not any_github:
            if state == "tagged-pending-tag-certification" and not normal_merge_artifact(
                root, head
            ):
                return None
            return "LOCAL_TAG_ARTIFACT_ONLY" if snapshot_main_ok else None
        if snapshot_main_ok and strict_tag_push_context(
            root,
            version,
            head,
            env,
            declaration.canonical_repository,
            branch=branch,
            diagnostic=diagnostic,
            diagnostic_site=f"{diagnostic_site}-tag-push",
        ):
            if state == "tagged-pending-tag-certification" and not normal_merge_artifact(
                root, head
            ):
                return None
            return "GITHUB_TAG_EVENT_CONTEXT_CONSISTENCY"
        return None
    if state in (
        "post-release-pending-closure",
        "post-release-closure-candidate",
    ):
        if not any_github and branch in ("main", release_branch, closure_branch):
            return "LOCAL_CLOSURE_ARTIFACT_ONLY"
        if strict_closure_push_context(
            root,
            version,
            head,
            context,
            env,
            declaration.canonical_repository,
        ):
            return "GITHUB_CLOSURE_PUSH_CONTEXT_CONSISTENCY"
        if strict_main_push_context(
            root, head, env, declaration.canonical_repository
        ):
            return "GITHUB_CLOSURE_MAIN_CONTEXT_CONSISTENCY"
        if strict_post_release_pr_context(
            root, version, head, env, declaration.canonical_repository
        ):
            return "GITHUB_CLOSURE_PR_CONTEXT_CONSISTENCY"
        return None
    return None


def git_context_matches(
    version: str,
    state: str,
    branch: str,
    env: dict[str, str],
    *,
    root: Optional[Path] = None,
    context: Optional[tuple[str, str, bool, bool, Optional[str]]] = None,
    declaration: Optional[LifecycleDeclaration] = None,
    diagnostic: Optional[DiagnosticRecorder] = None,
    diagnostic_site: str = "ckrs016",
    terminal_disposition: Optional[TerminalDisposition] = None,
    security_progression: Optional[SecurityToolchainProgression] = None,
    final_snapshot: Optional[FinalSourceSnapshot] = None,
) -> bool:
    """Return whether branch and GitHub refs are compatible with lifecycle."""
    if declaration is not None:
        return bool(
            root
            and context
            and v1_context_scope(
                root,
                version,
                state,
                context,
                env,
                declaration,
                diagnostic,
                diagnostic_site,
                terminal_disposition,
                security_progression,
                final_snapshot,
            )
        )
    release_branch = f"release/v{version}"
    if state in ("development", "pre-release"):
        return branch == release_branch or accepted_release_pr(version, env)
    if state == "merged-not-tagged":
        return (
            branch == "main"
            or branch.startswith(f"recovery/v{version}-")
            or env["GITHUB_REF"] == "refs/heads/main"
            or accepted_recovery_pr(version, env)
        )
    if state in ("post-release-pending-closure", "post-release-closed"):
        return (
            branch in (
                "main",
                release_branch,
                f"release/v{version}-post-publication-closure",
            )
            or env["GITHUB_REF"] == "refs/heads/main"
            or accepted_post_release_pr(version, env)
        )
    return state in ("tagged", "released")


def passed_gate(gate: Document) -> bool:
    """Return whether any unambiguous gate status is passed."""
    return any(
        re.match(r"^Passed(?:\b|\s|—)", value)
        for _, value in gate.metadata("Status")
    )


def train_marker_index(doc: Document) -> Optional[int]:
    """Locate the exact release-train historical boundary."""
    marker = "## Historical proposed continuation and final disposition"
    return next(
        (index for index, line in enumerate(doc.lines) if line == marker),
        None,
    )


def current_train_definitions(
    doc: Document,
    version: str,
    marker_index: int,
) -> list[tuple[int, str]]:
    """Return current definitions of one version above the boundary."""
    pattern = re.compile(rf"^### `v{re.escape(version)}\s+—\s+(.+)`$")
    return [
        (index + 1, match.group(1))
        for index, line in enumerate(doc.lines[:marker_index])
        if (match := pattern.match(line))
    ]


def train_definition_invalid(
    current: list[tuple[int, str]],
    title: Optional[str],
) -> bool:
    """Return whether one current definition with the expected title is absent."""
    return len(current) != 1 or bool(title and current and current[0][1] != title)


def unlabeled_historical_definitions(
    doc: Document,
    version: str,
    marker_index: int,
) -> list[int]:
    """Return lines of retired definitions missing their historical label."""
    pattern = re.compile(rf"^### `v{re.escape(version)}\s+—")
    return [
        index
        for index, line in enumerate(
            doc.lines[marker_index + 1 :],
            marker_index + 2,
        )
        if pattern.match(line) and "Historical proposed" not in line
    ]


def exact_tag_matches_head(
    head: str,
    exists: bool,
    annotated: bool,
    target: Optional[str],
) -> bool:
    """Return whether the exact annotated tag peels to HEAD."""
    return exists and annotated and target == head


def strict_git_ancestor(root: Path, ancestor: str, descendant: str) -> bool:
    """Return whether one commit is a strict Git ancestor of another."""
    if ancestor == descendant:
        return False
    completed = run_git(
        root,
        ["merge-base", "--is-ancestor", ancestor, descendant],
        allow_failure=True,
    )
    if completed.returncode in (0, 1):
        return completed.returncode == 0
    detail = completed.stderr.strip() or completed.stdout.strip() or "git merge-base failed"
    raise InternalError("git", detail)


def main_context(branch: str, env: dict[str, str]) -> bool:
    """Return whether local or GitHub context identifies main."""
    return branch == "main" or env["GITHUB_REF"] == "refs/heads/main"


def gate_status_exact(gate: Document, expected: str) -> bool:
    """Return whether a gate has one exact lifecycle status."""
    return [value for _, value in gate.metadata("Status")] == [expected]


def gate_verdict_present(gate: Document, *, allow_pending: bool = False) -> bool:
    """Return whether the final verdict is nonempty and suitably final."""
    verdict = gate.section("## Final verdict")
    if verdict is None:
        return False
    lines = verdict[1]
    if lines and lines[0] == "## Final verdict":
        lines = lines[1:]
    text = "\n".join(lines).strip()
    return bool(text) and (
        allow_pending or re.search(r"\bpending\b", text, re.I) is None
    )


def candidate_gate_valid(
    gate: Document,
    state: str,
    progression_valid: bool,
    boundaries: Optional[LifecycleBoundaries],
    declaration: Optional[LifecycleDeclaration] = None,
) -> bool:
    """Return whether a candidate gate matches its exact lifecycle state."""
    if not progression_valid:
        return False
    if boundaries is None:
        statuses = [value for _, value in gate.metadata("Status")]
        legacy_passed = (
            len(statuses) == 1 and statuses[0].startswith("Passed")
        )
        return legacy_passed and gate_verdict_present(gate)
    if declaration and state in (
        "pre-release",
        "merged-pending-final-main-certification",
        "tagged-pending-tag-certification",
    ):
        return gate_status_exact(
            gate, "Passed — pre-merge prerequisites complete"
        ) and gate_verdict_present(gate)
    expected = {
        "pre-release": "Passed — pre-merge prerequisites complete",
        "merged-not-tagged": "Passed — pre-publication prerequisites complete",
        "tagged": "Passed — pre-publication prerequisites complete",
    }.get(state)
    return bool(
        expected
        and gate_status_exact(gate, expected)
        and gate_verdict_present(gate)
    )
