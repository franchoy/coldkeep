"""Private support primitives for the Coldkeep release-state validator."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
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
SUPPORTED_LIFECYCLE_DECLARATION = "immutable-transition-v1"
GIT_SHA = re.compile(r"^[0-9a-f]{40}$")
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


DIAGNOSTIC_SCHEMA = "coldkeep-release-state-diagnostic/v1"
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
    ("pr.payload.merge_value", "event-payload", "merge_commit_sha is null or exactly the checkout SHA"),
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
        } and reason not in self.incomplete_reasons:
            self.incomplete_reasons.append(reason)

    def bind_sources(self, root: Path, validator_path: Path) -> None:
        """Measure loaded implementation bytes once at startup."""
        self.root = root
        try:
            self.checkout_commit = run_git(root, ["rev-parse", "HEAD"]).stdout.strip()
            self.checkout_tree = run_git(root, ["rev-parse", "HEAD^{tree}"]).stdout.strip()
            loaded = {
                "scripts/release_state_support.py": Path(__file__).resolve(),
                "scripts/validate_release_state.py": validator_path.resolve(),
            }
            records = []
            attribution_complete = True
            for relative in self._SOURCE_PATHS:
                actual_path = loaded[relative]
                expected_path = (root / relative).resolve()
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
                for relative in self._SOURCE_PATHS
            ]
            self.mark_incomplete("source-attribution-unavailable")

    def _runtime_context(
        self,
        env: dict[str, str],
        head: str,
        branch: str,
        version: str,
        canonical_repository: str,
    ) -> dict[str, object]:
        sha_pattern = GIT_SHA
        ref_pattern = re.compile(r"refs/pull/[1-9][0-9]*/merge")
        short_ref_pattern = re.compile(r"[1-9][0-9]*/merge")
        return {
            "github_actions": _bounded_identity(env.get("GITHUB_ACTIONS"), category="unexpected-actions", expected="true"),
            "event_name": _bounded_identity(env.get("GITHUB_EVENT_NAME"), category="unexpected-event", expected="pull_request"),
            "full_ref": _bounded_identity(env.get("GITHUB_REF"), category="unexpected-full-ref", allow=ref_pattern),
            "short_ref": _bounded_identity(env.get("GITHUB_REF_NAME"), category="unexpected-short-ref", allow=short_ref_pattern),
            "head_ref": _bounded_identity(env.get("GITHUB_HEAD_REF"), category="unexpected-head-ref", expected=f"release/v{version}"),
            "base_ref": _bounded_identity(env.get("GITHUB_BASE_REF"), category="unexpected-base-ref", expected="main"),
            "repository": _bounded_identity(env.get("GITHUB_REPOSITORY"), category="unexpected-repository", expected=canonical_repository),
            "runtime_sha": _bounded_identity(env.get("GITHUB_SHA"), category="unexpected-runtime-sha", allow=sha_pattern),
            "actual_checkout_commit": _bounded_identity(head, category="unexpected-checkout", allow=sha_pattern),
            "actual_branch": _bounded_identity(branch, category="unexpected-branch", expected="") if branch == "" else _bounded_identity(branch, category="unexpected-branch", expected=f"release/v{version}"),
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
    ) -> int:
        evaluation_id = len(self.evaluations) + 1
        self.evaluations.append({
            "id": evaluation_id,
            "site": site,
            "sequence": evaluation_id,
            "event_snapshot_id": None,
            "runtime_context": self._runtime_context(env, head, branch, version, canonical_repository),
            "result": "UNAVAILABLE",
            "first_rejecting_predicate": None,
            "predicates": [],
        })
        return evaluation_id

    def predicate(
        self,
        evaluation_id: int,
        predicate_id: str,
        passed: bool,
        observed: dict[str, object],
    ) -> bool:
        try:
            evaluation = self.evaluations[evaluation_id - 1]
            spec = next(
                item
                for item in (*PR_PREDICATES, *RELEASE_PUSH_PREDICATES)
                if item[0] == predicate_id
            )
            status = "PASS" if passed else "FAIL"
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
            if not passed and evaluation["first_rejecting_predicate"] is None:
                evaluation["first_rejecting_predicate"] = predicate_id
            return passed
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
            for key in ("checkout_parents", "checkout_tree", "payload_head_tree", "object_availability"):
                if key in values:
                    context[key] = values[key]
        except Exception:
            self.mark_incomplete("predicate-recording-unavailable")

    def finish_evaluation(self, evaluation_id: int, accepted: bool) -> bool:
        try:
            evaluation = self.evaluations[evaluation_id - 1]
            seen = {item["id"] for item in evaluation["predicates"]}
            specs = (
                RELEASE_PUSH_PREDICATES
                if str(evaluation["site"]).endswith("release-push")
                else PR_PREDICATES
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
            evaluation["result"] = "PASS" if accepted else "FAIL"
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
        return {
            "schema": DIAGNOSTIC_SCHEMA,
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
            },
            "attribution": {
                "repository": repository,
                "run_id": _bounded_identity(self.environment.get("GITHUB_RUN_ID"), category="unexpected-run-id", allow=digits),
                "run_attempt": _bounded_identity(self.environment.get("GITHUB_RUN_ATTEMPT"), category="unexpected-run-attempt", allow=digits),
                "job": _bounded_identity(self.environment.get("GITHUB_JOB"), category="unexpected-job", expected="quality"),
                "workflow": _bounded_identity(self.environment.get("GITHUB_WORKFLOW"), category="unexpected-workflow", expected="CI"),
                "event_name": _bounded_identity(self.environment.get("GITHUB_EVENT_NAME"), category="unexpected-event", expected="pull_request"),
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
            allow=GIT_SHA,
            allow_null=merge_present,
        ) if merge_present else _unavailable_identity("absent"),
    }


def _diagnostic_predicate(
    recorder: Optional[DiagnosticRecorder],
    evaluation_id: Optional[int],
    predicate_id: str,
    passed: bool,
    observed: dict[str, object],
) -> bool:
    """Record a predicate without permitting diagnostics to alter its value."""
    if recorder is not None and evaluation_id is not None:
        try:
            recorder.predicate(evaluation_id, predicate_id, passed, observed)
        except Exception:
            recorder.mark_incomplete("predicate-recording-unavailable")
    return passed


def _load_event(
    path: str,
    *,
    recorder: Optional[DiagnosticRecorder] = None,
    evaluation_id: Optional[int] = None,
    version: str = "",
    canonical_repository: str = "",
    predicate_prefix: str = "pr",
) -> Optional[dict[str, object]]:
    """Load the exact event byte snapshot consumed by one evaluation."""
    path_bytes = path.encode("utf-8", errors="surrogatepass")
    basename = Path(path).name
    snapshot: dict[str, object] = {
        "id": 0,
        "selector": "GITHUB_EVENT_PATH",
        "path_basename": _bounded_identity(
            basename,
            category="unexpected-event-basename",
            expected="event.json",
        ),
        "path_sha256": hashlib.sha256(path_bytes).hexdigest(),
        "present": None,
        "readable": False,
        "size_bytes": None,
        "content_sha256": None,
        "configured_size_limit_bytes": None,
        "size_limit_status": "NOT_EVALUATED",
        "utf8_status": "NOT_EVALUATED",
        "json_status": "NOT_EVALUATED",
        "top_level_shape": "unavailable",
        "projection": _empty_event_projection(),
    }
    if recorder is not None and evaluation_id is not None:
        recorder.attach_snapshot(evaluation_id, snapshot)
    # diagnostic-predicate: pr.event.read
    # diagnostic-predicate: release_push.event.read
    try:
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
    snapshot["size_bytes"] = len(raw)
    snapshot["content_sha256"] = hashlib.sha256(raw).hexdigest()
    _diagnostic_predicate(recorder, evaluation_id, f"{predicate_prefix}.event.read", True, _bounded_identity(len(raw), category="unexpected-size", allow_positive_integer=True))
    # diagnostic-predicate: pr.event.utf8
    # diagnostic-predicate: release_push.event.utf8
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
    _diagnostic_predicate(recorder, evaluation_id, f"{predicate_prefix}.event.object", is_object, _bounded_identity(_type_name(payload), category="unexpected-event-shape", expected="object"))
    if not is_object:
        return None
    snapshot["projection"] = _event_projection(payload, version, canonical_repository)
    return payload


def github_context_present(env: dict[str, str]) -> bool:
    """Return whether any reviewed GitHub context field is populated."""
    return any(env.values())


def _repository_name(value: object) -> Optional[str]:
    return value.get("full_name") if isinstance(value, dict) else None


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
) -> bool:
    """Validate a same-repository PR head or synthetic merge checkout."""
    evaluation_id: Optional[int] = None
    if diagnostic is not None:
        try:
            evaluation_id = diagnostic.begin_evaluation(
                diagnostic_site, env, head, branch, version, canonical_repository
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
    if not check("pr.env.head_ref", env["GITHUB_HEAD_REF"] == f"release/v{version}", observed(env["GITHUB_HEAD_REF"], "unexpected-head-ref", expected=f"release/v{version}")):
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
    if not check("pr.payload.head_ref", head_ref == f"release/v{version}", observed(head_ref, "unexpected-head-ref", expected=f"release/v{version}")):
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
    merge_field_valid = merge_sha is None or (isinstance(merge_sha, str) and merge_sha == head)
    # Preserve the pre-instrumentation probe order: parents are read before the
    # merge-field Boolean participates in the final short-circuit expression.
    parents = commit_parents(root, head)
    if diagnostic is not None and evaluation_id is not None:
        diagnostic.context_update(evaluation_id, checkout_parents=parents or [])
    # diagnostic-predicate: pr.payload.merge_value
    if not check("pr.payload.merge_value", merge_field_valid, observed(merge_sha, "unexpected-merge-value", allow=GIT_SHA, allow_null=True)):
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
    return bool(
        pull_request.get("merge_commit_sha") == head
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
    accepted = bool(
        env["GITHUB_ACTIONS"] == "true"
        and env["GITHUB_EVENT_NAME"] == "push"
        and env["GITHUB_REF"] == expected_ref
        and env["GITHUB_REF_NAME"] == f"release/v{version}"
        and env["GITHUB_REF_TYPE"] == "branch"
        and env["GITHUB_REPOSITORY"] == canonical_repository
        and env["GITHUB_SHA"] == head
        and payload
        and payload.get("ref") == expected_ref
        and payload.get("after") == head
        and isinstance(before, str)
        and GIT_SHA.fullmatch(before)
        and before != "0" * 40
        and payload.get("created") is False
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


def strict_tag_push_context(
    root: Path,
    version: str,
    head: str,
    env: dict[str, str],
    canonical_repository: str,
) -> bool:
    """Validate ordinary tag-creation event context separately from main."""
    expected_ref = f"refs/tags/v{version}"
    payload = _load_event(env["GITHUB_EVENT_PATH"]) if env["GITHUB_EVENT_PATH"] else None
    tag_object_result = run_git(
        root, ["rev-parse", expected_ref], allow_failure=True
    )
    tag_object = tag_object_result.stdout.strip()
    return bool(
        env["GITHUB_ACTIONS"] == "true"
        and env["GITHUB_EVENT_NAME"] == "push"
        and env["GITHUB_REF"] == expected_ref
        and env["GITHUB_REF_NAME"] == f"v{version}"
        and env["GITHUB_REF_TYPE"] == "tag"
        and env["GITHUB_REPOSITORY"] == canonical_repository
        and env["GITHUB_SHA"] == head
        and payload
        and payload.get("ref") == expected_ref
        and payload.get("before") == "0" * 40
        and tag_object_result.returncode == 0
        and GIT_SHA.fullmatch(tag_object)
        and payload.get("after") == head
        and payload.get("created") is True
        and payload.get("deleted") is False
        and payload.get("forced") is False
        and _repository_name(payload.get("repository")) == canonical_repository
    )


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
) -> Optional[str]:
    """Return bounded structural evidence scope for one v1 state."""
    head, branch, _, _, _ = context
    release_branch = f"release/v{version}"
    closure_branch = f"release/v{version}-post-publication-closure"
    any_github = github_context_present(env)
    if state in ("development", "pre-release"):
        if not any_github and branch == release_branch:
            return "LOCAL_RELEASE_BRANCH"
        if strict_release_push_context(
            version,
            head,
            env,
            declaration.canonical_repository,
            branch=branch,
            diagnostic=diagnostic,
            diagnostic_site=f"{diagnostic_site}-release-push",
        ):
            return "GITHUB_RELEASE_PUSH_CONTEXT_CONSISTENCY"
        if strict_release_pr_context(
            root,
            version,
            head,
            env,
            declaration.canonical_repository,
            branch=branch,
            diagnostic=diagnostic,
            diagnostic_site=diagnostic_site,
        ):
            return "GITHUB_PR_EVENT_CONTEXT_CONSISTENCY"
        return None
    if state == "merged-pending-final-main-certification":
        if not any_github and branch == "main" and normal_merge_artifact(root, head):
            return "LOCAL_ARTIFACT_ONLY"
        if strict_main_push_context(
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
        if not any_github:
            if state == "tagged-pending-tag-certification" and not normal_merge_artifact(
                root, head
            ):
                return None
            return "LOCAL_TAG_ARTIFACT_ONLY"
        if strict_tag_push_context(
            root, version, head, env, declaration.canonical_repository
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
