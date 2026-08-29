"""Private support primitives for the Coldkeep release-state validator."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Iterable, Optional


SEMVER = re.compile(r"^\d+\.\d+\.\d+$")
CHANGELOG_HEADING = re.compile(r"^## (?:v|\[)(\d+\.\d+\.\d+)(?:\]|\b)(.*)$")
PHASE_HEADING = re.compile(r"^## Phase (\d+)\b")
METADATA = re.compile(r"^\*\*(Release|Status|Branch|Phase status):\*\*\s*(.*)$")
LIFECYCLE_BOUNDARY = re.compile(
    r"^\*\*(Merge-complete phase|Tag/publication phase|Post-publication closure phase):\*\*\s*(.*)$",
)
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
        "merged-not-tagged",
        "tagged",
        "released",
        "post-release-pending-closure",
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
        if state in ("post-release-pending-closure", "post-release-closed")
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
    if state == "post-release-closed":
        return complete
    return False


def github_context() -> dict[str, str]:
    """Return only GitHub environment fields used by lifecycle inference."""
    keys = (
        "GITHUB_EVENT_NAME",
        "GITHUB_REF",
        "GITHUB_REF_NAME",
        "GITHUB_HEAD_REF",
        "GITHUB_EVENT_PATH",
        "GITHUB_REPOSITORY",
    )
    return {key: os.environ.get(key, "") for key in keys}


def accepted_release_pr(version: str, env: dict[str, str]) -> bool:
    """Return whether GitHub describes a PR merge ref for this release."""
    return (
        env["GITHUB_EVENT_NAME"] == "pull_request"
        and env["GITHUB_REF"].startswith("refs/pull/")
        and env["GITHUB_HEAD_REF"] == f"release/v{version}"
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


def git_context_matches(
    version: str,
    state: str,
    branch: str,
    env: dict[str, str],
) -> bool:
    """Return whether branch and GitHub refs are compatible with lifecycle."""
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
