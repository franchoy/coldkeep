"""Structural C2 authority; runtime evidence and independent review remain separate."""
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess

C1 = "06bde38380dd3a42be5c180beb99bf1aa509b8ac"
PREDECESSORS = {'phase1a-result.md': 'd1ac381791f35903bed3c9521c62017a2998bb2761113f4165c23cdeaae189c5', 'phase1d-result.md': '53375480f3772ea74571b8414168f5793a179a76bdf5b1751513ab11c96bdbcf', 'phase2-result.md': 'b96fd8f311e6d482c53f1e40f081d980678fc6f1173965adcc5279820528a1fa', 'phase2-followup-result.md': '176dbcd8f193bc3945eca8f5d13297e5648e05b89077c01c53f71d587ff93e60', 'phase3a-result.md': 'bb50da22c6638bb38351d01979f26c2c27e8c091fdb2a6e1d864f214feb3fe23', 'phase3a-followup-result.md': '1dec3603ba74d662681600d97316d85f610054ed06c17dc17e8902f5771e1407', 'phase3b-result.md': 'e8e1a39d27fd66c39fb984212b15cc7a3bb446eb6a76e17e01f82855228961c3', 'phase3b-followup-result.md': '7926c9def7ec4777effbcc306b5eefa1d41522370cc62968ff5e91d0261816ff', 'ci-acceptance-bridge.md': 'ed46ed4e73bdb5be745a710e28abfa30726bb7eae12cc8ff73849b3498179ed1', 'phase3c1-result.md': '5bf85f43c663bd09bcff9420e35066b521856db0cdd14c429a6a55e510d7d484', 'phase3c1-security-maintenance.md': 'c31e12399b71f5e1be1a4e4b565eaf4baf8a3654579ccdfcc2a55c2c4a3d5484', 'phase3c1-acceptance-bridge.md': '7f39c2e3827c7ef26c8ced82aeffdf237c0ed42ff679c6f48dab675621b082ce'}
CHECKS = {
    "focused-race", "recovery", "passive", "plain-packages", "aes-gcm-packages",
    "current-oracle", "d19-oracle", "python", "go-ci-enforcement", "build-vet-lint",
    "dependency-boundaries", "ci-audit-mutations", "phase2-integrated", "reachable-vulnerability",
    "index-continuity", "query-work", "dto-consumer", "fixture-cleanup",
}
# Qualification inputs are executable source, tests, embedded inputs, fixtures,
# tool configuration and workflow/staging source. Docs may follow separately.
INPUT_ROOTS = ("cmd", "internal", "db", "tests", "scripts", "web", "assets", "deploy", "testdata",
               ".github/workflows", ".devcontainer", "benchmarks", "samples", "samples_edge_cases")
# The OpenAPI document is executable frontend/contract input despite its docs
# location. Keep ordinary status documents outside this execution boundary.
INPUT_FILES = ("go.mod", "go.sum", ".golangci.yml", "Makefile", "Dockerfile", ".dockerignore",
               ".gitattributes", "docker-compose.yml", ".env.example",
               "docs/release/v2.0/proof-openapi.json")
IGNORED_DIRS = {"__pycache__", ".git"}
GENERATED_WEB_SUFFIXES = {".html", ".js", ".css", ".json", ".svg", ".png", ".jpg", ".webp", ".woff", ".woff2"}


def tracked_inputs(root: Path):
    """Bind tracked additions even inside otherwise generated/ignored paths."""
    if not (root / ".git").exists():
        return None
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    command = ["git", "-c", "core.fsmonitor=false", "-C", str(root)]
    def output(args):
        return subprocess.run(command + args, env=env, check=True, timeout=10,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout
    top = Path(os.fsdecode(output(["rev-parse", "--show-toplevel"]).rstrip(b"\n")))
    if top.resolve() != root.resolve():
        raise ValueError("qualification Git root mismatch")
    names = output(["ls-files", "-z", "--", *INPUT_ROOTS, *INPUT_FILES]).split(b"\0")
    paths = set()
    for raw in filter(None, names):
        name = Path(os.fsdecode(raw))
        if name.is_absolute() or ".." in name.parts:
            raise ValueError("invalid tracked qualification path")
        paths.add(root / name)
    return paths


def input_digest(root: Path) -> str:
    tracked = tracked_inputs(root)
    paths = list(tracked or ())
    for name in INPUT_ROOTS:
        directory = root / name
        if not directory.is_dir() or directory.is_symlink():
            raise ValueError("missing regular qualification input directory: " + name)
        pending = [directory]
        while pending:
            for path in pending.pop().iterdir():
                relative = path.relative_to(root).as_posix()
                if path.name in IGNORED_DIRS or relative in {"web/node_modules", "web/dist"}:
                    continue
                # Prepared assets are ignored build outputs; their exact binary
                # and manifest provenance is qualified separately by Phase2.
                # Only recognized untracked web outputs can be omitted. A
                # source export without Git binds every extra asset normally.
                mode = path.lstat().st_mode
                if stat.S_ISLNK(mode):
                    raise ValueError("linked qualification input")
                if stat.S_ISDIR(mode):
                    pending.append(path)
                elif stat.S_ISREG(mode) and path.suffix != ".pyc":
                    if not (tracked is not None and path not in tracked
                            and relative.startswith("internal/webui/assets/")
                            and path.suffix in GENERATED_WEB_SUFFIXES):
                        paths.append(path)
                else:
                    raise ValueError("nonregular qualification input")
    paths += [root / name for name in INPUT_FILES if (root / name).exists()]
    digest = hashlib.sha256()
    for path in sorted(set(paths)):
        mode = path.lstat().st_mode
        if not stat.S_ISREG(mode):
            raise ValueError("nonregular qualification input")
        digest.update(path.relative_to(root).as_posix().encode() + b"\0")
        digest.update(b"100755\0" if mode & 0o111 else b"100644\0")
        digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


def bounded(path: Path, limit=65536) -> bytes:
    if path.is_symlink() or not path.is_file():
        raise ValueError("missing regular authority record")
    with path.open("rb") as stream:
        data = stream.read(limit + 1)
    if len(data) > limit:
        raise ValueError("oversized authority record")
    return data


def unique(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate qualification field")
        value[key] = item
    return value


def complete(root: Path) -> bool:
    """Match the owning local completion, never infer hosted/product success."""
    try:
        directory = root / "docs/release/v2.0"
        for name, expected in PREDECESSORS.items():
            if hashlib.sha256(bounded(directory / name)).hexdigest() != expected:
                return False
        raw = bounded(directory / "phase3c2-qualification.json")
        record = json.loads(raw, object_pairs_hook=unique)
        if not isinstance(record, dict) or set(record) != {
                "schema", "accepted_c1", "head", "tree", "input_sha256", "compiler", "checks"}:
            return False
        if (record["schema"] != "coldkeep-phase3c2-local-qualification/v1"
                or record["accepted_c1"] != C1 or record["compiler"] != "go1.26.11"
                or not re.fullmatch(r"[0-9a-f]{40}", str(record["head"]))
                or not re.fullmatch(r"[0-9a-f]{40}", str(record["tree"]))
                or record["head"] == C1 or record["input_sha256"] != input_digest(root)
                or record["checks"] != {key: "PASS" for key in CHECKS}):
            return False
        required = {
            "Phase 3C2": "COMPLETE", "Phase 3C": "COMPLETE",
            "Local-profile action 2.04": "COMPLETE", "Implementation": "ACTIVE",
            "Product gates": "INCOMPLETE", "Profile": "installed Linux/PostgreSQL16 schema20",
            "Governing BUILD": "coldkeep_phase3c2_build_2026-11-12.md", "Accepted C1": C1,
            "Qualification HEAD": record["head"], "Qualification tree": record["tree"],
            "Qualification SHA-256": hashlib.sha256(raw).hexdigest(),
            "Ordinary schema18/19/20 writers and reclamation": "REFUSED",
            "IMPORT_READY maintenance/activation": "REFUSED", "Action 2.05": "NOT IMPLEMENTED",
        }
        fields = {key: [] for key in required}
        for line in bounded(directory / "phase3c2-result.md").decode("utf-8").splitlines():
            key, separator, value = line.strip().partition(":")
            if separator and key in fields:
                fields[key].append(value.strip())
        return all(fields[key] == [value] for key, value in required.items())
    except (OSError, ValueError, TypeError, KeyError, UnicodeError, subprocess.SubprocessError):
        return False
