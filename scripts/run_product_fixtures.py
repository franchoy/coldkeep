#!/usr/bin/env python3
"""Run the fixed 2.01 oracle on current code or exact D19, in disposable copies."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import time

ROOT = Path(__file__).resolve().parents[1]
BASELINE = "d3581602aee2599547564322fe083baae1d52063"
MANIFEST = "testdata/product-fixtures/manifest.json"
HARNESS = "tests/productfixtures/fixtures_test.go"


def git(*args):
    return subprocess.check_output(["git", "-C", str(ROOT), *args]).decode().strip()


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify_baseline_tree(source):
    """Verify every original tracked blob after extraction and harness overlay."""
    entries = subprocess.check_output(["git", "-C", str(ROOT), "ls-tree", "-rz", BASELINE])
    count = 0
    for entry in entries.split(b"\0"):
        if not entry:
            continue
        meta, name = entry.split(b"\t", 1)
        mode, kind, expected = meta.split()
        if kind != b"blob":
            raise ValueError("baseline must contain regular Git blobs, not submodules")
        path = source / os.fsdecode(name)
        data = os.fsencode(os.readlink(path)) if mode == b"120000" else path.read_bytes()
        blob = hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()
        if blob != expected.decode():
            raise ValueError(f"baseline byte mismatch: {os.fsdecode(name)}")
        count += 1
    return count


def validate_report(report, manifest, events, package):
    """Require each named case to execute and match its separately fixed oracle."""
    errors = []
    expected = {f["id"]: f for f in manifest["fixtures"]}
    observed = {f["fixture_id"]: f for f in report.get("fixtures", [])}
    if len(observed) != len(report.get("fixtures", [])) or set(observed) != set(expected):
        errors.append("missing, extra or duplicate fixture result")
    actions = {}
    for event in events:
        if event.get("Package") == package:
            actions.setdefault(event.get("Test", ""), []).append(event.get("Action"))
    if "pass" not in actions.get("", []):
        errors.append("package did not pass")
    for fixture_id, fixture in expected.items():
        result = observed.get(fixture_id, {})
        oracle = fixture["expected"]
        if result.get("observed_outcome") != oracle["outcome"]:
            errors.append(f"{fixture_id}: unexpected outcome")
        if result.get("observed_outputs") != oracle["outputs"]:
            errors.append(f"{fixture_id}: content/output mismatch")
        if not result.get("catalog_semantic_unchanged") or not result.get("payload_bytes_unchanged"):
            errors.append(f"{fixture_id}: source mutation or absent preservation result")
        if oracle["outcome"] == "REFUSED" and oracle["error_contains"].lower() not in result.get("observed_refusal", "").lower():
            errors.append(f"{fixture_id}: wrong refusal")
        if "normalized_name" in oracle and result.get("observed_normalized_name") != oracle["normalized_name"]:
            errors.append(f"{fixture_id}: normalization mismatch")
        test = actions.get("TestProductFixtures/" + fixture_id, [])
        if test.count("run") != 1 or test.count("pass") != 1 or "skip" in test or "fail" in test:
            errors.append(f"{fixture_id}: missing, skipped or failed execution")
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reader", choices=("baseline", "current"), required=True)
    parser.add_argument("--output", type=Path, required=True, help="new evidence directory; never reused")
    parser.add_argument("--go", default="go", help="pinned Go executable")
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    env = {k: v for k, v in os.environ.items() if not k.startswith(("COLDKEEP_", "DB_"))}
    env.update(GOTOOLCHAIN="local", GOWORK="off", CGO_ENABLED="1")
    go_version = subprocess.check_output([args.go, "version"], env=env, text=True).strip()
    if "go1.26.11 " not in go_version:
        raise ValueError(f"requires pinned go1.26.11, observed {go_version}")
    source = ROOT
    revision = git("rev-parse", "HEAD")
    baseline_blobs = None
    if args.reader == "baseline":
        revision = BASELINE
        source = output / "baseline-source"
        source.mkdir()
        archive = output / "baseline-source.tar"
        subprocess.run(["git", "-C", str(ROOT), "archive", "--format=tar", "--output=" + str(archive), BASELINE], check=True)
        with tarfile.open(archive) as tar:
            tar.extractall(source, filter="data")
        for relative in (MANIFEST, HARNESS):
            target = source / relative
            if target.exists():
                raise ValueError(f"refusing to replace baseline file {relative}")
            target.parent.mkdir(parents=True, exist_ok=True)
            data = (ROOT / relative).read_bytes()
            if relative == HARNESS:
                data = data.replace(b"github.com/franchoy/coldkeep/v2/", b"github.com/franchoy/coldkeep/")
            target.write_bytes(data)
        baseline_blobs = verify_baseline_tree(source)
    env.update(COLDKEEP_PRODUCT_FIXTURE_REPORT=str(output / "fixture-results.json"),
               COLDKEEP_PRODUCT_FIXTURE_WORK=str(output / "working-fixtures"),
               COLDKEEP_PRODUCT_READER_REVISION=revision)
    command = [args.go, "test", "-race", "-count=1", "-json", "-timeout=5m", "./tests/productfixtures"]
    start = time.monotonic()
    with (output / "go-test.jsonl").open("wb") as stdout, (output / "go-test.stderr").open("wb") as stderr:
        completed = subprocess.run(command, cwd=source, env=env, stdout=stdout, stderr=stderr)
    manifest = json.loads((ROOT / MANIFEST).read_text())
    report_path = output / "fixture-results.json"
    report = json.loads(report_path.read_text()) if report_path.exists() else {}
    package = "github.com/franchoy/coldkeep" + ("/v2" if args.reader == "current" else "") + "/tests/productfixtures"
    events = [json.loads(line) for line in (output / "go-test.jsonl").read_text().splitlines()]
    errors = validate_report(report, manifest, events, package)
    expected_version = "1.13.19" if args.reader == "baseline" else "2.0.0-dev.0"
    if report.get("reader_version") != expected_version or report.get("manifest_sha256") != sha(ROOT / MANIFEST):
        errors.append("reader version or oracle identity mismatch")
    if args.reader == "baseline":
        verify_baseline_tree(source)
    record = {"reader": args.reader, "source_revision": revision, "source_tree": git("rev-parse", revision + "^{tree}"),
              "current_worktree_status": git("status", "--porcelain=v1"), "command": command, "cwd": str(source),
              "go_version": go_version, "elapsed_seconds": round(time.monotonic() - start, 3), "go_exit": completed.returncode,
              "fixture_count": len(report.get("fixtures", [])), "manifest_sha256": sha(ROOT / MANIFEST),
              "harness_sha256": sha(ROOT / HARNESS), "executed_harness_sha256": sha(source / HARNESS),
              "original_baseline_blobs_verified": baseline_blobs, "failures": errors,
              "status": "PASS" if completed.returncode == 0 and not errors else "FAIL"}
    (output / "execution.json").write_text(json.dumps(record, indent=2) + "\n")
    print(json.dumps(record, indent=2))
    return 0 if record["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
