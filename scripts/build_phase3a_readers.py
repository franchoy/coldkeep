#!/usr/bin/env python3
"""Build untouched D19 and accepted writers/readers with exact blob provenance."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile

ROOT = Path(__file__).resolve().parents[1]
BASES = {"c1": "06bde38380dd3a42be5c180beb99bf1aa509b8ac", "pre3c": "3aa9cae7b15037e6f51c22311e2b2ee0c0178b86","pre3b": "cf70de06d2ab2da6f64fa7b13fa9610af07e32f2", "d19": "d3581602aee2599547564322fe083baae1d52063",
         "accepted": "1229563116f71067c74801516bcd64c92e7b6ed2"}
HARNESS = (ROOT / 'scripts/admissionprobe/main.go').read_text().replace('github.com/franchoy/coldkeep/v2', 'MODULE')


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    output, report = map(Path, sys.argv[1:])
    output.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, CGO_ENABLED="0", GOTOOLCHAIN="local", GOWORK="off")
    records = {}
    compiler = subprocess.check_output(["go", "env", "GOVERSION"], env=env, text=True).strip()
    if compiler != "go1.26.11":
        raise ValueError("exact Go 1.26.11 required for staged readers")
    for label, commit in BASES.items():
        archive = output / (label + ".tar")
        with archive.open("xb") as f:
            subprocess.run(["git", "archive", commit], cwd=ROOT, stdout=f, check=True)
        source = output / label
        source.mkdir()
        with tarfile.open(archive) as tar:
            tar.extractall(source, filter="data")
        original = {}
        entries = subprocess.check_output(["git", "ls-tree", "-rz", commit], cwd=ROOT)
        for entry in entries.split(b"\0"):
            if not entry:
                continue
            meta, name = entry.split(b"\t", 1)
            mode, kind, blob = meta.split()
            if kind != b"blob" or mode not in (b"100644", b"100755"):
                raise ValueError("unexpected original object")
            path = source / os.fsdecode(name)
            data = path.read_bytes()
            assert hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest() == blob.decode()
            original[os.fsdecode(name)] = digest(path)
        module = next(line.split()[1] for line in (source / "go.mod").read_text().splitlines() if line.startswith("module "))
        records[label] = dict(commit=commit, tree=subprocess.check_output(["git", "rev-parse", commit + "^{tree}"], cwd=ROOT, text=True).strip(),
                              original_source_hashes=original, compiler=compiler)
        if label != "c1":
            harness = source / "cmd/phase3a-admission-probe"
            harness.mkdir()
            (harness / "main.go").write_text(HARNESS.replace("MODULE", module))
            subprocess.run(["go", "build", "-o", str(output / (label + "-writer")), "./cmd/phase3a-admission-probe"], cwd=source, env=env, check=True)
            records[label].update(harness_sha256=digest(harness / "main.go"), writer_sha256=digest(output / (label + "-writer")))
        if label in ("accepted", "pre3b", "pre3c", "c1"):
            subprocess.run(["go", "build", "-o", str(output / (label + "-reader")), "./cmd/coldkeep-recovery"], cwd=source, env=env, check=True)
        assert all(digest(source / name) == expected for name, expected in original.items())
    # Current production probe uses the same harness in a private overlay copy of
    # its package only; go build receives that source file inside the module.
    # The checked-in current probe is test support, never a product distribution.
    subprocess.run(["go", "build", "-o", str(output / "current-writer"), "./scripts/admissionprobe"], cwd=ROOT, env=env, check=True)
    for label in ("accepted", "pre3b", "pre3c", "c1"):
        records[label]["reader_sha256"] = digest(output / (label + "-reader"))
    records["current_writer_sha256"] = digest(output / "current-writer")
    report.write_text(json.dumps(records, indent=2) + "\n")


if __name__ == "__main__":
    main()
