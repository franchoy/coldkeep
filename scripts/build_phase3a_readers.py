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
BASES = {"d19": "d3581602aee2599547564322fe083baae1d52063",
         "accepted": "1229563116f71067c74801516bcd64c92e7b6ed2"}
HARNESS = '''package main
import (
 "context"
 "fmt"
 "os"
 "MODULE/internal/storage"
 "MODULE/internal/engine"
)
func main(){if e:=run();e!=nil{fmt.Fprintln(os.Stderr,e);os.Exit(1)}}
func run()error{
 sc,e:=storage.OpenLocalStorage(os.Getenv("COLDKEEP_STORAGE_DIR"));if e!=nil{return e};defer func(){_ = sc.Close()}()
 eng,e:=engine.New(engine.Config{DB:sc.DB,ContainerDir:sc.ContainerDir,StoreContext:&sc});if e!=nil{return e}
 _,e=eng.SetConfiguration(context.Background(),engine.SetConfigurationRequest{Key:engine.ConfigurationKey("compression"),Value:"none"});return e
}
'''

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    output, report = map(Path, sys.argv[1:])
    output.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, CGO_ENABLED="0", GOTOOLCHAIN="local", GOWORK="off")
    records = {}
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
        harness = source / "cmd/phase3a-admission-probe"
        harness.mkdir()
        (harness / "main.go").write_text(HARNESS.replace("MODULE", module))
        subprocess.run(["go", "build", "-o", str(output / (label + "-writer")), "./cmd/phase3a-admission-probe"], cwd=source, env=env, check=True)
        if label == "accepted":
            subprocess.run(["go", "build", "-o", str(output / "accepted-reader"), "./cmd/coldkeep-recovery"], cwd=source, env=env, check=True)
        assert all(digest(source / name) == expected for name, expected in original.items())
        records[label] = dict(commit=commit, tree=subprocess.check_output(["git", "rev-parse", commit + "^{tree}"], cwd=ROOT, text=True).strip(),
                              original_source_hashes=original, harness_sha256=digest(harness / "main.go"), writer_sha256=digest(output / (label + "-writer")))
    # Current production probe uses the same harness in a private overlay copy of
    # its package only; go build receives that source file inside the module.
    # The checked-in current probe is test support, never a product distribution.
    subprocess.run(["go", "build", "-o", str(output / "current-writer"), "./scripts/admissionprobe"], cwd=ROOT, env=env, check=True)
    records["accepted"]["reader_sha256"] = digest(output / "accepted-reader")
    records["current_writer_sha256"] = digest(output / "current-writer")
    report.write_text(json.dumps(records, indent=2) + "\n")


if __name__ == "__main__":
    main()
