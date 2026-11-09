#!/usr/bin/env python3
"""Stage verified recovery readers for the complete lifetime of a Go test child."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import stat
import subprocess
import sys
import tempfile

from check_recovery_test_events import check_events


ROOT = Path(__file__).resolve().parents[1]
IMAGE = "postgres:16.15-bookworm@sha256:bb3e1a57e5407e0a5280b4211980a5e537f4abd234a87014ac979849a78dd825"
BASES = {"pre3b": "cf70de06d2ab2da6f64fa7b13fa9610af07e32f2",
         "d19": "d3581602aee2599547564322fe083baae1d52063",
         "accepted": "1229563116f71067c74801516bcd64c92e7b6ed2"}
COMMANDS = {
    "recovery": ["go", "test", "-race", "-count=1", "-p=1", "-json", "-timeout=10m",
                 "./internal/recoveryexport", "./internal/coordination", "./cmd/coldkeep-recovery"],
    "packages": ["go", "test", "-race", "-count=1", "-p=1", "-json", "-timeout=15m",
                 "./cmd/...", "./internal/..."],
}


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def terminate_child(child):
    """Join the Go process group before permitting reader cleanup."""
    try:
        os.killpg(child.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    if child.poll() is None:
        try:
            child.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass
    # A parent can exit after TERM while an inherited process ignores it.
    try:
        os.killpg(child.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    child.wait(timeout=10)


def command(args, env, *, timeout=60, stdout=None, stderr=None, check=True):
    child = subprocess.Popen(args, cwd=ROOT, env=env, start_new_session=True,
                             stdout=stdout or subprocess.PIPE, stderr=stderr or subprocess.PIPE)
    try:
        out, err = child.communicate(timeout=timeout)
    except BaseException:
        try:
            terminate_child(child)
        finally:
            for stream in (child.stdout, child.stderr):
                if stream is not None:
                    stream.close()
        raise
    if check and child.returncode:
        # Never echo a subprocess environment or arbitrary output to an exception.
        raise RuntimeError(f"{Path(args[0]).name} failed with exit {child.returncode}")
    return child.returncode, (out or b"").decode(), (err or b"").decode()


def prerequisites(env, scope):
    if os.getuid() == 0:
        raise ValueError("non-root fixture operator required")
    for key in ("DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD"):
        if not env.get(key):
            raise ValueError(f"explicit disposable {key} required")
    for key in ("COLDKEEP_PHASE1B_TEST", "COLDKEEP_PHASE1C_TEST"):
        if env.get(key) != "1":
            raise ValueError(f"{key}=1 required")
    if scope == "packages":
        if env.get("COLDKEEP_CODEC") not in ("plain", "aes-gcm"):
            raise ValueError("full packages require plain or aes-gcm codec")
        if env["COLDKEEP_CODEC"] == "aes-gcm" and not env.get("COLDKEEP_KEY"):
            raise ValueError("AES-GCM fixture key required")
    if command(["go", "env", "GOVERSION"], env)[1].strip() != "go1.26.10":
        raise ValueError("Go 1.26.10 required")
    versions = {}
    for label in ("dump", "restore"):
        path = Path(env.get("COLDKEEP_PG_" + label.upper(), ""))
        if not path.is_absolute() or not path.is_file() or not os.access(path, os.X_OK):
            raise ValueError(f"absolute executable pg_{label} required")
        version = command([str(path), "--version"], env)[1]
        match = re.match(r"^pg_" + label + r" \(PostgreSQL\) (16\.[0-9]+)(?:[ \n]|$)", version)
        if not match:
            raise ValueError(f"PostgreSQL 16 pg_{label} required")
        versions[label] = match.group(1)
    if versions["dump"] != versions["restore"]:
        raise ValueError("matched PostgreSQL client versions required")
    if env.get("COLDKEEP_RECOVERY_IMAGE") != IMAGE:
        raise ValueError("exact pinned recovery image required")
    info = json.loads(command(["docker", "image", "inspect", IMAGE], env)[1])
    if len(info) != 1 or not re.fullmatch(r"sha256:[0-9a-f]{64}", info[0].get("Id", "")):
        raise ValueError("invalid recovery image identity")
    expected_digest = IMAGE.split("@", 1)[1]
    if not any(value.split("@")[-1] == expected_digest for value in info[0].get("RepoDigests", [])):
        raise ValueError("recovery image digest mismatch")
    return {"clients": versions, "image": IMAGE, "image_id": info[0]["Id"],
            "repo_digests": info[0]["RepoDigests"]}


def verify_readers(readers, report, env):
    """Check source authorities and all staged executable hashes before use."""
    provenance = json.loads(report.read_text())
    hashes = {}
    for label, commit in BASES.items():
        record = provenance.get(label, {})
        tree = command(["git", "rev-parse", commit + "^{tree}"], env)[1].strip()
        if record.get("commit") != commit or record.get("tree") != tree:
            raise ValueError(f"wrong {label} reader source provenance")
        source_hashes = record.get("original_source_hashes")
        if not source_hashes:
            raise ValueError(f"missing {label} original source hashes")
        entries = command(["git", "ls-tree", "-rz", commit], env)[1].split("\0")
        originals = {}
        for entry in filter(None, entries):
            metadata, name = entry.split("\t", 1)
            mode, kind, blob = metadata.split()
            if kind != "blob" or mode not in ("100644", "100755"):
                raise ValueError("unexpected historical source object")
            originals[name] = (mode, blob)
        if source_hashes.keys() != originals.keys():
            raise ValueError(f"incomplete {label} historical source provenance")
        source_root = readers / "phase3a" / label
        for name, expected in source_hashes.items():
            source = readers / "phase3a" / label / name
            if source.is_symlink() or not source.resolve().is_relative_to(source_root.resolve()):
                raise ValueError("historical source path escaped staging")
            data = source.read_bytes()
            blob = hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()
            mode, expected_blob = originals[name]
            executable = bool(source.stat().st_mode & 0o111)
            if digest(source) != expected or blob != expected_blob or executable != (mode == "100755"):
                raise ValueError(f"changed {label} historical source")
        for kind in ("writer", "reader") if label != "d19" else ("writer",):
            path = readers / "phase3a" / (label + "-" + kind)
            hashes[path.name] = verify_binary(path, record.get(kind + "_sha256"))
    path = readers / "phase3a/current-writer"
    hashes[path.name] = verify_binary(path, provenance.get("current_writer_sha256"))
    path = readers / "coldkeep-recovery"
    hashes[path.name] = verify_binary(path)
    return hashes


def verify_binary(path, expected=None):
    mode = path.lstat().st_mode
    if not stat.S_ISREG(mode) or not os.access(path, os.X_OK):
        raise ValueError("missing regular executable staged reader")
    actual = digest(path)
    if expected is not None and actual != expected:
        raise ValueError("staged reader binary provenance mismatch")
    return actual


def resource_inventory(env):
    """Observe only identities and nonsecret attributes, never connection URLs."""
    psql = Path(env["COLDKEEP_PG_DUMP"]).with_name("psql")
    if not psql.is_file() or not os.access(psql, os.X_OK):
        raise ValueError("matched client psql is required for fixture cleanup inventory")
    query = """SELECT json_build_object(
        'databases', (SELECT coalesce(json_agg(json_build_array(oid::text, datname,
            datdba::text, encoding, datcollate, datctype) ORDER BY oid), '[]') FROM pg_database),
        'roles', (SELECT coalesce(json_agg(json_build_array(oid::text, rolname,
            rolsuper, rolcreatedb, rolcreaterole, rolcanlogin, rolinherit,
            rolreplication, rolbypassrls) ORDER BY oid), '[]') FROM pg_roles),
        'sessions', (SELECT coalesce(json_agg(json_build_array(pid::text, datid::text,
            usename, application_name, state) ORDER BY pid), '[]') FROM pg_stat_activity
            WHERE pid <> pg_backend_pid() AND backend_type = 'client backend'))"""
    pg_env = {key: value for key, value in env.items() if not key.startswith("PG")}
    pg_env.update(PGPASSWORD=env["DB_PASSWORD"], PGSSLMODE=env.get("DB_SSLMODE", "disable"))
    args = [str(psql), "-X", "-w", "-qAt", "--set=ON_ERROR_STOP=1", "--host", env["DB_HOST"],
            "--port", env["DB_PORT"], "--username", env["DB_USER"], "--dbname", "postgres", "--command", query]
    database = json.loads(command(args, pg_env)[1])
    if set(database) != {"databases", "roles", "sessions"} or any(not isinstance(v, list) for v in database.values()):
        raise ValueError("malformed fixture database inventory")
    containers = command(["docker", "container", "ls", "-a", "--no-trunc", "--format", "{{.ID}}\t{{.Names}}"], env)[1]
    networks = command(["docker", "network", "ls", "--no-trunc", "--format", "{{.ID}}\t{{.Name}}"], env)[1]
    return dict(database, containers=sorted(containers.splitlines()), networks=sorted(networks.splitlines()))


def run(scope, output):
    output = output.absolute()
    output.mkdir(parents=False, exist_ok=False)
    env = dict(os.environ, GOTOOLCHAIN="local", GOWORK="off", CGO_ENABLED="1", GOFLAGS="", LC_ALL="C")
    for key in ("COLDKEEP_PHASE1B_REPORT_DIR", "COLDKEEP_PHASE1C_REPORT_DIR", "COLDKEEP_PHASE1D_REPORT_DIR"):
        env[key] = str(output)
    readers = None
    before = None
    status = 0
    facts = {"scope": scope, "command": COMMANDS[scope], "status": "FAIL", "cleanup": {}}
    try:
        facts["prerequisites"] = prerequisites(env, scope)
        before = resource_inventory(env)
        write_json(output / "resources-before.json", before)
        facts["source_commit"] = command(["git", "rev-parse", "HEAD"], env)[1].strip()
        facts["source_tree"] = command(["git", "rev-parse", "HEAD^{tree}"], env)[1].strip()
        facts["working_diff_sha256"] = hashlib.sha256(command(["git", "diff", "--binary", "HEAD"], env)[1].encode()).hexdigest()
        readers = Path(tempfile.mkdtemp(prefix="coldkeep-package-readers-"))
        facts["reader_directory"] = str(readers)
        write_json(output / "execution.json", facts)
        build_env = dict(env, CGO_ENABLED="0")
        with (output / "staging.stdout").open("wb") as out, (output / "staging.stderr").open("wb") as err:
            command(["go", "build", "-o", str(readers / "coldkeep-recovery"), "./cmd/coldkeep-recovery"],
                    build_env, timeout=600, stdout=out, stderr=err)
            command([sys.executable, "scripts/build_phase3a_readers.py", str(readers / "phase3a"),
                     str(output / "phase3a-readers.json")], build_env, timeout=900, stdout=out, stderr=err)
        facts["reader_sha256"] = verify_readers(readers, output / "phase3a-readers.json", env)
        write_json(output / "staged-readers.json", facts["reader_sha256"])
        env["COLDKEEP_PHASE3A_READERS"] = str(readers / "phase3a")
        env["COLDKEEP_RECOVERY_READER"] = str(readers / "coldkeep-recovery")
        with (output / "go.jsonl").open("wb") as out, (output / "go.stderr").open("wb") as err:
            status = command(COMMANDS[scope], env, timeout=2400, stdout=out, stderr=err, check=False)[0]
        facts["go_exit"] = status
        events = [json.loads(line) for line in (output / "go.jsonl").read_text().splitlines() if line.strip()]
        result = check_events(events, broad=scope == "packages")
        write_json(output / "acceptance-events.json", result)
        print(json.dumps(result))
        if result["status"] != "PASS" and not status:
            status = 1
        # Readers stay intact through test exit and required-event inspection.
        if facts["reader_sha256"] != verify_readers(readers, output / "phase3a-readers.json", env):
            raise ValueError("staged readers changed during child execution")
    except (Exception, KeyboardInterrupt) as error:
        facts["error"] = f"{type(error).__name__}: {error}"
        print(facts["error"], file=sys.stderr)
        status = status or (130 if isinstance(error, KeyboardInterrupt) else 1)
    finally:
        if before is not None:
            try:
                after = resource_inventory(env)
                write_json(output / "resources-after.json", after)
                facts["cleanup"]["fixture_preserved"] = after == before
                if after != before:
                    facts["cleanup"]["changed_resource_kinds"] = [key for key in before if before[key] != after[key]]
                    status = status or 1
            except Exception as error:
                facts["cleanup"]["inventory_error"] = f"{type(error).__name__}: {error}"
                status = status or 1
        if readers is not None:
            try:
                shutil.rmtree(readers)
                if readers.exists():
                    raise RuntimeError("reader directory remains after cleanup")
                facts["cleanup"]["readers_removed"] = True
            except Exception as error:
                facts["cleanup"]["error"] = f"{type(error).__name__}: {error}"
                status = status or 1
        facts["exit"] = status
        facts["status"] = "PASS" if status == 0 else "FAIL"
        write_json(output / "execution.json", facts)
    return status


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scope", choices=COMMANDS, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    return run(args.scope, args.output)


if __name__ == "__main__":
    raise SystemExit(main())
