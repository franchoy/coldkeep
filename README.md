<p align="center">
  <img src="assets/logo/coldkeep-logo.png" alt="Coldkeep Logo" width="500"/>
</p>

<p align="center">
  Correctness-first cold storage engine
</p>

<p align="center">
  • Content-addressed • Built-in deduplication • Deterministic restore • Verifiable integrity • Crash-safe • GC-safe
</p>

## 🧊 Branding

<p align="left">
  <img src="assets/logo/coldkeep-icon.png" alt="Coldkeep Icon" width="120"/>
</p>

Coldkeep uses a visual identity based on an ice cube vault:

- cold storage (ice cube)
- secure data (vault door)
- structured containers (internal shelves)

# coldkeep

![CI](https://github.com/franchoy/coldkeep/actions/workflows/ci.yml/badge.svg)
![Go Version](https://img.shields.io/badge/go-1.23+-blue)
![License](https://img.shields.io/badge/license-Apache%202.0-blue)
![Status](https://img.shields.io/badge/status-v1.4%20snapshot%20clarity%20%26%20hardening-brightgreen)
![Release](https://img.shields.io/github/v/release/franchoy/coldkeep?include_prereleases)

> Status: v1.4 hardens snapshot operator clarity and release-readiness on top of the v1.3 snapshot layer. Snapshots remain self-contained immutable captures; lineage (`--from`) is metadata for analysis only.

coldkeep is a local-first content-addressed storage engine focused on deterministic restore,
explicit integrity verification, and safe lifecycle behavior under failure scenarios.

Now with snapshot lineage, diff summaries, and safe deletion insights.

## Why coldkeep?

coldkeep is designed for correctness-first cold storage.

Unlike traditional backup tools, it emphasizes:

- deterministic, byte-identical restore
- content-addressed deduplication
- explicit, test-backed integrity checks
- safe recovery and reference-safe garbage collection
- machine-readable CLI behavior suitable for automation

The goal is confidence and recoverability over maximum throughput.

## Features

- Snapshot lineage (`--from`)
- Snapshot diff summaries
- Snapshot tree visualization
- Safe deletion preview (`--dry-run`)
- Built-in deduplication
- Deterministic restore

## Status

Coldkeep has five explicit correctness layers:

- v1.0: storage correctness (restore determinism, integrity, recovery, GC safety)
- v1.1: interaction correctness (CLI orchestration, machine-readable contracts, batch semantics)
- v1.2: physical-file graph coherence, explicit repair semantics, audited GC refusal, and invariant-aware batch maintenance reporting
- v1.3: snapshot-based retention as a correctness layer (immutable point-in-time captures, snapshot-protected GC, reachability audits)
- v1.4: snapshot clarity and lifecycle hardening (explicit lineage semantics, safer dry-run wording, stricter pre-release verification guidance)

Guarantees are enforced through automated validation and CI gates; see VALIDATION_MATRIX.md for guarantee-to-evidence mapping.

## Core Guarantees

### Summary

- deterministic, byte-identical restore
- no exposure of partially written or inconsistent data
- GC is reference-safe: no reachable chunk is ever deleted
- Atomic restore replacement (within single-node local filesystem semantics)
- Safe in-process concurrent storage operations

### Core invariants

Guarantee IDs are stable and tracked in VALIDATION_MATRIX.md:

- G1: deterministic, byte-identical restore
- G2: repeat store does not drift chunk graph
- G3: no exposure of partially written or inconsistent data
- G4: GC is reference-safe (no reachable chunk is deleted)
- G5: atomic restore replacement (single-node local filesystem semantics)
- G6: safe in-process concurrent storage operations
- G7: deep corruption detection (payload/offset/tail)
- G8: corrective health gate contract stability
- G9: deterministic batch CLI orchestration and automation-safe contract behavior
- G10: current-state physical mapping graph coherence is audited in standard verify
- G11: GC executes only on an audited coherent physical-root graph
- G12: invariant failures expose stable machine-readable classification and operator guidance
- G13: batch maintenance commands expose deterministic execution semantics and invariant-aware per-item reporting
- G14: snapshot-retained content is GC-safe and protected by liveness union (current + snapshot roots)
- G15: snapshot deletion only changes metadata and future GC eligibility (content preserved)
- G16: stats expose snapshot-retention pressure to operators (retained-only-by-current, retained-only-by-snapshot, shared)
- G17: verify and doctor audit persisted snapshot reachability integrity and report retention context

Definitions and evidence mapping for G1-G17 are tracked in VALIDATION_MATRIX.md.

Documentation is split into:

- README.md (overview and usage)
- ARCHITECTURE.md (internal model and invariants)

For the deeper model (invariants, lifecycle, validity, recovery, trust boundary), see ARCHITECTURE.md.

## When to use coldkeep

Good fit:

- cold/backup storage where correctness matters more than speed
- environments needing explicit integrity verification
- deduplication + deterministic restore use cases

Not a fit (v1.x scope):

- hot-path high-throughput storage
- distributed/multi-node coordination

## Quickstart

A small samples directory is included for local testing.

### Local (no Docker)

```bash
# 1) Initialize key material (.env)
coldkeep init

# 2) Load environment
export $(cat .env | xargs)

# 3) Store and inspect
coldkeep store samples/hello.txt
coldkeep stats

# 4) Restore
coldkeep restore 1 ./restored
```

Security note: if the encryption key is lost, encrypted data cannot be recovered.

### Docker

```bash
# 1) Start services
docker compose up -d --build

# 2) Initialize key material on host-mounted workspace
docker compose run --rm -v "$PWD:/app" coldkeep init

# 3) Store a sample file
docker compose run --rm \
  --env-file .env \
  -v "$PWD/samples:/samples" \
  coldkeep store /samples/hello.txt
```

## Smoke Validation (Two Approaches)

If you are preparing a PR, run the smoke gate (`scripts/smoke.sh`) with either
workflow below. Both are valid and both are used by contributors.

PR author tip: use the PR template at [`.github/pull_request_template.md`](.github/pull_request_template.md)
to summarize invariants and lifecycle-semantics impact for reviewers.

### Approach A: Docker runner

Use the `coldkeep` service container to run the smoke script.

```bash
# 1) Ensure PostgreSQL service is up
docker compose up -d coldkeep_postgres

# 2) Load encryption env from .env generated by coldkeep init
set -a
source .env
set +a

# 3) Run smoke inside the coldkeep container
docker compose run --rm \
  -e COLDKEEP_KEY="$COLDKEEP_KEY" \
  -e COLDKEEP_CODEC="$COLDKEEP_CODEC" \
  -v "$PWD/samples:/samples:ro" \
  --entrypoint sh coldkeep \
  -lc 'apk add --no-cache jq >/dev/null && COLDKEEP_SAMPLES_DIR=/samples scripts/smoke.sh'
```

### Approach B: Host runner

Run the smoke script on host with a local binary, pointing to Docker PostgreSQL.

```bash
# 1) Ensure PostgreSQL service is up
docker compose up -d coldkeep_postgres

# 2) Build coldkeep locally and load encryption env
go build -o coldkeep ./cmd/coldkeep
set -a
source .env
set +a

# 3) Run smoke from host against Docker PostgreSQL
DB_HOST=127.0.0.1 \
DB_PORT=5432 \
DB_USER=coldkeep \
DB_PASSWORD=coldkeep \
DB_NAME=coldkeep \
DB_SSLMODE=disable \
PATH="$PWD:$PATH" \
./scripts/smoke.sh

# 4) Optional cleanup of local binary
rm -f coldkeep
```

Notes:

- `scripts/smoke.sh` requires `jq` and `coldkeep` on PATH in the execution environment.
- Containerized simulate checks may print a non-fatal warning about sqlite/cgo stubs; smoke continues unless `COLDKEEP_SMOKE_STRICT_SIMULATE=1` is set.

## CLI Basics

Typical flows:

```bash
coldkeep store file.txt
coldkeep store-folder ./data
coldkeep restore 12 ./out
coldkeep remove 12
coldkeep gc
coldkeep stats
coldkeep list
coldkeep search report
coldkeep verify system --standard
coldkeep doctor
```

Simulation (no physical writes):

```bash
coldkeep simulate store-folder ./data
coldkeep simulate store file.txt --output json
```

## Batch Operations (v1.2)

Batch restore/remove/repair extends the automation contract with deterministic orchestration and invariant-aware reporting.

```bash
coldkeep restore 12 18 24 ./out
coldkeep remove 12 18 24
coldkeep remove --input ids.txt
coldkeep remove --stored-paths /data/a.txt /data/b.txt --input paths.txt
coldkeep repair ref-counts --batch
coldkeep repair --batch --input repair_targets.txt
coldkeep restore 12 18 ./out --dry-run
```

Current `repair --batch` scope is target-oriented, not item-oriented:

- today the only supported target is `ref-counts`
- input files for `repair --batch --input <file>` currently contain repeated target names such as `ref-counts`
- they do not contain file IDs or stored paths

Semantics (summary):

- per-item isolation by default
- optional fail-fast for execution failures
- duplicate target skipping
- deterministic per-item report ordering
- JSON status values are intentionally two-layered:
  - overall payload status: ok, partial_failure, error
  - per-item result status: success, failed, skipped, planned
- JSON execution mode is explicit: `continue_on_error` (default) or `fail_fast`
- process exit is automation-friendly:
  - 0 when no item fails
  - 1 when one or more items fail
  - 2 for pre-execution validation/usage failures (including empty effective target sets after parsing input)

Example JSON payload:

```json
{
  "status": "partial_failure",
  "operation": "repair",
  "dry_run": false,
  "execution_mode": "continue_on_error",
  "summary": {
    "total": 2,
    "succeeded": 1,
    "failed": 1,
    "skipped": 0
  },
  "results": [
    {
      "id": "ref-counts",
      "status": "success",
      "message": "logical_file ref_count values repaired"
    },
    {
      "id": "ref-counts",
      "status": "failed",
      "message": "repair refused: orphan physical_file rows detected",
      "invariant_code": "REPAIR_REFUSED_ORPHAN_ROWS",
      "recommended_action": "Remove or correct orphan physical_file rows before retrying repair."
    }
  ]
}
```

For full batch contract details and examples, see ARCHITECTURE.md and PRE_RELEASE_CHECKLIST.md.

## Snapshot Layer (v1.4)

coldkeep snapshots capture an immutable, point-in-time view of your stored files.

Snapshots capture a complete, immutable view of the current system state.
Even when using `--from`, snapshots are always fully self-contained and do not depend on their parent.

Critical clarity:

- Snapshots are always self-contained.
- `--from` records lineage metadata for analysis only.
- `--from` does not create dependencies.
- A child snapshot restore never requires reading parent snapshot content.

### Creating snapshots

v1.4 flow example:

```bash
# Create initial snapshot
coldkeep snapshot create --id day1

# Modify files...

# Create snapshot with lineage
coldkeep snapshot create --id day2 --from day1

# Understand changes
coldkeep snapshot diff day1 day2 --summary

# Inspect snapshot reuse
coldkeep snapshot stats day2

# Visualize history
coldkeep snapshot list --tree

# Preview deletion
coldkeep snapshot delete day1 --dry-run
```

```bash
# Full snapshot (all physical_file entries)
coldkeep snapshot create

# Full snapshot with lineage metadata
coldkeep snapshot create --id day2 --from day1

# Partial snapshot (exact paths and/or directory prefixes)
coldkeep snapshot create docs/ report.txt --label release-2026-04
```

- `--id <snapshotID>`: snapshot_id system identifier. This is the command target for `show`, `restore`, `stats`, `diff`, and `delete`.
- `--label <string>`: optional user-facing metadata only. It is not an identifier and is never used for command targeting.
- `--from <snapshotID>`: optional parent snapshot lineage metadata on create. This is informational only and does not create any parent-content dependency during create or restore.

`--from <snapshotID>` behavior:

- snapshot recorded as derived from parent
- does not create a dependency
- snapshot content is still built from current system state
- parent relationship is used for comparison and visualization only

Current lineage scope policy:

- `--from` is currently supported only for full snapshots.
- Parent snapshot referenced by `--from` must also be full.
- Filtered parent/child lineage for partial snapshots is intentionally rejected in this phase.

Snapshot command targeting contract:

- There is no `--snapshot` selector flag for snapshot subcommands.
- Pass snapshot_id positionally (for example: `coldkeep snapshot restore <snapshotID>`).

### Listing and inspecting

```bash
coldkeep snapshot list
coldkeep snapshot list --type full --limit 10 --since 2026-01-01
coldkeep snapshot list --tree
coldkeep snapshot show snap-abc123
coldkeep snapshot show snap-abc123 --limit 50
coldkeep snapshot show snap-abc123 --prefix docs/
coldkeep snapshot show snap-abc123 --pattern "docs/*.txt" --min-size 1024
coldkeep snapshot stats
coldkeep snapshot stats snap-abc123
```

`snapshot list --tree` renders a lineage view from snapshot metadata (`id`, `parent_id`, `created_at`).
If a parent snapshot was deleted, affected snapshots are still shown as roots; snapshot usability is unchanged.
Lineage visualization is not a dependency graph for restore execution.
The snapshot tree represents lineage metadata, not dependency.

Conceptual lineage example:

```text
day1
 └── day2
  └── day3
```

Each snapshot is independent despite this structure.

`snapshot list --tree`:

- displays snapshots as a lineage tree based on parent relationships
- reflects metadata lineage only (not restore dependency)

`snapshot stats` lineage context:

- when a parent snapshot is available, stats include reused files, new files, and reuse ratio
- if the parent snapshot is missing, stats fall back gracefully with explanatory output

Snapshot file queries are reusable across `snapshot show`, `snapshot restore`, and `snapshot diff`.

Supported query flags:

- `--path <exact>`: exact normalized snapshot path match; repeatable
- `--prefix <dir/>`: normalized directory prefix match; repeatable and must end with `/`
- `--pattern <glob>`: slash-path glob (`path.Match`) against the normalized snapshot path
- `--regex <re>`: regular expression against the snapshot path
- `--min-size <bytes>` / `--max-size <bytes>`: inclusive logical size range
- `--modified-after <RFC3339|YYYY-MM-DD>` / `--modified-before <RFC3339|YYYY-MM-DD>`: inclusive mtime window

All active criteria are ANDed together. Path and prefix inputs are normalized before matching, and result ordering remains deterministic.

### Restoring from a snapshot

```bash
# Restore all files to their original paths
coldkeep snapshot restore snap-abc123

# Restore a subdirectory under a new prefix
coldkeep snapshot restore snap-abc123 docs/ --mode prefix --destination ./restored

# Restore a single file to an explicit destination
coldkeep snapshot restore snap-abc123 docs/report.txt --mode override --destination ./out/report.txt

# Restore only matching files from the snapshot query layer
coldkeep snapshot restore snap-abc123 --prefix docs/ --pattern "docs/*.txt" --mode prefix --destination ./restored
```

### Diffing two snapshots

`snapshot diff` compares two snapshots by path and logical file identity, classifying each change as `added`, `removed`, or `modified`.
When query filters include size or mtime constraints, diff evaluates `added` and `modified` entries against target-snapshot metadata, and `removed` entries against base-snapshot metadata.
A file is considered modified if its content changes, even when the path stays the same.

```bash
# Show all changes between two snapshots
coldkeep snapshot diff snap-1 snap-2

# Show only added files
coldkeep snapshot diff snap-1 snap-2 --filter added

# Restrict the diff view to a path subset
coldkeep snapshot diff snap-1 snap-2 --prefix docs/

# Return summary counts only (no per-entry list)
coldkeep snapshot diff snap-1 snap-2 --summary

# Combine diff classification with snapshot query filters
coldkeep snapshot diff snap-1 snap-2 --filter modified --regex "\\.yaml$"

# Machine-readable JSON output
coldkeep snapshot diff snap-1 snap-2 --output json
```

Text output example:

```
[SNAPSHOT DIFF]

Base:    snap-1
Target:  snap-2

+ docs/new.txt
- docs/old.txt
~ docs/config.yaml

Summary:
  added: 1
  removed: 1
  modified: 1
```

JSON output schema:

```json
{
  "status": "ok",
  "command": "snapshot diff",
  "data": {
    "base": "snap-1",
    "target": "snap-2",
    "summary": { "added": 1, "removed": 1, "modified": 1 },
    "entries": [
      { "path": "docs/new.txt",    "type": "added",    "base_logical_id": null, "target_logical_id": 2 },
      { "path": "docs/old.txt",    "type": "removed",  "base_logical_id": 1,    "target_logical_id": null },
      { "path": "docs/config.yaml","type": "modified", "base_logical_id": 3,    "target_logical_id": 4 }
    ],
    "duration_ms": 12
  }
}
```

`--filter` limits output to one change type (`added`, `removed`, or `modified`). Summary counts reflect the filtered set.
`--summary` returns counts only and skips detailed `entries` output.

`snapshot diff --summary`:

- displays a summary of changes
- includes added, removed, and modified counts

The JSON contract for snapshot commands is unchanged. Query flags only reduce the returned `files` or `entries` collections and the derived counts; field names and envelope structure remain stable.

### Deleting a snapshot

```bash
coldkeep snapshot delete snap-abc123 --force
coldkeep snapshot delete snap-abc123 --dry-run
```

Deletes only the snapshot row and its `snapshot_file` entries. The underlying logical files and blocks are not affected.
Deleting a snapshot removes metadata only. Data remains retained when still referenced by other snapshots or current state.

`--dry-run` is read-only and reports impact details (lineage preview and file-count breakdown) without applying changes.
Dry-run impact describes metadata/reference effects and does not guarantee disk-space reclamation.

`snapshot delete --dry-run` preview includes:

- number of files referenced by the snapshot
- files unique to this snapshot
- files shared with other snapshots
- lineage impact

No data is modified in dry-run mode.

### Safe lineage workflow (v1.4)

Use this sequence when operating on parent/child snapshots:

```bash
# 1) Create baseline and child lineage metadata
coldkeep snapshot create --id day1
coldkeep snapshot create --id day2 --from day1

# 2) Review lineage and impact before delete
coldkeep snapshot list --tree
coldkeep snapshot delete day1 --dry-run

# 3) If approved, delete parent metadata
coldkeep snapshot delete day1 --force

# 4) Verify child remains independently restorable
coldkeep snapshot restore day2
```

Expected behavior:

- Deleting `day1` changes lineage metadata and future GC eligibility only.
- `day2` remains restorable because snapshots are self-contained.
- `snapshot list --tree` may re-root children after parent delete; restore behavior is unchanged.

### Snapshot release gate (operator quick checklist)

Before tagging a release, run the dedicated snapshot/retention contract gate in `PRE_RELEASE_CHECKLIST.md`.

For the focused automated snapshot gate, run:

```bash
scripts/run_snapshot_release_gate.sh --count 1
```

Run the checklist step-by-step and in order. For the manual snapshot lifecycle gate, use a stable snapshot identifier (for example via `snapshot create --id pre-gc-gate`) and pass snapshot IDs positionally in `snapshot restore`, `snapshot diff`, and `snapshot delete`.

Manual lifecycle expected in the release gate:

- create snapshot
- remove current mapping
- confirm GC dry-run reports snapshot-retained logical files
- restore from snapshot
- diff two snapshots
- delete snapshot
- confirm GC eligibility changes only after delete

For the full release criteria, use the snapshot sign-off sections in `PRE_RELEASE_CHECKLIST.md`:

- `13) Snapshot sign-off checklist (Phases 1-7)`
- `C. Test surface checklist`
- `D. Documentation / release checklist`
- `15) Verify snapshot / retention contract (manual gate)`
- `16) Final global sign-off`

When opening the release PR, use [`.github/pull_request_template.md`](.github/pull_request_template.md)
to keep impact and validation context explicit.

### Post-v1.4 hardening backlog (non-blocking)

- Add fuzz coverage for snapshot query combinations (`--regex`, `--pattern`, `--prefix`) to further harden parser+matcher edge cases.
- This is a future hardening task and is not part of the current release gate.

## Doctor (recommended health gate)

coldkeep doctor is the operator health gate:

- runs recovery first (corrective)
- then schema/version sanity checks
- then verification (standard by default; full/deep optional)

Doctor is intentionally corrective, not read-only.

```bash
coldkeep doctor
coldkeep doctor --full
coldkeep doctor --deep --output json
```

## Verification

Verification levels:

- standard: metadata integrity
- full: structural/container integrity
- deep: full content read + hash validation

```bash
coldkeep verify system --standard
coldkeep verify system --full
coldkeep verify system --deep
```

Verification checks are observational. In CLI flows, startup recovery may run before verification.

## Documentation Map

- Architecture and internals: ARCHITECTURE.md
- Guarantee mapping and evidence: VALIDATION_MATRIX.md
- Contribution workflow: CONTRIBUTING.md
- Release readiness flow: PRE_RELEASE_CHECKLIST.md
- Security reporting and threat guidance: SECURITY.md

## Roadmap note (v1.4 and beyond)

v1.2 now includes the `physical_file` to `logical_file` mapping layer, explicit `repair ref-counts`, audited GC refusal on drifted roots, and deterministic batch maintenance semantics. Future work is expected to focus on performance, broader repair scopes, and higher-level orchestration rather than changing the core correctness model.

## Contributing

Contributions and discussions are welcome.
See CONTRIBUTING.md.

## License

Apache-2.0. See LICENSE.
