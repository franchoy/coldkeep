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
![Status](https://img.shields.io/badge/status-v1.3%20snapshot%20layer-brightgreen)
![Release](https://img.shields.io/github/v/release/franchoy/coldkeep?include_prereleases)

> Status: v1.3 adds the snapshot layer (immutable point-in-time captures, full/partial create, restore, list/show/stats, delete, and `snapshot diff` for change classification) on top of the v1.2 physical-file and operator semantics core.

coldkeep is a local-first content-addressed storage engine focused on deterministic restore,
explicit integrity verification, and safe lifecycle behavior under failure scenarios.

## Why coldkeep?

coldkeep is designed for correctness-first cold storage.

Unlike traditional backup tools, it emphasizes:

- deterministic, byte-identical restore
- content-addressed deduplication
- explicit, test-backed integrity checks
- safe recovery and reference-safe garbage collection
- machine-readable CLI behavior suitable for automation

The goal is confidence and recoverability over maximum throughput.

## Status

Coldkeep has three explicit correctness layers:

- v1.0: storage correctness (restore determinism, integrity, recovery, GC safety)
- v1.1: interaction correctness (CLI orchestration, machine-readable contracts, batch semantics)
- v1.2: physical-file graph coherence, explicit repair semantics, audited GC refusal, and invariant-aware batch maintenance reporting

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

Definitions and evidence mapping for G1-G13 are tracked in VALIDATION_MATRIX.md.

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

## Snapshot Layer (v1.3)

coldkeep snapshots capture an immutable, point-in-time view of your stored files.

### Creating snapshots

```bash
# Full snapshot (all physical_file entries)
coldkeep snapshot create

# Partial snapshot (exact paths and/or directory prefixes)
coldkeep snapshot create docs/ report.txt --label release-2026-04
```

### Listing and inspecting

```bash
coldkeep snapshot list
coldkeep snapshot list --type full --limit 10 --since 2026-01-01
coldkeep snapshot show snap-abc123
coldkeep snapshot show snap-abc123 --limit 50
coldkeep snapshot show snap-abc123 --prefix docs/
coldkeep snapshot show snap-abc123 --pattern "docs/*.txt" --min-size 1024
coldkeep snapshot stats
coldkeep snapshot stats snap-abc123
```

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

```bash
# Show all changes between two snapshots
coldkeep snapshot diff snap-1 snap-2

# Show only added files
coldkeep snapshot diff snap-1 snap-2 --filter added

# Restrict the diff view to a path subset
coldkeep snapshot diff snap-1 snap-2 --prefix docs/

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

The JSON contract for snapshot commands is unchanged. Query flags only reduce the returned `files` or `entries` collections and the derived counts; field names and envelope structure remain stable.

### Deleting a snapshot

```bash
coldkeep snapshot delete snap-abc123 --force
```

Deletes only the snapshot row and its `snapshot_file` entries. The underlying logical files and blocks are not affected.

### v1.3 release gate (operator quick checklist)

Before tagging a `v1.3.x` release, run the dedicated snapshot/retention contract gate in `PRE_RELEASE_CHECKLIST.md`.

Manual lifecycle expected in the release gate:

- create snapshot
- remove current mapping
- confirm GC dry-run reports snapshot-retained logical files
- restore from snapshot
- diff two snapshots
- delete snapshot
- confirm GC eligibility changes only after delete

For the full release criteria, use the v1.3 sections in `PRE_RELEASE_CHECKLIST.md`:

- `13) v1.3 snapshot sign-off checklist (Phases 1-7)`
- `C. Test surface checklist`
- `D. Documentation / release checklist`
- `15) Verify v1.3 snapshot / retention contract (manual gate)`
- `16) Final global sign-off`

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

## Roadmap note (v1.3 and beyond)

v1.2 now includes the `physical_file` to `logical_file` mapping layer, explicit `repair ref-counts`, audited GC refusal on drifted roots, and deterministic batch maintenance semantics. Future work is expected to focus on performance, broader repair scopes, and higher-level orchestration rather than changing the core correctness model.

## Contributing

Contributions and discussions are welcome.
See CONTRIBUTING.md.

## License

Apache-2.0. See LICENSE.
