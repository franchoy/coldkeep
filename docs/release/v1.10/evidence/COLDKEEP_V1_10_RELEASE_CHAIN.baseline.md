# Coldkeep v1.10 Release Chain Plan

**Status:** Planning document  
**Target series:** v1.10.x  
**Purpose:** Stabilization, correctness hardening, CI evolution, and engine-readiness preparation before the v1.11+ engine extraction work.

---

## 0. Executive Summary

Coldkeep v1.9.0 is considered functionally complete, but the accumulated review material shows that the next phase should not be engine extraction yet. The v1.10 series should be a deliberate stabilization train focused on turning Coldkeep from a feature-complete system into a correctness-audited, regression-protected, engine-ready system.

The v1.10 series should not be measured by how many findings are closed numerically. It should be measured by whether every known issue is either:

- fixed,
- explicitly accepted,
- deferred with rationale,
- suppressed as a documented false positive,
- or converted into a regression test / CI invariant.

The target is not theoretical perfection. The target is **zero unknown correctness risk**.

---

## 1. Inputs Used For This Plan

This plan assumes the following source material exists in the project planning backlog:

1. Codacy full issue export.
2. External ChatGPT audit issue list.
3. Coldkeep CI Evolution & Codacy Integration Proposal.
4. Existing v1.x roadmap and previous release notes.
5. Current v1.9.0 codebase.

The Codacy export is useful mainly for:

- security surfacing,
- dependency vulnerability tracking,
- duplication hotspots,
- complexity hotspots,
- unchecked errors,
- resource lifecycle warnings.

The external audit issue list is useful mainly for:

- correctness bugs,
- CLI contract inconsistencies,
- restore and GC edge cases,
- packed-storage gaps,
- verification blind spots,
- migration parity issues,
- CI/tooling defects.

The CI proposal is useful mainly for:

- critical-path coverage strategy,
- Codacy rollout strategy,
- filesystem fault-injection direction,
- mutation testing timing,
- cross-platform validation priorities,
- CodeRabbit deferral rationale.

---

## 2. v1.10 Strategy

### 2.1 Core Goal

The v1.10 series exists to make Coldkeep safe enough to refactor into an engine later.

The goal is not to rewrite Coldkeep. The goal is to:

1. stabilize current behavior,
2. centralize invariants,
3. remove correctness ambiguity,
4. harden CI gates,
5. reduce technical risk before engine extraction.

### 2.2 Non-Goals

The v1.10 series should not:

- introduce major user-facing features,
- rewrite the storage engine,
- change repository format unless necessary for correctness,
- perform large aesthetic refactors,
- chase zero Codacy warnings blindly,
- introduce CodeRabbit or aggressive AI review gates,
- begin full engine extraction.

### 2.3 Success Definition

The v1.10 series is successful when:

- all known S0/S1 issues are fixed or explicitly documented as impossible / accepted with rationale,
- no high-risk filesystem traversal or unsafe restore path remains unreviewed,
- packed-storage behavior is consistently covered by stats, verify, inspect, restore, GC, and repair paths,
- CLI contracts are strict and deterministic,
- JSON output is machine-consumable without mixed streams,
- CI has critical-path gates rather than superficial global gates,
- Codacy is integrated as observability first, then carefully gated,
- engine extraction can begin without preserving ambiguous behavior behind an API boundary.

---

## 3. Severity Model

### S0 — Catastrophic

Blocks release immediately.

Includes:

- possible silent data corruption,
- reachable data deletion,
- unrecoverable restore failure,
- false verification success on corrupted data,
- GC deleting retained data,
- snapshot retention violation,
- path traversal writing/deleting outside intended root.

### S1 — Critical

Must be fixed before engine extraction and normally before the next stabilization release.

Includes:

- crash-consistency failures,
- incorrect refcount repair,
- stale liveness causing data retention or deletion bugs,
- unsafe container filename trust,
- serious JSON automation breakage for correctness-critical commands,
- migration parity failures.

### S2 — Major

Should be fixed during v1.10, but may be batched.

Includes:

- inaccurate stats,
- misleading verify summaries,
- inconsistent CLI validation,
- benchmark comparison false success,
- test or CI workflow masking failures.

### S3 — Minor

Fix opportunistically or when touching related code.

Includes:

- misleading error text,
- documentation mismatch,
- small UX inconsistencies,
- low-risk duplicate flags,
- test-only static-analysis warnings.

### S4 — Optional

Track for later cleanup.

Includes:

- style-only issues,
- non-risky complexity warnings,
- future refactor ideas,
- naming preferences,
- low-value Codacy noise.

---

## 4. Required Issue Tracking Schema

Before or during v1.10.0, each issue should be represented in a machine-readable tracker.

Recommended fields:

```yaml
id: CK-110-0001
source: codacy | external-audit | manual | ci-proposal
source_id: optional-original-id
release_target: v1.10.x
status: open | fixed | accepted | deferred | suppressed | duplicate
severity: S0 | S1 | S2 | S3 | S4
domain: cli | json | storage | packed-storage | gc | restore | recovery | verify | snapshot | migration | benchmark | ci | docs | security | filesystem | concurrency
breaking_risk: none | low | medium | high
data_loss_risk: none | low | medium | high
security_risk: none | low | medium | high
determinism_risk: none | low | medium | high
requires_regression_test: true | false
requires_ci_gate: true | false
duplicate_of: optional-id
notes: free text
```

This tracker can be YAML, CSV, JSON, GitHub issues, or a project board. The important property is that every known issue becomes traceable.

---

## 5. Global Release Process For Every v1.10.x Release

Every v1.10.x release should follow the same process.

### Step 1 — Select Scope

For each release:

1. choose one primary domain,
2. select all S0/S1 issues in that domain,
3. add related S2 issues only if they share the same root invariant,
4. explicitly mark out-of-scope findings.

Avoid mixing unrelated work. Do not combine GC correctness fixes with style cleanup unless the cleanup is required for the fix.

### Step 2 — Create A Release Branch

Recommended branch naming:

```text
release/v1.10.N-<domain-name>
```

Examples:

```text
release/v1.10.1-cli-contracts
release/v1.10.4-gc-reachability
release/v1.10.9-fs-fault-injection
```

### Step 3 — Create A Release Checklist

Each release must have a checklist containing:

- included issue IDs,
- excluded issue IDs,
- expected behavior changes,
- compatibility impact,
- required tests,
- manual verification steps,
- rollback notes.

### Step 4 — Write Or Update Regression Tests First Where Practical

For every correctness bug:

1. write a failing regression test,
2. confirm it fails on v1.9.0 or current baseline,
3. apply the fix,
4. confirm the test passes,
5. add it to a stable CI suite if it is not too expensive.

For bugs that are hard to test directly, add an invariant test around the affected behavior.

### Step 5 — Implement Fixes In Small Commits

Each commit should ideally be one of:

- validation fix,
- storage invariant fix,
- regression test,
- CI gate,
- documentation update.

Avoid giant commits that mix behavior changes with formatting.

### Step 6 — Run Required Test Matrix

At minimum:

```bash
go test ./...
go test -race ./...
```

Plus release-specific test commands listed below.

### Step 7 — Run Static Analysis / Codacy Check

For every release:

- verify no new high-confidence production security findings,
- verify no new unchecked errors in critical paths,
- verify Codacy findings do not increase without explanation,
- update suppressions only with rationale.

### Step 8 — Update Documentation

Each release should update:

- changelog,
- known issue list,
- CLI help if behavior changed,
- README if command behavior changed,
- migration notes if repository semantics changed.

### Step 9 — Tag Release Candidate

Recommended pre-tag:

```text
v1.10.N-rc1
```

Run the full release gate.

### Step 10 — Tag Final Release

Only after:

- release checklist complete,
- CI green,
- no unexplained high-risk findings,
- changelog updated,
- regression tests present.

---

# 6. v1.10.0 — Stabilization Baseline

## 6.1 Objective

Create the operational baseline for the v1.10 stabilization train.

This release does not need to fix every issue. Its purpose is to:

- organize the backlog,
- establish the issue taxonomy,
- upgrade immediate unsafe dependencies where feasible,
- make the project ready for controlled release-by-release remediation.

## 6.2 Scope

### Include

- issue tracker creation,
- Codacy export normalization,
- external audit import,
- duplicate grouping,
- severity assignment,
- Go/toolchain vulnerability upgrade assessment,
- release checklist templates,
- known-issues document,
- baseline CI report.

### Exclude

- major refactors,
- engine extraction,
- fault injection implementation,
- mutation testing,
- CodeRabbit.

## 6.3 Detailed Steps

### Step 1 — Create `docs/release/v1.10/`

Recommended files:

```text
docs/release/v1.10/
  README.md
  issue-triage-schema.md
  issue-tracker.csv
  release-chain.md
  accepted-risks.md
  codacy-triage.md
  ci-evolution.md
```

### Step 2 — Import Codacy Findings

Normalize Codacy issues into the tracker.

Fields to preserve:

- file path,
- line,
- category,
- subcategory,
- rule ID,
- severity,
- message,
- whether production or test code.

Immediately classify:

- test-only false positives,
- dependency CVEs,
- real production candidates,
- complexity hotspots.

### Step 3 — Import External Audit Findings

Create one tracker row per distinct issue family.

Do not blindly create one row per bullet if several bullets share the same root cause.

Examples of issue families:

- CLI duplicate singleton flags,
- boolean parser treats `--flag=false` as true,
- empty value flags fall through,
- packed stats ignore `storage_blocks`,
- restore traversal in stored-path prefix mode,
- GC simulation ignores snapshot-retained packed blocks.

### Step 4 — Assign Severity

Use the S0–S4 model.

Immediate S0/S1 candidates likely include:

- path traversal,
- unsafe container filename trust,
- GC reachability bugs,
- restore overwrite races,
- false verification success,
- stale refcount/liveness bugs,
- migration data loss/parity gaps.

### Step 5 — Group By Release Target

Assign tentative release targets:

- v1.10.1 CLI/JSON,
- v1.10.2 validation/security,
- v1.10.3 packed storage,
- v1.10.4 GC/refcounts,
- v1.10.5 restore/recovery,
- v1.10.6 CI/Codacy,
- v1.10.7 coverage gates,
- v1.10.8 filesystem abstraction,
- v1.10.9 fault injection,
- v1.10.10 cross-platform,
- v1.10.11 stabilization,
- v1.10.12 engine readiness.

### Step 6 — Upgrade Go Toolchain If Feasible

Codacy reported many Go stdlib CVEs via `go.mod` toolchain version.

Actions:

1. determine current supported Go version,
2. upgrade to latest supported stable Go version,
3. update CI matrix,
4. run full test suite,
5. document minimum supported Go version.

Recommended commands:

```bash
go mod edit -go=<target-version>
go mod tidy
go test ./...
```

If the upgrade is not immediately safe, create a blocking S1 issue.

### Step 7 — Create Baseline Reports

Generate:

- current test pass/fail summary,
- current Codacy issue count by category,
- current external audit count by domain,
- current known S0/S1 list,
- current CI workflow list.

### Step 8 — Update README / Changelog

Add a note that v1.10 is a stabilization train.

## 6.4 Required Tests

```bash
go test ./...
go test -race ./...
```

Run existing project scripts:

```bash
scripts/smoke.sh
```

If available:

```bash
scripts/audit_ci_enforcement.sh
```

## 6.5 Acceptance Criteria

- issue tracker exists,
- every known issue is imported or explicitly excluded,
- S0/S1 issues are identified,
- release chain is documented,
- toolchain vulnerability plan exists,
- CI baseline exists,
- no engine extraction started.

---

# 7. v1.10.1 — CLI Correctness & Contract Stabilization

## 7.1 Objective

Make the CLI strict, deterministic, and automation-safe.

Coldkeep should reject malformed commands early, consistently, and before performing repository initialization when possible.

## 7.2 Scope

### Include

- extra positional argument rejection,
- duplicate singleton flag rejection,
- boolean flag parsing normalization,
- empty value rejection,
- numeric validation consistency,
- JSON shorthand consistency,
- output-mode contract fixes,
- command help behavior consistency,
- early validation before DB load.

### Exclude

- deep storage behavior fixes,
- GC/refcount fixes,
- packed storage verification,
- CI architecture changes.

## 7.3 Main Issue Families

### CLI-001 — Extra Positional Arguments Ignored

Examples:

- `init garbage`,
- `version garbage`,
- `inspect repository extra`,
- `verify system extra`,
- `snapshot stats extra`,
- `simulate store file1 extra`,
- `repair ref-counts extra`.

### CLI-002 — Value Flags Accept Another Flag As Value

Examples:

```bash
coldkeep search --name --limit 10
coldkeep list --output --limit 10
```

Expected behavior: reject missing value.

### CLI-003 — Boolean Flags Treat `=false` As True

Examples:

```bash
snapshot delete --force=false
snapshot delete --dry-run=false
list --reverse=false
```

Expected behavior:

- either reject values for presence-only bool flags,
- or parse explicit booleans correctly.

Recommended v1.10 decision: reject explicit values for presence-only flags unless the CLI intentionally supports boolean values globally.

### CLI-004 — Empty Explicit Values Fall Through

Examples:

- `--stored-path ""`,
- `--input ""`,
- `--codec=`,
- `--mode=`,
- `--output=`,
- `--id ""`.

Expected behavior: explicit empty values must be rejected.

### CLI-005 — Duplicate Singleton Flags Last-Win

Examples:

```bash
store --codec plain --codec aes-gcm
snapshot create --id a --id b
benchmark run --repeat 1 --repeat 5
```

Expected behavior: reject duplicate singleton flags.

### CLI-006 — Inconsistent Limit/Worker/Threshold Validation

Examples:

- `--limit 0`,
- `--workers 0`,
- `--workers -1`,
- `--threshold NaN`,
- `--threshold Inf`,
- `--limit +10`.

Expected behavior:

- reject NaN/Inf,
- reject non-canonical numeric forms unless intentionally supported,
- enforce positive vs non-negative consistently.

### CLI-007 — JSON Shorthand Inconsistency

Examples:

- `list --json`,
- `search --json`,
- `snapshot stats --json`,
- `simulate store --json`,
- `config get --json`.

Expected behavior: either support `--json` everywhere `--output json` is supported, or remove it consistently. Recommended: support consistently for automation-oriented commands.

### CLI-008 — Mixed JSON/Human Output

Examples:

- benchmark duplicate JSON,
- init/help emitting human output plus generic JSON,
- snapshot restore dry-run human lines in JSON mode,
- warnings printed to stderr during JSON output.

Expected behavior:

- stdout must contain one JSON object for JSON mode,
- stderr must not contain routine human warnings that break strict automation unless documented as diagnostics,
- command-specific JSON emitters must not be followed by generic success JSON.

## 7.4 Implementation Steps

### Step 1 — Define CLI Contract Document

Create:

```text
docs/cli-contract.md
```

Define:

- positional arity rules,
- singleton flag rules,
- repeatable flag rules,
- boolean flag rules,
- empty string policy,
- numeric parsing policy,
- JSON output policy,
- help behavior policy.

### Step 2 — Centralize Flag Metadata

Introduce a central command specification table if practical.

Each command/subcommand should define:

- allowed flags,
- required positionals,
- optional positionals,
- repeatable flags,
- singleton flags,
- flags requiring values,
- mutually exclusive groups,
- flags allowed only with another flag.

### Step 3 — Fix Parser Value Handling

Parser must reject:

- missing value after value flag,
- another flag token used as value unless explicitly allowed,
- empty explicit values for required-value flags.

### Step 4 — Fix Boolean Handling

Choose one of two policies.

Preferred strict policy:

- `--flag` means true,
- `--flag=true` rejected,
- `--flag=false` rejected,
- `--no-flag` only supported if explicitly defined.

Alternative flexible policy:

- parse true/false/1/0/yes/no,
- reject invalid values.

Do not keep presence-means-true while accepting arbitrary values.

### Step 5 — Add Duplicate Singleton Detection

For each singleton flag:

- reject multiple occurrences,
- include the flag name in the error,
- fail before DB initialization when possible.

### Step 6 — Normalize Numeric Validation

Create shared helpers:

- parsePositiveIntFlag,
- parseNonNegativeIntFlag,
- parseFinitePositiveFloatFlag,
- parseStrictUintID,
- parseWorkerCount.

All should:

- trim whitespace only if policy allows,
- reject NaN/Inf,
- reject plus-prefixed values if canonical numeric forms are required,
- produce consistent errors.

### Step 7 — Fix JSON Output Contract

Audit all command paths for:

- direct `fmt.Println`,
- `log.Printf`,
- ignored `json.Marshal` errors,
- generic success double-emission,
- command-specific JSON renderers.

Centralize JSON rendering through one path where possible.

### Step 8 — Validate Before Opening DB

Commands with invalid arity or invalid flags should fail before:

- opening repository,
- running recovery,
- loading storage context,
- acquiring locks.

Commands especially affected:

- config,
- repair,
- inspect,
- snapshot,
- simulate,
- benchmark.

### Step 9 — Update Help Text

Ensure help text matches accepted behavior.

Remove undocumented aliases or document them explicitly.

### Step 10 — Add Regression Tests

Create a CLI validation test suite.

Recommended test file:

```text
tests/cli/cli_contract_test.go
```

Test classes:

- extra args rejected,
- duplicate singleton flags rejected,
- missing value rejected,
- empty value rejected,
- bool values rejected or parsed consistently,
- JSON mode emits exactly one JSON object,
- invalid command fails before DB open where observable.

## 7.5 Required Tests

```bash
go test ./cmd/...
go test ./tests/cli/...
go test ./...
```

Manual CLI smoke examples:

```bash
coldkeep init garbage
coldkeep version garbage
coldkeep search --name --limit 10
coldkeep list --reverse=false
coldkeep snapshot delete --force=false snap1
coldkeep stats --output=
coldkeep list --json
```

## 7.6 Acceptance Criteria

- no command silently ignores extra positional args unless explicitly documented,
- no singleton flag silently last-wins,
- no value flag accepts another flag as its value,
- no explicit empty value falls through to default behavior,
- JSON mode produces valid single-object output for supported commands,
- CLI validation happens before repository initialization where possible,
- regression tests cover all fixed parser families.

---

# 8. v1.10.2 — Validation & Security Hardening

## 8.1 Objective

Eliminate high-risk validation gaps and filesystem trust issues.

This release protects filesystem boundaries, user-provided paths, DB-provided filenames, and security-sensitive config parsing.

## 8.2 Scope

### Include

- path traversal fixes,
- container filename validation,
- restore destination validation,
- symlink handling policy,
- temp file cleanup safety,
- environment parsing hardening,
- DSN escaping,
- script argument validation where security-sensitive.

### Exclude

- full filesystem fault injection,
- full restore/recovery rewrite,
- packed storage verification.

## 8.3 Main Issue Families

### SEC-001 — Container Filenames From DB Are Trusted

Affected areas include:

- container open/read paths,
- rollback,
- GC orphan/deletion paths,
- recovery quarantine,
- verify block reader,
- storage block reader.

Expected invariant:

> DB container filename must be a safe basename matching the Coldkeep container naming contract before any filesystem join.

### SEC-002 — Restore Stored Paths Can Traverse

Affected paths include:

- stored-path prefix restore,
- snapshot restore,
- original restore mode,
- path filters using `..`,
- Windows drive-qualified paths.

Expected invariant:

> No stored path, snapshot path, or restore selector may escape the intended restore root.

### SEC-003 — Symlink Handling Ambiguity

Affected areas:

- restore overwrite,
- snapshot restore override,
- store-folder symlink following,
- benchmark restored-tree hashing.

Expected invariant:

> Symlink behavior must be explicit, documented, and tested.

Recommended default:

- do not follow symlinks outside selected roots,
- do not overwrite symlink targets by accident,
- reject dangerous symlink destinations in restore unless explicitly allowed.

### SEC-004 — Unsafe Temp File Cleanup

Affected areas:

- restore temp files,
- simulated storage temp DB,
- benchmark temp DB cleanup,
- smoke script temp dirs.

Expected invariant:

> Cleanup should only remove paths that Coldkeep securely created and still owns.

### SEC-005 — Env Parsing Accepts Malformed Values

Affected examples:

- partial integers like `123bad`,
- huge values causing overflow,
- invalid booleans,
- invalid timeout values,
- invalid container size values.

Expected invariant:

> Environment parsing must be strict, finite, bounded, and fail loudly for invalid operational settings.

### SEC-006 — Postgres DSN / Options Escaping

Affected areas:

- DB user/password/name/host options,
- timeout options containing quotes/spaces.

Expected invariant:

> Connection string construction must safely escape or use structured parameters.

## 8.4 Implementation Steps

### Step 1 — Create Central Path Safety Package

Recommended package:

```text
internal/pathsafe
```

Functions:

```go
ValidateContainerFilename(name string) error
JoinContainerPath(root, name string) (string, error)
NormalizeSnapshotPathStrict(path string) (string, error)
ValidateRestoreRelativePath(path string) error
SafeJoinUnderRoot(root, relative string) (string, error)
RejectWindowsDrivePath(path string) error
RejectTraversalComponents(path string) error
```

### Step 2 — Define Container Filename Contract

Document allowed format.

Example:

```text
container-<numeric-id>-<hex-or-uuid>.bin
```

or whatever current format uses.

Rules:

- basename only,
- no path separators,
- no `.` or `..`,
- no empty string,
- no absolute path,
- no Windows drive prefix,
- optional strict extension check.

### Step 3 — Replace All Direct Joins With Safe Helper

Search for:

```go
filepath.Join(containersDir, filename)
filepath.Join(root, dbFilename)
filepath.Join(w.dir, filename)
```

Replace with safe helper.

### Step 4 — Harden Snapshot Path Normalization

Reject:

- empty path where not meaningful,
- `.`,
- `..`,
- any component equal to `..`,
- absolute paths,
- Windows drive paths,
- UNC paths,
- paths that normalize outside root.

Normalize:

- separators,
- duplicate slashes,
- optional leading `./`.

### Step 5 — Harden Restore Planning

Restore planner must produce final destination paths before writing.

For each planned file:

1. normalize stored path,
2. validate it is relative-safe,
3. compute final path under restore root,
4. ensure final path remains under root,
5. detect collisions after all path reinterpretation,
6. reject symlink hazards according to policy.

### Step 6 — Harden Temp File Lifecycle

Temp files should:

- be created inside target directory or secure temp dir,
- use unpredictable names,
- not follow symlinks,
- be cleaned only by handle/path known to be created by Coldkeep,
- avoid deleting arbitrary replaced paths.

### Step 7 — Replace Partial Env Parsers

Replace `fmt.Sscanf` style parsing with strict parsers:

```go
strconv.ParseInt(strings.TrimSpace(value), 10, 64)
```

Then validate:

- no empty string,
- range bounds,
- positive/finite requirements,
- no overflow when converting to `time.Duration` or `int`.

### Step 8 — Harden Shell Scripts

For scripts in release/benchmark/smoke paths:

- validate `$2` exists before reading,
- reject unknown options,
- fail on missing required inputs,
- avoid broad `rm -rf /tmp/coldkeep*`,
- use cleanup traps.

### Step 9 — Regression Tests

Add tests for:

- container filename traversal,
- restore `../evil`,
- Windows path forms,
- empty paths,
- symlink overwrite attempts,
- unsafe env values,
- overflow env values,
- malformed DSN values.

## 8.5 Required Tests

```bash
go test ./internal/pathsafe/...
go test ./internal/container/...
go test ./internal/storage/...
go test ./internal/snapshot/...
go test ./internal/recovery/...
go test ./...
```

Manual examples:

```bash
coldkeep restore --stored-path ../evil out
coldkeep snapshot list --path ../x
COLDKEEP_CONTAINER_MAX_SIZE_MB=0 coldkeep init
COLDKEEP_CONTAINER_LOCK_RETRY_ATTEMPTS=10bad coldkeep store file
```

## 8.6 Acceptance Criteria

- no DB-provided container filename reaches filesystem joins without validation,
- no restore path can escape intended root,
- no Windows absolute/drive path bypass remains,
- env parsing rejects malformed partial values,
- temp cleanup is bounded to owned paths,
- regression tests cover traversal and container filename trust.

---

# 9. v1.10.3 — Packed Storage & Metadata Integrity

## 9.1 Objective

Make packed storage a first-class path across stats, inspect, verify, restore, repair, and GC-related metadata.

v1.8+ packed storage cannot remain a partial secondary path.

## 9.2 Scope

### Include

- packed stats parity,
- packed inspect parity,
- packed verification parity,
- packed reader safety,
- packed metadata structural checks,
- packed restore descriptor correctness,
- packed block graph consistency.

### Exclude

- full GC policy changes unless required for packed metadata correctness,
- engine abstraction.

## 9.3 Main Issue Families

### PACK-001 — Legacy-Only Stats

Stats paths ignore or undercount:

- `storage_blocks`,
- `chunk_block_refs`,
- packed container live/dead bytes,
- packed dedup ratios,
- packed fragmentation.

Expected invariant:

> Stats must represent both legacy and packed storage consistently.

### PACK-002 — Legacy-Only Inspect / Graph Traversal

Affected commands:

- inspect chunk,
- inspect container,
- reverse references,
- graph chunk→container edges.

Expected invariant:

> Inspection must not make packed-only repositories look empty or missing.

### PACK-003 — Verify Gaps For Packed Storage

Examples:

- verify system container existence ignores packed-only containers,
- file deep verify skips packed-only payload checks,
- packed refs to non-completed chunks not detected,
- fast mode summary overstates checks,
- compression metadata insufficiently validated.

Expected invariant:

> Verify must fail closed on packed metadata corruption.

### PACK-004 — Packed Restore Descriptor Incorrectness

Examples:

- packed restore recipe stores `ContainerID=0`,
- packed block reader uses runtime max size instead of persisted metadata,
- restore loses location resolution errors.

Expected invariant:

> Restore descriptors must accurately represent packed storage locations and fail with precise errors.

### PACK-005 — Packed Metadata Structural Invariants

Need validation for:

- duplicate chunk IDs inside packed block,
- zero/negative chunk IDs,
- offset ordering,
- segment bounds,
- logical size vs referenced chunk totals,
- compression/encryption metadata validity,
- hash length validity,
- impossible cross-container topology.

## 9.4 Implementation Steps

### Step 1 — Define Packed Storage Invariant Document

Create:

```text
docs/invariants/packed-storage.md
```

Include:

- table relationships,
- required constraints,
- liveness model,
- snapshot reachability model,
- verify expectations,
- stats accounting rules.

### Step 2 — Create Unified Storage Placement Query Layer

Introduce query helpers that return chunk storage placement independent of legacy vs packed.

Example model:

```go
type ChunkPlacement struct {
    ChunkID int64
    StorageKind string // legacy | packed
    ContainerID int64
    ContainerName string
    Offset int64
    Size int64
    BlockID sql.NullInt64
    StorageBlockID sql.NullInt64
}
```

Use this in:

- restore,
- inspect,
- stats,
- verify,
- graph traversal.

### Step 3 — Fix Stats Aggregation

Update stats to calculate:

- logical bytes,
- physical bytes,
- live bytes,
- dead bytes,
- packed block bytes,
- legacy block bytes,
- per-container breakdown,
- dedup ratio,
- fragmentation.

Ensure hybrid repositories do not double-count.

### Step 4 — Fix Inspect And Graph Paths

Update:

- inspect chunk,
- inspect container,
- inspect repository,
- reverse references,
- graph traversal.

Packed-only repositories must produce meaningful output.

### Step 5 — Strengthen Packed Verify

Add structural checks:

- every `chunk_block_refs.chunk_id` exists,
- chunk status is valid,
- every referenced `storage_block_id` exists,
- every storage block has valid container,
- offsets/sizes are finite and bounded,
- hash lengths correct,
- compression metadata valid,
- referenced chunk sizes match decoded logical slices,
- no duplicate impossible mappings.

### Step 6 — Fix Packed Restore Descriptor

Ensure packed restore uses:

- real container ID,
- real container name,
- real block/storage block ID,
- accurate compressed/plaintext size,
- accurate transform metadata.

### Step 7 — Update Repair Awareness

Repair tools should not legitimize invalid packed graphs.

Before recomputing refcounts:

- verify chunk statuses,
- verify physical mappings,
- verify packed references,
- reject impossible topology.

### Step 8 — Regression Tests

Create packed-only and hybrid test repositories.

Test cases:

- packed-only stats,
- packed-only inspect container,
- packed-only verify system,
- corrupt packed ref missing storage block,
- corrupt packed ref to aborted chunk,
- duplicate packed ref,
- invalid compression metadata,
- packed restore descriptor container ID.

## 9.5 Required Tests

```bash
go test ./internal/storage/...
go test ./internal/verify/...
go test ./internal/observability/...
go test ./internal/graph/...
go test ./internal/blocks/...
go test ./...
```

## 9.6 Acceptance Criteria

- packed-only repositories do not appear empty in stats/inspect,
- verify detects packed metadata corruption,
- packed restore descriptors are accurate,
- packed and legacy accounting are not double-counted,
- hybrid repositories produce consistent stats,
- regression tests cover packed-only, legacy-only, and hybrid modes.

---

# 10. v1.10.4 — GC Correctness & Reachability

## 10.1 Objective

Make GC conservative, snapshot-aware, packed-aware, and repair-safe.

GC must never delete reachable data.

## 10.2 Scope

### Include

- current roots correctness,
- snapshot roots correctness,
- packed reachability,
- stale refcount repair behavior,
- live/dead classification,
- simulation parity with real deletion,
- dry-run correctness.

### Exclude

- filesystem fault injection,
- performance optimization unless required to avoid correctness problems.

## 10.3 Main Issue Families

### GC-001 — Snapshot-Retained Packed Blocks Marked Reclaimable

Problem family:

- live packed block IDs are used instead of reachable packed block IDs,
- snapshot-only packed chunks can be incorrectly marked dead.

Expected invariant:

> Snapshot-reachable data is retained even when not live through current physical files.

### GC-002 — Non-Completed Logical Files Included As Roots

Problem family:

- current roots may include physical mappings to failed/aborted logical files,
- repair can legitimize bad mappings,
- chunk refcount repair counts aborted logical files.

Expected invariant:

> Only completed/current logical files should contribute to live current roots unless explicitly retained through snapshots.

### GC-003 — Stale Chunk Live Refcounts

Problem family:

- remove-by-stored-path decrements logical refcount but not chunk liveness,
- replace physical target fails to adjust old/new chunk liveness,
- late physical mapping failure leaves file_chunk rows.

Expected invariant:

> chunk live_ref_count must reflect current completed physical mappings only.

### GC-004 — Packed Deletion Helpers Unsafe If Reused

Problem family:

- helpers use live/pinned refs but not snapshot reachability,
- caller-level protection is fragile.

Expected invariant:

> deletion helper functions should be safe by construction or clearly private and guarded.

### GC-005 — Simulation / Real Plan Divergence

Problem family:

- simulation metrics differ from real deletion criteria,
- dry-run legacy-only intermediate checks misleading.

Expected invariant:

> dry-run/simulation must use same reachability model as actual GC.

## 10.4 Implementation Steps

### Step 1 — Define Reachability Model

Create:

```text
docs/invariants/reachability.md
```

Define:

- current roots,
- snapshot roots,
- pinned roots,
- completed logical file rule,
- packed block reachability,
- legacy block reachability,
- deletion eligibility.

### Step 2 — Centralize Reachability Calculation

Create one reachability service used by:

- GC plan,
- GC simulation,
- stats dead/live classification,
- verify reachability checks,
- repair previews.

Avoid separate live/reachable implementations.

### Step 3 — Filter Root Sets

Ensure current logical roots join to completed logical files.

Ensure snapshot roots join to existing snapshots.

Reject or report:

- zero/negative IDs,
- physical mappings to non-completed logical files,
- orphan snapshot_file rows.

### Step 4 — Fix Packed Reachability

Compute:

- reachable chunks,
- reachable legacy blocks,
- reachable packed storage blocks,
- reachable containers.

Snapshot-only references must retain packed storage blocks.

### Step 5 — Fix Liveness Updates

Audit paths:

- store commit failure cleanup,
- empty-file store failure cleanup,
- remove by ID,
- remove by stored path,
- replace physical file target,
- repair refcounts.

Ensure chunk liveness and logical refcounts stay aligned.

### Step 6 — Strengthen Repair

Repair must not simply make current DB state internally consistent if current DB state violates logical invariants.

Before repair:

- detect bad physical mappings,
- detect non-completed logical roots,
- detect orphan file_chunk rows,
- detect invalid packed refs.

### Step 7 — Add GC Safety Tests

Test cases:

- snapshot-only packed block retained,
- failed logical file not current root,
- remove last stored path updates chunk liveness,
- repair rejects non-completed current mappings,
- dry-run equals real plan without deleting,
- hybrid repository reachability.

## 10.5 Required Tests

```bash
go test ./internal/gc/...
go test ./internal/maintenance/...
go test ./internal/retention/...
go test ./internal/graph/...
go test ./internal/storage/...
go test ./...
```

Recommended long run:

```bash
go test -race ./tests/adversarial/...
```

## 10.6 Acceptance Criteria

- GC uses one authoritative reachability model,
- packed snapshot-retained blocks are safe,
- repair does not bless invalid roots,
- remove/replace paths update chunk liveness correctly,
- simulation and real GC use same eligibility logic,
- regression tests cover snapshot-only retention.

---

# 11. v1.10.5 — Restore & Recovery Safety

## 11.1 Objective

Make restore and recovery deterministic, crash-safe, path-safe, and operationally honest.

Restore must not silently overwrite, escape, partially succeed ambiguously, or hide data reconstruction errors.

## 11.2 Scope

### Include

- restore overwrite semantics,
- TOCTOU prevention,
- strict metadata behavior,
- restore path planning,
- snapshot restore parity,
- batch restore output behavior,
- recovery quarantine behavior,
- recovery warning counters,
- interrupted recovery behavior.

### Exclude

- full filesystem fault injection implementation,
- engine extraction.

## 11.3 Main Issue Families

### REST-001 — Restore Non-Overwrite TOCTOU Race

Problem:

- checks destination absence,
- later rename can overwrite a file created in the meantime.

Expected invariant:

> non-overwrite restore must not replace a file that appears after planning.

### REST-002 — Snapshot Restore And Storage Restore Diverge

Problem:

- different sanitization,
- different worker validation,
- directory reinterpretation differences,
- collision planning mismatch.

Expected invariant:

> snapshot restore and stored-path restore must share restore planning logic.

### REST-003 — Restore Errors Hidden Or Misleading

Problem examples:

- write errors become “no restorable chunks found,”
- chunk-location resolution errors lost,
- metadata strict failure returned after content exists,
- fsync/open/close failure after rename returns error after visible success.

Expected invariant:

> restore errors must identify the actual failed phase and whether content was written.

### REST-004 — Batch JSON Double Emission

Problem:

- batch report JSON emitted,
- then generic JSON error emitted.

Expected invariant:

> batch JSON mode emits one envelope with partial/failure details.

### REST-005 — Recovery Trusts Filenames / Weak Quarantine

Problem:

- arbitrary non-directory files quarantined,
- warning counter not incremented,
- strict recovery parsing inconsistent.

Expected invariant:

> recovery should validate artifacts and report accurate structured diagnostics.

## 11.4 Implementation Steps

### Step 1 — Define Restore State Machine

Document phases:

1. plan,
2. validate destinations,
3. acquire/prepare temp outputs,
4. read/decode chunks,
5. write temp file,
6. fsync temp file,
7. atomic install,
8. metadata application,
9. directory fsync,
10. cleanup.

For each phase define:

- possible errors,
- whether output exists,
- rollback behavior,
- JSON report fields.

### Step 2 — Centralize Restore Planner

One planner should handle:

- normal restore,
- stored-path restore,
- snapshot restore,
- batch restore.

The planner should output final exact destinations before writing.

### Step 3 — Make Non-Overwrite Atomic

Use safe create/install behavior.

Potential approaches:

- create target with `O_EXCL` where possible,
- install via platform-specific no-replace operation,
- hold parent directory lock if available,
- detect and fail if target appears before install.

Document platform limitations.

### Step 4 — Fix Directory Reinterpretation

If planner decides exact output path, lower restore layers must not reinterpret an existing directory as “restore inside this directory” unless the plan explicitly requests that behavior.

### Step 5 — Preserve First Real Error

Restore should track:

- read error,
- decode error,
- write error,
- fsync error,
- rename/install error,
- metadata error.

Do not replace a specific error with a generic final error.

### Step 6 — Improve Strict Metadata Reporting

If content was restored but metadata failed:

- report `content_restored=true`,
- report `metadata_failed=true`,
- return failure if strict mode requires,
- make JSON explicit.

### Step 7 — Fix Batch Output

Batch command result should include:

- status,
- item results,
- partial count,
- failed count,
- skipped count,
- fatal error if any.

Do not emit a second generic error envelope.

### Step 8 — Harden Recovery

Recovery should:

- validate container filenames,
- validate container headers before quarantine row insertion where possible,
- increment warning count correctly,
- parse strict recovery env consistently,
- produce structured recovery summaries.

### Step 9 — Regression Tests

Test cases:

- non-overwrite target created after plan,
- symlink target overwrite attempt,
- snapshot restore exact file destination into existing directory,
- batch JSON partial failure emits one JSON object,
- strict metadata failure after content restore,
- recovery bad filename quarantine.

## 11.5 Required Tests

```bash
go test ./internal/storage/...
go test ./internal/snapshot/...
go test ./internal/recovery/...
go test ./internal/batch/...
go test ./...
```

Race/adversarial:

```bash
go test -race ./tests/adversarial/...
```

## 11.6 Acceptance Criteria

- restore planning is shared or semantically equivalent across restore modes,
- non-overwrite mode cannot silently replace a raced file,
- errors preserve actual failed phase,
- strict metadata failures are explicit,
- batch JSON emits one envelope,
- recovery diagnostics are accurate.

---

# 12. v1.10.6 — CI Evolution Phase 1 & Codacy Passive Integration

## 12.1 Objective

Integrate Codacy and improve CI observability without introducing noisy or style-driven gates.

## 12.2 Scope

### Include

- Codacy passive mode,
- CI workflow cleanup,
- static analysis export,
- security scan reporting,
- baseline suppression documentation,
- duplicate/complexity trend tracking.

### Exclude

- hard Codacy quality gates,
- CodeRabbit,
- mutation testing,
- full filesystem fault injection.

## 12.3 Implementation Steps

### Step 1 — Add Codacy Documentation

Create:

```text
docs/ci/codacy.md
```

Document:

- Codacy role,
- passive mode policy,
- what is actionable,
- what is noise,
- suppression rules,
- escalation rules.

### Step 2 — Categorize Codacy Findings

Create summaries:

- by category,
- by file,
- by production/test,
- by severity,
- by rule.

Track only meaningful production deltas.

### Step 3 — Configure Passive PR Annotations

Enable:

- PR annotations,
- security visibility,
- duplication visibility,
- complexity trend.

Disable:

- style-based blocking,
- generic maintainability blocking,
- architecture suggestions as blockers.

### Step 4 — Add Local Export Script

Create:

```text
scripts/export_codacy_issues.sh
```

Script should:

- use API token from env,
- page through all issues,
- save raw JSON,
- produce summaries,
- avoid printing token.

### Step 5 — Add Suppression Policy

Every suppression must include:

- rule ID,
- file/path,
- reason,
- owner/date,
- expiration/review condition if applicable.

### Step 6 — Update CI Invariant Audit

CI audit should confirm:

- core test jobs present,
- security scan job present,
- smoke job present,
- artifact upload for important reports,
- Codacy integration documented.

### Step 7 — Codacy Baseline Commit

Commit current baseline reports to docs or release artifacts.

Do not fail CI based on this baseline yet.

## 12.4 Required Tests

```bash
scripts/audit_ci_enforcement.sh
scripts/export_codacy_issues.sh --dry-run
```

If GitHub workflows changed, validate YAML.

## 12.5 Acceptance Criteria

- Codacy is integrated passively,
- Codacy findings are summarized and classified,
- no style gate blocks merges,
- security findings are visible,
- suppression policy exists,
- CI audit recognizes Codacy integration.

---

# 13. v1.10.7 — Critical Path Coverage Gates

## 13.1 Objective

Move from “tests exist” to “critical correctness paths are measurably protected.”

Avoid global coverage vanity metrics.

## 13.2 Scope

### Include

- coverage reports for critical packages,
- initial soft thresholds,
- invariant coverage tracking,
- release-only hard gates for high-risk packages.

### Exclude

- global coverage gate,
- coverage requirements for docs/scripts/CLI sugar,
- mutation testing.

## 13.3 Critical Packages

Initial critical package list:

```text
internal/storage
internal/container
internal/snapshot
internal/gc
internal/maintenance
internal/retention
internal/verify
internal/graph
internal/blocks
internal/db
internal/recovery
```

Adjust as architecture evolves.

## 13.4 Implementation Steps

### Step 1 — Create Coverage Script

Create:

```text
scripts/critical_coverage.sh
```

It should:

- run coverage on critical packages,
- produce text summary,
- produce machine-readable JSON/CSV if practical,
- fail only when gate mode enabled.

### Step 2 — Establish Baseline

Run baseline coverage.

Record:

- package coverage,
- uncovered critical functions,
- invariant tests missing.

### Step 3 — Define Soft Gates

Initial soft targets:

- 75% package coverage minimum for critical packages,
- 85% target for storage/restore/GC/snapshot/verify,
- no decrease without explanation.

Do not hard-fail immediately.

### Step 4 — Add Invariant Coverage Checklist

For each critical domain, identify invariants:

- GC never deletes reachable chunks,
- restore never writes outside root,
- verify detects corrupt payload,
- packed refs must resolve,
- snapshot roots retain data,
- refcount repair excludes invalid roots.

### Step 5 — Convert Important Issues Into Tests

Every fixed S0/S1 issue should have a regression test or invariant test.

### Step 6 — Enable Hard Gates Gradually

Suggested rollout:

- PR: report only,
- release branch: warning on decrease,
- release candidate: fail on critical package regression,
- post-v1.10: fail on threshold breach.

## 13.5 Required Tests

```bash
scripts/critical_coverage.sh
```

Full suite:

```bash
go test -cover ./...
```

## 13.6 Acceptance Criteria

- critical package list exists,
- coverage baseline exists,
- coverage script exists,
- fixed S0/S1 issues have regression coverage,
- release candidate gate prevents coverage regression in critical packages.

---

# 14. v1.10.8 — Filesystem Abstraction Groundwork

## 14.1 Objective

Prepare Coldkeep for deterministic filesystem fault injection without destabilizing storage behavior.

This release should introduce abstraction seams, not heavy chaos behavior yet.

## 14.2 Scope

### Include

- filesystem interface design,
- minimal adapter around standard OS calls,
- restore/container/recovery seam identification,
- no-op deterministic filesystem wrapper,
- tests proving behavior unchanged.

### Exclude

- full ENOSPC simulation,
- major storage rewrite,
- engine extraction.

## 14.3 Implementation Steps

### Step 1 — Inventory Filesystem Operations

Search for:

- `os.Open`,
- `os.OpenFile`,
- `os.Create`,
- `os.Rename`,
- `os.Remove`,
- `os.RemoveAll`,
- `os.MkdirAll`,
- `os.Stat`,
- `os.Lstat`,
- `os.ReadFile`,
- `os.WriteFile`,
- `File.Sync`,
- `File.Close`,
- `filepath.Walk`.

Classify each as:

- critical path,
- test/helper,
- script/tooling,
- non-critical.

### Step 2 — Define Minimal Interface

Recommended package:

```text
internal/fs
```

Initial interfaces:

```go
type FS interface {
    Open(name string) (File, error)
    OpenFile(name string, flag int, perm os.FileMode) (File, error)
    Rename(oldpath, newpath string) error
    Remove(name string) error
    MkdirAll(path string, perm os.FileMode) error
    Stat(name string) (os.FileInfo, error)
    Lstat(name string) (os.FileInfo, error)
}

type File interface {
    io.Reader
    io.Writer
    io.ReaderAt
    io.WriterAt
    io.Closer
    Sync() error
    Stat() (os.FileInfo, error)
}
```

Keep it small.

### Step 3 — Add OS Adapter

Implement default adapter using `os` package.

No behavior changes.

### Step 4 — Introduce Seams In Restore And Container Paths

Start with:

- restore temp file creation,
- final rename/install,
- container open/write/seal,
- recovery quarantine.

Do not migrate every file operation at once.

### Step 5 — Add Golden Behavior Tests

Before and after abstraction, outputs should match.

Test:

- normal store,
- normal restore,
- overwrite restore,
- container seal,
- recovery scan.

### Step 6 — Avoid Over-Abstraction

Do not create a large virtual filesystem framework yet.

The goal is fault-injection readiness, not architecture purity.

## 14.4 Acceptance Criteria

- minimal FS abstraction exists,
- OS adapter preserves behavior,
- critical paths can be injected in tests,
- no production behavior change,
- no major performance regression,
- no broad rewrite.

---

# 15. v1.10.9 — Filesystem Fault Injection Phase 1

## 15.1 Objective

Begin testing Coldkeep under realistic filesystem failures.

Focus on deterministic, targeted fault injection for the highest-risk operations.

## 15.2 Scope

### Include

- deterministic fault scripts,
- ENOSPC simulation,
- write failure simulation,
- fsync failure simulation,
- rename failure simulation,
- restore/container/recovery tests.

### Exclude

- randomized chaos by default,
- broad performance testing,
- production FS abstraction exposure.

## 15.3 Fault Classes

Initial classes:

1. fail next write,
2. partial write,
3. fail fsync,
4. fail close,
5. fail rename,
6. fail mkdir,
7. fail stat,
8. fail remove,
9. ENOSPC after N bytes.

## 15.4 Implementation Steps

### Step 1 — Implement Scripted Fault FS

Create test-only package or internal helper:

```text
internal/fs/faultfs
```

Features:

- operation counter,
- fail operation by name,
- fail after N calls,
- fail after N bytes,
- deterministic error type.

### Step 2 — Test Restore Failure Phases

Test failures in:

- temp file creation,
- chunk write,
- temp fsync,
- rename/install,
- directory fsync,
- cleanup.

Validate:

- no silent success,
- no corrupted final file,
- temp cleanup behavior documented,
- retry/recovery behavior deterministic.

### Step 3 — Test Container Write/Seal Failures

Inject failures in:

- container write,
- header write,
- seal update,
- fsync,
- close.

Validate:

- DB and file state do not falsely claim sealed success,
- recovery can identify incomplete containers,
- verify detects partial writes.

### Step 4 — Test GC Delete Failures

Inject failures in:

- file delete,
- DB update after delete,
- DB update before delete.

Validate:

- no reachable data deletion,
- partial GC is recoverable,
- retry behavior safe.

### Step 5 — Keep Tests Deterministic

No random fault injection in PR CI yet.

Use named scenarios with stable expected outcomes.

## 15.5 Required Tests

```bash
go test ./internal/fs/...
go test ./internal/storage/...
go test ./internal/container/...
go test ./internal/maintenance/...
go test ./internal/recovery/...
```

## 15.6 Acceptance Criteria

- deterministic fault FS exists,
- restore failure scenarios covered,
- container write/seal failure scenarios covered,
- GC delete failure scenarios covered,
- no silent corruption in injected failures,
- fault tests are stable enough for CI or scheduled workflow.

---

# 16. v1.10.10 — Cross-Platform Validation

## 16.1 Objective

Ensure Coldkeep behavior is deterministic across Linux, macOS, and Windows where supported.

## 16.2 Scope

### Include

- GitHub Actions OS matrix,
- path normalization tests,
- restore behavior tests,
- symlink behavior documentation,
- Windows rename/overwrite behavior validation,
- shell script portability notes.

### Exclude

- making every shell benchmark script Windows-native,
- enterprise packaging.

## 16.3 Implementation Steps

### Step 1 — Define Supported Platform Contract

Document:

- Linux support level,
- macOS support level,
- Windows support level,
- SQLite/Postgres support per platform,
- symlink behavior per platform.

### Step 2 — Add Matrix Jobs

At minimum:

```yaml
strategy:
  matrix:
    os: [ubuntu-latest, macos-latest, windows-latest]
```

Start with lightweight tests.

### Step 3 — Add Path Normalization Tests

Test:

- `/absolute`,
- `C:\path`,
- `C:/path`,
- `..\evil`,
- `../evil`,
- `a//b`,
- `a\b`,
- UNC-like paths.

### Step 4 — Validate Restore Semantics

On each OS:

- restore normal file,
- restore existing target no-overwrite,
- restore overwrite,
- restore stored-path prefix,
- snapshot restore.

### Step 5 — Validate Symlink Policy

If symlink tests require privileges on Windows, mark conditionally.

Document skipped behavior.

### Step 6 — Separate Script Portability From Core Correctness

Shell scripts may remain Unix-focused if documented.

Core Go behavior should be cross-platform if claimed.

## 16.4 Acceptance Criteria

- OS support contract documented,
- CI matrix includes at least lightweight cross-platform tests,
- path normalization passes on all supported OSes,
- restore overwrite semantics documented per platform,
- Windows-specific restore behavior is tested or explicitly unsupported.

---

# 17. v1.10.11 — Stabilization & Regression Burn-down

## 17.1 Objective

Burn down remaining regressions, stabilize CI, and prepare for final v1.10 readiness review.

## 17.2 Scope

### Include

- flaky test cleanup,
- remaining S0/S1 review,
- deferred issue audit,
- accepted risk review,
- full regression matrix,
- documentation cleanup,
- release candidate hardening.

### Exclude

- new major features,
- large architecture refactors,
- engine extraction.

## 17.3 Implementation Steps

### Step 1 — Re-run Full Issue Classification

For every issue:

- fixed,
- accepted,
- deferred,
- suppressed,
- duplicate,
- open.

No issue remains unknown.

### Step 2 — Review All S0/S1 Items

Every S0/S1 must be:

- fixed,
- proven duplicate,
- proven false positive,
- accepted with explicit written rationale.

Acceptance of S0 should be extremely rare.

### Step 3 — Review CI Stability

Look for:

- flaky tests,
- long-running unstable jobs,
- hidden failure masks,
- scripts using `|| true`,
- artifact discovery bugs.

### Step 4 — Run Long Regression Suite

Recommended:

```bash
go test ./...
go test -race ./...
go test ./tests/adversarial/...
go test ./tests/integration/...
```

Run benchmark gates if stable.

### Step 5 — Validate JSON Contracts

Run automated JSON parser checks against JSON-mode commands.

Every supported JSON command should emit one valid JSON object.

### Step 6 — Validate Upgrade/Migration Paths

Test:

- fresh SQLite,
- upgraded SQLite,
- fresh Postgres,
- upgraded Postgres,
- legacy repository compatibility.

### Step 7 — Prepare Release Candidate

Create:

```text
v1.10.11-rc1
```

Allow only regression fixes after RC.

### Step 8 — Final Known-Issues Review

Create:

```text
docs/release/v1.10/known-issues-after-v1.10.md
```

This file should contain only accepted/deferred non-blockers.

## 17.4 Acceptance Criteria

- no unknown S0/S1 remains,
- CI is green and stable,
- JSON-mode validation passes,
- migration validation passes,
- accepted risks documented,
- release candidate created and verified.

---

# 18. v1.10.12 — Engine Boundary Preparation

## 18.1 Objective

Prepare the codebase for engine extraction without actually performing the extraction.

This release validates that the system is behaviorally stable enough for v1.11+ architectural work.

## 18.2 Scope

### Include

- boundary analysis,
- dependency graph review,
- CLI/storage coupling inventory,
- invariant ownership mapping,
- engine API sketch,
- no-behavior-change facade planning.

### Exclude

- moving large amounts of code,
- changing repository format,
- rewriting storage logic,
- public engine API commitment.

## 18.3 Implementation Steps

### Step 1 — Create Engine Boundary Document

Create:

```text
docs/architecture/engine-boundary-plan.md
```

Define candidate engine responsibilities:

- store,
- restore,
- remove,
- verify,
- snapshot,
- GC,
- stats,
- recovery.

Define non-engine responsibilities:

- CLI parsing,
- rendering,
- shell tooling,
- release scripts,
- documentation.

### Step 2 — Map Current Call Graph

Identify:

- CLI directly touching DB,
- CLI directly touching filesystem,
- duplicate validation logic,
- command-specific storage orchestration,
- places where rendering and behavior are mixed.

### Step 3 — Define Engine Facade Shape

Initial facade should be behavior-preserving.

Example:

```go
type Engine interface {
    Store(ctx context.Context, req StoreRequest) (StoreResult, error)
    Restore(ctx context.Context, req RestoreRequest) (RestoreResult, error)
    Verify(ctx context.Context, req VerifyRequest) (VerifyResult, error)
    GC(ctx context.Context, req GCRequest) (GCResult, error)
}
```

Do not implement full extraction yet.

### Step 4 — Define Invariant Ownership

For each invariant, decide where it belongs:

- CLI validation,
- engine request validation,
- storage layer,
- DB constraints,
- verify layer,
- CI regression tests.

Important rule:

> Correctness invariants must not live only in CLI parsing.

### Step 5 — Identify No-Behavior-Change Refactors

Allowed only if small and low-risk:

- move pure data structs,
- extract request/result types,
- centralize validators already stabilized,
- isolate renderers.

### Step 6 — Define v1.11 Entry Criteria

Engine extraction may start only if:

- no open S0/S1 issue,
- restore/GC/snapshot/verify regression tests pass,
- packed storage parity achieved,
- CLI contract stabilized,
- critical coverage baseline exists,
- CI is stable.

## 18.4 Acceptance Criteria

- engine boundary plan exists,
- CLI/storage coupling inventory exists,
- invariant ownership map exists,
- v1.11 entry criteria documented,
- no behavior-changing engine extraction performed yet.

---

# 19. Post-v1.10 Recommendations

## 19.1 v1.11 — Engine Boundary Introduction

Introduce engine facade while preserving existing behavior.

Do not rewrite.

## 19.2 v1.12 — Engine Migration

Move orchestration from CLI to engine incrementally.

## 19.3 v1.13 — Engine Contract Stabilization

Stabilize request/result contracts, library behavior, and contributor-facing APIs.

## 19.4 CodeRabbit Timing

Re-evaluate CodeRabbit after:

- engine boundary exists,
- PRs are smaller,
- contributor count grows,
- architecture has stabilized.

Before that, AI review likely creates more noise than value.

---

# 20. Final v1.10 Readiness Checklist

The v1.10 chain is complete when all of the following are true.

## Issue Discipline

- [ ] Every known issue is classified.
- [ ] Every S0 is fixed or formally resolved.
- [ ] Every S1 is fixed or formally resolved.
- [ ] Every suppression has rationale.
- [ ] Every deferred issue has release target or acceptance rationale.

## CLI Discipline

- [ ] Extra args rejected.
- [ ] Duplicate singleton flags rejected.
- [ ] Empty explicit values rejected.
- [ ] Boolean flags behave consistently.
- [ ] Numeric values are finite and bounded.
- [ ] JSON output is valid and single-envelope.

## Storage Discipline

- [ ] Packed and legacy paths have parity.
- [ ] Refcount/liveness semantics are centralized.
- [ ] Container filename validation is central.
- [ ] Store failure cleanup is tested.
- [ ] Restore planning is centralized.

## GC Discipline

- [ ] Snapshot reachability protects packed and legacy storage.
- [ ] GC simulation matches real eligibility.
- [ ] Repair does not legitimize invalid graphs.
- [ ] No reachable data is reclaimable.

## Verification Discipline

- [ ] Verify catches packed corruption.
- [ ] Verify catches legacy corruption.
- [ ] Verify summaries are honest.
- [ ] Fast/deep semantics are documented.

## Recovery Discipline

- [ ] Recovery validates filenames.
- [ ] Recovery reports accurate warning/error counts.
- [ ] Quarantine behavior is safe.
- [ ] Interrupted states are deterministic.

## CI Discipline

- [ ] Codacy passive integration complete.
- [ ] Critical-path coverage baseline exists.
- [ ] Security findings are triaged.
- [ ] CI audit is stable.
- [ ] No script masks critical failures.

## Engine Readiness

- [ ] Engine boundary plan exists.
- [ ] Invariant ownership map exists.
- [ ] CLI/storage coupling inventory exists.
- [ ] v1.11 entry criteria are satisfied.

---

# 21. Final Principle

The v1.10 series should be treated as Coldkeep’s correctness hardening contract.

It is acceptable for this series to be long.

It is not acceptable for this series to leave unknown correctness risk unclassified.

Coldkeep should enter engine extraction only after the current system is stable, explicit, tested, and trusted.

