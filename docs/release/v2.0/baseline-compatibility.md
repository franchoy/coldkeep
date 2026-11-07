# Action 2.01 baseline and compatibility ledger

Adopted from planning edition 1.1 and the accepted Phase 0 follow-up on
7 November 2026. These are inspected baseline facts and explicit future
contracts, not a declaration that the v2 product is qualified.

## Bound identities and build requirements

The authoritative archive is `coldkeep-main_v1.13.19.zip`, 11,943,914 bytes,
SHA-256 `84888b7e47d4e571511edc91dfbeab5705e6d6a6b308769aea703eb465d85414`.
The accepted Phase 0 inspection matched every tracked original launch file to
the archive. Its original launch was `e8a501d6edbc32cd440562065f1a851d8afdc03d`;
the independently verified stable reader is D19
`d3581602aee2599547564322fe083baae1d52063`, targeted by annotated tag object
`60b6878114f936b295c415605e8af31def2aba5a`. An archive hash is not a commit.
Phase 1A starts separately at accepted correction
`eb9ddebfe7d6d2c24f7b84052b08b1facfe953cb`.

[go.mod](../../../go.mod) pins language Go 1.25.0, toolchain Go 1.26.10,
`lib/pq` 1.11.2, `mattn/go-sqlite3` 1.14.24, `klauspost/compress` 1.20.2 and
`x/sys` 0.44.0. Use `GOTOOLCHAIN=local`; SQLite execution requires CGO and a C
compiler. PostgreSQL is the normal operational CLI connection path in
[db.ConnectDB](../../../internal/db/db.go). Existing CI uses a disposable
PostgreSQL service; copied SQLite fixtures below use no server and do not change
the chosen PostgreSQL Docker product profile. No dependency/toolchain upgrade
or database-default change belongs to 2.01.

## Reused operation map

All 25 methods in [Engine](../../../internal/engine/engine.go) remain the
behavior-preserving facade; typed requests/results live in `internal/engine`.
The planned UI/API/CLI must share these operations through application services,
with later admission/authentication and durable-job ownership above them.

| Existing operations | Source seam | Reuse boundary / missing product behavior |
|---|---|---|
| `Stats`, `Inspect`, `Verify` | `internal/engine/default_engine.go`; `internal/catalog`, `internal/verify` | Existing observations and explicit verification levels; no single health flag or automatic background verification. |
| `SnapshotList`, `SnapshotShow`, `SnapshotStats`, `SnapshotDiff` | `internal/engine/default_engine.go`, `internal/snapshot/snapshot.go` | Inherited catalog membership/history, not proof of a fresh filesystem capture. |
| `SnapshotCreate`, `SnapshotDelete`, `SnapshotRestore` | `internal/engine/snapshot_create.go`, `snapshot_delete.go`, `snapshot_restore.go` | Catalog-only creation; metadata deletion and existing restore semantics. New capture completeness/directory membership remain 2.10–2.12. |
| `Store`, `StoreFolder` | `internal/engine/default_engine_routing_helpers.go`, `internal/storage/store.go` | Existing file ingestion and worker finalization. `discoverFiles` skips directories; chunkers collect buffers. No empty-directory capture, bounded generation pipeline or atomic tree snapshot is implied. |
| `ListFiles`, `SearchFiles` | `internal/engine/current_files.go`, `internal/catalog/current_files.go` | Mutable current-path views; loading/sorting before pagination does not establish bounded historical browsing. |
| `GetConfiguration`, `SetConfiguration` | `internal/engine/repository_configuration.go` | Existing validated repository write defaults, not new setup/admission authority. |
| `GarbageCollect`, `PlanGarbageCollection` | `internal/engine/default_engine_routing_helpers.go`; `internal/gc`, `internal/catalog` | Existing roots and coordination; live SQLite GC remains refused. Every future generation/pending root must join common reachability before reclamation. |
| `Repair`, `Recover`, `Doctor` | `internal/engine/repair.go`, `recover.go`, `doctor.go` | Corrective operations; doctor runs recovery before validation. None is passive inspection or a recovery exporter. |
| `Remove`, `RemoveStoredPaths` | `internal/engine/default_engine.go`, `remove_stored_paths.go` | By-ID removal and current path unlinking are distinct; payload reclamation is GC-owned. Neither deletes source originals. |
| `Restore`, `RestoreStoredPath` | `internal/engine/default_engine.go`, `restore_stored_path.go`; `internal/storage/restore.go` | Recipe replay, temporary chunk pins, destination confinement and atomic installation within documented local bounds. |

The CLI dispatch in [main.go](../../../cmd/coldkeep/main.go) also owns parsing,
batch orchestration, `init`, `version`, `simulate`, benchmark commands and
rendering. `simulate store`/`store-folder` are repository-free; live `simulate gc`
requires the repository lease. These are not new daemon endpoints.
[application.Open](../../../internal/application/session.go) composes one
configured runtime; callers own coordination. `db.ConnectDB` invokes schema
ensure/bootstrap. `ParseStorageContext` can migrate SQLite;
`OpenLocalStorage` runs startup recovery. The default application path does
not itself invoke that recovery. A `ContainerDir` override after construction
does not prove every adapter shares one root. 2.03 must introduce deliberate
passive modes and validate one catalog/root/key/profile tuple before reuse.

## Version domains and preserved readers

| Domain | Existing representation / owner | Compatibility boundary and required refusal |
|---|---|---|
| Application | `internal/version/version.go`: stable reader 1.13.19; active 2.0.0-dev.0 | SemVer/prerelease plus separate build provenance. An application change does not imply a catalog, payload, API or recovery-format change. |
| Local API | No daemon/local HTTP API exists. `/api/v1` is proposed at 2.02/2.08 | Later handlers negotiate explicit capabilities and reject unsupported operations before mutation. This is not implemented by 2.01. |
| CLI JSON | `internal/cli/render/envelope.go`: observation envelope `meta.version = v1.7`; command-specific envelopes also exist | Additive fields retain their contract; breaking envelope/meaning changes require a contract bump. Do not treat this as the proposed HTTP API version. |
| Catalog | `internal/db/schema_version.go`: schema 17 singleton `catalog_version`; SQLite/PostgreSQL inspectors and migrations | Future, malformed or missing metadata must stop compatible opening; no empty replacement. Schema-17 column rename also fences pre-v17 readers expecting `version`. Existing ensure/migrate constructors are not passive. |
| Container | `internal/container/format.go`: 64-byte `ColdKeep` header, current major/minor 1.0; legacy major 0 retained | Validate header/CRC, lengths and supported major. Header version is independent of the application; unknown major is refused. Old whole-container compression artifacts mentioned in SECURITY remain a historical limitation, not a newly qualified reader profile. |
| Legacy chunk payload | `blocks` rows; `internal/storage/restore.go`, `restore_dual_compat.go`, `internal/blocks/plain.go`, `aesgcm.go` | Direct chunk extents, format 1; plaintext or AES-GCM with per-row nonce. Check recipe/hash/codec rather than assuming all rows use current defaults. |
| Packed payload | `storage_blocks`/`chunk_block_refs`; `StorageBlockReader`, `DualCompatChunkResolver`; `internal/blocks/block_format_v1.go` | CKBL encoded header v1, internal codec none; separate storage encryption and compression metadata. Unknown encoded version is refused. See metadata-only limitation below. |
| Compression/encryption | `internal/storage/compression`, `internal/verify/verify_block_pipeline.go`, `internal/blocks` | `none` or `zstd` (bounded decoded block); `none`/plain or nonce-prefixed AES-GCM for packed blocks. Legacy AES uses its row nonce. Unknown codec/invalid sizes/authentication/hash failures refuse. Missing keys are not corrupt-data diagnoses. |
| Chunker provenance | `internal/chunk`, logical-file/chunk version columns | v1-simple-rolling and v2-fastcdc writers; restore replays recipes and tolerates well-formed future chunker labels, not malformed/empty metadata. No rechunking is required to restore. |
| Recovery package | No implemented export/import package format | Versioned, bounded manifest and explicit reader/schema/key/capability requirements belong to 2.03. Refuse incomplete, unsupported or corrupt packages before activation; no schema/IDs are added now to anticipate that format. |

The fixture investigation found that by-ID restore accepts a changed
`storage_blocks.format_version=2` when the encoded payload is still valid CKBL
v1. `VerifyRepositoryFast` rejects that catalog metadata; `DecodeBlock` separately
rejects an actual future encoded header. Both the refusal and restore limitation
are recorded in the oracle. A successful restore alone is therefore not a full
format-compatibility verdict. Future 2.03/2.15 admission must inspect catalog and
payload requirements explicitly; 2.01 does not change the reader to hide this.

## Failure model, names and metadata

[SECURITY.md](../../../SECURITY.md) retains incomplete crash consistency:
filesystem bytes and catalog state may diverge, orphan bytes can remain, and
process interruption tests do not prove sudden-power-loss durability. Later
2.11/2.14/2.17 must qualify payload flush, catalog commit and acknowledgement
ordering for each declared profile. Encryption covers payloads, not filenames,
catalog metadata or access patterns; operator key loss prevents recovery.
Cooperative same-host leases are not cross-host/NFS/SMB fencing or protection
against an attacker controlling both catalog and containers.

Restore verifies reconstructed content before installation, then applies
supported metadata. A strict metadata failure can occur after correct bytes are
visible. A separate final-file reread is an additional observation, not an
implicit production guarantee. Our fixture harness does that reread explicitly.
Mode and mtime are selected on Linux; UID/GID, ACLs, xattrs, links, sparse layout,
alternate streams and source-root metadata are not qualified here.

`NormalizeSnapshotPath` rejects leading/trailing whitespace and normalizes
backslashes to separators. Physical-path normalization and secure-install
configuration can also trim/clean paths. The fixtures restore Linux case pairs,
NFC/NFD names and embedded newlines distinctly, and retain trailing-space refusal
and backslash reinterpretation as baseline limits. This is not lossless capture
of arbitrary Unix bytes or proof of Windows/macOS destination compatibility.
Exact name encoding, opaque entry identity and destination mapping remain
2.04/2.10/2.12 obligations. Empty files are covered; empty directories require
future directory membership, not invented empty-file recipes.

## Maintained executable evidence

[Manifest](../../../testdata/product-fixtures/manifest.json) and
[harness guide](../../../testdata/product-fixtures/README.md) bind 23 selected
cases to retained bytes or named reproducible builders. Seven cases reuse the
persisted compression repository written by `550374d40d102bb5cbb5184799ef9b03fdaeefac`;
three source asset hashes are immutable. Legacy/header-0, mixed recipes,
snapshot history and native names use explicit disposable setups based on the
existing regression builders. Keys are referenced separately and excluded from
result output. Nothing recreates old payloads with the new product writer.

Run the same oracle against exact D19 and current code with
`scripts/run_product_fixtures.py`. The baseline runner verifies every original
Git blob, overlays only test harness/manifest, and adapts only the import prefix.
Ordinary Linux CI runs the current reader under `-race`, rejects missing/skipped
case evidence and uploads compact raw results. Source assets remain unchanged;
restore pin/unpin activity is checked for net catalog-semantic preservation.
Unexpected working-copy failures remain in the selected evidence directory.
The matrix is deliberately representative, not a Cartesian platform/backend
qualification. Existing PostgreSQL/runtime evidence is reused for unchanged code.
