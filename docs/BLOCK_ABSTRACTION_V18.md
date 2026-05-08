# v1.8 Block Abstraction Design Lock (Phase 1 Step 1)

Status: Locked implementation contract for v1.8 foundation work.

Purpose:

- Freeze non-negotiable constants and invariants before schema and runtime implementation.
- Prevent mid-implementation semantic drift.
- Keep v1.7 runtime behavior unchanged during Phase 1.

## Locked Constants

These values are the baseline for v1.8 unless an explicit later benchmark decision updates them.

```text
BlockFormatVersion    = 1
BlockBinaryCodec      = "none"   # codec field in the block binary header (always plaintext structure)
StorageBlocksCodec    = "none" | "aes-gcm"  # stored in storage_blocks.codec; depends on COLDKEEP_CODEC
TargetBlockSize       = 1 * 1024 * 1024   # 1 MiB
MaxBlockSize          = 1 * 1024 * 1024   # 1 MiB
BlockHashPolicy       = required
```

Note on the two codec concepts:

- `BlockBinaryCodec` is the codec field embedded in the block binary format header. It always equals `"none"` because the binary format always describes plaintext chunk layout regardless of whether the block is subsequently encrypted.
- `StorageBlocksCodec` is what gets persisted in `storage_blocks.codec`. It equals `"none"` for plain writes and `"aes-gcm"` for encrypted writes. When `"aes-gcm"`, stored bytes are a 12-byte nonce prefix followed by AES-GCM ciphertext of the full encoded block.

## Locked Invariants

1. Chunk hash is unchanged and always computed from plaintext chunk bytes.
2. Block payloads are immutable once committed.
3. A block is the atomic physical read/decode unit.
4. Each chunk belongs to exactly one block.
5. No chunk spans multiple blocks in v1.8.
6. Any open block builder must be flushed at operation end.
7. Every persisted v1.8 storage block must carry a plaintext block hash.

## Phase 1 Scope Guard

- Phase 1 may add schema/types/docs needed for v1.8.
- Phase 1 must not change v1.7 runtime behavior.
- Compatibility expectation remains additive: existing v1.7 data stays readable.

## Phase 1 Step 2 Findings (v1.7 Baseline)

- Current `blocks` model is effectively one chunk to one block (`blocks.chunk_id` is unique).
- Current block placement metadata already includes container placement (`container_id`, `block_offset`) and size fields.
- Current restore path resolves blocks by chunk identity (`JOIN blocks b ON b.chunk_id = c.id`).

## Phase 1 Step 3 Schema Strategy (Locked)

Chosen strategy: extend schema additively (recommended path).

v1.8 will not replace old layout in-place during upgrade. Instead, it introduces explicit packed-block entities while preserving v1.7 readability:

- `storage_blocks`: physical immutable block records (container placement, format/codec/sizes, integrity metadata).
- `chunk_block_refs`: chunk-to-block segment mapping (`chunk_id -> block_id + offset + size`).

Design intent:

- Preserve v1.7 data access via legacy `blocks` adapter behavior.
- Enable v1.8 packed blocks where one physical block can hold multiple chunks.
- Keep migration additive (no forced data rewrite).

## Naming and Compatibility Note

- `storage_blocks` is the canonical v1.8 packed-block table name in design docs and planning.
- Legacy `blocks` remains part of compatibility read-path support for mixed repositories.
- v1.8 reads v1.7 repositories; v1.7 is not expected to read v1.8 packed-block data.
- New data written by v1.8 uses packed blocks; existing v1.7 data is not auto-rewritten.

## Phase 1 Step 4 Target Schema (Locked)

### Table: `storage_blocks`

Represents the physical stored block unit.

```sql
storage_blocks (
    id                BIGSERIAL PRIMARY KEY,

    format_version    INT NOT NULL,
    codec             TEXT NOT NULL, -- "none" for plain writes; "aes-gcm" for encrypted writes

    plaintext_size    BIGINT NOT NULL,
    stored_size       BIGINT NOT NULL,

    container_id      BIGINT NOT NULL,
    container_offset  BIGINT NOT NULL,

    block_hash        BYTEA NOT NULL, -- required for block validity verification

created_at        TIMESTAMP NOT NULL DEFAULT NOW()
)
```

### Table: `chunk_block_refs`

Represents per-chunk placement inside one physical block.

```sql
chunk_block_refs (
    chunk_id          BIGINT NOT NULL,
    block_id          BIGINT NOT NULL,

    offset_in_block   BIGINT NOT NULL,
    size_in_block     BIGINT NOT NULL,

    PRIMARY KEY (chunk_id),
    FOREIGN KEY (block_id) REFERENCES storage_blocks(id)
)
```

### Existing Tables Kept Unchanged

- `chunk` identity remains plaintext-based (`chunk_hash = plaintext identity`).
- `file_chunk` restore recipe remains the same logical ordered mapping.

## Phase 1 Step 5 Compatibility Strategy (Locked)

v1.8 compatibility must support all of the following:

- v1.7 data (single-chunk blocks)
- v1.8 data (multi-chunk packed blocks)
- mixed repositories containing both layouts

Chosen implementation strategy for v1.8: Option A (cleanest).

- Read-path compatibility will treat each legacy v1.7 `blocks` row as an adapter-level storage block view with:
  - one chunk,
  - `offset_in_block = 0`,
  - `size_in_block = chunk/plaintext size`.
- New v1.8 packed data will use `storage_blocks` + `chunk_block_refs` directly.
- Mixed repositories are resolved through unified read abstraction that can load either legacy adapted layout or native packed layout.

Explicit Phase 1 decision:

- Option B (bulk migration converting legacy `blocks` into packed-table rows during upgrade) is deferred and is not part of Phase 1.
- No forced rewrite of existing physical payloads or metadata is allowed in Phase 1.

## Phase 1 Step 6 Versioning Strategy (Locked)

Version field introduced and fixed for v1.8 packed blocks:

```text
format_version = 1
```

Version mapping rules:

- Legacy v1.7 `blocks` rows are treated by adapter logic as implicit `format_version = 0`.
- Native v1.8 packed blocks in `storage_blocks` are persisted with `format_version = 1`.

Forward-compatibility intent:

- Reader abstraction must branch by effective format version and fail closed on unsupported versions.
- This mapping is reserved to enable future v1.9+ format evolution without rewriting v1.7/v1.8 data.

## Phase 1 Step 10 No-Behavior-Change Rule (Locked)

Phase 1 is preparation-only. At the end of Phase 1, runtime behavior must remain v1.7-equivalent.

Mandatory constraints:

- System behavior must match v1.7 for store, restore, verify, and GC execution paths.
- No block packing is enabled in Phase 1.
- No restore/read-path block-abstraction switching is enabled in Phase 1.
- New v1.8 schema/types/interfaces are foundational and must not alter runtime decisions yet.

Release gate for Phase 1 completion:

- Existing v1.7 repositories continue to operate unchanged.
- New Phase 1 artifacts exist for later phases but remain behaviorally inert.

## Phase 2 Design Decisions (Locked)

### Mandatory Block Hash

```text
block_hash = hash(plaintext_encoded_block)
```

Rules:

- Computed before encryption.
- Validates full block integrity.
- Independent from container placement and encryption state.

### v1.8 Encoding Pipeline

```text
chunks -> pack -> encode -> hash -> encrypt -> store
```

Implementation status (shipped v1.8):

- v1.8 fully supports both `plain` and `aes-gcm` codec settings for packed-block writes.
- `COLDKEEP_CODEC=plain`: `storage_blocks.codec = "none"`, stored payload = encoded block bytes.
- `COLDKEEP_CODEC=aes-gcm`: `storage_blocks.codec = "aes-gcm"`, stored payload = 12-byte nonce ∥ AES-GCM ciphertext of the encoded block. The read path (`StorageBlockReader.decryptBlock`) strips the nonce and decrypts before decoding.
- Block-level compression is not included in v1.8 and is deferred to v1.9.

Not used:

- hash(encrypted)
- hash(per chunk)

## Phase 2 Step 1 Binary Format (Locked)

Final deterministic layout:

```text
| HEADER | CHUNK_TABLE | PAYLOAD |
```

Header (fixed size):

- `magic` (`uint32`) = `0x434B424C` (`"CKBL"`)
- `version` (`uint16`) = `1`
- `codec` (`uint16`) = `0` (none)
- `chunk_count` (`uint32`)
- `plaintext_size` (`uint64`)

Chunk table entry (repeated `chunk_count` times):

- `chunk_id` (`uint64`)
- `offset` (`uint64`)
- `size` (`uint64`)

Payload:

- Concatenated chunk plaintext bytes.

Phase boundary note:

- Step 1 introduces isolated encode/decode/hash logic and format validation only.
- Runtime store/restore integration remains deferred to later Phase 2 steps.

## Phase 2 Step 5 Hashing Rule (Locked)

Block hash target for v1.8:

```text
block_hash = hash(encoded_plaintext_block_bytes)
```

Current project algorithm choice:

```text
sha256
```

Rules:

- Hash must be computed from encoded plaintext block bytes.
- Hash must not be computed from encrypted/transformed stored bytes.

Forward-compatibility note:

- v1.9 transform-aware metadata semantics are locked in `docs/storage_transform_semantics.md`.
- That document preserves `block_hash` as the logical payload hash and defines future additive semantics for compressed and physical layer metadata.

## Phase 2 Step 9 Isolation Rule (Locked)

Phase 2 block format work remains isolated until explicitly unlocked by later steps.

Do not during Phase 2 isolation window:

- plug block-format core into store path,
- plug block-format core into restore path,
- change DB write/read behavior for active runtime flows.

Scope allowed in Phase 2 isolation window:

- in-memory types,
- binary encode/decode/hash logic,
- isolated unit tests,
- design documentation.

## Phase 7 Step 1 Compatibility Contract (Locked)

Phase 7 validates real upgrade behavior from v1.7 repositories to v1.8 runtime.

Mandatory contract:

- v1.8 must read v1.7 repositories.
- v1.8 must restore v1.7 data byte-identically.
- v1.8 may write new data using packed blocks.
- Mixed repositories (legacy + packed) are valid and supported.
- v1.7 is not required to read repositories that contain v1.8 packed data.
- Upgrade must not force rewrite of existing v1.7 data.

Phase 7 compatibility evidence must cover all of the following repository states:

- legacy-only repositories,
- packed-only repositories,
- mixed legacy + packed repositories,
- snapshot-retained data after upgrade,
- GC execution after upgrade.

Canonical upgrade scenario under test:

```text
v1.7 repository -> opened by v1.8 -> new data added -> restore/verify/GC remain correct
```

## Phase 7 Step 2 Fixture Strategy (Implemented)

Deterministic compatibility fixture generation is implemented in integration tests.

Primary path (default):

- Use v1.8 test/runtime helpers to seed fixture data with v1-style chunker metadata and legacy `blocks` companion rows.
- Fixture shape includes:
  - one large file,
  - many small files,
  - duplicate-content files,
  - full snapshot creation,
  - one file replaced after snapshot to create a deleted-in-current-but-snapshot-retained logical file.

Optional path (if available):

- Use an actual released v1.7 binary to build the fixture, then validate with v1.8 verify/GC.
- Controlled by env var `COLDKEEP_V17_BIN` in integration tests.

Reference integration tests:

- `TestPhase7BuildDeterministicV17StyleFixtureIntegration`
- `TestPhase7BuildFixtureWithActualV17BinaryIntegration`

## Phase 7 Step 3 Legacy-Only Restore Test (Implemented)

Legacy-only compatibility test scope:

- open a legacy-only repository fixture with v1.8 runtime,
- restore all completed legacy logical files,
- compare restored file hashes with persisted logical hashes,
- run verify (`system`, standard level).

Expected outcome (locked):

- restore succeeds,
- verify succeeds,
- packed metadata tables are not required for success in this scenario.

Reference integration test:

- `TestPhase7LegacyOnlyRestoreAndVerifyIntegration`

## Phase 7 Step 4 Upgrade-and-Add-New-Data Test (Implemented)

Upgrade flow under test:

1. Create legacy repository fixture.
2. Open repository using v1.8 runtime.
3. Store new file and new folder content.
4. Assert new chunks are mapped through packed metadata (`storage_blocks` + `chunk_block_refs`).
5. Restore old data and verify hashes.
6. Restore new data and verify hashes.
7. Restore all data together and verify hashes.

Expected outcome (locked):

- old data remains on legacy path (no packed refs required for old chunks),
- new data uses packed path,
- old and new data both restore byte-identically.

Reference integration test:

- `TestPhase7UpgradeAndAddNewDataIntegration`

## Phase 7 Step 5 Mixed Verify Test (Implemented)

Scenario:

- Start from an upgraded mixed repository (legacy data + newly packed data).
- Run system verify in v1.8.

Verify must validate:

- legacy blocks,
- packed blocks,
- `chunk_block_refs`,
- `block_hash`,
- encoded block table,
- decoded chunk-slice hashes.

Expected outcome (locked):

- verify passes cleanly.

Reference integration test:

- `TestPhase7MixedVerifyAfterUpgradeIntegration`

## Phase 7 Step 6 Mixed GC Test (Implemented)

Flow under test:

1. Create legacy data.
2. Add packed data after upgrade.
3. Remove subset of legacy data.
4. Remove subset of packed data.
5. Run GC dry-run.
6. Run GC.
7. Restore remaining data.
8. Run verify.

Expected outcome (locked):

- legacy dead data is reclaimed safely,
- packed dead data is reclaimed only at whole-block granularity,
- partially-live packed blocks are retained,
- remaining legacy and packed data restore byte-identically,
- verify passes cleanly.

Reference integration test:

- `TestPhase7MixedGCAfterUpgradeIntegration`

## Phase 9 Finalization: Block Size Default Lock

Status: v1.8 finalization and release hardening.

Purpose:

- Lock the packed block size default at 1 MiB for production deployment.
- Retain operator override capability for tuning on specific workloads.
- Document override as advanced operator feature, not default behavior.

Release contract summary:

- v1.8 introduces packed storage blocks for new writes.
- v1.8 reads v1.7 repositories and does not rewrite historical v1.7 data during upgrade.
- New data written by v1.8 uses packed blocks; mixed repositories are valid steady-state.
- The compiled-in default packed block size is 1 MiB.
- `COLDKEEP_BLOCK_TARGET_SIZE_MB` is an advanced write-time override for operators.
- v1.7 is not guaranteed to read repositories that contain v1.8 packed-block data.
- Both `plain` and `aes-gcm` codecs are fully supported for packed-block writes in v1.8. When `COLDKEEP_CODEC=aes-gcm`, `storage_blocks.codec = "aes-gcm"` and the stored payload is a 12-byte nonce prefix followed by AES-GCM ciphertext of the encoded block.
- Block-level compression is not included in v1.8; v1.9 will extend this design with block-level compression.

### Default Block Size (Locked for v1.8)

#### Compiled-in default: 1 MiB (1,048,576 bytes)

This is the target packed block size used by default for all new writes.

Contract:

- All new writes will pack chunks into 1 MiB blocks.
- Existing blocks remain self-describing via per-block metadata (e.g., `storage_blocks.plaintext_size`).
- Readers never depend on the configured block size; they read per-block metadata from `storage_blocks` table.
- Restore behavior is independent of default block size; readers replay stored block metadata.

### Operator Override: COLDKEEP_BLOCK_TARGET_SIZE_MB

**Advanced tuning knob for operator experimentation. Not recommended for production unless benchmarked for your workload.**

Environment variable:

```text
COLDKEEP_BLOCK_TARGET_SIZE_MB = <size_in_mb>
```

Behavior:

- **Location**: Read at write time; affects new writes only.
- **Validated values**: 1, 2, 3 (from Phase 8 benchmarking).
- **Invalid/unsupported values**: Fall back to 1 MiB default with log warning.
- **Precedence**: `COLDKEEP_BLOCK_TARGET_SIZE_MB` is the documented v1.8 override for release builds.

Contract:

- Readers never check this override; they read per-block metadata from storage.
- Changing the override during repository lifetime is safe; existing blocks remain intact.
- New blocks written after override change use the new size; old blocks retain their persisted sizes.
- Multi-size repositories (mixed 1 MiB and 2 MiB blocks) are supported and expected if override is used.

### Use Cases for Override

**Allowed (supported by tests and documentation):**

1. **Benchmarking alternative sizes**: Evaluate 2 MiB or 3 MiB on replicated test workloads to measure throughput/latency tradeoffs.
2. **Size-specific tuning**: Workload-specific optimization on dedicated systems (e.g., large sparse-file workloads with 2 MiB packing).
3. **Integration testing**: Test multi-size repository semantics during development.

**Not recommended:**

- Switching sizes frequently; plan and benchmark before deploying a custom size.
- Sizes outside {1, 2, 3} MiB; only these candidates were measured and validated.
- Assuming production improvement without explicit benchmarking on your workload.

### Invariants Preserved

1. Block hash is computed from plaintext block bytes regardless of block size.
2. Restore correctness is unaffected by block size; reads follow per-block metadata.
3. GC safety is preserved; blocks are reclaimed only at whole-block granularity.
4. Verify behavior is size-independent; integrity checks remain per-block.

### Registry Impact

The override only affects:

- **Write path**: `StoreService.prepareAndStoreFile()` reads the override when initializing a `BlockBuilder`.
- **Reader path**: No impact; `StorageBlockReader.ReadBlock()` uses per-block metadata from `storage_blocks`.

The override does not affect:

- GC behavior (unit of deletion remains per-block).
- Verify behavior (per-block hash/integrity checks).
- Restore determinism (metadata-driven replay).
- Compatibility (existing blocks are self-describing).
