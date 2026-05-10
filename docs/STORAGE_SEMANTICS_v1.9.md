# Storage Semantics v1.9 — Frozen Contract

**Status:** FROZEN (v1.9 release lock)  
**Date:** 2026-05-09  
**Phase:** Step 6.14 — Freeze v1.9 Storage Semantics  
**Purpose:** Foundation for engine extraction; all storage behavior must be unambiguous

---

## 1. Transform Ordering (The Block Lifecycle)

### 1.1 Write-Path Transform Pipeline

All user data follows a deterministic transform sequence:

```
plaintext (file on disk)
    ↓
chunked (split into fixed-size chunks via chunker)
    ↓
[block encoding] (encode chunk payload + metadata structure)
    ↓
compute_logical_hash (SHA256 of encoded block before transformation)
    ↓
[compression stage: IF compression enabled]
  IF compression = "zstd":
    compress_payload (via zstd level N)
    compute_compressed_hash (SHA256 of compressed bytes)
  ELSE compression = "none":
    skip compression stage
      compressed_hash = logical_hash (same bytes at this stage)
      compressed_size = plaintext_size (encoded logical payload size)
    ↓
[encryption stage: IF encryption enabled]
  IF codec = "aes-gcm":
    encrypt_payload (AES-256-GCM, nonce = random 12 bytes per block)
      compute_physical_hash (SHA256 of persisted bytes: nonce || ciphertext)
   ELSE codec = "none":
    skip encryption stage
    physical_hash = compressed_hash
    ↓
[persist to container]
  Write to container file at current_offset
  fsync to disk
  Update block metadata in DB:
      - container_offset (position in container; storage_blocks.container_offset)
    - stored_size (bytes written to disk)
      - plaintext_size (encoded logical block payload size)
    - logical_hash (input to transformations)
      - compressed_hash (payload hash after compression stage; equals logical_hash when compression_codec=none for v1.9+ writes)
    - physical_hash (final state after all transforms)
   - codec (none or aes-gcm)
    - compression_codec (none or zstd)
      - compressed_size (bytes after compression stage; equals plaintext_size when compression_codec=none)
```

**FROZEN INVARIANT:** This ordering is immutable. Reordering stages requires v2.0.

### 1.2 Read-Path Verification Pipeline

All verify and restore operations traverse transforms in reverse:

```
persisted_bytes (container file at container_offset...container_offset+stored_size)
    ↓
[verify physical_hash IF present]
   Check: SHA256(persisted_payload_bytes) == physical_hash
     (aes-gcm persisted bytes are nonce || ciphertext)
  If fails: ABORT (physical integrity broken)
    ↓
[decrypt IF codec=aes-gcm]
  Decrypt COLDKEEP_KEY + nonce → compressed_payload
  If fails (auth tag mismatch): ABORT (tampering detected)
    ↓
[verify compressed_hash IF present]
  Check: SHA256(compressed_payload) == compressed_hash
  If fails: ABORT (compression layer corrupted)
    ↓
[decompress IF compression_codec=zstd]
  Decompress compressed_payload → logical_payload
  If fails (malformed): ABORT (decompression error)
    ↓
[verify logical_hash]
  Check: SHA256(logical_payload) == logical_hash
  If fails: ABORT (logical content corrupted)
  ↓
[decode block structure]
  Parse logical_payload as packed block (magic, version, chunks, offsets, hashes)
  Validate layout (chunk indexes, offsets, sizes contiguous)
  If fails: ABORT (block structure invalid)
    ↓
[verify chunk references]
  For each chunk in decoded block:
    Check chunk_id exists in DB and is COMPLETED
    Check chunk metadata matches decoded refs (offset, size, hash)
    If fails: ABORT (chunk graph broken)
    ↓
output: chunk payloads (combining all verified chunks)
```

**FROZEN INVARIANT:** Verification stages are fixed. Additional stages require v2.0 contract update.

---

## 2. Hash Semantics

### 2.1 Four Hash Types

| Hash | Scope | Computed On | Nullable | Version | Purpose |
|------|-------|-----------|----------|---------|---------|
| `logical_hash` (block_hash) | Encoded block (pre-transform) | Plaintext after encoding | NEVER | v1.0+ | Proof of logical content; read-path verification target |
| `compressed_hash` | Payload after compression stage | Zstd output bytes OR encoded logical bytes when compression_codec=none | YES (legacy v1.6-v1.8 null) | v1.9+ | Compression-stage integrity; expected for new blocks, including compression_codec=none |
| `physical_hash` | Persisted payload bytes | nonce || ciphertext (aes-gcm) OR compressed payload bytes (codec=none) | YES (legacy v1.6-v1.8 null) | v1.9+ | Final on-disk-state integrity; present for new writes in both none and aes-gcm modes |
| `chunk_hash` | Individual chunk | Raw plaintext chunk before encoding | NEVER | v1.0+ | Dedup identity; stored in `chunk.chunk_hash` |

### 2.2 Hash Computation Rules (FROZEN)

```
IF plaintext_payload → encode → logical_hash(encoded_block)
   result stored as storage_blocks.block_hash

IF compression_codec = "zstd":
   compressed_hash = SHA256(zstd_compressed(logical_payload))

IF compression_codec = "none":
   compressed_hash = logical_hash (same bytes at compression stage)

IF codec = "aes-gcm":
   physical_hash = SHA256(nonce || aes_gcm_ciphertext)

IF codec = "none":
   physical_hash = compressed_hash for v1.9+ writes; legacy rows may be NULL
   
RESTORE/VERIFY: Must validate in reverse order
   1. physical_hash (if present) — proves on-disk bytes untouched
   2. compressed_hash (if present) — proves compression layer
   3. logical_hash (always) — proves logical content
```

**CRITICAL:** Hash values are computed from the block's actual persisted metadata.
For v1.9+ writes, `compressed_hash` and `physical_hash` are expected regardless
of `compression_codec=none` or `codec=none`; missing values are tolerated only
for legacy rows.

Legacy blocks (v1.6-v1.8) may have NULL `compressed_hash` / `physical_hash`. Verify skips missing hashes with debug logging.

### 2.3 Hash Equality Invariants (FROZEN)

```
For uncompressed+none blocks:
   logical_hash = compressed_hash
   physical_hash = compressed_hash (v1.9+); legacy rows may be NULL

For uncompressed+aes-gcm blocks:
   logical_hash = compressed_hash
   logical_hash ≠ physical_hash (encryption changes bytes)

For compressed+none blocks:
   logical_hash ≠ compressed_hash (compression changes size/content)
   physical_hash = compressed_hash (no encryption)

For compressed+aes-gcm blocks:
   logical_hash ≠ compressed_hash ≠ physical_hash
   (compression then encryption)

Restore: Verify inProgressively in reverse; stop on first mismatch.
```

### 2.4 Hash Role Authority (FROZEN)

The three-hash model is canonical and role-locked:

- `block_hash`: canonical logical block identity
- `compressed_hash`: transform-stage integrity checkpoint
- `physical_hash`: persisted payload integrity checkpoint

Identity authority constraints:

- Only `block_hash` may drive logical correctness and restore identity semantics.
- `compressed_hash` and `physical_hash` MUST NOT drive dedup identity.
- `compressed_hash` and `physical_hash` MUST NOT drive GC identity/liveness semantics.
- `compressed_hash` and `physical_hash` MUST NOT drive snapshot identity.
- `compressed_hash` and `physical_hash` MUST NOT drive restore graph semantics.

**FROZEN INVARIANT:** Integrity-layer hashes validate bytes at their stage but do
not redefine logical identity.

---

## 3. Metadata Meanings

### 3.1 Storage Block (storage_blocks table)

**Core Identity:**
- `id` (PK): Unique storage block identifier
- `block_hash` (REQUIRED): SHA256 of logical block contents (before compression/encryption)

**Transform State (v1.9 semantics):**
- `codec` (none | aes-gcm): Persisted encryption transform state for each storage block
- `compression_codec` (none | zstd): Compression applied during write
- `compression_level` (INTEGER | NULL): Compression level used when `compression_codec=zstd`; NULL for `compression_codec=none`
- **Frozen validity contract:** `compression_codec=none` => `compression_level IS NULL`; `compression_codec=zstd` => `compression_level` in `[1,9]`
- `compressed_size` (bytes): Size after compression stage (for v1.9+ writes, always populated; equals `plaintext_size` when compression_codec=none; legacy rows may be NULL)
- `compression_ratio` (REAL | NULL): Persisted per-block compression size ratio, defined as `compressed_size / plaintext_size`
   - `1.0` means no size change at compression stage
   - `< 1.0` means compression reduced payload size
   - Distinct from user-facing `CompressionFactor = LogicalBytes / CompressedBytes`
- `stored_size` (bytes): Final on-disk bytes in container (including encryption overhead)
- `plaintext_size` (bytes): Encoded logical block payload size (pre-compression, pre-encryption)

**Hash State:**
- `logical_hash` = `block_hash`: Content hash before any transforms
- `compressed_hash` (SHA256 | NULL): Compression-stage integrity (legacy blocks may be NULL; v1.9+ writes set this even when compression_codec=none)
- `physical_hash` (SHA256 | NULL): On-disk state after all transforms (v1.9+ writes set this for both none and aes-gcm; legacy rows may be NULL)
- `payload_hash` (TEXT | NULL): **DEPRECATED** lowercase-hex mirror of `block_hash` for compatibility/observability only; never authoritative for identity

**Container Location:**
- `container_id`: Foreign key to `container` table
- `container_offset`: Byte offset in container file
- Chunk placement mapping is stored in `chunk_block_refs` rows keyed by `block_id`

**Semantics (FROZEN):**
- A block is an immutable collection of chunk payloads encoded, optionally compressed, optionally encrypted
- Transform stages match write-path ordering (Section 1.1)
- Hash verification follows read-path ordering (Section 1.2)
- **Metadata reflects reality at write time:** for v1.9+ writes with compression_codec=none, compressed_hash equals logical_hash and compressed_size equals plaintext_size; legacy blocks may keep NULL metadata fields.
- **Metadata is schema-validated:** rows violating the `compression_codec`/`compression_level` contract are invalid and rejected by migration/schema checks.

### 3.2 Chunk (chunk table)

**Identity:**
- `id` (PK): Unique chunk identifier
- `chunk_hash` (SHA256): Content-based dedup key (plaintext chunk before encoding)

**Status:**
- `status` (PROCESSING | COMPLETED | ABORTED): Lifecycle state
- `live_ref_count`: Files currently referencing this chunk (file_chunk rows)
- `pin_count`: Restore operations holding chunk in memory (temporary pins)

**Containment:**
- One `chunk_block_refs` row references this chunk via `chunk_id`
- `chunk_block_refs.block_id -> storage_blocks.id` determines container placement and in-block slice metadata

**Semantics (FROZEN):**
- Chunks are immutable content fragments identified by plaintext hash
- A chunk is COMPLETED only after all its blocks are persisted and verified
- GC deletion requires both `live_ref_count = 0` AND `pin_count = 0`
- Restore increments `pin_count` during operation, decrements after close

### 3.3 Chunk-Block Mapping

**Structure:**
- `chunk_block_refs` table row (one row per chunk):
   - `chunk_id` (PK, FK -> chunk.id)
   - `block_id` (FK -> storage_blocks.id)
   - `offset_in_block` (byte offset in decoded logical block payload)
   - `size_in_block` (chunk payload length)

**Semantics (FROZEN):**
- One block can contain multiple chunks (packing optimization)
- Offset/size pairs must be contiguous (no gaps or overlaps)
- Restore extracts chunks in order, producing original chunk sequence
- Offsets are logical (into decoded block payload), not physical (disk positions)

### 3.4 Container Metadata

**Fields:**
- `container_id` (PK)
- `current_size` (bytes): Logical bytes persisted to disk
- `max_size` (bytes): Hard limit for container rotation
- `sealed` (BOOLEAN): Immutable after sealed
- `sealing` (BOOLEAN): In-progress sealing (recovery marker)
- `quarantined` (BOOLEAN): Failed/corrupted container excluded from reuse

**Semantics (FROZEN):**
- `sealed=TRUE` && `sealing=FALSE`: Immutable, no new blocks accepted
- `sealed=FALSE` && `sealing=FALSE`: Open, accepting new blocks
- `sealed=FALSE` && `sealing=TRUE`: Sealing in progress (recovery cleanup flag)
- `quarantined=TRUE`: Even if not sealed, excluded from future writes
- `current_size` reflects actual disk position after last fsync
- Non-sealed containers are selected for write based on `max_size` available space

---

## 4. Compression Semantics

### 4.0 Canonical Compression Contract (FROZEN)

- **Scope:** Compression is block-level only.
- **Never:** Chunk-level or file-level compression authority.
- **Timing:** Compression always occurs before encryption.
- **Policy:** Store-if-smaller is canonical.
- **Read control:** Per-block metadata controls decompression behavior.

**FROZEN INVARIANT:** These rules are part of repository compatibility and
future engine semantics.

### 4.1 Compression Configuration Model

**Storage (repository_config table):**
- Key `compression`: Value = `"none"` (default) | `"zstd"`
- Key `compression_level`: Value = integer in [1, 9] (ignored if compression=none; default 3)
- Compression library capability: zstd levels [1, 22] are supported by the low-level compressor API; repository_config intentionally constrains public defaults to [1, 9] in v1.9.

**Lifecycle:**
1. **Repository init:** Writes compression=none, level=3 by default
2. **Schema validation (v1.9 startup):** `ValidateRepositoryCompressionConfig(tx)` enforces codec in {`none`,`zstd`} and level in bounds
3. **Write-path read:** `buildStoreFileRuntime` loads compression from DB
4. **Block encoding:** `applyPackedBlockTransforms` applies compression if config ≠ none
5. **Read/Verify:** Each block's `compression_codec` field determines decompression; config is read-only reference

**FROZEN INVARIANT:** Compression config never influences block reads. Read path trusts per-block metadata.

### 4.2 Compression Guarantees

```
IF repository default compression = "zstd", level = 3:
   - New blocks written with compression_codec = "zstd"
   - compressed_size < plaintext_size when stored as zstd
   - compressed_hash computed and stored

IF compression attempt would expand or not improve payload size:
   - Block is stored with compression_codec = "none"
   - compressed_size = plaintext_size
   - Store-if-smaller fallback is required behavior
   
IF compression config changes from "zstd" to "none":
   - Existing blocks keep their compression_codec (immutable)
   - New blocks written with compression_codec = "none"
   - Repository may have mixed compressed/uncompressed blocks
   
IF restore encounters mixed blocks:
   - Each block's compression_codec is independent
   - Decompress only if compression_codec = "zstd"
   - Decompressed output is identical to original plaintext
```

**FROZEN INVARIANT:** Compression is per-block, never per-file or per-repository-retroactively.

### 4.3 Compression Semantics Impact

- **Dedup:** Unaffected. Chunk identity is based on plaintext hash (before compression).
- **Restore:** Identical plaintext output regardless of compression state.
- **Verify:** All compression modes pass standard/deep verification equally.
- **Mixed Repos:** Compression mode changes do not require rewriting old blocks.

---

## 5. Mixed Repository Guarantees

### 5.0 First-Class Mixed Repository Contract (FROZEN)

Mixed repositories are normal, expected behavior.

A single repository may legitimately contain, at the same time:

| Block | Compression | Encryption | Packing |
| --- | --- | --- | --- |
| A | `none` | `none` | `legacy-single` |
| B | `none` | `aes-gcm` | `packed-multi` |
| C | `zstd` | `aes-gcm` | `packed-multi` |

Interpretation rules:

- Per-block metadata is authoritative for reads and verify.
- Repository defaults are non-authoritative for reads.
- Homogeneous-repository assumptions are invalid by contract.

**FROZEN INVARIANT:** Mixed-mode coexistence is a supported steady-state, not a
transition error.

### 5.1 Mixed Codec State

A single repository can contain:
- **Unencrypted:** `codec=none` blocks
- **AES-GCM:** `codec=aes-gcm` blocks (encrypted with COLDKEEP_KEY)

**Read-Path Guarantee (FROZEN):**
```
FOR each block in repository:
   IF codec = "aes-gcm":
      require COLDKEEP_KEY set and valid (non-empty, hex, 32 bytes)
      decrypt using key + stored nonce
   IF codec = "none":
      use plaintext directly (no decryption)
   
Read operation succeeds IFF:
   - All required codec keys are available
   - Block metadata codecs are supported by the v1.9 contract:
     storage codec in {`none`,`aes-gcm`}, compression codec in {`none`,`zstd`}
```

**Migration Guarantee (FROZEN):**
```
Repository codec cannot be retroactively changed for existing blocks.
New writes use current repository default codec (set at write time).
Reads/verify support only v1.9 contract codecs:
- storage codec: `none` | `aes-gcm`
- compression codec: `none` | `zstd`
Unknown/unsupported persisted metadata is rejected explicitly by
schema/migration validation and by read/verify paths; no fallback/default
interpretation is allowed.

Implication: Codec mismatch (none blocks in aes-gcm-requiring environment)
must be caught at verify/restore time with clear error.
```

### 5.2 Mixed Compression State

A single repository can contain:
- **Uncompressed:** `compression_codec=none` blocks (or NULL for legacy)
- **Zstd:** `compression_codec=zstd` blocks

**Read-Path Guarantee (FROZEN):**
```
FOR each block in repository:
   IF compression_codec = "zstd":
      decompress to logical_payload
      verify logical_hash(logical_payload) == stored_hash
   IF compression_codec = "none" (or NULL):
      use payload directly (no decompression)
   
Read operation succeeds regardless of compression_codec mix.
```

**Statistics Guarantee (FROZEN):**
```
Mixed-repo stats (BlockStats):
   - LogicalBytes = SUM(plaintext_size) across ALL blocks (independent of compression)
   - CompressedBytes = SUM(compressed_size) for zstd blocks + SUM(plaintext_size) for none blocks
   - StoredBytes = SUM(stored_size) across ALL blocks
   - CompressionFactor = LogicalBytes / CompressedBytes
   - PhysicalSizeRatio = StoredBytes / LogicalBytes
   - PhysicalFactor = LogicalBytes / StoredBytes
   
Stats reflect current repository state, not configuration defaults.
```

---

## 6. Read-Path Guarantees

### 6.1 Restore Guarantees

**Atomicity (FROZEN):**
```
FOR each file_id to restore:
   1. Acquire file pin (increment chunk.pin_count) in TX
   2. Load all file_chunks in order
   3. FOR each chunk:
      Load blocks referencing this chunk
      Extract chunk payload from block(s)
      Verify using read-path pipeline (Section 1.2)
      Write to output file
   4. Verify final file_hash matches logical_file.hash
   5. Release pin (decrement chunk.pin_count)
   
Success: Output file exists, byte-identical to stored file
Failure: Output file not created OR contains partial data; pin cleaned up
```

**Determinism (FROZEN):**
```
Same file_id + same repository state → identical restore output
Proof: Restore uses stored chunk order (chunk_order), decode offset map,
        and stored chunk_hashes. Deterministic inputs → deterministic output.
```

**Crash Safety (FROZEN):**
```
If restore crashes mid-operation:
   - Output file either fully complete OR doesn't exist (atomic close/cleanup)
   - Pin count cleaned up on process exit
   - No partial/corrupt files left on disk
```

### 6.2 Verify Guarantees

**Standard Verify (FROZEN):**
```
FOR each block in repository:
   1. Load container; read persisted bytes
   2. Verify physical_hash IF present
   3. Decrypt IF codec=aes-gcm
   4. Verify compressed_hash IF present (expected for v1.9+ writes, including compression_codec=none)
   5. Decompress IF compression_codec=zstd
   6. Verify logical_hash (SHA256(logical_bytes) == block_hash)
   7. Decode logical block structure
   8. Validate chunk_block_refs references
   9. Check all referenced chunks exist and are COMPLETED
   
Success: Block is readable and chunks are valid
Failure: Report error with storage_blocks.id, container_id, failure reason
```

**Deep Verify (FROZEN):**
```
All of Standard Verify, PLUS:
   1. FOR each block:
      a. Load actual container bytes (not cached)
      b. Verify block structure layout (offsets contiguous)
      c. Recompute all hashes from persisted bytes
      d. Compare with stored metadata
   2. FOR each chunk:
      a. Reconstruct from blocks
      b. Recompute chunk_hash
      c. Compare with stored chunk_hash
   3. Report aggregate error at end (all failures collected)
```

**Performance Guarantee (FROZEN):**
```
Standard Verify: Fast (metadata only, no large file I/O)
Deep Verify: Slow (full payload read + hash recomputation)
Verify skips containers that are quarantined (corrupt/missing)
```

### 6.3 Read-Path Negotiation Semantics (Step 7.8, FROZEN)

Read/verify negotiation is capability-aware and per-block metadata-driven.

Negotiation algorithm:

```
FOR each block:
   1. Read persisted block metadata fields (codec, compression_codec, hash columns).
   2. Resolve transform handlers for v1.9-supported codecs only:
      storage codec {none,aes-gcm}, compression codec {none,zstd}.
   3. Execute strict inverse transform pipeline for that block only.
   4. Validate integrity at each stage using that block's metadata.

Repository defaults are never consulted for this negotiation.
```

Contract boundaries:

- `repository_config` values are write-policy only for future blocks.
- `storage_blocks` metadata is authoritative for historical and current reads.
- Mixed repositories are expected steady-state and must verify/restore safely.
- Unsupported or unknown per-block codec/compression values fail explicitly at
  the relevant stage; they do not trigger default-based fallback.

**FROZEN INVARIANT:** Negotiation is resolved block-by-block from persisted
metadata plus runtime transform capability registration, never from repository
defaults or homogeneous repository assumptions.

---

## 7. Repository Defaults vs Block Reality

### 7.1 The Separation Contract (FROZEN)

**Repository Configuration (READ AT WRITE TIME):**
- `compression`: Policy for newly written blocks
- `compression_level`: Policy for newly written blocks (if zstd)
- Write-path encryption default is resolved from runtime `COLDKEEP_CODEC` (default `aes-gcm`)
- Capability-model baseline encryption (`repository_encryption_baseline`) is `none` for compatibility reporting and is not a write-policy source
- Capability-model fields `DefaultCompression` / `DefaultCompressionLevel` are reporting fields in repository capabilities, not repository_config DB keys
- `default_packing`: Policy for newly written blocks (v1.9 packed-multi by default)

**Block Metadata (IMMUTABLE):**
- `compression_codec` field: Actual compression applied (set at write time)
- `codec` field: Actual encryption applied (set at write time)
- Hash fields: Reflect actual transforms applied
- Packing/placement metadata (`storage_blocks` + `chunk_block_refs`): Actual block layout applied

**Read/Verify Logic (TRUSTS BLOCK METADATA):**
```
IGNORE repository configuration during read/verify.
USE block.codec to determine if decryption needed.
USE block.compression_codec to determine if decompression needed.
USE block hash fields to validate transformations.
USE persisted block mapping/layout metadata to reconstruct payload.

Reason: Repository config may have changed since block was written.
Block metadata is the source-of-truth for what was actually done.
```

**Example Scenario (FROZEN behavior):**
1. Time T1: Repository has compression=zstd. Store File A.
   - File A blocks written with compression_codec=zstd
2. Time T2: Admin changes repository to compression=none.
3. Time T3: Restore File A.
   - Read path sees File A blocks have compression_codec=zstd
   - Read path decompresses regardless of current config
   - Output is identical to File A original
4. Time T4: Store File B.
   - File B blocks written with compression_codec=none (current config)
   - File A blocks remain unchanged

**FROZEN INVARIANT:** Database block metadata is single source-of-truth. Configuration is guidance-only.

**RETROCOMPATIBILITY GUARANTEE (FROZEN):**
- Repository defaults MUST NEVER override historical block interpretation.
- Old blocks are always interpreted using their persisted metadata, even after default changes.

### 7.2 Implications for Engine Extraction

```
Engine API Contract:
   storage.VerifyStoredBlock(block, container_reader)
      → Does NOT access repository_config table
      → Uses only block.codec, block.compression_codec, hash fields
      → Deterministic: same block+container → same result always

   storage.RestoreChunk(chunk_id, blocks, container_reader)
      → Does NOT access repository_config table
      → Uses per-block metadata to route through transforms
      → Deterministic: same chunks+blocks → same output always

   maintenance.CollectBlockStats(repository)
      → Reads all blocks
      → Uses block.compression_codec to categorize (not config)
      → Sums actual stored_size/plaintext_size/compressed_size
      → Configuration-independent

Future v2.0: Repository config immutability, versioned schema, etc.
             will not break these contracts — only strengthen them.
```

---

## 8. Validation Checklist (Step 6.14)

### 8.1 No Ambiguous Semantics

- ✅ Transform ordering defined point-by-point (Section 1.1)
- ✅ Read-path verification stages listed (Section 1.2)
- ✅ Four hash types with nullable semantics (Section 2.1)
- ✅ Hash computation rules frozen (Section 2.2)
- ✅ Hash equality invariants documented (Section 2.3)
- ✅ Metadata field meanings stated (Section 3)
- ✅ Mixed codec/compression behavior explicit (Sections 5.1, 5.2)
- ✅ Read-path atomicity/determinism frozen (Section 6.1, 6.2)
- ✅ Read-path negotiation semantics frozen (Section 6.3)
- ✅ Config vs metadata separation locked (Section 7.1)

### 8.2 Engine Extraction Ready

```
Engine APIs can assume:
   1. Block metadata is immutable after write
   2. Transform evaluation is config-independent
   3. Read path is deterministic
   4. Hashes are verification sources-of-truth
   5. Chunk content is chunk_hash-identified
   6. No container rewrites or re-encoding required

v2.0 Planning:
   - Versioned block format (backwards compatible read)
   - Immutable repository config option
   - Explicit transform mode version field
   - Multi-codec registration system
```

### 8.3 Repository Behavior Fully Documented

- ✅ Creation: Defaults set (compression=none, codec implied by env)
- ✅ Write: Config read, block metadata set, stored
- ✅ Read: Block metadata trusted, config ignored
- ✅ Migration: Per-block state preserved, new blocks use current config
- ✅ Mixed: Multiple codecs/compressions supported in single repo
- ✅ Defaults Change: Affects new blocks only; old blocks immutable

---

## 9. References

- [Transform Pipeline](../docs/storage_transform_semantics.md): Detailed transform stage definitions
- [Block Abstraction](../docs/BLOCK_ABSTRACTION_V18.md): Block structure and encoding
- [Verify Pipeline](../internal/verify/verify_block_pipeline.go): Implementation (verify.VerifyStoredBlock)
- [Restore Implementation](../internal/storage/restore.go): Restore path using per-block metadata
- [Storage Config](../internal/storage/repository_config.go): Config read/write
- [Stats Collection](../internal/maintenance/stats.go): BlockStats computation (config-independent)
- [v1.9 Validation](../VALIDATION_MATRIX.md): Test evidence for all guarantees

---

## 10. Frozen Lock Statement

This document is the **FROZEN v1.9 storage semantics contract**. All guarantees in Sections 1–7 are immutable until v2.0 planning.

Any future changes (new transform stage, additional hash type, config immutability, etc.) require:
1. v2.0 major version bump
2. Engine extraction review
3. Full integrated test coverage
4. Operator documentation update

**Lock Date:** 2026-05-09  
**Approved For:** v1.9 release and forward engine extraction
