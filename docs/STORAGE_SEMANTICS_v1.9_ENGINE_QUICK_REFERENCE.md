# Storage Semantics v1.9 — Engine Extraction Quick Reference

**Status:** Quick reference for engine APIs  
**Date:** 2026-05-09  
**Full Contract:** See `docs/STORAGE_SEMANTICS_v1.9.md` for comprehensive frozen semantics

---

## 1-Page Contracts

### A. Transform Pipeline (IMMUTABLE)

**Write Path (deterministic, stage order frozen):**
```
plaintext → encode(block+metadata) 
    → compute logical_hash (SHA256)
  → [IF compression_codec=zstd] compress(zstd) → compute compressed_hash
  → [IF compression_codec=none] skip compression; set compressed_hash=logical_hash
    → [IF AES-GCM enabled] encrypt(key+nonce) → compute physical_hash
    → persist_to_container
    → update_block_metadata(hashes, sizes, codec, compression_codec)
```

**Read Path (verify in reverse, all stages required):**
```
container_bytes 
    → verify physical_hash (if present)
    → [IF AES-GCM] decrypt
    → verify compressed_hash (if present)
    → [IF compression_codec=zstd] decompress
    → verify logical_hash
    → decode_block_structure
    → verify_chunk_references
    → output_chunk_payloads
```

**Engine Implication:** No reordering permitted. New stages require v2.0.

---

### B. Four Hash Types (FROZEN)

| Hash | Field | Scope | Nullable | Used By |
|------|-------|-------|----------|---------|
| logical_hash | `storage_blocks.block_hash` | Encoded block (pre-transform) | ✗ NEVER | Verify logical content |
| compressed_hash | `storage_blocks.compressed_hash` | After compression stage (zstd bytes or logical bytes for compression_codec=none) | ✓ legacy only | Verify compression layer |
| physical_hash | `storage_blocks.physical_hash` | Persisted bytes (nonce||ciphertext for aes-gcm; compressed payload bytes for codec=none) | ✓ legacy only | Verify on-disk bytes |
| chunk_hash | `chunk.chunk_hash` | Plaintext chunk (dedup key) | ✗ NEVER | Dedup identity |

**Engine Rule:** Trust per-block metadata. Verify skips missing hashes (legacy support).

### B1. Canonical Hash Role Authority (FROZEN)

- `block_hash`: canonical logical block identity.
- `compressed_hash`: integrity checkpoint for compressed/pre-encryption payload.
- `physical_hash`: integrity checkpoint for persisted payload bytes.

Never use `compressed_hash` or `physical_hash` as identity keys for:

- dedup
- GC identity/liveness
- snapshot identity
- restore graph construction

---

### C. Block Metadata (SOURCE OF TRUTH AT READ TIME)

```go
type StoredBlock struct {
    // Identity
    BlockID        int64          // Primary key
    BlockHash      []byte         // logical_hash (required)
    
    // Transform state (IMMUTABLE after write)
    Codec          string         // "none" | "aes-gcm"
    CompressionCodec string       // "none" | "zstd"
    CompressionLevel *int         // MUST be nil when compression_codec="none"; MUST be [1..9] when "zstd"
    CompressedSize int64          // Bytes after compression stage (v1.9+ writes always populate; equals PlaintextSize if compression_codec=none; legacy rows may be null)
    CompressionRatio float64      // compressed_size / plaintext_size (persisted per-block size ratio)
                    // Distinct from user-facing CompressionFactor (logical / compressed)
    StoredSize     int64          // On-disk bytes
    PlaintextSize  int64          // Encoded logical block payload (pre-compression)
    
    // Hashes (IMMUTABLE after write)
    LogicalHash    []byte         // Encoded block (required)
    CompressedHash []byte         // After compression stage (legacy blocks may be null)
    PhysicalHash   []byte         // Persisted-byte hash (legacy blocks may be null)
    PayloadHash    string         // DEPRECATED mirror of block_hash (observability/compat only)
    
    // Location
    ContainerID    int64
    ContainerOffset int64
    
    // Chunk mapping (loaded from chunk_block_refs table)
    ChunkRefs      []ChunkRef      // [{chunk_id, offset_in_block, size_in_block}]
}
```

`storage_blocks.payload_hash` is a deprecated lowercase-hex mirror of
`storage_blocks.block_hash` and is not an identity authority field.

**Engine Rule:** Read `codec` + `compression_codec` fields; ignore repository config.

**Frozen DB contract:** `compression_codec='none'` requires
`compression_level IS NULL`; `compression_codec='zstd'` requires
`compression_level BETWEEN 1 AND 9`.

---

### D. Repository Config is NOT Engine Authority

**Repository Defaults (write policy only):**
```
repository_config table:
  - compression: "none" (default) | "zstd"
  - compression_level: int [1..9] (v1.9 public repository contract)
    Note: compression library supports zstd levels [1..22]; repository defaults
    are intentionally constrained to [1..9] for stable operator-facing semantics.
  - write-path encryption default: runtime `COLDKEEP_CODEC` (default: `aes-gcm`)
  - capability baseline encryption: `repository_encryption_baseline="none"` (compatibility reporting only; not a write default)
  - capability-model fields `DefaultCompression` / `DefaultCompressionLevel` are reporting fields, not repository_config keys
  - default_packing: "packed-multi" in v1.9
```

**Read/Verify Path:** IGNORES repository config
```
Read: Load block → Check block.codec (not config)
       Decompress IF block.compression_codec="zstd" (not config)
       Decrypt IF block.codec="aes-gcm" (not config)
```

**Engine Implication:** Defaults govern future writes only. Block metadata is source-of-truth forever.

### D1. Compression Contract (FROZEN)

- Scope: block-level only.
- Timing: compression before encryption.
- Policy: store-if-smaller.
- Reads: decompress strictly from each block's `compression_codec` metadata.

Never interpret compression as chunk-level or file-level identity semantics.

---

### E. Determinism Guarantee

**Same Input → Same Output ALWAYS:**
```
restore(file_id, same_repo_state)
    → chunk_order from DB is deterministic
    → chunk_hashes from DB are deterministic
    → block references from DB are deterministic
    → output file is byte-identical every time
```

**No Randomness:** For packed AES-GCM blocks, nonce is persisted as a payload prefix (`nonce || ciphertext`), not as a DB column.

**Engine Implication:** Engine restore is deterministic; no randomization.

---

### F. Mixed Repository Behavior (FROZEN)

Mixed repositories are first-class and expected.

Example valid repository state:

| Block | Compression | Encryption | Packing |
| --- | --- | --- | --- |
| A | `none` | `none` | `legacy-single` |
| B | `none` | `aes-gcm` | `packed-multi` |
| C | `zstd` | `aes-gcm` | `packed-multi` |

**One repository can contain:**
- Unencrypted (`codec=none`) + AES-GCM (`codec=aes-gcm`) blocks simultaneously
- Uncompressed (`compression_codec=none`) + Zstd (`compression_codec=zstd`) blocks simultaneously

**Read Path:** Per-block metadata determines action. Repository defaults are non-authoritative for reads.

**Stats:** Computed from block metadata; config-independent:
```
LogicalBytes = SUM(plaintext_size)  // All blocks
CompressedBytes = SUM(compressed_size if compression_codec=zstd else plaintext_size)
StoredBytes = SUM(stored_size)  // All blocks
CompressionFactor = LogicalBytes / CompressedBytes  // Always ≥ 1.0
PhysicalSizeRatio = StoredBytes / LogicalBytes
PhysicalFactor = LogicalBytes / StoredBytes
```

**Engine Implication:** Must handle mixed codecs in single restore/verify operation.

---

### G. Verify Guarantees (FROZEN STAGES)

```
Standard Verify:
  Given block metadata (`codec`, `compression_codec`, hashes, sizes, container_offset):
  1. Open container; read bytes at container_offset...container_offset+stored_size
  2. IF physical_hash present: SHA256(persisted_bytes) must match
  3. IF codec=aes-gcm: Decrypt (auth failure = corruption)
  4. IF compressed_hash present: SHA256(compressed_bytes) must match
  5. IF compression_codec=zstd: Decompress
  6. SHA256(logical_bytes) must == logical_hash
  7. Decode logical block structure (magic, version, chunk_count, offsets)
  8. Validate chunk_block_refs references (contiguous, all chunks exist)
```

**Engine Implication:** All stages are required. Missing hashes are skipped gracefully (legacy). No stages can be reordered.

**Negotiation Freeze (Step 7.8):**

- Resolve transforms per block from persisted metadata (`codec`,
  `compression_codec`, hash fields).
- Use runtime codec/compression registries for capability resolution only.
- Never consult repository defaults during read/verify negotiation.
- Unknown/unsupported per-block modes must fail explicitly; no default fallback.

---

### H. Restore Atomicity & Crash Safety

```
Restore Operation:
  1. Acquire chunk pin (TX, increment pin_count)
  2. Load file_chunks in order
  3. For each chunk:
     a. Load blocks referencing chunk
     b. Extract chunk from blocks (verify integrity)
     c. Write to output file
  4. Verify file_hash matches stored logical_file.hash
  5. Release pin (decrement pin_count)
  6. Close output file

Crash During Step 3:
  → Output file is cleaned up OR closed completely
  → Pin is released on process exit
  → No partial corruption left on disk
```

**Engine Implication:** Engine must manage pins. Output file handling must be atomic.

---

## 2. Engine API Sketch (v2.0 planning)

```go
// Engine-level APIs (config-independent)
// These APIs DO NOT access repository_config table

type Engine struct {
    // Core storage layer
}

// Verify stored block (deterministic)
func (e *Engine) VerifyStoredBlock(
    ctx context.Context,
    blockID int64,
    blockMetadata StorageBlockMetadata,
    containerReader ContainerReader,
) (VerifiedBlock, error)
// Returns: verified logical payload + decoded block struct
// Does NOT read repository_config
// Deterministic: same block → same result always

// Restore chunk (deterministic)
func (e *Engine) RestoreChunkFromBlocks(
    ctx context.Context,
    chunkID int64,
    blockIDs []int64,
    blockDataMap map[int64][]byte, // persisted block bytes
    chunkMetadata ChunkMetadata,
) ([]byte, error)
// Returns: plaintext chunk bytes
// Does NOT read repository_config
// Deterministic: same chunks+blocks → same output always

// Compute stats (config-independent)
func (e *Engine) ComputeRepositoryStats(
    ctx context.Context,
    blockMetadataList []StorageBlockMetadata,
) BlockStats
// Returns: BlockStats with logical_bytes, compressed_bytes, etc.
// Uses per-block metadata (codec, compression_codec fields)
// Does NOT read repository_config
```

---

## 3. Validation Checklist (For Engine Teams)

**Before Engine Extraction Proceeds:**

- [ ] Transform pipeline order validated (Section A)
- [ ] Hash semantics understood (Section B)
- [ ] Block metadata source-of-truth confirmed (Section C)
- [ ] Config/metadata separation acknowledged (Section D)
- [ ] Determinism guarantee tested (Section E)
- [ ] Mixed-codec handling designed (Section F)
- [ ] Verify stages all implemented (Section G)
- [ ] Restore atomicity guaranteed (Section H)
- [ ] No repository_config reads in core APIs (Section 2)

---

## 4. FAQ for Engine Teams

**Q: Can I optimize by reading repository_config instead of per-block metadata?**  
A: No. Config may have changed since block was written. Use per-block codec field.

**Q: What if a block has codec=aes-gcm but COLDKEEP_KEY is not set?**  
A: Detect at verify/restore time. Fail with clear "aes-gcm requires COLDKEEP_KEY" error.

**Q: Can I batch decompress/decrypt all blocks at once?**  
A: Not safely without per-block gating. Verify each block independently first.

**Q: What if verify fails on a hash?**  
A: Report error with block_id, stage, reason. Stop restore. Do NOT produce file.

**Q: Is compression transparent to engine?**  
A: Yes. Each block has compression_codec field. Decompress IF = "zstd", else skip.

**Q: Can I change hash algorithms?**  
A: No. SHA256 is locked for logical/compressed/physical hashes. Requires v2.0.

**Q: Must I support both none and aes-gcm in one repo?**  
A: Yes. Mixed repos are guaranteed. Per-block codec field handles routing.

---

## 5. References

- **Full Contract:** `docs/STORAGE_SEMANTICS_v1.9.md`
- **Verify Implementation:** `internal/verify/verify_block_pipeline.go`
- **Restore Implementation:** `internal/storage/restore.go`
- **Transform Implementation:** `internal/storage/store.go` (buildAndEncodePackedBlock, applyPackedBlockTransforms)
- **Stats Computation:** `internal/maintenance/stats.go`
- **Test Evidence:** `tests/adversarial/g67_deterministic_restore_matrix_test.go`, `tests/integration/v19_stats_semantics_integration_test.go`

---

**Locked for v1.9. Effective 2026-05-09. Engine extraction can proceed safely.**
