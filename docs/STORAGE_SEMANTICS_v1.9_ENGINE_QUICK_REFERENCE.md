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
    → [IF compression enabled] compress(zstd) → compute compressed_hash
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
| logical_hash | `blocks.block_hash` | Encoded block (pre-transform) | ✗ NEVER | Verify logical content |
| compressed_hash | `blocks.compressed_hash` | After zstd (if applied) | ✓ if codec=none | Verify compression layer |
| physical_hash | `blocks.physical_hash` | After encryption (if applied) | ✓ if plain/legacy | Verify on-disk bytes |
| chunk_hash | `chunks.chunk_hash` | Plaintext chunk (dedup key) | ✗ NEVER | Dedup identity |

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
    Codec          string         // "plain" | "aes-gcm"
    CompressionCodec string       // "none" | "zstd"
    CompressedSize int64          // (null if codec=none)
    StoredSize     int64          // On-disk bytes
    PlaintextSize  int64          // Original chunk payload
    
    // Hashes (IMMUTABLE after write)
    LogicalHash    []byte         // Encoded block (required)
    CompressedHash []byte         // After compression (null if codec=none)
    PhysicalHash   []byte         // After encryption (null if plain/legacy)
    
    // Location
    ContainerID    int64
    BlockOffset    int64
    
    // Chunk mapping (JSON)
    ChunkBlockMap  []ChunkRef     // [{chunk_id, offset_in_block, size_in_block}]
}
```

**Engine Rule:** Read `codec` + `compression_codec` fields; ignore repository config.

---

### D. Repository Config is NOT Engine Authority

**Repository Defaults (write policy only):**
```
repository_config table:
  - default_compression: "none" (default) | "zstd"
  - default_compression_level: int [1..9]
  - default_encryption: "none" by default (write path may opt into aes-gcm)
  - default_packing: "packed-multi" in v1.9
```

**Read/Verify Path:** IGNORES repository config
```
Read: Load block → Check block.codec (not config)
       Decompress IF block.compression_codec="zstd" (not config)
       Decrypt IF block.codec="aes-gcm" (not config)
```

**Engine Implication:** Defaults govern future writes only. Block metadata is source-of-truth forever.

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

**No Randomness:** Nonce is stored in DB; same block always uses same nonce.

**Engine Implication:** Engine restore is deterministic; no randomization.

---

### F. Mixed Repository Behavior (FROZEN)

**One repository can contain:**
- Plain (`codec=plain`) + AES-GCM (`codec=aes-gcm`) blocks simultaneously
- Uncompressed (`compression_codec=none`) + Zstd (`compression_codec=zstd`) blocks simultaneously

**Read Path:** Per-block field determines action. Configuration is ignored.

**Stats:** Computed from block metadata; config-independent:
```
LogicalBytes = SUM(plaintext_size)  // All blocks
CompressedBytes = SUM(compressed_size if compression_codec=zstd else plaintext_size)
StoredBytes = SUM(stored_size)  // All blocks
CompressionRatio = LogicalBytes / CompressedBytes  // Always ≥ 1.0
PhysicalRatio = LogicalBytes / StoredBytes  // Always ≤ 1.0
```

**Engine Implication:** Must handle mixed codecs in single restore/verify operation.

---

### G. Verify Guarantees (FROZEN STAGES)

```
Standard Verify:
  1. Load block metadata
  2. Open container; read bytes at block_offset...block_offset+stored_size
  3. IF physical_hash present: SHA256(bytes+nonce) must match
  4. IF codec=aes-gcm: Decrypt (auth failure = corruption)
  5. IF compressed_hash present: SHA256(compressed_bytes) must match
  6. IF compression_codec=zstd: Decompress
  7. SHA256(logical_bytes) must == logical_hash
  8. Decode packed block structure (magic, version, chunk_count, offsets)
  9. Validate chunk_block_map references (contiguous, all chunks exist)
```

**Engine Implication:** All stages are required. Missing hashes are skipped gracefully (legacy). No stages can be reordered.

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

**Q: Must I support both plain and aes-gcm in one repo?**  
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
