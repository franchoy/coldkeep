# Storage Transform Semantics

This document locks canonical metadata semantics for the compression-aware
storage transform model ahead of the v1.10 behavior freeze.

Current runtime contract:

- Compression is block-level.
- Compression happens before encryption.
- Compression settings affect newly written blocks only.
- Existing blocks are never recompressed automatically.
- Reads and verify paths resolve behavior from per-block metadata.
- Mixed repositories are valid steady-state: legacy and compression-era blocks
   can coexist.
- Dedup identity remains anchored to logical payload (`block_hash`) and does not
   change with compression/encryption settings.

## Payload Layers

Coldkeep storage metadata is defined across three distinct payload layers.

### 1. Logical Payload

Logical payload is the encoded plaintext block bytes before any transform is
applied.

Properties:

- deterministic for the same logical block content
- canonical logical identity layer
- input to logical integrity verification
- dedup and restore correctness anchor

In current behavior, this is the output of block encoding and the input to the
transform pipeline.

### 2. Compressed Payload

Compressed payload is the output of compression applied to the logical payload,
before encryption.

Properties:

- transform-stage artifact, not a user-visible identity
- decompression integrity checkpoint
- diagnostic and verification boundary for future transform-aware audits

Compression is active for new writes when configured.

Store-if-smaller contract:

- Compression is opportunistic at block write time.
- If compression would expand the payload, the block is stored uncompressed.
- As a result, repositories configured for zstd can still contain
   uncompressed blocks.

Compressed payload is not:

- logical identity
- dedup identity
- a replacement for logical payload verification

### 3. Physical Payload

Physical payload is the exact byte sequence persisted to the container file.

Properties:

- storage integrity layer
- transfer and replication integrity layer
- corruption detection boundary
- exact byte range addressed by container offset plus stored size

In current behavior, physical payload is the output of the active transform
pipeline. With AES-GCM enabled, this includes the nonce prefix and ciphertext
written to the container.

## Hash Semantics

Hash meanings are fixed by payload layer.

### `block_hash`

`block_hash` is the hash of the logical payload.

Definition:

- hash input: encoded plaintext block bytes before any transform
- meaning: logical block identity and restore integrity anchor
- role: dedup identity, correctness verification, fail-closed logical audit

`block_hash` is the only hash in this model that carries logical identity
semantics.

### `compressed_hash`

`compressed_hash` is the hash of the compressed payload.

Definition:

- hash input: compressed bytes before encryption
- meaning: transform-stage integrity checkpoint
- role: decompression integrity and transform-aware diagnostics

`compressed_hash` does not carry dedup semantics and must not be treated as a
replacement for `block_hash`.

### `physical_hash`

`physical_hash` is the hash of the physical payload.

Definition:

- hash input: exact persisted bytes written to the container
- meaning: physical storage integrity checkpoint
- role: corruption detection, transport validation, replication validation

`physical_hash` does not carry logical identity semantics.

## Size Semantics

Size fields are also fixed by payload layer.

### `plaintext_size`

`plaintext_size` is the size in bytes of the logical payload.

Meaning:

- size of the encoded plaintext block bytes
- independent of compression and encryption
- required input for logical decode and logical-layer verification

### `compressed_size`

`compressed_size` is the size in bytes of the compressed payload.

Meaning:

- size after compression, before encryption
- transform-stage observability value
- useful for compression ratio and decompression integrity reporting

`compressed_size` is nullable for blocks that are stored uncompressed.

### `stored_size`

`stored_size` is the size in bytes of the physical payload.

Meaning:

- exact persisted byte count in the container
- exact span used for container placement validation
- includes any transform framing required by the persisted representation

`stored_size` is the persisted payload size used by `storage_blocks`.

## Transform Ordering Contract

The transform-aware write path is defined conceptually as:

logical payload -> compress -> encrypt -> physical payload

The reverse read path is defined as:

physical payload -> decrypt -> decompress -> logical payload

Current runtime behavior:

- If compression codec is `none`, logical payload is forwarded unchanged to the
   encryption stage.
- If compression codec is `zstd`, logical payload is compressed first, and the
   compressed payload may be retained only when it is beneficial (store-if-smaller).
- If encryption codec is `none`, the selected pre-encryption payload is persisted
   directly.
- If encryption codec is `aes-gcm`, physical payload is `nonce || ciphertext`
   over the selected pre-encryption payload.

This transform ordering does not change dedup identity. Dedup identity remains
`block_hash` of logical payload.

## Metadata Invariants

The following semantic invariants are locked for v1.9 and later:

1. Logical identity is always defined at the logical payload layer.
2. Compression metadata, when introduced, is additive and never redefines
   logical identity.
3. Physical integrity metadata, when introduced, is additive and never redefines
   dedup identity.
4. Transform ordering must remain reversible in strict inverse order on reads.
5. Verify checks may evaluate logical, compressed, and physical layers
   separately, but failure at one layer must not reinterpret the meaning of the
   others.

## Integrity Checkpoints (Operator View)

- `logical_hash` (`block_hash`) verifies decoded logical block content.
- `compressed_hash` verifies the pre-encryption compressed payload.
- `physical_hash` verifies exact persisted bytes in container storage.
