# Storage Transform Semantics

This document locks the canonical metadata semantics for the v1.9 storage
transform model.

Phase 1 is preparation only:

- compression remains disabled
- repositories remain behaviorally equivalent to v1.8
- write path payload bytes remain identical to v1.8
- restore and verify semantics remain unchanged

These definitions are intended to remain stable after v1.9 so future schema and
verification changes are additive instead of reinterpretive.

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

In current v1.8 and Phase 1 behavior, this is the output of block encoding and
the input to the transform pipeline.

### 2. Compressed Payload

Compressed payload is the output of compression applied to the logical payload,
before encryption.

Properties:

- transform-stage artifact, not a user-visible identity
- decompression integrity checkpoint
- diagnostic and verification boundary for future transform-aware audits

Compressed payload is not active in Phase 1. It is defined now so future
compression can be added without changing metadata meanings.

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

In v1.8 and Phase 1, physical payload is equal to the output of the current
transform pipeline. With AES-GCM enabled, this includes the nonce prefix and the
ciphertext written to the container.

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
- role: future decompression integrity and transform-aware diagnostics

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
- future transform-stage observability value
- useful for compression ratio and decompression integrity reporting

`compressed_size` is not active in Phase 1 but its meaning is locked now.

### `stored_size`

`stored_size` is the size in bytes of the physical payload.

Meaning:

- exact persisted byte count in the container
- exact span used for container placement validation
- includes any transform framing required by the persisted representation

In v1.8 and Phase 1, `stored_size` remains the persisted payload size already in
use by `storage_blocks`.

## Transform Ordering Contract

The transform-aware write path is defined conceptually as:

logical payload -> compress -> encrypt -> physical payload

The reverse read path is defined as:

physical payload -> decrypt -> decompress -> logical payload

Phase 1 does not enable compression. The active runtime still behaves as v1.8:

- if codec is `none`, logical payload is persisted directly
- if codec is `aes-gcm`, logical payload is encrypted and the resulting physical
  payload is `nonce || ciphertext`

No Phase 1 change is allowed to alter:

- `block_hash` computation target
- current stored payload bytes
- restore determinism
- verify behavior

## Metadata Invariants

The following semantic invariants are locked for v1.9 and later:

1. Logical identity is always defined at the logical payload layer.
2. Compression metadata, when introduced, is additive and never redefines
   logical identity.
3. Physical integrity metadata, when introduced, is additive and never redefines
   dedup identity.
4. Transform ordering must remain reversible in strict inverse order on reads.
5. Future verify stages may check logical, compressed, and physical layers
   separately, but failure at one layer must not reinterpret the meaning of the
   others.

## Phase 1 Scope Boundary

This document defines semantics only. It does not itself require:

- repository rewrites
- compression enablement
- runtime behavior changes
- immediate schema backfill

Planned follow-up schema work may add nullable metadata columns such as
`compressed_hash`, `physical_hash`, and `compressed_size`, but those later
additions must conform to the definitions in this document.
