// Package storage implements Coldkeep repository read/write semantics.
//
// Frozen transform ordering contract (v1.9):
//
// Write path:
//  1. logical encode
//  2. logical hash
//  3. compression
//  4. compressed hash
//  5. encryption
//  6. physical hash
//  7. persist
//
// Read path (strict inverse):
//  1. read payload
//  2. physical hash verify
//  3. decrypt
//  4. compressed hash verify
//  5. decompress
//  6. logical hash verify
//  7. decode logical block
//
// This ordering is stable repository semantics. Repository defaults govern
// future writes only; reads are driven by persisted per-block metadata.
//
// Frozen compression semantics (v1.9):
//   - Scope: block-level only.
//   - Timing: compression always runs before encryption.
//   - Policy: store-if-smaller is canonical.
//   - Reads: per-block metadata controls decompression behavior.
//
// Compression semantics are part of repository compatibility and future
// engine contracts.
//
// Frozen hash layer semantics (v1.9):
//   - block_hash: canonical logical block identity.
//   - payload_hash: deprecated lowercase-hex mirror of block_hash for
//     compatibility/observability only.
//   - compressed_hash: transform-stage integrity checkpoint.
//   - physical_hash: persisted payload integrity checkpoint.
//
// Identity authority is intentionally limited:
//   - Only block_hash participates in logical correctness and restore identity.
//   - payload_hash must not be used as a source of truth for identity.
//   - Dedup, GC identity, snapshot identity, and restore graph semantics must
//     not be derived from compressed_hash or physical_hash.
package storage
