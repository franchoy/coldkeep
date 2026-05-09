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
// Frozen hash layer semantics (v1.9):
//   - block_hash: canonical logical block identity.
//   - compressed_hash: transform-stage integrity checkpoint.
//   - physical_hash: persisted payload integrity checkpoint.
//
// Identity authority is intentionally limited:
//   - Only block_hash participates in logical correctness and restore identity.
//   - Dedup, GC identity, snapshot identity, and restore graph semantics must
//     not be derived from compressed_hash or physical_hash.
package storage
