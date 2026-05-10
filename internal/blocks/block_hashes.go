package blocks

import "crypto/sha256"

// BlockHashes holds hash digests for the three semantic layers of a stored block.
//
// Layer semantics:
//
//	LogicalHash    = hash(encoded plaintext block bytes) — "what was written logically"
//	                 Identical to the legacy block_hash (v1.8 contract preserved).
//	CompressedHash = hash(pre-encryption transform output) — "what the encryptor received"
//	                 When compression is disabled, CompressedHash == LogicalHash.
//	PhysicalHash   = hash(exact persisted payload bytes) — "what lives on disk / in the container"
//	                 When no encryption is applied, PhysicalHash == CompressedHash.
//
// Phase 2 invariant (compression disabled, codec = "none"):
//
//	CompressedHash == LogicalHash
type BlockHashes struct {
	LogicalHash    []byte
	CompressedHash []byte
	PhysicalHash   []byte
}

// HashLogical returns the SHA-256 digest of the encoded plaintext block bytes.
// This is the canonical logical hash (block_hash) and is identical to the
// v1.8 block_hash contract. payload_hash is a deprecated lowercase-hex mirror
// retained for compatibility and observability only.
func HashLogical(encodedPlaintext []byte) []byte {
	sum := sha256.Sum256(encodedPlaintext)
	return sum[:]
}

// HashCompressed returns the SHA-256 digest of the pre-encryption transform
// output (compressed bytes when compression is active, or encoded plaintext
// when compression is disabled).
//
// When compression codec is "none": HashCompressed(x) == HashLogical(x).
func HashCompressed(preEncryptionPayload []byte) []byte {
	sum := sha256.Sum256(preEncryptionPayload)
	return sum[:]
}

// HashPhysical returns the SHA-256 digest of the exact persisted payload bytes
// (i.e. after all transforms including encryption).
//
// When no encryption is applied: HashPhysical(x) == HashCompressed(x).
func HashPhysical(persistedPayload []byte) []byte {
	sum := sha256.Sum256(persistedPayload)
	return sum[:]
}
