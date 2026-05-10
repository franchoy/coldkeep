package metadata

// CompressionMetadata describes the compression stage for a persisted block.
// Level is nil when compression is disabled or when no explicit level was
// recorded.
type CompressionMetadata struct {
	Codec string
	Level *int
}

// HashMetadata centralizes the three payload-layer hashes used by transform-
// aware block metadata.
type HashMetadata struct {
	LogicalHash    []byte
	CompressedHash []byte
	PhysicalHash   []byte
}

// PayloadMetadata captures the logical, compressed, and stored sizes for a
// single block payload. v1.9+ writes always populate CompressedSize (equal to
// PlaintextSize when compression codec is none); nil is legacy-row only.
type PayloadMetadata struct {
	PlaintextSize  int64
	CompressedSize *int64
	StoredSize     int64
}

// BlockStorageMetadata is the canonical internal representation of packed-block
// transform metadata. It is intentionally internal-only.
type BlockStorageMetadata struct {
	Compression CompressionMetadata
	Hashes      HashMetadata
	Sizes       PayloadMetadata
}
