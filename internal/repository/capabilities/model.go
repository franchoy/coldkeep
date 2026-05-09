package capabilities

// RepositoryCapabilities is the centralized internal model describing what a
// repository can represent (supported) and what it currently contains
// (observed).
//
// This is intentionally local semantic modeling. It is not network
// negotiation, plugin discovery, or runtime capability exchange.
type RepositoryCapabilities struct {
	SupportedCompression []string `json:"supported_compression"`
	SupportedEncryption  []string `json:"supported_encryption"`
	SupportedPacking     []string `json:"supported_packing"`

	ObservedCompression []string `json:"observed_compression"`
	ObservedEncryption  []string `json:"observed_encryption"`
	ObservedPacking     []string `json:"observed_packing"`

	SupportsPhysicalHash   bool `json:"supports_physical_hash"`
	SupportsCompressedHash bool `json:"supports_compressed_hash"`

	RepositoryFormatVersion int `json:"repository_format_version"`

	DefaultCompression      string `json:"default_compression"`
	DefaultCompressionLevel int    `json:"default_compression_level"`
}

const (
	CompressionNone = "none"
	CompressionZstd = "zstd"

	EncryptionNone   = "none"
	EncryptionAESGCM = "aes-gcm"

	PackingLegacySingle = "legacy-single"
	PackingPackedMulti  = "packed-multi"
)
