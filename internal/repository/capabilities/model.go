package capabilities

import "strings"

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
	// RepositoryEncryptionBaseline is a repository-compat baseline for
	// capability reporting only. It is not the write-path default, which is
	// resolved at runtime (for example via COLDKEEP_CODEC).
	RepositoryEncryptionBaseline string `json:"repository_encryption_baseline"`
	DefaultPacking               string `json:"default_packing"`

	// Read semantics are intentionally metadata-driven forever: repository
	// defaults are write policy for future blocks only.
	ReadPathMetadataDriven bool `json:"read_path_metadata_driven"`
}

// DefaultRepositoryCapabilities returns a safe internal baseline used when a
// repository connection is unavailable or introspection fails.
func DefaultRepositoryCapabilities() RepositoryCapabilities {
	return RepositoryCapabilities{
		DefaultCompression:           CompressionNone,
		DefaultCompressionLevel:      3,
		RepositoryEncryptionBaseline: EncryptionNone,
		DefaultPacking:               PackingPackedMulti,
		ReadPathMetadataDriven:       true,
	}
}

// SupportsCompression reports whether a compression codec is supported by the
// current repository layout.
func (c RepositoryCapabilities) SupportsCompression(codec string) bool {
	return containsCodec(c.SupportedCompression, normalizeCompressionCodec(codec))
}

// SupportsEncryption reports whether an encryption codec is supported by the
// current repository layout.
func (c RepositoryCapabilities) SupportsEncryption(codec string) bool {
	return containsCodec(c.SupportedEncryption, normalizeEncryptionCodec(codec))
}

// SupportsPacking reports whether a packing mode is supported by the current
// repository layout.
func (c RepositoryCapabilities) SupportsPacking(packing string) bool {
	normalized := strings.TrimSpace(strings.ToLower(packing))
	return containsCodec(c.SupportedPacking, normalized)
}

// SupportsHashLayer reports whether the repository supports an integrity layer.
// Logical hash is always required and therefore always supported.
func (c RepositoryCapabilities) SupportsHashLayer(layer string) bool {
	switch strings.TrimSpace(strings.ToLower(layer)) {
	case "logical", "block", "payload":
		return true
	case "compressed", "compressed-payload", "compressed_payload":
		return c.SupportsCompressedHash
	case "physical", "stored", "physical-payload", "physical_payload":
		return c.SupportsPhysicalHash
	default:
		return false
	}
}

func containsCodec(values []string, target string) bool {
	if target == "" {
		return false
	}
	for _, value := range values {
		if strings.TrimSpace(strings.ToLower(value)) == target {
			return true
		}
	}
	return false
}

const (
	CompressionNone = "none"
	CompressionZstd = "zstd"

	EncryptionNone   = "none"
	EncryptionAESGCM = "aes-gcm"

	PackingLegacySingle = "legacy-single"
	PackingPackedMulti  = "packed-multi"
)
