package chunk

import "github.com/franchoy/coldkeep/internal/chunk/shared"

// Version is the chunker version identifier type used across the abstraction
// layer and persisted metadata.
type Version = shared.Version

const (
	// VersionV1SimpleRolling is the canonical persisted value for repository data
	// produced by the historical v1 simple rolling chunker.
	VersionV1SimpleRolling = shared.VersionV1SimpleRolling
	DefaultChunkerVersion  = VersionV1SimpleRolling
)
