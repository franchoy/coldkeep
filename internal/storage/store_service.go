package storage

import "github.com/franchoy/coldkeep/internal/chunk"

// StoreService owns store-path dependencies that must be resolved once per
// store operation. Phase 3 starts by making the active chunker explicit.
type StoreService struct {
	chunker chunk.Chunker
}

// NewStoreService builds a store service with an explicit active chunker.
// If active is nil, it resolves the current runtime default from the registry.
func NewStoreService(active chunk.Chunker) StoreService {
	if active == nil {
		active = chunk.DefaultRegistry().Default()
	}
	return StoreService{chunker: active}
}

// ActiveChunker returns the chunker selected for this store operation.
func (s StoreService) ActiveChunker() chunk.Chunker {
	return s.chunker
}

// ActiveChunkerVersion returns the persisted version marker for this store operation.
func (s StoreService) ActiveChunkerVersion() chunk.Version {
	return s.chunker.Version()
}
