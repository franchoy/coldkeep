package storage

import "github.com/franchoy/coldkeep/internal/chunk"

// StoreService owns store-path dependencies that must be resolved once per
// store operation. Phase 3 starts by making the active chunker explicit.
type StoreService struct {
	chunker chunk.Chunker
}

// ActiveChunkerResolution is the per-operation resolved chunker decision.
// It is resolved once and then reused for chunking and metadata persistence.
type ActiveChunkerResolution struct {
	Chunker chunk.Chunker
	Version chunk.Version
}

// NewStoreService builds a store service with an explicit active chunker.
// If active is nil, it resolves the current runtime default from the registry.
// Phase 3 intentionally keeps this internal-only (Option A): no user-facing
// chunker selection is introduced here.
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

// ResolveActiveChunker resolves and snapshots the active chunker decision for one
// store operation. Callers should invoke this once at operation start and reuse
// the returned Chunker and Version for the rest of the flow.
func (s StoreService) ResolveActiveChunker() ActiveChunkerResolution {
	active := s.ActiveChunker()
	return ActiveChunkerResolution{
		Chunker: active,
		Version: active.Version(),
	}
}
