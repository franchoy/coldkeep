package storage

import (
	"github.com/franchoy/coldkeep/internal/chunk"
)

// StoreService owns store-path dependencies that must be resolved once per
// store operation. Phase 3 starts by making the active chunker explicit.
type StoreService struct {
	repo    *Repository
	chunker chunk.Chunker
}

// ActiveChunkerResolution is the per-operation resolved chunker decision.
// It is resolved once and then reused for chunking and metadata persistence.
type ActiveChunkerResolution struct {
	Chunker chunk.Chunker
	Version chunk.Version
}

// NewStoreService builds a store service.
//
// If active is non-nil, it is treated as an explicit per-operation override.
// If active is nil, ResolveActiveChunker reads repository defaults once for the
// current operation.
func NewStoreService(repo *Repository, active chunk.Chunker) *StoreService {
	return &StoreService{repo: repo, chunker: active}
}

func (s *StoreService) Repository() *Repository {
	if s == nil {
		return nil
	}
	return s.repo
}

// ActiveChunker returns the explicitly injected chunker override, if any.
func (s *StoreService) ActiveChunker() chunk.Chunker {
	return s.chunker
}

// ResolveActiveChunker resolves and snapshots the active chunker decision for one
// store operation. Callers should invoke this once at operation start and reuse
// the returned Chunker and Version for the rest of the flow.
func (s *StoreService) ResolveActiveChunker() (ActiveChunkerResolution, error) {
	if active := s.ActiveChunker(); active != nil {
		return ActiveChunkerResolution{Chunker: active, Version: active.Version()}, nil
	}

	version, err := s.Repository().GetDefaultChunkerVersion()
	if err != nil {
		return ActiveChunkerResolution{}, err
	}
	// MustGet keeps the runtime contract strict once version metadata is accepted.
	active := chunk.DefaultRegistry().MustGet(version)

	return ActiveChunkerResolution{
		Chunker: active,
		Version: active.Version(),
	}, nil
}
