package storage

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/franchoy/coldkeep/internal/chunk"
)

const repositoryDefaultChunkerKey = "default_chunker"

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

// NewStoreService builds a store service with an explicit active chunker.
// If active is nil, it resolves the current runtime default from the registry.
// Phase 3 intentionally keeps this internal-only (Option A): no user-facing
// chunker selection is introduced here.
func NewStoreService(repo *Repository, active chunk.Chunker) *StoreService {
	if active == nil {
		active = chunk.DefaultRegistry().Default()
	}
	return &StoreService{repo: repo, chunker: active}
}

func (s *StoreService) Repository() *Repository {
	if s == nil {
		return nil
	}
	return s.repo
}

// ActiveChunker returns the chunker selected for this store operation.
func (s *StoreService) ActiveChunker() chunk.Chunker {
	return s.chunker
}

// ActiveChunkerVersion returns the persisted version marker for this store operation.
func (s *StoreService) ActiveChunkerVersion() chunk.Version {
	return s.chunker.Version()
}

// ResolveActiveChunker resolves and snapshots the active chunker decision for one
// store operation. Callers should invoke this once at operation start and reuse
// the returned Chunker and Version for the rest of the flow.
func (s *StoreService) ResolveActiveChunker() ActiveChunkerResolution {
	active := s.ActiveChunker()
	return ActiveChunkerResolution{
		Chunker: active,
		Version: active.Version(),
	}
}

// resolveConfiguredChunker returns the write-time default chunker from
// repository_config.default_chunker when available.
//
// Invariants:
// - explicit overrides always win (handled by caller)
// - this affects only new writes, never existing restore recipes
// - missing config row falls back to registry default for backward compatibility
func resolveConfiguredChunker(dbconn *sql.DB) (chunk.Chunker, error) {
	defaultChunker := chunk.DefaultRegistry().Default()
	if dbconn == nil {
		return defaultChunker, nil
	}

	var versionRaw string
	err := dbconn.QueryRow(
		`SELECT value FROM repository_config WHERE key = $1`,
		repositoryDefaultChunkerKey,
	).Scan(&versionRaw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return defaultChunker, nil
		}
		return nil, fmt.Errorf("read repository default chunker: %w", err)
	}

	configuredVersion := chunk.Version(strings.TrimSpace(versionRaw))
	if !chunk.IsWellFormedVersion(configuredVersion) {
		return nil, fmt.Errorf("repository default chunker version %q is malformed", configuredVersion)
	}

	resolved, ok := chunk.DefaultRegistry().Get(configuredVersion)
	if !ok {
		return nil, fmt.Errorf("repository default chunker version %q is not registered in this binary", configuredVersion)
	}

	return resolved, nil
}
