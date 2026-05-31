package engine

import "context"

// Config holds configuration for a DefaultEngine.
//
// Database backend selection (SQLite vs PostgreSQL) is not decided in Phase 1.
// Config fields will expand in Phase 2 as wrapper-only implementations require
// additional dependencies.
type Config struct {
	RepositoryRoot string
}

// DefaultEngine is the canonical Engine implementation.
//
// Phase 1: skeleton only. All methods return ErrNotImplemented.
// Wrapper-only implementation begins in Phase 2.
type DefaultEngine struct {
	config Config
}

// New returns a new DefaultEngine with the given configuration.
func New(cfg Config) *DefaultEngine {
	return &DefaultEngine{config: cfg}
}

func (e *DefaultEngine) Stats(ctx context.Context, req StatsRequest) (StatsResult, error) {
	return StatsResult{}, ErrNotImplemented
}

func (e *DefaultEngine) Inspect(ctx context.Context, req InspectRequest) (InspectResult, error) {
	return InspectResult{}, ErrNotImplemented
}

func (e *DefaultEngine) Verify(ctx context.Context, req VerifyRequest) (VerifyResult, error) {
	return VerifyResult{}, ErrNotImplemented
}
