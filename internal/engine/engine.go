// Package engine defines the behavior-preserving facade for coldkeep operations.
//
// v1.11.0 — Behavior-Preserving Engine Facade Baseline.
//
// This package introduces the engine boundary without changing any observable
// behavior. CLI commands are not routed through the engine in Phase 1.
// Wrapper-only implementation begins in Phase 2.
//
// # Invariants
//
// Engine callers must not weaken these invariants:
//
//   - GC must never delete reachable data.
//   - Restore must never write outside the intended destination.
//   - Verify must fail closed on inconsistent catalog/storage state.
//   - Recovery must not legitimize corrupt mappings.
//   - Packed and legacy storage behavior must remain aligned.
//   - CLI parsing must not be the only place where correctness invariants live.
//   - Engine APIs must not weaken existing safety guarantees.
//
// # Dependency direction
//
//	cmd/coldkeep    may import internal/engine
//	internal/engine must not import cmd/coldkeep
//	domain packages must not import internal/engine
package engine

import (
	"context"
	"errors"
)

// ErrNotImplemented is returned by engine methods that are not yet wired to
// real implementations. Wrapper-only implementation begins in Phase 2.
var ErrNotImplemented = errors.New("engine operation not implemented")

// Engine is the behavior-preserving facade for coldkeep operations.
//
// All implementations must preserve existing CLI output, JSON output, exit
// codes, storage format, repository format, and schema behavior.
//
// Phase 1: interface contract only. Methods return ErrNotImplemented until
// Phase 2 wrapper-only implementation is complete.
type Engine interface {
	// Stats returns repository statistics.
	Stats(ctx context.Context, req StatsRequest) (StatsResult, error)

	// Inspect returns metadata about a stored file.
	Inspect(ctx context.Context, req InspectRequest) (InspectResult, error)

	// Verify runs repository verification.
	Verify(ctx context.Context, req VerifyRequest) (VerifyResult, error)
}
