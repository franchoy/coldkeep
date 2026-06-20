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

	// SnapshotList returns snapshots matching the request filters.
	SnapshotList(ctx context.Context, req SnapshotListRequest) (SnapshotListResult, error)

	// SnapshotShow returns metadata and filtered files for a single snapshot.
	SnapshotShow(ctx context.Context, req SnapshotShowRequest) (SnapshotShowResult, error)

	// SnapshotStats returns aggregate or per-snapshot statistics.
	SnapshotStats(ctx context.Context, req SnapshotStatsRequest) (SnapshotStatsResult, error)

	// SnapshotDiff compares two snapshots and returns change entries.
	SnapshotDiff(ctx context.Context, req SnapshotDiffRequest) (SnapshotDiffResult, error)

	// GarbageCollect runs dry-run or live GC against the repository.
	// Safety invariant: GC must never delete reachable data.
	// Live GC is only supported on the PostgreSQL backend; dry-run is supported
	// on both backends.
	GarbageCollect(ctx context.Context, req GarbageCollectRequest) (GarbageCollectResult, error)

	// Store stores a file into the repository.
	// Safety invariant: Store must not create inconsistent catalog/storage state.
	// Phase 8: single-file mode is active; folder mode remains deferred.
	Store(ctx context.Context, req StoreRequest) (StoreResult, error)

	// Remove removes logical files from the repository by logical file ID.
	// Safety invariant: Remove must never make valid data unrecoverable.
	// Active semantics are limited to by-ID remove.
	Remove(ctx context.Context, req RemoveRequest) (RemoveResult, error)

	// Restore restores logical files by logical file ID.
	// Safety invariant: Restore must never write outside the intended destination.
	// Active semantics are limited to by-ID restore.
	Restore(ctx context.Context, req RestoreRequest) (RestoreResult, error)

	// RestoreStoredPath restores one current stored physical-path mapping.
	//
	// Safety invariant: the operation must preserve logical identity,
	// physical mappings, snapshot state, and ref-count ownership. Storage may
	// temporarily pin chunks while reconstructing payloads.
	RestoreStoredPath(ctx context.Context, req RestoreStoredPathRequest) (RestoreStoredPathResult, error)
}
