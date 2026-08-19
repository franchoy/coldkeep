// Package catalog is the behavior-preserving metadata facade for coldkeep.
//
// v1.12 Phase 3 — Catalog Facade Skeleton.
//
// The catalog owns metadata truth: logical identity, physical-file mapping,
// snapshot metadata and graph, reachability, chunk/block/container placement,
// restore-plan metadata, and GC-plan metadata. Storage owns payload bytes;
// the engine owns operation orchestration. This package introduces the catalog
// boundary WITHOUT migrating any command orchestration. No snapshot, GC,
// restore, store, remove, repair, or recovery behavior is moved here in Phase 3.
//
// # Invariants
//
// The catalog facade must expose metadata truth without changing metadata
// behavior:
//
//   - No schema changes.
//   - No SQL behavior changes.
//   - No SQLite-only or PostgreSQL-only assumptions (backend-neutral).
//   - Packed and legacy storage roots must both be representable; neither may be
//     silently assumed by reachability or restore planning.
//
// # Dependency direction
//
//	internal/engine    may import internal/catalog
//	internal/catalog   must not import internal/engine
//	internal/catalog   must not import cmd/coldkeep
//	internal/catalog   must not import CLI renderer packages
//	internal/catalog   must not import storage byte-I/O packages
//
// # Contract neutrality
//
// Exported request/result types must not expose *sql.DB, *sql.Tx, sql.Rows,
// sql.Row, io.Writer, cobra, or any renderer type. The catalog uses database/sql
// internally via the DB abstraction, but that handle must not leak into exported
// contracts.
package catalog

import (
	"context"
	"database/sql"
)

// DB is the minimal database abstraction the catalog depends on internally.
// Both *sql.DB and *sql.Tx satisfy it, which keeps the facade testable and
// transaction-aware without leaking a concrete handle to callers.
//
// Queries use positional placeholders ($1, $2, ...). These work on PostgreSQL
// directly and on SQLite via the go-sqlite3 driver's automatic conversion, so
// the catalog stays backend-neutral.
type DB interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// LogicalFileCatalog exposes logical-file identity metadata.
type LogicalFileCatalog interface {
	// FindLogicalFile returns the logical file with the given ID.
	// Returns (nil, nil) when no row exists.
	FindLogicalFile(ctx context.Context, id int64) (*LogicalFileRef, error)
}

// PhysicalFileCatalog exposes physical-file mapping metadata.
type PhysicalFileCatalog interface {
	// FindPhysicalFilesForLogicalFile returns the physical files mapping to the
	// given logical file ID, ordered by path.
	FindPhysicalFilesForLogicalFile(ctx context.Context, logicalFileID int64) ([]PhysicalFileRef, error)
}

// CurrentFileCatalog owns completed current-state path query truth.
type CurrentFileCatalog interface {
	ListCurrentFiles(ctx context.Context, page CurrentFilePage) ([]CurrentFileRef, error)
	SearchCurrentFiles(ctx context.Context, filter CurrentFileSearch) ([]CurrentFileRef, error)
}

// RepositoryConfigurationCatalog owns repository_config metadata truth.
type RepositoryConfigurationCatalog interface {
	GetRepositoryConfiguration(ctx context.Context, key string) (RepositoryConfigurationRef, error)
	SetRepositoryConfiguration(ctx context.Context, key, value string) (SetRepositoryConfigurationResult, error)
}

// SnapshotCatalog exposes snapshot metadata.
type SnapshotCatalog interface {
	// FindSnapshot returns the snapshot with the given ID.
	// Returns (nil, nil) when no row exists.
	FindSnapshot(ctx context.Context, id string) (*SnapshotRef, error)
	// ListSnapshots returns snapshots matching the filter, newest first.
	ListSnapshots(ctx context.Context, filter SnapshotFilter) ([]SnapshotRef, error)
}

// SnapshotGraphCatalog exposes the snapshot lineage graph.
//
// Phase 5 implemented this contract.
// Empty catalogs return empty ordered slices. Historical missing parents are
// represented by SnapshotParentMissing; malformed cycles return a typed
// invariant_violation. The catalog never invents or silently repairs edges.
type SnapshotGraphCatalog interface {
	LoadSnapshotGraph(ctx context.Context) (*SnapshotGraph, error)
}

// ReachabilityCatalog exposes reachability roots used by GC and verification.
type ReachabilityCatalog interface {
	// LoadReachabilityRoots returns the current-state and snapshot-referenced
	// logical file ID sets.
	LoadReachabilityRoots(ctx context.Context) (*ReachabilityRoots, error)
}

// PlacementCatalog exposes chunk/block/container placement metadata.
//
// Phase 6 implemented this contract. It represents packed and legacy roots as
// a strict tagged union and never returns a partial placement result.
// A missing logical file returns not_found. A zero-length logical file returns
// an empty placement slice. Missing, duplicate, mixed, or malformed placement
// rows return invariant_violation rather than a partial recipe.
type PlacementCatalog interface {
	LoadChunkPlacements(ctx context.Context, logicalFileID int64) ([]ChunkPlacementRef, error)
}

// RestorePlanCatalog exposes restore-plan metadata.
//
// Phase 7 implemented this contract, where the "restore must not write
// outside destination" invariant is enforced at the engine/catalog boundary.
// The service never opens or commits a transaction: constructing it with the
// caller's *sql.Tx keeps selector resolution and recipe loading in that exact
// transaction.
// A missing target returns not_found; a non-exclusive selector returns
// invalid_argument; ambiguous or incomplete metadata returns conflict or
// invariant_violation. No partial plan is returned on any error.
type RestorePlanCatalog interface {
	LoadRestorePlanMetadata(ctx context.Context, input RestorePlanInput) (*RestorePlanMetadata, error)
}

// GCPlanCatalog exposes GC-plan metadata.
//
// Phase 9 implements and adopts this contract, where the "GC must never delete
// reachable data" invariant is tested at the engine/catalog boundary.
// Excluded snapshot IDs are validated before reads; missing IDs return a typed
// not_found error, while malformed graph rows return invariant_violation.
type GCPlanCatalog interface {
	LoadGCPlanMetadata(ctx context.Context, input GCPlanInput) (*GCPlanMetadata, error)
}

// Catalog is the aggregate facade interface composed of the per-responsibility
// interfaces above. Service implements it.
type Catalog interface {
	LogicalFileCatalog
	PhysicalFileCatalog
	CurrentFileCatalog
	RepositoryConfigurationCatalog
	SnapshotCatalog
	SnapshotGraphCatalog
	ReachabilityCatalog
	PlacementCatalog
	RestorePlanCatalog
	GCPlanCatalog
}
