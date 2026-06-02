package catalog

import "time"

// LogicalFileRef is the catalog's neutral representation of a logical_file row.
// Field names and types are aligned with the logical_file table schema.
type LogicalFileRef struct {
	ID           int64
	OriginalName string
	TotalSize    int64
	FileHash     string
	RefCount     int
	Status       string // "PROCESSING" | "COMPLETED" | "ABORTED"
}

// PhysicalFileRef is the catalog's neutral representation of a physical_file
// row. Path is the primary key in the schema.
// MTime is nil when the mtime column is NULL; Mode is zero when NULL.
// IsMetadataComplete maps to is_metadata_complete (bool/INTEGER 0-1).
type PhysicalFileRef struct {
	Path               string
	LogicalFileID      int64
	Mode               int
	MTime              *time.Time // nil when NULL in DB
	IsMetadataComplete bool
}

// SnapshotRef is the catalog's neutral representation of a snapshot row.
// Type is "full" or "partial". ParentID and Label are empty when NULL in the DB.
type SnapshotRef struct {
	ID        string
	Type      string // "full" | "partial"
	Label     string
	ParentID  string
	CreatedAt time.Time
}

// SnapshotFilter constrains the result set of ListSnapshots. Zero values mean
// "no filter on this dimension".
type SnapshotFilter struct {
	// Type filters by snapshot type; empty means all.
	Type string
	// LabelSubstring filters snapshots whose label contains the substring.
	LabelSubstring string
	// Since and Until bound the created_at range (inclusive).
	Since *time.Time
	Until *time.Time
	// Limit caps the result count when greater than zero.
	Limit int
}

// SnapshotGraph is a renderer-neutral representation of the snapshot lineage.
// Deferred to Phase 5/6; fields are minimal so the skeleton compiles without
// committing to the graph traversal shape.
type SnapshotGraph struct {
	// Roots are the snapshot IDs with no parent.
	Roots []string
	// Edges maps snapshot ID to parent snapshot ID.
	Edges map[string]string
}

// ReachabilityRoots holds the logical file ID sets used by GC and verification.
// Current is the set reachable from physical_file (active working set);
// Snapshot is the set reachable from snapshot_file (snapshot-protected).
//
// Both packed and legacy storage roots contribute to the same reachability sets
// because reachability is expressed in terms of logical file IDs, not storage
// format.
type ReachabilityRoots struct {
	// Current is the set of logical file IDs referenced by physical_file rows.
	Current map[int64]struct{}
	// Snapshot is the set of logical file IDs referenced by snapshot_file rows.
	Snapshot map[int64]struct{}
}

// ChunkPlacementRef describes where a single chunk is stored.
// It covers both packed storage (storage_blocks/chunk_block_refs) and legacy
// storage (blocks). The Packed field distinguishes the two paths.
//
// Deferred to Phase 7/8.
type ChunkPlacementRef struct {
	ChunkID     int64
	ChunkHash   string
	ContainerID int64
	// BlockID is storage_blocks.id (packed) or blocks.id (legacy).
	BlockID       int64
	Offset        int64 // container_offset (packed) or block_offset (legacy)
	Size          int64 // stored_size
	OffsetInBlock int64 // offset_in_block (packed only; zero for legacy)
	SizeInBlock   int64 // size_in_block (packed only; zero for legacy)
	Packed        bool  // true=packed (storage_blocks); false=legacy (blocks)
}

// RestorePlanInput selects a restore target. Exactly one of FileID,
// SnapshotID, or StoredPath should be populated for a given lookup.
//
// Deferred to Phase 7 (restore migration).
type RestorePlanInput struct {
	FileID     int64
	SnapshotID string
	StoredPath string
}

// RestorePlanMetadata is the catalog-level resolution of a restore target.
//
// Safety note: "restore must not write outside the intended destination" will
// be enforced by the engine using this metadata. The catalog provides complete
// and accurate metadata; the engine enforces destination safety.
//
// Deferred to Phase 7.
type RestorePlanMetadata struct {
	LogicalFile   LogicalFileRef
	PhysicalFiles []PhysicalFileRef
	Placements    []ChunkPlacementRef
}

// GCPlanInput parameterises a GC-plan metadata query. ExcludeSnapshotIDs lists
// snapshot IDs excluded from the reachability roots (e.g. being deleted).
//
// Deferred to Phase 6 (GC migration).
type GCPlanInput struct {
	ExcludeSnapshotIDs []string
}

// GCPlanMetadata is the catalog-level metadata needed to plan a GC run.
// Reachability is expressed in terms of logical file IDs so the engine can
// compute chunk/block/container deletion lists without knowing the storage
// format. Both packed and legacy roots are included.
//
// Deferred to Phase 6.
type GCPlanMetadata struct {
	// ReachableLogicalFileIDs is the set of all retained logical file IDs.
	ReachableLogicalFileIDs map[int64]struct{}
	// ProtectedSnapshotIDs is the set of snapshot IDs that contribute to
	// reachability.
	ProtectedSnapshotIDs []string
}
