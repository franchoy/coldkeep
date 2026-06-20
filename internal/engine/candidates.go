package engine

import "time"

// Mutating and read-side operation candidates — inactive in v1.12 Phase 2.
//
// These request and result types define the future operation contracts for the
// engine facade. They are NOT part of the active Engine interface (Stats,
// Inspect, Verify) and must not be routed from the CLI until explicit later
// phases. Phase 2 expands these placeholders into realistic, renderer-neutral,
// backend-neutral contracts that can preserve existing command behavior when
// the migrations begin.
//
// Contract rules (see docs/release/v1.12/engine-baseline.md):
//   - Requests represent operation intent, not CLI syntax.
//   - Results represent operation outcomes, not human or JSON rendering.
//   - No CLI parser types (cobra/command), renderer types, or io.Writer/stdout/
//     stderr fields.
//   - No SQLite-only or PostgreSQL-only assumptions (backend-neutral).
//   - Contracts must be rich enough to preserve existing CLI behavior before
//     any routing happens.
//
// Safety invariants that any future activating phase must preserve:
//   - GC must never delete reachable data.
//   - Restore must never write outside the intended destination.
//   - Recovery must not legitimize corrupt mappings.
//   - Snapshot operations must preserve immutability and retention semantics.
//   - Transaction, locking, validation, and safety behavior must remain
//     consistent with the existing implementation.

// ---------------------------------------------------------------------------
// Shared operation-neutral types
// ---------------------------------------------------------------------------

// OperationWarning is a structured, renderer-neutral warning produced by an
// engine operation. CLI rendering (human or JSON) is the caller's concern.
type OperationWarning struct {
	// Code is a stable machine-readable identifier for the warning class.
	Code string
	// Message is a short human-readable description.
	Message string
	// Detail carries optional additional context (e.g. an offending path).
	Detail string
}

// ExecutionMode describes how a batch operation was executed. It mirrors the
// existing execution semantics without encoding CLI syntax.
type ExecutionMode string

const (
	// ExecutionModeSequential processes batch items one at a time.
	ExecutionModeSequential ExecutionMode = "sequential"
	// ExecutionModeParallel processes batch items with multiple workers.
	ExecutionModeParallel ExecutionMode = "parallel"
)

// BatchItemStatus is the outcome of a single item within a batch operation.
type BatchItemStatus string

const (
	// BatchItemOK indicates the item completed successfully.
	BatchItemOK BatchItemStatus = "ok"
	// BatchItemFailed indicates the item failed.
	BatchItemFailed BatchItemStatus = "failed"
	// BatchItemSkipped indicates the item was skipped (e.g. dry-run/no-op).
	BatchItemSkipped BatchItemStatus = "skipped"
)

// BatchSummary aggregates the outcome counts of a batch operation.
type BatchSummary struct {
	OK      int
	Failed  int
	Skipped int
}

// SnapshotQuery represents the renderer-neutral file-selection filters shared by
// snapshot show, diff, and restore. All fields are optional; zero values mean
// "no filter on this dimension". Size and time fields use pointers so that a
// zero value can be distinguished from "unset".
//
// Support limitation in v1.13.1: only one exact Path and one Prefix can cross
// the current engine seam, even though CLI parsing may accept richer repeated
// path/prefix inputs before narrowing. Query-shape cleanup belongs to v1.13.3.
type SnapshotQuery struct {
	// Path matches an exact stored path.
	// Support limitation in v1.13.1: only one exact path is preserved at the
	// current engine seam.
	Path string
	// Prefix matches stored paths by prefix.
	// Support limitation in v1.13.1: only one prefix is preserved at the
	// current engine seam.
	Prefix string
	// Pattern is a glob-style match against stored paths.
	Pattern string
	// Regex is a regular-expression match against stored paths.
	Regex string
	// MinSize, when set, filters files at or above the given byte size.
	MinSize *int64
	// MaxSize, when set, filters files at or below the given byte size.
	MaxSize *int64
	// ModifiedAfter, when set, filters files modified at or after the time.
	ModifiedAfter *time.Time
	// ModifiedBefore, when set, filters files modified at or before the time.
	ModifiedBefore *time.Time
	// Limit, when greater than zero, caps the number of returned files.
	Limit int
}

// ---------------------------------------------------------------------------
// Store / store-folder
// ---------------------------------------------------------------------------

// StoreRequest is a candidate request for a future Store / store-folder
// operation. Not part of the active v1.12 Engine interface.
//
// Support limitation in v1.13.1: the active Engine.Store path owns only
// single-file store. Recursive/folder semantics remain deferred, and
// Engine.Store returns ErrNotImplemented when Recursive is true. Full folder
// store cleanup remains outside engine scope in v2.x.
type StoreRequest struct {
	// SourcePath is the file or folder to store.
	SourcePath string
	// Codec selects the storage codec (e.g. "plain", "aes-gcm"). Empty means
	// the repository default.
	Codec string
	// Recursive requests folder store semantics (store-folder).
	// Support limitation in v1.13.1: active Engine.Store callers must leave
	// this false; true returns ErrNotImplemented.
	Recursive bool
	// Workers is the parallelism for folder store; zero means the default.
	// Support limitation in v1.13.1: this is candidate-only until recursive
	// folder store is activated outside the current engine route.
	Workers int
	// Tags carries optional caller-supplied tags.
	Tags []string
}

// StoreResult is a candidate result for a future Store operation.
// Not part of the active v1.12 Engine interface.
type StoreResult struct {
	// SourcePath echoes the stored source path.
	SourcePath string
	// StoredPath is the canonical stored path recorded in the catalog.
	StoredPath string
	// LogicalFileID identifies the stored logical file.
	LogicalFileID int64
	// PhysicalFileID identifies the underlying physical file when applicable.
	PhysicalFileID int64
	// FileHash is the content hash (e.g. SHA-256) of the stored file.
	FileHash string
	// AlreadyStored indicates the content was already present (dedup hit).
	AlreadyStored bool
	// BytesLogical is the logical (pre-transform) size in bytes.
	BytesLogical int64
	// BytesStored is the physical (post-transform) size in bytes.
	BytesStored int64
	// ChunksCreated and ChunksReused describe chunk-level dedup outcomes.
	ChunksCreated int
	ChunksReused  int
	// Warnings carries structured, non-fatal warnings.
	Warnings []OperationWarning
}

// ---------------------------------------------------------------------------
// Restore
// ---------------------------------------------------------------------------

// RestoreDestinationMode controls how a restored file's output location is
// derived. It mirrors the existing stored-path restore modes.
type RestoreDestinationMode string

const (
	// RestoreDestinationOriginal reconstructs the file at its original path.
	RestoreDestinationOriginal RestoreDestinationMode = "original"
	// RestoreDestinationPrefix prepends a destination prefix to the path.
	RestoreDestinationPrefix RestoreDestinationMode = "prefix"
	// RestoreDestinationOverride writes to an exact destination path.
	RestoreDestinationOverride RestoreDestinationMode = "override"
)

// RestoreRequest is the active by-ID restore request contract.
//
// Safety invariant: Restore must never write outside the intended destination.
type RestoreRequest struct {
	// FileIDs is the ordered set of logical file IDs to restore.
	FileIDs []int64
	// DestinationRoot is the output root used to derive per-item destinations.
	DestinationRoot string

	// Overwrite permits overwriting existing files.
	Overwrite bool
	// DryRun simulates without writing.
	DryRun bool
	// FailFast stops a batch on the first failure.
	FailFast bool
}

// RestoreItemResult is the outcome of restoring a single target.
type RestoreItemResult struct {
	// FileID is the restored logical file ID.
	FileID int64
	// DestinationPath is the path the file was (or would be) written to.
	DestinationPath string
	// RestoredHash is the content hash of the restored file.
	RestoredHash string
	// Status is the per-item outcome.
	Status BatchItemStatus
	// Error is a non-empty message when Status is failed.
	Error string
}

// RestoreResult is a candidate result for a future Restore operation.
// Not part of the active v1.12 Engine interface.
type RestoreResult struct {
	// DryRun echoes whether the operation was a simulation.
	DryRun bool
	// ExecutionMode echoes how the batch was executed.
	ExecutionMode ExecutionMode
	// Items holds per-target outcomes.
	Items []RestoreItemResult
	// Summary aggregates the item outcomes.
	Summary BatchSummary
	// Warnings carries structured, non-fatal warnings.
	Warnings []OperationWarning
}

// ---------------------------------------------------------------------------
// Remove
// ---------------------------------------------------------------------------

// RemoveRequest is the active by-ID remove request contract.
type RemoveRequest struct {
	// FileIDs is the ordered set of logical file IDs to remove.
	FileIDs []int64
	// DryRun simulates without mutating.
	DryRun bool
	// FailFast stops a batch on the first failure.
	FailFast bool
}

// RemoveItemResult is the outcome of removing a single target.
type RemoveItemResult struct {
	// FileID is the removed logical file ID.
	FileID int64
	// LogicalFileRemoved reports whether the logical file row was removed.
	LogicalFileRemoved bool
	// RemovedChunkAssociations is the count of removed file_chunk associations.
	RemovedChunkAssociations int
	// Status is the per-item outcome.
	Status BatchItemStatus
	// Error is a non-empty message when Status is failed.
	Error string
	// InvariantCode is the machine-readable invariant identifier when available.
	InvariantCode string
	// RecommendedAction is operator guidance associated with InvariantCode.
	RecommendedAction string
}

// RemoveResult is a candidate result for a future Remove operation.
// Not part of the active v1.12 Engine interface.
type RemoveResult struct {
	// DryRun echoes whether the operation was a simulation.
	DryRun bool
	// ExecutionMode echoes how the batch was executed.
	ExecutionMode ExecutionMode
	// Items holds per-target outcomes.
	Items []RemoveItemResult
	// Summary aggregates the item outcomes.
	Summary BatchSummary
}

// ---------------------------------------------------------------------------
// Garbage collection
// ---------------------------------------------------------------------------

// GarbageCollectRequest is a candidate request for a future GarbageCollect
// operation. Not part of the active v1.12 Engine interface.
//
// Safety invariant: GC must never delete reachable data.
type GarbageCollectRequest struct {
	// DryRun simulates the collection without deleting.
	DryRun bool
	// Workers is the parallelism; zero means the default.
	Workers int
}

// GarbageCollectResult is a candidate result for a future GarbageCollect
// operation. Not part of the active v1.12 Engine interface.
//
// The retention fields represent both packed and legacy roots so that GC plan
// reporting can stay backend- and storage-format-neutral.
type GarbageCollectResult struct {
	// DryRun echoes whether the operation was a simulation.
	DryRun bool
	// AffectedContainers is the count of containers deleted (or that would be).
	AffectedContainers int
	// ContainerFilenames lists the affected container filenames.
	ContainerFilenames []string
	// SnapshotRetainedContainers is the count of containers retained because a
	// snapshot references them.
	SnapshotRetainedContainers int
	// SnapshotRetainedLogicalFiles is the count of logical files retained by
	// snapshots.
	SnapshotRetainedLogicalFiles int
	// CurrentOnlyRetainedLogicalFiles, SnapshotOnlyRetainedLogicalFiles, and
	// SharedRetainedLogicalFiles break down retention by reachability source.
	CurrentOnlyRetainedLogicalFiles  int
	SnapshotOnlyRetainedLogicalFiles int
	SharedRetainedLogicalFiles       int
	// BytesReclaimed is the number of bytes reclaimed (or reclaimable).
	BytesReclaimed int64
	// Warnings carries structured, non-fatal warnings.
	Warnings []OperationWarning
}

// ---------------------------------------------------------------------------
// Snapshot operations
// ---------------------------------------------------------------------------

// SnapshotType distinguishes full and partial snapshots.
type SnapshotType string

const (
	// SnapshotTypeFull captures the whole current state.
	SnapshotTypeFull SnapshotType = "full"
	// SnapshotTypePartial captures a path-scoped subset.
	SnapshotTypePartial SnapshotType = "partial"
)

// SnapshotMeta is the renderer-neutral metadata for a snapshot.
type SnapshotMeta struct {
	ID        string
	Type      SnapshotType
	Label     string
	ParentID  string
	CreatedAt time.Time
	FileCount int
}

// SnapshotCreateRequest is a candidate request for a future SnapshotCreate
// operation. Not part of the active v1.12 Engine interface.
//
// Candidate-only in v1.13.1: request/result presence must not be mistaken for
// active engine ownership. Snapshot create/delete/restore remain CLI/domain
// owned until the explicit snapshot-mutation follow-up in v1.13.9.
//
// Safety invariant: Snapshot operations must preserve immutability and
// retention semantics.
type SnapshotCreateRequest struct {
	// ID is an optional caller-supplied snapshot ID; empty means auto-generate.
	ID string
	// Label is an optional human label.
	Label string
	// ParentID establishes lineage for delta/reuse analysis (the --from source).
	ParentID string
	// Paths scopes a partial snapshot; empty means a full snapshot.
	Paths []string
}

// SnapshotCreateResult is a candidate result for a future SnapshotCreate
// operation. Not part of the active v1.12 Engine interface.
type SnapshotCreateResult struct {
	SnapshotID    string
	Type          SnapshotType
	PathsCount    int
	FilesInserted int
	Label         string
	ParentID      string
	Warnings      []OperationWarning
}

// SnapshotListRequest is a candidate request for a future SnapshotList
// operation. Not part of the active v1.12 Engine interface.
type SnapshotListRequest struct {
	// Type filters by snapshot type; empty means all.
	Type SnapshotType
	// Label filters by label substring.
	Label string
	// Since and Until bound the created-at range.
	Since *time.Time
	Until *time.Time
	// Limit caps the number of results when greater than zero.
	Limit int
	// Tree requests lineage-tree ordering/visualization data.
	// Support limitation in v1.13.1: this is a provisional view-shaping flag
	// and does not prove engine ownership of lineage presentation semantics.
	// Read-side cleanup belongs to v1.13.3 / v1.13.12.
	Tree bool
}

// SnapshotListResult is a candidate result for a future SnapshotList operation.
// Not part of the active v1.12 Engine interface.
//
// Support limitation in v1.13.1: TreeMode and TreeLines are provisional
// view-shaping fields. They do not prove engine ownership of lineage
// presentation semantics; read-side cleanup belongs to v1.13.3 / v1.13.12.
type SnapshotListResult struct {
	Snapshots []SnapshotMeta
	Count     int
	// TreeMode echoes whether tree data was requested.
	TreeMode bool
	// TreeLines holds renderer-neutral lineage rows when TreeMode is set.
	TreeLines []string
}

// SnapshotFile is a renderer-neutral file entry within a snapshot.
type SnapshotFile struct {
	StoredPath    string
	LogicalFileID int64
	Size          int64
	Mode          uint32
	ModTime       time.Time
}

// SnapshotShowRequest is a candidate request for a future SnapshotShow (files)
// operation. Not part of the active v1.12 Engine interface.
type SnapshotShowRequest struct {
	SnapshotID string
	// Query filters which files are returned.
	Query SnapshotQuery
}

// SnapshotShowResult is a candidate result for a future SnapshotShow operation.
// Not part of the active v1.12 Engine interface.
//
// Support limitation in v1.13.1: this coherent result shape is still
// provisional and does not prove fully unified engine ownership. Metadata,
// listing, and counts still come from mixed seams; read-side cleanup belongs
// to v1.13.3.
type SnapshotShowResult struct {
	Snapshot SnapshotMeta
	Files    []SnapshotFile
	// MatchedFileCount is the number of files matching Query.
	MatchedFileCount int
	// TotalFileCount is the total number of files in the snapshot.
	TotalFileCount int
}

// SnapshotStatsRequest is a candidate request for a future SnapshotStats
// operation. Not part of the active v1.12 Engine interface.
//
// SnapshotID is optional; empty means aggregate stats across all snapshots.
type SnapshotStatsRequest struct {
	SnapshotID string
}

// SnapshotStatsResult is a candidate result for a future SnapshotStats
// operation. Not part of the active v1.12 Engine interface.
//
// Reuse fields are populated only for a specific snapshot that has a parent.
type SnapshotStatsResult struct {
	SnapshotCount     int
	SnapshotFileCount int
	TotalSizeBytes    int64
	// HasReuse indicates whether the reuse metrics below are meaningful.
	HasReuse bool
	Reused   int
	New      int
	// ReuseRatio is a percentage in [0,100].
	ReuseRatio float64
	// LineageStatus explains why HasReuse is false, when applicable.
	// Mirrors snapshot.SnapshotLineageStatus string values.
	// Empty when HasReuse is true or when SnapshotID is empty (aggregate call).
	LineageStatus string
	// ParentSnapshotID is the parent snapshot's ID when HasReuse is true.
	ParentSnapshotID string
}

// SnapshotDiffFilter narrows a diff to a single change class.
type SnapshotDiffFilter string

const (
	// SnapshotDiffAll includes all change classes.
	SnapshotDiffAll SnapshotDiffFilter = ""
	// SnapshotDiffAdded includes only added entries.
	SnapshotDiffAdded SnapshotDiffFilter = "added"
	// SnapshotDiffRemoved includes only removed entries.
	SnapshotDiffRemoved SnapshotDiffFilter = "removed"
	// SnapshotDiffModified includes only modified entries.
	SnapshotDiffModified SnapshotDiffFilter = "modified"
)

// SnapshotDiffChange classifies a single diff entry.
type SnapshotDiffChange string

const (
	SnapshotDiffChangeAdded    SnapshotDiffChange = "added"
	SnapshotDiffChangeRemoved  SnapshotDiffChange = "removed"
	SnapshotDiffChangeModified SnapshotDiffChange = "modified"
)

// SnapshotDiffEntry is a renderer-neutral diff entry.
type SnapshotDiffEntry struct {
	StoredPath string
	Change     SnapshotDiffChange
}

// SnapshotDiffRequest is a candidate request for a future SnapshotDiff
// operation. Not part of the active v1.12 Engine interface.
//
// Support limitation in v1.13.1: summary fast-path behavior and query/filter
// semantics remain provisional. The CLI can parse richer repeated path/prefix
// inputs than the current engine seam preserves, and full read-side cleanup
// belongs to v1.13.3.
type SnapshotDiffRequest struct {
	BaseID   string
	TargetID string
	// Summary requests the summary-only fast path (no per-entry list).
	// Support limitation in v1.13.1: when this fast path is used, the current
	// engine result reports summary-only semantics rather than a full entry list.
	Summary bool
	// Filter narrows the diff to a single change class.
	// Support limitation in v1.13.1: filter behavior is layered on top of a
	// provisional diff seam and is not yet a frozen contract.
	Filter SnapshotDiffFilter
	// Query filters which entries are considered.
	Query SnapshotQuery
}

// SnapshotDiffSummary aggregates change counts.
type SnapshotDiffSummary struct {
	Added    int
	Removed  int
	Modified int
}

// SnapshotDiffResult is a candidate result for a future SnapshotDiff operation.
// Not part of the active v1.12 Engine interface.
//
// Support limitation in v1.13.1: SummaryMode, MatchedEntryCount, and
// TotalEntryCount are provisional read-side semantics. They reflect the
// current summary-versus-detailed seam and filtering behavior, not a frozen
// daemon/API-ready diff contract. Read-side cleanup belongs to v1.13.3.
type SnapshotDiffResult struct {
	BaseID   string
	TargetID string
	// SummaryMode echoes whether the summary-only fast path was used.
	SummaryMode bool
	Summary     SnapshotDiffSummary
	// Entries is populated only when SummaryMode is false.
	Entries []SnapshotDiffEntry
	// MatchedEntryCount and TotalEntryCount describe filtering.
	MatchedEntryCount int
	TotalEntryCount   int
}

// SnapshotRestoreRequest is a candidate request for a future SnapshotRestore
// operation. Not part of the active v1.12 Engine interface.
//
// Candidate-only in v1.13.1: request/result presence must not be mistaken for
// active engine ownership. Snapshot create/delete/restore remain CLI/domain
// owned until the explicit snapshot-mutation follow-up in v1.13.9.
//
// Safety invariant: Restore must never write outside the intended destination.
type SnapshotRestoreRequest struct {
	SnapshotID string
	// Paths scopes a partial restore; empty means restore all snapshot files.
	Paths []string
	// DestinationMode controls output location derivation.
	DestinationMode RestoreDestinationMode
	// Destination is the prefix or override target.
	Destination string
	// Overwrite permits overwriting existing files.
	Overwrite bool
	// Strict enforces strict metadata application.
	Strict bool
	// NoMetadata disables metadata application.
	NoMetadata bool
	// Query filters which snapshot files are restored.
	Query SnapshotQuery
}

// SnapshotRestoreResult is a candidate result for a future SnapshotRestore
// operation. Not part of the active v1.12 Engine interface.
type SnapshotRestoreResult struct {
	SnapshotID string
	Type       SnapshotType
	// RestoredFiles is the number of files restored (or that would be).
	RestoredFiles int
	// OutputRoot is the effective destination root.
	OutputRoot string
	Warnings   []OperationWarning
}

// SnapshotDeleteRequest is a candidate request for a future SnapshotDelete
// operation. Not part of the active v1.12 Engine interface.
//
// Candidate-only in v1.13.1: request/result presence must not be mistaken for
// active engine ownership. Snapshot create/delete/restore remain CLI/domain
// owned until the explicit snapshot-mutation follow-up in v1.13.9.
//
// Safety invariant: Snapshot operations must preserve immutability and
// retention semantics. Deleting a snapshot removes only its metadata; content
// referenced by other snapshots or the current state must be retained.
type SnapshotDeleteRequest struct {
	SnapshotID string
	// Force performs a live delete (mutually exclusive with DryRun).
	Force bool
	// DryRun simulates the delete.
	DryRun bool
}

// SnapshotDeleteResult is a candidate result for a future SnapshotDelete
// operation. Not part of the active v1.12 Engine interface.
type SnapshotDeleteResult struct {
	SnapshotID string
	DryRun     bool
	ParentID   string
	// ParentMissing indicates the recorded parent no longer exists.
	ParentMissing bool
	// Children lists snapshot IDs whose lineage references this snapshot.
	Children []string
	// TotalFiles, UniqueFiles, and SharedFiles describe content impact.
	TotalFiles  int
	UniqueFiles int
	SharedFiles int
	Warnings    []OperationWarning
}

// ---------------------------------------------------------------------------
// Repair
// ---------------------------------------------------------------------------

// RepairTarget selects which integrity recomputation a repair performs.
type RepairTarget string

const (
	// RepairTargetRefCounts recomputes logical_file.ref_count.
	RepairTargetRefCounts RepairTarget = "ref-counts"
	// RepairTargetChunkLiveRefCounts recomputes chunk.live_ref_count.
	RepairTargetChunkLiveRefCounts RepairTarget = "chunk-live-ref-counts"
)

// RepairRequest is a candidate request for a future Repair operation.
// Not part of the active v1.12 Engine interface.
//
// Candidate-only in v1.13.1: request/result presence must not be mistaken for
// active engine ownership. Repair and recover remain CLI/domain owned until
// the explicit corrective-integrity follow-up in v1.13.10.
type RepairRequest struct {
	// Target selects the single-target repair (when Batch is false).
	Target RepairTarget
	// Batch processes multiple targets.
	Batch bool
	// Targets is the explicit batch target list.
	Targets []RepairTarget
	// FailFast stops a batch on the first failure.
	FailFast bool
	// InputPath is an optional batch-input source.
	//
	// Deferred: batch input parsing ownership is decided in Phase 9; it may
	// remain a CLI-level concern rather than an engine input.
	InputPath string
	// DryRun simulates without mutating, where supported.
	DryRun bool
	// Limit caps the number of rows processed when greater than zero.
	Limit int
}

// RepairTargetResult is the outcome of a single repair target.
type RepairTargetResult struct {
	Target RepairTarget
	// ScannedRows and UpdatedRows are generic counters covering both
	// logical-file and chunk recomputations.
	ScannedRows int
	UpdatedRows int
	// OrphanRows captures orphan physical-file rows for ref-count repair.
	OrphanRows int
	Status     BatchItemStatus
	Error      string
}

// RepairResult is a candidate result for a future Repair operation.
// Not part of the active v1.12 Engine interface.
type RepairResult struct {
	// Targets holds per-target outcomes.
	Targets []RepairTargetResult
	// Summary aggregates the target outcomes.
	Summary  BatchSummary
	Warnings []OperationWarning
}

// ---------------------------------------------------------------------------
// Recovery
// ---------------------------------------------------------------------------

// RecoverRequest is a candidate request for a future corrective Recover
// operation. Not part of the active v1.12 Engine interface.
//
// Candidate-only in v1.13.1: request/result presence must not be mistaken for
// active engine ownership. Repair and recover remain CLI/domain owned until
// the explicit corrective-integrity follow-up in v1.13.10.
//
// Safety invariant: Recovery must not legitimize corrupt mappings. Recovery is
// a corrective integrity pass (abort dangling writes, clear stale sealing
// markers, quarantine corrupt/orphaned data), NOT a restore. The previous
// placeholder modeled it like a restore; that was incorrect.
type RecoverRequest struct {
	// DryRun reports what recovery would do without mutating.
	DryRun bool
}

// RecoverResult is a candidate result for a future Recover operation.
// Not part of the active v1.12 Engine interface.
//
// Fields mirror the existing recovery report so the corrective outcome can be
// represented without CLI rendering.
type RecoverResult struct {
	AbortedLogicalFiles    int
	AbortedChunks          int
	QuarantinedMissing     int
	QuarantinedCorruptTail int
	QuarantinedOrphan      int
	SkippedDirEntries      int
	CheckedContainerRecord int
	CheckedDiskFiles       int
	SealingCompleted       int
	SealingQuarantined     int
	Warnings               []OperationWarning
}
