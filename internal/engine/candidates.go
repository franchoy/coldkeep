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
type SnapshotQuery struct {
	// Path matches an exact stored path.
	Path string
	// Prefix matches stored paths by prefix.
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
// Recursive distinguishes single-file store from folder store. Workers applies
// to folder store only.
type StoreRequest struct {
	// SourcePath is the file or folder to store.
	SourcePath string
	// Codec selects the storage codec (e.g. "plain", "aes-gcm"). Empty means
	// the repository default.
	Codec string
	// Recursive requests folder store semantics (store-folder).
	Recursive bool
	// Workers is the parallelism for folder store; zero means the default.
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

// RestoreMode selects how restore targets are addressed.
type RestoreMode string

const (
	// RestoreModeFileIDs restores one or more logical file IDs to a directory.
	RestoreModeFileIDs RestoreMode = "file_ids"
	// RestoreModeStoredPath restores a single stored path.
	RestoreModeStoredPath RestoreMode = "stored_path"
)

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

// RestoreRequest is a candidate request for a future Restore operation.
// Not part of the active v1.12 Engine interface.
//
// Safety invariant: Restore must never write outside the intended destination.
// The destination/mode fields exist precisely so this invariant can be enforced
// in the engine/catalog rather than only in the CLI.
type RestoreRequest struct {
	// Mode selects file-ID or stored-path addressing.
	Mode RestoreMode

	// FileIDs is the set of logical file IDs to restore (Mode == file_ids).
	FileIDs []int64
	// OutputDir is the destination directory for file-ID restore.
	OutputDir string

	// StoredPath is the single stored path to restore (Mode == stored_path).
	StoredPath string
	// DestinationMode controls output location derivation for stored-path
	// restore.
	DestinationMode RestoreDestinationMode
	// Destination is the prefix or override target, required by prefix/override
	// destination modes.
	Destination string
	// Strict enforces strict metadata application.
	Strict bool
	// NoMetadata disables metadata application (mutually exclusive with Strict).
	NoMetadata bool

	// Overwrite permits overwriting existing files.
	Overwrite bool
	// DryRun simulates without writing.
	DryRun bool
	// FailFast stops a batch on the first failure.
	FailFast bool
	// InputPath is an optional batch-input source for file-ID restore.
	//
	// Deferred: whether batch input parsing remains a CLI-level concern or moves
	// into the engine is decided in Phase 7. Retained here so the contract can
	// represent the existing command.
	InputPath string
	// Workers is the batch parallelism; zero means the default.
	Workers int
	// Limit caps the number of restored items when greater than zero.
	Limit int
}

// RestoreItemResult is the outcome of restoring a single target.
type RestoreItemResult struct {
	// FileID is the logical file ID (file-ID mode).
	FileID int64
	// StoredPath is the stored path (stored-path mode).
	StoredPath string
	// OutputPath is the path the file was (or would be) written to.
	OutputPath string
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

// RemoveMode selects how remove targets are addressed.
type RemoveMode string

const (
	// RemoveModeFileIDs removes one or more logical file IDs.
	RemoveModeFileIDs RemoveMode = "file_ids"
	// RemoveModeStoredPath removes a single stored path.
	RemoveModeStoredPath RemoveMode = "stored_path"
	// RemoveModeStoredPaths removes a batch of stored paths.
	RemoveModeStoredPaths RemoveMode = "stored_paths"
)

// RemoveRequest is a candidate request for a future Remove operation.
// Not part of the active v1.12 Engine interface.
type RemoveRequest struct {
	// Mode selects file-ID, single stored-path, or stored-paths addressing.
	Mode RemoveMode
	// FileIDs is the set of logical file IDs to remove (Mode == file_ids).
	FileIDs []int64
	// StoredPath is the single stored path to remove (Mode == stored_path).
	StoredPath string
	// StoredPaths is the batch of stored paths (Mode == stored_paths).
	StoredPaths []string
	// DryRun simulates without mutating.
	DryRun bool
	// FailFast stops a batch on the first failure.
	FailFast bool
	// InputPath is an optional batch-input source.
	//
	// Deferred: batch input parsing ownership is decided in Phase 9.
	InputPath string
}

// RemoveItemResult is the outcome of removing a single target.
type RemoveItemResult struct {
	// FileID is the logical file ID (file-ID mode).
	FileID int64
	// StoredPath is the stored path (stored-path modes).
	StoredPath string
	// RemainingRefCount is the logical-file ref count after removal.
	RemainingRefCount int
	// Removed indicates whether the logical file row was removed.
	Removed bool
	// Status is the per-item outcome.
	Status BatchItemStatus
	// Error is a non-empty message when Status is failed.
	Error string
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
	// Warnings carries structured, non-fatal warnings.
	Warnings []OperationWarning
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
	Tree bool
}

// SnapshotListResult is a candidate result for a future SnapshotList operation.
// Not part of the active v1.12 Engine interface.
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
type SnapshotDiffRequest struct {
	BaseID   string
	TargetID string
	// Summary requests the summary-only fast path (no per-entry list).
	Summary bool
	// Filter narrows the diff to a single change class.
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
