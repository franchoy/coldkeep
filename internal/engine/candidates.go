package engine

import "time"

// Engine operation contracts.
//
// This historically named file contains active Engine request/result contracts
// plus corrective Repair and Recover contracts that later v1.13.12 phases
// activate. Active method ownership is defined by the Engine interface, not by
// this file name.
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
// Safety invariants that active contracts and any future activation must
// preserve:
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
	// BatchItemPlanned indicates the item was validated/read-only planned.
	BatchItemPlanned BatchItemStatus = "planned"
	// BatchItemFailed indicates the item failed.
	BatchItemFailed BatchItemStatus = "failed"
	// BatchItemSkipped indicates the item was skipped (e.g. duplicate/no-op).
	BatchItemSkipped BatchItemStatus = "skipped"
)

// BatchSummary aggregates the outcome counts of a batch operation.
type BatchSummary struct {
	OK      int
	Failed  int
	Skipped int
}

// SnapshotQuery represents the renderer-neutral file-selection filters shared by
// snapshot show and diff. All fields are optional; zero values mean
// "no filter on this dimension". Size and time fields use pointers so that a
// zero value can be distinguished from "unset".
type SnapshotQuery struct {
	// Paths match exact normalized stored paths.
	Paths []string
	// Prefixes match normalized stored paths by directory prefix.
	Prefixes []string
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

// StoreRequest is the active request contract for Engine.Store.
//
// Store is intentionally single-file only. Folder traversal and aggregation
// use the distinct StoreFolder operation.
type StoreRequest struct {
	// SourcePath is the file to store.
	SourcePath string
	// Codec selects the storage codec (e.g. "plain", "aes-gcm"). Empty means
	// the repository default.
	Codec string
}

// StoreResult is the active result contract for Engine.Store.
type StoreResult struct {
	// SourcePath echoes the stored source path.
	SourcePath string
	// StoredPath is the canonical stored path recorded in the catalog.
	StoredPath string
	// LogicalFileID identifies the stored logical file.
	LogicalFileID int64
	// FileHash is the content hash (e.g. SHA-256) of the stored file.
	FileHash string
	// AlreadyStored indicates the content was already present (dedup hit).
	AlreadyStored bool
}

// StoreFolderRequest is the recursive folder-store contract. Workers zero
// selects the established default; positive values request bounded file-level
// fan-out subject to writer capability.
type StoreFolderRequest struct {
	SourcePath string
	Codec      string
	Workers    int
}

// StoreFolderResult reports deterministic aggregate execution statistics.
// Partial statistics are returned with an error when work fails after some
// files have completed.
type StoreFolderResult struct {
	SourcePath   string
	FilesStored  int
	BytesLogical int64
	WorkersUsed  int
}

const MaxFileQueryLimit int64 = 10000

// CurrentFile is a presentation-neutral completed current-state path.
// JSON tags preserve the established CLI projection when the CLI embeds it.
type CurrentFile struct {
	ID        int64  `json:"id"`
	Name      string `json:"name"`
	FileHash  string `json:"file_hash"`
	SizeBytes int64  `json:"size_bytes"`
	CreatedAt string `json:"created_at"`
}

type ListFilesRequest struct {
	Limit  *int64
	Offset *int64
}

type ListFilesResult struct {
	Files []CurrentFile
}

// SearchFilesRequest preserves repeated filters and their historical AND
// semantics without exposing raw CLI tokens to the engine or catalog.
type SearchFilesRequest struct {
	NameContains []string
	MinSizeBytes []int64
	MaxSizeBytes []int64
	Limit        *int64
	Offset       *int64
}

type SearchFilesResult struct {
	Files []CurrentFile
}

type ConfigurationKey string

const (
	ConfigurationDefaultChunker   ConfigurationKey = "default-chunker"
	ConfigurationCompression      ConfigurationKey = "compression"
	ConfigurationCompressionLevel ConfigurationKey = "compression-level"
)

type GetConfigurationRequest struct {
	Key ConfigurationKey
}

type GetConfigurationResult struct {
	Key          ConfigurationKey
	Value        string
	IntegerValue *int64
}

type SetConfigurationRequest struct {
	Key   ConfigurationKey
	Value string
}

type SetConfigurationResult struct {
	Key          ConfigurationKey
	Value        string
	IntegerValue *int64
	Changed      bool
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

// RestoreResult is the active by-ID restore batch result contract.
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

// RestoreStoredPathRequest is the active stored-path restore request contract.
//
// It restores exactly one current persisted physical_file.path mapping using
// destination semantics that remain distinct from by-ID restore.
type RestoreStoredPathRequest struct {
	// StoredPath identifies exactly one persisted physical_file.path.
	StoredPath string
	// DestinationMode controls how the output location is derived.
	DestinationMode RestoreDestinationMode
	// DestinationRoot is used only by prefix mode.
	DestinationRoot string
	// DestinationPath is used only by override mode.
	DestinationPath string
	// Overwrite permits overwriting existing files.
	Overwrite bool
	// StrictMetadata enforces strict metadata application.
	StrictMetadata bool
	// NoMetadata disables metadata application.
	NoMetadata bool
}

// RestoreStoredPathResult is the active stored-path restore success contract.
//
// It is a single-operation result shape; execution failures are returned as
// errors rather than embedded item status fields.
type RestoreStoredPathResult struct {
	// StoredPath is the trimmed stored path used for the catalog lookup.
	StoredPath string
	// FileID identifies the owning logical file.
	FileID int64
	// DestinationMode is the normalized destination mode that executed.
	DestinationMode RestoreDestinationMode
	// DestinationPath is the exact resolved output path.
	DestinationPath string
	// RestoredHash is the successful restored content hash.
	RestoredHash string
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

// RemoveResult is the active by-ID remove batch result contract.
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

// RemoveStoredPathsRequest is the active stored-path batch remove contract.
type RemoveStoredPathsRequest struct {
	// StoredPaths is the ordered set of raw stored-path targets.
	StoredPaths []string
	// DryRun simulates unlinking without mutating.
	DryRun bool
	// FailFast stops on the first executable target failure.
	FailFast bool
}

// RemoveStoredPathItemResult is the outcome of unlinking one stored-path
// target.
type RemoveStoredPathItemResult struct {
	// RawTarget is the exact caller-provided text before trimming.
	RawTarget string
	// StoredPath is the trimmed stored-path value used for lookup/mutation.
	StoredPath string
	// LogicalFileID is the logical file owning the current mapping.
	LogicalFileID int64
	// RemainingRefCount is the remaining current ref-count after a live unlink.
	RemainingRefCount int64
	// MappingRemoved reports whether one physical_file row was removed.
	MappingRemoved bool
	// Status is the per-item outcome.
	Status BatchItemStatus
	// Error is a non-empty message when Status is failed.
	Error string
	// InvariantCode is the machine-readable invariant identifier when available.
	InvariantCode string
	// RecommendedAction is operator guidance associated with InvariantCode.
	RecommendedAction string
}

// RemoveStoredPathsResult is the active stored-path batch remove result.
type RemoveStoredPathsResult struct {
	// DryRun echoes whether the operation was a simulation.
	DryRun bool
	// ExecutionMode echoes how the batch was executed.
	ExecutionMode ExecutionMode
	// Items holds per-target outcomes.
	Items []RemoveStoredPathItemResult
	// Summary aggregates the item outcomes.
	Summary BatchSummary
}

// ---------------------------------------------------------------------------
// Garbage collection
// ---------------------------------------------------------------------------

// GarbageCollectRequest is the active request contract for
// Engine.GarbageCollect.
//
// Safety invariant: GC must never delete reachable data. Dry-run is supported
// on SQLite and PostgreSQL; live collection is supported on PostgreSQL only.
type GarbageCollectRequest struct {
	// DryRun simulates the collection without deleting.
	DryRun bool
	// Workers is the parallelism; zero means the default.
	Workers int
}

// GarbageCollectResult is the active result contract for
// Engine.GarbageCollect.
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

// SnapshotParentState distinguishes a root, a resolved parent, and historical
// missing-parent metadata without inventing a relationship.
type SnapshotParentState string

const (
	SnapshotParentNone    SnapshotParentState = "none"
	SnapshotParentPresent SnapshotParentState = "present"
	SnapshotParentMissing SnapshotParentState = "missing"
)

// SnapshotGraphNode is one renderer-neutral lineage node.
type SnapshotGraphNode struct {
	Snapshot    SnapshotMeta
	ParentState SnapshotParentState
	ChildIDs    []string
}

// SnapshotGraph is ordered by created_at ascending, then snapshot ID.
type SnapshotGraph struct {
	Nodes   []SnapshotGraphNode
	RootIDs []string
}

// SnapshotCreateRequest is the frozen active v1.13.9 Engine snapshot-create
// request surface.
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

// SnapshotCreateResult is the frozen active v1.13.9 Engine snapshot-create
// result surface.
type SnapshotCreateResult struct {
	SnapshotID    string
	Type          SnapshotType
	PathsCount    int
	FilesInserted int
	Label         string
	ParentID      string
}

// SnapshotListRequest is the active request contract for Engine.SnapshotList.
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
	// Rendering remains the caller's responsibility.
	Tree bool
}

// SnapshotListResult is the active result contract for Engine.SnapshotList.
type SnapshotListResult struct {
	Snapshots []SnapshotMeta
	Count     int
	// TreeMode echoes whether tree data was requested.
	TreeMode bool
	// Graph is populated only when TreeMode is true. It contains metadata and
	// relationships, never rendered lines.
	Graph *SnapshotGraph
}

// SnapshotFile is a renderer-neutral file entry within a snapshot.
type SnapshotFile struct {
	StoredPath    string
	LogicalFileID int64
	Size          *int64
	Mode          *int64
	ModTime       *time.Time
}

// SnapshotShowRequest is the active request contract for Engine.SnapshotShow.
type SnapshotShowRequest struct {
	SnapshotID string
	// Query filters which files are returned.
	Query SnapshotQuery
}

// SnapshotShowResult is the active result contract for Engine.SnapshotShow.
type SnapshotShowResult struct {
	Snapshot SnapshotMeta
	Files    []SnapshotFile
	// MatchedFileCount is the number of files matching Query.
	MatchedFileCount int
	// TotalFileCount is the total number of files in the snapshot.
	TotalFileCount int
}

// SnapshotStatsRequest is the active request contract for Engine.SnapshotStats.
//
// SnapshotID is optional; empty means aggregate stats across all snapshots.
type SnapshotStatsRequest struct {
	SnapshotID string
}

// SnapshotStatsResult is the active result contract for Engine.SnapshotStats.
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
	StoredPath      string
	Change          SnapshotDiffChange
	BaseLogicalID   *int64
	TargetLogicalID *int64
}

// SnapshotDiffRequest is the active request contract for Engine.SnapshotDiff.
type SnapshotDiffRequest struct {
	BaseID   string
	TargetID string
	// Summary requests the summary-only fast path (no per-entry list).
	// When this fast path is used, the current engine result reports summary-only
	// semantics rather than a full entry list.
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

// SnapshotDiffResult is the complete result contract for Engine.SnapshotDiff.
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

// SnapshotRestoreDestinationMode is part of the frozen v1.13.9 active
// Engine.SnapshotRestore contract. It remains intentionally distinct from the
// stored-path RestoreDestinationMode contract.
type SnapshotRestoreDestinationMode string

const (
	SnapshotRestoreDestinationOriginal SnapshotRestoreDestinationMode = "original"
	SnapshotRestoreDestinationPrefix   SnapshotRestoreDestinationMode = "prefix"
	SnapshotRestoreDestinationOverride SnapshotRestoreDestinationMode = "override"
)

// SnapshotRestoreDestination is the explicit snapshot-restore destination
// contract frozen in v1.13.9 Phase 4.
type SnapshotRestoreDestination struct {
	Mode SnapshotRestoreDestinationMode
	Path string
}

// SnapshotRestoreMetadataMode is the frozen snapshot-restore metadata policy
// contract. Zero value means best-effort metadata application.
type SnapshotRestoreMetadataMode string

const (
	SnapshotRestoreMetadataBestEffort SnapshotRestoreMetadataMode = ""
	SnapshotRestoreMetadataStrict     SnapshotRestoreMetadataMode = "strict"
	SnapshotRestoreMetadataNone       SnapshotRestoreMetadataMode = "none"
)

// SnapshotRestoreWarningCode is the stable machine-readable restore warning
// code surface frozen in v1.13.9 Phase 4.
type SnapshotRestoreWarningCode string

const (
	SnapshotRestoreWarningMetadata SnapshotRestoreWarningCode = "metadata_apply_failed"
)

// SnapshotRestoreWarning is the renderer-neutral structured restore warning
// shape frozen in v1.13.9 Phase 4.
type SnapshotRestoreWarning struct {
	Code      SnapshotRestoreWarningCode
	Path      string
	Operation string
	Detail    string
}

// SnapshotRestoreSelection is the frozen snapshot-restore selection contract.
// It intentionally differs from the active read-side SnapshotQuery:
// repeated exact paths and prefixes remain representable as slices, regex
// crosses the boundary as a string, and no Limit field exists here.
type SnapshotRestoreSelection struct {
	ExactPaths     []string
	Prefixes       []string
	Pattern        string
	Regex          string
	MinSize        *int64
	MaxSize        *int64
	ModifiedAfter  *time.Time
	ModifiedBefore *time.Time
}

// SnapshotRestoreRequest is the frozen v1.13.9 active request for
// Engine.SnapshotRestore.
//
// Safety invariant: Restore must never write outside the intended destination.
type SnapshotRestoreRequest struct {
	SnapshotID string
	// Paths scopes a partial restore; empty means restore all snapshot files.
	Paths []string
	// Selection applies query-style restore filters without narrowing repeated
	// exact paths or repeated prefixes to a single value.
	Selection SnapshotRestoreSelection
	// Destination is the explicit restore destination contract.
	Destination SnapshotRestoreDestination
	// Overwrite permits overwriting existing files.
	Overwrite bool
	// Metadata controls best-effort, strict, or disabled metadata behavior.
	Metadata SnapshotRestoreMetadataMode
}

// SnapshotRestoreResult is the frozen v1.13.9 active result for
// Engine.SnapshotRestore.
type SnapshotRestoreResult struct {
	SnapshotID          string
	DestinationMode     SnapshotRestoreDestinationMode
	RequestedPathsCount int
	RestoredFiles       int64
	OutputTarget        string
	OutputPaths         []string
	Warnings            []SnapshotRestoreWarning
}

// SnapshotDeleteMode is the frozen v1.13.9 snapshot-delete mode enum.
type SnapshotDeleteMode string

const (
	SnapshotDeleteModePreview SnapshotDeleteMode = "preview"
	SnapshotDeleteModeExecute SnapshotDeleteMode = "execute"
)

// SnapshotDeleteParentState distinguishes no parent, present parent, and
// recorded-but-missing parent in the frozen v1.13.9 delete contract.
type SnapshotDeleteParentState string

const (
	SnapshotDeleteParentNone    SnapshotDeleteParentState = "none"
	SnapshotDeleteParentPresent SnapshotDeleteParentState = "present"
	SnapshotDeleteParentMissing SnapshotDeleteParentState = "missing"
)

// SnapshotDeleteParent is the renderer-neutral parent-state surface frozen in
// v1.13.9 Phase 3.
type SnapshotDeleteParent struct {
	ID    string
	State SnapshotDeleteParentState
}

// SnapshotDeletePreviewResult is the renderer-neutral delete-preview shape
// frozen in v1.13.9 Phase 3.
type SnapshotDeletePreviewResult struct {
	Parent      SnapshotDeleteParent
	Children    []string
	TotalFiles  int64
	UniqueFiles int64
	SharedFiles int64
}

// SnapshotDeleteRequest is the frozen v1.13.9 active request for
// Engine.SnapshotDelete.
//
// Safety invariant: Snapshot operations must preserve immutability and
// retention semantics. Deleting a snapshot removes only its metadata; content
// referenced by other snapshots or the current state must be retained.
type SnapshotDeleteRequest struct {
	SnapshotID string
	Mode       SnapshotDeleteMode
}

// SnapshotDeleteResult is the frozen v1.13.9 active result for
// Engine.SnapshotDelete.
type SnapshotDeleteResult struct {
	SnapshotID string
	Mode       SnapshotDeleteMode
	Deleted    bool
	Preview    *SnapshotDeletePreviewResult
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

// RepairRequest is a candidate-only request contract for a future Repair
// operation. Repair is not a method on the current Engine interface.
//
// Request/result presence must not be mistaken for active engine ownership.
// Phase 14 and the Phase 16 honesty proof confirmed current CLI/domain
// ownership. Any early-v2.0 activation design must be explicit and
// behavior-preserving.
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
	// Batch-input parsing remains caller-side under current ownership and would
	// require an explicit decision if Repair were activated.
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

// RepairResult is a candidate-only result contract for a future Repair
// operation. Repair is not a method on the current Engine interface.
// Phase 14 and the Phase 16 honesty proof confirmed current CLI/domain
// ownership; any early-v2.0 activation design must be explicit and
// behavior-preserving.
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

// RecoverRequest is a candidate-only request contract for a future corrective
// Recover operation. Recover is not a method on the current Engine interface.
//
// Request/result presence must not be mistaken for active engine ownership.
// Phase 14 and the Phase 16 honesty proof confirmed current CLI/domain
// ownership. Any early-v2.0 activation design must be explicit and
// behavior-preserving.
//
// Safety invariant: Recovery must not legitimize corrupt mappings. Recovery is
// a corrective integrity pass (abort dangling writes, clear stale sealing
// markers, quarantine corrupt/orphaned data), NOT a restore. The previous
// placeholder modeled it like a restore; that was incorrect.
type RecoverRequest struct {
	// DryRun reports what recovery would do without mutating.
	DryRun bool
}

// RecoverResult is a candidate-only result contract for a future Recover
// operation. Recover is not a method on the current Engine interface.
// Phase 14 and the Phase 16 honesty proof confirmed current CLI/domain
// ownership; any early-v2.0 activation design must be explicit and
// behavior-preserving.
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
