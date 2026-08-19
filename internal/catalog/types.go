package catalog

import "time"

type LogicalFileRef struct {
	ID           int64
	OriginalName string
	TotalSize    int64
	FileHash     string
	RefCount     int
	Status       string
}

// PhysicalFileRef preserves the historical lookup projection: Mode is zero
// when the database value is NULL. RestoreSourceRef preserves exact nullness.
type PhysicalFileRef struct {
	Path               string
	LogicalFileID      int64
	Mode               int
	MTime              *time.Time
	IsMetadataComplete bool
}

const MaxCurrentFilePageSize int64 = 10000

// CurrentFileRef is one completed current-state physical path joined to its
// logical identity. Snapshot-only and non-completed logical files are excluded.
type CurrentFileRef struct {
	LogicalFileID int64
	Path          string
	FileHash      string
	SizeBytes     int64
	CreatedAt     time.Time
}

type CurrentFilePage struct {
	Limit  *int64
	Offset *int64
}

// CurrentFileSearch preserves repeated CLI filters. Repeated name and size
// constraints are combined with AND, matching the historical query behavior.
type CurrentFileSearch struct {
	NameContains []string
	MinSizeBytes []int64
	MaxSizeBytes []int64
	Page         CurrentFilePage
}

type RepositoryConfigurationRef struct {
	Key    string
	Value  string
	Exists bool
}

type SetRepositoryConfigurationResult struct {
	Key           string
	Value         string
	PreviousValue string
	PreviouslySet bool
	Changed       bool
}

type SnapshotRef struct {
	ID        string
	Type      string
	Label     string
	ParentID  string
	CreatedAt time.Time
}

type SnapshotFilter struct {
	Type           string
	LabelSubstring string
	Since          *time.Time
	Until          *time.Time
	Limit          int
}

// SnapshotParentState distinguishes roots from historical missing-parent rows.
// Missing parents never create invented edges and do not turn children into roots.
type SnapshotParentState string

const (
	SnapshotParentNone    SnapshotParentState = "none"
	SnapshotParentPresent SnapshotParentState = "present"
	SnapshotParentMissing SnapshotParentState = "missing"
)

// SnapshotGraphNode contains child IDs ordered by created_at, then ID.
type SnapshotGraphNode struct {
	Snapshot    SnapshotRef
	ParentState SnapshotParentState
	ChildIDs    []string
}

// SnapshotGraph nodes and roots are ordered by created_at ascending, then ID.
type SnapshotGraph struct {
	Nodes   []SnapshotGraphNode
	RootIDs []string
}

// ReachabilityRoots is the compatibility shape used by existing callers.
type ReachabilityRoots struct {
	Current  map[int64]struct{}
	Snapshot map[int64]struct{}
}

type PlacementKind string

const (
	PlacementLegacy PlacementKind = "legacy"
	PlacementPacked PlacementKind = "packed"
)

// ContainerPlacementRef contains the facts required for bounded reads.
type ContainerPlacementRef struct {
	ID            int64
	Filename      string
	Sealed        bool
	Sealing       bool
	ContainerHash string
	Quarantined   bool
	CurrentSize   int64
	MaxSize       int64
}

type LegacyChunkPlacement struct {
	BlockID         int64
	Codec           string
	FormatVersion   int
	PlaintextSize   int64
	StoredSize      int64
	Nonce           []byte
	Container       ContainerPlacementRef
	ContainerOffset int64
}

type PackedChunkPlacement struct {
	BlockID          int64
	FormatVersion    int
	Codec            string
	PlaintextSize    int64
	CompressionCodec string
	CompressionLevel *int
	CompressedSize   *int64
	StoredSize       int64
	BlockHash        []byte
	CompressedHash   []byte
	PhysicalHash     []byte
	Container        ContainerPlacementRef
	ContainerOffset  int64
	OffsetInBlock    int64
	SizeInBlock      int64
}

// ChunkPlacementRef is one ordered logical-file recipe entry. Kind and the
// placement pointers form a strict tagged union. Results are contiguous from 0.
type ChunkPlacementRef struct {
	ChunkOrder     int64
	ChunkID        int64
	ChunkHash      string
	ChunkSize      int64
	ChunkerVersion string
	ChunkStatus    string
	Kind           PlacementKind
	Legacy         *LegacyChunkPlacement
	Packed         *PackedChunkPlacement
}

type RestoreSelector string

const (
	RestoreByFileID       RestoreSelector = "file_id"
	RestoreByStoredPath   RestoreSelector = "stored_path"
	RestoreBySnapshotPath RestoreSelector = "snapshot_path"
)

// RestorePlanInput is a strict selector union: FileID; StoredPath; or
// SnapshotID plus SnapshotPath, selected by Selector.
type RestorePlanInput struct {
	Selector     RestoreSelector
	FileID       int64
	StoredPath   string
	SnapshotID   string
	SnapshotPath string
}

type RestoreLogicalFileRef struct {
	ID             int64
	OriginalName   string
	TotalSize      int64
	FileHash       string
	ChunkerVersion string
	Status         string
}

// RestoreSourceRef preserves nullable current-file and snapshot metadata.
type RestoreSourceRef struct {
	StoredPath         string
	SnapshotID         string
	SnapshotPath       string
	Size               *int64
	Mode               *int64
	MTime              *time.Time
	UID                *int64
	GID                *int64
	IsMetadataComplete bool
}

// RestorePlanMetadata is a complete immutable recipe; payload I/O stays below
// the catalog boundary.
type RestorePlanMetadata struct {
	Selector    RestorePlanInput
	LogicalFile RestoreLogicalFileRef
	Source      RestoreSourceRef
	Placements  []ChunkPlacementRef
}

type GCPlanInput struct {
	ExcludeSnapshotIDs []string
}

// GCReachabilityRoot records all reasons a logical file remains protected.
type GCReachabilityRoot struct {
	LogicalFileID int64
	Current       bool
	SnapshotIDs   []string
}

// GCPlanMetadata contains deterministic mark roots, never sweep instructions.
// Roots are ordered by logical ID; snapshots by created_at, then ID.
type GCPlanMetadata struct {
	Roots              []GCReachabilityRoot
	ProtectedSnapshots []SnapshotRef
}
