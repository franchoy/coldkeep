package engine

import "time"

// ValueKind identifies the exact neutral representation stored in Value.
type ValueKind string

const (
	ValueNull    ValueKind = "null"
	ValueBoolean ValueKind = "boolean"
	ValueString  ValueKind = "string"
	ValueInteger ValueKind = "integer"
	ValueDecimal ValueKind = "decimal"
	ValueObject  ValueKind = "object"
	ValueArray   ValueKind = "array"
)

// Value is a recursive, renderer-neutral representation for dynamic inspect
// and trace metadata. Integer and decimal text preserve exact numeric tokens
// across the engine boundary.
type Value struct {
	Kind    ValueKind
	Boolean bool
	String  string
	Integer string
	Decimal string
	Object  map[string]Value
	Array   []Value
}

// TraceEvent is a sanitized, ordered diagnostic event returned by an engine
// operation. Callers decide whether and how to render it.
type TraceEvent struct {
	Step     string
	Entity   string
	EntityID string
	Message  string
	Metadata map[string]Value
}

// StatsRequest carries parameters for the Stats operation.
type StatsRequest struct {
	IncludeContainers bool
	IncludeTrace      bool
}

// StatsResult is the complete neutral repository-statistics result.
type StatsResult struct {
	GeneratedAtUTC time.Time
	Repository     StatsRepository
	Logical        StatsLogical
	Physical       StatsPhysical
	Chunks         StatsChunks
	BlockLayout    StatsBlockLayout
	Containers     StatsContainers
	Efficiency     StatsEfficiency
	Snapshots      StatsSnapshots
	Retention      StatsRetention
	Graph          StatsGraph
	Warnings       []OperationWarning
	Trace          []TraceEvent
}

type StatsRepository struct {
	ActiveWriteChunker string
}

type StatsLogical struct {
	TotalFiles             int64
	CompletedFiles         int64
	ProcessingFiles        int64
	AbortedFiles           int64
	TotalSizeBytes         int64
	CompletedSizeBytes     int64
	EstimatedDedupRatioPct float64
}

type StatsPhysical struct {
	TotalPhysicalFiles int64
}

type StatsChunks struct {
	TotalChunks      int64
	CompletedChunks  int64
	CompletedBytes   int64
	CountsByVersion  map[string]int64
	BytesByVersion   map[string]int64
	ChunkerVersions  []StatsVersion
	TotalReferences  int64
	UniqueReferenced int64
}

type StatsBlockLayout struct {
	StorageBlocksCount        int64
	ChunkBlockRefsCount       int64
	AvgChunksPerBlock         float64
	AvgBlockPlaintextSize     float64
	AvgBlockStoredSize        float64
	LogicalBytes              int64
	CompressedBytes           int64
	StoredBytes               int64
	CompressionSizeRatio      float64
	CompressionFactor         float64
	PhysicalSizeRatio         float64
	PhysicalFactor            float64
	CompressedBlocks          int64
	UncompressedBlocks        int64
	CompressionCodecBreakdown map[string]int64
	AvgBlockFillRatio         float64
	LegacyBlockCount          int64
	PackedBlockCount          int64
	CodecDistribution         map[string]int64
}

type StatsVersion struct {
	Version string
	Chunks  int64
	Bytes   int64
}

type StatsContainers struct {
	TotalContainers       int64
	HealthyContainers     int64
	QuarantineContainers  int64
	TotalBytes            int64
	HealthyBytes          int64
	QuarantineBytes       int64
	LiveBlockBytes        int64
	DeadBlockBytes        int64
	FragmentationRatioPct float64
	Records               []StatsContainerRecord
}

type StatsContainerRecord struct {
	ID           int64
	Filename     string
	TotalBytes   int64
	LiveBytes    int64
	DeadBytes    int64
	Quarantine   bool
	LiveRatioPct float64
}

type StatsEfficiency struct {
	LogicalBytes         int64
	UniqueChunkBytes     int64
	ContainerBytes       int64
	DedupRatio           float64
	DedupRatioPercent    float64
	ContainerOverheadPct float64
	StorageOverheadPct   float64
}

type StatsSnapshots struct {
	TotalSnapshots int64
}

type StatsRetention struct {
	CurrentOnlyLogicalFiles        int64
	CurrentOnlyBytes               int64
	SnapshotReferencedLogicalFiles int64
	SnapshotReferencedBytes        int64
	SnapshotOnlyLogicalFiles       int64
	SnapshotOnlyBytes              int64
	SharedLogicalFiles             int64
	SharedBytes                    int64
}

type StatsGraph struct {
	SnapshotReachableChunks int64
	SnapshotReachableBytes  int64
}

// InspectEntity identifies a supported inspection target without importing
// the observability implementation contract.
type InspectEntity string

const (
	InspectRepository   InspectEntity = "repository"
	InspectFile         InspectEntity = "file"
	InspectLogicalFile  InspectEntity = "logical_file"
	InspectPhysicalFile InspectEntity = "physical_file"
	InspectChunk        InspectEntity = "chunk"
	InspectContainer    InspectEntity = "container"
	InspectSnapshot     InspectEntity = "snapshot"
)

type RelationDirection string

const (
	RelationOutgoing RelationDirection = "outgoing"
	RelationIncoming RelationDirection = "incoming"
)

type InspectOptions struct {
	Deep         bool
	Relations    bool
	Reverse      bool
	Limit        int
	IncludeTrace bool
}

type InspectRequest struct {
	Entity   InspectEntity
	EntityID string
	Options  InspectOptions
}

type InspectRelation struct {
	Type       string
	Direction  RelationDirection
	TargetType InspectEntity
	TargetID   string
	Metadata   map[string]Value
}

type InspectResult struct {
	GeneratedAtUTC time.Time
	Entity         InspectEntity
	EntityID       string
	Summary        map[string]Value
	Metadata       map[string]Value
	Relations      []InspectRelation
	Warnings       []OperationWarning
	Trace          []TraceEvent
}

// VerifyRequest carries parameters for the Verify operation.
type VerifyRequest struct {
	Level  string
	Target string
	FileID int
}

// VerifyResult includes the complete stable summary required by the existing
// CLI after successful verification.
type VerifyResult struct {
	BlocksChecked           int64
	PhysicalHashChecked     int64
	CompressedHashChecked   int64
	LogicalHashChecked      int64
	CompressedBlocksChecked int64
}
