package observability

import "time"

type OutputFormat string

const (
	OutputHuman OutputFormat = "human"
	OutputJSON  OutputFormat = "json"
)

type EntityType string

const (
	EntityRepository   EntityType = "repository"
	EntityFile         EntityType = "file"
	EntityLogicalFile  EntityType = "logical_file"
	EntityPhysicalFile EntityType = "physical_file"
	EntityChunk        EntityType = "chunk"
	EntityContainer    EntityType = "container"
	EntitySnapshot     EntityType = "snapshot"
)

type RelationDirection string

const (
	RelationOutgoing RelationDirection = "outgoing"
	RelationIncoming RelationDirection = "incoming"
)

type Relation struct {
	Type       string            `json:"type"`
	Direction  RelationDirection `json:"direction"`
	TargetType EntityType        `json:"target_type"`
	TargetID   string            `json:"target_id"`
	Metadata   map[string]any    `json:"metadata,omitempty"`
}

type StatsResult struct {
	GeneratedAtUTC time.Time `json:"generated_at_utc"`

	Repository RepositoryStats `json:"repository"`
	Logical    LogicalStats    `json:"logical"`
	Physical   PhysicalStats   `json:"physical"`
	Chunks     ChunkStats      `json:"chunks"`
	Containers ContainerStats  `json:"containers"`
	Efficiency EfficiencyStats `json:"efficiency"`
	Snapshots  SnapshotStats   `json:"snapshots"`
	Retention  RetentionStats  `json:"retention"`
	Graph      GraphStats      `json:"graph"`

	Warnings []ObservationWarning `json:"warnings,omitempty"`
}

type RepositoryStats struct {
	ActiveWriteChunker string `json:"active_write_chunker"`
}

type LogicalStats struct {
	TotalFiles         int64 `json:"total_files"`
	CompletedFiles     int64 `json:"completed_files"`
	ProcessingFiles    int64 `json:"processing_files"`
	AbortedFiles       int64 `json:"aborted_files"`
	TotalSizeBytes     int64 `json:"total_size_bytes"`
	CompletedSizeBytes int64 `json:"completed_size_bytes"`
	// EstimatedDedupRatioPct is a legacy aggregate signal from maintenance stats.
	// Prefer EfficiencyStats for exact metadata-derived dedup/overhead values.
	EstimatedDedupRatioPct float64 `json:"estimated_dedup_ratio_pct"`
}

type PhysicalStats struct {
	TotalPhysicalFiles int64 `json:"total_physical_files"`
}

type ChunkStats struct {
	TotalChunks      int64            `json:"total_chunks"`
	CompletedChunks  int64            `json:"completed_chunks"`
	CompletedBytes   int64            `json:"completed_bytes"`
	CountsByVersion  map[string]int64 `json:"-"`
	BytesByVersion   map[string]int64 `json:"-"`
	ChunkerVersions  []VersionStat    `json:"chunker_versions,omitempty"`
	TotalReferences  int64            `json:"total_references"`
	UniqueReferenced int64            `json:"unique_referenced"`
}

type VersionStat struct {
	Version string `json:"version"`
	Chunks  int64  `json:"chunks"`
	Bytes   int64  `json:"bytes"`
}

type ContainerStats struct {
	TotalContainers       int64                 `json:"total_containers"`
	HealthyContainers     int64                 `json:"healthy_containers"`
	QuarantineContainers  int64                 `json:"quarantine_containers"`
	TotalBytes            int64                 `json:"total_bytes"`
	HealthyBytes          int64                 `json:"healthy_bytes"`
	QuarantineBytes       int64                 `json:"quarantine_bytes"`
	LiveBlockBytes        int64                 `json:"live_block_bytes"`
	DeadBlockBytes        int64                 `json:"dead_block_bytes"`
	FragmentationRatioPct float64               `json:"fragmentation_ratio_pct"`
	Records               []ContainerStatRecord `json:"records,omitempty"`
}

type ContainerStatRecord struct {
	ID           int64   `json:"id"`
	Filename     string  `json:"filename"`
	TotalBytes   int64   `json:"total_bytes"`
	LiveBytes    int64   `json:"live_bytes"`
	DeadBytes    int64   `json:"dead_bytes"`
	Quarantine   bool    `json:"quarantine"`
	LiveRatioPct float64 `json:"live_ratio_pct"`
}

type EfficiencyStats struct {
	LogicalBytes     int64 `json:"logical_bytes"`
	UniqueChunkBytes int64 `json:"unique_chunk_bytes"`
	ContainerBytes   int64 `json:"container_bytes"`
	// DedupRatio is metadata-derived logical-to-unique multiplier (e.g. 2.29x).
	DedupRatio float64 `json:"dedup_ratio"`
	// DedupRatioPercent is metadata-derived dedup savings percentage.
	DedupRatioPercent float64 `json:"dedup_ratio_percent"`
	// ContainerOverheadPct captures container overhead against unique chunk bytes.
	ContainerOverheadPct float64 `json:"container_overhead_pct"`
	// StorageOverheadPct is a deprecated alias kept for backward compatibility.
	StorageOverheadPct float64 `json:"storage_overhead_pct"`
}

type SnapshotStats struct {
	TotalSnapshots int64 `json:"total_snapshots"`
}

type RetentionStats struct {
	CurrentOnlyLogicalFiles        int64 `json:"current_only_logical_files"`
	CurrentOnlyBytes               int64 `json:"current_only_bytes"`
	SnapshotReferencedLogicalFiles int64 `json:"snapshot_referenced_logical_files"`
	SnapshotReferencedBytes        int64 `json:"snapshot_referenced_bytes"`
	SnapshotOnlyLogicalFiles       int64 `json:"snapshot_only_logical_files"`
	SnapshotOnlyBytes              int64 `json:"snapshot_only_bytes"`
	SharedLogicalFiles             int64 `json:"shared_logical_files"`
	SharedBytes                    int64 `json:"shared_bytes"`
}

type GraphStats struct {
	SnapshotReachableChunks int64 `json:"snapshot_reachable_chunks"`
	SnapshotReachableBytes  int64 `json:"snapshot_reachable_bytes"`
}

type InspectResult struct {
	GeneratedAtUTC time.Time            `json:"generated_at_utc"`
	EntityType     EntityType           `json:"entity_type"`
	EntityID       string               `json:"entity_id"`
	Summary        map[string]any       `json:"summary"`
	Metadata       map[string]any       `json:"metadata,omitempty"`
	Relations      []Relation           `json:"relations,omitempty"`
	Warnings       []ObservationWarning `json:"warnings,omitempty"`
}

type SimulationResult struct {
	GeneratedAtUTC time.Time `json:"generated_at_utc"`
	Kind           string    `json:"kind"`
	Exact          bool      `json:"exact"`
	Mutated        bool      `json:"mutated"`

	Summary  map[string]any       `json:"summary"`
	GC       *GCSimulationResult  `json:"gc,omitempty"`
	Warnings []ObservationWarning `json:"warnings,omitempty"`
}

// GCSimulationResult carries the structured output of a GC dry-run plan.
type GCSimulationResult struct {
	TotalChunks                int64                       `json:"total_chunks"`
	ReachableChunks            int64                       `json:"reachable_chunks"`
	UnreachableChunks          int64                       `json:"unreachable_chunks"`
	LogicallyReclaimableBytes  int64                       `json:"logically_reclaimable_bytes"`
	PhysicallyReclaimableBytes int64                       `json:"physically_reclaimable_bytes"`
	FullyReclaimableContainers int64                       `json:"fully_reclaimable_containers"`
	PartiallyDeadContainers    int64                       `json:"partially_dead_containers"`
	AffectedContainers         []ContainerSimulationImpact `json:"affected_containers"`
}

// ContainerSimulationImpact is the per-container summary within a GC simulation.
type ContainerSimulationImpact struct {
	ContainerID        int64  `json:"container_id"`
	Filename           string `json:"filename"`
	TotalBytes         int64  `json:"total_bytes"`
	LiveBytesAfterGC   int64  `json:"live_bytes_after_gc"`
	ReclaimableBytes   int64  `json:"reclaimable_bytes"`
	ReclaimableChunks  int64  `json:"reclaimable_chunks"`
	TotalChunks        int64  `json:"total_chunks"`
	FullyReclaimable   bool   `json:"fully_reclaimable"`
	RequiresCompaction bool   `json:"requires_compaction"`
}

type ObservationWarning struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
