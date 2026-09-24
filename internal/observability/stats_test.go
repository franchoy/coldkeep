package observability

import (
	"context"
	"database/sql"
	"encoding/json"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/tests/testdb"
)

type traceCollectorSink struct {
	events []TraceEvent
}

func (s *traceCollectorSink) Event(event TraceEvent) {
	s.events = append(s.events, event)
}

func TestMapStatsResultMapsMaintenanceResultToStableModel(t *testing.T) {
	fixedNow := time.Date(2026, time.April, 26, 10, 0, 0, 0, time.UTC)

	raw := &maintenance.StatsResult{
		TotalFiles:               7,
		CompletedFiles:           5,
		ProcessingFiles:          1,
		AbortedFiles:             1,
		TotalLogicalSizeBytes:    700,
		CompletedSizeBytes:       500,
		EstimatedDedupRatioPct:   42.5,
		ActiveWriteChunker:       "v2-fastcdc",
		TotalChunks:              11,
		CompletedChunks:          9,
		CompletedChunkBytes:      450,
		ChunkCountsByVersion:     map[string]int64{"v2-fastcdc": 11},
		ChunkBytesByVersion:      map[string]int64{"v2-fastcdc": 450},
		TotalChunkReferences:     30,
		UniqueReferencedChunks:   11,
		TotalContainers:          3,
		HealthyContainers:        2,
		QuarantineContainers:     1,
		TotalContainerBytes:      900,
		HealthyContainerBytes:    800,
		QuarantineContainerBytes: 100,
		LiveBlockBytes:           400,
		DeadBlockBytes:           50,
		FragmentationRatioPct:    6.25,
		Containers: []maintenance.ContainerStatRecord{
			{
				ID:           10,
				Filename:     "container_10.bin",
				TotalBytes:   400,
				LiveBytes:    300,
				DeadBytes:    100,
				Quarantine:   false,
				LiveRatioPct: 75,
			},
		},
		SnapshotRetention: maintenance.SnapshotRetentionStats{
			CurrentOnlyLogicalFiles:        1,
			CurrentOnlyBytes:               10,
			SnapshotReferencedLogicalFiles: 3,
			SnapshotReferencedBytes:        60,
			SnapshotOnlyLogicalFiles:       2,
			SnapshotOnlyBytes:              50,
			SharedLogicalFiles:             1,
			SharedBytes:                    10,
		},
		BlockStats: maintenance.BlockStats{
			StorageBlocks:             4,
			ChunkBlockRefs:            10,
			AvgChunksPerBlock:         2.5,
			AvgPlaintextSize:          900,
			AvgStoredSize:             860,
			LogicalBytes:              3600,
			CompressedBytes:           2800,
			StoredBytes:               3440,
			CompressionSizeRatio:      0.7777777778,
			CompressionFactor:         1.2857142857,
			PhysicalSizeRatio:         0.9555555556,
			PhysicalFactor:            1.0465116279,
			CompressedBlocks:          3,
			UncompressedBlocks:        1,
			FillRatio:                 0.88,
			LegacyBlocks:              2,
			PackedBlocks:              4,
			CodecDistribution:         map[string]int64{"none": 4},
			CompressionCodecBreakdown: map[string]int64{"none": 1, "zstd": 3},
		},
	}

	result := mapStatsResult(fixedNow, raw)

	if result.GeneratedAtUTC != fixedNow {
		t.Fatalf("generated_at_utc mismatch: got %s want %s", result.GeneratedAtUTC, fixedNow)
	}
	if result.Repository.ActiveWriteChunker != "v2-fastcdc" {
		t.Fatalf("unexpected active_write_chunker: %q", result.Repository.ActiveWriteChunker)
	}
	if result.Logical.TotalFiles != 7 || result.Logical.CompletedFiles != 5 {
		t.Fatalf("unexpected logical stats: %+v", result.Logical)
	}
	if result.Chunks.TotalReferences != 30 || result.Chunks.UniqueReferenced != 11 {
		t.Fatalf("unexpected chunk refs: %+v", result.Chunks)
	}
	if len(result.Containers.Records) != 1 {
		t.Fatalf("expected one container record, got %d", len(result.Containers.Records))
	}
	withoutRecords := (&Service{now: func() time.Time { return fixedNow }}).mapMaintenanceStats(raw, StatsOptions{})
	if len(withoutRecords.Containers.Records) != 0 {
		t.Fatalf("expected no container records when include option is false, got %d", len(withoutRecords.Containers.Records))
	}
	if result.Physical.TotalPhysicalFiles != 0 {
		t.Fatalf("expected phase-1 default physical total=0, got %d", result.Physical.TotalPhysicalFiles)
	}
	if result.Retention.SnapshotOnlyLogicalFiles != 2 {
		t.Fatalf("unexpected retention stats: %+v", result.Retention)
	}
	if result.BlockLayout.StorageBlocksCount != 4 || result.BlockLayout.ChunkBlockRefsCount != 10 {
		t.Fatalf("unexpected block layout stats: %+v", result.BlockLayout)
	}
	if result.BlockLayout.LogicalBytes != 3600 || result.BlockLayout.CompressedBytes != 2800 || result.BlockLayout.StoredBytes != 3440 {
		t.Fatalf("unexpected block layout byte aggregates: %+v", result.BlockLayout)
	}
	if result.BlockLayout.CompressedBlocks != 3 || result.BlockLayout.UncompressedBlocks != 1 {
		t.Fatalf("unexpected block layout compressed/uncompressed counts: %+v", result.BlockLayout)
	}
	assertFloatApprox(t, result.BlockLayout.CompressionSizeRatio, 0.7777777778, 1e-9, "block layout compression_size_ratio")
	assertFloatApprox(t, result.BlockLayout.CompressionFactor, 1.2857142857, 1e-9, "block layout compression_factor")
	assertFloatApprox(t, result.BlockLayout.PhysicalSizeRatio, 0.9555555556, 1e-9, "block layout physical_size_ratio")
	assertFloatApprox(t, result.BlockLayout.PhysicalFactor, 1.0465116279, 1e-9, "block layout physical_factor")
	if got := result.BlockLayout.CodecDistribution["none"]; got != 4 {
		t.Fatalf("unexpected block layout codec distribution: %+v", result.BlockLayout.CodecDistribution)
	}
	if got := result.BlockLayout.CompressionCodecBreakdown["none"]; got != 1 {
		t.Fatalf("unexpected block layout compression codec distribution: %+v", result.BlockLayout.CompressionCodecBreakdown)
	}
	if got := result.BlockLayout.CompressionCodecBreakdown["zstd"]; got != 3 {
		t.Fatalf("unexpected block layout compression codec distribution: %+v", result.BlockLayout.CompressionCodecBreakdown)
	}
	if result.Efficiency.LogicalBytes != 500 {
		t.Fatalf("unexpected efficiency logical_bytes: %d", result.Efficiency.LogicalBytes)
	}
	if result.Efficiency.UniqueChunkBytes != 450 {
		t.Fatalf("unexpected efficiency unique_chunk_bytes: %d", result.Efficiency.UniqueChunkBytes)
	}
	if result.Efficiency.ContainerBytes != 900 {
		t.Fatalf("unexpected efficiency container_bytes: %d", result.Efficiency.ContainerBytes)
	}
	assertFloatApprox(t, result.Efficiency.DedupRatio, 500.0/450.0, 1e-9, "efficiency dedup_ratio")
	assertFloatApprox(t, result.Efficiency.DedupRatioPercent, 10.0, 1e-9, "efficiency dedup_ratio_percent")
	if result.Efficiency.ContainerOverheadPct != 100 {
		t.Fatalf("unexpected efficiency container_overhead_pct: %f", result.Efficiency.ContainerOverheadPct)
	}
	if result.Efficiency.StorageOverheadPct != 100 {
		t.Fatalf("unexpected efficiency storage_overhead_pct: %f", result.Efficiency.StorageOverheadPct)
	}
}

func TestMapStatsResultHandlesNilMaintenanceResult(t *testing.T) {
	result := mapStatsResult(time.Date(2026, time.April, 26, 11, 0, 0, 0, time.UTC), nil)
	if result.GeneratedAtUTC.IsZero() {
		t.Fatal("expected generated_at_utc to be populated")
	}
	if result.Chunks.CountsByVersion != nil {
		t.Fatal("expected zero-value result when maintenance payload is nil")
	}
}

func TestBuildEfficiencyStatsHandlesZeroDenominators(t *testing.T) {
	result := buildEfficiencyStats(0, 0, 123)
	if result.LogicalBytes != 0 || result.UniqueChunkBytes != 0 || result.ContainerBytes != 123 {
		t.Fatalf("unexpected passthrough values: %+v", result)
	}
	if result.DedupRatio != 0 {
		t.Fatalf("expected zero dedup ratio, got %f", result.DedupRatio)
	}
	if result.DedupRatioPercent != 0 {
		t.Fatalf("expected zero dedup ratio percent, got %f", result.DedupRatioPercent)
	}
	if result.StorageOverheadPct != 0 {
		t.Fatalf("expected zero storage overhead pct, got %f", result.StorageOverheadPct)
	}
	if result.ContainerOverheadPct != 0 {
		t.Fatalf("expected zero container overhead pct, got %f", result.ContainerOverheadPct)
	}
}

func TestBuildVersionStatsSortedAndComplete(t *testing.T) {
	stats := buildVersionStats(
		map[string]int64{"v2-fastcdc": 2, "unknown": 1},
		map[string]int64{"v1-simple-rolling": 30, "v2-fastcdc": 20, "unknown": 9},
	)

	if len(stats) != 3 {
		t.Fatalf("expected 3 version stats, got %d", len(stats))
	}

	if stats[0].Version != "unknown" || stats[0].Chunks != 1 || stats[0].Bytes != 9 {
		t.Fatalf("unexpected first version stat: %+v", stats[0])
	}
	if stats[1].Version != "v1-simple-rolling" || stats[1].Chunks != 0 || stats[1].Bytes != 30 {
		t.Fatalf("unexpected second version stat: %+v", stats[1])
	}
	if stats[2].Version != "v2-fastcdc" || stats[2].Chunks != 2 || stats[2].Bytes != 20 {
		t.Fatalf("unexpected third version stat: %+v", stats[2])
	}
}

func TestStatsReturnsErrorWhenDBIsMissing(t *testing.T) {
	svc := newServiceForTest(nil, func() time.Time { return time.Now().UTC() })

	_, err := svc.Stats(context.TODO(), StatsOptions{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestNewServiceStoresInjectedDB(t *testing.T) {
	dbconn := &sql.DB{}
	svc, err := NewService(dbconn)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if svc.db != dbconn {
		t.Fatal("service db was not injected")
	}
}

func TestStatsDelegatesAndMapsMaintenanceStats(t *testing.T) {
	dbconn := openInspectTestDB(t)

	lfRes, err := dbconn.Exec(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`,
		"alpha.txt", 123, "hash-alpha", "COMPLETED", "v2-fastcdc",
	)
	if err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}
	logicalFileID, err := lfRes.LastInsertId()
	if err != nil {
		t.Fatalf("logical_file last insert id: %v", err)
	}

	insertSimPhysicalFile(t, dbconn, "/data/alpha.txt", logicalFileID)

	if _, err := dbconn.Exec(
		`INSERT INTO snapshot (id, created_at, type) VALUES (?, ?, ?)`,
		"snap-stats", time.Now().UTC(), "full",
	); err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
	testdb.InsertSnapshotFileRef(t, dbconn, "snap-stats", "snap/alpha.txt", logicalFileID)

	ctrRes, err := dbconn.Exec(`INSERT INTO container (filename, current_size, max_size, quarantine) VALUES (?, ?, ?, ?)`, "ctr_1.bin", 512, 1024, 0)
	if err != nil {
		t.Fatalf("insert container: %v", err)
	}
	containerID, err := ctrRes.LastInsertId()
	if err != nil {
		t.Fatalf("container last insert id: %v", err)
	}

	chunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "chunk-alpha", 64, "COMPLETED", 1, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert chunk: %v", err)
	}
	chunkID, err := chunkRes.LastInsertId()
	if err != nil {
		t.Fatalf("chunk last insert id: %v", err)
	}

	if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES (?, ?, ?)`, logicalFileID, chunkID, 0); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}
	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		chunkID, "plain", 1, 64, 64, containerID, 0,
	); err != nil {
		t.Fatalf("insert block: %v", err)
	}

	svc := newServiceForTest(dbconn, func() time.Time {
		return time.Date(2026, time.April, 26, 16, 0, 0, 0, time.UTC)
	})

	result, err := svc.Stats(context.Background(), StatsOptions{IncludeContainers: true})
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}

	if result.GeneratedAtUTC.IsZero() {
		t.Fatal("expected generated timestamp")
	}
	if result.Logical.TotalFiles != 1 {
		t.Fatalf("expected total logical files=1, got %d", result.Logical.TotalFiles)
	}
	if len(result.Chunks.CountsByVersion) == 0 {
		t.Fatalf("expected non-empty chunker version map, got %+v", result.Chunks.CountsByVersion)
	}
	if got := result.Chunks.CountsByVersion["v2-fastcdc"]; got != 1 {
		t.Fatalf("expected v2-fastcdc chunk count=1, got %d", got)
	}
	if result.Retention.SnapshotReferencedLogicalFiles == 0 {
		t.Fatalf("expected retention stats to be populated, got %+v", result.Retention)
	}
	if len(result.Containers.Records) != 1 {
		t.Fatalf("expected one container record, got %d", len(result.Containers.Records))
	}
	if result.Snapshots.TotalSnapshots != 1 {
		t.Fatalf("expected snapshot total=1, got %d", result.Snapshots.TotalSnapshots)
	}
	if hasWarningCode(result.Warnings, "snapshot_ids_non_numeric_skipped") {
		t.Fatalf("did not expect non-numeric snapshot id warning, got %+v", result.Warnings)
	}
	if result.Graph.SnapshotReachableChunks != 1 {
		t.Fatalf("expected graph snapshot_reachable_chunks=1, got %d", result.Graph.SnapshotReachableChunks)
	}
	if result.Graph.SnapshotReachableBytes != 64 {
		t.Fatalf("expected graph snapshot_reachable_bytes=64, got %d", result.Graph.SnapshotReachableBytes)
	}
}

func TestPhase8DefectAnchorPhysicalFileCountOneMapping(t *testing.T) {
	dbconn := openInspectTestDB(t)
	logicalResult, err := dbconn.Exec(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version) VALUES (?, ?, ?, ?, ?, ?)`,
		"phase8-one.txt", 1, "phase8-one-hash", "COMPLETED", 1, "v2-fastcdc",
	)
	if err != nil {
		t.Fatalf("insert logical file: %v", err)
	}
	logicalFileID, err := logicalResult.LastInsertId()
	if err != nil {
		t.Fatalf("logical file id: %v", err)
	}
	insertSimPhysicalFile(t, dbconn, "/phase8/one.txt", logicalFileID)

	svc := newServiceForTest(dbconn, time.Now)
	result, err := svc.Stats(context.Background(), StatsOptions{})
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if result.Physical.TotalPhysicalFiles != 1 {
		t.Fatalf("DEFECT_ANCHOR physical.total_physical_files = %d, want 1 current mapping", result.Physical.TotalPhysicalFiles)
	}
}

func TestPhase8DefectAnchorPhysicalFileCountTwoPathsOneLogical(t *testing.T) {
	dbconn := openInspectTestDB(t)
	logicalResult, err := dbconn.Exec(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version) VALUES (?, ?, ?, ?, ?, ?)`,
		"phase8-shared.txt", 1, "phase8-shared-hash", "COMPLETED", 2, "v2-fastcdc",
	)
	if err != nil {
		t.Fatalf("insert logical file: %v", err)
	}
	logicalFileID, err := logicalResult.LastInsertId()
	if err != nil {
		t.Fatalf("logical file id: %v", err)
	}
	insertSimPhysicalFile(t, dbconn, "/phase8/first.txt", logicalFileID)
	insertSimPhysicalFile(t, dbconn, "/phase8/second.txt", logicalFileID)

	svc := newServiceForTest(dbconn, time.Now)
	result, err := svc.Stats(context.Background(), StatsOptions{})
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if result.Physical.TotalPhysicalFiles != 2 {
		t.Fatalf("DEFECT_ANCHOR physical.total_physical_files = %d, want 2 current mappings to one logical file", result.Physical.TotalPhysicalFiles)
	}
}

func TestPhase8PreservationControlEmptyPhysicalFileCount(t *testing.T) {
	dbconn := openInspectTestDB(t)
	svc := newServiceForTest(dbconn, time.Now)
	result, err := svc.Stats(context.Background(), StatsOptions{})
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if result.Physical.TotalPhysicalFiles != 0 {
		t.Fatalf("PRESERVATION_CONTROL physical.total_physical_files = %d, want 0", result.Physical.TotalPhysicalFiles)
	}
}

func TestStatsEnrichesGraphSnapshotReachabilityForNumericSnapshotIDs(t *testing.T) {
	dbconn := openInspectTestDB(t)

	lfRes, err := dbconn.Exec(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`,
		"beta.txt", 42, "hash-beta", "COMPLETED", "v2-fastcdc",
	)
	if err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}
	logicalFileID, err := lfRes.LastInsertId()
	if err != nil {
		t.Fatalf("logical_file last insert id: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO snapshot (id, created_at, type) VALUES (?, ?, ?)`,
		"101", time.Now().UTC(), "full",
	); err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
	testdb.InsertSnapshotFileRef(t, dbconn, "101", "snap/beta.txt", logicalFileID)

	chunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "chunk-beta", 77, "COMPLETED", 1, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert chunk: %v", err)
	}
	chunkID, err := chunkRes.LastInsertId()
	if err != nil {
		t.Fatalf("chunk last insert id: %v", err)
	}

	if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES (?, ?, ?)`, logicalFileID, chunkID, 0); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	svc := newServiceForTest(dbconn, nil)
	result, err := svc.Stats(context.Background(), StatsOptions{})
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}

	if result.Graph.SnapshotReachableChunks != 1 {
		t.Fatalf("expected graph snapshot_reachable_chunks=1, got %d", result.Graph.SnapshotReachableChunks)
	}
	if result.Graph.SnapshotReachableBytes != 77 {
		t.Fatalf("expected graph snapshot_reachable_bytes=77, got %d", result.Graph.SnapshotReachableBytes)
	}
	if hasWarningCode(result.Warnings, "graph_snapshot_reachable_chunks_mismatch") {
		t.Fatalf("unexpected chunks mismatch warning: %+v", result.Warnings)
	}
	if hasWarningCode(result.Warnings, "graph_snapshot_reachable_bytes_mismatch") {
		t.Fatalf("unexpected bytes mismatch warning: %+v", result.Warnings)
	}
}

func TestObservabilityDoesNotMutateState(t *testing.T) {
	dbconn := openInspectTestDB(t)

	lfRes, err := dbconn.Exec(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`,
		"immutable.txt", 100, "hash-immutable", "COMPLETED", "v2-fastcdc",
	)
	if err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}
	logicalFileID, err := lfRes.LastInsertId()
	if err != nil {
		t.Fatalf("logical_file last insert id: %v", err)
	}

	ctrRes, err := dbconn.Exec(`INSERT INTO container (filename, current_size, max_size, quarantine) VALUES (?, ?, ?, ?)`, "ctr_immut.bin", 256, 1024, 0)
	if err != nil {
		t.Fatalf("insert container: %v", err)
	}
	containerID, err := ctrRes.LastInsertId()
	if err != nil {
		t.Fatalf("container last insert id: %v", err)
	}

	chunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "chunk-immut", 40, "COMPLETED", 1, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert chunk: %v", err)
	}
	chunkID, err := chunkRes.LastInsertId()
	if err != nil {
		t.Fatalf("chunk last insert id: %v", err)
	}

	if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES (?, ?, ?)`, logicalFileID, chunkID, 0); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}
	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		chunkID, "plain", 1, 40, 40, containerID, 0,
	); err != nil {
		t.Fatalf("insert block: %v", err)
	}

	beforeLogical := mustCount(t, dbconn, `SELECT COUNT(*) FROM logical_file`)
	beforeChunks := mustCount(t, dbconn, `SELECT COUNT(*) FROM chunk`)
	beforeContainers := mustCount(t, dbconn, `SELECT COUNT(*) FROM container`)

	svc := newServiceForTest(dbconn, nil)
	if _, err := svc.Stats(context.Background(), StatsOptions{IncludeContainers: true}); err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if _, err := svc.Inspect(context.Background(), EntityLogicalFile, strconv.FormatInt(logicalFileID, 10), InspectOptions{}); err != nil {
		t.Fatalf("Inspect: %v", err)
	}

	afterLogical := mustCount(t, dbconn, `SELECT COUNT(*) FROM logical_file`)
	afterChunks := mustCount(t, dbconn, `SELECT COUNT(*) FROM chunk`)
	afterContainers := mustCount(t, dbconn, `SELECT COUNT(*) FROM container`)

	if beforeLogical != afterLogical {
		t.Fatalf("logical_file count mutated: before=%d after=%d", beforeLogical, afterLogical)
	}
	if beforeChunks != afterChunks {
		t.Fatalf("chunk count mutated: before=%d after=%d", beforeChunks, afterChunks)
	}
	if beforeContainers != afterContainers {
		t.Fatalf("container count mutated: before=%d after=%d", beforeContainers, afterContainers)
	}
}

func mustCount(t *testing.T, dbconn *sql.DB, query string, args ...any) int64 {
	t.Helper()
	var count int64
	if err := dbconn.QueryRow(query, args...).Scan(&count); err != nil {
		t.Fatalf("count query failed (%s): %v", query, err)
	}
	return count
}

func hasWarningCode(warnings []ObservationWarning, code string) bool {
	for _, warning := range warnings {
		if warning.Code == code {
			return true
		}
	}
	return false
}

func assertFloatApprox(t *testing.T, got, want, eps float64, label string) {
	t.Helper()
	if math.Abs(got-want) > eps {
		t.Fatalf("unexpected %s: got=%f want=%f eps=%f", label, got, want, eps)
	}
}

func TestStatsReturnsStructuredSections(t *testing.T) {
	dbconn := openInspectTestDB(t)
	svc := newServiceForTest(dbconn, nil)

	result, err := svc.Stats(context.Background(), StatsOptions{})
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.GeneratedAtUTC.IsZero() {
		t.Fatal("expected generated_at_utc to be set")
	}

	if strings.TrimSpace(result.Repository.ActiveWriteChunker) == "" {
		t.Fatalf("expected repository section to include active_write_chunker, got %+v", result.Repository)
	}
	if result.Logical.TotalFiles != 0 || result.Logical.CompletedFiles != 0 {
		t.Fatalf("expected zero logical counts for empty DB, got %+v", result.Logical)
	}
	if result.Chunks.TotalChunks != 0 || result.Chunks.TotalReferences != 0 {
		t.Fatalf("expected zero chunk totals for empty DB, got %+v", result.Chunks)
	}
	if result.Containers.TotalContainers != 0 || result.Containers.TotalBytes != 0 {
		t.Fatalf("expected zero container totals for empty DB, got %+v", result.Containers)
	}
	if result.Retention.CurrentOnlyLogicalFiles != 0 || result.Retention.SharedLogicalFiles != 0 {
		t.Fatalf("expected zero retention section for empty DB, got %+v", result.Retention)
	}
}

func TestStatsIncludesChunkerVersionDistribution(t *testing.T) {
	result := mapStatsResult(time.Now().UTC(), &maintenance.StatsResult{
		ChunkCountsByVersion: map[string]int64{
			"v2-fastcdc":        3,
			"v1-simple-rolling": 1,
		},
		ChunkBytesByVersion: map[string]int64{
			"v2-fastcdc":        300,
			"v1-simple-rolling": 100,
		},
	})

	if len(result.Chunks.ChunkerVersions) != 2 {
		t.Fatalf("expected 2 chunker versions, got %d", len(result.Chunks.ChunkerVersions))
	}
	if result.Chunks.ChunkerVersions[0].Version != "v1-simple-rolling" {
		t.Fatalf("expected sorted chunker_versions[0]=v1-simple-rolling, got %q", result.Chunks.ChunkerVersions[0].Version)
	}
	if result.Chunks.ChunkerVersions[1].Version != "v2-fastcdc" {
		t.Fatalf("expected sorted chunker_versions[1]=v2-fastcdc, got %q", result.Chunks.ChunkerVersions[1].Version)
	}
}

func TestStatsCalculatesEfficiencyWithoutDivisionByZero(t *testing.T) {
	result := buildEfficiencyStats(0, 0, 123)
	if result.DedupRatio != 0 {
		t.Fatalf("expected zero dedup ratio, got %f", result.DedupRatio)
	}
	if result.DedupRatioPercent != 0 {
		t.Fatalf("expected zero dedup savings pct, got %f", result.DedupRatioPercent)
	}
	if result.ContainerOverheadPct != 0 {
		t.Fatalf("expected zero container overhead pct, got %f", result.ContainerOverheadPct)
	}
}

func TestStatsIncludesContainerRecordsOnlyWhenRequested(t *testing.T) {
	dbconn := openInspectTestDB(t)

	lfRes, err := dbconn.Exec(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`,
		"gamma.txt", 55, "hash-gamma", "COMPLETED", "v2-fastcdc",
	)
	if err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}
	logicalFileID, err := lfRes.LastInsertId()
	if err != nil {
		t.Fatalf("logical_file last insert id: %v", err)
	}

	ctrRes, err := dbconn.Exec(`INSERT INTO container (filename, current_size, max_size, quarantine) VALUES (?, ?, ?, ?)`, "ctr_records.bin", 512, 1024, 0)
	if err != nil {
		t.Fatalf("insert container: %v", err)
	}
	containerID, err := ctrRes.LastInsertId()
	if err != nil {
		t.Fatalf("container last insert id: %v", err)
	}

	chunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "chunk-records", 64, "COMPLETED", 1, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert chunk: %v", err)
	}
	chunkID, err := chunkRes.LastInsertId()
	if err != nil {
		t.Fatalf("chunk last insert id: %v", err)
	}

	if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES (?, ?, ?)`, logicalFileID, chunkID, 0); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}
	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		chunkID, "plain", 1, 64, 64, containerID, 0,
	); err != nil {
		t.Fatalf("insert block: %v", err)
	}

	svc := newServiceForTest(dbconn, nil)
	without, err := svc.Stats(context.Background(), StatsOptions{IncludeContainers: false})
	if err != nil {
		t.Fatalf("Stats without containers: %v", err)
	}
	with, err := svc.Stats(context.Background(), StatsOptions{IncludeContainers: true})
	if err != nil {
		t.Fatalf("Stats with containers: %v", err)
	}

	if len(without.Containers.Records) != 0 {
		t.Fatalf("expected no container records without option, got %d", len(without.Containers.Records))
	}
	if len(with.Containers.Records) == 0 {
		t.Fatal("expected container records when option is enabled")
	}
}

func TestStatsDoesNotRepairWhenSnapshotIDsAreNonNumeric(t *testing.T) {
	dbconn := openInspectTestDB(t)

	if _, err := dbconn.Exec(
		`INSERT INTO snapshot (id, created_at, type) VALUES (?, ?, ?)`,
		"non-numeric-snap", time.Now().UTC(), "full",
	); err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}

	before := mustCount(t, dbconn, `SELECT COUNT(*) FROM snapshot`)
	svc := newServiceForTest(dbconn, nil)
	result, err := svc.Stats(context.Background(), StatsOptions{})
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	after := mustCount(t, dbconn, `SELECT COUNT(*) FROM snapshot`)

	if hasWarningCode(result.Warnings, "snapshot_ids_non_numeric_skipped") {
		t.Fatalf("did not expect warning for non-numeric snapshot ids, got %+v", result.Warnings)
	}
	if before != after {
		t.Fatalf("expected no mutation/repair during stats: before=%d after=%d", before, after)
	}
}

func TestStatsDoesNotMutateState(t *testing.T) {
	dbconn := openInspectTestDB(t)

	if _, err := dbconn.Exec(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`,
		"delta.txt", 100, "hash-delta", "COMPLETED", "v2-fastcdc",
	); err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}

	beforeLogical := mustCount(t, dbconn, `SELECT COUNT(*) FROM logical_file`)
	beforeChunks := mustCount(t, dbconn, `SELECT COUNT(*) FROM chunk`)
	beforeContainers := mustCount(t, dbconn, `SELECT COUNT(*) FROM container`)

	svc := newServiceForTest(dbconn, nil)
	if _, err := svc.Stats(context.Background(), StatsOptions{IncludeContainers: true}); err != nil {
		t.Fatalf("Stats: %v", err)
	}

	afterLogical := mustCount(t, dbconn, `SELECT COUNT(*) FROM logical_file`)
	afterChunks := mustCount(t, dbconn, `SELECT COUNT(*) FROM chunk`)
	afterContainers := mustCount(t, dbconn, `SELECT COUNT(*) FROM container`)

	if beforeLogical != afterLogical {
		t.Fatalf("logical_file count mutated: before=%d after=%d", beforeLogical, afterLogical)
	}
	if beforeChunks != afterChunks {
		t.Fatalf("chunk count mutated: before=%d after=%d", beforeChunks, afterChunks)
	}
	if beforeContainers != afterContainers {
		t.Fatalf("container count mutated: before=%d after=%d", beforeContainers, afterContainers)
	}
}

func TestStatsTraceEmitsHighLevelCollectionEvents(t *testing.T) {
	dbconn := openInspectTestDB(t)
	svc := newServiceForTest(dbconn, nil)
	sink := &traceCollectorSink{}

	_, err := svc.Stats(context.Background(), StatsOptions{
		Trace: TraceOptions{Enabled: true, Sink: sink},
	})
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}

	steps := make([]string, 0, len(sink.events))
	for _, event := range sink.events {
		steps = append(steps, event.Step)
	}

	want := []string{
		"stats.collect.start",
		"stats.collect.repository",
		"stats.collect.logical",
		"stats.collect.chunks",
		"stats.collect.containers",
		"stats.collect.retention",
		"stats.graph.enrich",
		"stats.collect.complete",
	}

	if len(steps) != len(want) {
		t.Fatalf("unexpected trace step count: got=%d want=%d steps=%v", len(steps), len(want), steps)
	}
	if !slices.Equal(steps, want) {
		t.Fatalf("unexpected trace steps:\n got=%v\nwant=%v", steps, want)
	}
}

func TestStatsTraceDisabledDoesNotEmitEvents(t *testing.T) {
	dbconn := openInspectTestDB(t)
	svc := newServiceForTest(dbconn, nil)
	sink := &traceCollectorSink{}

	_, err := svc.Stats(context.Background(), StatsOptions{
		Trace: TraceOptions{Enabled: false, Sink: sink},
	})
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}

	if len(sink.events) != 0 {
		t.Fatalf("expected no trace events when trace is disabled, got %d", len(sink.events))
	}
}

func TestStatsTraceEmitsExpectedHighLevelEvents(t *testing.T) {
	TestStatsTraceEmitsHighLevelCollectionEvents(t)
}

func TestTraceDoesNotMutateState(t *testing.T) {
	dbconn := openInspectTestDB(t)

	// Seed minimal state: one logical file, one chunk, one container, one snapshot.
	fileRes, err := dbconn.Exec(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version) VALUES (?, ?, ?, ?, ?, ?)`,
		"trace-mut.txt", 50, "h-trace-mut", "COMPLETED", 0, "v2-fastcdc",
	)
	if err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}
	fileID, _ := fileRes.LastInsertId()

	chunkRes, err := dbconn.Exec(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`,
		"chunk-trace-mut", 50, "COMPLETED", 1, "v2-fastcdc",
	)
	if err != nil {
		t.Fatalf("insert chunk: %v", err)
	}
	chunkID, _ := chunkRes.LastInsertId()

	ctrRes, err := dbconn.Exec(
		`INSERT INTO container (filename, current_size, max_size, quarantine) VALUES (?, ?, ?, ?)`,
		"ctr_trace_mut.bin", 256, 1024, 0,
	)
	if err != nil {
		t.Fatalf("insert container: %v", err)
	}
	containerID, _ := ctrRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES (?, ?, ?)`, fileID, chunkID, 0); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}
	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		chunkID, "plain", 1, 50, 50, containerID, 0,
	); err != nil {
		t.Fatalf("insert blocks: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES (?, CURRENT_TIMESTAMP, 'full')`, "trace-mut-snap"); err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES (?)`, "trace-mut.txt"); err != nil {
		t.Fatalf("insert snapshot_path: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES (?, 1, ?)`, "trace-mut-snap", fileID); err != nil {
		t.Fatalf("insert snapshot_file: %v", err)
	}

	countTables := []string{"logical_file", "chunk", "container", "file_chunk", "blocks", "snapshot", "snapshot_file", "snapshot_path"}
	snapshot := func() map[string]int64 {
		m := make(map[string]int64, len(countTables))
		for _, table := range countTables {
			var c int64
			if err := dbconn.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&c); err != nil {
				t.Fatalf("count %s: %v", table, err)
			}
			m[table] = c
		}
		return m
	}

	before := snapshot()
	sink := &traceCollectorSink{}
	traceOpts := TraceOptions{Enabled: true, Sink: sink}
	svc := newServiceForTest(dbconn, nil)

	// Stats with trace.
	if _, err := svc.Stats(context.Background(), StatsOptions{IncludeContainers: true, Trace: traceOpts}); err != nil {
		t.Fatalf("Stats with trace: %v", err)
	}
	// Inspect with trace: logical file, chunk, and snapshot entity types.
	if _, err := svc.Inspect(context.Background(), EntityLogicalFile, strconv.FormatInt(fileID, 10), InspectOptions{Relations: true, Deep: true, Trace: traceOpts}); err != nil {
		t.Fatalf("Inspect(logical_file) with trace: %v", err)
	}
	if _, err := svc.Inspect(context.Background(), EntityChunk, strconv.FormatInt(chunkID, 10), InspectOptions{Relations: true, Reverse: true, Trace: traceOpts}); err != nil {
		t.Fatalf("Inspect(chunk) with trace: %v", err)
	}
	if _, err := svc.Inspect(context.Background(), EntitySnapshot, "trace-mut-snap", InspectOptions{Relations: true, Trace: traceOpts}); err != nil {
		t.Fatalf("Inspect(snapshot) with trace: %v", err)
	}
	// Simulate GC with trace.
	if _, err := svc.Simulate(context.Background(), SimulationOptions{Kind: SimulationKindGC, Trace: traceOpts}); err != nil {
		t.Fatalf("Simulate with trace: %v", err)
	}

	after := snapshot()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("trace-enabled calls mutated repository state\nbefore: %+v\nafter:  %+v", before, after)
	}
	if len(sink.events) == 0 {
		t.Fatal("expected trace events to be emitted but sink is empty")
	}
}

func TestTraceDoesNotChangeStatsResult(t *testing.T) {
	dbconn := openInspectTestDB(t)
	fixedNow := time.Date(2026, time.April, 27, 10, 0, 0, 0, time.UTC)
	svc := newServiceForTest(dbconn, func() time.Time { return fixedNow })

	withoutTrace, err := svc.Stats(context.Background(), StatsOptions{})
	if err != nil {
		t.Fatalf("Stats without trace: %v", err)
	}

	sink := &traceCollectorSink{}
	withTrace, err := svc.Stats(context.Background(), StatsOptions{
		Trace: TraceOptions{Enabled: true, Sink: sink},
	})
	if err != nil {
		t.Fatalf("Stats with trace: %v", err)
	}

	if !reflect.DeepEqual(withoutTrace, withTrace) {
		t.Fatalf("stats result changed with trace enabled\nwithout=%+v\nwith=%+v", withoutTrace, withTrace)
	}
}

func TestSumChunkSizesByIDMatchesLegacyLoop(t *testing.T) {
	dbconn := openInspectTestDB(t)
	svc := newServiceForTest(dbconn, nil)

	chunkResA, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "sum-a", 11, "COMPLETED", 1, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert chunk A: %v", err)
	}
	chunkA, err := chunkResA.LastInsertId()
	if err != nil {
		t.Fatalf("chunk A last insert id: %v", err)
	}

	chunkResB, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "sum-b", 22, "COMPLETED", 1, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert chunk B: %v", err)
	}
	chunkB, err := chunkResB.LastInsertId()
	if err != nil {
		t.Fatalf("chunk B last insert id: %v", err)
	}

	chunkIDs := map[int64]struct{}{
		chunkA: {},
		chunkB: {},
		999999: {}, // missing chunk id should be ignored
	}

	got, err := svc.sumChunkSizesByID(context.Background(), chunkIDs)
	if err != nil {
		t.Fatalf("sumChunkSizesByID: %v", err)
	}

	want, err := legacySumChunkSizesByID(context.Background(), dbconn, chunkIDs)
	if err != nil {
		t.Fatalf("legacySumChunkSizesByID: %v", err)
	}

	if got != want {
		t.Fatalf("sumChunkSizesByID mismatch: got=%d want=%d", got, want)
	}
}

func TestSnapshotReachabilityViaSQLMatchesLegacyLoop(t *testing.T) {
	dbconn := openInspectTestDB(t)
	svc := newServiceForTest(dbconn, nil)

	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES (?, ?, ?)`, "reach-a", time.Now().UTC(), "full"); err != nil {
		t.Fatalf("insert snapshot reach-a: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES (?, ?, ?)`, "reach-b", time.Now().UTC(), "full"); err != nil {
		t.Fatalf("insert snapshot reach-b: %v", err)
	}

	fileRes1, err := dbconn.Exec(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`,
		"reach-1.txt", 30, "hash-reach-1", "COMPLETED", "v2-fastcdc",
	)
	if err != nil {
		t.Fatalf("insert logical_file 1: %v", err)
	}
	file1, err := fileRes1.LastInsertId()
	if err != nil {
		t.Fatalf("logical_file 1 last insert id: %v", err)
	}

	fileRes2, err := dbconn.Exec(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`,
		"reach-2.txt", 40, "hash-reach-2", "COMPLETED", "v2-fastcdc",
	)
	if err != nil {
		t.Fatalf("insert logical_file 2: %v", err)
	}
	file2, err := fileRes2.LastInsertId()
	if err != nil {
		t.Fatalf("logical_file 2 last insert id: %v", err)
	}

	chunkRes1, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "reach-c1", 10, "COMPLETED", 1, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert chunk c1: %v", err)
	}
	chunk1, err := chunkRes1.LastInsertId()
	if err != nil {
		t.Fatalf("chunk c1 last insert id: %v", err)
	}

	chunkRes2, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "reach-c2", 20, "COMPLETED", 1, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert chunk c2: %v", err)
	}
	chunk2, err := chunkRes2.LastInsertId()
	if err != nil {
		t.Fatalf("chunk c2 last insert id: %v", err)
	}

	chunkRes3, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "reach-c3", 30, "COMPLETED", 1, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert chunk c3: %v", err)
	}
	chunk3, err := chunkRes3.LastInsertId()
	if err != nil {
		t.Fatalf("chunk c3 last insert id: %v", err)
	}

	if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES (?, ?, ?), (?, ?, ?)`, file1, chunk1, 0, file1, chunk2, 1); err != nil {
		t.Fatalf("insert file_chunk mappings for file1: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES (?, ?, ?), (?, ?, ?)`, file2, chunk2, 0, file2, chunk3, 1); err != nil {
		t.Fatalf("insert file_chunk mappings for file2: %v", err)
	}

	testdb.InsertSnapshotFileRef(t, dbconn, "reach-a", "snap/reach-1.txt", file1)
	testdb.InsertSnapshotFileRef(t, dbconn, "reach-b", "snap/reach-2.txt", file2)

	snapshotIDs := []string{"reach-a", "reach-b", "reach-a", "missing"}

	gotChunks, gotBytes, err := svc.snapshotReachabilityViaSQL(context.Background(), snapshotIDs)
	if err != nil {
		t.Fatalf("snapshotReachabilityViaSQL: %v", err)
	}

	wantChunks, wantBytes, err := legacySnapshotReachabilityViaSQL(context.Background(), dbconn, snapshotIDs)
	if err != nil {
		t.Fatalf("legacySnapshotReachabilityViaSQL: %v", err)
	}

	if gotChunks != wantChunks || gotBytes != wantBytes {
		t.Fatalf("snapshotReachabilityViaSQL mismatch: got=(%d,%d) want=(%d,%d)", gotChunks, gotBytes, wantChunks, wantBytes)
	}
}

func legacySumChunkSizesByID(ctx context.Context, dbconn *sql.DB, chunkIDs map[int64]struct{}) (int64, error) {
	var total int64
	for chunkID := range chunkIDs {
		var size int64
		err := dbconn.QueryRowContext(ctx, `SELECT size FROM chunk WHERE id = $1`, chunkID).Scan(&size)
		if err != nil {
			if err == sql.ErrNoRows {
				continue
			}
			return 0, err
		}
		total += size
	}
	return total, nil
}

func legacySnapshotReachabilityViaSQL(ctx context.Context, dbconn *sql.DB, snapshotIDs []string) (int64, int64, error) {
	uniqueChunkSizes := make(map[int64]int64)
	for _, snapshotID := range snapshotIDs {
		rows, err := dbconn.QueryContext(
			ctx,
			`SELECT fc.chunk_id, c.size
			 FROM snapshot_file sf
			 JOIN file_chunk fc ON fc.logical_file_id = sf.logical_file_id
			 JOIN chunk c ON c.id = fc.chunk_id
			 WHERE sf.snapshot_id = $1`,
			snapshotID,
		)
		if err != nil {
			return 0, 0, err
		}

		for rows.Next() {
			var chunkID int64
			var size int64
			if err := rows.Scan(&chunkID, &size); err != nil {
				_ = rows.Close()
				return 0, 0, err
			}
			if _, exists := uniqueChunkSizes[chunkID]; !exists {
				uniqueChunkSizes[chunkID] = size
			}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return 0, 0, err
		}
		_ = rows.Close()
	}

	var totalBytes int64
	for _, size := range uniqueChunkSizes {
		totalBytes += size
	}

	return int64(len(uniqueChunkSizes)), totalBytes, nil
}

func TestStatsDeterministicAcrossCalls(t *testing.T) {
	dbconn := openInspectTestDB(t)
	fixedNow := time.Date(2026, time.April, 27, 12, 0, 0, 0, time.UTC)
	svc := newServiceForTest(dbconn, func() time.Time { return fixedNow })

	opts := StatsOptions{IncludeContainers: true}

	first, err := svc.Stats(context.Background(), opts)
	if err != nil {
		t.Fatalf("first Stats: %v", err)
	}
	second, err := svc.Stats(context.Background(), opts)
	if err != nil {
		t.Fatalf("second Stats: %v", err)
	}

	// Normalize GeneratedAtUTC before byte comparison so the test remains
	// deterministic even if the system clock advances between calls.
	first.GeneratedAtUTC = time.Time{}
	second.GeneratedAtUTC = time.Time{}

	b1, err := json.Marshal(first)
	if err != nil {
		t.Fatalf("marshal first: %v", err)
	}
	b2, err := json.Marshal(second)
	if err != nil {
		t.Fatalf("marshal second: %v", err)
	}
	if string(b1) != string(b2) {
		t.Fatalf("stats output not deterministic across calls\nfirst:  %s\nsecond: %s", string(b1), string(b2))
	}
}
