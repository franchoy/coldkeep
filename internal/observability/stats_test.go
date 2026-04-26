package observability

import (
	"context"
	"database/sql"
	"strconv"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/tests/testdb"
)

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
	if result.Efficiency.LogicalBytes != 500 {
		t.Fatalf("unexpected efficiency logical_bytes: %d", result.Efficiency.LogicalBytes)
	}
	if result.Efficiency.UniqueChunkBytes != 450 {
		t.Fatalf("unexpected efficiency unique_chunk_bytes: %d", result.Efficiency.UniqueChunkBytes)
	}
	if result.Efficiency.ContainerBytes != 900 {
		t.Fatalf("unexpected efficiency container_bytes: %d", result.Efficiency.ContainerBytes)
	}
	if result.Efficiency.DedupRatio != 0.9 {
		t.Fatalf("unexpected efficiency dedup_ratio: %f", result.Efficiency.DedupRatio)
	}
	if result.Efficiency.DedupRatioPercent != 90 {
		t.Fatalf("unexpected efficiency dedup_ratio_percent: %f", result.Efficiency.DedupRatioPercent)
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
}

func TestStatsReturnsErrorWhenDBIsMissing(t *testing.T) {
	svc := newServiceForTest(nil, func() time.Time { return time.Now().UTC() })

	_, err := svc.Stats(nil, StatsOptions{})
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

	if _, err := dbconn.Exec(
		`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES (?, ?, 1)`,
		"/data/alpha.txt", logicalFileID,
	); err != nil {
		t.Fatalf("insert physical_file: %v", err)
	}

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
	if result.Chunks.CountsByVersion == nil || len(result.Chunks.CountsByVersion) == 0 {
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
