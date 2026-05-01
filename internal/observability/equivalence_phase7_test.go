package observability

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/snapshot"
)

type phase7ScenarioRefs struct {
	inspectSnapshotID string
	inspectContainer  int64
	diffBaseID        string
	diffTargetID      string
	snapshotStatsID   string
}

type legacyStatsCore struct {
	totalFiles               int64
	totalLogicalSizeBytes    int64
	completedFiles           int64
	completedSizeBytes       int64
	processingFiles          int64
	processingSizeBytes      int64
	abortedFiles             int64
	abortedSizeBytes         int64
	healthyContainers        int64
	healthyContainerBytes    int64
	quarantineContainers     int64
	quarantineContainerBytes int64
	totalContainers          int64
	totalContainerBytes      int64
}

func TestPhase7EquivalenceAcrossScenarios(t *testing.T) {
	t.Parallel()

	scenarios := []struct {
		name  string
		setup func(t *testing.T, dbconn *sql.DB) phase7ScenarioRefs
	}{
		{name: "empty_repo", setup: setupEquivalenceEmptyRepo},
		{name: "large_repo", setup: setupEquivalenceLargeRepo},
		{name: "multiple_snapshots", setup: setupEquivalenceMultipleSnapshots},
		{name: "deleted_snapshots", setup: setupEquivalenceDeletedSnapshots},
		{name: "gc_after_churn", setup: setupEquivalenceGCAfterChurn},
	}

	for _, tc := range scenarios {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			dbconn := openSimulateTestDB(t)
			refs := tc.setup(t, dbconn)
			ctx := context.Background()
			fixedNow := time.Date(2026, time.May, 1, 12, 0, 0, 0, time.UTC)
			svc := newServiceForTest(dbconn, func() time.Time { return fixedNow })

			// 1) stats output identical (core fields impacted by query-shape refactors)
			gotStats, err := svc.Stats(ctx, StatsOptions{IncludeContainers: true})
			if err != nil {
				t.Fatalf("Stats: %v", err)
			}
			wantStats, err := legacyStatsCoreFromDB(ctx, dbconn)
			if err != nil {
				t.Fatalf("legacyStatsCoreFromDB: %v", err)
			}
			assertStatsCoreEquivalent(t, gotStats, wantStats)

			// 2) inspect output identical (repository always, snapshot/container when present)
			gotRepo, err := svc.Inspect(ctx, EntityRepository, "", InspectOptions{})
			if err != nil {
				t.Fatalf("Inspect repository: %v", err)
			}
			wantRepo, err := legacyInspectRepositorySummary(ctx, dbconn)
			if err != nil {
				t.Fatalf("legacyInspectRepositorySummary: %v", err)
			}
			if !reflect.DeepEqual(gotRepo.Summary, wantRepo) {
				t.Fatalf("repository inspect mismatch\n got: %#v\nwant: %#v", gotRepo.Summary, wantRepo)
			}

			if refs.inspectSnapshotID != "" {
				gotSnapshot, err := svc.Inspect(ctx, EntitySnapshot, refs.inspectSnapshotID, InspectOptions{})
				if err != nil {
					t.Fatalf("Inspect snapshot %s: %v", refs.inspectSnapshotID, err)
				}
				wantSnapshot, err := legacyInspectSnapshotSummary(ctx, dbconn, refs.inspectSnapshotID)
				if err != nil {
					t.Fatalf("legacyInspectSnapshotSummary(%s): %v", refs.inspectSnapshotID, err)
				}
				if !reflect.DeepEqual(gotSnapshot.Summary, wantSnapshot) {
					t.Fatalf("snapshot inspect mismatch\n got: %#v\nwant: %#v", gotSnapshot.Summary, wantSnapshot)
				}
			}

			if refs.inspectContainer > 0 {
				containerID := strconv.FormatInt(refs.inspectContainer, 10)
				gotContainer, err := svc.Inspect(ctx, EntityContainer, containerID, InspectOptions{})
				if err != nil {
					t.Fatalf("Inspect container %s: %v", containerID, err)
				}
				wantContainer, err := legacyInspectContainerSummary(ctx, dbconn, refs.inspectContainer)
				if err != nil {
					t.Fatalf("legacyInspectContainerSummary(%d): %v", refs.inspectContainer, err)
				}
				if !reflect.DeepEqual(gotContainer.Summary, wantContainer) {
					t.Fatalf("container inspect mismatch\n got: %#v\nwant: %#v", gotContainer.Summary, wantContainer)
				}
			}

			// 3) snapshot stats identical (global always, per-snapshot when present)
			gotGlobalStats, err := snapshot.GetSnapshotStats(ctx, dbconn, "")
			if err != nil {
				t.Fatalf("GetSnapshotStats(global): %v", err)
			}
			wantGlobalStats, err := legacyGetSnapshotStats(ctx, dbconn, "")
			if err != nil {
				t.Fatalf("legacyGetSnapshotStats(global): %v", err)
			}
			if !reflect.DeepEqual(gotGlobalStats, wantGlobalStats) {
				t.Fatalf("global snapshot stats mismatch\n got: %#v\nwant: %#v", gotGlobalStats, wantGlobalStats)
			}

			if refs.snapshotStatsID != "" {
				gotPerSnapshot, err := snapshot.GetSnapshotStats(ctx, dbconn, refs.snapshotStatsID)
				if err != nil {
					t.Fatalf("GetSnapshotStats(%s): %v", refs.snapshotStatsID, err)
				}
				wantPerSnapshot, err := legacyGetSnapshotStats(ctx, dbconn, refs.snapshotStatsID)
				if err != nil {
					t.Fatalf("legacyGetSnapshotStats(%s): %v", refs.snapshotStatsID, err)
				}
				if !reflect.DeepEqual(gotPerSnapshot, wantPerSnapshot) {
					t.Fatalf("per-snapshot stats mismatch\n got: %#v\nwant: %#v", gotPerSnapshot, wantPerSnapshot)
				}
			}

			// 4) snapshot diff identical (summary SQL and full diff)
			if refs.diffBaseID != "" && refs.diffTargetID != "" {
				gotSummary, err := snapshot.DiffSnapshotsSummarySQL(ctx, dbconn, refs.diffBaseID, refs.diffTargetID)
				if err != nil {
					t.Fatalf("DiffSnapshotsSummarySQL(%s,%s): %v", refs.diffBaseID, refs.diffTargetID, err)
				}
				wantSummary, err := legacyDiffSummary(ctx, dbconn, refs.diffBaseID, refs.diffTargetID)
				if err != nil {
					t.Fatalf("legacyDiffSummary(%s,%s): %v", refs.diffBaseID, refs.diffTargetID, err)
				}
				if !reflect.DeepEqual(gotSummary, wantSummary) {
					t.Fatalf("snapshot diff summary mismatch\n got: %#v\nwant: %#v", gotSummary, wantSummary)
				}

				gotFull, err := snapshot.DiffSnapshots(ctx, dbconn, refs.diffBaseID, refs.diffTargetID, nil)
				if err != nil {
					t.Fatalf("DiffSnapshots(%s,%s): %v", refs.diffBaseID, refs.diffTargetID, err)
				}
				wantFull, err := legacyDiffSnapshots(ctx, dbconn, refs.diffBaseID, refs.diffTargetID)
				if err != nil {
					t.Fatalf("legacyDiffSnapshots(%s,%s): %v", refs.diffBaseID, refs.diffTargetID, err)
				}
				if !reflect.DeepEqual(gotFull, wantFull) {
					t.Fatalf("snapshot full diff mismatch\n got: %#v\nwant: %#v", gotFull, wantFull)
				}
			}
		})
	}
}

func setupEquivalenceEmptyRepo(t *testing.T, dbconn *sql.DB) phase7ScenarioRefs {
	t.Helper()
	return phase7ScenarioRefs{}
}

func setupEquivalenceLargeRepo(t *testing.T, dbconn *sql.DB) phase7ScenarioRefs {
	t.Helper()

	insertEqSnapshot(t, dbconn, "large-s1", "")
	insertEqSnapshot(t, dbconn, "large-s2", "large-s1")

	containerID := insertSimContainer(t, dbconn, "large-ctr.bin", 0, true, false)

	for i := 0; i < 180; i++ {
		fileID := insertSimLogicalFile(t, dbconn, fmt.Sprintf("large-file-%03d.txt", i))
		chunkID := insertSimChunk(t, dbconn, fmt.Sprintf("large-chunk-%03d", i), 100, 1, 0, "v2-fastcdc")
		linkSimFileChunk(t, dbconn, fileID, chunkID, 0)
		insertSimBlock(t, dbconn, chunkID, containerID, 100)

		path := fmt.Sprintf("large/path/%03d.txt", i)
		insertEqSnapshotFileRef(t, dbconn, "large-s1", path, fileID, 100)

		if i%3 != 0 {
			targetFileID := fileID
			if i%10 == 0 {
				targetFileID = insertSimLogicalFile(t, dbconn, fmt.Sprintf("large-file-mod-%03d.txt", i))
			}
			insertEqSnapshotFileRef(t, dbconn, "large-s2", path, targetFileID, 100)
		}
	}

	return phase7ScenarioRefs{
		inspectSnapshotID: "large-s2",
		inspectContainer:  containerID,
		diffBaseID:        "large-s1",
		diffTargetID:      "large-s2",
		snapshotStatsID:   "large-s2",
	}
}

func setupEquivalenceMultipleSnapshots(t *testing.T, dbconn *sql.DB) phase7ScenarioRefs {
	t.Helper()

	insertEqSnapshot(t, dbconn, "multi-s1", "")
	insertEqSnapshot(t, dbconn, "multi-s2", "multi-s1")
	insertEqSnapshot(t, dbconn, "multi-s3", "multi-s2")

	containerID := insertSimContainer(t, dbconn, "multi-ctr.bin", 0, true, false)

	for i := 0; i < 12; i++ {
		fileID := insertSimLogicalFile(t, dbconn, fmt.Sprintf("multi-file-%02d", i))
		chunkID := insertSimChunk(t, dbconn, fmt.Sprintf("multi-chunk-%02d", i), 100, 1, 0, "v2-fastcdc")
		linkSimFileChunk(t, dbconn, fileID, chunkID, 0)
		insertSimBlock(t, dbconn, chunkID, containerID, 100)

		path := fmt.Sprintf("multi/path/%02d.txt", i)
		if i < 8 {
			insertEqSnapshotFileRef(t, dbconn, "multi-s1", path, fileID, 100)
		}
		if i >= 2 {
			insertEqSnapshotFileRef(t, dbconn, "multi-s2", path, fileID, 100)
		}
		if i >= 4 {
			insertEqSnapshotFileRef(t, dbconn, "multi-s3", path, fileID, 100)
		}
	}

	return phase7ScenarioRefs{
		inspectSnapshotID: "multi-s3",
		inspectContainer:  containerID,
		diffBaseID:        "multi-s2",
		diffTargetID:      "multi-s3",
		snapshotStatsID:   "multi-s3",
	}
}

func setupEquivalenceDeletedSnapshots(t *testing.T, dbconn *sql.DB) phase7ScenarioRefs {
	t.Helper()

	insertEqSnapshot(t, dbconn, "del-s1", "")
	insertEqSnapshot(t, dbconn, "del-s2", "del-s1")
	insertEqSnapshot(t, dbconn, "del-s3", "del-s2")

	containerID := insertSimContainer(t, dbconn, "del-ctr.bin", 0, true, false)
	for i := 0; i < 8; i++ {
		fileID := insertSimLogicalFile(t, dbconn, fmt.Sprintf("del-file-%02d", i))
		chunkID := insertSimChunk(t, dbconn, fmt.Sprintf("del-chunk-%02d", i), 100, 1, 0, "v2-fastcdc")
		linkSimFileChunk(t, dbconn, fileID, chunkID, 0)
		insertSimBlock(t, dbconn, chunkID, containerID, 100)

		path := fmt.Sprintf("deleted/path/%02d.txt", i)
		insertEqSnapshotFileRef(t, dbconn, "del-s1", path, fileID, 100)
		if i%2 == 0 {
			insertEqSnapshotFileRef(t, dbconn, "del-s2", path, fileID, 100)
		}
		if i >= 3 {
			insertEqSnapshotFileRef(t, dbconn, "del-s3", path, fileID, 100)
		}
	}

	if err := snapshot.DeleteSnapshot(context.Background(), dbconn, "del-s2"); err != nil {
		t.Fatalf("DeleteSnapshot(del-s2): %v", err)
	}

	return phase7ScenarioRefs{
		inspectSnapshotID: "del-s3",
		inspectContainer:  containerID,
		diffBaseID:        "del-s1",
		diffTargetID:      "del-s3",
		snapshotStatsID:   "del-s3",
	}
}

func setupEquivalenceGCAfterChurn(t *testing.T, dbconn *sql.DB) phase7ScenarioRefs {
	t.Helper()

	insertEqSnapshot(t, dbconn, "churn-s1", "")
	insertEqSnapshot(t, dbconn, "churn-s2", "churn-s1")

	liveContainerID := insertSimContainer(t, dbconn, "churn-live.bin", 0, true, false)
	deadContainerID := insertSimContainer(t, dbconn, "churn-dead.bin", 0, true, false)

	for i := 0; i < 6; i++ {
		fileID := insertSimLogicalFile(t, dbconn, fmt.Sprintf("churn-live-file-%02d", i))
		chunkID := insertSimChunk(t, dbconn, fmt.Sprintf("churn-live-%02d", i), 100, 1, 0, "v2-fastcdc")
		linkSimFileChunk(t, dbconn, fileID, chunkID, 0)
		insertSimBlock(t, dbconn, chunkID, liveContainerID, 100)
		insertEqSnapshotFileRef(t, dbconn, "churn-s1", fmt.Sprintf("churn/live/%02d.txt", i), fileID, 100)
		insertEqSnapshotFileRef(t, dbconn, "churn-s2", fmt.Sprintf("churn/live/%02d.txt", i), fileID, 100)
	}

	for i := 0; i < 5; i++ {
		deadChunkID := insertSimChunk(t, dbconn, fmt.Sprintf("churn-dead-%02d", i), 80, 0, 0, "v2-fastcdc")
		insertSimBlock(t, dbconn, deadChunkID, deadContainerID, 80)
	}

	if err := snapshot.DeleteSnapshot(context.Background(), dbconn, "churn-s1"); err != nil {
		t.Fatalf("DeleteSnapshot(churn-s1): %v", err)
	}

	return phase7ScenarioRefs{
		inspectSnapshotID: "churn-s2",
		inspectContainer:  deadContainerID,
		diffBaseID:        "churn-s2",
		diffTargetID:      "churn-s2",
		snapshotStatsID:   "churn-s2",
	}
}

func insertEqSnapshot(t *testing.T, dbconn *sql.DB, snapshotID, parentID string) {
	t.Helper()
	if parentID == "" {
		if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES (?, ?, 'full')`, snapshotID, time.Now().UTC()); err != nil {
			t.Fatalf("insert snapshot %s: %v", snapshotID, err)
		}
		return
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type, parent_id) VALUES (?, ?, 'full', ?)`, snapshotID, time.Now().UTC(), parentID); err != nil {
		t.Fatalf("insert snapshot %s parent=%s: %v", snapshotID, parentID, err)
	}
}

func insertEqSnapshotFileRef(t *testing.T, dbconn *sql.DB, snapshotID, path string, logicalFileID, size int64) {
	t.Helper()
	if _, err := dbconn.Exec(`INSERT OR IGNORE INTO snapshot_path (path) VALUES (?)`, path); err != nil {
		t.Fatalf("insert snapshot_path %s: %v", path, err)
	}
	var pathID int64
	if err := dbconn.QueryRow(`SELECT id FROM snapshot_path WHERE path = ?`, path).Scan(&pathID); err != nil {
		t.Fatalf("lookup snapshot_path id for %s: %v", path, err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id, size) VALUES (?, ?, ?, ?)`, snapshotID, pathID, logicalFileID, size); err != nil {
		t.Fatalf("insert snapshot_file snapshot=%s path=%s logical_file=%d: %v", snapshotID, path, logicalFileID, err)
	}
}

func assertStatsCoreEquivalent(t *testing.T, got *StatsResult, want legacyStatsCore) {
	t.Helper()
	if got == nil {
		t.Fatal("stats result is nil")
	}

	if got.Logical.TotalFiles != want.totalFiles ||
		got.Logical.TotalSizeBytes != want.totalLogicalSizeBytes ||
		got.Logical.CompletedFiles != want.completedFiles ||
		got.Logical.CompletedSizeBytes != want.completedSizeBytes ||
		got.Logical.ProcessingFiles != want.processingFiles ||
		got.Logical.AbortedFiles != want.abortedFiles {
		t.Fatalf("logical stats mismatch\n got: %+v\nwant: %+v", got.Logical, want)
	}

	if got.Containers.HealthyContainers != want.healthyContainers ||
		got.Containers.HealthyBytes != want.healthyContainerBytes ||
		got.Containers.QuarantineContainers != want.quarantineContainers ||
		got.Containers.QuarantineBytes != want.quarantineContainerBytes ||
		got.Containers.TotalContainers != want.totalContainers ||
		got.Containers.TotalBytes != want.totalContainerBytes {
		t.Fatalf("container stats mismatch\n got: %+v\nwant: %+v", got.Containers, want)
	}
}

func legacyStatsCoreFromDB(ctx context.Context, dbconn *sql.DB) (legacyStatsCore, error) {
	var out legacyStatsCore

	if err := dbconn.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(total_size),0) FROM logical_file`).Scan(&out.totalFiles, &out.totalLogicalSizeBytes); err != nil {
		return out, err
	}
	if err := dbconn.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(total_size),0) FROM logical_file WHERE status = ?`, "COMPLETED").Scan(&out.completedFiles, &out.completedSizeBytes); err != nil {
		return out, err
	}
	if err := dbconn.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(total_size),0) FROM logical_file WHERE status = ?`, "PROCESSING").Scan(&out.processingFiles, &out.processingSizeBytes); err != nil {
		return out, err
	}
	if err := dbconn.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(total_size),0) FROM logical_file WHERE status = ?`, "ABORTED").Scan(&out.abortedFiles, &out.abortedSizeBytes); err != nil {
		return out, err
	}
	if err := dbconn.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(current_size),0) FROM container WHERE quarantine = FALSE`).Scan(&out.healthyContainers, &out.healthyContainerBytes); err != nil {
		return out, err
	}
	if err := dbconn.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(current_size),0) FROM container WHERE quarantine = TRUE`).Scan(&out.quarantineContainers, &out.quarantineContainerBytes); err != nil {
		return out, err
	}

	out.totalContainers = out.healthyContainers + out.quarantineContainers
	out.totalContainerBytes = out.healthyContainerBytes + out.quarantineContainerBytes
	return out, nil
}

func legacyInspectRepositorySummary(ctx context.Context, dbconn *sql.DB) (map[string]any, error) {
	var logicalFiles, chunks, snapshots int64
	if err := dbconn.QueryRowContext(ctx, `SELECT COUNT(*) FROM logical_file`).Scan(&logicalFiles); err != nil {
		return nil, err
	}
	if err := dbconn.QueryRowContext(ctx, `SELECT COUNT(*) FROM chunk`).Scan(&chunks); err != nil {
		return nil, err
	}
	if err := dbconn.QueryRowContext(ctx, `SELECT COUNT(*) FROM snapshot`).Scan(&snapshots); err != nil {
		return nil, err
	}
	return map[string]any{
		"total_files":     logicalFiles,
		"total_chunks":    chunks,
		"total_snapshots": snapshots,
	}, nil
}

func legacyInspectSnapshotSummary(ctx context.Context, dbconn *sql.DB, snapshotID string) (map[string]any, error) {
	var createdAt string
	var snapshotType string
	var label sql.NullString
	var parentID sql.NullString
	if err := dbconn.QueryRowContext(ctx, `SELECT CAST(created_at AS TEXT), type, label, parent_id FROM snapshot WHERE id = ?`, snapshotID).Scan(&createdAt, &snapshotType, &label, &parentID); err != nil {
		return nil, err
	}

	var fileCount int64
	if err := dbconn.QueryRowContext(ctx, `SELECT COUNT(*) FROM snapshot_file WHERE snapshot_id = ?`, snapshotID).Scan(&fileCount); err != nil {
		return nil, err
	}

	var totalSizeBytes int64
	if err := dbconn.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(lf.total_size), 0)
		FROM snapshot_file sf
		JOIN logical_file lf ON lf.id = sf.logical_file_id
		WHERE sf.snapshot_id = ?
	`, snapshotID).Scan(&totalSizeBytes); err != nil {
		return nil, err
	}

	return map[string]any{
		"snapshot_id":        snapshotID,
		"created_at":         createdAt,
		"logical_file_count": fileCount,
		"total_size_bytes":   totalSizeBytes,
		"type":               snapshotType,
		"label":              nullableString(label),
		"parent_id":          nullableString(parentID),
	}, nil
}

func legacyInspectContainerSummary(ctx context.Context, dbconn *sql.DB, containerID int64) (map[string]any, error) {
	var filename string
	var sealed int64
	var sealing int64
	var quarantine int64
	var currentSize int64
	var maxSize int64
	if err := dbconn.QueryRowContext(ctx, `
		SELECT filename, CAST(sealed AS INTEGER), CAST(sealing AS INTEGER), CAST(quarantine AS INTEGER), current_size, max_size
		FROM container
		WHERE id = ?
	`, containerID).Scan(&filename, &sealed, &sealing, &quarantine, &currentSize, &maxSize); err != nil {
		return nil, err
	}

	var chunkCount int64
	if err := dbconn.QueryRowContext(ctx, `SELECT COUNT(*) FROM blocks WHERE container_id = ?`, containerID).Scan(&chunkCount); err != nil {
		return nil, err
	}

	return map[string]any{
		"container_id": containerID,
		"filename":     filename,
		"size_bytes":   currentSize,
		"chunk_count":  chunkCount,
		"quarantine":   quarantine == 1,
		"sealed":       sealed == 1,
		"sealing":      sealing == 1,
		"current_size": currentSize,
		"max_size":     maxSize,
	}, nil
}

func legacyGetSnapshotStats(ctx context.Context, dbconn *sql.DB, snapshotID string) (*snapshot.SnapshotStats, error) {
	stats := &snapshot.SnapshotStats{SnapshotID: snapshotID}
	if snapshotID == "" {
		if err := dbconn.QueryRowContext(ctx, `SELECT COUNT(*) FROM snapshot`).Scan(&stats.SnapshotCount); err != nil {
			return nil, err
		}
		if err := dbconn.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(size), 0) FROM snapshot_file`).Scan(&stats.SnapshotFileCount, &stats.TotalSizeBytes); err != nil {
			return nil, err
		}
		return stats, nil
	}

	var snapshotType string
	var parentID sql.NullString
	if err := dbconn.QueryRowContext(ctx, `SELECT type, parent_id FROM snapshot WHERE id = ?`, snapshotID).Scan(&snapshotType, &parentID); err != nil {
		return nil, err
	}
	stats.SnapshotCount = 1
	if err := dbconn.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(size), 0) FROM snapshot_file WHERE snapshot_id = ?`, snapshotID).Scan(&stats.SnapshotFileCount, &stats.TotalSizeBytes); err != nil {
		return nil, err
	}

	if !parentID.Valid {
		stats.LineageStatus = snapshot.SnapshotLineageStatusNoParent
		return stats, nil
	}
	if snapshotType != "full" {
		stats.LineageStatus = snapshot.SnapshotLineageStatusSkipped
		return stats, nil
	}

	var parentType string
	err := dbconn.QueryRowContext(ctx, `SELECT type FROM snapshot WHERE id = ?`, parentID.String).Scan(&parentType)
	if err != nil {
		if err == sql.ErrNoRows {
			stats.LineageStatus = snapshot.SnapshotLineageStatusParentMissing
			return stats, nil
		}
		return nil, err
	}
	if parentType != "full" {
		stats.LineageStatus = snapshot.SnapshotLineageStatusSkipped
		return stats, nil
	}

	stats.ParentSnapshotID = parentID
	var reused int64
	if err := dbconn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM snapshot_file child
		JOIN snapshot_file parent
			ON parent.snapshot_id = ?
			AND parent.path_id = child.path_id
			AND parent.logical_file_id = child.logical_file_id
		WHERE child.snapshot_id = ?
	`, parentID.String, snapshotID).Scan(&reused); err != nil {
		return nil, err
	}

	newCount := stats.SnapshotFileCount - reused
	if newCount < 0 {
		newCount = 0
	}
	reuseRatio := 0.0
	if stats.SnapshotFileCount > 0 {
		reuseRatio = float64(reused) * 100.0 / float64(stats.SnapshotFileCount)
	}
	stats.ReusedFileCount = sql.NullInt64{Int64: reused, Valid: true}
	stats.NewFileCount = sql.NullInt64{Int64: newCount, Valid: true}
	stats.ReuseRatioPct = sql.NullFloat64{Float64: reuseRatio, Valid: true}
	stats.LineageStatus = snapshot.SnapshotLineageStatusComputed
	return stats, nil
}

func legacyDiffSummary(ctx context.Context, dbconn *sql.DB, baseID, targetID string) (*snapshot.SnapshotDiffSummary, error) {
	diff, err := legacyDiffSnapshots(ctx, dbconn, baseID, targetID)
	if err != nil {
		return nil, err
	}
	return &diff.Summary, nil
}

func legacyDiffSnapshots(ctx context.Context, dbconn *sql.DB, baseID, targetID string) (*snapshot.SnapshotDiffResult, error) {
	baseRows, err := legacyLoadSnapshotFilesByPath(ctx, dbconn, baseID)
	if err != nil {
		return nil, err
	}
	targetRows, err := legacyLoadSnapshotFilesByPath(ctx, dbconn, targetID)
	if err != nil {
		return nil, err
	}

	allPaths := make(map[string]struct{}, len(baseRows)+len(targetRows))
	for p := range baseRows {
		allPaths[p] = struct{}{}
	}
	for p := range targetRows {
		allPaths[p] = struct{}{}
	}

	paths := make([]string, 0, len(allPaths))
	for p := range allPaths {
		paths = append(paths, p)
	}
	sort.Strings(paths)

	entries := make([]snapshot.SnapshotDiffEntry, 0, len(paths))
	summary := snapshot.SnapshotDiffSummary{}

	for _, p := range paths {
		bID, bOK := baseRows[p]
		tID, tOK := targetRows[p]
		entry := snapshot.SnapshotDiffEntry{Path: p}
		if bOK {
			entry.BaseLogicalID = sql.NullInt64{Int64: bID, Valid: true}
		}
		if tOK {
			entry.TargetLogicalID = sql.NullInt64{Int64: tID, Valid: true}
		}

		switch {
		case !bOK && tOK:
			entry.Type = snapshot.DiffAdded
			summary.Added++
		case bOK && !tOK:
			entry.Type = snapshot.DiffRemoved
			summary.Removed++
		case bOK && tOK:
			if bID == tID {
				continue
			}
			entry.Type = snapshot.DiffModified
			summary.Modified++
		default:
			continue
		}

		entries = append(entries, entry)
	}

	return &snapshot.SnapshotDiffResult{
		BaseSnapshotID:   baseID,
		TargetSnapshotID: targetID,
		Entries:          entries,
		Summary:          summary,
	}, nil
}

func legacyLoadSnapshotFilesByPath(ctx context.Context, dbconn *sql.DB, snapshotID string) (map[string]int64, error) {
	rows, err := dbconn.QueryContext(ctx, `
		SELECT sp.path, sf.logical_file_id
		FROM snapshot_file sf
		JOIN snapshot_path sp ON sp.id = sf.path_id
		WHERE sf.snapshot_id = ?
	`, snapshotID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := make(map[string]int64)
	for rows.Next() {
		var path string
		var logicalFileID int64
		if err := rows.Scan(&path, &logicalFileID); err != nil {
			return nil, err
		}
		out[path] = logicalFileID
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
