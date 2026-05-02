package recovery

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/db"
	filestate "github.com/franchoy/coldkeep/internal/status"
	_ "github.com/mattn/go-sqlite3"
)

func openRecoveryTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	if err := db.RunMigrations(dbconn); err != nil {
		_ = dbconn.Close()
		t.Fatalf("run migrations: %v", err)
	}
	return dbconn
}

func TestIsStrictRecoveryDefaultsToTrue(t *testing.T) {
	t.Setenv("COLDKEEP_STRICT_RECOVERY", "")
	if !isStrictRecovery() {
		t.Fatalf("expected strict recovery default to true")
	}
}

func TestIsStrictRecoveryExplicitFalse(t *testing.T) {
	t.Setenv("COLDKEEP_STRICT_RECOVERY", "false")
	if isStrictRecovery() {
		t.Fatalf("expected strict recovery false when explicitly configured")
	}
}

func TestBuildReportMapsAllFields(t *testing.T) {
	stats := &recoveryStats{
		abortedLogicalFiles:    1,
		abortedChunks:          2,
		quarantinedMissing:     3,
		quarantinedCorruptTail: 4,
		quarantinedOrphan:      5,
		skippedDirEntries:      6,
		totalContainersChecked: 7,
		totalDiskFilesChecked:  8,
		sealingCompleted:       9,
		sealingQuarantined:     10,
	}

	report := buildReport(stats)
	if report.AbortedLogicalFiles != 1 || report.AbortedChunks != 2 || report.QuarantinedMissing != 3 || report.QuarantinedCorruptTail != 4 || report.QuarantinedOrphan != 5 || report.SkippedDirEntries != 6 || report.CheckedContainerRecord != 7 || report.CheckedDiskFiles != 8 || report.SealingCompleted != 9 || report.SealingQuarantined != 10 {
		t.Fatalf("unexpected report mapping: %+v", report)
	}
}

func TestAbortProcessingLogicalFilesTransitionsOnlyProcessingRows(t *testing.T) {
	dbconn := openRecoveryTestDB(t)
	defer func() { _ = dbconn.Close() }()

	if _, err := dbconn.Exec(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1,$2,$3,$4,$5,$6), ($7,$8,$9,$10,$11,$12)`,
		"processing.bin", int64(0), strings.Repeat("a", 64), filestate.LogicalFileProcessing, int64(0), "v1-simple-rolling",
		"completed.bin", int64(0), strings.Repeat("b", 64), filestate.LogicalFileCompleted, int64(0), "v1-simple-rolling",
	); err != nil {
		t.Fatalf("insert logical_file rows: %v", err)
	}

	stats := &recoveryStats{}
	if err := abortProcessingLogicalFiles(dbconn, stats); err != nil {
		t.Fatalf("abortProcessingLogicalFiles: %v", err)
	}
	if stats.abortedLogicalFiles != 1 {
		t.Fatalf("aborted logical file count mismatch: got=%d want=1", stats.abortedLogicalFiles)
	}
}

func TestAbortProcessingChunksTransitionsOnlyProcessingRows(t *testing.T) {
	dbconn := openRecoveryTestDB(t)
	defer func() { _ = dbconn.Close() }()

	if _, err := dbconn.Exec(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, retry_count, chunker_version)
		 VALUES ($1,$2,$3,$4,$5,$6,$7), ($8,$9,$10,$11,$12,$13,$14)`,
		strings.Repeat("c", 64), int64(1), filestate.ChunkProcessing, int64(0), int64(0), int64(0), "v1-simple-rolling",
		strings.Repeat("d", 64), int64(1), filestate.ChunkCompleted, int64(0), int64(0), int64(0), "v1-simple-rolling",
	); err != nil {
		t.Fatalf("insert chunk rows: %v", err)
	}

	stats := &recoveryStats{}
	if err := abortProcessingChunks(dbconn, stats); err != nil {
		t.Fatalf("abortProcessingChunks: %v", err)
	}
	if stats.abortedChunks != 1 {
		t.Fatalf("aborted chunk count mismatch: got=%d want=1", stats.abortedChunks)
	}
}
