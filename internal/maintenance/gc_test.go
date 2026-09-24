package maintenance

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	dbschema "github.com/franchoy/coldkeep/db"
	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/invariants"
	"github.com/franchoy/coldkeep/internal/retention"
	"github.com/franchoy/coldkeep/internal/snapshot"
	"github.com/franchoy/coldkeep/internal/storage"
	storagecompression "github.com/franchoy/coldkeep/internal/storage/compression"
	"github.com/franchoy/coldkeep/internal/verify"
	"github.com/franchoy/coldkeep/tests/testdb"
)

func requireDB(t *testing.T) {
	t.Helper()
	if os.Getenv("COLDKEEP_TEST_DB") == "" {
		t.Skip("Set COLDKEEP_TEST_DB=1 to run DB-backed maintenance tests")
	}
}

func applySchema(t *testing.T, dbconn *sql.DB) {
	t.Helper()

	var logicalFileTable sql.NullString
	if err := dbconn.QueryRow(`SELECT to_regclass('public.logical_file')`).Scan(&logicalFileTable); err == nil && logicalFileTable.Valid {
		return
	}

	if strings.TrimSpace(dbschema.PostgresSchema) == "" {
		t.Fatalf("embedded postgres schema is empty")
	}

	if _, err := dbconn.Exec(dbschema.PostgresSchema); err != nil && !isDuplicateSchemaError(err) {
		t.Fatalf("apply schema: %v", err)
	}
}

func isDuplicateSchemaError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, sql.ErrNoRows) {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "already exists") || strings.Contains(msg, "42710")
}

func resetDB(t *testing.T, dbconn *sql.DB) {
	t.Helper()
	_, err := dbconn.Exec(`
		TRUNCATE TABLE
			snapshot_path,
			snapshot_file,
			snapshot,
			file_chunk,
			chunk,
			logical_file,
			container
		RESTART IDENTITY CASCADE
	`)
	if err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
}

func seedRootlessRecipeCleanupFixture(t *testing.T, dbconn *sql.DB, suffix string, pinCount int64) (int64, int64) {
	t.Helper()
	var logicalID int64
	if err := dbconn.QueryRow(`
		INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		VALUES ($1, 8, $2, 'COMPLETED', 0, 'v1-simple-rolling') RETURNING id
	`, "rootless-"+suffix+".bin", "rootless-logical-"+suffix).Scan(&logicalID); err != nil {
		t.Fatalf("insert rootless logical recipe: %v", err)
	}
	var chunkID int64
	if err := dbconn.QueryRow(`
		INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version)
		VALUES ($1, 8, 'COMPLETED', 0, $2, 'v1-simple-rolling') RETURNING id
	`, "rootless-chunk-"+suffix, pinCount).Scan(&chunkID); err != nil {
		t.Fatalf("insert rootless recipe chunk: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, 0)`, logicalID, chunkID); err != nil {
		t.Fatalf("insert rootless recipe occurrence: %v", err)
	}
	return logicalID, chunkID
}

func TestCleanupRootlessLogicalRecipesRemovesRecipeBeforeChunkSweep(t *testing.T) {
	dbconn := openMaintenanceSQLiteDB(t)
	defer func() { _ = dbconn.Close() }()
	logicalID, chunkID := seedRootlessRecipeCleanupFixture(t, dbconn, "eligible", 0)

	deleted, err := cleanupRootlessLogicalRecipes(context.Background(), dbconn)
	if err != nil {
		t.Fatalf("cleanup rootless recipe: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted rootless recipes=%d, want 1", deleted)
	}
	var logicalRows, recipeRows, chunkRows int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE id = $1`, logicalID).Scan(&logicalRows); err != nil {
		t.Fatalf("count logical rows: %v", err)
	}
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE logical_file_id = $1`, logicalID).Scan(&recipeRows); err != nil {
		t.Fatalf("count recipe rows: %v", err)
	}
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE id = $1`, chunkID).Scan(&chunkRows); err != nil {
		t.Fatalf("count chunk rows: %v", err)
	}
	if logicalRows != 0 || recipeRows != 0 || chunkRows != 1 {
		t.Fatalf("cleanup order mismatch: logical=%d recipe=%d chunk=%d", logicalRows, recipeRows, chunkRows)
	}
}

func TestCleanupRootlessLogicalRecipesHonorsPinUntilRelease(t *testing.T) {
	dbconn := openMaintenanceSQLiteDB(t)
	defer func() { _ = dbconn.Close() }()
	logicalID, chunkID := seedRootlessRecipeCleanupFixture(t, dbconn, "pinned", 1)

	deleted, err := cleanupRootlessLogicalRecipes(context.Background(), dbconn)
	if err != nil {
		t.Fatalf("cleanup pinned rootless recipe: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("pinned pass deleted %d recipes, want 0", deleted)
	}
	var logicalRows int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM logical_file WHERE id = $1`, logicalID).Scan(&logicalRows); err != nil {
		t.Fatalf("count pinned logical recipe: %v", err)
	}
	if logicalRows != 1 {
		t.Fatalf("pinned logical recipe did not survive: rows=%d", logicalRows)
	}

	if _, err := dbconn.Exec(`UPDATE chunk SET pin_count = 0 WHERE id = $1`, chunkID); err != nil {
		t.Fatalf("release recipe pin: %v", err)
	}
	deleted, err = cleanupRootlessLogicalRecipes(context.Background(), dbconn)
	if err != nil {
		t.Fatalf("cleanup after pin release: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("post-release pass deleted %d recipes, want 1", deleted)
	}
}

func setupAdvisoryLockHeldGCFixture(t *testing.T) (*sql.DB, *sql.DB, string, string, string) {
	t.Helper()

	lockerDB, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect locker db: %v", err)
	}
	t.Cleanup(func() {
		_ = lockerDB.Close()
	})

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	t.Cleanup(func() {
		_ = dbconn.Close()
	})

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	containersDir := t.TempDir()
	originalContainersDir := container.ContainersDir
	t.Cleanup(func() {
		container.ContainersDir = originalContainersDir
	})
	container.ContainersDir = containersDir

	filename := "gc-lock-held.bin"
	containerPath := filepath.Join(containersDir, filename)
	payload := []byte("gc lock held")
	if err := os.WriteFile(containerPath, payload, 0o600); err != nil {
		t.Fatalf("write container file: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
		 VALUES ($1, $2, $3, TRUE, FALSE)`,
		filename,
		int64(len(payload)),
		container.GetContainerMaxSize(),
	); err != nil {
		t.Fatalf("insert container row: %v", err)
	}

	return lockerDB, dbconn, containersDir, filename, containerPath
}

func holdGCAdvisoryLock(t *testing.T, lockerDB *sql.DB) {
	t.Helper()

	conn, err := lockerDB.Conn(context.Background())
	if err != nil {
		t.Fatalf("reserve advisory-lock holder session: %v", err)
	}
	var locked bool
	if err := conn.QueryRowContext(context.Background(), `SELECT pg_try_advisory_lock($1)`, gcAdvisoryLockID).Scan(&locked); err != nil {
		_ = conn.Close()
		t.Fatalf("acquire advisory lock: %v", err)
	}
	if !locked {
		_ = conn.Close()
		t.Fatal("expected to acquire advisory lock in test setup")
	}
	t.Cleanup(func() {
		var unlocked bool
		_ = conn.QueryRowContext(context.Background(), `SELECT pg_advisory_unlock($1)`, gcAdvisoryLockID).Scan(&unlocked)
		_ = conn.Close()
	})
}

func assertGCRefusalPreservesContainerState(t *testing.T, dbconn *sql.DB, containerPath, filename string) {
	t.Helper()

	if _, err := os.Stat(containerPath); err != nil {
		t.Fatalf("expected container file to remain after lock-held refusal, stat err=%v", err)
	}

	var remaining int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container WHERE filename = $1`, filename).Scan(&remaining); err != nil {
		t.Fatalf("count remaining container rows: %v", err)
	}
	if remaining != 1 {
		t.Fatalf("expected metadata to remain after lock-held refusal, got %d rows", remaining)
	}
}

func TestGCAdvisoryLockUsesDedicatedSessionAndReleases(t *testing.T) {
	requireDB(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	observer, err := dbconn.Conn(context.Background())
	if err != nil {
		t.Fatalf("reserve advisory observer session: %v", err)
	}
	defer observer.Close()

	preflightEntered := make(chan struct{})
	resumePreflight := make(chan struct{})
	originalCheck := gcPhysicalIntegrityCheck
	gcPhysicalIntegrityCheck = func(_ *sql.DB) (verify.PhysicalFileIntegritySummary, error) {
		close(preflightEntered)
		<-resumePreflight
		return verify.PhysicalFileIntegritySummary{}, nil
	}
	t.Cleanup(func() { gcPhysicalIntegrityCheck = originalCheck })

	originalUnlock := gcAdvisoryUnlock
	unlockPID := make(chan int, 1)
	gcAdvisoryUnlock = func(ctx context.Context, conn *sql.Conn) (bool, error) {
		var pid int
		if err := conn.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
			return false, err
		}
		unlockPID <- pid
		return originalUnlock(ctx, conn)
	}
	t.Cleanup(func() { gcAdvisoryUnlock = originalUnlock })

	type gcResult struct {
		result GCResult
		err    error
	}
	resultCh := make(chan gcResult, 1)
	go func() {
		result, runErr := RunGCWithDB(context.Background(), dbconn, false, t.TempDir())
		resultCh <- gcResult{result: result, err: runErr}
	}()

	select {
	case <-preflightEntered:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for GC preflight after advisory acquisition")
	}

	var holderPID int
	if err := observer.QueryRowContext(context.Background(), `
		SELECT pid
		FROM pg_locks
		WHERE locktype = 'advisory'
		  AND granted
		  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
		  AND classid::bigint = 0
		  AND objid::bigint = $1
		  AND objsubid = 1
	`, gcAdvisoryLockID).Scan(&holderPID); err != nil {
		t.Fatalf("observe GC advisory-lock holder PID: %v", err)
	}
	if locked := tryGCAdvisoryLockOnConn(t, observer); locked {
		t.Fatal("independent session unexpectedly acquired held GC advisory lock")
	}

	close(resumePreflight)
	select {
	case run := <-resultCh:
		if run.err != nil {
			t.Fatalf("RunGCWithDB: %v", run.err)
		}
		if run.result.DryRun {
			t.Fatalf("expected live GC result, got %+v", run.result)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for GC completion")
	}

	select {
	case pid := <-unlockPID:
		if pid != holderPID {
			t.Fatalf("advisory unlock PID=%d want acquisition PID=%d", pid, holderPID)
		}
	default:
		t.Fatal("advisory unlock did not report its backend PID")
	}
	if locked := tryGCAdvisoryLockOnConn(t, observer); !locked {
		t.Fatal("independent session could not reacquire advisory lock immediately after GC")
	}
	unlockGCAdvisoryLockOnConn(t, observer)
}

func TestRunGCReleasesAdvisoryLockAfterOperationFailure(t *testing.T) {
	requireDB(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	defer dbconn.Close()
	applySchema(t, dbconn)
	resetDB(t, dbconn)

	observer, err := dbconn.Conn(context.Background())
	if err != nil {
		t.Fatalf("reserve advisory observer session: %v", err)
	}
	defer observer.Close()

	operationErr := errors.New("forced GC operation failure")
	originalCheck := gcPhysicalIntegrityCheck
	gcPhysicalIntegrityCheck = func(_ *sql.DB) (verify.PhysicalFileIntegritySummary, error) {
		return verify.PhysicalFileIntegritySummary{}, operationErr
	}
	t.Cleanup(func() { gcPhysicalIntegrityCheck = originalCheck })

	_, runErr := RunGCWithDB(context.Background(), dbconn, false, t.TempDir())
	if !errors.Is(runErr, operationErr) {
		t.Fatalf("RunGCWithDB error=%v want operation failure", runErr)
	}
	if locked := tryGCAdvisoryLockOnConn(t, observer); !locked {
		t.Fatal("independent session could not reacquire advisory lock after GC operation failure")
	}
	unlockGCAdvisoryLockOnConn(t, observer)
}

func TestRunGCAdvisoryCleanupFailureReturnsErrorAndDiscardsSession(t *testing.T) {
	requireDB(t)

	tests := []struct {
		name      string
		unlockErr error
		wantText  string
	}{
		{name: "SQL error", unlockErr: errors.New("forced advisory unlock failure")},
		{name: "false result", wantText: "returned false"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dbconn, err := db.ConnectDB()
			if err != nil {
				t.Fatalf("connect db: %v", err)
			}
			defer dbconn.Close()
			applySchema(t, dbconn)
			resetDB(t, dbconn)
			containersDir := t.TempDir()
			payload := []byte("phase11 unlock partial result")
			filename := "phase11-unlock-success.bin"
			if err := os.WriteFile(filepath.Join(containersDir, filename), payload, 0o600); err != nil {
				t.Fatalf("write successful GC fixture: %v", err)
			}
			if _, err := dbconn.Exec(`
				INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
				VALUES ($1, $2, $3, TRUE, FALSE)
			`, filename, int64(len(payload)), container.GetContainerMaxSize()); err != nil {
				t.Fatalf("insert successful GC fixture: %v", err)
			}

			observerDB, err := db.ConnectDB()
			if err != nil {
				t.Fatalf("connect observer db: %v", err)
			}
			defer observerDB.Close()
			observer, err := observerDB.Conn(context.Background())
			if err != nil {
				t.Fatalf("reserve observer session: %v", err)
			}
			defer observer.Close()

			originalUnlock := gcAdvisoryUnlock
			var discardedPID int
			gcAdvisoryUnlock = func(ctx context.Context, conn *sql.Conn) (bool, error) {
				if err := conn.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&discardedPID); err != nil {
					return false, err
				}
				return false, test.unlockErr
			}
			t.Cleanup(func() { gcAdvisoryUnlock = originalUnlock })

			result, runErr := RunGCWithDB(context.Background(), dbconn, false, containersDir)
			if runErr == nil {
				t.Fatal("expected advisory cleanup failure")
			}
			if test.unlockErr != nil && !errors.Is(runErr, test.unlockErr) {
				t.Fatalf("cleanup error=%v want errors.Is(%v)", runErr, test.unlockErr)
			}
			if test.wantText != "" && !strings.Contains(runErr.Error(), test.wantText) {
				t.Fatalf("cleanup error=%v want text %q", runErr, test.wantText)
			}
			if result.DryRun || result.AffectedContainers != 1 ||
				!reflect.DeepEqual(result.ContainerFilenames, []string{filename}) ||
				result.BytesReclaimed != int64(len(payload)) {
				t.Fatalf("expected completed live GC result to survive unlock failure, got %+v", result)
			}
			if discardedPID == 0 {
				t.Fatal("unlock failure did not observe the owning backend PID")
			}

			var remaining int
			if err := observer.QueryRowContext(context.Background(), `
				SELECT COUNT(*) FROM pg_stat_activity WHERE pid = $1
			`, discardedPID).Scan(&remaining); err != nil {
				t.Fatalf("observe discarded advisory session: %v", err)
			}
			if remaining != 0 {
				t.Fatalf("advisory session PID %d remained active after cleanup failure", discardedPID)
			}
			if locked := tryGCAdvisoryLockOnConn(t, observer); !locked {
				t.Fatal("independent session could not acquire advisory lock after failed cleanup discard")
			}
			unlockGCAdvisoryLockOnConn(t, observer)
		})
	}
}

func TestRunGCLiveRefusesSingleConnectionPool(t *testing.T) {
	requireDB(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	defer dbconn.Close()
	dbconn.SetMaxOpenConns(1)

	_, runErr := RunGCWithDB(context.Background(), dbconn, false, t.TempDir())
	if runErr == nil || !strings.Contains(runErr.Error(), "requires at least two database connections") {
		t.Fatalf("single-connection GC error=%v want explicit pool-capacity refusal", runErr)
	}
}

func tryGCAdvisoryLockOnConn(t *testing.T, conn *sql.Conn) bool {
	t.Helper()
	var locked bool
	if err := conn.QueryRowContext(context.Background(), `SELECT pg_try_advisory_lock($1)`, gcAdvisoryLockID).Scan(&locked); err != nil {
		t.Fatalf("try GC advisory lock: %v", err)
	}
	return locked
}

func unlockGCAdvisoryLockOnConn(t *testing.T, conn *sql.Conn) {
	t.Helper()
	var unlocked bool
	if err := conn.QueryRowContext(context.Background(), `SELECT pg_advisory_unlock($1)`, gcAdvisoryLockID).Scan(&unlocked); err != nil {
		t.Fatalf("unlock GC advisory lock: %v", err)
	}
	if !unlocked {
		t.Fatal("expected observer session to release GC advisory lock")
	}
}

func TestRunGCRefusesWhenAdvisoryLockAlreadyHeld(t *testing.T) {
	requireDB(t)

	lockerDB, dbconn, containersDir, filename, containerPath := setupAdvisoryLockHeldGCFixture(t)
	holdGCAdvisoryLock(t, lockerDB)

	_, gcErr := RunGCWithContainersDirResult(false, containersDir)
	if gcErr == nil {
		t.Fatal("expected gc refusal when advisory lock is held")
	}
	if !strings.Contains(gcErr.Error(), "already running") {
		t.Fatalf("expected lock-held refusal message, got: %v", gcErr)
	}
	assertGCRefusalPreservesContainerState(t, dbconn, containerPath, filename)
}

func TestRunGCRefusesOnPhysicalIntegrityIssues(t *testing.T) {
	// This test stubs gcPhysicalIntegrityCheck to simulate a drifted graph.
	// No DB connection required — the refusal path is exercised before any
	// container work begins.
	requireDB(t)

	originalCheck := gcPhysicalIntegrityCheck
	t.Cleanup(func() { gcPhysicalIntegrityCheck = originalCheck })

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	defer dbconn.Close()

	applySchema(t, dbconn)
	resetDB(t, dbconn)

	gcPhysicalIntegrityCheck = func(_ *sql.DB) (verify.PhysicalFileIntegritySummary, error) {
		return verify.PhysicalFileIntegritySummary{
			OrphanPhysicalFileRows:    0,
			LogicalRefCountMismatches: 3,
			NegativeLogicalRefCounts:  0,
		}, nil
	}

	_, gcErr := RunGCWithContainersDirResult(false, t.TempDir())
	if gcErr == nil {
		t.Fatal("expected GC to be refused but got no error")
	}
	if code, ok := invariants.Code(gcErr); !ok || code != invariants.CodeGCRefusedIntegrity {
		t.Fatalf("expected invariant code %s, got code=%q ok=%v err=%v", invariants.CodeGCRefusedIntegrity, code, ok, gcErr)
	}
	if !strings.Contains(gcErr.Error(), "GC refused") {
		t.Fatalf("expected error to mention 'GC refused', got: %v", gcErr)
	}
	if !strings.Contains(gcErr.Error(), "ref_count_mismatches=3") {
		t.Fatalf("expected error to include mismatch count, got: %v", gcErr)
	}
}

func TestRunGCRefusesOnOrphanPhysicalFileRows(t *testing.T) {
	requireDB(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	defer dbconn.Close()
	applySchema(t, dbconn)
	resetDB(t, dbconn)

	originalCheck := gcPhysicalIntegrityCheck
	t.Cleanup(func() { gcPhysicalIntegrityCheck = originalCheck })
	gcPhysicalIntegrityCheck = func(_ *sql.DB) (verify.PhysicalFileIntegritySummary, error) {
		return verify.PhysicalFileIntegritySummary{OrphanPhysicalFileRows: 2}, nil
	}

	_, gcErr := RunGCWithContainersDirResult(false, t.TempDir())
	if gcErr == nil {
		t.Fatal("expected GC to be refused but got no error")
	}
	if code, ok := invariants.Code(gcErr); !ok || code != invariants.CodeGCRefusedIntegrity {
		t.Fatalf("expected invariant code %s, got code=%q ok=%v err=%v", invariants.CodeGCRefusedIntegrity, code, ok, gcErr)
	}
	if !strings.Contains(gcErr.Error(), "GC refused") {
		t.Fatalf("expected 'GC refused' in error, got: %v", gcErr)
	}
	if !strings.Contains(gcErr.Error(), "orphan_rows=2") {
		t.Fatalf("expected orphan_rows=2 in error, got: %v", gcErr)
	}
}

func TestRunGCRefusesOnNegativeLogicalRefCounts(t *testing.T) {
	requireDB(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	defer dbconn.Close()
	applySchema(t, dbconn)
	resetDB(t, dbconn)

	originalCheck := gcPhysicalIntegrityCheck
	t.Cleanup(func() { gcPhysicalIntegrityCheck = originalCheck })
	gcPhysicalIntegrityCheck = func(_ *sql.DB) (verify.PhysicalFileIntegritySummary, error) {
		return verify.PhysicalFileIntegritySummary{NegativeLogicalRefCounts: 1}, nil
	}

	_, gcErr := RunGCWithContainersDirResult(false, t.TempDir())
	if gcErr == nil {
		t.Fatal("expected GC to be refused but got no error")
	}
	if !strings.Contains(gcErr.Error(), "GC refused") {
		t.Fatalf("expected 'GC refused' in error, got: %v", gcErr)
	}
	if !strings.Contains(gcErr.Error(), "negative_ref_counts=1") {
		t.Fatalf("expected negative_ref_counts=1 in error, got: %v", gcErr)
	}
}

func TestRunGCDryRunRefusesOnDriftedGraph(t *testing.T) {
	// Dry-run GC is subject to the same physical-root integrity pre-flight as
	// real GC. "What would be deleted" is only meaningful on a coherent graph.
	requireDB(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	defer dbconn.Close()
	applySchema(t, dbconn)
	resetDB(t, dbconn)

	originalCheck := gcPhysicalIntegrityCheck
	t.Cleanup(func() { gcPhysicalIntegrityCheck = originalCheck })
	gcPhysicalIntegrityCheck = func(_ *sql.DB) (verify.PhysicalFileIntegritySummary, error) {
		return verify.PhysicalFileIntegritySummary{LogicalRefCountMismatches: 1}, nil
	}

	_, gcErr := RunGCWithContainersDirResult(true /* dryRun */, t.TempDir())
	if gcErr == nil {
		t.Fatal("expected dry-run GC to be refused but got no error")
	}
	if !strings.Contains(gcErr.Error(), "GC refused") {
		t.Fatalf("expected 'GC refused' in dry-run error, got: %v", gcErr)
	}
}

func TestRunGCSucceedsAfterRepairLogicalRefCounts(t *testing.T) {
	// Full operator recovery path:
	// 1. Healthy graph: logical_file + physical_file rows consistent.
	// 2. Corrupt: drift logical_file.ref_count so integrity check fires.
	// 3. GC refuses (real CheckPhysicalFileGraphIntegrity, no stub).
	// 4. Repair: RepairLogicalRefCountsResultWithDB fixes ref_count.
	// 5. GC succeeds (no containers to collect, but no refusal).
	requireDB(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	defer dbconn.Close()
	applySchema(t, dbconn)
	resetDB(t, dbconn)

	// Step 1: insert a consistent logical_file + physical_file pair.
	var logicalID int64
	if err := dbconn.QueryRow(`
		INSERT INTO logical_file (original_name, total_size, file_hash, ref_count, status, chunker_version)
		VALUES ('gc-repair-smoke.bin', 1024, 'aabbcc', 1, 'COMPLETED', 'v1-simple-rolling')
		RETURNING id
	`).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}
	if _, err := dbconn.Exec(`
		INSERT INTO physical_file (path, logical_file_id, is_metadata_complete)
		VALUES ('/data/gc-repair-smoke.bin', $1, TRUE)
	`, logicalID); err != nil {
		t.Fatalf("insert physical_file: %v", err)
	}

	// Step 2: drift ref_count (1→5) to create a mismatch.
	if _, err := dbconn.Exec(`UPDATE logical_file SET ref_count = 5 WHERE id = $1`, logicalID); err != nil {
		t.Fatalf("corrupt ref_count: %v", err)
	}

	// Step 3: GC must be refused.
	if _, gcErr := RunGCWithContainersDirResult(false, t.TempDir()); gcErr == nil {
		t.Fatal("expected GC to be refused on drifted graph but got no error")
	} else if !strings.Contains(gcErr.Error(), "GC refused") {
		t.Fatalf("expected 'GC refused', got: %v", gcErr)
	}

	// Step 4: repair via RepairLogicalRefCountsResultWithDB.
	repairResult, repairErr := RepairLogicalRefCountsResultWithDB(dbconn)
	if repairErr != nil {
		t.Fatalf("RepairLogicalRefCountsResultWithDB: %v", repairErr)
	}
	if repairResult.UpdatedLogicalFiles != 1 {
		t.Fatalf("expected 1 updated logical_file, got %d", repairResult.UpdatedLogicalFiles)
	}

	// Step 5: GC must now succeed (clean graph, no containers to collect).
	gcResult, gcErr := RunGCWithContainersDirResult(false, t.TempDir())
	if gcErr != nil {
		t.Fatalf("GC should succeed after repair, got: %v", gcErr)
	}
	if gcResult.AffectedContainers != 0 {
		t.Fatalf("expected 0 affected containers, got %d", gcResult.AffectedContainers)
	}
}

// setupSnapshotRetainedContainer creates a sealed, empty (live_ref_count == 0)
// container whose sole chunk is logically reachable via a snapshot_file. It
// returns the container ID and filename so callers can assert GC behaviour.
// The file on disk is written to containersDir.
func setupSnapshotRetainedContainer(t *testing.T, dbconn *sql.DB, containersDir string) (containerID int64, filename string) {
	t.Helper()

	// Insert a logical file and its chunk, leaving live_ref_count = 0 to
	// simulate a state where the ref-count model says "reclaimable" but the
	// snapshot layer says "retained".
	var logicalID int64
	if err := dbconn.QueryRow(`
		INSERT INTO logical_file (original_name, total_size, file_hash, ref_count, status, chunker_version)
		VALUES ('snap-retained.bin', 512, 'deadbeef01', 0, 'COMPLETED', 'v1-simple-rolling')
		RETURNING id
	`).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(`
		INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version)
		VALUES ('deadbeef01chunk', 512, 'COMPLETED', 0, 0, 'v1-simple-rolling')
		RETURNING id
	`).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk: %v", err)
	}

	if _, err := dbconn.Exec(`
		INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order)
		VALUES ($1, $2, 0)
	`, logicalID, chunkID); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	filename = "snap-retained.bin"
	containerPath := filepath.Join(containersDir, filename)
	if err := os.WriteFile(containerPath, []byte("snap retained test"), 0o600); err != nil {
		t.Fatalf("write container file: %v", err)
	}

	if err := dbconn.QueryRow(`
		INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
		VALUES ($1, $2, $3, TRUE, FALSE)
		RETURNING id
	`, filename, int64(len("snap retained test")), container.GetContainerMaxSize()).Scan(&containerID); err != nil {
		t.Fatalf("insert container: %v", err)
	}

	if _, err := dbconn.Exec(`
		INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset)
		VALUES ($1, 'plain', 1, 512, 512, $2, 0)
	`, chunkID, containerID); err != nil {
		t.Fatalf("insert blocks: %v", err)
	}

	// Attach a snapshot that retains the logical file.
	if _, err := dbconn.Exec(`
		INSERT INTO snapshot (id, created_at, type) VALUES ('snap-gc-guard-1', NOW(), 'full')
	`); err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
	testdb.InsertSnapshotFileRef(t, dbconn, "snap-gc-guard-1", "/snap/snap-retained.bin", logicalID)

	return containerID, filename
}

// TestRunGCDoesNotDeleteSnapshotRetainedContainer verifies that a sealed
// container whose chunks are reachable from a snapshot is not reclaimed by GC,
// even when all chunk live_ref_counts are zero.
func TestRunGCDoesNotDeleteSnapshotRetainedContainer(t *testing.T) {
	requireDB(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	defer dbconn.Close()
	applySchema(t, dbconn)
	resetDB(t, dbconn)

	containersDir := t.TempDir()
	containerID, filename := setupSnapshotRetainedContainer(t, dbconn, containersDir)

	result, gcErr := RunGCWithContainersDirResult(false, containersDir)
	if gcErr != nil {
		t.Fatalf("GC should succeed: %v", gcErr)
	}
	if result.AffectedContainers != 0 {
		t.Fatalf("expected 0 affected containers (snapshot-retained), got %d", result.AffectedContainers)
	}
	if result.SnapshotRetainedContainers != 1 {
		t.Fatalf("expected 1 snapshot-retained container, got %d", result.SnapshotRetainedContainers)
	}

	// Container row and file must still exist.
	var remaining int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container WHERE id = $1`, containerID).Scan(&remaining); err != nil {
		t.Fatalf("count container rows: %v", err)
	}
	if remaining != 1 {
		t.Fatalf("expected container row to survive GC, got count=%d", remaining)
	}
	if _, err := os.Stat(filepath.Join(containersDir, filename)); err != nil {
		t.Fatalf("expected container file to survive GC: %v", err)
	}
}

// TestRunGCDryRunDoesNotCountSnapshotRetainedContainerAsReclaimable verifies
// that dry-run GC does not flag snapshot-retained containers as reclaimable.
func TestRunGCDryRunDoesNotCountSnapshotRetainedContainerAsReclaimable(t *testing.T) {
	requireDB(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	defer dbconn.Close()
	applySchema(t, dbconn)
	resetDB(t, dbconn)

	containersDir := t.TempDir()
	setupSnapshotRetainedContainer(t, dbconn, containersDir)

	result, gcErr := RunGCWithContainersDirResult(true /* dryRun */, containersDir)
	if gcErr != nil {
		t.Fatalf("dry-run GC should succeed: %v", gcErr)
	}
	if result.AffectedContainers != 0 {
		t.Fatalf("expected 0 reclaimable containers in dry-run (snapshot-retained), got %d", result.AffectedContainers)
	}
	if result.SnapshotRetainedContainers != 1 {
		t.Fatalf("expected 1 snapshot-retained container in dry-run result, got %d", result.SnapshotRetainedContainers)
	}
}

// TestRunGCResultPopulatesSnapshotRetainedLogicalFiles verifies that
// GCResult.SnapshotRetainedLogicalFiles is populated from the reachability
// summary without requiring the container sweep to fire.
func TestRunGCResultPopulatesSnapshotRetainedLogicalFiles(t *testing.T) {
	// Stub gcComputeReachability to return a known set of snapshot-retained IDs.
	originalReachability := gcComputeReachability
	t.Cleanup(func() { gcComputeReachability = originalReachability })

	gcComputeReachability = func(_ context.Context, _ *sql.DB) (*retention.ReachabilitySummary, error) {
		return &retention.ReachabilitySummary{
			CurrentLogicalIDs: map[int64]struct{}{1: {}, 2: {}},
			SnapshotLogicalIDs: map[int64]struct{}{
				3: {},
				4: {},
				5: {},
			},
			RetainedLogicalIDs: map[int64]struct{}{1: {}, 2: {}, 3: {}, 4: {}, 5: {}},
		}, nil
	}

	requireDB(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	defer dbconn.Close()
	applySchema(t, dbconn)
	resetDB(t, dbconn)

	result, gcErr := RunGCWithContainersDirResult(false, t.TempDir())
	if gcErr != nil {
		t.Fatalf("GC should succeed: %v", gcErr)
	}
	if result.SnapshotRetainedLogicalFiles != 3 {
		t.Fatalf("expected SnapshotRetainedLogicalFiles=3, got %d", result.SnapshotRetainedLogicalFiles)
	}
	if result.RetainedCurrentOnlyLogical != 2 {
		t.Fatalf("expected RetainedCurrentOnlyLogical=2, got %d", result.RetainedCurrentOnlyLogical)
	}
	if result.RetainedSnapshotOnlyLogical != 3 {
		t.Fatalf("expected RetainedSnapshotOnlyLogical=3, got %d", result.RetainedSnapshotOnlyLogical)
	}
	if result.RetainedSharedLogical != 0 {
		t.Fatalf("expected RetainedSharedLogical=0, got %d", result.RetainedSharedLogical)
	}
}

// TestRunGCDryRunBecomesEligibleAfterSnapshotDelete verifies that content
// retained only by a deleted snapshot becomes GC-eligible when no other refs
// remain.
func TestRunGCDryRunBecomesEligibleAfterSnapshotDelete(t *testing.T) {
	requireDB(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	defer dbconn.Close()
	applySchema(t, dbconn)
	resetDB(t, dbconn)

	containersDir := t.TempDir()
	setupSnapshotRetainedContainer(t, dbconn, containersDir)

	before, gcErr := RunGCWithContainersDirResult(true /* dryRun */, containersDir)
	if gcErr != nil {
		t.Fatalf("dry-run GC before snapshot delete should succeed: %v", gcErr)
	}
	if before.AffectedContainers != 0 || before.SnapshotRetainedContainers != 1 {
		t.Fatalf("unexpected pre-delete dry-run result: %+v", before)
	}

	if _, err := dbconn.Exec(`DELETE FROM snapshot_file WHERE snapshot_id = 'snap-gc-guard-1'`); err != nil {
		t.Fatalf("delete snapshot_file rows: %v", err)
	}
	if _, err := dbconn.Exec(`DELETE FROM snapshot WHERE id = 'snap-gc-guard-1'`); err != nil {
		t.Fatalf("delete snapshot row: %v", err)
	}

	after, gcErr := RunGCWithContainersDirResult(true /* dryRun */, containersDir)
	if gcErr != nil {
		t.Fatalf("dry-run GC after snapshot delete should succeed: %v", gcErr)
	}
	if after.AffectedContainers != 1 {
		t.Fatalf("expected 1 reclaimable container after snapshot delete, got %d", after.AffectedContainers)
	}
	if after.SnapshotRetainedContainers != 0 {
		t.Fatalf("expected 0 snapshot-retained containers after snapshot delete, got %d", after.SnapshotRetainedContainers)
	}
}

// TestRunGCDryRunRetainsContainerWhenAnotherSnapshotStillReferences verifies
// shared retention semantics across multiple snapshots for the same logical
// file.
func TestRunGCDryRunRetainsContainerWhenAnotherSnapshotStillReferences(t *testing.T) {
	requireDB(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	defer dbconn.Close()
	applySchema(t, dbconn)
	resetDB(t, dbconn)

	containersDir := t.TempDir()
	setupSnapshotRetainedContainer(t, dbconn, containersDir)

	var logicalID int64
	if err := dbconn.QueryRow(`SELECT id FROM logical_file WHERE original_name = 'snap-retained.bin' LIMIT 1`).Scan(&logicalID); err != nil {
		t.Fatalf("lookup retained logical_file: %v", err)
	}

	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES ('snap-gc-guard-2', NOW(), 'full')`); err != nil {
		t.Fatalf("insert second snapshot: %v", err)
	}
	testdb.InsertSnapshotFileRef(t, dbconn, "snap-gc-guard-2", "/snap/also-retained.bin", logicalID)

	if _, err := dbconn.Exec(`DELETE FROM snapshot_file WHERE snapshot_id = 'snap-gc-guard-1'`); err != nil {
		t.Fatalf("delete first snapshot_file rows: %v", err)
	}
	if _, err := dbconn.Exec(`DELETE FROM snapshot WHERE id = 'snap-gc-guard-1'`); err != nil {
		t.Fatalf("delete first snapshot row: %v", err)
	}

	result, gcErr := RunGCWithContainersDirResult(true /* dryRun */, containersDir)
	if gcErr != nil {
		t.Fatalf("dry-run GC should succeed: %v", gcErr)
	}
	if result.AffectedContainers != 0 {
		t.Fatalf("expected 0 reclaimable containers while second snapshot still retains content, got %d", result.AffectedContainers)
	}
	if result.SnapshotRetainedContainers != 1 {
		t.Fatalf("expected 1 snapshot-retained container via second snapshot, got %d", result.SnapshotRetainedContainers)
	}
}

func TestGCReachabilityMatchesLegacy(t *testing.T) {
	requireDB(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	defer dbconn.Close()
	applySchema(t, dbconn)
	resetDB(t, dbconn)

	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES ('1', NOW(), 'full'), ('2', NOW(), 'full')`); err != nil {
		t.Fatalf("insert snapshots: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES ('docs/a.txt'), ('docs/b.txt')`); err != nil {
		t.Fatalf("insert snapshot_path rows: %v", err)
	}

	var lfAID, lfBID int64
	if err := dbconn.QueryRow(`
		INSERT INTO logical_file (original_name, total_size, file_hash, ref_count, status, chunker_version)
		VALUES ('a.txt', 100, 'lf-a', 0, 'COMPLETED', 'v2-fastcdc') RETURNING id
	`).Scan(&lfAID); err != nil {
		t.Fatalf("insert logical_file a: %v", err)
	}
	if err := dbconn.QueryRow(`
		INSERT INTO logical_file (original_name, total_size, file_hash, ref_count, status, chunker_version)
		VALUES ('b.txt', 100, 'lf-b', 0, 'COMPLETED', 'v2-fastcdc') RETURNING id
	`).Scan(&lfBID); err != nil {
		t.Fatalf("insert logical_file b: %v", err)
	}

	if _, err := dbconn.Exec(`
		INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id)
		VALUES
		  ('1', (SELECT id FROM snapshot_path WHERE path = 'docs/a.txt'), $1),
		  ('2', (SELECT id FROM snapshot_path WHERE path = 'docs/b.txt'), $2)
	`, lfAID, lfBID); err != nil {
		t.Fatalf("insert snapshot_file rows: %v", err)
	}

	var chunkAID, chunkBID, chunkSharedID int64
	if err := dbconn.QueryRow(`
		INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version)
		VALUES ('chunk-a', 40, 'COMPLETED', 1, 0, 'v2-fastcdc') RETURNING id
	`).Scan(&chunkAID); err != nil {
		t.Fatalf("insert chunk a: %v", err)
	}
	if err := dbconn.QueryRow(`
		INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version)
		VALUES ('chunk-b', 50, 'COMPLETED', 1, 0, 'v2-fastcdc') RETURNING id
	`).Scan(&chunkBID); err != nil {
		t.Fatalf("insert chunk b: %v", err)
	}
	if err := dbconn.QueryRow(`
		INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version)
		VALUES ('chunk-shared', 60, 'COMPLETED', 2, 0, 'v2-fastcdc') RETURNING id
	`).Scan(&chunkSharedID); err != nil {
		t.Fatalf("insert shared chunk: %v", err)
	}

	if _, err := dbconn.Exec(`
		INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order)
		VALUES
		  ($1, $2, 0),
		  ($1, $4, 1),
		  ($3, $4, 0),
		  ($3, $5, 1)
	`, lfAID, chunkAID, lfBID, chunkSharedID, chunkBID); err != nil {
		t.Fatalf("insert file_chunk rows: %v", err)
	}

	graphSet, err := MarkReachableChunks(context.Background(), dbconn)
	if err != nil {
		t.Fatalf("MarkReachableChunks: %v", err)
	}

	legacySet, err := legacyMarkReachableChunksForTest(context.Background(), dbconn)
	if err != nil {
		t.Fatalf("legacy mark reachable chunks: %v", err)
	}

	if len(graphSet) != len(legacySet) {
		t.Fatalf("reachable set size mismatch: graph=%d legacy=%d", len(graphSet), len(legacySet))
	}
	for id := range legacySet {
		if _, ok := graphSet[id]; !ok {
			t.Fatalf("graph set missing legacy chunk id %d", id)
		}
	}
	for id := range graphSet {
		if _, ok := legacySet[id]; !ok {
			t.Fatalf("graph set has unexpected chunk id %d", id)
		}
	}
}

func legacyMarkReachableChunksForTest(ctx context.Context, dbconn *sql.DB) (map[int64]struct{}, error) {
	rows, err := dbconn.QueryContext(ctx, `
		SELECT DISTINCT fc.chunk_id
		FROM snapshot_file sf
		JOIN file_chunk fc ON fc.logical_file_id = sf.logical_file_id
		ORDER BY fc.chunk_id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[int64]struct{})
	for rows.Next() {
		var chunkID int64
		if err := rows.Scan(&chunkID); err != nil {
			return nil, err
		}
		out[chunkID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

// TestRunGCDryRunCurrentAndSnapshotSharedRetentionSurvivesCurrentDelete
// verifies that removing only current-state mapping does not make content
// collectible while snapshot retention remains.
func TestRunGCDryRunCurrentAndSnapshotSharedRetentionSurvivesCurrentDelete(t *testing.T) {
	requireDB(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	defer dbconn.Close()
	applySchema(t, dbconn)
	resetDB(t, dbconn)

	containersDir := t.TempDir()
	setupSnapshotRetainedContainer(t, dbconn, containersDir)

	var logicalID int64
	if err := dbconn.QueryRow(`SELECT id FROM logical_file WHERE original_name = 'snap-retained.bin' LIMIT 1`).Scan(&logicalID); err != nil {
		t.Fatalf("lookup retained logical_file: %v", err)
	}

	if _, err := dbconn.Exec(`
		INSERT INTO physical_file (path, logical_file_id, is_metadata_complete)
		VALUES ('/data/shared-current-and-snapshot.bin', $1, TRUE)
	`, logicalID); err != nil {
		t.Fatalf("insert current-state mapping: %v", err)
	}
	// Keep ref_count in sync: the logical_file was created with ref_count=0 in
	// setupSnapshotRetainedContainer; adding a physical_file row must increment it.
	if _, err := dbconn.Exec(`UPDATE logical_file SET ref_count = ref_count + 1 WHERE id = $1`, logicalID); err != nil {
		t.Fatalf("increment ref_count for added physical_file: %v", err)
	}

	before, gcErr := RunGCWithContainersDirResult(true /* dryRun */, containersDir)
	if gcErr != nil {
		t.Fatalf("dry-run GC before removing current mapping should succeed: %v", gcErr)
	}
	if before.AffectedContainers != 0 {
		t.Fatalf("expected 0 reclaimable containers before removing current mapping, got %d", before.AffectedContainers)
	}

	if _, err := dbconn.Exec(`DELETE FROM physical_file WHERE logical_file_id = $1`, logicalID); err != nil {
		t.Fatalf("delete current-state mapping: %v", err)
	}
	// Decrement ref_count to match the deleted physical_file rows.
	if _, err := dbconn.Exec(`UPDATE logical_file SET ref_count = 0 WHERE id = $1`, logicalID); err != nil {
		t.Fatalf("reset ref_count after deleting physical_file: %v", err)
	}

	after, gcErr := RunGCWithContainersDirResult(true /* dryRun */, containersDir)
	if gcErr != nil {
		t.Fatalf("dry-run GC after removing current mapping should succeed: %v", gcErr)
	}
	if after.AffectedContainers != 0 {
		t.Fatalf("expected 0 reclaimable containers after removing current mapping (snapshot still retains), got %d", after.AffectedContainers)
	}
	if after.SnapshotRetainedContainers != 1 {
		t.Fatalf("expected 1 snapshot-retained container after removing current mapping, got %d", after.SnapshotRetainedContainers)
	}
}

func TestRunGCSnapshotRetainsPackedBlockAndRestoreSucceedsWhenLiveNamespaceRemoved(t *testing.T) {
	requireDB(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	defer dbconn.Close()
	applySchema(t, dbconn)
	resetDB(t, dbconn)

	containersDir := t.TempDir()

	retainedPayload := []byte("snapshot-packed-retained")
	deadPayload := []byte("dead-neighbor-chunk")

	retainedChunkHash := sha256.Sum256(retainedPayload)
	deadChunkHash := sha256.Sum256(deadPayload)

	var logicalID int64
	if err := dbconn.QueryRow(`
		INSERT INTO logical_file (original_name, total_size, file_hash, ref_count, status, chunker_version)
		VALUES ('A.bin', $1, $2, 0, 'COMPLETED', 'v2-fastcdc')
		RETURNING id
	`, int64(len(retainedPayload)), hex.EncodeToString(retainedChunkHash[:])).Scan(&logicalID); err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}

	var retainedChunkID int64
	if err := dbconn.QueryRow(`
		INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version)
		VALUES ($1, $2, 'COMPLETED', 0, 0, 'v2-fastcdc')
		RETURNING id
	`, hex.EncodeToString(retainedChunkHash[:]), int64(len(retainedPayload))).Scan(&retainedChunkID); err != nil {
		t.Fatalf("insert retained chunk: %v", err)
	}

	var deadChunkID int64
	if err := dbconn.QueryRow(`
		INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version)
		VALUES ($1, $2, 'COMPLETED', 0, 0, 'v2-fastcdc')
		RETURNING id
	`, hex.EncodeToString(deadChunkHash[:]), int64(len(deadPayload))).Scan(&deadChunkID); err != nil {
		t.Fatalf("insert dead chunk: %v", err)
	}

	if _, err := dbconn.Exec(`
		INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order)
		VALUES ($1, $2, 0)
	`, logicalID, retainedChunkID); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	encodedBlock, packedBlockHash := step11EncodePackedBlockV1(t, []int64{retainedChunkID, deadChunkID}, retainedPayload, deadPayload)

	containerFilename := "snapshot-packed-retained.bin"
	containerPath := filepath.Join(containersDir, containerFilename)
	if err := writeTestContainerFileWithPayload(containerPath, encodedBlock); err != nil {
		t.Fatalf("write container file: %v", err)
	}

	var containerID int64
	if err := dbconn.QueryRow(`
		INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
		VALUES ($1, $2, $3, TRUE, FALSE)
		RETURNING id
	`, containerFilename, int64(container.ContainerHdrLen+len(encodedBlock)), container.GetContainerMaxSize()).Scan(&containerID); err != nil {
		t.Fatalf("insert container: %v", err)
	}

	var storageBlockID int64
	if err := dbconn.QueryRow(`
		INSERT INTO storage_blocks (format_version, codec, plaintext_size, stored_size, container_id, container_offset, block_hash, physical_hash)
		VALUES (1, 'none', $1, $2, $3, $4, $5, $6)
		RETURNING id
	`, int64(len(encodedBlock)), int64(len(encodedBlock)), containerID, int64(container.ContainerHdrLen), packedBlockHash, blocks.HashPhysical(encodedBlock)).Scan(&storageBlockID); err != nil {
		t.Fatalf("insert packed storage block: %v", err)
	}

	if _, err := dbconn.Exec(`
		INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block)
		VALUES ($1, $2, 0, $3)
	`, retainedChunkID, storageBlockID, int64(len(retainedPayload))); err != nil {
		t.Fatalf("insert packed retained ref: %v", err)
	}
	if _, err := dbconn.Exec(`
		INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block)
		VALUES ($1, $2, $3, $4)
	`, deadChunkID, storageBlockID, int64(len(retainedPayload)), int64(len(deadPayload))); err != nil {
		t.Fatalf("insert packed dead ref: %v", err)
	}
	step11InsertLegacyCompanionRows(
		t,
		dbconn,
		containerID,
		int64(container.ContainerHdrLen),
		len(encodedBlock),
		[]int64{retainedChunkID, deadChunkID},
		retainedPayload,
		deadPayload,
	)

	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES ('S1', NOW(), 'full')`); err != nil {
		t.Fatalf("insert snapshot S1: %v", err)
	}
	testdb.InsertSnapshotFileRef(t, dbconn, "S1", "snap/A.bin", logicalID)

	// File A removed from live namespace: no physical_file rows, logical ref_count stays 0.
	if _, err := dbconn.Exec(`DELETE FROM physical_file WHERE logical_file_id = $1`, logicalID); err != nil {
		t.Fatalf("delete physical_file rows for logical: %v", err)
	}
	if _, err := dbconn.Exec(`UPDATE logical_file SET ref_count = 0 WHERE id = $1`, logicalID); err != nil {
		t.Fatalf("set logical ref_count to 0: %v", err)
	}

	gcResult, gcErr := RunGCWithContainersDirResult(false, containersDir)
	if gcErr != nil {
		t.Fatalf("GC should succeed: %v", gcErr)
	}
	if gcResult.AffectedContainers != 0 {
		t.Fatalf("expected 0 affected containers (snapshot-retained), got %d", gcResult.AffectedContainers)
	}
	if gcResult.SnapshotRetainedContainers != 1 {
		t.Fatalf("expected 1 snapshot-retained container, got %d", gcResult.SnapshotRetainedContainers)
	}

	var packedBlockCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks WHERE id = $1`, storageBlockID).Scan(&packedBlockCount); err != nil {
		t.Fatalf("count packed block after GC: %v", err)
	}
	if packedBlockCount != 1 {
		t.Fatalf("expected packed block to remain live via snapshot-retained chunk, got count=%d", packedBlockCount)
	}

	restoreDir := t.TempDir()
	sgctx := storage.StorageContext{DB: dbconn, ContainerDir: containersDir}
	restoreResult, err := snapshot.RestoreSnapshot(context.Background(), dbconn, "S1", nil, snapshot.RestoreSnapshotOptions{
		DestinationMode: storage.RestoreDestinationPrefix,
		Destination:     restoreDir,
		Overwrite:       true,
		NoMetadata:      true,
		StorageContext:  &sgctx,
	})
	if err != nil {
		t.Fatalf("restore from snapshot S1: %v", err)
	}
	if restoreResult.RestoredFiles != 1 {
		t.Fatalf("expected one restored file, got %d", restoreResult.RestoredFiles)
	}

	restoreTarget := filepath.Join(restoreDir, "snap", "A.bin")
	restored, err := os.ReadFile(restoreTarget)
	if err != nil {
		t.Fatalf("read restored file: %v", err)
	}
	if string(restored) != string(retainedPayload) {
		t.Fatalf("restored payload mismatch: got %q want %q", string(restored), string(retainedPayload))
	}
}

func writeTestContainerFileWithPayload(path string, payload []byte) error {
	hdr := make([]byte, container.ContainerHdrLen)
	copy(hdr[0:8], []byte(container.ContainerMagic))
	binary.LittleEndian.PutUint16(hdr[8:10], container.LegacyContainerFormatVersionMajor)
	binary.LittleEndian.PutUint16(hdr[10:12], 9)
	binary.LittleEndian.PutUint32(hdr[12:16], uint32(container.ContainerHdrLen))
	binary.LittleEndian.PutUint64(hdr[28:36], uint64(container.GetContainerMaxSize()))
	binary.LittleEndian.PutUint32(hdr[52:56], crc32.ChecksumIEEE(hdr[0:52]))

	buf := append(hdr, payload...)
	return os.WriteFile(path, buf, 0o600)
}

func insertPackedStorageBlockFixtureWithCompression(t *testing.T, dbconn *sql.DB, containersDir, containerFilename string, chunkIDs []int64, chunkPayloads [][]byte, compressionCodec string) (int64, int64) {
	t.Helper()

	encodedBlock, logicalHash := step11EncodePackedBlockV1(t, chunkIDs, chunkPayloads...)

	storedPayload := encodedBlock
	compressionLevel := any(nil)
	if compressionCodec == storagecompression.CompressionZstd {
		compressor, err := storagecompression.NewZstdCompressor(3)
		if err != nil {
			t.Fatalf("new zstd compressor: %v", err)
		}
		compressed, err := compressor.Compress(encodedBlock)
		if err != nil {
			t.Fatalf("compress encoded packed block: %v", err)
		}
		storedPayload = compressed
		compressionLevel = 3
	}

	containerPath := filepath.Join(containersDir, containerFilename)
	if err := writeTestContainerFileWithPayload(containerPath, storedPayload); err != nil {
		t.Fatalf("write container file: %v", err)
	}

	var containerID int64
	if err := dbconn.QueryRow(`
		INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
		VALUES ($1, $2, $3, TRUE, FALSE)
		RETURNING id
	`, containerFilename, int64(container.ContainerHdrLen+len(storedPayload)), container.GetContainerMaxSize()).Scan(&containerID); err != nil {
		t.Fatalf("insert container: %v", err)
	}

	var storageBlockID int64
	if err := dbconn.QueryRow(`
		INSERT INTO storage_blocks (
			format_version, codec, plaintext_size, stored_size, container_id, container_offset,
			block_hash, compression_codec, compression_level, compressed_size, compressed_hash, physical_hash
		)
		VALUES (1, 'none', $1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		RETURNING id
	`, int64(len(encodedBlock)), int64(len(storedPayload)), containerID, int64(container.ContainerHdrLen), logicalHash,
		compressionCodec, compressionLevel, int64(len(storedPayload)), blocks.HashCompressed(storedPayload), blocks.HashPhysical(storedPayload)).Scan(&storageBlockID); err != nil {
		t.Fatalf("insert packed storage block fixture: %v", err)
	}

	offset := int64(0)
	for i, payload := range chunkPayloads {
		if _, err := dbconn.Exec(`
			INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block)
			VALUES ($1, $2, $3, $4)
		`, chunkIDs[i], storageBlockID, offset, int64(len(payload))); err != nil {
			t.Fatalf("insert chunk_block_refs: %v", err)
		}
		offset += int64(len(payload))
	}

	step11InsertLegacyCompanionRows(t, dbconn, containerID, int64(container.ContainerHdrLen), len(storedPayload), chunkIDs, chunkPayloads...)

	return containerID, storageBlockID
}

func TestRunGCMixedCompressionAgnosticPackedReachabilityAndRestoreStep311(t *testing.T) {
	requireDB(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	defer dbconn.Close()
	applySchema(t, dbconn)
	resetDB(t, dbconn)

	containersDir := t.TempDir()

	// Uncompressed snapshot-retained legacy container (existing behavior baseline).
	legacyLiveContainerID, _ := setupSnapshotRetainedContainer(t, dbconn, containersDir)

	// Compressed snapshot-retained packed block.
	compressedLivePayload := []byte("step311-compressed-live")
	compressedLiveHash := sha256.Sum256(compressedLivePayload)

	var compressedLiveFileID int64
	if err := dbconn.QueryRow(`
		INSERT INTO logical_file (original_name, total_size, file_hash, ref_count, status, chunker_version)
		VALUES ('step311-live-compressed.bin', $1, $2, 0, 'COMPLETED', 'v2-fastcdc')
		RETURNING id
	`, int64(len(compressedLivePayload)), hex.EncodeToString(compressedLiveHash[:])).Scan(&compressedLiveFileID); err != nil {
		t.Fatalf("insert compressed live logical file: %v", err)
	}

	var compressedLiveChunkID int64
	if err := dbconn.QueryRow(`
		INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version)
		VALUES ($1, $2, 'COMPLETED', 0, 0, 'v2-fastcdc')
		RETURNING id
	`, hex.EncodeToString(compressedLiveHash[:]), int64(len(compressedLivePayload))).Scan(&compressedLiveChunkID); err != nil {
		t.Fatalf("insert compressed live chunk: %v", err)
	}

	if _, err := dbconn.Exec(`
		INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order)
		VALUES ($1, $2, 0)
	`, compressedLiveFileID, compressedLiveChunkID); err != nil {
		t.Fatalf("insert compressed live file_chunk: %v", err)
	}

	compressedLiveContainerID, compressedLiveBlockID := insertPackedStorageBlockFixtureWithCompression(
		t,
		dbconn,
		containersDir,
		"step311-compressed-live.bin",
		[]int64{compressedLiveChunkID},
		[][]byte{compressedLivePayload},
		storagecompression.CompressionZstd,
	)

	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES ('S311-live', NOW(), 'full')`); err != nil {
		t.Fatalf("insert snapshot S311-live: %v", err)
	}
	testdb.InsertSnapshotFileRef(t, dbconn, "S311-live", "snap/live-compressed.bin", compressedLiveFileID)
	if _, err := dbconn.Exec(`DELETE FROM physical_file WHERE logical_file_id = $1`, compressedLiveFileID); err != nil {
		t.Fatalf("delete compressed live physical_file rows: %v", err)
	}

	// Compressed orphan packed block (must be collectable).
	orphanPayload := []byte("step311-compressed-orphan")
	orphanHash := sha256.Sum256(orphanPayload)
	var orphanChunkID int64
	if err := dbconn.QueryRow(`
		INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version)
		VALUES ($1, $2, 'COMPLETED', 0, 0, 'v2-fastcdc')
		RETURNING id
	`, hex.EncodeToString(orphanHash[:]), int64(len(orphanPayload))).Scan(&orphanChunkID); err != nil {
		t.Fatalf("insert compressed orphan chunk: %v", err)
	}
	const orphanFilename = "step311-compressed-orphan.bin"
	orphanContainerID, orphanBlockID := insertPackedStorageBlockFixtureWithCompression(
		t,
		dbconn,
		containersDir,
		orphanFilename,
		[]int64{orphanChunkID},
		[][]byte{orphanPayload},
		storagecompression.CompressionZstd,
	)
	orphanPhysicalInfo, err := os.Stat(filepath.Join(containersDir, orphanFilename))
	if err != nil {
		t.Fatalf("Stat compressed orphan before GC: %v", err)
	}

	gcResult, gcErr := RunGCWithContainersDirResult(false, containersDir)
	if gcErr != nil {
		t.Fatalf("GC should succeed: %v", gcErr)
	}
	if gcResult.AffectedContainers < 1 {
		t.Fatalf("expected at least one affected container (compressed orphan), got %d", gcResult.AffectedContainers)
	}
	if gcResult.BytesReclaimed != orphanPhysicalInfo.Size() {
		t.Fatalf("compressed orphan reclaimed bytes=%d, want independently observed %d", gcResult.BytesReclaimed, orphanPhysicalInfo.Size())
	}
	if gcResult.SnapshotRetainedContainers < 2 {
		t.Fatalf("expected at least two snapshot-retained containers (compressed + uncompressed), got %d", gcResult.SnapshotRetainedContainers)
	}

	var orphanContainerCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container WHERE id = $1`, orphanContainerID).Scan(&orphanContainerCount); err != nil {
		t.Fatalf("count orphan container after GC: %v", err)
	}
	if orphanContainerCount != 0 {
		t.Fatalf("expected compressed orphan container to be reclaimed, still present")
	}
	var orphanBlockCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks WHERE id = $1`, orphanBlockID).Scan(&orphanBlockCount); err != nil {
		t.Fatalf("count orphan storage block after GC: %v", err)
	}
	if orphanBlockCount != 0 {
		t.Fatalf("expected compressed orphan storage block to be reclaimed, still present")
	}

	var compressedLiveBlockCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM storage_blocks WHERE id = $1`, compressedLiveBlockID).Scan(&compressedLiveBlockCount); err != nil {
		t.Fatalf("count compressed live block after GC: %v", err)
	}
	if compressedLiveBlockCount != 1 {
		t.Fatalf("expected compressed live block to remain, got count=%d", compressedLiveBlockCount)
	}
	var compressedLiveContainerCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container WHERE id = $1`, compressedLiveContainerID).Scan(&compressedLiveContainerCount); err != nil {
		t.Fatalf("count compressed live container after GC: %v", err)
	}
	if compressedLiveContainerCount != 1 {
		t.Fatalf("expected compressed live container to remain, got count=%d", compressedLiveContainerCount)
	}
	var legacyLiveContainerCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container WHERE id = $1`, legacyLiveContainerID).Scan(&legacyLiveContainerCount); err != nil {
		t.Fatalf("count uncompressed legacy live container after GC: %v", err)
	}
	if legacyLiveContainerCount != 1 {
		t.Fatalf("expected uncompressed snapshot-retained container to remain, got count=%d", legacyLiveContainerCount)
	}

	restoreDir := t.TempDir()
	sgctx := storage.StorageContext{DB: dbconn, ContainerDir: containersDir}
	restoreResult, err := snapshot.RestoreSnapshot(context.Background(), dbconn, "S311-live", nil, snapshot.RestoreSnapshotOptions{
		DestinationMode: storage.RestoreDestinationPrefix,
		Destination:     restoreDir,
		Overwrite:       true,
		NoMetadata:      true,
		StorageContext:  &sgctx,
	})
	if err != nil {
		t.Fatalf("restore compressed snapshot after GC: %v", err)
	}
	if restoreResult.RestoredFiles != 1 {
		t.Fatalf("expected one restored file from compressed snapshot, got %d", restoreResult.RestoredFiles)
	}

	restored, err := os.ReadFile(filepath.Join(restoreDir, "snap", "live-compressed.bin"))
	if err != nil {
		t.Fatalf("read restored compressed snapshot file: %v", err)
	}
	if string(restored) != string(compressedLivePayload) {
		t.Fatalf("restored compressed payload mismatch: got %q want %q", string(restored), string(compressedLivePayload))
	}
}
