package maintenance_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	dbschema "github.com/franchoy/coldkeep/db"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/observability"
)

func requireParityDB(t *testing.T) {
	t.Helper()
	if os.Getenv("COLDKEEP_TEST_DB") == "" {
		t.Skip("Set COLDKEEP_TEST_DB=1 to run DB-backed parity tests")
	}
}

func applyParitySchema(t *testing.T, dbconn *sql.DB) {
	t.Helper()

	var logicalFileTable sql.NullString
	if err := dbconn.QueryRow(`SELECT to_regclass('public.logical_file')`).Scan(&logicalFileTable); err == nil && logicalFileTable.Valid {
		return
	}

	if strings.TrimSpace(dbschema.PostgresSchema) == "" {
		t.Fatalf("embedded postgres schema is empty")
	}
	if _, err := dbconn.Exec(dbschema.PostgresSchema); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
}

func resetParityDB(t *testing.T, dbconn *sql.DB) {
	t.Helper()
	if _, err := dbconn.Exec(`
		TRUNCATE TABLE
			snapshot_file,
			snapshot,
			file_chunk,
			chunk,
			logical_file,
			container
		RESTART IDENTITY CASCADE
	`); err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
}

func setupParityRunEnv(t *testing.T) (*sql.DB, string) {
	t.Helper()

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	t.Cleanup(func() {
		_ = dbconn.Close()
	})

	applyParitySchema(t, dbconn)
	resetParityDB(t, dbconn)

	containersDir := t.TempDir()
	originalContainersDir := container.ContainersDir
	t.Cleanup(func() {
		container.ContainersDir = originalContainersDir
	})
	container.ContainersDir = containersDir

	return dbconn, containersDir
}

func insertDeadContainerFixture(t *testing.T, dbconn *sql.DB, containersDir, filename string, payload []byte, chunkHash string) (int64, int64, string) {
	t.Helper()

	containerPath := filepath.Join(containersDir, filename)
	if err := os.WriteFile(containerPath, payload, 0o600); err != nil {
		t.Fatalf("write container file %s: %v", filename, err)
	}

	var containerID int64
	if err := dbconn.QueryRow(
		`INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
		 VALUES ($1, $2, $3, TRUE, FALSE)
		 RETURNING id`,
		filename,
		int64(len(payload)),
		container.GetContainerMaxSize(),
	).Scan(&containerID); err != nil {
		t.Fatalf("insert container %s: %v", filename, err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version)
		 VALUES ($1, $2, 'COMPLETED', 0, 0, 'v2-fastcdc')
		 RETURNING id`,
		chunkHash,
		int64(len(payload)),
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk for %s: %v", filename, err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset)
		 VALUES ($1, 'plain', 1, $2, $3, $4, 0)`,
		chunkID,
		int64(len(payload)),
		int64(len(payload)),
		containerID,
	); err != nil {
		t.Fatalf("insert block for %s: %v", filename, err)
	}

	return containerID, chunkID, containerPath
}

func assertFileExistsState(t *testing.T, path string, shouldExist bool) {
	t.Helper()

	_, err := os.Stat(path)
	if shouldExist {
		if err != nil {
			t.Fatalf("expected %s to exist, stat err=%v", path, err)
		}
		return
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected %s to be absent, stat err=%v", path, err)
	}
}

func assertContainerRowCountByID(t *testing.T, dbconn *sql.DB, containerID int64, want int) {
	t.Helper()

	var got int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container WHERE id = $1`, containerID).Scan(&got); err != nil {
		t.Fatalf("count container rows: %v", err)
	}
	if got != want {
		t.Fatalf("container row count = %d, want %d", got, want)
	}
}

func assertFilenameListEqual(t *testing.T, got, want []string) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("filename count mismatch: got=%v want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("filename[%d] mismatch: got=%q want=%q", i, got[i], want[i])
		}
	}
}

func TestSimulateGCMatchesActualGCDeletion(t *testing.T) {
	requireParityDB(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	defer dbconn.Close()

	applyParitySchema(t, dbconn)
	resetParityDB(t, dbconn)

	containersDir := t.TempDir()
	originalContainersDir := container.ContainersDir
	t.Cleanup(func() {
		container.ContainersDir = originalContainersDir
	})
	container.ContainersDir = containersDir

	payload := []byte("gc parity payload")
	filename := "gc-parity.bin"
	containerPath := filepath.Join(containersDir, filename)
	if err := os.WriteFile(containerPath, payload, 0o600); err != nil {
		t.Fatalf("write container file: %v", err)
	}

	var containerID int64
	if err := dbconn.QueryRow(
		`INSERT INTO container (filename, current_size, max_size, sealed, quarantine)
		 VALUES ($1, $2, $3, TRUE, FALSE)
		 RETURNING id`,
		filename,
		int64(len(payload)),
		container.GetContainerMaxSize(),
	).Scan(&containerID); err != nil {
		t.Fatalf("insert container row: %v", err)
	}

	var chunkID int64
	if err := dbconn.QueryRow(
		`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, chunker_version)
		 VALUES ($1, $2, 'COMPLETED', 0, 0, 'v2-fastcdc')
		 RETURNING id`,
		"gc-parity-chunk",
		int64(len(payload)),
	).Scan(&chunkID); err != nil {
		t.Fatalf("insert chunk row: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset)
		 VALUES ($1, 'plain', 1, $2, $3, $4, 0)`,
		chunkID,
		int64(len(payload)),
		int64(len(payload)),
		containerID,
	); err != nil {
		t.Fatalf("insert block row: %v", err)
	}

	svc, err := observability.NewService(dbconn)
	if err != nil {
		t.Fatalf("new observability service: %v", err)
	}

	simulated, err := svc.Simulate(context.Background(), observability.SimulationOptions{Kind: observability.SimulationKindGC})
	if err != nil {
		t.Fatalf("simulate gc: %v", err)
	}
	if simulated == nil || simulated.GC == nil {
		t.Fatal("expected gc simulation result")
	}
	if simulated.GC.Summary.FullyReclaimableContainers != 1 {
		t.Fatalf("expected one fully reclaimable container, got %d", simulated.GC.Summary.FullyReclaimableContainers)
	}
	if len(simulated.GC.Containers) != 1 {
		t.Fatalf("expected one simulated container, got %d", len(simulated.GC.Containers))
	}
	if simulated.GC.Containers[0].Filename != filename {
		t.Fatalf("simulated filename = %q, want %q", simulated.GC.Containers[0].Filename, filename)
	}

	gcResult, err := maintenance.RunGCWithContainersDirResult(false, containersDir)
	if err != nil {
		t.Fatalf("run actual gc: %v", err)
	}
	if gcResult.AffectedContainers != int(simulated.GC.Summary.FullyReclaimableContainers) {
		t.Fatalf("affected containers = %d, want %d", gcResult.AffectedContainers, simulated.GC.Summary.FullyReclaimableContainers)
	}
	if len(gcResult.ContainerFilenames) != 1 || gcResult.ContainerFilenames[0] != filename {
		t.Fatalf("actual gc filenames = %v, want [%s]", gcResult.ContainerFilenames, filename)
	}
	if gcResult.BytesReclaimed != int64(len(payload)) {
		t.Fatalf("actual gc bytes = %d, want independently observed physical size %d", gcResult.BytesReclaimed, len(payload))
	}
	if _, err := os.Stat(containerPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected container file to be removed, stat err=%v", err)
	}

	var remainingContainers int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM container`).Scan(&remainingContainers); err != nil {
		t.Fatalf("count remaining containers: %v", err)
	}
	if remainingContainers != 0 {
		t.Fatalf("expected container metadata to be deleted, got %d rows", remainingContainers)
	}
}

func TestRunGCDryRunIsNonMutating(t *testing.T) {
	requireParityDB(t)

	dbconn, containersDir := setupParityRunEnv(t)
	payload := []byte("dry-run-non-mutation-payload")
	containerID, _, containerPath := insertDeadContainerFixture(
		t,
		dbconn,
		containersDir,
		"dry-run-non-mutation.bin",
		payload,
		"dry-run-non-mutation-chunk",
	)

	dryResult, err := maintenance.RunGCWithContainersDirResult(true, containersDir)
	if err != nil {
		t.Fatalf("dry-run gc: %v", err)
	}
	if !dryResult.DryRun {
		t.Fatal("expected DryRun=true in result")
	}
	if dryResult.AffectedContainers != 1 {
		t.Fatalf("dry-run AffectedContainers = %d, want 1", dryResult.AffectedContainers)
	}
	if dryResult.BytesReclaimed != int64(len(payload)) {
		t.Fatalf("dry-run bytes = %d, want %d", dryResult.BytesReclaimed, len(payload))
	}

	assertFileExistsState(t, containerPath, true)
	assertContainerRowCountByID(t, dbconn, containerID, 1)
}

func TestPhase8DefectAnchorActiveContainerDryRunIsNonMutating(t *testing.T) {
	requireParityDB(t)

	dbconn, containersDir := setupParityRunEnv(t)
	payload := []byte("phase8-active-dry-run-payload")
	containerID, chunkID, containerPath := insertDeadContainerFixture(
		t,
		dbconn,
		containersDir,
		"phase8-active-dry-run.bin",
		payload,
		"phase8-active-dry-run-chunk",
	)
	if _, err := dbconn.Exec(`UPDATE container SET sealed = FALSE WHERE id = $1`, containerID); err != nil {
		t.Fatalf("make container active: %v", err)
	}

	result, err := maintenance.RunGCWithContainersDirResult(true, containersDir)
	if err != nil {
		t.Fatalf("dry-run gc: %v", err)
	}
	if result.AffectedContainers != 1 || result.BytesReclaimed != int64(len(payload)) {
		t.Fatalf("DEFECT_ANCHOR active dry-run result=%+v, want one candidate and %d inspected bytes", result, len(payload))
	}
	assertFileExistsState(t, containerPath, true)
	assertContainerRowCountByID(t, dbconn, containerID, 1)
	var chunkCount int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE id = $1`, chunkID).Scan(&chunkCount); err != nil {
		t.Fatalf("count active dry-run chunk: %v", err)
	}
	if chunkCount != 1 {
		t.Fatalf("active dry-run chunk count=%d, want 1", chunkCount)
	}
}

func TestPhase8DefectAnchorPostgreSQLPhysicalFileCount(t *testing.T) {
	requireParityDB(t)

	dbconn, _ := setupParityRunEnv(t)
	var logicalFileID int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		 VALUES ($1, 1, $2, 'COMPLETED', 2, 'v2-fastcdc') RETURNING id`,
		"phase8-postgres-physical.txt", "phase8-postgres-physical-hash",
	).Scan(&logicalFileID); err != nil {
		t.Fatalf("insert logical file: %v", err)
	}
	if _, err := dbconn.Exec(
		`INSERT INTO physical_file (path, logical_file_id) VALUES ($1, $3), ($2, $3)`,
		"/phase8/postgres-a.txt", "/phase8/postgres-b.txt", logicalFileID,
	); err != nil {
		t.Fatalf("insert physical mappings: %v", err)
	}

	svc, err := observability.NewService(dbconn)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	stats, err := svc.Stats(context.Background(), observability.StatsOptions{})
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.Physical.TotalPhysicalFiles != 2 {
		t.Fatalf("DEFECT_ANCHOR PostgreSQL physical.total_physical_files=%d, want 2", stats.Physical.TotalPhysicalFiles)
	}
}

func TestRunGCDryRunCandidateCountMatchesDestructiveGC(t *testing.T) {
	requireParityDB(t)

	dbconn, containersDir := setupParityRunEnv(t)
	fixtures := []struct {
		filename string
		payload  []byte
		chunk    string
	}{
		{filename: "parity-dead-a.bin", payload: []byte("parity candidate a"), chunk: "parity-dead-a-chunk"},
		{filename: "parity-dead-b.bin", payload: []byte("parity candidate b"), chunk: "parity-dead-b-chunk"},
	}
	for _, fixture := range fixtures {
		insertDeadContainerFixture(t, dbconn, containersDir, fixture.filename, fixture.payload, fixture.chunk)
	}

	dryResult, err := maintenance.RunGCWithContainersDirResult(true, containersDir)
	if err != nil {
		t.Fatalf("dry-run gc: %v", err)
	}
	if dryResult.AffectedContainers != 2 {
		t.Fatalf("dry-run AffectedContainers = %d, want 2", dryResult.AffectedContainers)
	}
	wantBytes := int64(len(fixtures[0].payload) + len(fixtures[1].payload))
	if dryResult.BytesReclaimed != wantBytes {
		t.Fatalf("dry-run bytes = %d, want %d", dryResult.BytesReclaimed, wantBytes)
	}
	assertFilenameListEqual(t, dryResult.ContainerFilenames, []string{"parity-dead-a.bin", "parity-dead-b.bin"})

	gcResult, err := maintenance.RunGCWithContainersDirResult(false, containersDir)
	if err != nil {
		t.Fatalf("destructive gc: %v", err)
	}
	if gcResult.AffectedContainers != dryResult.AffectedContainers {
		t.Fatalf("candidate count mismatch: dry-run=%d destructive=%d",
			dryResult.AffectedContainers, gcResult.AffectedContainers)
	}
	assertFilenameListEqual(t, gcResult.ContainerFilenames, dryResult.ContainerFilenames)
	if gcResult.BytesReclaimed != dryResult.BytesReclaimed {
		t.Fatalf("byte mismatch: dry-run=%d destructive=%d", dryResult.BytesReclaimed, gcResult.BytesReclaimed)
	}
	for _, fn := range gcResult.ContainerFilenames {
		assertFileExistsState(t, filepath.Join(containersDir, fn), false)
	}
}

func TestRunGCDryRunDoesNotAuthorizeLaterDeletionAfterMutation(t *testing.T) {
	requireParityDB(t)

	dbconn, containersDir := setupParityRunEnv(t)
	containerID, chunkID, containerPath := insertDeadContainerFixture(
		t,
		dbconn,
		containersDir,
		"dry-run-not-authorization.bin",
		[]byte("dry-run-authorization-boundary"),
		"dry-run-not-authorization-chunk",
	)

	dryResult, err := maintenance.RunGCWithContainersDirResult(true, containersDir)
	if err != nil {
		t.Fatalf("dry-run gc: %v", err)
	}
	if dryResult.AffectedContainers != 1 {
		t.Fatalf("dry-run AffectedContainers = %d, want 1", dryResult.AffectedContainers)
	}

	if _, err := dbconn.Exec(`UPDATE chunk SET live_ref_count = 1 WHERE id = $1`, chunkID); err != nil {
		t.Fatalf("mutate chunk liveness between runs: %v", err)
	}

	destructiveResult, err := maintenance.RunGCWithContainersDirResult(false, containersDir)
	if err != nil {
		t.Fatalf("destructive gc after mutation: %v", err)
	}
	if destructiveResult.AffectedContainers != 0 {
		t.Fatalf("destructive AffectedContainers = %d, want 0 after liveness mutation", destructiveResult.AffectedContainers)
	}

	assertFileExistsState(t, containerPath, true)
	assertContainerRowCountByID(t, dbconn, containerID, 1)
}

func TestRunGCMissingContainerFileFailsBeforeMetadataCommit(t *testing.T) {
	requireParityDB(t)

	dbconn, containersDir := setupParityRunEnv(t)
	containerID, _, containerPath := insertDeadContainerFixture(
		t,
		dbconn,
		containersDir,
		"missing-file.bin",
		[]byte("missing-file-payload"),
		"missing-file-chunk",
	)

	if err := os.Remove(containerPath); err != nil {
		t.Fatalf("remove container file before gc: %v", err)
	}

	result, err := maintenance.RunGCWithContainersDirResult(false, containersDir)
	if err == nil {
		t.Fatal("destructive GC with missing file succeeded")
	}
	if result.DryRun {
		t.Fatal("expected DryRun=false")
	}
	if result.AffectedContainers != 0 || result.BytesReclaimed != 0 || len(result.ContainerFilenames) != 0 {
		t.Fatalf("missing-file result = %+v, want zero unit credit", result)
	}
	assertFileExistsState(t, containerPath, false)
	assertContainerRowCountByID(t, dbconn, containerID, 1)
}
