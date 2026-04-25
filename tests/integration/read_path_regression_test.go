package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/storage"
	testutils "github.com/franchoy/coldkeep/tests/utils"
	"github.com/franchoy/coldkeep/tests/utils/testgate"
)

func prepareReadPathRegressionRepo(t *testing.T) string {
	t.Helper()

	tmp := t.TempDir()
	container.ContainersDir = filepath.Join(tmp, "containers")
	_ = os.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	testutils.ResetStorage(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	testutils.ApplySchema(t, dbconn)
	if _, err := dbconn.Exec(`
		TRUNCATE TABLE
			snapshot_file,
			snapshot,
			snapshot_path,
			physical_file,
			file_chunk,
			blocks,
			chunk,
			logical_file,
			container
		RESTART IDENTITY CASCADE
	`); err != nil {
		t.Fatalf("truncate fixtures: %v", err)
	}

	return tmp
}

func mustReadSchemaVersion(t *testing.T) int {
	t.Helper()

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB for schema version: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	var schemaVersion int
	if err := dbconn.QueryRow(`SELECT MAX(version) FROM schema_version`).Scan(&schemaVersion); err != nil {
		t.Fatalf("read schema version: %v", err)
	}
	return schemaVersion
}

func TestReadPathRestoreAfterMigrationIntegration(t *testing.T) {
	testgate.RequireDB(t)

	tmp := prepareReadPathRegressionRepo(t)
	if got := mustReadSchemaVersion(t); got < 10 {
		t.Fatalf("expected migrated schema version >= 10, got %d", got)
	}

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("reconnect DB: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	inputDir := filepath.Join(tmp, "input")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		t.Fatalf("mkdir input: %v", err)
	}
	inPath := testutils.CreateTempFile(t, inputDir, "restore-after-migration.bin", 96*1024)
	wantHash := testutils.SHA256File(t, inPath)

	sgctx := storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: container.ContainersDir,
	}
	if err := storage.StoreFileWithStorageContext(sgctx, inPath); err != nil {
		t.Fatalf("store file for restore-after-migration regression: %v", err)
	}

	fileID := testutils.FetchFileIDByHash(t, dbconn, wantHash)

	var logicalFileChunkerVersion string
	if err := dbconn.QueryRow(`SELECT chunker_version FROM logical_file WHERE id = $1`, fileID).Scan(&logicalFileChunkerVersion); err != nil {
		t.Fatalf("read logical_file.chunker_version: %v", err)
	}
	if strings.TrimSpace(logicalFileChunkerVersion) == "" {
		t.Fatal("expected non-empty logical_file.chunker_version on migrated read path")
	}

	outPath := filepath.Join(tmp, "out", "restore-after-migration.bin")
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		t.Fatalf("mkdir output dir: %v", err)
	}
	if err := storage.RestoreFileWithStorageContext(sgctx, fileID, outPath); err != nil {
		t.Fatalf("restore after migration: %v", err)
	}

	if got := testutils.SHA256File(t, outPath); got != wantHash {
		t.Fatalf("restore-after-migration hash mismatch: got=%s want=%s", got, wantHash)
	}
}

func TestReadPathRestoreNewlyStoredFileAfterPhase3Integration(t *testing.T) {
	testgate.RequireDB(t)

	tmp := prepareReadPathRegressionRepo(t)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("reconnect DB: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	inputDir := filepath.Join(tmp, "input")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		t.Fatalf("mkdir input: %v", err)
	}
	inPath := testutils.CreateTempFile(t, inputDir, "phase3-store-restore.bin", 128*1024)
	wantHash := testutils.SHA256File(t, inPath)

	sgctx := storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: container.ContainersDir,
	}
	if _, err := storage.StoreFileWithStorageContextAndCodecResult(sgctx, inPath, blocks.CodecPlain); err != nil {
		t.Fatalf("store file for phase3 restore regression: %v", err)
	}

	fileID := testutils.FetchFileIDByHash(t, dbconn, wantHash)

	var configuredDefaultChunker string
	if err := dbconn.QueryRow(`SELECT value FROM repository_config WHERE key = 'default_chunker'`).Scan(&configuredDefaultChunker); err != nil {
		t.Fatalf("read repository default_chunker: %v", err)
	}

	var logicalFileChunkerVersion string
	if err := dbconn.QueryRow(`SELECT chunker_version FROM logical_file WHERE id = $1`, fileID).Scan(&logicalFileChunkerVersion); err != nil {
		t.Fatalf("read logical_file.chunker_version: %v", err)
	}
	if logicalFileChunkerVersion != configuredDefaultChunker {
		t.Fatalf("logical_file.chunker_version mismatch: got=%q want=%q", logicalFileChunkerVersion, configuredDefaultChunker)
	}

	var mismatchedChunkVersions int
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM chunk c
		JOIN file_chunk fc ON fc.chunk_id = c.id
		WHERE fc.logical_file_id = $1 AND c.chunker_version <> $2
	`, fileID, configuredDefaultChunker).Scan(&mismatchedChunkVersions); err != nil {
		t.Fatalf("count mismatched chunk versions: %v", err)
	}
	if mismatchedChunkVersions != 0 {
		t.Fatalf("expected all chunk rows to persist chunker_version=%q, mismatches=%d", configuredDefaultChunker, mismatchedChunkVersions)
	}

	outPath := filepath.Join(tmp, "out", "phase3-store-restore.bin")
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		t.Fatalf("mkdir output dir: %v", err)
	}
	if err := storage.RestoreFileWithStorageContext(sgctx, fileID, outPath); err != nil {
		t.Fatalf("restore newly stored file after phase3: %v", err)
	}

	if got := testutils.SHA256File(t, outPath); got != wantHash {
		t.Fatalf("phase3 store+restore hash mismatch: got=%s want=%s", got, wantHash)
	}
}

func TestReadPathSnapshotRestoreAfterMigrationIntegration(t *testing.T) {
	testgate.RequireDB(t)

	tmp := prepareReadPathRegressionRepo(t)
	if got := mustReadSchemaVersion(t); got < 10 {
		t.Fatalf("expected migrated schema version >= 10, got %d", got)
	}

	repoRoot := testutils.FindRepoRoot(t)
	binPath := testutils.BuildColdkeepBinary(t, repoRoot)
	env := testutils.DefaultCLIEnv(container.ContainersDir)

	inputDir := filepath.Join(tmp, "input")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		t.Fatalf("mkdir input: %v", err)
	}
	inPath := testutils.CreateTempFile(t, inputDir, "snapshot-restore-after-migration.bin", 72*1024)
	wantHash := testutils.SHA256File(t, inPath)

	storePayload := testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
		"store", inPath, "--output", "json"), "store")
	storeData := testutils.JSONMap(t, storePayload, "data")
	storedPath, ok := storeData["stored_path"].(string)
	if !ok || strings.TrimSpace(storedPath) == "" {
		t.Fatalf("store JSON missing stored_path: payload=%v", storePayload)
	}
	trimmedStoredPath := strings.TrimLeft(filepath.ToSlash(storedPath), "/")

	testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
		"snapshot", "create", "--id", "snap-step11-read-path", "--output", "json"), "snapshot")

	restoreRoot := filepath.Join(tmp, "snapshot-restore")
	if err := os.MkdirAll(restoreRoot, 0o755); err != nil {
		t.Fatalf("mkdir snapshot restore root: %v", err)
	}
	testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
		"snapshot", "restore", "snap-step11-read-path", "--mode", "prefix", "--destination", restoreRoot, "--output", "json"), "snapshot")

	restoredPath := filepath.Join(restoreRoot, filepath.FromSlash(trimmedStoredPath))
	if got := testutils.SHA256File(t, restoredPath); got != wantHash {
		t.Fatalf("snapshot restore hash mismatch after migration: got=%s want=%s", got, wantHash)
	}
}
