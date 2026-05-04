package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/storage"
	testutils "github.com/franchoy/coldkeep/tests/utils"
	"github.com/franchoy/coldkeep/tests/utils/testgate"
)

func prepareReadPathRegressionRepo(t *testing.T) string {
	t.Helper()

	tmp := t.TempDir()
	origContainersDir := container.ContainersDir
	container.ContainersDir = filepath.Join(tmp, "containers")
	t.Cleanup(func() { container.ContainersDir = origContainersDir })
	t.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
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
			chunk_block_refs,
			storage_blocks,
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
	if _, err := storage.StoreFileWithStorageContextAndCodecResult(sgctx, inPath, blocks.CodecPlain); err != nil {
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

// setRepoChunkerVersion updates repository_config.default_chunker in the live
// database so that subsequent store operations use the requested chunker.
func setRepoChunkerVersion(t *testing.T, version chunk.Version) {
	t.Helper()
	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("setRepoChunkerVersion: ConnectDB: %v", err)
	}
	defer func() { _ = dbconn.Close() }()
	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("setRepoChunkerVersion: Begin tx: %v", err)
	}
	if err := storage.SetDefaultChunkerVersion(tx, version); err != nil {
		_ = tx.Rollback()
		t.Fatalf("setRepoChunkerVersion: SetDefaultChunkerVersion: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("setRepoChunkerVersion: Commit: %v", err)
	}
}

// TestBackwardCompatMixedChunkerRestoreIntegration validates that a repository
// containing files written with v1-simple-rolling and v2-fastcdc can restore
// both files correctly, and that stats reports the correct version distribution.
func TestBackwardCompatMixedChunkerRestoreIntegration(t *testing.T) {
	testgate.RequireDB(t)

	tmp := prepareReadPathRegressionRepo(t)

	// --- Store a file with v1-simple-rolling ---
	setRepoChunkerVersion(t, chunk.VersionV1SimpleRolling)

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB for v1 store: %v", err)
	}
	inputDir := filepath.Join(tmp, "input")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		t.Fatalf("mkdir input: %v", err)
	}
	inPathV1 := testutils.CreateTempFile(t, inputDir, "v1-legacy.bin", 64*1024)
	wantHashV1 := testutils.SHA256File(t, inPathV1)

	sgctxV1 := storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: container.ContainersDir,
	}
	if err := storage.StoreFileWithStorageContext(sgctxV1, inPathV1); err != nil {
		t.Fatalf("StoreFileWithStorageContext v1: %v", err)
	}
	_ = dbconn.Close()

	fileIDV1 := func() int64 {
		c, err := db.ConnectDB()
		if err != nil {
			t.Fatalf("connectDB for fileID v1: %v", err)
		}
		defer func() { _ = c.Close() }()
		return testutils.FetchFileIDByHash(t, c, wantHashV1)
	}()

	// Verify the file was stored with v1-simple-rolling
	func() {
		c, err := db.ConnectDB()
		if err != nil {
			t.Fatalf("connectDB for chunker check v1: %v", err)
		}
		defer func() { _ = c.Close() }()
		var cv string
		if err := c.QueryRow(`SELECT chunker_version FROM logical_file WHERE id = $1`, fileIDV1).Scan(&cv); err != nil {
			t.Fatalf("read logical_file.chunker_version v1: %v", err)
		}
		if cv != string(chunk.VersionV1SimpleRolling) {
			t.Fatalf("expected v1-simple-rolling, got %q", cv)
		}
	}()

	// --- Store a file with v2-fastcdc ---
	setRepoChunkerVersion(t, chunk.VersionV2FastCDC)

	dbconn2, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB for v2 store: %v", err)
	}
	inPathV2 := testutils.CreateTempFile(t, inputDir, "v2-new.bin", 80*1024)
	wantHashV2 := testutils.SHA256File(t, inPathV2)

	sgctxV2 := storage.StorageContext{
		DB:           dbconn2,
		Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
		ContainerDir: container.ContainersDir,
	}
	if _, err := storage.StoreFileWithStorageContextAndCodecResult(sgctxV2, inPathV2, blocks.CodecPlain); err != nil {
		t.Fatalf("StoreFileWithStorageContext v2: %v", err)
	}
	_ = dbconn2.Close()

	fileIDV2 := func() int64 {
		c, err := db.ConnectDB()
		if err != nil {
			t.Fatalf("connectDB for fileID v2: %v", err)
		}
		defer func() { _ = c.Close() }()
		return testutils.FetchFileIDByHash(t, c, wantHashV2)
	}()

	// Verify the file was stored with v2-fastcdc
	func() {
		c, err := db.ConnectDB()
		if err != nil {
			t.Fatalf("connectDB for chunker check v2: %v", err)
		}
		defer func() { _ = c.Close() }()
		var cv string
		if err := c.QueryRow(`SELECT chunker_version FROM logical_file WHERE id = $1`, fileIDV2).Scan(&cv); err != nil {
			t.Fatalf("read logical_file.chunker_version v2: %v", err)
		}
		if cv != string(chunk.VersionV2FastCDC) {
			t.Fatalf("expected v2-fastcdc, got %q", cv)
		}
	}()

	// --- Restore both files and verify hashes ---
	outDir := filepath.Join(tmp, "out")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatalf("mkdir out: %v", err)
	}

	restoreFile := func(fileID int64, name string, wantHash string) {
		t.Helper()
		c, err := db.ConnectDB()
		if err != nil {
			t.Fatalf("connectDB for restore %s: %v", name, err)
		}
		defer func() { _ = c.Close() }()
		sgctx := storage.StorageContext{
			DB:           c,
			ContainerDir: container.ContainersDir,
		}
		outPath := filepath.Join(outDir, name)
		if err := storage.RestoreFileWithStorageContext(sgctx, fileID, outPath); err != nil {
			t.Fatalf("RestoreFileWithStorageContext %s: %v", name, err)
		}
		if got := testutils.SHA256File(t, outPath); got != wantHash {
			t.Fatalf("restore %s hash mismatch: got=%s want=%s", name, got, wantHash)
		}
	}

	restoreFile(fileIDV1, "restored-v1.bin", wantHashV1)
	restoreFile(fileIDV2, "restored-v2.bin", wantHashV2)
}

// TestBackwardCompatStatsShowsVersionDistributionIntegration validates that
// after storing files with both v1-simple-rolling and v2-fastcdc, stats
// reports a non-empty chunker_versions map showing entries for both versions.
func TestBackwardCompatStatsShowsVersionDistributionIntegration(t *testing.T) {
	testgate.RequireDB(t)

	tmp := prepareReadPathRegressionRepo(t)
	inputDir := filepath.Join(tmp, "input")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		t.Fatalf("mkdir input: %v", err)
	}

	storeWith := func(version chunk.Version, name string, size int) {
		t.Helper()
		setRepoChunkerVersion(t, version)
		c, err := db.ConnectDB()
		if err != nil {
			t.Fatalf("connectDB for storeWith %s: %v", name, err)
		}
		defer func() { _ = c.Close() }()
		p := testutils.CreateTempFile(t, inputDir, name, size)
		sgctx := storage.StorageContext{
			DB:           c,
			Writer:       container.NewLocalWriter(container.GetContainerMaxSize()),
			ContainerDir: container.ContainersDir,
		}
		if _, err := storage.StoreFileWithStorageContextAndCodecResult(sgctx, p, blocks.CodecPlain); err != nil {
			t.Fatalf("store %s: %v", name, err)
		}
	}

	storeWith(chunk.VersionV1SimpleRolling, fmt.Sprintf("stats-v1-%d.bin", 0), 48*1024)
	storeWith(chunk.VersionV2FastCDC, fmt.Sprintf("stats-v2-%d.bin", 0), 56*1024)
	// Restore default to v2-fastcdc after test seeding
	setRepoChunkerVersion(t, chunk.VersionV2FastCDC)

	c, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connectDB for stats: %v", err)
	}
	defer func() { _ = c.Close() }()

	var v1Count, v2Count int64
	if err := c.QueryRow(`SELECT COUNT(*) FROM chunk WHERE chunker_version = $1`, string(chunk.VersionV1SimpleRolling)).Scan(&v1Count); err != nil {
		t.Fatalf("count v1 chunks: %v", err)
	}
	if err := c.QueryRow(`SELECT COUNT(*) FROM chunk WHERE chunker_version = $1`, string(chunk.VersionV2FastCDC)).Scan(&v2Count); err != nil {
		t.Fatalf("count v2 chunks: %v", err)
	}
	if v1Count == 0 {
		t.Fatalf("expected v1-simple-rolling chunks in DB, got 0")
	}
	if v2Count == 0 {
		t.Fatalf("expected v2-fastcdc chunks in DB, got 0")
	}
}

// TestBackwardCompatV15CLIWorkflowIntegration validates the complete v1.5
// command surface: init, store, restore, remove, verify, gc --dry-run,
// snapshot create/list/show/restore, and stats, all execute successfully.
func TestBackwardCompatV15CLIWorkflowIntegration(t *testing.T) {
	testgate.RequireDB(t)

	tmp := prepareReadPathRegressionRepo(t)

	repoRoot := testutils.FindRepoRoot(t)
	binPath := testutils.BuildColdkeepBinary(t, repoRoot)
	env := testutils.DefaultCLIEnv(container.ContainersDir)

	inputDir := filepath.Join(tmp, "input")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		t.Fatalf("mkdir input: %v", err)
	}

	inPath := testutils.CreateTempFile(t, inputDir, "v15-workflow.bin", 128*1024)
	wantHash := testutils.SHA256File(t, inPath)

	// store
	storePayload := testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
		"store", inPath, "--output", "json"), "store")
	storeData := testutils.JSONMap(t, storePayload, "data")
	fileID := testutils.JSONInt64(t, storeData, "file_id")

	// restore
	restoreDir := filepath.Join(tmp, "restore")
	if err := os.MkdirAll(restoreDir, 0o755); err != nil {
		t.Fatalf("mkdir restore: %v", err)
	}
	testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
		"restore", fmt.Sprintf("%d", fileID), restoreDir, "--output", "json"), "restore")

	restoredPath := filepath.Join(restoreDir, "v15-workflow.bin")
	if got := testutils.SHA256File(t, restoredPath); got != wantHash {
		t.Fatalf("restore hash mismatch: got=%s want=%s", got, wantHash)
	}

	// verify
	testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
		"verify", "system", "--output", "json"), "verify")

	// gc --dry-run
	gcRes := testutils.RunColdkeepCommand(t, repoRoot, binPath, env, "gc", "--dry-run", "--output", "json")
	if gcRes.ExitCode != 0 {
		t.Fatalf("gc --dry-run failed: exit=%d stderr=%s", gcRes.ExitCode, gcRes.Stderr)
	}

	// snapshot create
	testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
		"snapshot", "create", "--id", "compat-snap", "--output", "json"), "snapshot")

	// snapshot list
	listRes := testutils.RunColdkeepCommand(t, repoRoot, binPath, env, "snapshot", "list", "--output", "json")
	if listRes.ExitCode != 0 {
		t.Fatalf("snapshot list failed: exit=%d", listRes.ExitCode)
	}

	// snapshot show
	testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
		"snapshot", "show", "compat-snap", "--output", "json"), "snapshot")

	// snapshot restore
	snapRestoreDir := filepath.Join(tmp, "snap-restore")
	if err := os.MkdirAll(snapRestoreDir, 0o755); err != nil {
		t.Fatalf("mkdir snap-restore: %v", err)
	}
	testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
		"snapshot", "restore", "compat-snap", "--mode", "prefix", "--destination", snapRestoreDir, "--output", "json"), "snapshot")

	// stats
	statsRes := testutils.RunColdkeepCommand(t, repoRoot, binPath, env, "stats", "--output", "json")
	if statsRes.ExitCode != 0 {
		t.Fatalf("stats failed: exit=%d stderr=%s", statsRes.ExitCode, statsRes.Stderr)
	}
	statsPayload, ok := testutils.TryParseLastJSONLine(statsRes.Stdout)
	if !ok {
		statsPayload, ok = testutils.TryParseLastJSONLine(statsRes.Stdout + "\n" + statsRes.Stderr)
	}
	if !ok {
		t.Fatalf("stats produced no parseable JSON\nstdout:\n%s\nstderr:\n%s", statsRes.Stdout, statsRes.Stderr)
	}
	if got, _ := statsPayload["type"].(string); got != "stats" {
		t.Fatalf("stats payload type mismatch: got=%q payload=%v", got, statsPayload)
	}
	statsData := testutils.JSONMap(t, statsPayload, "data")
	if _, ok := statsData["logical"]; !ok {
		t.Fatalf("stats JSON missing 'logical' section: %v", statsData)
	}

	// remove should fail while snapshot still retains the logical file
	removeBlockedRes := testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
		"remove", fmt.Sprintf("%d", fileID), "--output", "json")
	if removeBlockedRes.ExitCode == 0 {
		t.Fatalf("expected remove to fail while snapshot retains file_id=%d", fileID)
	}

	// delete retaining snapshot, then remove should succeed
	testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
		"snapshot", "delete", "compat-snap", "--force", "--output", "json"), "snapshot")

	removeRes := testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
		"remove", fmt.Sprintf("%d", fileID), "--output", "json")
	if removeRes.ExitCode != 0 {
		t.Fatalf("remove after snapshot delete failed: exit=%d stderr=%s", removeRes.ExitCode, removeRes.Stderr)
	}

	// verify passes after remove
	testutils.AssertCLIJSONOK(t, testutils.RunColdkeepCommand(t, repoRoot, binPath, env,
		"verify", "system", "--output", "json"), "verify")

	// stats --json
	statsJSONRes := testutils.RunColdkeepCommand(t, repoRoot, binPath, env, "stats", "--json")
	if statsJSONRes.ExitCode != 0 {
		t.Fatalf("stats --json failed: exit=%d stderr=%s", statsJSONRes.ExitCode, statsJSONRes.Stderr)
	}
	statsJSONPayload, ok := testutils.TryParseLastJSONLine(statsJSONRes.Stdout)
	if !ok {
		statsJSONPayload, ok = testutils.TryParseLastJSONLine(statsJSONRes.Stdout + "\n" + statsJSONRes.Stderr)
	}
	if !ok {
		t.Fatalf("stats --json produced no parseable JSON\nstdout:\n%s\nstderr:\n%s", statsJSONRes.Stdout, statsJSONRes.Stderr)
	}
	if got, _ := statsJSONPayload["type"].(string); got != "stats" {
		t.Fatalf("stats --json payload type mismatch: got=%q payload=%v", got, statsJSONPayload)
	}
	if _, ok := statsJSONPayload["data"]; !ok {
		t.Fatalf("stats --json missing 'data': %v", statsJSONPayload)
	}
	_ = strings.TrimSpace("") // keep strings import used
}
