package main

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/container"
	dbpkg "github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/storage"
	_ "github.com/mattn/go-sqlite3"
)

func openPortabilitySQLiteDB(t *testing.T, catalogPath string) *sql.DB {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(catalogPath), 0o700); err != nil {
		t.Fatalf("mkdir catalog dir: %v", err)
	}

	dbconn, err := sql.Open("sqlite3", catalogPath)
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	dbconn.SetMaxOpenConns(1)
	dbconn.SetMaxIdleConns(1)

	pingCtx, cancel := dbpkg.NewOperationContext(context.Background())
	defer cancel()
	if err := dbconn.PingContext(pingCtx); err != nil {
		_ = dbconn.Close()
		t.Fatalf("ping sqlite db: %v", err)
	}

	if err := dbpkg.ApplySQLiteSessionPragmas(dbconn); err != nil {
		_ = dbconn.Close()
		t.Fatalf("ApplySQLiteSessionPragmas: %v", err)
	}
	if err := dbpkg.RunMigrations(dbconn); err != nil {
		_ = dbconn.Close()
		t.Fatalf("RunMigrations: %v", err)
	}

	return dbconn
}

func newPortabilityStoreContext(t *testing.T, dbconn *sql.DB, containersDir string) storage.StorageContext {
	t.Helper()

	if err := os.MkdirAll(containersDir, 0o700); err != nil {
		t.Fatalf("mkdir containers dir: %v", err)
	}
	writer := container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn)
	if writer == nil {
		t.Fatal("NewLocalWriterWithDirAndDB returned nil")
	}

	return storage.StorageContext{
		DB:           dbconn,
		Writer:       writer,
		ContainerDir: containersDir,
	}
}

func newPortabilityEngine(t *testing.T, dbconn *sql.DB, containersDir string, storeContext *storage.StorageContext) *engine.DefaultEngine {
	t.Helper()

	eng, err := engine.New(engine.Config{
		DB:           dbconn,
		ContainerDir: containersDir,
		StoreContext: storeContext,
	})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	return eng
}

func requirePortabilityPathExists(t *testing.T, path string) {
	t.Helper()

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected path %q to exist: %v", path, err)
	}
}

func requirePortabilityPathAbsent(t *testing.T, path string) {
	t.Helper()

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected path %q to be absent, stat err=%v", path, err)
	}
}

func requireRelocatedPhysicalMappingInvariant(t *testing.T, dbconn *sql.DB, fileID int64) {
	t.Helper()

	var refCount int64
	var mappingCount int64
	if err := dbconn.QueryRow(`
		SELECT
			lf.ref_count,
			(
				SELECT COUNT(*)
				FROM physical_file pf
				WHERE pf.logical_file_id = lf.id
			)
		FROM logical_file lf
		WHERE lf.id = ?
	`, fileID).Scan(&refCount, &mappingCount); err != nil {
		t.Fatalf("query relocated physical mapping invariant: %v", err)
	}
	if refCount != mappingCount {
		t.Fatalf("expected ref_count and physical mapping count to match for file %d, got ref_count=%d mappings=%d", fileID, refCount, mappingCount)
	}
	if mappingCount != 1 {
		t.Fatalf("expected exactly 1 physical mapping for file %d after reopen, got %d", fileID, mappingCount)
	}

	var migratedCount int64
	if err := dbconn.QueryRow(`
		SELECT COUNT(*)
		FROM physical_file
		WHERE logical_file_id = ?
		  AND path LIKE '/migrated/%'
	`, fileID).Scan(&migratedCount); err != nil {
		t.Fatalf("count synthetic migrated physical mappings: %v", err)
	}
	if migratedCount != 0 {
		t.Fatalf("expected no synthetic /migrated physical mappings for file %d, got %d", fileID, migratedCount)
	}
}

func requireSuccessfulPortabilityRestore(t *testing.T, result engine.RestoreResult) string {
	t.Helper()

	if got := len(result.Items); got != 1 {
		t.Fatalf("expected 1 restore item, got %d", got)
	}
	if got := result.Items[0].Status; got != engine.BatchItemOK {
		t.Fatalf("expected restore item status %q, got %q", engine.BatchItemOK, got)
	}
	if got := result.Summary.OK; got != 1 {
		t.Fatalf("expected restore summary OK=1, got %d", got)
	}
	if got := result.Summary.Failed; got != 0 {
		t.Fatalf("expected restore summary Failed=0, got %d", got)
	}
	if result.Items[0].OutputPath == "" {
		t.Fatal("expected non-empty restore output path")
	}

	return result.Items[0].OutputPath
}

func TestSQLiteRepositoryRelocationReopenVerifyRestoreIntegration(t *testing.T) {
	parentDir := t.TempDir()
	sourceRoot := filepath.Join(parentDir, "source-repo")
	destinationRoot := filepath.Join(parentDir, "moved-repo")
	sourceCatalogPath := filepath.Join(sourceRoot, ".coldkeep", "catalog.sqlite")
	sourceContainersDir := filepath.Join(sourceRoot, "containers")
	inputDir := filepath.Join(parentDir, "input")
	restoreDir := filepath.Join(parentDir, "restore")

	if err := os.MkdirAll(inputDir, 0o700); err != nil {
		t.Fatalf("mkdir input dir: %v", err)
	}
	if err := os.MkdirAll(restoreDir, 0o700); err != nil {
		t.Fatalf("mkdir restore dir: %v", err)
	}

	inputBytes := []byte("coldkeep-v1.13.7-portability-smoke")
	inputPath := filepath.Join(inputDir, "portable.txt")
	if err := os.WriteFile(inputPath, inputBytes, 0o600); err != nil {
		t.Fatalf("write input file: %v", err)
	}

	sourceDB := openPortabilitySQLiteDB(t, sourceCatalogPath)
	sourceStoreContext := newPortabilityStoreContext(t, sourceDB, sourceContainersDir)
	sourceClosed := false
	defer func() {
		if !sourceClosed {
			if err := sourceStoreContext.Close(); err != nil {
				t.Fatalf("close source storage context during cleanup: %v", err)
			}
		}
	}()
	sourceEngine := newPortabilityEngine(t, sourceDB, sourceContainersDir, &sourceStoreContext)

	storeResult, err := sourceEngine.Store(context.Background(), engine.StoreRequest{
		SourcePath: inputPath,
		Codec:      "plain",
	})
	if err != nil {
		t.Fatalf("source Store: %v", err)
	}
	if storeResult.LogicalFileID <= 0 {
		t.Fatalf("expected positive LogicalFileID, got %d", storeResult.LogicalFileID)
	}
	if storeResult.SourcePath == "" {
		t.Fatal("expected non-empty source path in store result")
	}

	requirePortabilityPathExists(t, sourceCatalogPath)
	requirePortabilityPathExists(t, sourceContainersDir)

	containerEntries, err := os.ReadDir(sourceContainersDir)
	if err != nil {
		t.Fatalf("read source containers dir: %v", err)
	}
	if len(containerEntries) == 0 {
		t.Fatal("expected payload artifacts under source containers dir")
	}

	if _, err := sourceEngine.Verify(context.Background(), engine.VerifyRequest{
		Target: "system",
		Level:  "standard",
	}); err != nil {
		t.Fatalf("source Verify: %v", err)
	}

	requirePortabilityPathExists(t, sourceRoot)
	requirePortabilityPathExists(t, sourceCatalogPath)
	requirePortabilityPathExists(t, sourceContainersDir)
	requirePortabilityPathAbsent(t, destinationRoot)

	if err := sourceStoreContext.Close(); err != nil {
		t.Fatalf("close source storage context before rename: %v", err)
	}
	sourceClosed = true
	sourceEngine = nil

	if err := os.Rename(sourceRoot, destinationRoot); err != nil {
		t.Fatalf("rename repository root: %v", err)
	}

	requirePortabilityPathAbsent(t, sourceRoot)
	requirePortabilityPathAbsent(t, sourceCatalogPath)
	requirePortabilityPathAbsent(t, sourceContainersDir)
	requirePortabilityPathExists(t, destinationRoot)

	destinationCatalogPath := filepath.Join(destinationRoot, ".coldkeep", "catalog.sqlite")
	destinationContainersDir := filepath.Join(destinationRoot, "containers")
	if destinationCatalogPath == sourceCatalogPath {
		t.Fatal("expected destination catalog path to differ from source catalog path")
	}
	if destinationContainersDir == sourceContainersDir {
		t.Fatal("expected destination containers dir to differ from source containers dir")
	}

	requirePortabilityPathExists(t, destinationCatalogPath)
	requirePortabilityPathExists(t, destinationContainersDir)

	destinationDB := openPortabilitySQLiteDB(t, destinationCatalogPath)
	defer func() {
		if err := destinationDB.Close(); err != nil {
			t.Fatalf("close destination db: %v", err)
		}
	}()
	destinationEngine := newPortabilityEngine(t, destinationDB, destinationContainersDir, nil)

	requireRelocatedPhysicalMappingInvariant(t, destinationDB, storeResult.LogicalFileID)

	if _, err := destinationEngine.Verify(context.Background(), engine.VerifyRequest{
		Target: "system",
		Level:  "standard",
	}); err != nil {
		t.Fatalf("destination Verify: %v", err)
	}

	restoreResult, err := destinationEngine.Restore(context.Background(), engine.RestoreRequest{
		Mode:      engine.RestoreModeFileIDs,
		FileIDs:   []int64{storeResult.LogicalFileID},
		OutputDir: restoreDir,
		Overwrite: true,
	})
	if err != nil {
		t.Fatalf("destination Restore: %v", err)
	}

	restoreOutputPath := requireSuccessfulPortabilityRestore(t, restoreResult)
	restoredBytes, err := os.ReadFile(restoreOutputPath)
	if err != nil {
		t.Fatalf("read restored output: %v", err)
	}
	if !bytes.Equal(restoredBytes, inputBytes) {
		t.Fatal("restored bytes differ from original input bytes")
	}

	requirePortabilityPathAbsent(t, sourceRoot)
	requirePortabilityPathAbsent(t, sourceCatalogPath)
	requirePortabilityPathAbsent(t, sourceContainersDir)
}
