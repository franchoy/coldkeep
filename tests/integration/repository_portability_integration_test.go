package main

import (
	"bytes"
	"context"
	"database/sql"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/container"
	dbpkg "github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/pathsafe"
	"github.com/franchoy/coldkeep/internal/storage"
	_ "github.com/mattn/go-sqlite3"
)

type portabilityFixture struct {
	sourceRoot               string
	destinationRoot          string
	sourceCatalogPath        string
	sourceContainersDir      string
	destinationCatalogPath   string
	destinationContainersDir string
	inputPath                string
	restoreDir               string
	inputBytes               []byte
}

type portabilitySourceState struct {
	dbconn       *sql.DB
	storeContext storage.StorageContext
	engine       *engine.DefaultEngine
	storeResult  engine.StoreResult
	closed       bool
}

func createPortabilityDir(t *testing.T, path string) {
	t.Helper()

	if _, err := os.Stat(path); err == nil {
		return
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat dir %q: %v", path, err)
	}

	parent := filepath.Dir(path)
	tempDir, err := os.MkdirTemp(parent, filepath.Base(path)+"-")
	if err != nil {
		t.Fatalf("mkdtemp %q: %v", path, err)
	}
	if err := os.Rename(tempDir, path); err != nil {
		_ = os.RemoveAll(tempDir)
		t.Fatalf("rename temp dir to %q: %v", path, err)
	}
}

func newPortabilityFixture(t *testing.T) portabilityFixture {
	t.Helper()

	parentDir := t.TempDir()
	sourceRoot := filepath.Join(parentDir, "source-repo")
	inputDir := filepath.Join(parentDir, "input")
	restoreDir := filepath.Join(parentDir, "restore")
	createPortabilityDir(t, sourceRoot)
	createPortabilityDir(t, inputDir)
	createPortabilityDir(t, restoreDir)

	inputBytes := []byte("coldkeep-v1.13.7-portability-smoke")
	inputPath := filepath.Join(inputDir, "portable.txt")
	if err := os.WriteFile(inputPath, inputBytes, 0o600); err != nil {
		t.Fatalf("write input file: %v", err)
	}

	destinationRoot := filepath.Join(parentDir, "moved-repo")
	return portabilityFixture{
		sourceRoot:               sourceRoot,
		destinationRoot:          destinationRoot,
		sourceCatalogPath:        filepath.Join(sourceRoot, ".coldkeep", "catalog.sqlite"),
		sourceContainersDir:      filepath.Join(sourceRoot, "containers"),
		destinationCatalogPath:   filepath.Join(destinationRoot, ".coldkeep", "catalog.sqlite"),
		destinationContainersDir: filepath.Join(destinationRoot, "containers"),
		inputPath:                inputPath,
		restoreDir:               restoreDir,
		inputBytes:               inputBytes,
	}
}

func openPortabilitySQLiteDB(t *testing.T, catalogPath string) *sql.DB {
	t.Helper()

	createPortabilityDir(t, filepath.Dir(catalogPath))

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

	createPortabilityDir(t, containersDir)
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
	if result.Items[0].DestinationPath == "" {
		t.Fatal("expected non-empty restore output path")
	}

	return result.Items[0].DestinationPath
}

func newPortabilitySourceState(t *testing.T, fixture portabilityFixture) *portabilitySourceState {
	t.Helper()

	dbconn := openPortabilitySQLiteDB(t, fixture.sourceCatalogPath)
	storeContext := newPortabilityStoreContext(t, dbconn, fixture.sourceContainersDir)
	state := &portabilitySourceState{
		dbconn:       dbconn,
		storeContext: storeContext,
	}
	state.engine = newPortabilityEngine(t, dbconn, fixture.sourceContainersDir, &state.storeContext)
	t.Cleanup(func() {
		if !state.closed {
			if err := state.storeContext.Close(); err != nil {
				t.Fatalf("close source storage context during cleanup: %v", err)
			}
		}
	})
	return state
}

func runPortabilitySourceFlow(t *testing.T, fixture portabilityFixture, state *portabilitySourceState) {
	t.Helper()

	storeResult, err := state.engine.Store(context.Background(), engine.StoreRequest{
		SourcePath: fixture.inputPath,
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
	state.storeResult = storeResult

	requirePortabilityPathExists(t, fixture.sourceCatalogPath)
	requirePortabilityPathExists(t, fixture.sourceContainersDir)
	entries, err := os.ReadDir(fixture.sourceContainersDir)
	if err != nil {
		t.Fatalf("read source containers dir: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("expected payload artifacts under source containers dir")
	}
	if _, err := state.engine.Verify(context.Background(), engine.VerifyRequest{
		Target: "system",
		Level:  "standard",
	}); err != nil {
		t.Fatalf("source Verify: %v", err)
	}
}

func relocatePortabilityRepository(t *testing.T, fixture portabilityFixture, state *portabilitySourceState) {
	t.Helper()

	requirePortabilityPathExists(t, fixture.sourceRoot)
	requirePortabilityPathExists(t, fixture.sourceCatalogPath)
	requirePortabilityPathExists(t, fixture.sourceContainersDir)
	requirePortabilityPathAbsent(t, fixture.destinationRoot)

	if err := state.storeContext.Close(); err != nil {
		t.Fatalf("close source storage context before rename: %v", err)
	}
	state.closed = true
	state.dbconn = nil
	state.engine = nil

	if err := os.Rename(fixture.sourceRoot, fixture.destinationRoot); err != nil {
		t.Fatalf("rename repository root: %v", err)
	}

	requirePortabilityPathAbsent(t, fixture.sourceRoot)
	requirePortabilityPathAbsent(t, fixture.sourceCatalogPath)
	requirePortabilityPathAbsent(t, fixture.sourceContainersDir)
	requirePortabilityPathExists(t, fixture.destinationRoot)
	requirePortabilityPathExists(t, fixture.destinationCatalogPath)
	requirePortabilityPathExists(t, fixture.destinationContainersDir)
	if fixture.destinationCatalogPath == fixture.sourceCatalogPath {
		t.Fatal("expected destination catalog path to differ from source catalog path")
	}
	if fixture.destinationContainersDir == fixture.sourceContainersDir {
		t.Fatal("expected destination containers dir to differ from source containers dir")
	}
}

func readSingleRestoredPortabilityFile(t *testing.T, restoreDir string) []byte {
	t.Helper()

	entries, err := os.ReadDir(restoreDir)
	if err != nil {
		t.Fatalf("read restore dir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 restored filesystem entry, got %d", len(entries))
	}
	if entries[0].IsDir() {
		t.Fatalf("expected restored entry %q to be a file", entries[0].Name())
	}
	entryName := entries[0].Name()
	if err := pathsafe.ValidateSafeFileName(entryName); err != nil {
		t.Fatalf("expected restored entry name to be safe, got %q: %v", entryName, err)
	}
	data, err := fs.ReadFile(os.DirFS(restoreDir), entryName)
	if err != nil {
		t.Fatalf("read restored output: %v", err)
	}
	return data
}

func runPortabilityDestinationFlow(t *testing.T, fixture portabilityFixture, fileID int64) []byte {
	t.Helper()

	destinationDB := openPortabilitySQLiteDB(t, fixture.destinationCatalogPath)
	defer func() {
		if err := destinationDB.Close(); err != nil {
			t.Fatalf("close destination db: %v", err)
		}
	}()
	destinationEngine := newPortabilityEngine(t, destinationDB, fixture.destinationContainersDir, nil)

	requireRelocatedPhysicalMappingInvariant(t, destinationDB, fileID)
	if _, err := destinationEngine.Verify(context.Background(), engine.VerifyRequest{
		Target: "system",
		Level:  "standard",
	}); err != nil {
		t.Fatalf("destination Verify: %v", err)
	}

	restoreResult, err := destinationEngine.Restore(context.Background(), engine.RestoreRequest{
		FileIDs:         []int64{fileID},
		DestinationRoot: fixture.restoreDir,
		Overwrite:       true,
	})
	if err != nil {
		t.Fatalf("destination Restore: %v", err)
	}

	restoreOutputPath := requireSuccessfulPortabilityRestore(t, restoreResult)
	absRestoreDir, err := filepath.Abs(fixture.restoreDir)
	if err != nil {
		t.Fatalf("abs restore dir: %v", err)
	}
	absRestoreOutputPath, err := filepath.Abs(restoreOutputPath)
	if err != nil {
		t.Fatalf("abs restore output path: %v", err)
	}
	if !strings.HasPrefix(absRestoreOutputPath, absRestoreDir+string(os.PathSeparator)) {
		t.Fatalf("expected restore output path %q to remain under %q", absRestoreOutputPath, absRestoreDir)
	}

	return readSingleRestoredPortabilityFile(t, fixture.restoreDir)
}

func TestSQLiteRepositoryRelocationReopenVerifyRestoreIntegration(t *testing.T) {
	fixture := newPortabilityFixture(t)
	sourceState := newPortabilitySourceState(t, fixture)

	runPortabilitySourceFlow(t, fixture, sourceState)
	relocatePortabilityRepository(t, fixture, sourceState)
	restoredBytes := runPortabilityDestinationFlow(t, fixture, sourceState.storeResult.LogicalFileID)

	if !bytes.Equal(restoredBytes, fixture.inputBytes) {
		t.Fatal("restored bytes differ from original input bytes")
	}

	requirePortabilityPathAbsent(t, fixture.sourceRoot)
	requirePortabilityPathAbsent(t, fixture.sourceCatalogPath)
	requirePortabilityPathAbsent(t, fixture.sourceContainersDir)
}
