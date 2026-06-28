package engine_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	dbpkg "github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/storage"
	"github.com/franchoy/coldkeep/tests/utils/testgate"
)

type storedPathRestoreFixture struct {
	db      *sql.DB
	sgctx   storage.StorageContext
	engine  *engine.DefaultEngine
	stored  storage.StoreFileResult
	payload []byte
}

type restoreCatalogState struct {
	logicalFileCount int
	refCount         int64
	physicalCount    int
	physicalPath     string
	snapshotCount    int
	fileChunkCount   int
	chunkLiveRefs    map[int64]int64
	chunkPinCounts   map[int64]int64
}

func TestRestoreStoredPathRejectsCancelledContext(t *testing.T) {
	fixture := newStoredPathRestoreFixture(t, "restore-stored-path-cancelled")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := fixture.engine.RestoreStoredPath(ctx, engine.RestoreStoredPathRequest{
		StoredPath: fixture.stored.Path,
	})
	if err == nil {
		t.Fatal("expected cancelled context error")
	}
	if engine.IsUnsupported(err) {
		t.Fatalf("cancelled context must not classify as unsupported: %v", err)
	}
	if result != (engine.RestoreStoredPathResult{}) {
		t.Fatalf("expected zero result on cancelled context, got %+v", result)
	}
}

func TestRestoreStoredPathRejectsBlankStoredPath(t *testing.T) {
	fixture := newStoredPathRestoreFixture(t, "restore-stored-path-blank")

	result, err := fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
		StoredPath: "   ",
	})
	assertRestoreStoredPathValidationError(t, result, err, "engine: restore stored path is required")
}

func TestRestoreStoredPathRejectsConflictingMetadataModes(t *testing.T) {
	fixture := newStoredPathRestoreFixture(t, "restore-stored-path-metadata-conflict")

	result, err := fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
		StoredPath:     fixture.stored.Path,
		StrictMetadata: true,
		NoMetadata:     true,
	})
	assertRestoreStoredPathValidationError(t, result, err, "engine: restore stored path strict metadata and no metadata are mutually exclusive")
}

func TestRestoreStoredPathDefaultsEmptyModeToOriginal(t *testing.T) {
	fixture := newStoredPathRestoreFixture(t, "restore-stored-path-default-mode")

	result, err := fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
		StoredPath: fixture.stored.Path,
		Overwrite:  true,
	})
	if err != nil {
		t.Fatalf("RestoreStoredPath default original mode: %v", err)
	}
	if result.DestinationMode != engine.RestoreDestinationOriginal {
		t.Fatalf("expected default mode %q, got %q", engine.RestoreDestinationOriginal, result.DestinationMode)
	}
	if result.DestinationPath != fixture.stored.Path {
		t.Fatalf("expected original destination path %q, got %q", fixture.stored.Path, result.DestinationPath)
	}
	assertRestoredBytes(t, result.DestinationPath, fixture.payload)
}

func TestRestoreStoredPathRejectsUnknownDestinationMode(t *testing.T) {
	fixture := newStoredPathRestoreFixture(t, "restore-stored-path-unknown-mode")

	result, err := fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
		StoredPath:      fixture.stored.Path,
		DestinationMode: engine.RestoreDestinationMode("weird"),
	})
	assertRestoreStoredPathValidationError(t, result, err, `engine: invalid restore stored-path destination mode "weird"`)
}

func TestRestoreStoredPathValidatesOriginalDestinationFields(t *testing.T) {
	fixture := newStoredPathRestoreFixture(t, "restore-stored-path-original-validation")

	cases := []struct {
		name    string
		req     engine.RestoreStoredPathRequest
		wantErr string
	}{
		{
			name: "destination root forbidden",
			req: engine.RestoreStoredPathRequest{
				StoredPath:      fixture.stored.Path,
				DestinationMode: engine.RestoreDestinationOriginal,
				DestinationRoot: "/tmp/out",
			},
			wantErr: "engine: restore stored path original mode does not accept a destination root",
		},
		{
			name: "destination path forbidden",
			req: engine.RestoreStoredPathRequest{
				StoredPath:      fixture.stored.Path,
				DestinationMode: engine.RestoreDestinationOriginal,
				DestinationPath: "/tmp/out.txt",
			},
			wantErr: "engine: restore stored path original mode does not accept a destination path",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := fixture.engine.RestoreStoredPath(context.Background(), tc.req)
			assertRestoreStoredPathValidationError(t, result, err, tc.wantErr)
		})
	}
}

func TestRestoreStoredPathValidatesPrefixDestinationFields(t *testing.T) {
	fixture := newStoredPathRestoreFixture(t, "restore-stored-path-prefix-validation")

	cases := []struct {
		name    string
		req     engine.RestoreStoredPathRequest
		wantErr string
	}{
		{
			name: "destination root required",
			req: engine.RestoreStoredPathRequest{
				StoredPath:      fixture.stored.Path,
				DestinationMode: engine.RestoreDestinationPrefix,
			},
			wantErr: "engine: restore stored path prefix mode requires a destination root",
		},
		{
			name: "destination path forbidden",
			req: engine.RestoreStoredPathRequest{
				StoredPath:      fixture.stored.Path,
				DestinationMode: engine.RestoreDestinationPrefix,
				DestinationRoot: t.TempDir(),
				DestinationPath: filepath.Join(t.TempDir(), "out.txt"),
			},
			wantErr: "engine: restore stored path prefix mode does not accept an exact destination path",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := fixture.engine.RestoreStoredPath(context.Background(), tc.req)
			assertRestoreStoredPathValidationError(t, result, err, tc.wantErr)
		})
	}
}

func TestRestoreStoredPathValidatesOverrideDestinationFields(t *testing.T) {
	fixture := newStoredPathRestoreFixture(t, "restore-stored-path-override-validation")

	cases := []struct {
		name    string
		req     engine.RestoreStoredPathRequest
		wantErr string
	}{
		{
			name: "destination path required",
			req: engine.RestoreStoredPathRequest{
				StoredPath:      fixture.stored.Path,
				DestinationMode: engine.RestoreDestinationOverride,
			},
			wantErr: "engine: restore stored path override mode requires an exact destination path",
		},
		{
			name: "destination root forbidden",
			req: engine.RestoreStoredPathRequest{
				StoredPath:      fixture.stored.Path,
				DestinationMode: engine.RestoreDestinationOverride,
				DestinationRoot: t.TempDir(),
				DestinationPath: filepath.Join(t.TempDir(), "out.txt"),
			},
			wantErr: "engine: restore stored path override mode does not accept a destination root",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := fixture.engine.RestoreStoredPath(context.Background(), tc.req)
			assertRestoreStoredPathValidationError(t, result, err, tc.wantErr)
		})
	}
}

func TestRestoreStoredPathRequiresDatabaseDependency(t *testing.T) {
	result, err := (&engine.DefaultEngine{}).RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
		StoredPath: "/tmp/file.txt",
	})
	assertRestoreStoredPathValidationError(t, result, err, "engine: restore stored path database is required")
}

func TestRestoreStoredPathRequiresContainerDirectoryDependency(t *testing.T) {
	db := openSnapshotTestDB(t)
	eng, err := engine.New(engine.Config{DB: db})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	result, restoreErr := eng.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
		StoredPath: "/tmp/file.txt",
	})
	assertRestoreStoredPathValidationError(t, result, restoreErr, "engine: restore stored path container directory is required")
}

func TestRestoreStoredPathOriginalMode(t *testing.T) {
	fixture := newStoredPathRestoreFixture(t, "restore-stored-path-original")

	result, err := fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
		StoredPath: fixture.stored.Path,
		Overwrite:  true,
	})
	if err != nil {
		t.Fatalf("RestoreStoredPath original mode: %v", err)
	}

	assertRestoreStoredPathResult(t, result, fixture.stored.Path, fixture.stored.FileID, engine.RestoreDestinationOriginal, fixture.stored.Path, fixture.stored.FileHash)
	assertRestoredBytes(t, result.DestinationPath, fixture.payload)
}

func TestRestoreStoredPathPrefixMode(t *testing.T) {
	fixture := newStoredPathRestoreFixture(t, "restore-stored-path-prefix")
	prefixRoot := t.TempDir()
	expectedPath := expectedPrefixModeOutputPath(prefixRoot, fixture.stored.Path)

	result, err := fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
		StoredPath:      fixture.stored.Path,
		DestinationMode: engine.RestoreDestinationPrefix,
		DestinationRoot: prefixRoot,
		Overwrite:       true,
	})
	if err != nil {
		t.Fatalf("RestoreStoredPath prefix mode: %v", err)
	}

	assertRestoreStoredPathResult(t, result, fixture.stored.Path, fixture.stored.FileID, engine.RestoreDestinationPrefix, expectedPath, fixture.stored.FileHash)
	assertRestoredBytes(t, result.DestinationPath, fixture.payload)
}

func TestRestoreStoredPathOverrideMode(t *testing.T) {
	fixture := newStoredPathRestoreFixture(t, "restore-stored-path-override")
	overridePath := filepath.Join(t.TempDir(), "custom", "restore-target.bin")

	result, err := fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
		StoredPath:      fixture.stored.Path,
		DestinationMode: engine.RestoreDestinationOverride,
		DestinationPath: overridePath,
		Overwrite:       true,
	})
	if err != nil {
		t.Fatalf("RestoreStoredPath override mode: %v", err)
	}

	assertRestoreStoredPathResult(t, result, fixture.stored.Path, fixture.stored.FileID, engine.RestoreDestinationOverride, overridePath, fixture.stored.FileHash)
	assertRestoredBytes(t, result.DestinationPath, fixture.payload)
}

func TestRestoreStoredPathPrefixModeAllowsOuterAliasAboveTrustedRoot(t *testing.T) {
	fixture := newStoredPathRestoreFixture(t, "restore-stored-path-prefix-outer-alias")
	realParent := t.TempDir()
	aliasLink := filepath.Join(t.TempDir(), "outer-link")
	requireSymlink(t, realParent, aliasLink)

	realRoot := filepath.Join(realParent, "trusted-root")
	if err := os.MkdirAll(realRoot, 0o700); err != nil {
		t.Fatalf("mkdir real root: %v", err)
	}
	aliasRoot := filepath.Join(aliasLink, "trusted-root")
	expectedPath := expectedPrefixModeOutputPath(aliasRoot, fixture.stored.Path)

	result, err := fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
		StoredPath:      fixture.stored.Path,
		DestinationMode: engine.RestoreDestinationPrefix,
		DestinationRoot: aliasRoot,
		Overwrite:       true,
	})
	if err != nil {
		t.Fatalf("RestoreStoredPath prefix outer alias: %v", err)
	}

	assertRestoreStoredPathResult(t, result, fixture.stored.Path, fixture.stored.FileID, engine.RestoreDestinationPrefix, expectedPath, fixture.stored.FileHash)
	assertRestoredBytes(t, result.DestinationPath, fixture.payload)
}

func TestRestoreStoredPathOverrideModeAllowsOuterAliasAboveDerivedRoot(t *testing.T) {
	fixture := newStoredPathRestoreFixture(t, "restore-stored-path-override-outer-alias")
	realParent := t.TempDir()
	aliasLink := filepath.Join(t.TempDir(), "outer-link")
	requireSymlink(t, realParent, aliasLink)

	realRoot := filepath.Join(realParent, "override-root")
	if err := os.MkdirAll(realRoot, 0o755); err != nil {
		t.Fatalf("mkdir real root: %v", err)
	}
	overridePath := filepath.Join(aliasLink, "override-root", "restore-target.bin")

	result, err := fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
		StoredPath:      fixture.stored.Path,
		DestinationMode: engine.RestoreDestinationOverride,
		DestinationPath: overridePath,
		Overwrite:       true,
	})
	if err != nil {
		t.Fatalf("RestoreStoredPath override outer alias: %v", err)
	}

	assertRestoreStoredPathResult(t, result, fixture.stored.Path, fixture.stored.FileID, engine.RestoreDestinationOverride, overridePath, fixture.stored.FileHash)
	assertRestoredBytes(t, result.DestinationPath, fixture.payload)
}

func TestRestoreStoredPathPreservesExistingDestinationWithoutOverwrite(t *testing.T) {
	fixture := newStoredPathRestoreFixture(t, "restore-stored-path-no-overwrite")

	result, err := fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
		StoredPath: fixture.stored.Path,
	})
	if err == nil || !strings.Contains(err.Error(), "output file already exists") {
		t.Fatalf("expected destination conflict error, got result=%+v err=%v", result, err)
	}
	if engine.IsUnsupported(err) {
		t.Fatalf("destination conflict must not classify as unsupported: %v", err)
	}
	if result != (engine.RestoreStoredPathResult{}) {
		t.Fatalf("expected zero result on destination conflict, got %+v", result)
	}
}

func TestRestoreStoredPathOverwritesWhenRequested(t *testing.T) {
	fixture := newStoredPathRestoreFixture(t, "restore-stored-path-overwrite")

	result, err := fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
		StoredPath: fixture.stored.Path,
		Overwrite:  true,
	})
	if err != nil {
		t.Fatalf("RestoreStoredPath overwrite=true: %v", err)
	}

	assertRestoreStoredPathResult(t, result, fixture.stored.Path, fixture.stored.FileID, engine.RestoreDestinationOriginal, fixture.stored.Path, fixture.stored.FileHash)
	assertRestoredBytes(t, result.DestinationPath, fixture.payload)
}

func TestRestoreStoredPathForwardsStrictMetadata(t *testing.T) {
	fixture := newStoredPathRestoreFixture(t, "restore-stored-path-strict")
	setPhysicalFileMetadataComplete(t, fixture.db, fixture.stored.FileID, false)

	result, err := fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
		StoredPath:     fixture.stored.Path,
		Overwrite:      true,
		StrictMetadata: true,
	})
	if err == nil || !strings.Contains(err.Error(), "metadata is incomplete") {
		t.Fatalf("expected strict metadata error, got result=%+v err=%v", result, err)
	}
	if engine.IsUnsupported(err) {
		t.Fatalf("strict metadata error must not classify as unsupported: %v", err)
	}
	if result != (engine.RestoreStoredPathResult{}) {
		t.Fatalf("expected zero result on strict metadata failure, got %+v", result)
	}
}

func TestRestoreStoredPathForwardsNoMetadata(t *testing.T) {
	fixture := newStoredPathRestoreFixture(t, "restore-stored-path-no-metadata")
	setPhysicalFileMetadataComplete(t, fixture.db, fixture.stored.FileID, false)

	result, err := fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
		StoredPath: fixture.stored.Path,
		Overwrite:  true,
		NoMetadata: true,
	})
	if err != nil {
		t.Fatalf("RestoreStoredPath no-metadata: %v", err)
	}

	assertRestoreStoredPathResult(t, result, fixture.stored.Path, fixture.stored.FileID, engine.RestoreDestinationOriginal, fixture.stored.Path, fixture.stored.FileHash)
	assertRestoredBytes(t, result.DestinationPath, fixture.payload)
}

func TestRestoreStoredPathPreservesCatalogIdentityAndMappingState(t *testing.T) {
	fixture := newStoredPathRestoreFixture(t, "restore-stored-path-invariants")
	seedSnapshotRetentionReference(t, fixture.db, fixture.stored.FileID, fixture.stored.Path)
	before := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)

	prefixRoot := t.TempDir()
	result, err := fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
		StoredPath:      fixture.stored.Path,
		DestinationMode: engine.RestoreDestinationPrefix,
		DestinationRoot: prefixRoot,
		Overwrite:       true,
	})
	if err != nil {
		t.Fatalf("RestoreStoredPath invariant restore: %v", err)
	}
	assertRestoredBytes(t, result.DestinationPath, fixture.payload)

	after := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)
	assertRestoreCatalogStateEqual(t, before, after)
}

func TestRestoreStoredPathReleasesPinsAfterSuccess(t *testing.T) {
	fixture := newStoredPathRestoreFixture(t, "restore-stored-path-pin-success")
	chunkIDs := logicalFileChunkIDs(t, fixture.db, fixture.stored.FileID)

	result, err := fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
		StoredPath: fixture.stored.Path,
		Overwrite:  true,
	})
	if err != nil {
		t.Fatalf("RestoreStoredPath pin success: %v", err)
	}
	assertRestoredBytes(t, result.DestinationPath, fixture.payload)

	for _, chunkID := range chunkIDs {
		if got := chunkPinCount(t, fixture.db, chunkID); got != 0 {
			t.Fatalf("expected pin_count=0 after successful restore for chunk %d, got %d", chunkID, got)
		}
	}
}

func TestRestoreStoredPathReleasesPinsAfterFailure(t *testing.T) {
	fixture := newStoredPathRestoreFixture(t, "restore-stored-path-pin-failure")
	chunkIDs := logicalFileChunkIDs(t, fixture.db, fixture.stored.FileID)

	hookCalled := false
	storage.TestRestoreFailBeforeRenameHook = func(tempOutputPath, outputPath string) error {
		hookCalled = true
		return fmt.Errorf("forced failure before rename")
	}
	defer func() { storage.TestRestoreFailBeforeRenameHook = nil }()

	result, err := fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
		StoredPath:      fixture.stored.Path,
		DestinationMode: engine.RestoreDestinationOverride,
		DestinationPath: filepath.Join(t.TempDir(), "out-failure.bin"),
		Overwrite:       true,
	})
	if err == nil || !strings.Contains(err.Error(), "test hook restore failure") {
		t.Fatalf("expected restore failure from rename hook, got result=%+v err=%v", result, err)
	}
	if !hookCalled {
		t.Fatal("expected rename failure hook to be called")
	}

	for _, chunkID := range chunkIDs {
		if got := chunkPinCount(t, fixture.db, chunkID); got != 0 {
			t.Fatalf("expected pin_count=0 after failed restore for chunk %d, got %d", chunkID, got)
		}
	}
}

func TestRestoreStoredPathReturnsMissingMappingErrorUnchanged(t *testing.T) {
	fixture := newStoredPathRestoreFixture(t, "restore-stored-path-missing-mapping")

	result, err := fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
		StoredPath: "/missing/path.bin",
		Overwrite:  true,
	})
	if err == nil || !strings.Contains(err.Error(), `physical file path "/missing/path.bin" not found`) {
		t.Fatalf("expected missing mapping error, got result=%+v err=%v", result, err)
	}
	if engine.IsUnsupported(err) {
		t.Fatalf("missing mapping error must not classify as unsupported: %v", err)
	}
	if result != (engine.RestoreStoredPathResult{}) {
		t.Fatalf("expected zero result on missing mapping error, got %+v", result)
	}
}

func TestRestoreStoredPathReturnsDestinationConflictErrorUnchanged(t *testing.T) {
	fixture := newStoredPathRestoreFixture(t, "restore-stored-path-conflict")

	result, err := fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
		StoredPath: fixture.stored.Path,
	})
	if err == nil || !strings.Contains(err.Error(), "output file already exists") {
		t.Fatalf("expected destination conflict error, got result=%+v err=%v", result, err)
	}
	if engine.IsUnsupported(err) {
		t.Fatalf("destination conflict error must not classify as unsupported: %v", err)
	}
	if result != (engine.RestoreStoredPathResult{}) {
		t.Fatalf("expected zero result on destination conflict, got %+v", result)
	}
}

func TestRestoreStoredPathPostgres(t *testing.T) {
	testgate.RequireDB(t)
	t.Setenv("COLDKEEP_DB_AUTO_BOOTSTRAP", "true")

	db := openTempPostgresEngineDatabase(t, "coldkeep_phase6_restore")
	if err := dbpkg.EnsurePostgresSchema(db); err != nil {
		t.Fatalf("EnsurePostgresSchema: %v", err)
	}

	payload := []byte("restore-stored-path-postgres")
	fixture := newStoredPathRestoreFixtureFromDB(t, db, payload, t.TempDir())
	seedSnapshotRetentionReference(t, fixture.db, fixture.stored.FileID, fixture.stored.Path)
	before := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)

	prefixRoot := t.TempDir()
	result, err := fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
		StoredPath:      fixture.stored.Path,
		DestinationMode: engine.RestoreDestinationPrefix,
		DestinationRoot: prefixRoot,
		Overwrite:       true,
	})
	if err != nil {
		t.Fatalf("RestoreStoredPath postgres: %v", err)
	}

	expectedPath := expectedPrefixModeOutputPath(prefixRoot, fixture.stored.Path)
	assertRestoreStoredPathResult(t, result, fixture.stored.Path, fixture.stored.FileID, engine.RestoreDestinationPrefix, expectedPath, fixture.stored.FileHash)
	assertRestoredBytes(t, result.DestinationPath, fixture.payload)

	after := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)
	assertRestoreCatalogStateEqual(t, before, after)
}

func newStoredPathRestoreFixture(t *testing.T, content string) storedPathRestoreFixture {
	t.Helper()

	return newStoredPathRestoreFixtureFromDB(t, openSnapshotTestDB(t), []byte(content), t.TempDir())
}

func newStoredPathRestoreFixtureFromDB(t *testing.T, db *sql.DB, payload []byte, containerDir string) storedPathRestoreFixture {
	t.Helper()

	sgctx := storage.StorageContext{
		DB:           db,
		Writer:       container.NewLocalWriterWithDirAndDB(containerDir, container.GetContainerMaxSize(), db),
		ContainerDir: containerDir,
	}

	inputPath := filepath.Join(t.TempDir(), "restore-stored-path-postgres.txt")
	if err := os.WriteFile(inputPath, payload, 0o600); err != nil {
		t.Fatalf("write input: %v", err)
	}
	stored, err := storage.StoreFileWithStorageContextAndCodecResult(sgctx, inputPath, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("store fixture: %v", err)
	}
	return newStoredPathRestoreFixtureFromExistingStore(t, db, sgctx, stored, payload)
}

func newStoredPathRestoreFixtureFromExistingStore(t *testing.T, db *sql.DB, sgctx storage.StorageContext, stored storage.StoreFileResult, payload []byte) storedPathRestoreFixture {
	t.Helper()

	eng := newRemoveTestEngine(t, db, sgctx.ContainerDir)
	return storedPathRestoreFixture{
		db:      db,
		sgctx:   sgctx,
		engine:  eng,
		stored:  stored,
		payload: payload,
	}
}

func assertRestoreStoredPathValidationError(t *testing.T, result engine.RestoreStoredPathResult, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected validation error %q", want)
	}
	if engine.IsUnsupported(err) {
		t.Fatalf("expected validation error to remain non-unsupported: %v", err)
	}
	if err.Error() != want {
		t.Fatalf("expected validation error %q, got %q", want, err.Error())
	}
	if result != (engine.RestoreStoredPathResult{}) {
		t.Fatalf("expected zero result on validation failure, got %+v", result)
	}
}

func assertRestoreStoredPathResult(t *testing.T, got engine.RestoreStoredPathResult, wantStoredPath string, wantFileID int64, wantMode engine.RestoreDestinationMode, wantDestinationPath string, wantHash string) {
	t.Helper()
	if got.StoredPath != wantStoredPath {
		t.Fatalf("StoredPath: got %q want %q", got.StoredPath, wantStoredPath)
	}
	if got.FileID != wantFileID {
		t.Fatalf("FileID: got %d want %d", got.FileID, wantFileID)
	}
	if got.DestinationMode != wantMode {
		t.Fatalf("DestinationMode: got %q want %q", got.DestinationMode, wantMode)
	}
	if got.DestinationPath != wantDestinationPath {
		t.Fatalf("DestinationPath: got %q want %q", got.DestinationPath, wantDestinationPath)
	}
	if got.RestoredHash != wantHash {
		t.Fatalf("RestoredHash: got %q want %q", got.RestoredHash, wantHash)
	}
}
