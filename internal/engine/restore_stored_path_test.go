package engine_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	dbpkg "github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/storage"
	"github.com/franchoy/coldkeep/tests/utils/testgate"

	_ "github.com/lib/pq"
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

func assertRestoredBytes(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read restored bytes: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("restored bytes mismatch: got=%q want=%q", string(got), string(want))
	}
}

func expectedPrefixModeOutputPath(prefixRoot string, storedPath string) string {
	relativePath := storedPath
	if vol := filepath.VolumeName(relativePath); vol != "" {
		relativePath = strings.TrimPrefix(relativePath, vol)
	}
	relativePath = strings.TrimLeft(relativePath, `/\`)
	return filepath.Join(prefixRoot, relativePath)
}

func setPhysicalFileMetadataComplete(t *testing.T, db *sql.DB, fileID int64, complete bool) {
	t.Helper()
	if _, err := db.Exec(`UPDATE physical_file SET is_metadata_complete = $1, mode = NULL, mtime = NULL, uid = NULL, gid = NULL WHERE logical_file_id = $2`, complete, fileID); err != nil {
		t.Fatalf("update physical_file metadata completeness: %v", err)
	}
}

func seedSnapshotRetentionReference(t *testing.T, db *sql.DB, fileID int64, storedPath string) {
	t.Helper()
	snapshotID := fmt.Sprintf("snap-restore-%d", time.Now().UnixNano())
	pathID := time.Now().UnixNano()
	if _, err := db.Exec(`INSERT INTO snapshot (id, created_at, type, label) VALUES ($1, $2, $3, $4)`, snapshotID, time.Now().UTC().Format(time.RFC3339), "full", "restore-stored-path"); err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO snapshot_path (id, path) VALUES ($1, $2)`, pathID, storedPath); err != nil {
		t.Fatalf("insert snapshot_path: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id, size) VALUES ($1, $2, $3, $4)`, snapshotID, pathID, fileID, int64(len(storedPath))); err != nil {
		t.Fatalf("insert snapshot_file: %v", err)
	}
}

func snapshotRestoreCatalogState(t *testing.T, db *sql.DB, fileID int64) restoreCatalogState {
	t.Helper()

	state := restoreCatalogState{
		chunkLiveRefs:  make(map[int64]int64),
		chunkPinCounts: make(map[int64]int64),
	}

	if err := db.QueryRow(`SELECT COUNT(*) FROM logical_file`).Scan(&state.logicalFileCount); err != nil {
		t.Fatalf("count logical_file rows: %v", err)
	}
	if err := db.QueryRow(`SELECT ref_count FROM logical_file WHERE id = $1`, fileID).Scan(&state.refCount); err != nil {
		t.Fatalf("read logical_file.ref_count: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM physical_file WHERE logical_file_id = $1`, fileID).Scan(&state.physicalCount); err != nil {
		t.Fatalf("count physical_file rows: %v", err)
	}
	if state.physicalCount > 0 {
		if err := db.QueryRow(`SELECT path FROM physical_file WHERE logical_file_id = $1 ORDER BY path LIMIT 1`, fileID).Scan(&state.physicalPath); err != nil {
			t.Fatalf("read physical_file.path: %v", err)
		}
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM snapshot_file WHERE logical_file_id = $1`, fileID).Scan(&state.snapshotCount); err != nil {
		t.Fatalf("count snapshot_file rows: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM file_chunk WHERE logical_file_id = $1`, fileID).Scan(&state.fileChunkCount); err != nil {
		t.Fatalf("count file_chunk rows: %v", err)
	}

	rows, err := db.Query(`
		SELECT c.id, c.live_ref_count, c.pin_count
		FROM file_chunk fc
		JOIN chunk c ON c.id = fc.chunk_id
		WHERE fc.logical_file_id = $1
		ORDER BY fc.chunk_order ASC
	`, fileID)
	if err != nil {
		t.Fatalf("query chunk refs: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var chunkID, liveRefCount, pinCount int64
		if err := rows.Scan(&chunkID, &liveRefCount, &pinCount); err != nil {
			t.Fatalf("scan chunk refs: %v", err)
		}
		state.chunkLiveRefs[chunkID] = liveRefCount
		state.chunkPinCounts[chunkID] = pinCount
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate chunk refs: %v", err)
	}

	return state
}

func assertRestoreCatalogStateEqual(t *testing.T, before, after restoreCatalogState) {
	t.Helper()
	if before.logicalFileCount != after.logicalFileCount {
		t.Fatalf("logical_file count changed: before=%d after=%d", before.logicalFileCount, after.logicalFileCount)
	}
	if before.refCount != after.refCount {
		t.Fatalf("logical_file.ref_count changed: before=%d after=%d", before.refCount, after.refCount)
	}
	if before.physicalCount != after.physicalCount {
		t.Fatalf("physical_file count changed: before=%d after=%d", before.physicalCount, after.physicalCount)
	}
	if before.physicalPath != after.physicalPath {
		t.Fatalf("physical_file.path changed: before=%q after=%q", before.physicalPath, after.physicalPath)
	}
	if before.snapshotCount != after.snapshotCount {
		t.Fatalf("snapshot_file count changed: before=%d after=%d", before.snapshotCount, after.snapshotCount)
	}
	if before.fileChunkCount != after.fileChunkCount {
		t.Fatalf("file_chunk count changed: before=%d after=%d", before.fileChunkCount, after.fileChunkCount)
	}
	if len(before.chunkLiveRefs) != len(after.chunkLiveRefs) {
		t.Fatalf("chunk.live_ref_count set changed: before=%v after=%v", before.chunkLiveRefs, after.chunkLiveRefs)
	}
	for chunkID, beforeCount := range before.chunkLiveRefs {
		if after.chunkLiveRefs[chunkID] != beforeCount {
			t.Fatalf("chunk.live_ref_count changed for chunk %d: before=%d after=%d", chunkID, beforeCount, after.chunkLiveRefs[chunkID])
		}
		if after.chunkPinCounts[chunkID] != before.chunkPinCounts[chunkID] {
			t.Fatalf("chunk.pin_count changed for chunk %d: before=%d after=%d", chunkID, before.chunkPinCounts[chunkID], after.chunkPinCounts[chunkID])
		}
	}
}

func logicalFileChunkIDs(t *testing.T, db *sql.DB, fileID int64) []int64 {
	t.Helper()

	rows, err := db.Query(`SELECT chunk_id FROM file_chunk WHERE logical_file_id = $1 ORDER BY chunk_order ASC`, fileID)
	if err != nil {
		t.Fatalf("query file chunks: %v", err)
	}
	defer rows.Close()

	var ids []int64
	for rows.Next() {
		var chunkID int64
		if err := rows.Scan(&chunkID); err != nil {
			t.Fatalf("scan chunk id: %v", err)
		}
		ids = append(ids, chunkID)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate file chunks: %v", err)
	}
	return ids
}

func chunkPinCount(t *testing.T, db *sql.DB, chunkID int64) int64 {
	t.Helper()
	var pinCount int64
	if err := db.QueryRow(`SELECT pin_count FROM chunk WHERE id = $1`, chunkID).Scan(&pinCount); err != nil {
		t.Fatalf("read chunk pin_count: %v", err)
	}
	return pinCount
}

func openTempPostgresEngineDatabase(t *testing.T, prefix string) *sql.DB {
	t.Helper()

	adminDB := openRawPostgresDB(t, "")
	testDBName := fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	if _, err := adminDB.Exec(fmt.Sprintf("CREATE DATABASE %s", testDBName)); err != nil {
		_ = adminDB.Close()
		t.Fatalf("create temporary postgres database %s: %v", testDBName, err)
	}

	t.Cleanup(func() {
		_, _ = adminDB.Exec(`
			SELECT pg_terminate_backend(pid)
			FROM pg_stat_activity
			WHERE datname = $1 AND pid <> pg_backend_pid()
		`, testDBName)
		_, _ = adminDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDBName))
		_ = adminDB.Close()
	})

	db := openRawPostgresDB(t, testDBName)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func openRawPostgresDB(t *testing.T, dbName string) *sql.DB {
	t.Helper()

	connStr, err := dbpkg.BuildPostgresConnStringFromEnv(dbName)
	if err != nil {
		t.Fatalf("BuildPostgresConnStringFromEnv(%q): %v", dbName, err)
	}
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("sql.Open(postgres): %v", err)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		t.Fatalf("ping postgres: %v", err)
	}
	return db
}
