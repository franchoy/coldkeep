package engine_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/tests/utils/testgate"
)

func requirePathAbsent(t *testing.T, path string) {
	t.Helper()

	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("expected path %q to be absent, stat=%v", path, err)
	}
}

func requireFileBytes(t *testing.T, path string, want []byte) {
	t.Helper()
	assertRestoredBytes(t, path, want)
}

func requireNoRestoreTempFiles(t *testing.T, dir string) {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir %q: %v", dir, err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".coldkeep-restore-") {
			t.Fatalf("unexpected restore temp file left behind: %q", filepath.Join(dir, entry.Name()))
		}
	}
}

func requireSymlink(t *testing.T, oldname, newname string) {
	t.Helper()

	if err := os.Symlink(oldname, newname); err != nil {
		t.Skipf("symlink unavailable on this platform/environment: %v", err)
	}
}

func updateStoredPathMapping(t *testing.T, db *sql.DB, fileID int64, storedPath string) {
	t.Helper()

	if _, err := db.Exec(`UPDATE physical_file SET path = $1 WHERE logical_file_id = $2`, storedPath, fileID); err != nil {
		t.Fatalf("update stored path mapping: %v", err)
	}
}

func requirePinnedChunksReleased(t *testing.T, db *sql.DB, fileID int64) {
	t.Helper()

	for _, chunkID := range logicalFileChunkIDs(t, db, fileID) {
		if got := chunkPinCount(t, db, chunkID); got != 0 {
			t.Fatalf("expected chunk %d pin_count=0, got %d", chunkID, got)
		}
	}
}

func removeAllContainerPayloads(t *testing.T, fixture storedPathRestoreFixture) {
	t.Helper()

	entries, err := os.ReadDir(fixture.sgctx.ContainerDir)
	if err != nil {
		t.Fatalf("read containers dir: %v", err)
	}
	for _, entry := range entries {
		if err := os.Remove(filepath.Join(fixture.sgctx.ContainerDir, entry.Name())); err != nil {
			t.Fatalf("remove container payload %q: %v", entry.Name(), err)
		}
	}
}

func applyPostgresSchemaFromRepo(t *testing.T, db *sql.DB) {
	t.Helper()

	schemaSQL, err := os.ReadFile(filepath.Join("..", "..", "db", "schema_postgres.sql"))
	if err != nil {
		t.Fatalf("read postgres schema SQL: %v", err)
	}
	if _, err := db.Exec(string(schemaSQL)); err != nil {
		t.Fatalf("apply postgres schema SQL: %v", err)
	}
}

func TestRestoreStoredPathValidationFailuresDoNotMutateState(t *testing.T) {
	fixture := newStoredPathRestoreFixture(t, "restore-stored-path-validation-safety")
	before := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)

	overrideTarget := filepath.Join(t.TempDir(), "override.bin")
	cases := []struct {
		name           string
		req            engine.RestoreStoredPathRequest
		wantErr        string
		expectNoOutput string
	}{
		{
			name: "blank stored path",
			req: engine.RestoreStoredPathRequest{
				StoredPath:      "   ",
				DestinationMode: engine.RestoreDestinationOverride,
				DestinationPath: overrideTarget,
			},
			wantErr:        "engine: restore stored path is required",
			expectNoOutput: overrideTarget,
		},
		{
			name: "strict and no metadata",
			req: engine.RestoreStoredPathRequest{
				StoredPath:     fixture.stored.Path,
				StrictMetadata: true,
				NoMetadata:     true,
			},
			wantErr: "engine: restore stored path strict metadata and no metadata are mutually exclusive",
		},
		{
			name: "unknown destination mode",
			req: engine.RestoreStoredPathRequest{
				StoredPath:      fixture.stored.Path,
				DestinationMode: engine.RestoreDestinationMode("weird"),
			},
			wantErr: `engine: invalid restore stored-path destination mode "weird"`,
		},
		{
			name: "original forbids destination root",
			req: engine.RestoreStoredPathRequest{
				StoredPath:      fixture.stored.Path,
				DestinationMode: engine.RestoreDestinationOriginal,
				DestinationRoot: t.TempDir(),
			},
			wantErr: "engine: restore stored path original mode does not accept a destination root",
		},
		{
			name: "prefix requires destination root",
			req: engine.RestoreStoredPathRequest{
				StoredPath:      fixture.stored.Path,
				DestinationMode: engine.RestoreDestinationPrefix,
			},
			wantErr: "engine: restore stored path prefix mode requires a destination root",
		},
		{
			name: "override requires destination path",
			req: engine.RestoreStoredPathRequest{
				StoredPath:      fixture.stored.Path,
				DestinationMode: engine.RestoreDestinationOverride,
			},
			wantErr: "engine: restore stored path override mode requires an exact destination path",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := fixture.engine.RestoreStoredPath(context.Background(), tc.req)
			assertRestoreStoredPathValidationError(t, result, err, tc.wantErr)
			if tc.expectNoOutput != "" {
				requirePathAbsent(t, tc.expectNoOutput)
			}
			after := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)
			assertRestoreCatalogStateEqual(t, before, after)
			requirePinnedChunksReleased(t, fixture.db, fixture.stored.FileID)
		})
	}
}

func TestRestoreStoredPathOriginalModeWritesOnlyToStoredPath(t *testing.T) {
	fixture := newStoredPathRestoreFixture(t, "restore-stored-path-original-safety")
	siblingPath := filepath.Join(filepath.Dir(fixture.stored.Path), "neighbor.bin")
	before := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)

	if err := os.Remove(fixture.stored.Path); err != nil {
		t.Fatalf("remove stored path before restore: %v", err)
	}
	requirePathAbsent(t, fixture.stored.Path)

	result, err := fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
		StoredPath: fixture.stored.Path,
		Overwrite:  true,
	})
	if err != nil {
		t.Fatalf("RestoreStoredPath original safety: %v", err)
	}

	assertRestoreStoredPathResult(t, result, fixture.stored.Path, fixture.stored.FileID, engine.RestoreDestinationOriginal, fixture.stored.Path, fixture.stored.FileHash)
	requireFileBytes(t, result.DestinationPath, fixture.payload)
	requirePathAbsent(t, siblingPath)
	after := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)
	assertRestoreCatalogStateEqual(t, before, after)
	requirePinnedChunksReleased(t, fixture.db, fixture.stored.FileID)
}

func TestRestoreStoredPathPrefixModeStaysUnderDestinationRoot(t *testing.T) {
	fixture := newStoredPathRestoreFixture(t, "restore-stored-path-prefix-safety")
	prefixRoot := t.TempDir()
	before := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)

	if err := os.Remove(fixture.stored.Path); err != nil {
		t.Fatalf("remove stored path before prefix restore: %v", err)
	}

	result, err := fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
		StoredPath:      fixture.stored.Path,
		DestinationMode: engine.RestoreDestinationPrefix,
		DestinationRoot: prefixRoot,
		Overwrite:       true,
	})
	if err != nil {
		t.Fatalf("RestoreStoredPath prefix safety: %v", err)
	}

	rel, err := filepath.Rel(prefixRoot, result.DestinationPath)
	if err != nil {
		t.Fatalf("filepath.Rel: %v", err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		t.Fatalf("prefix restore escaped root: root=%q dst=%q rel=%q", prefixRoot, result.DestinationPath, rel)
	}
	requireFileBytes(t, result.DestinationPath, fixture.payload)
	requirePathAbsent(t, fixture.stored.Path)
	after := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)
	assertRestoreCatalogStateEqual(t, before, after)
	requirePinnedChunksReleased(t, fixture.db, fixture.stored.FileID)
}

func TestRestoreStoredPathPrefixRejectsTraversalWithoutOutsideWrite(t *testing.T) {
	cases := []string{
		"../escape.bin",
		"nested/../../escape.bin",
		"/../../escape.bin",
		`..\escape.bin`,
		`nested\..\..\escape.bin`,
	}

	for _, storedPath := range cases {
		t.Run(strings.ReplaceAll(storedPath, "/", "_"), func(t *testing.T) {
			fixture := newStoredPathRestoreFixture(t, "restore-stored-path-prefix-traversal")
			updateStoredPathMapping(t, fixture.db, fixture.stored.FileID, storedPath)
			prefixRoot := t.TempDir()
			outside := filepath.Join(filepath.Dir(prefixRoot), "escape.bin")
			before := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)

			result, err := fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
				StoredPath:      storedPath,
				DestinationMode: engine.RestoreDestinationPrefix,
				DestinationRoot: prefixRoot,
				Overwrite:       true,
			})
			if err == nil {
				t.Fatalf("expected traversal restore to fail, got result=%+v", result)
			}
			if result != (engine.RestoreStoredPathResult{}) {
				t.Fatalf("expected zero result on traversal failure, got %+v", result)
			}
			requirePathAbsent(t, outside)
			requireNoRestoreTempFiles(t, prefixRoot)
			after := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)
			assertRestoreCatalogStateEqual(t, before, after)
			requirePinnedChunksReleased(t, fixture.db, fixture.stored.FileID)
		})
	}
}

func TestRestoreStoredPathPrefixFailsClosedForForeignRootedPaths(t *testing.T) {
	cases := []string{
		`C:\outside\file.bin`,
		`C:/outside/file.bin`,
		`\\server\share\file.bin`,
		`//server/share/file.bin`,
	}

	for _, storedPath := range cases {
		t.Run(strings.ReplaceAll(strings.ReplaceAll(storedPath, "\\", "_"), "/", "_"), func(t *testing.T) {
			fixture := newStoredPathRestoreFixture(t, "restore-stored-path-prefix-rooted")
			updateStoredPathMapping(t, fixture.db, fixture.stored.FileID, storedPath)
			prefixRoot := t.TempDir()
			before := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)

			result, err := fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
				StoredPath:      storedPath,
				DestinationMode: engine.RestoreDestinationPrefix,
				DestinationRoot: prefixRoot,
				Overwrite:       true,
			})
			if err == nil {
				t.Fatalf("expected rooted-path restore to fail, got result=%+v", result)
			}
			if result != (engine.RestoreStoredPathResult{}) {
				t.Fatalf("expected zero result on rooted-path failure, got %+v", result)
			}
			requireNoRestoreTempFiles(t, prefixRoot)
			after := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)
			assertRestoreCatalogStateEqual(t, before, after)
			requirePinnedChunksReleased(t, fixture.db, fixture.stored.FileID)
		})
	}
}

func TestRestoreStoredPathRejectsSymlinkedPrefixRoot(t *testing.T) {
	fixture := newStoredPathRestoreFixture(t, "restore-stored-path-prefix-symlink-root")
	realRoot := t.TempDir()
	symlinkRoot := filepath.Join(t.TempDir(), "restore-root-link")
	requireSymlink(t, realRoot, symlinkRoot)
	before := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)

	result, err := fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
		StoredPath:      fixture.stored.Path,
		DestinationMode: engine.RestoreDestinationPrefix,
		DestinationRoot: symlinkRoot,
		Overwrite:       true,
	})
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("expected symlink root rejection, got result=%+v err=%v", result, err)
	}
	requireNoRestoreTempFiles(t, realRoot)
	after := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)
	assertRestoreCatalogStateEqual(t, before, after)
	requirePinnedChunksReleased(t, fixture.db, fixture.stored.FileID)
}

func TestRestoreStoredPathPrefixRejectsSymlinkedParentWithoutOutsideWrite(t *testing.T) {
	fixture := newStoredPathRestoreFixture(t, "restore-stored-path-prefix-symlink-parent")
	prefixRoot := t.TempDir()
	outside := t.TempDir()
	storedPath := "/tmp/parent/escape.bin"
	updateStoredPathMapping(t, fixture.db, fixture.stored.FileID, storedPath)
	requireSymlink(t, outside, filepath.Join(prefixRoot, "tmp"))
	before := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)

	result, err := fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
		StoredPath:      storedPath,
		DestinationMode: engine.RestoreDestinationPrefix,
		DestinationRoot: prefixRoot,
		Overwrite:       true,
	})
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("expected symlink parent rejection, got result=%+v err=%v", result, err)
	}
	requirePathAbsent(t, filepath.Join(outside, "parent", "escape.bin"))
	requireNoRestoreTempFiles(t, prefixRoot)
	after := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)
	assertRestoreCatalogStateEqual(t, before, after)
	requirePinnedChunksReleased(t, fixture.db, fixture.stored.FileID)
}

func TestRestoreStoredPathOverrideRejectsSymlinkedParentWithoutOutsideWrite(t *testing.T) {
	fixture := newStoredPathRestoreFixture(t, "restore-stored-path-override-symlink-parent")
	outside := t.TempDir()
	overrideRoot := t.TempDir()
	linkParent := filepath.Join(overrideRoot, "linked-parent")
	requireSymlink(t, outside, linkParent)
	overridePath := filepath.Join(linkParent, "restored.bin")
	before := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)

	result, err := fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
		StoredPath:      fixture.stored.Path,
		DestinationMode: engine.RestoreDestinationOverride,
		DestinationPath: overridePath,
		Overwrite:       true,
	})
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("expected symlinked override parent rejection, got result=%+v err=%v", result, err)
	}
	requirePathAbsent(t, filepath.Join(outside, "restored.bin"))
	requireNoRestoreTempFiles(t, outside)
	after := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)
	assertRestoreCatalogStateEqual(t, before, after)
	requirePinnedChunksReleased(t, fixture.db, fixture.stored.FileID)
}

func TestRestoreStoredPathOriginalModeRejectsSymlinkTarget(t *testing.T) {
	fixture := newStoredPathRestoreFixture(t, "restore-stored-path-original-symlink-target")
	outside := filepath.Join(t.TempDir(), "sentinel.bin")
	sentinel := []byte("outside-sentinel")
	if err := os.WriteFile(outside, sentinel, 0o600); err != nil {
		t.Fatalf("write sentinel: %v", err)
	}
	symlinkPath := filepath.Join(t.TempDir(), "stored-link.bin")
	requireSymlink(t, outside, symlinkPath)
	updateStoredPathMapping(t, fixture.db, fixture.stored.FileID, symlinkPath)
	before := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)

	result, err := fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
		StoredPath: symlinkPath,
		Overwrite:  true,
	})
	if err == nil {
		t.Fatalf("expected original symlink target restore to fail closed, got result=%+v", result)
	}
	requireFileBytes(t, outside, sentinel)
	requireNoRestoreTempFiles(t, filepath.Dir(symlinkPath))
	after := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)
	assertRestoreCatalogStateEqual(t, before, after)
	requirePinnedChunksReleased(t, fixture.db, fixture.stored.FileID)
}

func TestRestoreStoredPathOriginalModeRejectsSymlinkedParentWithoutOutsideWrite(t *testing.T) {
	fixture := newStoredPathRestoreFixture(t, "restore-stored-path-original-symlink-parent")
	outside := t.TempDir()
	linkParent := filepath.Join(t.TempDir(), "stored-parent-link")
	requireSymlink(t, outside, linkParent)
	storedPath := filepath.Join(linkParent, "restored.bin")
	updateStoredPathMapping(t, fixture.db, fixture.stored.FileID, storedPath)
	before := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)

	result, err := fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
		StoredPath: storedPath,
		Overwrite:  true,
	})
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("expected original symlink parent rejection, got result=%+v err=%v", result, err)
	}
	requirePathAbsent(t, filepath.Join(outside, "restored.bin"))
	requireNoRestoreTempFiles(t, outside)
	after := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)
	assertRestoreCatalogStateEqual(t, before, after)
	requirePinnedChunksReleased(t, fixture.db, fixture.stored.FileID)
}

func TestRestoreStoredPathOverwriteFalsePreservesExistingDestination(t *testing.T) {
	cases := []struct {
		name           string
		buildRequest   func(t *testing.T, fixture storedPathRestoreFixture, sentinel []byte) engine.RestoreStoredPathRequest
		destination    func(t *testing.T, fixture storedPathRestoreFixture) string
		expectedErrSub string
	}{
		{
			name: "original",
			buildRequest: func(t *testing.T, fixture storedPathRestoreFixture, sentinel []byte) engine.RestoreStoredPathRequest {
				if err := os.WriteFile(fixture.stored.Path, sentinel, 0o600); err != nil {
					t.Fatalf("write original sentinel: %v", err)
				}
				return engine.RestoreStoredPathRequest{StoredPath: fixture.stored.Path}
			},
			destination:    func(t *testing.T, fixture storedPathRestoreFixture) string { return fixture.stored.Path },
			expectedErrSub: "output file already exists",
		},
		{
			name: "prefix",
			buildRequest: func(t *testing.T, fixture storedPathRestoreFixture, sentinel []byte) engine.RestoreStoredPathRequest {
				prefixRoot := t.TempDir()
				dst := expectedPrefixModeOutputPath(prefixRoot, fixture.stored.Path)
				if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
					t.Fatalf("mkdir prefix dst: %v", err)
				}
				if err := os.WriteFile(dst, sentinel, 0o600); err != nil {
					t.Fatalf("write prefix sentinel: %v", err)
				}
				return engine.RestoreStoredPathRequest{
					StoredPath:      fixture.stored.Path,
					DestinationMode: engine.RestoreDestinationPrefix,
					DestinationRoot: prefixRoot,
				}
			},
			destination:    func(t *testing.T, fixture storedPathRestoreFixture) string { return "" },
			expectedErrSub: "output file already exists",
		},
		{
			name: "override",
			buildRequest: func(t *testing.T, fixture storedPathRestoreFixture, sentinel []byte) engine.RestoreStoredPathRequest {
				overridePath := filepath.Join(t.TempDir(), "override-existing.bin")
				if err := os.MkdirAll(filepath.Dir(overridePath), 0o755); err != nil {
					t.Fatalf("mkdir override dst: %v", err)
				}
				if err := os.WriteFile(overridePath, sentinel, 0o600); err != nil {
					t.Fatalf("write override sentinel: %v", err)
				}
				return engine.RestoreStoredPathRequest{
					StoredPath:      fixture.stored.Path,
					DestinationMode: engine.RestoreDestinationOverride,
					DestinationPath: overridePath,
				}
			},
			destination:    func(t *testing.T, fixture storedPathRestoreFixture) string { return "" },
			expectedErrSub: "output file already exists",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newStoredPathRestoreFixture(t, "restore-stored-path-overwrite-false")
			before := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)
			sentinel := []byte("existing-destination")
			req := tc.buildRequest(t, fixture, sentinel)

			result, err := fixture.engine.RestoreStoredPath(context.Background(), req)
			if err == nil || !strings.Contains(err.Error(), tc.expectedErrSub) {
				t.Fatalf("expected overwrite=false failure, got result=%+v err=%v", result, err)
			}
			if result != (engine.RestoreStoredPathResult{}) {
				t.Fatalf("expected zero result on overwrite=false failure, got %+v", result)
			}

			switch req.DestinationMode {
			case engine.RestoreDestinationPrefix:
				requireFileBytes(t, expectedPrefixModeOutputPath(req.DestinationRoot, fixture.stored.Path), sentinel)
				requireNoRestoreTempFiles(t, req.DestinationRoot)
			case engine.RestoreDestinationOverride:
				requireFileBytes(t, req.DestinationPath, sentinel)
				requireNoRestoreTempFiles(t, filepath.Dir(req.DestinationPath))
			default:
				requireFileBytes(t, fixture.stored.Path, sentinel)
				requireNoRestoreTempFiles(t, filepath.Dir(fixture.stored.Path))
			}

			after := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)
			assertRestoreCatalogStateEqual(t, before, after)
			requirePinnedChunksReleased(t, fixture.db, fixture.stored.FileID)
		})
	}
}

func TestRestoreStoredPathMissingPayloadFailsWithoutDestinationMutation(t *testing.T) {
	fixture := newStoredPathRestoreFixture(t, "restore-stored-path-missing-payload")
	removeAllContainerPayloads(t, fixture)
	overridePath := filepath.Join(t.TempDir(), "missing-payload.bin")
	before := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)

	result, err := fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
		StoredPath:      fixture.stored.Path,
		DestinationMode: engine.RestoreDestinationOverride,
		DestinationPath: overridePath,
		Overwrite:       true,
	})
	if err == nil {
		t.Fatalf("expected missing payload failure, got result=%+v", result)
	}
	requirePathAbsent(t, overridePath)
	requireNoRestoreTempFiles(t, filepath.Dir(overridePath))
	after := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)
	assertRestoreCatalogStateEqual(t, before, after)
	requirePinnedChunksReleased(t, fixture.db, fixture.stored.FileID)
}

func TestRestoreStoredPathHashMismatchFailsClosed(t *testing.T) {
	fixture := newStoredPathRestoreFixture(t, "restore-stored-path-hash-mismatch")
	overridePath := filepath.Join(t.TempDir(), "hash-mismatch.bin")
	if _, err := fixture.db.Exec(`UPDATE logical_file SET file_hash = $1 WHERE id = $2`, strings.Repeat("f", 64), fixture.stored.FileID); err != nil {
		t.Fatalf("update logical file hash: %v", err)
	}
	before := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)

	result, err := fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
		StoredPath:      fixture.stored.Path,
		DestinationMode: engine.RestoreDestinationOverride,
		DestinationPath: overridePath,
		Overwrite:       true,
	})
	if err == nil || !strings.Contains(err.Error(), "restored file hash mismatch") {
		t.Fatalf("expected restored file hash mismatch, got result=%+v err=%v", result, err)
	}
	requirePathAbsent(t, overridePath)
	requireNoRestoreTempFiles(t, filepath.Dir(overridePath))
	after := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)
	assertRestoreCatalogStateEqual(t, before, after)
	requirePinnedChunksReleased(t, fixture.db, fixture.stored.FileID)
}

func TestRestoreStoredPathPostgresDestinationSafety(t *testing.T) {
	testgate.RequireDB(t)

	db := openTempPostgresEngineDatabase(t, "restore_stored_path_pg_safety")
	applyPostgresSchemaFromRepo(t, db)

	payload := []byte("restore-stored-path-postgres-safety")
	fixture := newStoredPathRestoreFixtureFromDB(t, db, payload, t.TempDir())
	prefixRoot := t.TempDir()
	before := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)

	result, err := fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
		StoredPath:      fixture.stored.Path,
		DestinationMode: engine.RestoreDestinationPrefix,
		DestinationRoot: prefixRoot,
		Overwrite:       true,
	})
	if err != nil {
		t.Fatalf("RestoreStoredPath postgres safety: %v", err)
	}

	requireFileBytes(t, result.DestinationPath, payload)
	rel, err := filepath.Rel(prefixRoot, result.DestinationPath)
	if err != nil {
		t.Fatalf("filepath.Rel: %v", err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		t.Fatalf("postgres prefix restore escaped root: root=%q dst=%q rel=%q", prefixRoot, result.DestinationPath, rel)
	}

	after := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)
	assertRestoreCatalogStateEqual(t, before, after)
	requirePinnedChunksReleased(t, fixture.db, fixture.stored.FileID)
}

func TestRestoreStoredPathPostgresFailurePreservesCatalogState(t *testing.T) {
	testgate.RequireDB(t)

	db := openTempPostgresEngineDatabase(t, "restore_stored_path_pg_preserve")
	applyPostgresSchemaFromRepo(t, db)

	fixture := newStoredPathRestoreFixtureFromDB(t, db, []byte("restore-stored-path-postgres-failure"), t.TempDir())
	removeAllContainerPayloads(t, fixture)
	overridePath := filepath.Join(t.TempDir(), "postgres-missing-payload.bin")
	before := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)

	result, err := fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
		StoredPath:      fixture.stored.Path,
		DestinationMode: engine.RestoreDestinationOverride,
		DestinationPath: overridePath,
		Overwrite:       true,
	})
	if err == nil {
		t.Fatalf("expected postgres missing payload failure, got result=%+v", result)
	}

	requirePathAbsent(t, overridePath)
	requireNoRestoreTempFiles(t, filepath.Dir(overridePath))
	after := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)
	assertRestoreCatalogStateEqual(t, before, after)
	requirePinnedChunksReleased(t, fixture.db, fixture.stored.FileID)
}

func TestRestoreStoredPathFailuresPreserveCatalogOwnershipState(t *testing.T) {
	cases := []struct {
		name string
		run  func(t *testing.T, fixture storedPathRestoreFixture) (engine.RestoreStoredPathResult, error)
	}{
		{
			name: "destination conflict",
			run: func(t *testing.T, fixture storedPathRestoreFixture) (engine.RestoreStoredPathResult, error) {
				if err := os.WriteFile(fixture.stored.Path, []byte("sentinel"), 0o600); err != nil {
					t.Fatalf("write sentinel: %v", err)
				}
				return fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
					StoredPath: fixture.stored.Path,
				})
			},
		},
		{
			name: "missing payload",
			run: func(t *testing.T, fixture storedPathRestoreFixture) (engine.RestoreStoredPathResult, error) {
				removeAllContainerPayloads(t, fixture)
				return fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
					StoredPath:      fixture.stored.Path,
					DestinationMode: engine.RestoreDestinationOverride,
					DestinationPath: filepath.Join(t.TempDir(), "missing-payload.bin"),
					Overwrite:       true,
				})
			},
		},
		{
			name: "symlink rejection",
			run: func(t *testing.T, fixture storedPathRestoreFixture) (engine.RestoreStoredPathResult, error) {
				outside := t.TempDir()
				linkParent := filepath.Join(t.TempDir(), "override-parent-link")
				requireSymlink(t, outside, linkParent)
				return fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
					StoredPath:      fixture.stored.Path,
					DestinationMode: engine.RestoreDestinationOverride,
					DestinationPath: filepath.Join(linkParent, "out.bin"),
					Overwrite:       true,
				})
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newStoredPathRestoreFixture(t, fmt.Sprintf("restore-stored-path-failure-%s", tc.name))
			before := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)
			result, err := tc.run(t, fixture)
			if err == nil {
				t.Fatalf("expected failure, got result=%+v", result)
			}
			after := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)
			assertRestoreCatalogStateEqual(t, before, after)
			requirePinnedChunksReleased(t, fixture.db, fixture.stored.FileID)
		})
	}
}
