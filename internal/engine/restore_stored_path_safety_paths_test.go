package engine_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/engine"
)

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
		`C:/outside/file.bin`,
	}
	if runtime.GOOS == "windows" {
		cases = append(cases, `//server/share/data/file.bin`)
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

func TestRestoreStoredPathPrefixAcceptsWindowsDriveQualifiedStoredPath(t *testing.T) {
	fixture := newStoredPathRestoreFixture(t, "restore-stored-path-prefix-drive")
	storedPath := `C:\data\file.bin`
	updateStoredPathMapping(t, fixture.db, fixture.stored.FileID, storedPath)
	prefixRoot := t.TempDir()
	before := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)

	result, err := fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
		StoredPath:      storedPath,
		DestinationMode: engine.RestoreDestinationPrefix,
		DestinationRoot: prefixRoot,
		Overwrite:       true,
	})
	if err != nil {
		t.Fatalf("expected drive-qualified prefix restore to succeed, got: %v", err)
	}

	expectedPath := filepath.Join(prefixRoot, "data", "file.bin")
	assertRestoreStoredPathResult(t, result, storedPath, fixture.stored.FileID, engine.RestoreDestinationPrefix, expectedPath, fixture.stored.FileHash)
	requireFileBytes(t, result.DestinationPath, fixture.payload)
	requireNoRestoreTempFiles(t, prefixRoot)
	after := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)
	assertRestoreCatalogStateEqual(t, before, after)
	requirePinnedChunksReleased(t, fixture.db, fixture.stored.FileID)
}

func TestRestoreStoredPathPrefixAcceptsWindowsUNCStoredPath(t *testing.T) {
	fixture := newStoredPathRestoreFixture(t, "restore-stored-path-prefix-unc")
	storedPath := `\\server\share\data\file.bin`
	updateStoredPathMapping(t, fixture.db, fixture.stored.FileID, storedPath)
	prefixRoot := t.TempDir()
	before := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)

	result, err := fixture.engine.RestoreStoredPath(context.Background(), engine.RestoreStoredPathRequest{
		StoredPath:      storedPath,
		DestinationMode: engine.RestoreDestinationPrefix,
		DestinationRoot: prefixRoot,
		Overwrite:       true,
	})
	if err != nil {
		t.Fatalf("expected UNC prefix restore to succeed, got: %v", err)
	}

	expectedPath := filepath.Join(prefixRoot, "data", "file.bin")
	assertRestoreStoredPathResult(t, result, storedPath, fixture.stored.FileID, engine.RestoreDestinationPrefix, expectedPath, fixture.stored.FileHash)
	requireFileBytes(t, result.DestinationPath, fixture.payload)
	requireNoRestoreTempFiles(t, prefixRoot)
	after := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)
	assertRestoreCatalogStateEqual(t, before, after)
	requirePinnedChunksReleased(t, fixture.db, fixture.stored.FileID)
}

func TestRestoreStoredPathPrefixRejectsMalformedOrCorruptWindowsStoredPaths(t *testing.T) {
	cases := []string{
		`\\server`,
		`\\server\`,
		`\root-relative\file.bin`,
		`\data\file.bin`,
		`C:drive-relative\file.bin`,
		`relative\file.bin`,
		"relative/file.bin",
		`..\escape.bin`,
	}
	if runtime.GOOS == "windows" {
		cases = append(cases, `//server/share/data/file.bin`, `/data/file.bin`)
	}

	for _, storedPath := range cases {
		t.Run(strings.ReplaceAll(strings.ReplaceAll(storedPath, "\\", "_"), "/", "_"), func(t *testing.T) {
			fixture := newStoredPathRestoreFixture(t, "restore-stored-path-prefix-corrupt-windows")
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
				t.Fatalf("expected corrupt Windows stored path restore to fail, got result=%+v", result)
			}
			if result != (engine.RestoreStoredPathResult{}) {
				t.Fatalf("expected zero result on corrupt Windows stored path failure, got %+v", result)
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
	if runtime.GOOS == "windows" {
		storedPath = `C:\tmp\parent\escape.bin`
	}
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
	after := snapshotRestoreCatalogState(t, fixture.db, fixture.stored.FileID)
	assertRestoreCatalogStateEqual(t, before, after)
	requirePinnedChunksReleased(t, fixture.db, fixture.stored.FileID)
}
