//go:build windows

package snapshot

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/storage"
	"golang.org/x/sys/windows"
)

func windowsShortPath(path string) (string, error) {
	longPath, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return "", err
	}
	buffer := make([]uint16, windows.MAX_PATH)
	n, err := windows.GetShortPathName(longPath, &buffer[0], uint32(len(buffer)))
	if err != nil {
		return "", err
	}
	if n >= uint32(len(buffer)) {
		buffer = make([]uint16, n+1)
		n, err = windows.GetShortPathName(longPath, &buffer[0], uint32(len(buffer)))
		if err != nil {
			return "", err
		}
	}
	return windows.UTF16ToString(buffer[:n]), nil
}

func TestRestoreSnapshotCreateUsesCanonicalWindowsSourceAndNativeSelectionBases(t *testing.T) {
	db := openTestDB(t)
	containersDir := t.TempDir()
	sgctx := storage.StorageContext{
		DB:           db,
		Writer:       container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), db),
		ContainerDir: containersDir,
	}

	rawSelectionBase := t.TempDir()
	stored := storeSnapshotCaptureRootFixtureFile(t, db, sgctx, rawSelectionBase, "docs/native-windows.txt", []byte("native windows snapshot path"))
	canonicalSource, err := filepath.EvalSymlinks(filepath.Join(rawSelectionBase, "docs", "native-windows.txt"))
	if err != nil {
		t.Fatalf("resolve native source: %v", err)
	}
	canonicalSource = filepath.Clean(canonicalSource)

	var recordedSource string
	if err := db.QueryRow(`SELECT path FROM physical_file WHERE logical_file_id = ?`, stored.FileID).Scan(&recordedSource); err != nil {
		t.Fatalf("query stored physical source: %v", err)
	}
	if recordedSource != canonicalSource || stored.Path != canonicalSource {
		t.Fatalf("store did not retain canonical native source: result=%q recorded=%q want=%q", stored.Path, recordedSource, canonicalSource)
	}

	const snapshotID = "snap-native-windows-temp-base"
	if err := CreateSnapshotWithOptions(context.Background(), db, SnapshotCreateOptions{ID: snapshotID, Type: "full", SelectionBase: rawSelectionBase}); err != nil {
		t.Fatalf("create snapshot from native Windows selection base: %v", err)
	}
	files, err := ListSnapshotFiles(context.Background(), db, snapshotID, 0, nil)
	if err != nil {
		t.Fatalf("list snapshot members: %v", err)
	}
	if len(files) != 1 || files[0].Path != "docs/native-windows.txt" {
		t.Fatalf("unexpected native Windows snapshot members: %+v", files)
	}

	if shortBase, shortErr := windowsShortPath(rawSelectionBase); shortErr != nil || strings.EqualFold(shortBase, rawSelectionBase) {
		t.Logf("Windows short-path subproof unavailable: short=%q err=%v", shortBase, shortErr)
	} else if err := CreateSnapshotWithOptions(context.Background(), db, SnapshotCreateOptions{ID: "snap-native-windows-short-base", Type: "full", SelectionBase: shortBase}); err != nil {
		t.Fatalf("create snapshot from available Windows short-path selection base %q: %v", shortBase, err)
	} else {
		t.Logf("proved Windows short-path selection-base alias: long=%q short=%q", rawSelectionBase, shortBase)
	}

	baseVolume := strings.ToUpper(filepath.VolumeName(rawSelectionBase))
	otherVolume := `Z:`
	if strings.EqualFold(baseVolume, otherVolume) {
		otherVolume = `Y:`
	}
	outside := otherVolume + `\coldkeep-outside\member.txt`
	contained, containErr := snapshotPhysicalPathContained(rawSelectionBase, outside)
	if containErr == nil && contained {
		t.Fatalf("different-volume path was incorrectly contained: base=%q outside=%q", rawSelectionBase, outside)
	}
	if _, memberContained, memberErr := snapshotMemberPath(rawSelectionBase, outside); memberErr == nil && memberContained {
		t.Fatalf("different-volume path unexpectedly became a snapshot member: base=%q outside=%q", rawSelectionBase, outside)
	}
}
