//go:build darwin

package snapshot

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/storage"
)

func TestRestoreSnapshotCreateUsesCanonicalDarwinSourceWithRawTempSelectionBase(t *testing.T) {
	db := openTestDB(t)
	containersDir := t.TempDir()
	sgctx := storage.StorageContext{
		DB:           db,
		Writer:       container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), db),
		ContainerDir: containersDir,
	}

	rawSelectionBase := t.TempDir()
	stored := storeSnapshotCaptureRootFixtureFile(t, db, sgctx, rawSelectionBase, "docs/native-darwin.txt", []byte("native darwin snapshot path"))
	canonicalSource, err := filepath.EvalSymlinks(filepath.Join(rawSelectionBase, "docs", "native-darwin.txt"))
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

	const snapshotID = "snap-native-darwin-temp-base"
	if err := CreateSnapshotWithOptions(context.Background(), db, SnapshotCreateOptions{ID: snapshotID, Type: "full", SelectionBase: rawSelectionBase}); err != nil {
		t.Fatalf("create snapshot from raw Darwin temp selection base: %v", err)
	}
	files, err := ListSnapshotFiles(context.Background(), db, snapshotID, 0, nil)
	if err != nil {
		t.Fatalf("list snapshot members: %v", err)
	}
	if len(files) != 1 || files[0].Path != "docs/native-darwin.txt" {
		t.Fatalf("unexpected native Darwin snapshot members: %+v", files)
	}

	resolvedBase, err := filepath.EvalSymlinks(rawSelectionBase)
	if err != nil {
		t.Fatalf("resolve raw Darwin selection base: %v", err)
	}
	if rawSelectionBase != filepath.Clean(resolvedBase) {
		t.Logf("proved Darwin outer temp alias: raw=%q canonical=%q", rawSelectionBase, filepath.Clean(resolvedBase))
	} else {
		t.Logf("Darwin temp root has no outer alias in this runner; canonical-source snapshot proof still passed: %q", rawSelectionBase)
	}
	if _, err := os.Stat(canonicalSource); err != nil {
		t.Fatalf("canonical source disappeared during proof: %v", err)
	}
}
