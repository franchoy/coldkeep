package snapshot

import (
	"context"
	"strings"
	"testing"
)

func TestLoadDeleteLineagePreviewComputesLineageAndSharing(t *testing.T) {
	db := openTestDB(t)

	if _, err := db.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES
		('s1', CURRENT_TIMESTAMP, 'full'),
		('s2', CURRENT_TIMESTAMP, 'full')`); err != nil {
		t.Fatalf("seed snapshots: %v", err)
	}
	if _, err := db.Exec(`UPDATE snapshot SET parent_id = 's1' WHERE id = 's2'`); err != nil {
		t.Fatalf("set parent: %v", err)
	}

	logicalA := insertLogicalFile(t, db, "hash-a")
	logicalB := insertLogicalFile(t, db, "hash-b")

	res, err := db.Exec(`INSERT INTO snapshot_path (path) VALUES ('a.txt')`)
	if err != nil {
		t.Fatalf("insert snapshot_path a: %v", err)
	}
	pathA, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("snapshot_path a id: %v", err)
	}
	res, err = db.Exec(`INSERT INTO snapshot_path (path) VALUES ('b.txt')`)
	if err != nil {
		t.Fatalf("insert snapshot_path b: %v", err)
	}
	pathB, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("snapshot_path b id: %v", err)
	}

	if _, err := db.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES (?, ?, ?)`, "s1", pathA, logicalA); err != nil {
		t.Fatalf("insert s1/a: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES (?, ?, ?)`, "s1", pathB, logicalB); err != nil {
		t.Fatalf("insert s1/b: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES (?, ?, ?)`, "s2", pathA, logicalA); err != nil {
		t.Fatalf("insert s2/a shared: %v", err)
	}

	preview, err := LoadDeleteLineagePreview(context.Background(), db, "s1")
	if err != nil {
		t.Fatalf("LoadDeleteLineagePreview: %v", err)
	}
	if preview.SnapshotID != "s1" {
		t.Fatalf("snapshot id mismatch: got %q", preview.SnapshotID)
	}
	if len(preview.ChildSnapshotIDs) != 1 || preview.ChildSnapshotIDs[0] != "s2" {
		t.Fatalf("children mismatch: got %v", preview.ChildSnapshotIDs)
	}
	if preview.TotalFiles != 2 {
		t.Fatalf("total files mismatch: got %d", preview.TotalFiles)
	}
	if preview.UniqueFiles != 1 {
		t.Fatalf("unique files mismatch: got %d", preview.UniqueFiles)
	}
	if preview.SharedFiles != 1 {
		t.Fatalf("shared files mismatch: got %d", preview.SharedFiles)
	}
}

func TestLoadDeleteLineagePreviewNotFound(t *testing.T) {
	db := openTestDB(t)

	_, err := LoadDeleteLineagePreview(context.Background(), db, "missing")
	if err == nil {
		t.Fatal("expected error")
	}
	if got := err.Error(); got != `snapshot "missing" not found` {
		t.Fatalf("expected exact error %q, got %q", `snapshot "missing" not found`, got)
	}
}

func TestLoadDeleteLineagePreviewRejectsEmptySnapshotID(t *testing.T) {
	db := openTestDB(t)

	_, err := LoadDeleteLineagePreview(context.Background(), db, "   ")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "snapshot id cannot be empty") {
		t.Fatalf("expected empty-id error, got %v", err)
	}
}
