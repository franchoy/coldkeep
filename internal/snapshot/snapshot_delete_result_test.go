package snapshot

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestDeleteSnapshotWithResultReportsCommittedDeletion(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-delete-result-a", 5)
	parent := Snapshot{ID: "snap-delete-result-parent", CreatedAt: time.Now().UTC(), Type: "full"}
	child := Snapshot{ID: "snap-delete-result-child", CreatedAt: time.Now().UTC().Add(time.Second), Type: "full", ParentID: sqlNullString(parent.ID)}
	for _, item := range []Snapshot{parent, child} {
		if err := InsertSnapshot(ctx, db, item); err != nil {
			t.Fatalf("InsertSnapshot %s: %v", item.ID, err)
		}
	}
	insertSnapshotFileRow(t, db, parent.ID, "docs/a.txt", logicalA, sqlNullInt64(), sqlNullTime())

	result, err := DeleteSnapshotWithResult(ctx, db, parent.ID)
	if err != nil {
		t.Fatalf("DeleteSnapshotWithResult: %v", err)
	}
	if result != (DeleteSnapshotResult{SnapshotID: parent.ID, Deleted: true}) {
		t.Fatalf("unexpected delete result: %+v", result)
	}
	if snapshotRowCount(t, db, parent.ID) != 0 {
		t.Fatal("expected deleted snapshot row to be gone")
	}
	if snapshotFileRowCount(t, db, parent.ID) != 0 {
		t.Fatal("expected deleted snapshot_file rows to be gone")
	}
	if parentID := snapshotParentValue(t, db, child.ID); parentID.Valid {
		t.Fatalf("expected child parent_id to be NULL after parent delete, got %+v", parentID)
	}
}

func TestDeleteSnapshotWithResultRollsBackToZeroResult(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-delete-trigger-a", 5)
	snap := Snapshot{ID: "snap-delete-trigger", CreatedAt: time.Now().UTC(), Type: "full"}
	if err := InsertSnapshot(ctx, db, snap); err != nil {
		t.Fatalf("InsertSnapshot: %v", err)
	}
	insertSnapshotFileRow(t, db, snap.ID, "docs/a.txt", logicalA, sqlNullInt64(), sqlNullTime())

	if _, err := db.Exec(`
		CREATE TRIGGER snapshot_delete_abort
		BEFORE DELETE ON snapshot
		FOR EACH ROW
		BEGIN
			SELECT RAISE(ABORT, 'delete blocked');
		END;
	`); err != nil {
		t.Fatalf("create trigger: %v", err)
	}

	result, err := DeleteSnapshotWithResult(ctx, db, snap.ID)
	if err == nil || !strings.Contains(err.Error(), "delete blocked") {
		t.Fatalf("expected trigger failure, got result=%+v err=%v", result, err)
	}
	if result != (DeleteSnapshotResult{}) {
		t.Fatalf("expected zero result on rollback, got %+v", result)
	}
	if snapshotRowCount(t, db, snap.ID) != 1 {
		t.Fatal("expected snapshot row to remain after rollback")
	}
	if snapshotFileRowCount(t, db, snap.ID) != 1 {
		t.Fatal("expected snapshot_file rows to remain after rollback")
	}
}

func TestDeleteSnapshotWrapperRemainsCompatible(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	logicalA := insertLogicalFileWithSize(t, db, "hash-delete-wrapper-a", 5)
	snap := Snapshot{ID: "snap-delete-wrapper", CreatedAt: time.Now().UTC(), Type: "full"}
	if err := InsertSnapshot(ctx, db, snap); err != nil {
		t.Fatalf("InsertSnapshot: %v", err)
	}
	insertSnapshotFileRow(t, db, snap.ID, "docs/a.txt", logicalA, sqlNullInt64(), sqlNullTime())

	if err := DeleteSnapshot(ctx, db, snap.ID); err != nil {
		t.Fatalf("DeleteSnapshot wrapper: %v", err)
	}
	if snapshotRowCount(t, db, snap.ID) != 0 {
		t.Fatal("expected wrapper to remove snapshot row")
	}
}
