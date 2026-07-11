package snapshot

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

func TestCreateSnapshotWithOptionsResultReportsCommittedCounts(t *testing.T) {
	t.Run("full", func(t *testing.T) {
		db := openTestDB(t)
		ctx := context.Background()
		insertPhysicalFile(t, db, "docs/a.txt", insertLogicalFileWithSize(t, db, "hash-result-full-a", 11), sqlNullInt64(), sqlNullTime())
		insertPhysicalFile(t, db, "docs/b.txt", insertLogicalFileWithSize(t, db, "hash-result-full-b", 22), sqlNullInt64(), sqlNullTime())

		result, err := CreateSnapshotWithOptionsResult(ctx, db, SnapshotCreateOptions{ID: "snap-result-full", Type: "full"})
		if err != nil {
			t.Fatalf("CreateSnapshotWithOptionsResult full: %v", err)
		}
		assertCreateSnapshotResult(t, result, "snap-result-full", "full", 0, 2, "", "")
	})

	t.Run("partial", func(t *testing.T) {
		db := openTestDB(t)
		ctx := context.Background()
		insertPhysicalFile(t, db, "docs/a.txt", insertLogicalFileWithSize(t, db, "hash-result-partial-a", 11), sqlNullInt64(), sqlNullTime())
		insertPhysicalFile(t, db, "img/x.png", insertLogicalFileWithSize(t, db, "hash-result-partial-b", 22), sqlNullInt64(), sqlNullTime())

		result, err := CreateSnapshotWithOptionsResult(ctx, db, SnapshotCreateOptions{
			ID:    "snap-result-partial",
			Type:  "partial",
			Paths: []string{"docs/a.txt", "docs/a.txt", "img/x.png"},
		})
		if err != nil {
			t.Fatalf("CreateSnapshotWithOptionsResult partial: %v", err)
		}
		assertCreateSnapshotResult(t, result, "snap-result-partial", "partial", 3, 2, "", "")
	})
}

func TestCreateSnapshotWithOptionsResultSupportsEmptySnapshots(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	insertPhysicalFile(t, db, "docs/a.txt", insertLogicalFileWithSize(t, db, "hash-result-empty", 11), sqlNullInt64(), sqlNullTime())

	result, err := CreateSnapshotWithOptionsResult(ctx, db, SnapshotCreateOptions{
		ID:    "snap-result-empty",
		Type:  "partial",
		Paths: []string{"missing/"},
	})
	if err != nil {
		t.Fatalf("CreateSnapshotWithOptionsResult empty partial: %v", err)
	}
	assertCreateSnapshotResult(t, result, "snap-result-empty", "partial", 1, 0, "", "")
}

func TestCreateSnapshotWithOptionsResultRollsBackToZeroResult(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	insertPhysicalFile(t, db, "docs/a.txt", insertLogicalFileWithSize(t, db, "hash-result-rollback", 11), sqlNullInt64(), sqlNullTime())

	result, err := CreateSnapshotWithOptionsResult(ctx, db, SnapshotCreateOptions{
		ID:    "snap-result-rollback",
		Type:  "partial",
		Paths: []string{"docs/a.txt", "missing.txt"},
	})
	if err == nil || !strings.Contains(err.Error(), "path not found in current state") {
		t.Fatalf("expected path-not-found rollback, got result=%+v err=%v", result, err)
	}
	if result != (CreateSnapshotResult{}) {
		t.Fatalf("expected zero result on rollback, got %+v", result)
	}
}

func TestCreateSnapshotWithOptionsWrapperRemainsCompatible(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	insertPhysicalFile(t, db, "docs/a.txt", insertLogicalFileWithSize(t, db, "hash-result-wrapper", 11), sqlNullInt64(), sqlNullTime())

	if err := CreateSnapshotWithOptions(ctx, db, SnapshotCreateOptions{ID: "snap-result-wrapper", Type: "full"}); err != nil {
		t.Fatalf("CreateSnapshotWithOptions wrapper: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM snapshot_file WHERE snapshot_id = ?`, "snap-result-wrapper").Scan(&count); err != nil {
		t.Fatalf("count snapshot_file rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected compatibility wrapper to commit one membership row, got %d", count)
	}
}

func assertCreateSnapshotResult(
	t *testing.T,
	result CreateSnapshotResult,
	wantID string,
	wantType string,
	wantPathsCount int,
	wantFilesInserted int,
	wantLabel string,
	wantParentID string,
) {
	t.Helper()

	if result.SnapshotID != wantID || result.Type != wantType {
		t.Fatalf("unexpected identity/type: %+v", result)
	}
	if result.PathsCount != wantPathsCount || result.FilesInserted != wantFilesInserted {
		t.Fatalf("unexpected counts: %+v", result)
	}
	if result.Label != wantLabel || result.ParentID != wantParentID {
		t.Fatalf("unexpected metadata: %+v", result)
	}
}

func sqlNullInt64() sql.NullInt64 { return sql.NullInt64{} }

func sqlNullTime() sql.NullTime { return sql.NullTime{} }

func sqlNullString(v string) sql.NullString { return sql.NullString{String: v, Valid: true} }

func snapshotRowCount(t *testing.T, db *sql.DB, snapshotID string) int {
	t.Helper()

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM snapshot WHERE id = ?`, snapshotID).Scan(&count); err != nil {
		t.Fatalf("count snapshot rows %s: %v", snapshotID, err)
	}
	return count
}

func snapshotFileRowCount(t *testing.T, db *sql.DB, snapshotID string) int {
	t.Helper()

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM snapshot_file WHERE snapshot_id = ?`, snapshotID).Scan(&count); err != nil {
		t.Fatalf("count snapshot_file rows %s: %v", snapshotID, err)
	}
	return count
}

func snapshotParentValue(t *testing.T, db *sql.DB, snapshotID string) sql.NullString {
	t.Helper()

	var parentID sql.NullString
	if err := db.QueryRow(`SELECT parent_id FROM snapshot WHERE id = ?`, snapshotID).Scan(&parentID); err != nil {
		t.Fatalf("query snapshot parent_id %s: %v", snapshotID, err)
	}
	return parentID
}
