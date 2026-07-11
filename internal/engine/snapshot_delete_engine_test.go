package engine

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/snapshot"
)

func TestSnapshotDeletePreviewRoutesThroughEngine(t *testing.T) {
	dbconn, eng := setupSnapshotDeletePreviewEngineCase(t)
	result := runSnapshotDeletePreview(t, eng, " snap-delete-target ")
	assertSnapshotDeletePreviewReadOnly(t, dbconn, "snap-delete-target", "snap-delete-child")
	assertSnapshotDeletePreviewResult(
		t,
		result,
		SnapshotDeleteParent{ID: "snap-delete-parent", State: SnapshotDeleteParentPresent},
		[]string{"snap-delete-child"},
		3,
		0,
		3,
	)
}

func TestSnapshotDeletePreviewReportsMissingParentCompatibilityState(t *testing.T) {
	dbconn := openSnapshotCreateEngineDB(t)
	eng := newSnapshotCreateEngine(t, dbconn)

	insertEngineSnapshotRow(t, dbconn, "snap-delete-missing-parent", "full", "")
	if _, err := dbconn.Exec(`PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("disable foreign keys: %v", err)
	}
	if _, err := dbconn.Exec(`UPDATE snapshot SET parent_id = ? WHERE id = ?`, "snap-gone", "snap-delete-missing-parent"); err != nil {
		t.Fatalf("inject missing parent metadata: %v", err)
	}
	if _, err := dbconn.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		t.Fatalf("enable foreign keys: %v", err)
	}

	result, err := eng.SnapshotDelete(context.Background(), SnapshotDeleteRequest{
		SnapshotID: "snap-delete-missing-parent",
		Mode:       SnapshotDeleteModePreview,
	})
	if err != nil {
		t.Fatalf("SnapshotDelete preview missing-parent: %v", err)
	}
	if result.Preview == nil {
		t.Fatal("expected preview payload")
	}
	if result.Preview.Parent != (SnapshotDeleteParent{ID: "snap-gone", State: SnapshotDeleteParentMissing}) {
		t.Fatalf("unexpected missing-parent preview: %+v", result.Preview.Parent)
	}
}

func TestSnapshotDeleteExecuteRoutesThroughEngine(t *testing.T) {
	dbconn := openSnapshotCreateEngineDB(t)
	seedSnapshotCreateEngineFiles(t, dbconn)
	eng := newSnapshotCreateEngine(t, dbconn)

	mustCreateSnapshotResult(t, dbconn, snapshot.SnapshotCreateOptions{
		ID:   "snap-delete-exec-parent",
		Type: "full",
	})
	mustCreateSnapshotResult(t, dbconn, snapshot.SnapshotCreateOptions{
		ID:       "snap-delete-exec-target",
		Type:     "full",
		ParentID: stringPtr("snap-delete-exec-parent"),
	})
	insertEngineSnapshotRow(t, dbconn, "snap-delete-exec-child", "full", "snap-delete-exec-target")

	result, err := eng.SnapshotDelete(context.Background(), SnapshotDeleteRequest{
		SnapshotID: " snap-delete-exec-target ",
		Mode:       SnapshotDeleteModeExecute,
	})
	if err != nil {
		t.Fatalf("SnapshotDelete execute: %v", err)
	}
	if result != (SnapshotDeleteResult{
		SnapshotID: "snap-delete-exec-target",
		Mode:       SnapshotDeleteModeExecute,
		Deleted:    true,
	}) {
		t.Fatalf("unexpected execute result: %+v", result)
	}
	if snapshotExists(t, dbconn, "snap-delete-exec-target") {
		t.Fatal("expected deleted snapshot row to be removed")
	}
	childParent := snapshotParentID(t, dbconn, "snap-delete-exec-child")
	if childParent.Valid {
		t.Fatalf("expected child parent_id to be nulled, got %+v", childParent)
	}
}

func TestSnapshotDeleteRejectsInvalidRequests(t *testing.T) {
	dbconn := openSnapshotCreateEngineDB(t)
	eng := newSnapshotCreateEngine(t, dbconn)

	tests := []struct {
		name    string
		req     SnapshotDeleteRequest
		wantErr string
	}{
		{
			name:    "blank snapshot id",
			req:     SnapshotDeleteRequest{SnapshotID: "   ", Mode: SnapshotDeleteModePreview},
			wantErr: "snapshot id cannot be empty",
		},
		{
			name:    "missing mode",
			req:     SnapshotDeleteRequest{SnapshotID: "snap-delete"},
			wantErr: "snapshot delete mode is required",
		},
		{
			name:    "unknown mode",
			req:     SnapshotDeleteRequest{SnapshotID: "snap-delete", Mode: SnapshotDeleteMode("boom")},
			wantErr: `unknown snapshot delete mode "boom"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := eng.SnapshotDelete(context.Background(), tc.req)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}

func insertEngineSnapshotRow(t *testing.T, dbconn *sql.DB, snapshotID, snapshotType, parentID string) {
	t.Helper()

	var parent any
	if parentID != "" {
		parent = parentID
	}
	_, err := dbconn.Exec(
		`INSERT INTO snapshot (id, created_at, type, parent_id) VALUES (?, ?, ?, ?)`,
		snapshotID, time.Now().UTC().Format(time.RFC3339), snapshotType, parent,
	)
	if err != nil {
		t.Fatalf("insert snapshot %s: %v", snapshotID, err)
	}
}

func mustCreateSnapshotResult(t *testing.T, dbconn *sql.DB, opts snapshot.SnapshotCreateOptions) {
	t.Helper()

	if _, err := snapshot.CreateSnapshotWithOptionsResult(context.Background(), dbconn, opts); err != nil {
		t.Fatalf("CreateSnapshotWithOptionsResult %+v: %v", opts, err)
	}
}

func stringPtr(v string) *string {
	return &v
}

func setupSnapshotDeletePreviewEngineCase(t *testing.T) (*sql.DB, *DefaultEngine) {
	t.Helper()

	dbconn := openSnapshotCreateEngineDB(t)
	seedSnapshotCreateEngineFiles(t, dbconn)
	eng := newSnapshotCreateEngine(t, dbconn)

	mustCreateSnapshotResult(t, dbconn, snapshot.SnapshotCreateOptions{
		ID:   "snap-delete-parent",
		Type: "full",
	})
	mustCreateSnapshotResult(t, dbconn, snapshot.SnapshotCreateOptions{
		ID:       "snap-delete-target",
		Type:     "full",
		ParentID: stringPtr("snap-delete-parent"),
	})
	mustCreateSnapshotResult(t, dbconn, snapshot.SnapshotCreateOptions{
		ID:       "snap-delete-child",
		Type:     "full",
		ParentID: stringPtr("snap-delete-target"),
	})

	return dbconn, eng
}

func runSnapshotDeletePreview(t *testing.T, eng *DefaultEngine, snapshotID string) SnapshotDeleteResult {
	t.Helper()

	result, err := eng.SnapshotDelete(context.Background(), SnapshotDeleteRequest{
		SnapshotID: snapshotID,
		Mode:       SnapshotDeleteModePreview,
	})
	if err != nil {
		t.Fatalf("SnapshotDelete preview: %v", err)
	}
	return result
}

func assertSnapshotDeletePreviewReadOnly(t *testing.T, dbconn *sql.DB, snapshotID, childSnapshotID string) {
	t.Helper()

	if !snapshotExists(t, dbconn, snapshotID) {
		t.Fatalf("expected preview to keep snapshot %q", snapshotID)
	}
	childParent := snapshotParentID(t, dbconn, childSnapshotID)
	if !childParent.Valid || childParent.String != snapshotID {
		t.Fatalf("expected preview to preserve child parent_id=%q, got %+v", snapshotID, childParent)
	}
}

func assertSnapshotDeletePreviewResult(
	t *testing.T,
	result SnapshotDeleteResult,
	wantParent SnapshotDeleteParent,
	wantChildren []string,
	wantTotalFiles int64,
	wantUniqueFiles int64,
	wantSharedFiles int64,
) {
	t.Helper()

	if result.SnapshotID != "snap-delete-target" || result.Mode != SnapshotDeleteModePreview || result.Deleted {
		t.Fatalf("unexpected preview result header: %+v", result)
	}
	if result.Preview == nil {
		t.Fatal("expected preview payload")
	}
	if result.Preview.Parent != wantParent {
		t.Fatalf("unexpected parent preview: %+v", result.Preview.Parent)
	}
	if strings.Join(result.Preview.Children, "|") != strings.Join(wantChildren, "|") {
		t.Fatalf("unexpected children preview: %+v", result.Preview.Children)
	}
	if result.Preview.TotalFiles != wantTotalFiles || result.Preview.UniqueFiles != wantUniqueFiles || result.Preview.SharedFiles != wantSharedFiles {
		t.Fatalf("unexpected preview counts: %+v", result.Preview)
	}
}
