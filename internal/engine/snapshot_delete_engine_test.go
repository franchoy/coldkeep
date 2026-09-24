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
		snapshotDeletePreviewExpectation{
			SnapshotID: "snap-delete-target",
			Mode:       SnapshotDeleteModePreview,
			Deleted:    false,
			Parent: snapshotDeleteParentExpectation{
				ID:    "snap-delete-parent",
				State: SnapshotDeleteParentPresent,
			},
			Children:    []string{"snap-delete-child"},
			TotalFiles:  3,
			UniqueFiles: 0,
			SharedFiles: 3,
		},
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
	selectionBase := seedSnapshotCreateEngineFiles(t, dbconn)
	eng := newSnapshotCreateEngine(t, dbconn)

	mustCreateSnapshotResult(t, dbconn, snapshot.SnapshotCreateOptions{
		ID:            "snap-delete-exec-parent",
		Type:          "full",
		SelectionBase: selectionBase,
	})
	mustCreateSnapshotResult(t, dbconn, snapshot.SnapshotCreateOptions{
		ID:            "snap-delete-exec-target",
		Type:          "full",
		ParentID:      stringPtr("snap-delete-exec-parent"),
		SelectionBase: selectionBase,
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
	selectionBase := seedSnapshotCreateEngineFiles(t, dbconn)
	eng := newSnapshotCreateEngine(t, dbconn)

	mustCreateSnapshotResult(t, dbconn, snapshot.SnapshotCreateOptions{
		ID:            "snap-delete-parent",
		Type:          "full",
		SelectionBase: selectionBase,
	})
	mustCreateSnapshotResult(t, dbconn, snapshot.SnapshotCreateOptions{
		ID:            "snap-delete-target",
		Type:          "full",
		ParentID:      stringPtr("snap-delete-parent"),
		SelectionBase: selectionBase,
	})
	mustCreateSnapshotResult(t, dbconn, snapshot.SnapshotCreateOptions{
		ID:            "snap-delete-child",
		Type:          "full",
		ParentID:      stringPtr("snap-delete-target"),
		SelectionBase: selectionBase,
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

type snapshotDeleteParentExpectation struct {
	ID    string
	State SnapshotDeleteParentState
}

type snapshotDeletePreviewExpectation struct {
	SnapshotID  string
	Mode        SnapshotDeleteMode
	Deleted     bool
	Parent      snapshotDeleteParentExpectation
	Children    []string
	TotalFiles  int64
	UniqueFiles int64
	SharedFiles int64
}

func assertSnapshotDeletePreviewResult(
	t *testing.T,
	result SnapshotDeleteResult,
	want snapshotDeletePreviewExpectation,
) {
	t.Helper()

	if result.Preview == nil {
		t.Fatal("expected preview payload")
	}
	assertSnapshotDeletePreviewEnvelope(t, result, want)
	assertSnapshotDeletePreviewParent(t, result.Preview.Parent, want.Parent)
	assertSnapshotDeletePreviewChildren(t, result.Preview.Children, want.Children)
	assertSnapshotDeletePreviewCounts(t, result.Preview, want)
}

func assertSnapshotDeletePreviewEnvelope(t *testing.T, got SnapshotDeleteResult, want snapshotDeletePreviewExpectation) {
	t.Helper()

	if got.SnapshotID != want.SnapshotID || got.Mode != want.Mode || got.Deleted != want.Deleted {
		t.Fatalf("unexpected preview result header: %+v", got)
	}
}

func assertSnapshotDeletePreviewParent(t *testing.T, got SnapshotDeleteParent, want snapshotDeleteParentExpectation) {
	t.Helper()

	if got.ID != want.ID || got.State != want.State {
		t.Fatalf("unexpected parent preview: %+v", got)
	}
}

func assertSnapshotDeletePreviewChildren(t *testing.T, got []string, want []string) {
	t.Helper()

	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("unexpected children preview: %+v", got)
	}
}

func assertSnapshotDeletePreviewCounts(t *testing.T, got *SnapshotDeletePreviewResult, want snapshotDeletePreviewExpectation) {
	t.Helper()

	if got.TotalFiles != want.TotalFiles || got.UniqueFiles != want.UniqueFiles || got.SharedFiles != want.SharedFiles {
		t.Fatalf("unexpected preview counts: %+v", got)
	}
	if got.SharedFiles != got.TotalFiles-got.UniqueFiles {
		t.Fatalf("unexpected preview count invariant: %+v", got)
	}
}
