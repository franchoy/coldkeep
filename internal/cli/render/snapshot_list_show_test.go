package render

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/snapshot"
)

func TestRenderSnapshotListHumanFlatAndTree(t *testing.T) {
	now := time.Date(2026, 4, 27, 10, 0, 0, 0, time.UTC)
	items := []snapshot.Snapshot{{ID: "root", Type: "full", CreatedAt: now, Label: sql.NullString{String: "release", Valid: true}}}

	var flat strings.Builder
	if err := RenderSnapshotListHuman(&flat, items, false, nil, 5, "hint"); err != nil {
		t.Fatalf("RenderSnapshotListHuman flat: %v", err)
	}
	for _, want := range []string{"Snapshots:", "root  full  2026-04-27T10:00:00Z", "label=release", "Duration: 5ms", "Hint: hint"} {
		if !strings.Contains(flat.String(), want) {
			t.Fatalf("expected flat output to contain %q, got:\n%s", want, flat.String())
		}
	}

	var tree strings.Builder
	if err := RenderSnapshotListHuman(&tree, items, true, []string{"root", "└── child"}, 7, "hint"); err != nil {
		t.Fatalf("RenderSnapshotListHuman tree: %v", err)
	}
	if !strings.Contains(tree.String(), "└── child") {
		t.Fatalf("expected tree output, got:\n%s", tree.String())
	}
}

func TestRenderSnapshotShowHumanIncludesCountsAndFiles(t *testing.T) {
	item := snapshot.Snapshot{ID: "snap-1", Type: "partial", CreatedAt: time.Date(2026, 4, 27, 11, 0, 0, 0, time.UTC)}
	files := []snapshot.SnapshotFileEntry{{Path: "docs/a.txt"}, {Path: "docs/b.txt"}}

	var out strings.Builder
	if err := RenderSnapshotShowHuman(&out, item, files, 2, 3, 9, "hint"); err != nil {
		t.Fatalf("RenderSnapshotShowHuman: %v", err)
	}
	for _, want := range []string{"Snapshot: snap-1", "Files (matched): 2", "Files (total): 3", "docs/a.txt", "docs/b.txt", "Duration: 9ms", "Hint: hint"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("expected show output to contain %q, got:\n%s", want, out.String())
		}
	}
}
