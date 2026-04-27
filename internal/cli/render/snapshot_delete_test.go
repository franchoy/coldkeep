package render

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/snapshot"
)

func TestSnapshotDeleteWarningsWithChildren(t *testing.T) {
	preview := &snapshot.DeleteLineagePreview{ChildSnapshotIDs: []string{"child-a", "child-b"}}
	warnings := SnapshotDeleteWarnings(preview)
	if len(warnings) != 1 {
		t.Fatalf("expected one warning, got %d", len(warnings))
	}
	warning := warnings[0]
	if got, _ := warning["type"].(string); got != "lineage_breakage" {
		t.Fatalf("warning type mismatch: got %v", warning["type"])
	}
}

func TestSnapshotDeleteWarningsWithoutChildren(t *testing.T) {
	if warnings := SnapshotDeleteWarnings(&snapshot.DeleteLineagePreview{}); warnings != nil {
		t.Fatalf("expected nil warnings for no children, got %v", warnings)
	}
	if warnings := SnapshotDeleteWarnings(nil); warnings != nil {
		t.Fatalf("expected nil warnings for nil preview, got %v", warnings)
	}
}

func TestFormatSnapshotDeleteDryRunOutputIncludesMissingParentAndImpact(t *testing.T) {
	preview := &snapshot.DeleteLineagePreview{
		SnapshotID:       "child",
		ParentID:         sql.NullString{String: "ghost", Valid: true},
		ParentMissing:    true,
		ChildSnapshotIDs: []string{"leaf"},
		TotalFiles:       1200,
		UniqueFiles:      300,
		SharedFiles:      900,
	}

	output := FormatSnapshotDeleteDryRunOutput("child", preview)
	for _, want := range []string{
		"Snapshot: child",
		"Total:        1,200",
		"Unique:       300",
		"Shared:       900",
		"Parent: (missing)",
		"Parent note: parent snapshot metadata is missing",
		"Children:",
		"- leaf",
		"Warning:",
		"remove 300 unique snapshot file reference(s) from metadata",
		"Dry run: no changes applied.",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, output)
		}
	}
}
