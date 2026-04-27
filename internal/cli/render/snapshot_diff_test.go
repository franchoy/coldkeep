package render

import (
	"bytes"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/snapshot"
)

func TestRenderSnapshotDiffSummaryHumanLayout(t *testing.T) {
	var buf bytes.Buffer
	err := RenderSnapshotDiffSummaryHuman(&buf, "day1", "day2", snapshot.SnapshotDiffSummary{Added: 1, Removed: 2, Modified: 3})
	if err != nil {
		t.Fatalf("RenderSnapshotDiffSummaryHuman: %v", err)
	}
	out := buf.String()
	for _, want := range []string{
		"Snapshot diff: day1 -> day2",
		"Added:     1 files",
		"Removed:   2 files",
		"Modified:  3 files",
		"Total changes: 6",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestRenderSnapshotDiffDetailedHumanLayout(t *testing.T) {
	entries := []snapshot.SnapshotDiffEntry{
		{Path: "docs/new.txt", Type: snapshot.DiffAdded},
		{Path: "docs/old.txt", Type: snapshot.DiffRemoved},
		{Path: "docs/config.yaml", Type: snapshot.DiffModified},
	}

	var buf bytes.Buffer
	err := RenderSnapshotDiffDetailedHuman(
		&buf,
		"day1",
		"day2",
		entries,
		snapshot.SnapshotDiffSummary{Added: 1, Removed: 1, Modified: 1},
		3,
		3,
		12,
		"run doctor",
	)
	if err != nil {
		t.Fatalf("RenderSnapshotDiffDetailedHuman: %v", err)
	}
	out := buf.String()
	for _, want := range []string{
		"[SNAPSHOT DIFF]",
		"Base:    day1",
		"Target:  day2",
		"+ docs/new.txt",
		"- docs/old.txt",
		"~ docs/config.yaml",
		"entries (matched): 3",
		"entries (total): 3",
		"added: 1",
		"removed: 1",
		"modified: 1",
		"Duration: 12ms",
		"Hint: run doctor",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestRenderSnapshotDiffDetailedHumanNoChanges(t *testing.T) {
	var buf bytes.Buffer
	err := RenderSnapshotDiffDetailedHuman(
		&buf,
		"base",
		"target",
		nil,
		snapshot.SnapshotDiffSummary{},
		0,
		0,
		3,
		"hint",
	)
	if err != nil {
		t.Fatalf("RenderSnapshotDiffDetailedHuman: %v", err)
	}
	if !strings.Contains(buf.String(), "(no changes)") {
		t.Fatalf("expected no-changes marker, got:\n%s", buf.String())
	}
}
