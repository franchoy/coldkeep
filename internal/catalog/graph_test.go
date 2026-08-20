package catalog

import (
	"reflect"
	"testing"
	"time"
)

func TestBuildSnapshotGraphOrdersRelationsWithoutMutatingInput(t *testing.T) {
	base := time.Date(2026, 8, 19, 10, 0, 0, 0, time.UTC)
	input := []SnapshotRef{
		{ID: "child-b", ParentID: "root", CreatedAt: base.Add(time.Hour)},
		{ID: "root", CreatedAt: base},
		{ID: "child-a", ParentID: "root", CreatedAt: base.Add(time.Hour)},
	}
	original := append([]SnapshotRef(nil), input...)
	graph, err := buildSnapshotGraph(input)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(input, original) {
		t.Fatalf("input mutated: got %+v want %+v", input, original)
	}
	if got, want := graph.RootIDs, []string{"root"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("roots=%v want=%v", got, want)
	}
	if got, want := graph.Nodes[0].ChildIDs, []string{"child-a", "child-b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("children=%v want=%v", got, want)
	}
}

func TestBuildSnapshotGraphClassifiesMissingParentWithoutInventingEdge(t *testing.T) {
	graph, err := buildSnapshotGraph([]SnapshotRef{{ID: "orphan", ParentID: "historical-parent", CreatedAt: time.Now().UTC()}})
	if err != nil {
		t.Fatal(err)
	}
	if len(graph.RootIDs) != 0 {
		t.Fatalf("missing-parent row became a true root: %v", graph.RootIDs)
	}
	if len(graph.Nodes) != 1 || graph.Nodes[0].ParentState != SnapshotParentMissing || len(graph.Nodes[0].ChildIDs) != 0 {
		t.Fatalf("missing-parent classification: %+v", graph)
	}
}

func TestBuildSnapshotGraphRejectsCyclesAndDuplicateIDs(t *testing.T) {
	base := time.Now().UTC()
	for name, refs := range map[string][]SnapshotRef{
		"self cycle":     {{ID: "a", ParentID: "a", CreatedAt: base}},
		"two node cycle": {{ID: "a", ParentID: "b", CreatedAt: base}, {ID: "b", ParentID: "a", CreatedAt: base}},
		"duplicate":      {{ID: "a", CreatedAt: base}, {ID: "a", CreatedAt: base}},
	} {
		t.Run(name, func(t *testing.T) {
			graph, err := buildSnapshotGraph(refs)
			if graph != nil || !IsCode(err, ErrorInvariantViolation) {
				t.Fatalf("graph=%+v err=%v", graph, err)
			}
		})
	}
}

func TestCatalogTimestampPreservesBackendRepresentations(t *testing.T) {
	want := time.Date(2026, 8, 19, 12, 34, 56, 123456000, time.UTC)
	for _, input := range []any{want, want.Format(time.RFC3339Nano), []byte(want.Format(time.RFC3339Nano))} {
		got, err := catalogTimestamp(input)
		if err != nil || !got.Equal(want) {
			t.Fatalf("input=%T(%v) got=%v err=%v", input, input, got, err)
		}
	}
	if _, err := catalogTimestamp("not-a-time"); err == nil {
		t.Fatal("expected malformed timestamp error")
	}
}
