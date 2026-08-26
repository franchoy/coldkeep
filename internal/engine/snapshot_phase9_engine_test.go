package engine_test

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/engine"
)

func TestSnapshotDiffLimitAppliesAfterFilteringWithTruthfulCounts(t *testing.T) {
	db := openSnapshotTestDB(t)
	seedPhase9SnapshotDiffFixture(t, db)

	eng, err := engine.New(engine.Config{DB: db})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	result, err := eng.SnapshotDiff(context.Background(), engine.SnapshotDiffRequest{
		BaseID:   "phase9-diff-base",
		TargetID: "phase9-diff-target",
		Filter:   engine.SnapshotDiffAdded,
		Query: engine.SnapshotQuery{
			Prefixes: []string{"docs/"},
			Limit:    1,
		},
	})
	if err != nil {
		t.Fatalf("SnapshotDiff: %v", err)
	}

	if got, want := len(result.Entries), 1; got != want {
		t.Fatalf("limited Entries length = %d, want %d (entries=%+v)", got, want, result.Entries)
	}
	if got, want := result.TotalEntryCount, 6; got != want {
		t.Errorf("TotalEntryCount = %d, want raw total %d", got, want)
	}
	if got, want := result.MatchedEntryCount, 2; got != want {
		t.Errorf("MatchedEntryCount = %d, want pre-limit matched count %d", got, want)
	}
	if got, want := result.Summary, (engine.SnapshotDiffSummary{Added: 2}); got != want {
		t.Errorf("Summary = %+v, want pre-limit summary %+v", got, want)
	}
	if got, want := result.Entries[0].StoredPath, "docs/added-a.txt"; got != want {
		t.Errorf("first limited path = %q, want %q", got, want)
	}
}

func TestSnapshotDiffLimitProjectionMatrix(t *testing.T) {
	db := openSnapshotTestDB(t)
	seedPhase9SnapshotDiffFixture(t, db)
	eng, err := engine.New(engine.Config{DB: db})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	docsQuery := engine.SnapshotQuery{Prefixes: []string{"docs/"}}
	tests := []struct {
		name        string
		filter      engine.SnapshotDiffFilter
		query       engine.SnapshotQuery
		summaryMode bool
		wantMatched int
		wantSummary engine.SnapshotDiffSummary
		wantPaths   []string
	}{
		{
			name: "limit_zero_is_unlimited", wantMatched: 6,
			wantSummary: engine.SnapshotDiffSummary{Added: 3, Removed: 2, Modified: 1},
			wantPaths:   []string{"docs/added-a.txt", "docs/added-c.txt", "docs/modified-b.txt", "docs/removed-a.txt", "misc/added-z.txt", "misc/removed-z.txt"},
		},
		{
			name: "limit_one", query: engine.SnapshotQuery{Limit: 1}, wantMatched: 6,
			wantSummary: engine.SnapshotDiffSummary{Added: 3, Removed: 2, Modified: 1},
			wantPaths:   []string{"docs/added-a.txt"},
		},
		{
			name: "limit_less_than_matched", query: engine.SnapshotQuery{Limit: 2}, wantMatched: 6,
			wantSummary: engine.SnapshotDiffSummary{Added: 3, Removed: 2, Modified: 1},
			wantPaths:   []string{"docs/added-a.txt", "docs/added-c.txt"},
		},
		{
			name: "limit_equals_matched", query: engine.SnapshotQuery{Limit: 6}, wantMatched: 6,
			wantSummary: engine.SnapshotDiffSummary{Added: 3, Removed: 2, Modified: 1},
			wantPaths:   []string{"docs/added-a.txt", "docs/added-c.txt", "docs/modified-b.txt", "docs/removed-a.txt", "misc/added-z.txt", "misc/removed-z.txt"},
		},
		{
			name: "limit_exceeds_matched", query: engine.SnapshotQuery{Limit: 9}, wantMatched: 6,
			wantSummary: engine.SnapshotDiffSummary{Added: 3, Removed: 2, Modified: 1},
			wantPaths:   []string{"docs/added-a.txt", "docs/added-c.txt", "docs/modified-b.txt", "docs/removed-a.txt", "misc/added-z.txt", "misc/removed-z.txt"},
		},
		{
			name: "query_only", query: docsQuery, wantMatched: 4,
			wantSummary: engine.SnapshotDiffSummary{Added: 2, Removed: 1, Modified: 1},
			wantPaths:   []string{"docs/added-a.txt", "docs/added-c.txt", "docs/modified-b.txt", "docs/removed-a.txt"},
		},
		{
			name: "change_filter_only_added", filter: engine.SnapshotDiffAdded, wantMatched: 3,
			wantSummary: engine.SnapshotDiffSummary{Added: 3},
			wantPaths:   []string{"docs/added-a.txt", "docs/added-c.txt", "misc/added-z.txt"},
		},
		{
			name: "change_filter_only_removed", filter: engine.SnapshotDiffRemoved, wantMatched: 2,
			wantSummary: engine.SnapshotDiffSummary{Removed: 2},
			wantPaths:   []string{"docs/removed-a.txt", "misc/removed-z.txt"},
		},
		{
			name: "change_filter_only_modified", filter: engine.SnapshotDiffModified, wantMatched: 1,
			wantSummary: engine.SnapshotDiffSummary{Modified: 1},
			wantPaths:   []string{"docs/modified-b.txt"},
		},
		{
			name: "query_and_filter", filter: engine.SnapshotDiffAdded, query: docsQuery, wantMatched: 2,
			wantSummary: engine.SnapshotDiffSummary{Added: 2},
			wantPaths:   []string{"docs/added-a.txt", "docs/added-c.txt"},
		},
		{
			name: "query_filter_and_limit", filter: engine.SnapshotDiffAdded,
			query: engine.SnapshotQuery{Prefixes: []string{"docs/"}, Limit: 1}, wantMatched: 2,
			wantSummary: engine.SnapshotDiffSummary{Added: 2},
			wantPaths:   []string{"docs/added-a.txt"},
		},
		{
			name: "summary_ignores_positive_limit", filter: engine.SnapshotDiffAdded,
			query: engine.SnapshotQuery{Prefixes: []string{"docs/"}, Limit: 1}, summaryMode: true, wantMatched: 2,
			wantSummary: engine.SnapshotDiffSummary{Added: 2}, wantPaths: nil,
		},
		{
			name: "summary_fast_path_ignores_positive_limit", query: engine.SnapshotQuery{Limit: 1},
			summaryMode: true, wantMatched: 6,
			wantSummary: engine.SnapshotDiffSummary{Added: 3, Removed: 2, Modified: 1}, wantPaths: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := eng.SnapshotDiff(context.Background(), engine.SnapshotDiffRequest{
				BaseID: "phase9-diff-base", TargetID: "phase9-diff-target",
				Filter: tc.filter, Query: tc.query, Summary: tc.summaryMode,
			})
			if err != nil {
				t.Fatalf("SnapshotDiff: %v", err)
			}
			if got, want := result.TotalEntryCount, 6; got != want {
				t.Errorf("TotalEntryCount = %d, want raw total %d", got, want)
			}
			if got := result.MatchedEntryCount; got != tc.wantMatched {
				t.Errorf("MatchedEntryCount = %d, want pre-limit %d", got, tc.wantMatched)
			}
			if result.Summary != tc.wantSummary {
				t.Errorf("Summary = %+v, want %+v", result.Summary, tc.wantSummary)
			}
			if got := snapshotDiffResultPaths(result); !reflect.DeepEqual(got, tc.wantPaths) {
				t.Errorf("Entries paths = %v, want %v", got, tc.wantPaths)
			}
		})
	}

	result, err := eng.SnapshotDiff(context.Background(), engine.SnapshotDiffRequest{
		BaseID: "phase9-diff-base", TargetID: "phase9-diff-target",
		Query: engine.SnapshotQuery{Limit: -1},
	})
	if !engine.IsCode(err, engine.ErrorInvalidArgument) {
		t.Fatalf("negative Limit error = %v, want invalid_argument", err)
	}
	if !reflect.DeepEqual(result, engine.SnapshotDiffResult{}) {
		t.Fatalf("negative Limit result = %+v, want zero value", result)
	}
}

func seedPhase9SnapshotDiffFixture(t *testing.T, db *sql.DB) {
	t.Helper()
	now := time.Date(2026, 8, 25, 9, 0, 0, 0, time.UTC)
	insertTestSnapshot(t, db, "phase9-diff-base", "full", "", "", now)
	insertTestSnapshot(t, db, "phase9-diff-target", "full", "", "", now.Add(time.Second))

	insertTestSnapshotFile(t, db, "phase9-diff-base", "docs/modified-b.txt", 20)
	insertTestSnapshotFile(t, db, "phase9-diff-base", "docs/removed-a.txt", 11)
	insertTestSnapshotFile(t, db, "phase9-diff-base", "docs/unchanged.txt", 5)
	insertTestSnapshotFile(t, db, "phase9-diff-base", "misc/removed-z.txt", 12)

	insertTestSnapshotFile(t, db, "phase9-diff-target", "docs/added-a.txt", 13)
	insertTestSnapshotFile(t, db, "phase9-diff-target", "docs/added-c.txt", 14)
	insertTestSnapshotFile(t, db, "phase9-diff-target", "docs/modified-b.txt", 21)
	insertTestSnapshotFile(t, db, "phase9-diff-target", "docs/unchanged.txt", 5)
	insertTestSnapshotFile(t, db, "phase9-diff-target", "misc/added-z.txt", 15)
}

func snapshotDiffResultPaths(result engine.SnapshotDiffResult) []string {
	if result.Entries == nil {
		return nil
	}
	paths := make([]string, len(result.Entries))
	for i, entry := range result.Entries {
		paths[i] = entry.StoredPath
	}
	return paths
}

func TestSnapshotMetaFileCountIsTotalPersistedMembership(t *testing.T) {
	db := openSnapshotTestDB(t)
	now := time.Date(2026, 8, 25, 10, 0, 0, 0, time.UTC)
	insertTestSnapshot(t, db, "phase9-empty", "full", "empty", "", now)
	insertTestSnapshot(t, db, "phase9-parent", "full", "parent", "", now.Add(time.Second))
	insertTestSnapshot(t, db, "phase9-child", "partial", "child", "phase9-parent", now.Add(2*time.Second))

	insertTestSnapshotFile(t, db, "phase9-parent", "docs/a.txt", 10)
	insertTestSnapshotFile(t, db, "phase9-parent", "docs/b.txt", 20)
	insertTestSnapshotFile(t, db, "phase9-parent", "misc/c.txt", 30)
	insertTestSnapshotFile(t, db, "phase9-child", "docs/a.txt", 10)
	insertTestSnapshotFile(t, db, "phase9-child", "child-only.txt", 40)

	eng, err := engine.New(engine.Config{DB: db})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}

	list, err := eng.SnapshotList(context.Background(), engine.SnapshotListRequest{})
	if err != nil {
		t.Fatalf("SnapshotList: %v", err)
	}
	assertSnapshotMetaFileCounts(t, list.Snapshots, map[string]int{
		"phase9-empty": 0, "phase9-parent": 3, "phase9-child": 2,
	})

	limitedList, err := eng.SnapshotList(context.Background(), engine.SnapshotListRequest{Limit: 1})
	if err != nil {
		t.Fatalf("SnapshotList limited: %v", err)
	}
	assertSnapshotMetaFileCounts(t, limitedList.Snapshots, map[string]int{"phase9-child": 2})

	show, err := eng.SnapshotShow(context.Background(), engine.SnapshotShowRequest{
		SnapshotID: "phase9-parent",
		Query: engine.SnapshotQuery{
			Prefixes: []string{"docs/"},
			Limit:    1,
		},
	})
	if err != nil {
		t.Fatalf("SnapshotShow filtered and limited: %v", err)
	}
	if got, want := show.Snapshot.FileCount, 3; got != want {
		t.Errorf("SnapshotShow Snapshot.FileCount = %d, want full membership %d", got, want)
	}
	if got, want := show.TotalFileCount, 3; got != want {
		t.Errorf("SnapshotShow TotalFileCount = %d, want %d", got, want)
	}
	if got, want := len(show.Files), 1; got != want {
		t.Errorf("SnapshotShow Files length = %d, want selector limit %d", got, want)
	}
	emptyShow, err := eng.SnapshotShow(context.Background(), engine.SnapshotShowRequest{SnapshotID: "phase9-empty"})
	if err != nil {
		t.Fatalf("SnapshotShow empty: %v", err)
	}
	if emptyShow.Snapshot.FileCount != 0 || emptyShow.TotalFileCount != 0 || len(emptyShow.Files) != 0 {
		t.Errorf("empty SnapshotShow = %+v, want zero membership", emptyShow)
	}

	tree, err := eng.SnapshotList(context.Background(), engine.SnapshotListRequest{Tree: true})
	if err != nil {
		t.Fatalf("SnapshotList tree: %v", err)
	}
	assertSnapshotMetaFileCounts(t, tree.Snapshots, map[string]int{
		"phase9-empty": 0, "phase9-parent": 3, "phase9-child": 2,
	})
	graphMetas := make([]engine.SnapshotMeta, len(tree.Graph.Nodes))
	for i, node := range tree.Graph.Nodes {
		graphMetas[i] = node.Snapshot
	}
	assertSnapshotMetaFileCounts(t, graphMetas, map[string]int{
		"phase9-empty": 0, "phase9-parent": 3, "phase9-child": 2,
	})

	limitedTree, err := eng.SnapshotList(context.Background(), engine.SnapshotListRequest{Tree: true, Limit: 1})
	if err != nil {
		t.Fatalf("SnapshotList limited tree: %v", err)
	}
	assertSnapshotMetaFileCounts(t, limitedTree.Snapshots, map[string]int{"phase9-child": 2})
	if got, want := limitedTree.Graph.Nodes[0].Snapshot.FileCount, 2; got != want {
		t.Errorf("limited tree child FileCount = %d, want own membership %d", got, want)
	}
}

func assertSnapshotMetaFileCounts(t *testing.T, metas []engine.SnapshotMeta, want map[string]int) {
	t.Helper()
	if len(metas) != len(want) {
		t.Fatalf("metadata length = %d, want %d: %+v", len(metas), len(want), metas)
	}
	for _, meta := range metas {
		wantCount, ok := want[meta.ID]
		if !ok {
			t.Errorf("unexpected snapshot metadata %q", meta.ID)
			continue
		}
		if meta.FileCount != wantCount {
			t.Errorf("snapshot %q FileCount = %d, want persisted membership %d", meta.ID, meta.FileCount, wantCount)
		}
	}
}
