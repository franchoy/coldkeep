package main

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/storage"
)

func installSnapshotReadEngineStub(t *testing.T, stub engine.Engine) {
	t.Helper()
	dbconn := openSnapshotRoutingDB(t)
	originalLoad := loadDefaultStorageContextPhase
	originalFactory := newSnapshotReadCommandEngine
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		newSnapshotReadCommandEngine = originalFactory
	})
	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		return storage.StorageContext{DB: dbconn}, nil
	}
	newSnapshotReadCommandEngine = func(storage.StorageContext) (engine.Engine, error) { return stub, nil }
}

func TestSnapshotShowUsesOneTypedEngineOperationWithRepeatedSelectors(t *testing.T) {
	calls := 0
	installSnapshotReadEngineStub(t, stubCommandEngine{
		snapshotShowFunc: func(_ context.Context, req engine.SnapshotShowRequest) (engine.SnapshotShowResult, error) {
			calls++
			if req.SnapshotID != "snap-read" {
				t.Fatalf("snapshot id = %q", req.SnapshotID)
			}
			if !reflect.DeepEqual(req.Query.Paths, []string{"a.txt", "b.txt"}) {
				t.Fatalf("paths = %#v", req.Query.Paths)
			}
			if !reflect.DeepEqual(req.Query.Prefixes, []string{"docs/", "src/"}) {
				t.Fatalf("prefixes = %#v", req.Query.Prefixes)
			}
			if req.Query.Pattern != "*.txt" || req.Query.Regex != `^(a|b)` || req.Query.Limit != 7 {
				t.Fatalf("query = %+v", req.Query)
			}
			size := int64(9007199254740993)
			return engine.SnapshotShowResult{
				Snapshot:         engine.SnapshotMeta{ID: req.SnapshotID, Type: engine.SnapshotTypeFull, FileCount: 1},
				Files:            []engine.SnapshotFile{{StoredPath: "a.txt", LogicalFileID: 11, Size: &size}},
				MatchedFileCount: 1, TotalFileCount: 2,
			}, nil
		},
	})

	output := captureStdout(t, func() {
		err := runSnapshotCommand(parsedCommandLine{
			method: "snapshot", positionals: []string{"show", "snap-read"},
			flags: map[string][]string{
				"path": {"b.txt", "a.txt"}, "prefix": {"docs/", "src/"},
				"pattern": {"*.txt"}, "regex": {`^(a|b)`}, "limit": {"7"}, "output": {"json"},
			},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("snapshot show: %v", err)
		}
	})
	if calls != 1 {
		t.Fatalf("SnapshotShow calls = %d, want 1", calls)
	}
	if !strings.Contains(output, `"size":9007199254740993`) || !strings.Contains(output, `"total_snapshot_file_count":2`) {
		t.Fatalf("compatibility JSON = %s", output)
	}
}

func TestSnapshotListStatsAndDiffEachUseOneEngineOperation(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	listCalls, statsCalls, diffCalls := 0, 0, 0
	installSnapshotReadEngineStub(t, stubCommandEngine{
		snapshotListFunc: func(_ context.Context, req engine.SnapshotListRequest) (engine.SnapshotListResult, error) {
			listCalls++
			if req.Type != engine.SnapshotTypePartial || req.Label != "daily" || !req.Tree || req.Limit != 4 {
				t.Fatalf("list request = %+v", req)
			}
			meta := engine.SnapshotMeta{ID: "s1", Type: engine.SnapshotTypePartial, CreatedAt: now}
			return engine.SnapshotListResult{
				Snapshots: []engine.SnapshotMeta{meta}, Count: 1, TreeMode: true,
				Graph: &engine.SnapshotGraph{Nodes: []engine.SnapshotGraphNode{{Snapshot: meta, ParentState: engine.SnapshotParentNone}}, RootIDs: []string{"s1"}},
			}, nil
		},
		snapshotStatsFunc: func(_ context.Context, req engine.SnapshotStatsRequest) (engine.SnapshotStatsResult, error) {
			statsCalls++
			if req.SnapshotID != "s1" {
				t.Fatalf("stats request = %+v", req)
			}
			return engine.SnapshotStatsResult{SnapshotCount: 1, SnapshotFileCount: 2, TotalSizeBytes: 3}, nil
		},
		snapshotDiffFunc: func(_ context.Context, req engine.SnapshotDiffRequest) (engine.SnapshotDiffResult, error) {
			diffCalls++
			if req.BaseID != "s1" || req.TargetID != "s2" || !req.Summary || req.Filter != engine.SnapshotDiffAdded {
				t.Fatalf("diff request = %+v", req)
			}
			if !reflect.DeepEqual(req.Query.Paths, []string{"a", "b"}) || !reflect.DeepEqual(req.Query.Prefixes, []string{"p/", "q/"}) {
				t.Fatalf("diff query = %+v", req.Query)
			}
			return engine.SnapshotDiffResult{BaseID: "s1", TargetID: "s2", SummaryMode: true, Summary: engine.SnapshotDiffSummary{Added: 1}, MatchedEntryCount: 1, TotalEntryCount: 3}, nil
		},
	})

	commands := []parsedCommandLine{
		{method: "snapshot", positionals: []string{"list"}, flags: map[string][]string{"type": {"partial"}, "label": {"daily"}, "limit": {"4"}, "tree": {""}, "output": {"json"}}},
		{method: "snapshot", positionals: []string{"stats", "s1"}, flags: map[string][]string{"output": {"json"}}},
		{method: "snapshot", positionals: []string{"diff", "s1", "s2"}, flags: map[string][]string{"summary": {""}, "filter": {"added"}, "path": {"b", "a"}, "prefix": {"p/", "q/"}, "output": {"json"}}},
	}
	for _, command := range commands {
		captureStdout(t, func() {
			if err := runSnapshotCommand(command, outputModeJSON); err != nil {
				t.Fatalf("%v: %v", command.positionals, err)
			}
		})
	}
	if listCalls != 1 || statsCalls != 1 || diffCalls != 1 {
		t.Fatalf("calls list=%d stats=%d diff=%d", listCalls, statsCalls, diffCalls)
	}
}

func TestSnapshotReadEngineErrorPropagatesWithoutFallback(t *testing.T) {
	want := errors.New("snapshot engine failure")
	installSnapshotReadEngineStub(t, stubCommandEngine{
		snapshotShowFunc: func(context.Context, engine.SnapshotShowRequest) (engine.SnapshotShowResult, error) {
			return engine.SnapshotShowResult{}, want
		},
	})
	err := runSnapshotCommand(parsedCommandLine{method: "snapshot", positionals: []string{"show", "s1"}, flags: map[string][]string{}}, outputModeText)
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
}
