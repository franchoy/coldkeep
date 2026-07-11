package main

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/batch"
	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/invariants"
	"github.com/franchoy/coldkeep/internal/storage"
)

type stubCommandEngine struct {
	engine.Engine
	snapshotCreateFunc    func(context.Context, engine.SnapshotCreateRequest) (engine.SnapshotCreateResult, error)
	snapshotDeleteFunc    func(context.Context, engine.SnapshotDeleteRequest) (engine.SnapshotDeleteResult, error)
	snapshotRestoreFunc   func(context.Context, engine.SnapshotRestoreRequest) (engine.SnapshotRestoreResult, error)
	restoreStoredPathFunc func(context.Context, engine.RestoreStoredPathRequest) (engine.RestoreStoredPathResult, error)
	removeStoredPathsFunc func(context.Context, engine.RemoveStoredPathsRequest) (engine.RemoveStoredPathsResult, error)
}

func (s stubCommandEngine) SnapshotCreate(ctx context.Context, req engine.SnapshotCreateRequest) (engine.SnapshotCreateResult, error) {
	if s.snapshotCreateFunc != nil {
		return s.snapshotCreateFunc(ctx, req)
	}
	return engine.SnapshotCreateResult{}, errors.New("unexpected SnapshotCreate call")
}

func (s stubCommandEngine) SnapshotDelete(ctx context.Context, req engine.SnapshotDeleteRequest) (engine.SnapshotDeleteResult, error) {
	if s.snapshotDeleteFunc != nil {
		return s.snapshotDeleteFunc(ctx, req)
	}
	return engine.SnapshotDeleteResult{}, errors.New("unexpected SnapshotDelete call")
}

func (s stubCommandEngine) SnapshotRestore(ctx context.Context, req engine.SnapshotRestoreRequest) (engine.SnapshotRestoreResult, error) {
	if s.snapshotRestoreFunc != nil {
		return s.snapshotRestoreFunc(ctx, req)
	}
	return engine.SnapshotRestoreResult{}, errors.New("unexpected SnapshotRestore call")
}

func (s stubCommandEngine) RestoreStoredPath(ctx context.Context, req engine.RestoreStoredPathRequest) (engine.RestoreStoredPathResult, error) {
	if s.restoreStoredPathFunc != nil {
		return s.restoreStoredPathFunc(ctx, req)
	}
	return engine.RestoreStoredPathResult{}, errors.New("unexpected RestoreStoredPath call")
}

func (s stubCommandEngine) RemoveStoredPaths(ctx context.Context, req engine.RemoveStoredPathsRequest) (engine.RemoveStoredPathsResult, error) {
	if s.removeStoredPathsFunc != nil {
		return s.removeStoredPathsFunc(ctx, req)
	}
	return engine.RemoveStoredPathsResult{}, errors.New("unexpected RemoveStoredPaths call")
}

func TestRunRestoreCommandStoredPathUsesEngineJSONParity(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)
	restoreByIDCalled := false
	installStoredPathCommandStubs(t, dbconn, stubCommandEngine{
		restoreStoredPathFunc: func(_ context.Context, req engine.RestoreStoredPathRequest) (engine.RestoreStoredPathResult, error) {
			if req.StoredPath != "/docs/routed.txt" {
				t.Fatalf("stored path mismatch: %+v", req)
			}
			if req.DestinationMode != engine.RestoreDestinationPrefix || req.DestinationRoot != "/tmp/out" || req.DestinationPath != "" {
				t.Fatalf("unexpected restore request: %+v", req)
			}
			if !req.Overwrite || !req.StrictMetadata || req.NoMetadata {
				t.Fatalf("unexpected restore flags: %+v", req)
			}
			return engine.RestoreStoredPathResult{
				StoredPath:      "/docs/routed.txt",
				FileID:          42,
				DestinationMode: engine.RestoreDestinationPrefix,
				DestinationPath: "/tmp/out/docs/routed.txt",
				RestoredHash:    "abc123",
			}, nil
		},
	}, &restoreByIDCalled, nil)

	output := captureStdout(t, func() {
		runStoredPathRestoreJSONCommand(t)
	})

	assertStoredPathRestoreJSONParity(t, output)
	if restoreByIDCalled {
		t.Fatal("stored-path restore must not route through restoreByIDPhase")
	}
}

func TestRunRestoreCommandStoredPathUsesEngineTextParity(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)
	installStoredPathCommandStubs(t, dbconn, stubCommandEngine{
		restoreStoredPathFunc: func(_ context.Context, req engine.RestoreStoredPathRequest) (engine.RestoreStoredPathResult, error) {
			return engine.RestoreStoredPathResult{
				StoredPath:      req.StoredPath,
				FileID:          77,
				DestinationMode: engine.RestoreDestinationOverride,
				DestinationPath: "/tmp/out.txt",
				RestoredHash:    "deadbeef",
			}, nil
		},
	}, nil, nil)

	output := captureStdout(t, func() {
		err := runRestoreCommand(parsedCommandLine{
			method: "restore",
			flags: map[string][]string{
				"stored-path": {"/docs/a.txt"},
				"mode":        {"override"},
				"destination": {"/tmp/out.txt"},
			},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runRestoreCommand: %v", err)
		}
	})

	for _, want := range []string{
		"File restored successfully: /tmp/out.txt",
		"FileID: 77",
		"SHA256: deadbeef",
		"Hint: " + doctorOperationalHint,
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, output)
		}
	}
}

func TestRunRestoreCommandStoredPathPropagatesEngineErrors(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)
	installStoredPathCommandStubs(t, dbconn, stubCommandEngine{
		restoreStoredPathFunc: func(_ context.Context, req engine.RestoreStoredPathRequest) (engine.RestoreStoredPathResult, error) {
			return engine.RestoreStoredPathResult{}, errors.New("output path already exists")
		},
	}, nil, nil)

	err := runRestoreCommand(parsedCommandLine{
		method: "restore",
		flags: map[string][]string{
			"stored-path": {"/docs/a.txt"},
			"mode":        {"override"},
			"destination": {"/tmp/out.txt"},
		},
	}, outputModeText)
	if err == nil || err.Error() != "output path already exists" {
		t.Fatalf("expected engine restore error to propagate unchanged, got %v", err)
	}
}

func TestRunRemoveCommandStoredPathUsesEngineJSONParity(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)
	removeByIDCalled := false
	installStoredPathCommandStubs(t, dbconn, stubCommandEngine{
		removeStoredPathsFunc: func(_ context.Context, req engine.RemoveStoredPathsRequest) (engine.RemoveStoredPathsResult, error) {
			if len(req.StoredPaths) != 1 || req.StoredPaths[0] != "/docs/a.txt" || req.DryRun || !req.FailFast {
				t.Fatalf("unexpected remove request: %+v", req)
			}
			return engine.RemoveStoredPathsResult{
				Items: []engine.RemoveStoredPathItemResult{{
					RawTarget:         "/docs/a.txt",
					StoredPath:        "/docs/a.txt",
					LogicalFileID:     12,
					RemainingRefCount: 0,
					MappingRemoved:    true,
					Status:            engine.BatchItemOK,
				}},
				Summary: engine.BatchSummary{OK: 1},
			}, nil
		},
	}, nil, &removeByIDCalled)

	output := captureStdout(t, func() {
		err := runRemoveCommand(parsedCommandLine{
			method: "remove",
			flags: map[string][]string{
				"stored-path": {"/docs/a.txt"},
				"output":      {"json"},
			},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runRemoveCommand: %v", err)
		}
	})

	payload := assertSingleJSONObjectLine(t, output)
	data, ok := payload["data"].(map[string]any)
	if !ok {
		t.Fatalf("expected data object, got %v", payload)
	}
	if data["stored_path"] != "/docs/a.txt" || data["logical_file_id"] != float64(12) || data["remaining_ref_count"] != float64(0) || data["removed"] != true {
		t.Fatalf("unexpected remove payload data: %v", data)
	}
	if removeByIDCalled {
		t.Fatal("single stored-path remove must not route through removeByIDPhase")
	}
}

func TestRunRemoveCommandStoredPathsUsesEngineBatchParity(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)
	installStoredPathCommandStubs(t, dbconn, stubCommandEngine{
		removeStoredPathsFunc: func(_ context.Context, req engine.RemoveStoredPathsRequest) (engine.RemoveStoredPathsResult, error) {
			want := []string{"   ", " /docs/a.txt ", "/docs/a.txt"}
			if strings.Join(req.StoredPaths, "|") != strings.Join(want, "|") || req.FailFast || !req.DryRun {
				t.Fatalf("unexpected batch remove request: %+v", req)
			}
			return engine.RemoveStoredPathsResult{
				DryRun: true,
				Items: []engine.RemoveStoredPathItemResult{
					{
						RawTarget:      "   ",
						StoredPath:     "",
						MappingRemoved: false,
						Status:         engine.BatchItemFailed,
						Error:          "stored path is required",
					},
					{
						RawTarget:      " /docs/a.txt ",
						StoredPath:     "/docs/a.txt",
						LogicalFileID:  9,
						MappingRemoved: false,
						Status:         engine.BatchItemPlanned,
					},
					{
						RawTarget:      "/docs/a.txt",
						StoredPath:     "/docs/a.txt",
						MappingRemoved: false,
						Status:         engine.BatchItemSkipped,
						Error:          "duplicate target",
					},
				},
				Summary: engine.BatchSummary{OK: 1, Failed: 1, Skipped: 1},
			}, nil
		},
	}, nil, nil)

	output := captureStdout(t, func() {
		runStoredPathBatchParityCommand(t)
	})

	assertStoredPathBatchParity(t, output)
}

func TestRunRemoveCommandStoredPathsInputFileRemainsCLIOwned(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)
	inputPath := filepath.Join(t.TempDir(), "targets.txt")
	if err := os.WriteFile(inputPath, []byte("# comment\n /docs/a.txt \n/docs/b.txt\n"), 0o600); err != nil {
		t.Fatalf("write input file: %v", err)
	}

	called := false
	installStoredPathCommandStubs(t, dbconn, stubCommandEngine{
		removeStoredPathsFunc: func(_ context.Context, req engine.RemoveStoredPathsRequest) (engine.RemoveStoredPathsResult, error) {
			called = true
			want := []string{"/docs/a.txt", "/docs/b.txt"}
			if strings.Join(req.StoredPaths, ",") != strings.Join(want, ",") {
				t.Fatalf("expected CLI-loaded input targets %v, got %v", want, req.StoredPaths)
			}
			return engine.RemoveStoredPathsResult{
				Items: []engine.RemoveStoredPathItemResult{
					{RawTarget: "/docs/a.txt", StoredPath: "/docs/a.txt", LogicalFileID: 1, RemainingRefCount: 0, MappingRemoved: true, Status: engine.BatchItemOK},
					{RawTarget: "/docs/b.txt", StoredPath: "/docs/b.txt", LogicalFileID: 2, RemainingRefCount: 0, MappingRemoved: true, Status: engine.BatchItemOK},
				},
				Summary: engine.BatchSummary{OK: 2},
			}, nil
		},
	}, nil, nil)

	err := runRemoveCommand(parsedCommandLine{
		method: "remove",
		flags: map[string][]string{
			"stored-paths": {""},
			"input":        {inputPath},
		},
	}, outputModeText)
	if err != nil {
		t.Fatalf("runRemoveCommand: %v", err)
	}
	if !called {
		t.Fatal("expected CLI to load input file and route targets to Engine.RemoveStoredPaths")
	}
}

func TestRunRemoveCommandStoredPathsAllInvalidSkipsRepositoryInitialization(t *testing.T) {
	originalLoad := loadDefaultStorageContextPhase
	originalNewEngine := newCommandEngine
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		newCommandEngine = originalNewEngine
	})

	loadCalls := 0
	engineCalls := 0
	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		loadCalls++
		return storage.StorageContext{}, errors.New("repository should not be loaded")
	}
	newCommandEngine = func(_ *sql.DB, _ string) (engine.Engine, error) {
		engineCalls++
		return stubCommandEngine{}, errors.New("engine should not be constructed")
	}

	output := captureStdout(t, func() {
		err := runRemoveCommand(parsedCommandLine{
			method:      "remove",
			positionals: []string{"   ", "\t"},
			flags: map[string][]string{
				"stored-paths": {""},
				"dry-run":      {""},
				"output":       {"json"},
			},
		}, outputModeJSON)
		if err == nil {
			t.Fatal("expected non-nil batch error due to invalid items")
		}
	})

	if loadCalls != 0 {
		t.Fatalf("expected no repository load, got %d", loadCalls)
	}
	if engineCalls != 0 {
		t.Fatalf("expected no engine construction, got %d", engineCalls)
	}

	payload := assertSingleJSONObjectLine(t, output)
	summary, _ := payload["summary"].(map[string]any)
	if summary["total"] != float64(2) || summary["failed"] != float64(2) {
		t.Fatalf("unexpected summary: %v", summary)
	}
	results := payload["results"].([]any)
	if len(results) != 2 {
		t.Fatalf("expected two results, got %d", len(results))
	}
}

func runStoredPathRestoreJSONCommand(t *testing.T) {
	t.Helper()
	err := runRestoreCommand(parsedCommandLine{
		method: "restore",
		flags: map[string][]string{
			"stored-path": {"/docs/routed.txt"},
			"mode":        {"prefix"},
			"destination": {"/tmp/out"},
			"overwrite":   {""},
			"strict":      {""},
			"output":      {"json"},
		},
	}, outputModeJSON)
	if err != nil {
		t.Fatalf("runRestoreCommand: %v", err)
	}
}

func assertStoredPathRestoreJSONParity(t *testing.T, output string) {
	t.Helper()
	payload := assertSingleJSONObjectLine(t, output)
	data, ok := payload["data"].(map[string]any)
	if !ok {
		t.Fatalf("expected data object, got %v", payload)
	}
	if data["stored_path"] != "/docs/routed.txt" || data["output_path"] != "/tmp/out/docs/routed.txt" {
		t.Fatalf("unexpected restore payload data: %v", data)
	}
	if data["file_id"] != float64(42) || data["restored_hash"] != "abc123" || data["mode"] != "prefix" {
		t.Fatalf("unexpected restore payload fields: %v", data)
	}
	if _, ok := data["perf_spans"].([]any); !ok {
		t.Fatalf("expected perf_spans array, got %T", data["perf_spans"])
	}
}

func runStoredPathBatchParityCommand(t *testing.T) {
	t.Helper()
	err := runRemoveCommand(parsedCommandLine{
		method:      "remove",
		positionals: []string{"   ", " /docs/a.txt ", "/docs/a.txt"},
		flags: map[string][]string{
			"stored-paths": {""},
			"dry-run":      {""},
			"output":       {"json"},
		},
	}, outputModeJSON)
	if err == nil {
		t.Fatal("expected non-nil batch error due to failed item")
	}
}

func assertStoredPathBatchParity(t *testing.T, output string) {
	t.Helper()
	payload := assertSingleJSONObjectLine(t, output)
	if payload["execution_mode"] != string(batch.ExecutionModeContinueOnError) {
		t.Fatalf("unexpected execution mode: %v", payload)
	}
	summary, _ := payload["summary"].(map[string]any)
	if summary["planned"] != float64(1) || summary["failed"] != float64(1) || summary["skipped"] != float64(1) {
		t.Fatalf("unexpected summary: %v", summary)
	}
	results := payload["results"].([]any)
	assertStoredPathBatchParityResults(t, results)
}

func assertStoredPathBatchParityResults(t *testing.T, results []any) {
	t.Helper()
	assertStoredPathBlankProjection(t, results[0].(map[string]any))
	assertStoredPathPlannedProjection(t, results[1].(map[string]any))
	assertStoredPathDuplicateProjection(t, results[2].(map[string]any))
}

func assertStoredPathBlankProjection(t *testing.T, result map[string]any) {
	t.Helper()
	if _, hasRawValue := result["raw_value"]; hasRawValue {
		t.Fatalf("blank-target raw_value should stay omitted after trimming, got %v", result)
	}
	if result["error"] != "invalid stored path \"   \"" {
		t.Fatalf("unexpected blank-target projection: %v", result)
	}
}

func assertStoredPathPlannedProjection(t *testing.T, result map[string]any) {
	t.Helper()
	if result["status"] != string(batch.ResultPlanned) || result["raw_value"] != "/docs/a.txt" || result["message"] != "would remove stored-path mapping" {
		t.Fatalf("unexpected planned projection: %v", result)
	}
}

func assertStoredPathDuplicateProjection(t *testing.T, result map[string]any) {
	t.Helper()
	if result["status"] != string(batch.ResultSkipped) || result["raw_value"] != "/docs/a.txt" || result["message"] != "duplicate target" {
		t.Fatalf("unexpected duplicate projection: %v", result)
	}
}

func TestRunRemoveCommandStoredPathPreservesInvariantErrorProjection(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)
	installStoredPathCommandStubs(t, dbconn, stubCommandEngine{
		removeStoredPathsFunc: func(_ context.Context, req engine.RemoveStoredPathsRequest) (engine.RemoveStoredPathsResult, error) {
			return engine.RemoveStoredPathsResult{
				Items: []engine.RemoveStoredPathItemResult{{
					RawTarget:         req.StoredPaths[0],
					StoredPath:        req.StoredPaths[0],
					Status:            engine.BatchItemFailed,
					Error:             "snapshot retained delete blocked",
					InvariantCode:     invariants.CodeSnapshotRetainedDeleteBlocked,
					RecommendedAction: invariants.RecommendedActionForCode(invariants.CodeSnapshotRetainedDeleteBlocked),
				}},
				Summary: engine.BatchSummary{Failed: 1},
			}, nil
		},
	}, nil, nil)

	err := runRemoveCommand(parsedCommandLine{
		method: "remove",
		flags: map[string][]string{
			"stored-path": {"/docs/a.txt"},
		},
	}, outputModeText)
	if err == nil {
		t.Fatal("expected invariant failure")
	}
	if code, ok := invariants.Code(err); !ok || code != invariants.CodeSnapshotRetainedDeleteBlocked {
		t.Fatalf("expected invariant code to survive projection, got err=%v code=%q ok=%v", err, code, ok)
	}
}

func TestStoredPathCommandsDoNotBypassEngine(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)

	restoreCalls := 0
	removeCalls := 0
	installStoredPathCommandStubs(t, dbconn, stubCommandEngine{
		restoreStoredPathFunc: func(_ context.Context, req engine.RestoreStoredPathRequest) (engine.RestoreStoredPathResult, error) {
			restoreCalls++
			return engine.RestoreStoredPathResult{
				StoredPath:      req.StoredPath,
				FileID:          7,
				DestinationMode: req.DestinationMode,
				DestinationPath: "/tmp/out.txt",
				RestoredHash:    "abc123",
			}, nil
		},
		removeStoredPathsFunc: func(_ context.Context, req engine.RemoveStoredPathsRequest) (engine.RemoveStoredPathsResult, error) {
			removeCalls++
			items := make([]engine.RemoveStoredPathItemResult, 0, len(req.StoredPaths))
			for _, storedPath := range req.StoredPaths {
				items = append(items, engine.RemoveStoredPathItemResult{
					RawTarget:         storedPath,
					StoredPath:        strings.TrimSpace(storedPath),
					LogicalFileID:     9,
					RemainingRefCount: 0,
					MappingRemoved:    !req.DryRun,
					Status:            engine.BatchItemOK,
				})
			}
			return engine.RemoveStoredPathsResult{
				DryRun:  req.DryRun,
				Items:   items,
				Summary: engine.BatchSummary{OK: len(items)},
			}, nil
		},
	}, nil, nil)

	if err := runRestoreCommand(parsedCommandLine{
		method: "restore",
		flags: map[string][]string{
			"stored-path": {"/docs/a.txt"},
			"mode":        {"override"},
			"destination": {"/tmp/out.txt"},
			"output":      {"json"},
		},
	}, outputModeJSON); err != nil {
		t.Fatalf("runRestoreCommand stored-path: %v", err)
	}

	if err := runRemoveCommand(parsedCommandLine{
		method: "remove",
		flags: map[string][]string{
			"stored-path": {"/docs/a.txt"},
			"output":      {"json"},
		},
	}, outputModeJSON); err != nil {
		t.Fatalf("runRemoveCommand stored-path: %v", err)
	}

	if err := runRemoveCommand(parsedCommandLine{
		method:      "remove",
		positionals: []string{"/docs/a.txt", "/docs/b.txt"},
		flags: map[string][]string{
			"stored-paths": {""},
			"output":       {"json"},
		},
	}, outputModeJSON); err != nil {
		t.Fatalf("runRemoveCommand stored-paths: %v", err)
	}

	if restoreCalls != 1 {
		t.Fatalf("expected exactly one stored-path restore engine call, got %d", restoreCalls)
	}
	if removeCalls != 2 {
		t.Fatalf("expected exactly two stored-path remove engine calls, got %d", removeCalls)
	}
}

func installStoredPathCommandStubs(
	t *testing.T,
	dbconn *sql.DB,
	stub stubCommandEngine,
	restoreByIDCalled *bool,
	removeByIDCalled *bool,
) {
	t.Helper()

	originalLoad := loadDefaultStorageContextPhase
	originalNewEngine := newCommandEngine
	originalRestoreByID := restoreByIDPhase
	originalRemoveByID := removeByIDPhase
	t.Cleanup(func() {
		loadDefaultStorageContextPhase = originalLoad
		newCommandEngine = originalNewEngine
		restoreByIDPhase = originalRestoreByID
		removeByIDPhase = originalRemoveByID
	})

	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		return storage.StorageContext{DB: dbconn}, nil
	}
	newCommandEngine = func(dbconn *sql.DB, containerDir string) (engine.Engine, error) {
		return stub, nil
	}
	if restoreByIDCalled != nil {
		restoreByIDPhase = func(_ *storage.StorageContext, _ int64, _ string, _ bool, _ bool) (storage.RestoreFileResult, error) {
			*restoreByIDCalled = true
			return storage.RestoreFileResult{}, nil
		}
	}
	if removeByIDCalled != nil {
		removeByIDPhase = func(_ *storage.StorageContext, _ int64, _ bool) batch.ItemResult {
			*removeByIDCalled = true
			return batch.ItemResult{}
		}
	}
}
