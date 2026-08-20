package main

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/engine"
)

func TestRunListCommandUsesEngineJSONParity(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)
	installCurrentFileCommandStubs(t, dbconn, stubCommandEngine{
		listFilesFunc: func(_ context.Context, req engine.ListFilesRequest) (engine.ListFilesResult, error) {
			if req.Limit == nil || *req.Limit != 2 || req.Offset == nil || *req.Offset != 1 {
				t.Fatalf("unexpected ListFiles request: %+v", req)
			}
			return engine.ListFilesResult{Files: []engine.CurrentFile{{
				ID: 7, Name: "/docs/a.txt", FileHash: "abc", SizeBytes: 12, CreatedAt: "2026-01-02 03:04:05",
			}}}, nil
		},
	})

	output := captureStdout(t, func() {
		err := runListCommand(parsedCommandLine{
			method: "list", flags: map[string][]string{"limit": {"2"}, "offset": {"1"}},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runListCommand: %v", err)
		}
	})
	want := `{"command":"list","files":[{"id":7,"name":"/docs/a.txt","file_hash":"abc","size_bytes":12,"created_at":"2026-01-02 03:04:05"}],"status":"ok"}`
	if strings.TrimSpace(output) != want {
		t.Fatalf("unexpected list JSON:\n%s", output)
	}
}

func TestRunSearchCommandUsesTypedEngineFiltersAndTextParity(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)
	installCurrentFileCommandStubs(t, dbconn, stubCommandEngine{
		searchFilesFunc: func(_ context.Context, req engine.SearchFilesRequest) (engine.SearchFilesResult, error) {
			if strings.Join(req.NameContains, ",") != "docs,report" || len(req.MinSizeBytes) != 2 || req.MinSizeBytes[0] != 10 || req.MinSizeBytes[1] != 20 || len(req.MaxSizeBytes) != 1 || req.MaxSizeBytes[0] != 100 {
				t.Fatalf("unexpected SearchFiles request: %+v", req)
			}
			return engine.SearchFilesResult{Files: []engine.CurrentFile{{
				ID: 9, Name: "/docs/report.txt", SizeBytes: 42, CreatedAt: "2026-02-03 04:05:06",
			}}}, nil
		},
	})

	output := captureStdout(t, func() {
		err := runSearchCommand(parsedCommandLine{
			method: "search",
			flags: map[string][]string{
				"name": {"docs", "report"}, "min-size": {"10", "20"}, "max-size": {"100"},
			},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runSearchCommand: %v", err)
		}
	})
	for _, want := range []string{"ID", "PATH", "/docs/report.txt", "42", "2026-02-03 04:05:06"} {
		if !strings.Contains(output, want) {
			t.Fatalf("expected search output to contain %q, got:\n%s", want, output)
		}
	}
}

func TestRunCurrentFileCommandsPropagateEngineErrors(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)
	installCurrentFileCommandStubs(t, dbconn, stubCommandEngine{
		listFilesFunc: func(context.Context, engine.ListFilesRequest) (engine.ListFilesResult, error) {
			return engine.ListFilesResult{}, errors.New("list failed")
		},
	})
	err := runListCommand(parsedCommandLine{method: "list", flags: map[string][]string{}}, outputModeText)
	if err == nil || err.Error() != "list failed" {
		t.Fatalf("expected engine error unchanged, got %v", err)
	}
}

func installCurrentFileCommandStubs(t *testing.T, dbconn *sql.DB, stub stubCommandEngine) {
	t.Helper()
	originalConnect := connectListSearchDBPhase
	originalNewEngine := newCommandEngine
	t.Cleanup(func() {
		connectListSearchDBPhase = originalConnect
		newCommandEngine = originalNewEngine
	})
	connectListSearchDBPhase = func() (*sql.DB, error) { return dbconn, nil }
	newCommandEngine = func(*sql.DB, string) (engine.Engine, error) { return stub, nil }
}
