package engine

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/snapshot"
	"github.com/franchoy/coldkeep/internal/storage"
)

func TestSnapshotRestoreRejectsInvalidRequests(t *testing.T) {
	dbconn := openSnapshotCreateEngineDB(t)
	sgctx := newSnapshotCreateStorageContext(t.TempDir(), dbconn)
	eng := newSnapshotRestoreEngine(t, dbconn, &sgctx)
	for _, tc := range snapshotRestoreInvalidRequestCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			_, err := eng.SnapshotRestore(context.Background(), tc.req)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}

type snapshotRestoreInvalidRequestCase struct {
	name    string
	req     SnapshotRestoreRequest
	wantErr string
}

func snapshotRestoreInvalidRequestCases(t *testing.T) []snapshotRestoreInvalidRequestCase {
	t.Helper()
	cases := snapshotRestoreInvalidDestinationCases(t)
	minSize := int64(8)
	maxSize := int64(4)
	return append(cases,
		snapshotRestoreInvalidRequestCase{
			name: "invalid regex",
			req: SnapshotRestoreRequest{
				SnapshotID:  "snap-restore",
				Destination: SnapshotRestoreDestination{Mode: SnapshotRestoreDestinationOriginal, Path: t.TempDir()},
				Selection:   SnapshotRestoreSelection{Regex: "("},
			},
			wantErr: "invalid snapshot restore regex",
		},
		snapshotRestoreInvalidRequestCase{
			name: "invalid size range",
			req: SnapshotRestoreRequest{
				SnapshotID:  "snap-restore",
				Destination: SnapshotRestoreDestination{Mode: SnapshotRestoreDestinationOriginal, Path: t.TempDir()},
				Selection:   SnapshotRestoreSelection{MinSize: &minSize, MaxSize: &maxSize},
			},
			wantErr: "min size cannot exceed max size",
		},
	)
}

func snapshotRestoreInvalidDestinationCases(t *testing.T) []snapshotRestoreInvalidRequestCase {
	t.Helper()
	originalPath := SnapshotRestoreDestination{Mode: SnapshotRestoreDestinationOriginal, Path: t.TempDir()}
	return []snapshotRestoreInvalidRequestCase{
		{name: "blank snapshot id", req: SnapshotRestoreRequest{SnapshotID: "   ", Destination: originalPath}, wantErr: "snapshot id cannot be empty"},
		{name: "missing destination mode", req: SnapshotRestoreRequest{SnapshotID: "snap-restore", Destination: SnapshotRestoreDestination{Path: t.TempDir()}}, wantErr: "snapshot restore destination mode is required"},
		{name: "unknown destination mode", req: SnapshotRestoreRequest{SnapshotID: "snap-restore", Destination: SnapshotRestoreDestination{Mode: SnapshotRestoreDestinationMode("boom"), Path: t.TempDir()}}, wantErr: `unknown snapshot restore destination mode "boom"`},
		{name: "missing original root", req: SnapshotRestoreRequest{SnapshotID: "snap-restore", Destination: SnapshotRestoreDestination{Mode: SnapshotRestoreDestinationOriginal}}, wantErr: "original mode requires destination path"},
		{name: "missing prefix root", req: SnapshotRestoreRequest{SnapshotID: "snap-restore", Destination: SnapshotRestoreDestination{Mode: SnapshotRestoreDestinationPrefix}}, wantErr: "prefix mode requires destination path"},
		{name: "missing override path", req: SnapshotRestoreRequest{SnapshotID: "snap-restore", Destination: SnapshotRestoreDestination{Mode: SnapshotRestoreDestinationOverride}}, wantErr: "override mode requires destination path"},
		{name: "unknown metadata mode", req: SnapshotRestoreRequest{SnapshotID: "snap-restore", Destination: originalPath, Metadata: SnapshotRestoreMetadataMode("boom")}, wantErr: `unknown snapshot restore metadata mode "boom"`},
	}
}

func TestSnapshotRestoreUsesExplicitOriginalRootAndPreservesLexicalPaths(t *testing.T) {
	dbconn, snapshotID, sgctx := setupSnapshotRestoreEngineCase(t)
	eng := newSnapshotRestoreEngine(t, dbconn, &sgctx)

	outputRoot := filepath.Join(t.TempDir(), "restore-root")
	res, err := eng.SnapshotRestore(context.Background(), SnapshotRestoreRequest{
		SnapshotID: " " + snapshotID + " ",
		Paths:      []string{"docs/a.txt"},
		Destination: SnapshotRestoreDestination{
			Mode: SnapshotRestoreDestinationOriginal,
			Path: outputRoot,
		},
		Overwrite: true,
	})
	if err != nil {
		t.Fatalf("SnapshotRestore original: %v", err)
	}

	wantPath := filepath.Join(outputRoot, "docs", "a.txt")
	if res.SnapshotID != snapshotID || res.DestinationMode != SnapshotRestoreDestinationOriginal {
		t.Fatalf("unexpected restore header: %+v", res)
	}
	if res.RequestedPathsCount != 1 || res.RestoredFiles != 1 {
		t.Fatalf("unexpected restore counts: %+v", res)
	}
	if res.OutputTarget != outputRoot {
		t.Fatalf("output target mismatch: got=%q want=%q", res.OutputTarget, outputRoot)
	}
	if !reflect.DeepEqual(res.OutputPaths, []string{wantPath}) {
		t.Fatalf("output paths mismatch: got=%v want=%v", res.OutputPaths, []string{wantPath})
	}
	assertSnapshotRestoreBytes(t, wantPath, []byte("snapshot-create-a"))
}

func TestMapSnapshotRestoreWarningsCopiesStructuredWarnings(t *testing.T) {
	got := mapSnapshotRestoreWarnings([]snapshot.RestoreSnapshotWarning{
		{
			Code:      snapshot.RestoreSnapshotWarningMetadata,
			Path:      "/tmp/out.txt",
			Operation: "chmod",
			Detail:    "permission denied",
		},
	})
	if len(got) != 1 {
		t.Fatalf("expected one warning, got %v", got)
	}
	if got[0] != (SnapshotRestoreWarning{
		Code:      SnapshotRestoreWarningMetadata,
		Path:      "/tmp/out.txt",
		Operation: "chmod",
		Detail:    "permission denied",
	}) {
		t.Fatalf("unexpected mapped warning: %+v", got[0])
	}
}

func TestSnapshotRestoreCopiesCallerOwnedInputs(t *testing.T) {
	after := time.Date(2026, 7, 11, 9, 0, 0, 0, time.UTC)
	req := SnapshotRestoreRequest{
		SnapshotID: " snap-copy ",
		Paths:      []string{"docs/a.txt"},
		Selection: SnapshotRestoreSelection{
			ExactPaths:    []string{"docs/a.txt", "docs/a.txt"},
			Prefixes:      []string{"docs/"},
			ModifiedAfter: &after,
		},
		Destination: SnapshotRestoreDestination{
			Mode: SnapshotRestoreDestinationOriginal,
			Path: "./restore-root",
		},
	}
	sgctx := snapshotRestoreDummyStorageContext()
	prepared, err := prepareSnapshotRestoreRequest(req, sgctx)
	if err != nil {
		t.Fatalf("prepareSnapshotRestoreRequest: %v", err)
	}
	mutateSnapshotRestoreCallerInputs(&req, &after)
	assertPreparedSnapshotRestoreIdentityCopy(t, prepared)
	assertPreparedSnapshotRestoreQueryCopy(t, prepared.restoreSnapshotOpts.Query)
}

func mutateSnapshotRestoreCallerInputs(req *SnapshotRestoreRequest, after *time.Time) {
	req.Paths[0] = "mutated"
	req.Selection.ExactPaths[0] = "changed"
	req.Selection.Prefixes[0] = "changed/"
	*after = after.Add(2 * time.Hour)
}

func assertPreparedSnapshotRestoreIdentityCopy(t *testing.T, prepared preparedSnapshotRestoreRequest) {
	t.Helper()
	if prepared.snapshotID != "snap-copy" {
		t.Fatalf("snapshot id not normalized: %q", prepared.snapshotID)
	}
	if !reflect.DeepEqual(prepared.paths, []string{"docs/a.txt"}) {
		t.Fatalf("paths mutated through caller alias: %v", prepared.paths)
	}
}

func assertPreparedSnapshotRestoreQueryCopy(t *testing.T, query *snapshot.SnapshotQuery) {
	t.Helper()
	if query == nil {
		t.Fatal("expected restore query")
	}
	if _, ok := query.ExactPaths["docs/a.txt"]; !ok {
		t.Fatalf("expected exact path copy, got %+v", query.ExactPaths)
	}
	if !reflect.DeepEqual(query.Prefixes, []string{"docs/"}) {
		t.Fatalf("prefixes mutated through caller alias: %v", query.Prefixes)
	}
	if query.ModifiedAfter == nil || !query.ModifiedAfter.Equal(time.Date(2026, 7, 11, 9, 0, 0, 0, time.UTC)) {
		t.Fatalf("modified-after pointer mutated through caller alias: %v", query.ModifiedAfter)
	}
}

func newSnapshotRestoreEngine(t *testing.T, dbconn *sql.DB, sgctx *storage.StorageContext) *DefaultEngine {
	t.Helper()

	eng, err := New(Config{DB: dbconn, StoreContext: sgctx})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	return eng
}

func setupSnapshotRestoreEngineCase(t *testing.T) (*sql.DB, string, storage.StorageContext) {
	t.Helper()

	dbconn := openSnapshotCreateEngineDB(t)
	root := t.TempDir()
	containerDir := t.TempDir()
	writer := container.NewLocalWriterWithDirAndDB(containerDir, container.GetContainerMaxSize(), dbconn)
	sgctx := storage.StorageContext{DB: dbconn, Writer: writer, ContainerDir: containerDir}
	storeSnapshotCreateCurrentFile(t, dbconn, sgctx, root, "docs/a.txt", "snapshot-create-a")
	mustCreateSnapshotResult(t, dbconn, snapshot.SnapshotCreateOptions{
		ID:    "snap-restore-engine",
		Type:  "partial",
		Paths: []string{"docs/a.txt"},
	})
	return dbconn, "snap-restore-engine", sgctx
}

func snapshotRestoreDummyStorageContext() *storage.StorageContext {
	return &storage.StorageContext{}
}

func assertSnapshotRestoreBytes(t *testing.T, path string, want []byte) {
	t.Helper()

	got, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		t.Fatalf("read restored bytes: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("restored bytes mismatch: got=%q want=%q", string(got), string(want))
	}
}
