package engine

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"sort"
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
			wantErr: "invalid snapshot query regex",
		},
		snapshotRestoreInvalidRequestCase{
			name: "invalid size range",
			req: SnapshotRestoreRequest{
				SnapshotID:  "snap-restore",
				Destination: SnapshotRestoreDestination{Mode: SnapshotRestoreDestinationOriginal, Path: t.TempDir()},
				Selection:   SnapshotRestoreSelection{MinSize: &minSize, MaxSize: &maxSize},
			},
			wantErr: "minimum exceeds maximum",
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

func TestSnapshotRestoreOverwriteReachesLowerRestoreContract(t *testing.T) {
	dbconn, snapshotID, sgctx := setupSnapshotRestoreEngineCase(t)
	eng := newSnapshotRestoreEngine(t, dbconn, &sgctx)

	outputRoot := t.TempDir()
	targetPath := filepath.Join(outputRoot, "docs", "a.txt")
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		t.Fatalf("create existing target parent: %v", err)
	}
	oldBytes := []byte("existing-destination")
	if err := os.WriteFile(targetPath, oldBytes, 0o600); err != nil {
		t.Fatalf("write existing target: %v", err)
	}

	req := SnapshotRestoreRequest{
		SnapshotID: snapshotID,
		Paths:      []string{"docs/a.txt"},
		Destination: SnapshotRestoreDestination{
			Mode: SnapshotRestoreDestinationOriginal,
			Path: outputRoot,
		},
	}
	if _, err := eng.SnapshotRestore(context.Background(), req); err == nil {
		t.Fatal("SnapshotRestore overwrite=false succeeded for existing target")
	}
	assertSnapshotRestoreBytes(t, targetPath, oldBytes)

	req.Overwrite = true
	result, err := eng.SnapshotRestore(context.Background(), req)
	if err != nil {
		t.Fatalf("SnapshotRestore overwrite=true: %v", err)
	}
	if result.RestoredFiles != 1 {
		t.Fatalf("RestoredFiles = %d, want 1", result.RestoredFiles)
	}
	assertSnapshotRestoreBytes(t, targetPath, []byte("snapshot-create-a"))
}

func TestSnapshotSelectorNormalizationParityAcrossShowDiffRestore(t *testing.T) {
	dbconn, snapshotID, sgctx := setupSnapshotRestoreEngineCase(t)
	eng := newSnapshotRestoreEngine(t, dbconn, &sgctx)

	negative := int64(-1)
	minSize := int64(2)
	maxSize := int64(1)
	modifiedBefore := time.Date(2026, 7, 11, 8, 0, 0, 0, time.UTC)
	modifiedAfter := modifiedBefore.Add(time.Hour)
	tests := []struct {
		name      string
		query     SnapshotQuery
		selection SnapshotRestoreSelection
	}{
		{name: "blank exact path", query: SnapshotQuery{Paths: []string{""}}, selection: SnapshotRestoreSelection{ExactPaths: []string{""}}},
		{name: "absolute exact path", query: SnapshotQuery{Paths: []string{"/docs/a.txt"}}, selection: SnapshotRestoreSelection{ExactPaths: []string{"/docs/a.txt"}}},
		{name: "traversal exact path", query: SnapshotQuery{Paths: []string{"../docs/a.txt"}}, selection: SnapshotRestoreSelection{ExactPaths: []string{"../docs/a.txt"}}},
		{name: "prefix missing trailing slash", query: SnapshotQuery{Prefixes: []string{"docs"}}, selection: SnapshotRestoreSelection{Prefixes: []string{"docs"}}},
		{name: "invalid glob", query: SnapshotQuery{Pattern: "["}, selection: SnapshotRestoreSelection{Pattern: "["}},
		{name: "invalid regex", query: SnapshotQuery{Regex: "("}, selection: SnapshotRestoreSelection{Regex: "("}},
		{name: "negative min size", query: SnapshotQuery{MinSize: &negative}, selection: SnapshotRestoreSelection{MinSize: &negative}},
		{name: "negative max size", query: SnapshotQuery{MaxSize: &negative}, selection: SnapshotRestoreSelection{MaxSize: &negative}},
		{name: "min exceeds max", query: SnapshotQuery{MinSize: &minSize, MaxSize: &maxSize}, selection: SnapshotRestoreSelection{MinSize: &minSize, MaxSize: &maxSize}},
		{name: "modified after exceeds before", query: SnapshotQuery{ModifiedAfter: &modifiedAfter, ModifiedBefore: &modifiedBefore}, selection: SnapshotRestoreSelection{ModifiedAfter: &modifiedAfter, ModifiedBefore: &modifiedBefore}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			operations := []struct {
				name string
				run  func() error
			}{
				{name: "show", run: func() error {
					_, err := eng.SnapshotShow(context.Background(), SnapshotShowRequest{SnapshotID: snapshotID, Query: tc.query})
					return err
				}},
				{name: "diff", run: func() error {
					_, err := eng.SnapshotDiff(context.Background(), SnapshotDiffRequest{BaseID: snapshotID, TargetID: snapshotID, Query: tc.query})
					return err
				}},
				{name: "restore", run: func() error {
					_, err := eng.SnapshotRestore(context.Background(), SnapshotRestoreRequest{
						SnapshotID: snapshotID,
						Selection:  tc.selection,
						Destination: SnapshotRestoreDestination{
							Mode: SnapshotRestoreDestinationOriginal,
							Path: t.TempDir(),
						},
					})
					return err
				}},
			}
			for _, operation := range operations {
				t.Run(operation.name, func(t *testing.T) {
					err := operation.run()
					if !IsCode(err, ErrorInvalidArgument) {
						t.Fatalf("error = %v, want %s", err, ErrorInvalidArgument)
					}
				})
			}
		})
	}
}

func TestSnapshotSelectorMatchingParityAcrossShowDiffRestore(t *testing.T) {
	eng, baseID, targetID := setupSnapshotSelectorParityEngineCase(t)
	minSize := int64(0)
	maxSize := int64(1 << 20)
	modifiedAfter := time.Unix(0, 0).UTC()
	modifiedBefore := time.Now().UTC().Add(24 * time.Hour)
	tests := []struct {
		name      string
		query     SnapshotQuery
		selection SnapshotRestoreSelection
		wantPaths []string
	}{
		{name: "normalized exact path", query: SnapshotQuery{Paths: []string{"./docs//a.txt"}}, selection: SnapshotRestoreSelection{ExactPaths: []string{"./docs//a.txt"}}, wantPaths: []string{"docs/a.txt"}},
		{name: "windows exact path", query: SnapshotQuery{Paths: []string{`.\docs\a.txt`}}, selection: SnapshotRestoreSelection{ExactPaths: []string{`.\docs\a.txt`}}, wantPaths: []string{"docs/a.txt"}},
		{name: "directory prefix isolation", query: SnapshotQuery{Prefixes: []string{"./docs/"}}, selection: SnapshotRestoreSelection{Prefixes: []string{"./docs/"}}, wantPaths: []string{"docs/a.txt"}},
		{name: "glob", query: SnapshotQuery{Pattern: "docs/*.txt"}, selection: SnapshotRestoreSelection{Pattern: "docs/*.txt"}, wantPaths: []string{"docs/a.txt"}},
		{name: "regex", query: SnapshotQuery{Regex: `^docs/a[.]txt$`}, selection: SnapshotRestoreSelection{Regex: `^docs/a[.]txt$`}, wantPaths: []string{"docs/a.txt"}},
		{name: "size range", query: SnapshotQuery{MinSize: &minSize, MaxSize: &maxSize}, selection: SnapshotRestoreSelection{MinSize: &minSize, MaxSize: &maxSize}, wantPaths: []string{"docs-old/a.txt", "docs/a.txt", "docs2/a.txt"}},
		{name: "time range", query: SnapshotQuery{ModifiedAfter: &modifiedAfter, ModifiedBefore: &modifiedBefore}, selection: SnapshotRestoreSelection{ModifiedAfter: &modifiedAfter, ModifiedBefore: &modifiedBefore}, wantPaths: []string{"docs-old/a.txt", "docs/a.txt", "docs2/a.txt"}},
		{name: "empty query", wantPaths: []string{"docs-old/a.txt", "docs/a.txt", "docs2/a.txt"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			show, err := eng.SnapshotShow(context.Background(), SnapshotShowRequest{SnapshotID: targetID, Query: tc.query})
			if err != nil {
				t.Fatalf("SnapshotShow: %v", err)
			}
			if got := snapshotShowStoredPaths(show); !reflect.DeepEqual(got, tc.wantPaths) {
				t.Fatalf("SnapshotShow paths = %v, want %v", got, tc.wantPaths)
			}

			diff, err := eng.SnapshotDiff(context.Background(), SnapshotDiffRequest{BaseID: baseID, TargetID: targetID, Query: tc.query})
			if err != nil {
				t.Fatalf("SnapshotDiff: %v", err)
			}
			if got := snapshotDiffStoredPaths(diff); !reflect.DeepEqual(got, tc.wantPaths) {
				t.Fatalf("SnapshotDiff paths = %v, want %v", got, tc.wantPaths)
			}

			outputRoot := t.TempDir()
			restore, err := eng.SnapshotRestore(context.Background(), SnapshotRestoreRequest{
				SnapshotID: targetID,
				Selection:  tc.selection,
				Destination: SnapshotRestoreDestination{
					Mode: SnapshotRestoreDestinationOriginal,
					Path: outputRoot,
				},
			})
			if err != nil {
				t.Fatalf("SnapshotRestore: %v", err)
			}
			if got := snapshotRestoreStoredPaths(t, outputRoot, restore); !reflect.DeepEqual(got, tc.wantPaths) {
				t.Fatalf("SnapshotRestore paths = %v, want %v", got, tc.wantPaths)
			}
		})
	}
}

func TestSnapshotRestorePreservesExplicitPathAccountingWithSelectionDeduplication(t *testing.T) {
	dbconn, snapshotID, sgctx := setupSnapshotRestoreEngineCase(t)
	eng := newSnapshotRestoreEngine(t, dbconn, &sgctx)
	req := SnapshotRestoreRequest{
		SnapshotID: snapshotID,
		Paths:      []string{"./docs/a.txt", "docs/a.txt"},
		Selection: SnapshotRestoreSelection{
			ExactPaths: []string{"./docs/a.txt", "docs/a.txt"},
		},
		Destination: SnapshotRestoreDestination{
			Mode: SnapshotRestoreDestinationOriginal,
			Path: t.TempDir(),
		},
	}

	result, err := eng.SnapshotRestore(context.Background(), req)
	if err != nil {
		t.Fatalf("SnapshotRestore: %v", err)
	}
	if result.RequestedPathsCount != 2 {
		t.Fatalf("RequestedPathsCount = %d, want raw count 2", result.RequestedPathsCount)
	}
	if result.RestoredFiles != 1 {
		t.Fatalf("RestoredFiles = %d, want normalized selection count 1", result.RestoredFiles)
	}
	if !reflect.DeepEqual(req.Paths, []string{"./docs/a.txt", "docs/a.txt"}) {
		t.Fatalf("caller-owned Paths mutated: %v", req.Paths)
	}
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
		Overwrite: true,
	}
	sgctx := snapshotRestoreDummyStorageContext()
	prepared, err := prepareSnapshotRestoreRequest(req, sgctx)
	if err != nil {
		t.Fatalf("prepareSnapshotRestoreRequest: %v", err)
	}
	mutateSnapshotRestoreCallerInputs(&req, &after)
	assertPreparedSnapshotRestoreIdentityCopy(t, prepared)
	assertPreparedSnapshotRestoreQueryCopy(t, prepared.restoreSnapshotOpts.Query)
	if !prepared.restoreSnapshotOpts.Overwrite {
		t.Fatal("Overwrite was not preserved in lower snapshot restore options")
	}
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

func setupSnapshotSelectorParityEngineCase(t *testing.T) (*DefaultEngine, string, string) {
	t.Helper()

	dbconn := openSnapshotCreateEngineDB(t)
	baseID := "snap-selector-base"
	targetID := "snap-selector-target"
	if err := snapshot.InsertSnapshot(context.Background(), dbconn, snapshot.Snapshot{
		ID:        baseID,
		CreatedAt: time.Date(2026, 8, 25, 6, 0, 0, 0, time.UTC),
		Type:      "full",
	}); err != nil {
		t.Fatalf("insert empty base snapshot: %v", err)
	}

	root := t.TempDir()
	containerDir := t.TempDir()
	writer := container.NewLocalWriterWithDirAndDB(containerDir, container.GetContainerMaxSize(), dbconn)
	sgctx := storage.StorageContext{DB: dbconn, Writer: writer, ContainerDir: containerDir}
	storeSnapshotCreateCurrentFile(t, dbconn, sgctx, root, "docs/a.txt", "docs-content")
	storeSnapshotCreateCurrentFile(t, dbconn, sgctx, root, "docs-old/a.txt", "docs-old-content")
	storeSnapshotCreateCurrentFile(t, dbconn, sgctx, root, "docs2/a.txt", "docs2-content")
	mustCreateSnapshotResult(t, dbconn, snapshot.SnapshotCreateOptions{ID: targetID, Type: "full"})

	return newSnapshotRestoreEngine(t, dbconn, &sgctx), baseID, targetID
}

func snapshotShowStoredPaths(result SnapshotShowResult) []string {
	paths := make([]string, 0, len(result.Files))
	for _, file := range result.Files {
		paths = append(paths, file.StoredPath)
	}
	sort.Strings(paths)
	return paths
}

func snapshotDiffStoredPaths(result SnapshotDiffResult) []string {
	paths := make([]string, 0, len(result.Entries))
	for _, entry := range result.Entries {
		paths = append(paths, entry.StoredPath)
	}
	sort.Strings(paths)
	return paths
}

func snapshotRestoreStoredPaths(t *testing.T, root string, result SnapshotRestoreResult) []string {
	t.Helper()

	paths := make([]string, 0, len(result.OutputPaths))
	for _, outputPath := range result.OutputPaths {
		relative, err := filepath.Rel(root, outputPath)
		if err != nil {
			t.Fatalf("relative restore path for %q: %v", outputPath, err)
		}
		paths = append(paths, filepath.ToSlash(relative))
	}
	sort.Strings(paths)
	return paths
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
