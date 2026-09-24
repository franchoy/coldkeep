package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/snapshot"
	"github.com/franchoy/coldkeep/internal/storage"
)

const phase3UnrepresentableMemberInvariant = "SNAPSHOT_MEMBER_PATH_UNREPRESENTABLE"

func phase3SetSelectionBase(req *SnapshotCreateRequest, base string) bool {
	value := reflect.ValueOf(req).Elem()
	field := value.FieldByName("SelectionBase")
	if !field.IsValid() || !field.CanSet() || field.Kind() != reflect.String {
		return false
	}
	field.SetString(base)
	return true
}

func phase3SnapshotCreateRequest(id, base string, paths ...string) SnapshotCreateRequest {
	req := SnapshotCreateRequest{ID: id, Paths: paths}
	phase3SetSelectionBase(&req, base)
	return req
}

func phase3StoreRealSource(t *testing.T, sgctx storage.StorageContext, sourcePath, content string) int64 {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(sourcePath), 0o755); err != nil {
		t.Fatalf("create real Store source parent: %v", err)
	}
	if err := os.WriteFile(sourcePath, []byte(content), 0o600); err != nil {
		t.Fatalf("write real Store source: %v", err)
	}
	result, err := storage.StoreFileWithStorageContextAndCodecResult(sgctx, sourcePath, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("real Store operation: %v", err)
	}

	var storedPath string
	if err := sgctx.DB.QueryRow(`SELECT path FROM physical_file WHERE logical_file_id = ?`, result.FileID).Scan(&storedPath); err != nil {
		t.Fatalf("read Store-produced physical path: %v", err)
	}
	wantPath, err := filepath.Abs(sourcePath)
	if err != nil {
		t.Fatalf("resolve expected physical path: %v", err)
	}
	wantPath = filepath.Clean(wantPath)
	if !filepath.IsAbs(storedPath) || storedPath != wantPath {
		t.Fatalf("real Store boundary was not preserved: physical_file.path=%q want canonical absolute %q", storedPath, wantPath)
	}
	return result.FileID
}

func phase3NewCreateFixture(t *testing.T) (*DefaultEngine, storage.StorageContext, string) {
	t.Helper()
	dbconn := openSnapshotCreateEngineDB(t)
	containerDir := t.TempDir()
	sgctx := storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewSimulatedWriter(container.GetContainerMaxSize()),
		ContainerDir: containerDir,
	}
	eng := newSnapshotCreateEngine(t, dbconn)
	return eng, sgctx, t.TempDir()
}

func phase3SnapshotMembers(t *testing.T, eng *DefaultEngine, snapshotID string) []string {
	t.Helper()
	result, err := eng.SnapshotShow(context.Background(), SnapshotShowRequest{SnapshotID: snapshotID})
	if err != nil {
		t.Fatalf("show snapshot %q: %v", snapshotID, err)
	}
	members := make([]string, 0, len(result.Files))
	for _, file := range result.Files {
		members = append(members, file.StoredPath)
	}
	return members
}

func phase3TableCount(t *testing.T, eng *DefaultEngine, query string, args ...any) int {
	t.Helper()
	var count int
	if err := eng.config.DB.QueryRow(query, args...).Scan(&count); err != nil {
		t.Fatalf("count rollback state: %v", err)
	}
	return count
}

func phase3AssertCreateRollback(t *testing.T, eng *DefaultEngine, snapshotID string, initialPathRows int) {
	t.Helper()
	if snapshots := phase3TableCount(t, eng, `SELECT COUNT(*) FROM snapshot WHERE id = ?`, snapshotID); snapshots != 0 {
		t.Fatalf("snapshot rollback left %d snapshot rows for %q", snapshots, snapshotID)
	}
	if members := phase3TableCount(t, eng, `SELECT COUNT(*) FROM snapshot_file WHERE snapshot_id = ?`, snapshotID); members != 0 {
		t.Fatalf("snapshot rollback left %d membership rows for %q", members, snapshotID)
	}
	if paths := phase3TableCount(t, eng, `SELECT COUNT(*) FROM snapshot_path`); paths != initialPathRows {
		t.Fatalf("snapshot rollback changed snapshot_path rows: got %d want %d", paths, initialPathRows)
	}
}

func TestPhase3R1SnapshotCreateOwnsSelectionBase(t *testing.T) {
	field, ok := reflect.TypeOf(SnapshotCreateRequest{}).FieldByName("SelectionBase")
	if !ok || field.Type.Kind() != reflect.String {
		t.Fatalf("CK-V11316-001: expected frozen behavior: SnapshotCreateRequest owns string SelectionBase; observed baseline behavior: field absent or non-string (present=%t)", ok)
	}
}

func TestPhase3R2RealStoreRelativeExactSelector(t *testing.T) {
	eng, sgctx, base := phase3NewCreateFixture(t)
	source := filepath.Join(base, "docs", "a.txt")
	phase3StoreRealSource(t, sgctx, source, "phase3-r2")

	result, err := eng.SnapshotCreate(context.Background(), phase3SnapshotCreateRequest("phase3-r2", base, "docs/a.txt"))
	if err != nil {
		t.Fatalf("CK-V11316-001: expected frozen behavior: real Store source selected as docs/a.txt and snapshot create succeeds; observed baseline behavior: %v", err)
	}
	members := phase3SnapshotMembers(t, eng, result.SnapshotID)
	if result.FilesInserted != 1 || !reflect.DeepEqual(members, []string{"docs/a.txt"}) {
		t.Fatalf("CK-V11316-001: expected frozen behavior: one member docs/a.txt; observed baseline behavior: files=%d members=%v", result.FilesInserted, members)
	}
}

func TestPhase3R3RealStoreRelativePrefixSelector(t *testing.T) {
	eng, sgctx, base := phase3NewCreateFixture(t)
	phase3StoreRealSource(t, sgctx, filepath.Join(base, "docs", "a.txt"), "phase3-r3")

	result, err := eng.SnapshotCreate(context.Background(), phase3SnapshotCreateRequest("phase3-r3", base, "docs/"))
	if err != nil {
		t.Fatalf("CK-V11316-001: expected frozen behavior: docs/ selects the real Store source; observed baseline behavior: %v", err)
	}
	members := phase3SnapshotMembers(t, eng, result.SnapshotID)
	if result.FilesInserted != 1 || !reflect.DeepEqual(members, []string{"docs/a.txt"}) {
		t.Fatalf("CK-V11316-001: expected frozen behavior: prefix selects one member docs/a.txt; observed baseline behavior: files=%d members=%v", result.FilesInserted, members)
	}
}

func TestPhase3R4MissingPrefixRollsBack(t *testing.T) {
	eng, sgctx, base := phase3NewCreateFixture(t)
	phase3StoreRealSource(t, sgctx, filepath.Join(base, "docs", "a.txt"), "phase3-r4")
	initialPathRows := phase3TableCount(t, eng, `SELECT COUNT(*) FROM snapshot_path`)

	_, err := eng.SnapshotCreate(context.Background(), phase3SnapshotCreateRequest("phase3-r4", base, "missing/"))
	if err == nil {
		t.Fatalf("CK-V11316-002: expected frozen behavior: unmatched prefix returns typed not_found and rolls back; observed baseline behavior: create succeeded with snapshot=%t members=%d", snapshotExists(t, eng.config.DB, "phase3-r4"), snapshotMembershipCount(t, eng.config.DB, "phase3-r4"))
	}
	if !IsCode(err, ErrorNotFound) {
		t.Fatalf("CK-V11316-002: expected frozen behavior: unmatched prefix returns typed not_found; observed baseline behavior: code=%q err=%v", CodeOf(err), err)
	}
	phase3AssertCreateRollback(t, eng, "phase3-r4", initialPathRows)
}

func TestPhase3R5MixedMatchAndMissingPrefixRollsBack(t *testing.T) {
	eng, sgctx, base := phase3NewCreateFixture(t)
	source := filepath.Join(base, "docs", "a.txt")
	phase3StoreRealSource(t, sgctx, source, "phase3-r5")
	matching := strings.TrimPrefix(filepath.ToSlash(filepath.Clean(source)), "/")
	initialPathRows := phase3TableCount(t, eng, `SELECT COUNT(*) FROM snapshot_path`)

	_, err := eng.SnapshotCreate(context.Background(), phase3SnapshotCreateRequest("phase3-r5", string(filepath.Separator), matching, "missing/"))
	if err == nil {
		t.Fatalf("CK-V11316-002: expected frozen behavior: matching selector plus missing/ fails atomically; observed baseline behavior: create succeeded with snapshot=%t members=%d", snapshotExists(t, eng.config.DB, "phase3-r5"), snapshotMembershipCount(t, eng.config.DB, "phase3-r5"))
	}
	if !IsCode(err, ErrorNotFound) {
		t.Fatalf("CK-V11316-002: expected frozen behavior: mixed selector miss returns typed not_found; observed baseline behavior: code=%q err=%v", CodeOf(err), err)
	}
	phase3AssertCreateRollback(t, eng, "phase3-r5", initialPathRows)
}

func TestPhase3R6FullSnapshotUsesRelativeMemberIdentity(t *testing.T) {
	eng, sgctx, base := phase3NewCreateFixture(t)
	phase3StoreRealSource(t, sgctx, filepath.Join(base, "docs", "a.txt"), "phase3-r6")

	result, err := eng.SnapshotCreate(context.Background(), phase3SnapshotCreateRequest("phase3-r6", base))
	if err != nil {
		t.Fatalf("CK-V11316-001: expected frozen behavior: in-root full snapshot succeeds; observed baseline behavior: %v", err)
	}
	members := phase3SnapshotMembers(t, eng, result.SnapshotID)
	if !reflect.DeepEqual(members, []string{"docs/a.txt"}) {
		t.Fatalf("CK-V11316-001: expected frozen behavior: full snapshot member is docs/a.txt relative to SelectionBase; observed baseline behavior: members=%v", members)
	}
}

func TestPhase3R7FullSnapshotOutsideRootConflictsAndRollsBack(t *testing.T) {
	eng, sgctx, root := phase3NewCreateFixture(t)
	base := filepath.Join(root, "capture")
	phase3StoreRealSource(t, sgctx, filepath.Join(base, "docs", "a.txt"), "phase3-r7-in")
	phase3StoreRealSource(t, sgctx, filepath.Join(root, "outside", "b.txt"), "phase3-r7-out")
	initialPathRows := phase3TableCount(t, eng, `SELECT COUNT(*) FROM snapshot_path`)

	_, err := eng.SnapshotCreate(context.Background(), phase3SnapshotCreateRequest("phase3-r7", base))
	if err == nil {
		t.Fatalf("CK-V11316-001: expected frozen behavior: outside-root full source returns conflict and rolls back all rows; observed baseline behavior: create succeeded with members=%v", phase3SnapshotMembers(t, eng, "phase3-r7"))
	}
	if !IsCode(err, ErrorConflict) {
		t.Fatalf("CK-V11316-001: expected frozen behavior: outside-root full source returns typed conflict; observed baseline behavior: code=%q err=%v", CodeOf(err), err)
	}
	phase3AssertCreateRollback(t, eng, "phase3-r7", initialPathRows)
}

func TestPhase3R8POSIXLiteralBackslashIsUnrepresentable(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX literal-backslash filename contract")
	}
	eng, sgctx, base := phase3NewCreateFixture(t)
	source := filepath.Join(base, `a\b.txt`)
	if filepath.Base(source) != `a\b.txt` {
		t.Fatalf("fixture does not contain one literal-backslash filename component: %q", filepath.Base(source))
	}
	phase3StoreRealSource(t, sgctx, source, "phase3-r8")
	initialPathRows := phase3TableCount(t, eng, `SELECT COUNT(*) FROM snapshot_path`)

	_, err := eng.SnapshotCreate(context.Background(), phase3SnapshotCreateRequest("phase3-r8", base))
	if err == nil {
		t.Fatalf("CK-V11316-001: expected frozen behavior: literal-backslash source returns invariant_violation/%s and rolls back; observed baseline behavior: create succeeded with members=%v", phase3UnrepresentableMemberInvariant, phase3SnapshotMembers(t, eng, "phase3-r8"))
	}
	var typed *Error
	if !errors.As(err, &typed) || typed.Code != ErrorInvariantViolation || typed.InvariantCode != phase3UnrepresentableMemberInvariant {
		t.Fatalf("CK-V11316-001: expected frozen behavior: invariant_violation/%s; observed baseline behavior: code=%q err=%v", phase3UnrepresentableMemberInvariant, CodeOf(err), err)
	}
	phase3AssertCreateRollback(t, eng, "phase3-r8", initialPathRows)
}

func TestPhase3R9AbsoluteCreateSelectorRemainsInvalid(t *testing.T) {
	selectors := []struct {
		name     string
		selector string
	}{
		{name: "host-native", selector: filepath.Join(string(filepath.Separator), "absolute", "a.txt")},
		{name: "windows-drive", selector: `C:\repo\docs\a.txt`},
		{name: "windows-unc", selector: `\\server\share\repo\docs\a.txt`},
	}
	for _, testCase := range selectors {
		t.Run(testCase.name, func(t *testing.T) {
			eng, _, base := phase3NewCreateFixture(t)
			_, err := eng.SnapshotCreate(context.Background(), phase3SnapshotCreateRequest("phase3-r9", base, testCase.selector))
			if !IsCode(err, ErrorInvalidArgument) {
				t.Fatalf("absolute create selector compatibility guard for %q: expected invalid_argument, got code=%q err=%v", testCase.selector, CodeOf(err), err)
			}
			if snapshotExists(t, eng.config.DB, "phase3-r9") {
				t.Fatalf("absolute create selector compatibility guard for %q committed a snapshot", testCase.selector)
			}
		})
	}
}

func TestPhase3R10DotAliasesRemainInvalid(t *testing.T) {
	for _, selector := range []string{".", "./"} {
		t.Run(fmt.Sprintf("selector_%q", selector), func(t *testing.T) {
			eng, _, base := phase3NewCreateFixture(t)
			_, err := eng.SnapshotCreate(context.Background(), phase3SnapshotCreateRequest("phase3-r10", base, selector))
			if !IsCode(err, ErrorInvalidArgument) {
				t.Fatalf("dot create selector compatibility guard for %q: expected invalid_argument, got code=%q err=%v", selector, CodeOf(err), err)
			}
			if snapshotExists(t, eng.config.DB, "phase3-r10") {
				t.Fatalf("dot create selector compatibility guard for %q committed a snapshot", selector)
			}
		})
	}
}

func TestPhase3R12LegacyRelativeMemberReadDiffRestore(t *testing.T) {
	dbconn, targetID, sgctx := setupSnapshotRestoreEngineCase(t)
	eng := newSnapshotRestoreEngine(t, dbconn, &sgctx)
	baseID := "phase3-r12-empty-base"
	if err := snapshot.InsertSnapshot(context.Background(), dbconn, snapshot.Snapshot{
		ID: baseID, CreatedAt: time.Now().UTC(), Type: "full",
	}); err != nil {
		t.Fatalf("insert legacy read-contract base snapshot: %v", err)
	}

	show, err := eng.SnapshotShow(context.Background(), SnapshotShowRequest{
		SnapshotID: targetID,
		Query:      SnapshotQuery{Paths: []string{"docs/a.txt"}, Prefixes: []string{"docs/"}},
	})
	if err != nil || !reflect.DeepEqual(snapshotShowStoredPaths(show), []string{"docs/a.txt"}) {
		t.Fatalf("legacy read contract: show/query got paths=%v err=%v", snapshotShowStoredPaths(show), err)
	}
	diff, err := eng.SnapshotDiff(context.Background(), SnapshotDiffRequest{BaseID: baseID, TargetID: targetID})
	if err != nil || !reflect.DeepEqual(snapshotDiffStoredPaths(diff), []string{"docs/a.txt"}) {
		t.Fatalf("legacy read contract: diff got paths=%v err=%v", snapshotDiffStoredPaths(diff), err)
	}

	outputRoot := t.TempDir()
	restored, err := eng.SnapshotRestore(context.Background(), SnapshotRestoreRequest{
		SnapshotID: targetID,
		Selection:  SnapshotRestoreSelection{ExactPaths: []string{"docs/a.txt"}},
		Destination: SnapshotRestoreDestination{
			Mode: SnapshotRestoreDestinationOriginal,
			Path: outputRoot,
		},
		Overwrite: true,
	})
	if err != nil || !reflect.DeepEqual(snapshotRestoreStoredPaths(t, outputRoot, restored), []string{"docs/a.txt"}) {
		t.Fatalf("legacy read contract: restore got paths=%v err=%v", snapshotRestoreStoredPaths(t, outputRoot, restored), err)
	}
}

func TestPhase3R13ReadQueryRestoreCWDIndependence(t *testing.T) {
	const marker = "member=docs/a.txt\nrestore=docs/a.txt\n"
	for _, name := range []string{"cwd-a", "cwd-b"} {
		t.Run(name, func(t *testing.T) {
			workingDir := filepath.Join(t.TempDir(), name)
			if err := os.MkdirAll(workingDir, 0o755); err != nil {
				t.Fatalf("create subprocess working directory: %v", err)
			}
			resultPath := filepath.Join(t.TempDir(), "result.txt")
			cmd := exec.Command(os.Args[0], "-test.run=^TestPhase3R13ReadQueryRestoreCWDChild$")
			cmd.Dir = workingDir
			cmd.Env = append(os.Environ(),
				"COLDKEEP_PHASE3_R13_CHILD=1",
				"COLDKEEP_PHASE3_R13_RESULT="+resultPath,
			)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("CWD-independent child in %q failed: %v\n%s", workingDir, err, output)
			}
			got, err := os.ReadFile(resultPath)
			if err != nil {
				t.Fatalf("read CWD-independent child result: %v", err)
			}
			if string(got) != marker {
				t.Fatalf("CWD-independent read/query/restore in %q returned %q, want %q", workingDir, got, marker)
			}
		})
	}
}

func TestPhase3R13ReadQueryRestoreCWDChild(t *testing.T) {
	if os.Getenv("COLDKEEP_PHASE3_R13_CHILD") != "1" {
		t.Skip("R13 subprocess helper")
	}
	resultPath := os.Getenv("COLDKEEP_PHASE3_R13_RESULT")
	if !filepath.IsAbs(resultPath) {
		t.Fatalf("R13 subprocess result path must be absolute: %q", resultPath)
	}

	dbconn, snapshotID, sgctx := setupSnapshotRestoreEngineCase(t)
	eng := newSnapshotRestoreEngine(t, dbconn, &sgctx)
	show, err := eng.SnapshotShow(context.Background(), SnapshotShowRequest{
		SnapshotID: snapshotID,
		Query:      SnapshotQuery{Paths: []string{"docs/a.txt"}},
	})
	if err != nil || !reflect.DeepEqual(snapshotShowStoredPaths(show), []string{"docs/a.txt"}) {
		t.Fatalf("R13 child show/query paths=%v err=%v", snapshotShowStoredPaths(show), err)
	}
	outputRoot := t.TempDir()
	restored, err := eng.SnapshotRestore(context.Background(), SnapshotRestoreRequest{
		SnapshotID: snapshotID,
		Selection:  SnapshotRestoreSelection{ExactPaths: []string{"docs/a.txt"}},
		Destination: SnapshotRestoreDestination{
			Mode: SnapshotRestoreDestinationOriginal,
			Path: outputRoot,
		},
		Overwrite: true,
	})
	if err != nil {
		t.Fatalf("R13 child restore: %v", err)
	}
	restorePaths := snapshotRestoreStoredPaths(t, outputRoot, restored)
	if !reflect.DeepEqual(restorePaths, []string{"docs/a.txt"}) {
		t.Fatalf("R13 child restore paths=%v", restorePaths)
	}
	marker := "member=" + show.Files[0].StoredPath + "\nrestore=" + restorePaths[0] + "\n"
	if err := os.WriteFile(resultPath, []byte(marker), 0o600); err != nil {
		t.Fatalf("write R13 subprocess result: %v", err)
	}
}
