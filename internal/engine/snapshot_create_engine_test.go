package engine

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	idb "github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/storage"

	_ "github.com/mattn/go-sqlite3"
)

var snapshotCreateStoreCWDMu sync.Mutex

func openSnapshotCreateEngineDB(t *testing.T) *sql.DB {
	t.Helper()

	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })
	if err := idb.RunMigrations(dbconn); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}
	return dbconn
}

func newSnapshotCreateEngine(t *testing.T, dbconn *sql.DB) *DefaultEngine {
	t.Helper()

	eng, err := New(Config{DB: dbconn})
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	return eng
}

func setSnapshotCreateIDGenerator(t *testing.T, eng *DefaultEngine, generator func() (string, error)) {
	t.Helper()
	eng.snapshotIDGenerator = generator
}

func newSnapshotCreateStorageContext(containerDir string, dbconn *sql.DB) storage.StorageContext {
	return storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewSimulatedWriter(1024 * 1024),
		ContainerDir: containerDir,
	}
}

func storeSnapshotCreateEngineFile(t *testing.T, sgctx storage.StorageContext, root, storedPath string, content string) int64 {
	t.Helper()

	inputPath := filepath.Join(root, filepath.FromSlash(storedPath))
	ensureSnapshotCreateInputDir(t, filepath.Dir(inputPath))
	if err := os.WriteFile(inputPath, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile %s: %v", storedPath, err)
	}

	snapshotCreateStoreCWDMu.Lock()
	defer snapshotCreateStoreCWDMu.Unlock()

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	if err := os.Chdir(root); err != nil {
		t.Fatalf("Chdir %s: %v", root, err)
	}
	defer func() {
		if chdirErr := os.Chdir(cwd); chdirErr != nil {
			t.Fatalf("restore cwd %s: %v", cwd, chdirErr)
		}
	}()

	result, err := storage.StoreFileWithStorageContextAndCodecResult(sgctx, filepath.ToSlash(storedPath), blocks.CodecPlain)
	if err != nil {
		t.Fatalf("store fixture %s: %v", storedPath, err)
	}
	return result.FileID
}

func ensureSnapshotCreateInputDir(t *testing.T, dir string) {
	t.Helper()

	if info, err := os.Stat(dir); err == nil {
		if !info.IsDir() {
			t.Fatalf("input parent %s is not a directory", dir)
		}
		return
	}

	parent := filepath.Dir(dir)
	if parent != dir {
		ensureSnapshotCreateInputDir(t, parent)
	}

	tempDir, err := os.MkdirTemp(parent, ".snapshot-create-input-*")
	if err != nil {
		t.Fatalf("MkdirTemp %s: %v", dir, err)
	}
	if err := os.Rename(tempDir, dir); err != nil {
		_ = os.Remove(tempDir)
		t.Fatalf("rename temp input dir %s: %v", dir, err)
	}
}

func snapshotMembershipCount(t *testing.T, dbconn *sql.DB, snapshotID string) int {
	t.Helper()

	var count int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM snapshot_file WHERE snapshot_id = ?`, snapshotID).Scan(&count); err != nil {
		t.Fatalf("count snapshot membership %s: %v", snapshotID, err)
	}
	return count
}

func snapshotExists(t *testing.T, dbconn *sql.DB, snapshotID string) bool {
	t.Helper()

	var count int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM snapshot WHERE id = ?`, snapshotID).Scan(&count); err != nil {
		t.Fatalf("count snapshot rows %s: %v", snapshotID, err)
	}
	return count == 1
}

func snapshotParentID(t *testing.T, dbconn *sql.DB, snapshotID string) sql.NullString {
	t.Helper()

	var parentID sql.NullString
	if err := dbconn.QueryRow(`SELECT parent_id FROM snapshot WHERE id = ?`, snapshotID).Scan(&parentID); err != nil {
		t.Fatalf("query parent_id for %s: %v", snapshotID, err)
	}
	return parentID
}

func insertSnapshotRow(t *testing.T, dbconn *sql.DB, snapshotID, snapshotType string) {
	t.Helper()

	_, err := dbconn.Exec(
		`INSERT INTO snapshot (id, created_at, type) VALUES (?, ?, ?)`,
		snapshotID, time.Now().UTC().Format(time.RFC3339), snapshotType,
	)
	if err != nil {
		t.Fatalf("insert snapshot %s: %v", snapshotID, err)
	}
}

func seedSnapshotCreateEngineFiles(t *testing.T, dbconn *sql.DB) string {
	t.Helper()

	root := t.TempDir()
	sgctx := newSnapshotCreateStorageContext(t.TempDir(), dbconn)
	storeSnapshotCreateCurrentFile(t, dbconn, sgctx, root, "docs/a.txt", "snapshot-create-a")
	storeSnapshotCreateCurrentFile(t, dbconn, sgctx, root, "docs/sub/b.txt", "snapshot-create-b-content")
	storeSnapshotCreateCurrentFile(t, dbconn, sgctx, root, "img/c.png", "snapshot-create-image-payload")
	return root
}

func storeSnapshotCreateCurrentFile(
	t *testing.T,
	dbconn *sql.DB,
	sgctx storage.StorageContext,
	root string,
	storedPath string,
	content string,
) {
	t.Helper()

	fileID := storeSnapshotCreateEngineFile(t, sgctx, root, storedPath, content)
	updateSnapshotCreateStoredPathMapping(t, dbconn, fileID, filepath.Join(root, filepath.FromSlash(storedPath)))
}

func snapshotCreateRequestWithBase(base string, req SnapshotCreateRequest) SnapshotCreateRequest {
	req.SelectionBase = base
	return req
}

func updateSnapshotCreateStoredPathMapping(t *testing.T, dbconn *sql.DB, fileID int64, storedPath string) {
	t.Helper()

	if _, err := dbconn.Exec(`UPDATE physical_file SET path = $1 WHERE logical_file_id = $2`, storedPath, fileID); err != nil {
		t.Fatalf("update stored path mapping: %v", err)
	}
}

func TestSnapshotCreateFullAndPartialRouteThroughEngine(t *testing.T) {
	t.Run("full", func(t *testing.T) {
		dbconn := openSnapshotCreateEngineDB(t)
		base := seedSnapshotCreateEngineFiles(t, dbconn)
		eng := newSnapshotCreateEngine(t, dbconn)

		result, err := eng.SnapshotCreate(context.Background(), snapshotCreateRequestWithBase(base, SnapshotCreateRequest{ID: "snap-full-engine"}))
		if err != nil {
			t.Fatalf("SnapshotCreate full: %v", err)
		}
		assertSnapshotCreateEngineResult(t, result, "snap-full-engine", SnapshotTypeFull, 0, 3, "", "")
		if !snapshotExists(t, dbconn, "snap-full-engine") || snapshotMembershipCount(t, dbconn, "snap-full-engine") != 3 {
			t.Fatal("expected full snapshot rows to be committed")
		}
	})

	t.Run("partial", func(t *testing.T) {
		dbconn := openSnapshotCreateEngineDB(t)
		base := seedSnapshotCreateEngineFiles(t, dbconn)
		eng := newSnapshotCreateEngine(t, dbconn)

		req := snapshotCreateRequestWithBase(base, SnapshotCreateRequest{ID: "snap-partial-engine", Paths: []string{"docs/", "docs/a.txt", "img/c.png"}})
		result, err := eng.SnapshotCreate(context.Background(), req)
		if err != nil {
			t.Fatalf("SnapshotCreate partial: %v", err)
		}
		assertSnapshotCreateEngineResult(t, result, "snap-partial-engine", SnapshotTypePartial, 3, 3, "", "")
		if !snapshotExists(t, dbconn, "snap-partial-engine") || snapshotMembershipCount(t, dbconn, "snap-partial-engine") != 3 {
			t.Fatal("expected partial snapshot rows to be committed")
		}
	})
}

func TestSnapshotCreateEmptyAndParentedRouteThroughEngine(t *testing.T) {
	t.Run("empty full", func(t *testing.T) {
		dbconn := openSnapshotCreateEngineDB(t)
		eng := newSnapshotCreateEngine(t, dbconn)
		base := t.TempDir()

		result, err := eng.SnapshotCreate(context.Background(), snapshotCreateRequestWithBase(base, SnapshotCreateRequest{ID: "snap-empty-engine"}))
		if err != nil {
			t.Fatalf("SnapshotCreate empty full: %v", err)
		}
		assertSnapshotCreateEngineResult(t, result, "snap-empty-engine", SnapshotTypeFull, 0, 0, "", "")
		if !snapshotExists(t, dbconn, "snap-empty-engine") || snapshotMembershipCount(t, dbconn, "snap-empty-engine") != 0 {
			t.Fatal("expected empty full snapshot row without membership rows")
		}
	})

	t.Run("parented full", func(t *testing.T) {
		dbconn := openSnapshotCreateEngineDB(t)
		base := seedSnapshotCreateEngineFiles(t, dbconn)
		insertSnapshotRow(t, dbconn, "snap-parent-engine", "full")
		eng := newSnapshotCreateEngine(t, dbconn)

		result, err := eng.SnapshotCreate(context.Background(), SnapshotCreateRequest{
			ID:            "snap-child-engine",
			Label:         "  child label  ",
			ParentID:      "  snap-parent-engine  ",
			SelectionBase: base,
		})
		if err != nil {
			t.Fatalf("SnapshotCreate parented full: %v", err)
		}
		assertSnapshotCreateEngineResult(t, result, "snap-child-engine", SnapshotTypeFull, 0, 3, "child label", "snap-parent-engine")
		parentID := snapshotParentID(t, dbconn, "snap-child-engine")
		if !parentID.Valid || parentID.String != "snap-parent-engine" {
			t.Fatalf("stored parent_id mismatch: %+v", parentID)
		}
		if snapshotMembershipCount(t, dbconn, "snap-child-engine") != 3 {
			t.Fatal("expected parented full snapshot membership rows to be committed")
		}
	})
}

func TestSnapshotCreateGeneratedIDBehavior(t *testing.T) {
	t.Run("generated id is returned and committed", func(t *testing.T) {
		dbconn := openSnapshotCreateEngineDB(t)
		eng := newSnapshotCreateEngine(t, dbconn)
		base := t.TempDir()
		setSnapshotCreateIDGenerator(t, eng, func() (string, error) { return " snap-generated-01 ", nil })

		result, err := eng.SnapshotCreate(context.Background(), snapshotCreateRequestWithBase(base, SnapshotCreateRequest{}))
		if err != nil {
			t.Fatalf("SnapshotCreate generated id: %v", err)
		}
		assertSnapshotCreateEngineResult(t, result, "snap-generated-01", SnapshotTypeFull, 0, 0, "", "")
		if !snapshotExists(t, dbconn, "snap-generated-01") {
			t.Fatal("expected generated snapshot id to be committed")
		}
	})

	t.Run("default generator keeps format", func(t *testing.T) {
		dbconn := openSnapshotCreateEngineDB(t)
		eng := newSnapshotCreateEngine(t, dbconn)
		base := t.TempDir()

		result, err := eng.SnapshotCreate(context.Background(), snapshotCreateRequestWithBase(base, SnapshotCreateRequest{}))
		if err != nil {
			t.Fatalf("SnapshotCreate default generated id: %v", err)
		}
		generatedID := result.SnapshotID
		if !regexp.MustCompile(`^snap-[0-9a-f]{16}$`).MatchString(generatedID) {
			t.Fatalf("generated id format drifted: %q", generatedID)
		}
	})

	t.Run("generator errors and empty results fail before mutation", func(t *testing.T) {
		dbconn := openSnapshotCreateEngineDB(t)
		eng := newSnapshotCreateEngine(t, dbconn)
		base := t.TempDir()
		setSnapshotCreateIDGenerator(t, eng, func() (string, error) { return "", errors.New("generate snapshot id entropy: boom") })

		_, err := eng.SnapshotCreate(context.Background(), snapshotCreateRequestWithBase(base, SnapshotCreateRequest{}))
		if err == nil || !strings.Contains(err.Error(), "generate snapshot id entropy: boom") {
			t.Fatalf("expected generator error, got %v", err)
		}
		if snapshotExists(t, dbconn, "snap-generated-01") {
			t.Fatal("unexpected snapshot row after generator error")
		}

		setSnapshotCreateIDGenerator(t, eng, func() (string, error) { return "   ", nil })
		_, err = eng.SnapshotCreate(context.Background(), snapshotCreateRequestWithBase(base, SnapshotCreateRequest{}))
		if err == nil || !strings.Contains(err.Error(), "generated snapshot id cannot be empty") {
			t.Fatalf("expected whitespace-only generator rejection, got %v", err)
		}
	})
}

func TestSnapshotCreateFailureAndImmutabilityBehavior(t *testing.T) {
	t.Run("request failures return zero result and leave no rows", func(t *testing.T) {
		dbconn := openSnapshotCreateEngineDB(t)
		base := seedSnapshotCreateEngineFiles(t, dbconn)
		eng := newSnapshotCreateEngine(t, dbconn)

		result, err := eng.SnapshotCreate(context.Background(), SnapshotCreateRequest{
			ID:            "snap-fail-engine",
			ParentID:      "snap-parent",
			SelectionBase: base,
			Paths:         []string{"docs/"},
		})
		if err == nil || !strings.Contains(err.Error(), "--from is currently supported only for full snapshots") {
			t.Fatalf("expected parent-plus-paths rejection, got result=%+v err=%v", result, err)
		}
		if result != (SnapshotCreateResult{}) {
			t.Fatalf("expected zero result on failure, got %+v", result)
		}
		if snapshotExists(t, dbconn, "snap-fail-engine") {
			t.Fatal("unexpected committed snapshot after request failure")
		}
	})

	t.Run("caller path slice is copied", func(t *testing.T) {
		dbconn := openSnapshotCreateEngineDB(t)
		base := seedSnapshotCreateEngineFiles(t, dbconn)
		eng := newSnapshotCreateEngine(t, dbconn)

		paths := []string{"docs/"}
		result, err := eng.SnapshotCreate(context.Background(), snapshotCreateRequestWithBase(base, SnapshotCreateRequest{ID: "snap-copy-engine", Paths: paths}))
		paths[0] = "img/"
		if err != nil {
			t.Fatalf("SnapshotCreate copy test: %v", err)
		}
		assertSnapshotCreateEngineResult(t, result, "snap-copy-engine", SnapshotTypePartial, 1, 2, "", "")
		if snapshotMembershipCount(t, dbconn, "snap-copy-engine") != 2 {
			t.Fatal("expected copied-path snapshot membership rows to be committed")
		}
	})
}

func TestSnapshotCreateGeneratedDuplicateIDIsNotRetried(t *testing.T) {
	dbconn := openSnapshotCreateEngineDB(t)
	insertSnapshotRow(t, dbconn, "snap-duplicate-engine", "full")
	eng := newSnapshotCreateEngine(t, dbconn)
	base := t.TempDir()

	calls := 0
	setSnapshotCreateIDGenerator(t, eng, func() (string, error) {
		calls++
		return "snap-duplicate-engine", nil
	})

	_, err := eng.SnapshotCreate(context.Background(), snapshotCreateRequestWithBase(base, SnapshotCreateRequest{}))
	if err == nil {
		t.Fatal("expected duplicate snapshot id error")
	}
	if calls != 1 {
		t.Fatalf("expected exactly one generator call, got %d", calls)
	}
}

func TestSnapshotCreateGeneratorIsolationAcrossEngines(t *testing.T) {
	dbA := openSnapshotCreateEngineDB(t)
	dbB := openSnapshotCreateEngineDB(t)
	engA := newSnapshotCreateEngine(t, dbA)
	engB := newSnapshotCreateEngine(t, dbB)
	baseA := t.TempDir()
	baseB := t.TempDir()
	setSnapshotCreateIDGenerator(t, engA, func() (string, error) { return "snap-engine-a", nil })
	setSnapshotCreateIDGenerator(t, engB, func() (string, error) { return "snap-engine-b", nil })

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, err := engA.SnapshotCreate(context.Background(), snapshotCreateRequestWithBase(baseA, SnapshotCreateRequest{}))
		errs <- err
	}()
	go func() {
		defer wg.Done()
		_, err := engB.SnapshotCreate(context.Background(), snapshotCreateRequestWithBase(baseB, SnapshotCreateRequest{}))
		errs <- err
	}()
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent SnapshotCreate error: %v", err)
		}
	}
	if !snapshotExists(t, dbA, "snap-engine-a") || !snapshotExists(t, dbB, "snap-engine-b") {
		t.Fatal("expected separate engines to commit their own generated IDs")
	}
}

func TestSnapshotCreateSuppliedIDAndMissingPrefixBehavior(t *testing.T) {
	t.Run("supplied id is trimmed and whitespace label is absent", func(t *testing.T) {
		dbconn := openSnapshotCreateEngineDB(t)
		base := seedSnapshotCreateEngineFiles(t, dbconn)
		eng := newSnapshotCreateEngine(t, dbconn)

		result, err := eng.SnapshotCreate(context.Background(), SnapshotCreateRequest{
			ID:            "  snap-trimmed-engine  ",
			Label:         "   ",
			SelectionBase: base,
			Paths:         []string{"docs/a.txt"},
		})
		if err != nil {
			t.Fatalf("SnapshotCreate trimmed ID: %v", err)
		}
		assertSnapshotCreateEngineResult(t, result, "snap-trimmed-engine", SnapshotTypePartial, 1, 1, "", "")
	})

	t.Run("missing directory prefix rolls back", func(t *testing.T) {
		dbconn := openSnapshotCreateEngineDB(t)
		base := seedSnapshotCreateEngineFiles(t, dbconn)
		eng := newSnapshotCreateEngine(t, dbconn)
		initialPathRows := phase3TableCount(t, eng, `SELECT COUNT(*) FROM snapshot_path`)

		result, err := eng.SnapshotCreate(context.Background(), SnapshotCreateRequest{
			ID:            "snap-empty-prefix-engine",
			SelectionBase: base,
			Paths:         []string{"missing/"},
		})
		if !IsCode(err, ErrorNotFound) {
			t.Fatalf("SnapshotCreate missing prefix: expected not_found, got result=%+v code=%q err=%v", result, CodeOf(err), err)
		}
		if result != (SnapshotCreateResult{}) {
			t.Fatalf("SnapshotCreate missing prefix returned nonzero result: %+v", result)
		}
		phase3AssertCreateRollback(t, eng, "snap-empty-prefix-engine", initialPathRows)
	})
}

func TestSnapshotCreateValidationAndRollbackCases(t *testing.T) {
	t.Run("invalid path fails before mutation", func(t *testing.T) {
		dbconn := openSnapshotCreateEngineDB(t)
		base := seedSnapshotCreateEngineFiles(t, dbconn)
		eng := newSnapshotCreateEngine(t, dbconn)

		assertSnapshotCreateFailure(t, dbconn, eng, SnapshotCreateRequest{
			ID:            "snap-invalid-path-engine",
			SelectionBase: base,
			Paths:         []string{"/absolute/invalid"},
		}, "snapshot path")
	})

	t.Run("missing exact path rolls back", func(t *testing.T) {
		dbconn := openSnapshotCreateEngineDB(t)
		base := seedSnapshotCreateEngineFiles(t, dbconn)
		eng := newSnapshotCreateEngine(t, dbconn)

		assertSnapshotCreateFailure(t, dbconn, eng, SnapshotCreateRequest{
			ID:            "snap-missing-path-engine",
			SelectionBase: base,
			Paths:         []string{"docs/a.txt", "missing.txt"},
		}, "path not found in current state")
	})
}

func TestSnapshotCreateSelectorAtomicityAndAccounting(t *testing.T) {
	t.Run("multiple misses are stable and rollback preserves shared paths", func(t *testing.T) {
		dbconn := openSnapshotCreateEngineDB(t)
		base := seedSnapshotCreateEngineFiles(t, dbconn)
		eng := newSnapshotCreateEngine(t, dbconn)
		if _, err := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES (?)`, "shared/preexisting.txt"); err != nil {
			t.Fatalf("seed preexisting snapshot_path: %v", err)
		}
		initialPathRows := phase3TableCount(t, eng, `SELECT COUNT(*) FROM snapshot_path`)

		_, err := eng.SnapshotCreate(context.Background(), SnapshotCreateRequest{
			ID:            "snap-multiple-misses-engine",
			SelectionBase: base,
			Paths:         []string{"z-missing/", "docs/a.txt", "a-missing.txt"},
		})
		if !IsCode(err, ErrorNotFound) {
			t.Fatalf("multiple misses: expected not_found, got code=%q err=%v", CodeOf(err), err)
		}
		if !strings.Contains(err.Error(), "a-missing.txt, z-missing/") {
			t.Fatalf("multiple misses: expected sorted selector report, got %v", err)
		}
		phase3AssertCreateRollback(t, eng, "snap-multiple-misses-engine", initialPathRows)
		if got := phase3TableCount(t, eng, `SELECT COUNT(*) FROM snapshot_path WHERE path = ?`, "shared/preexisting.txt"); got != 1 {
			t.Fatalf("failed create changed preexisting snapshot_path row: got %d", got)
		}
	})

	t.Run("matched prefix and missing exact rollback together", func(t *testing.T) {
		dbconn := openSnapshotCreateEngineDB(t)
		base := seedSnapshotCreateEngineFiles(t, dbconn)
		eng := newSnapshotCreateEngine(t, dbconn)
		initialPathRows := phase3TableCount(t, eng, `SELECT COUNT(*) FROM snapshot_path`)

		_, err := eng.SnapshotCreate(context.Background(), SnapshotCreateRequest{
			ID:            "snap-prefix-exact-miss-engine",
			SelectionBase: base,
			Paths:         []string{"docs/", "missing.txt"},
		})
		if !IsCode(err, ErrorNotFound) || !strings.Contains(err.Error(), "missing.txt") {
			t.Fatalf("matched prefix plus missing exact: code=%q err=%v", CodeOf(err), err)
		}
		phase3AssertCreateRollback(t, eng, "snap-prefix-exact-miss-engine", initialPathRows)
	})

	t.Run("duplicates and overlaps count independently without duplicate members", func(t *testing.T) {
		dbconn := openSnapshotCreateEngineDB(t)
		base := seedSnapshotCreateEngineFiles(t, dbconn)
		oldBoundaryPath := filepath.Join(base, "img", "c.png")
		newBoundaryPath := filepath.Join(base, "docs-old", "outside.txt")
		ensureSnapshotCreateInputDir(t, filepath.Dir(newBoundaryPath))
		if err := os.Rename(oldBoundaryPath, newBoundaryPath); err != nil {
			t.Fatalf("move prefix-boundary fixture: %v", err)
		}
		if _, err := dbconn.Exec(`UPDATE physical_file SET path = ? WHERE path = ?`, newBoundaryPath, oldBoundaryPath); err != nil {
			t.Fatalf("update prefix-boundary fixture mapping: %v", err)
		}
		eng := newSnapshotCreateEngine(t, dbconn)

		result, err := eng.SnapshotCreate(context.Background(), SnapshotCreateRequest{
			ID:            "snap-overlap-engine",
			SelectionBase: base,
			Paths:         []string{"docs/", "./docs//", "docs/a.txt", "./docs/a.txt", "docs/sub/"},
		})
		if err != nil {
			t.Fatalf("duplicate and overlapping selectors: %v", err)
		}
		if result.FilesInserted != 2 || snapshotMembershipCount(t, dbconn, result.SnapshotID) != 2 {
			t.Fatalf("duplicate and overlapping selectors produced duplicate/boundary members: %+v", result)
		}
		members := phase3SnapshotMembers(t, eng, result.SnapshotID)
		if !reflect.DeepEqual(members, []string{"docs/a.txt", "docs/sub/b.txt"}) {
			t.Fatalf("prefix boundary/overlap members: got %v", members)
		}
	})

	t.Run("non-completed source does not satisfy selector", func(t *testing.T) {
		dbconn := openSnapshotCreateEngineDB(t)
		base := seedSnapshotCreateEngineFiles(t, dbconn)
		eng := newSnapshotCreateEngine(t, dbconn)
		if _, err := dbconn.Exec(`UPDATE logical_file SET status = 'PROCESSING' WHERE id = (
			SELECT logical_file_id FROM physical_file WHERE path = ?
		)`, filepath.Join(base, "docs", "a.txt")); err != nil {
			t.Fatalf("mark selected source non-completed: %v", err)
		}
		initialPathRows := phase3TableCount(t, eng, `SELECT COUNT(*) FROM snapshot_path`)

		_, err := eng.SnapshotCreate(context.Background(), SnapshotCreateRequest{
			ID:            "snap-ineligible-engine",
			SelectionBase: base,
			Paths:         []string{"docs/a.txt"},
		})
		if !IsCode(err, ErrorNotFound) {
			t.Fatalf("non-completed selector: expected not_found, got code=%q err=%v", CodeOf(err), err)
		}
		phase3AssertCreateRollback(t, eng, "snap-ineligible-engine", initialPathRows)
	})
}

func TestSnapshotCreateParentValidationCases(t *testing.T) {
	t.Run("missing parent rolls back", func(t *testing.T) {
		dbconn := openSnapshotCreateEngineDB(t)
		base := seedSnapshotCreateEngineFiles(t, dbconn)
		eng := newSnapshotCreateEngine(t, dbconn)

		assertSnapshotCreateFailure(t, dbconn, eng, SnapshotCreateRequest{
			ID:            "snap-missing-parent-engine",
			ParentID:      "snap-parent-missing",
			SelectionBase: base,
		}, `parent snapshot "snap-parent-missing" not found`)
	})

	t.Run("partial parent rolls back", func(t *testing.T) {
		dbconn := openSnapshotCreateEngineDB(t)
		base := seedSnapshotCreateEngineFiles(t, dbconn)
		insertSnapshotRow(t, dbconn, "snap-parent-partial-engine", "partial")
		eng := newSnapshotCreateEngine(t, dbconn)

		assertSnapshotCreateFailure(t, dbconn, eng, SnapshotCreateRequest{
			ID:            "snap-child-full-engine",
			ParentID:      "snap-parent-partial-engine",
			SelectionBase: base,
		}, `is partial; --from is currently supported only for full snapshots`)
	})

	t.Run("self parent fails before mutation", func(t *testing.T) {
		dbconn := openSnapshotCreateEngineDB(t)
		base := seedSnapshotCreateEngineFiles(t, dbconn)
		eng := newSnapshotCreateEngine(t, dbconn)

		assertSnapshotCreateFailure(t, dbconn, eng, SnapshotCreateRequest{
			ID:            "snap-self-parent-engine",
			ParentID:      "snap-self-parent-engine",
			SelectionBase: base,
		}, `cannot reference itself`)
	})
}

func assertSnapshotCreateFailure(
	t *testing.T,
	dbconn *sql.DB,
	eng *DefaultEngine,
	req SnapshotCreateRequest,
	wantSubstring string,
) {
	t.Helper()

	result, err := eng.SnapshotCreate(context.Background(), req)
	if err == nil || !strings.Contains(err.Error(), wantSubstring) {
		t.Fatalf("expected error containing %q, got result=%+v err=%v", wantSubstring, result, err)
	}
	if result != (SnapshotCreateResult{}) {
		t.Fatalf("expected zero result on failure, got %+v", result)
	}

	snapshotID := strings.TrimSpace(req.ID)
	if snapshotID != "" {
		if snapshotExists(t, dbconn, snapshotID) {
			t.Fatalf("unexpected committed snapshot row for %s", snapshotID)
		}
		if snapshotMembershipCount(t, dbconn, snapshotID) != 0 {
			t.Fatalf("unexpected committed snapshot membership for %s", snapshotID)
		}
	}
}

func assertSnapshotCreateEngineResult(
	t *testing.T,
	result SnapshotCreateResult,
	wantID string,
	wantType SnapshotType,
	wantPathsCount int,
	wantFilesInserted int,
	wantLabel string,
	wantParentID string,
) {
	t.Helper()

	if result.SnapshotID != wantID || result.Type != wantType {
		t.Fatalf("unexpected snapshot create identity/type: %+v", result)
	}
	if result.PathsCount != wantPathsCount || result.FilesInserted != wantFilesInserted {
		t.Fatalf("unexpected snapshot create counts: %+v", result)
	}
	if result.Label != wantLabel || result.ParentID != wantParentID {
		t.Fatalf("unexpected snapshot create metadata: %+v", result)
	}
}
