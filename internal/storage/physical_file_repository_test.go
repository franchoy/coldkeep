package storage

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	filestate "github.com/franchoy/coldkeep/internal/status"

	_ "github.com/mattn/go-sqlite3"
)

func insertLogicalForPhysicalTest(t *testing.T, dbconn *sql.DB, name, hash string, refCount int64) int64 {
	t.Helper()
	var id int64
	if err := dbconn.QueryRow(
		`INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count)
		 VALUES ($1, $2, $3, $4, $5)
		 RETURNING id`,
		name,
		int64(8),
		hash,
		filestate.LogicalFileCompleted,
		refCount,
	).Scan(&id); err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}
	return id
}

func TestEnsurePhysicalFileForPathDefaultPolicyWithTx(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	logical1 := insertLogicalForPhysicalTest(t, dbconn, "a.bin", "hash-a", 0)
	logical2 := insertLogicalForPhysicalTest(t, dbconn, "b.bin", "hash-b", 0)

	ctx := context.Background()
	meta := physicalFileMetadata{IsMetadataComplete: true}

	tx1, err := dbconn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx1: %v", err)
	}
	alreadyMapped, err := ensurePhysicalFileForPathDefaultPolicyWithTx(ctx, dbconn, tx1, "/x/a.bin", logical1, meta)
	if err != nil {
		_ = tx1.Rollback()
		t.Fatalf("ensure mapping (insert): %v", err)
	}
	if alreadyMapped {
		_ = tx1.Rollback()
		t.Fatalf("expected insert path to report not already mapped")
	}
	if err := tx1.Commit(); err != nil {
		t.Fatalf("commit tx1: %v", err)
	}

	var refCount1 int64
	if err := dbconn.QueryRow(`SELECT ref_count FROM logical_file WHERE id = $1`, logical1).Scan(&refCount1); err != nil {
		t.Fatalf("read logical1 ref_count after insert: %v", err)
	}
	if refCount1 != 1 {
		t.Fatalf("expected logical1 ref_count=1 after first mapping, got %d", refCount1)
	}

	tx2, err := dbconn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	alreadyMapped, err = ensurePhysicalFileForPathDefaultPolicyWithTx(ctx, dbconn, tx2, "/x/a.bin", logical1, meta)
	if err != nil {
		_ = tx2.Rollback()
		t.Fatalf("ensure mapping (same path same logical): %v", err)
	}
	if !alreadyMapped {
		_ = tx2.Rollback()
		t.Fatalf("expected existing mapping to report already mapped")
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("commit tx2: %v", err)
	}

	if err := dbconn.QueryRow(`SELECT ref_count FROM logical_file WHERE id = $1`, logical1).Scan(&refCount1); err != nil {
		t.Fatalf("read logical1 ref_count after no-op update: %v", err)
	}
	if refCount1 != 1 {
		t.Fatalf("expected logical1 ref_count to remain 1 on idempotent write, got %d", refCount1)
	}

	tx3, err := dbconn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx3: %v", err)
	}
	_, err = ensurePhysicalFileForPathDefaultPolicyWithTx(ctx, dbconn, tx3, "/x/a.bin", logical2, meta)
	if err == nil {
		_ = tx3.Rollback()
		t.Fatalf("expected conflict for same path mapped to different logical file")
	}
	var conflictErr *physicalPathConflictError
	if !errors.As(err, &conflictErr) {
		_ = tx3.Rollback()
		t.Fatalf("expected physicalPathConflictError, got: %v", err)
	}
	_ = tx3.Rollback()
}

func TestReplacePhysicalFileLogicalTargetTx(t *testing.T) {
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	logical1 := insertLogicalForPhysicalTest(t, dbconn, "a.bin", "hash-a", 1)
	logical2 := insertLogicalForPhysicalTest(t, dbconn, "b.bin", "hash-b", 0)

	if _, err := dbconn.Exec(
		`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES ($1, $2, $3)`,
		"/x/data.bin",
		logical1,
		1,
	); err != nil {
		t.Fatalf("insert physical_file seed row: %v", err)
	}

	tx, err := dbconn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if err := replacePhysicalFileLogicalTargetTx(context.Background(), dbconn, tx, "/x/data.bin", logical2, physicalFileMetadata{IsMetadataComplete: true}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("replace physical_file target: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit tx: %v", err)
	}

	var mappedLogical int64
	if err := dbconn.QueryRow(`SELECT logical_file_id FROM physical_file WHERE path = $1`, "/x/data.bin").Scan(&mappedLogical); err != nil {
		t.Fatalf("read updated physical_file row: %v", err)
	}
	if mappedLogical != logical2 {
		t.Fatalf("expected physical_file to point to logical2=%d, got %d", logical2, mappedLogical)
	}

	var ref1, ref2 int64
	if err := dbconn.QueryRow(`SELECT ref_count FROM logical_file WHERE id = $1`, logical1).Scan(&ref1); err != nil {
		t.Fatalf("read logical1 ref_count: %v", err)
	}
	if err := dbconn.QueryRow(`SELECT ref_count FROM logical_file WHERE id = $1`, logical2).Scan(&ref2); err != nil {
		t.Fatalf("read logical2 ref_count: %v", err)
	}
	if ref1 != 0 || ref2 != 1 {
		t.Fatalf("unexpected ref_count transition after replace: old=%d new=%d", ref1, ref2)
	}
}

func TestStoreFileMaintainsPhysicalFileAndLogicalRefCountInvariants(t *testing.T) {
	t.Setenv("COLDKEEP_REUSE_SEMANTIC_VALIDATION", "off")

	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	sgctx := StorageContext{
		DB:           dbconn,
		Writer:       container.NewSimulatedWriter(container.GetContainerMaxSize()),
		ContainerDir: t.TempDir(),
	}

	codec, err := blocks.ParseCodec("plain")
	if err != nil {
		t.Fatalf("parse plain codec: %v", err)
	}

	tmpDir := t.TempDir()
	content := []byte("dedupe-content-v1")
	pathA := filepath.Join(tmpDir, "a.txt")
	pathB := filepath.Join(tmpDir, "b.txt")
	if err := os.WriteFile(pathA, content, 0o600); err != nil {
		t.Fatalf("write pathA: %v", err)
	}
	if err := os.WriteFile(pathB, content, 0o600); err != nil {
		t.Fatalf("write pathB: %v", err)
	}

	resA, err := StoreFileWithStorageContextAndCodecResult(sgctx, pathA, codec)
	if err != nil {
		t.Fatalf("store pathA: %v", err)
	}
	if resA.AlreadyStored {
		t.Fatalf("expected first store to report AlreadyStored=false")
	}

	resB, err := StoreFileWithStorageContextAndCodecResult(sgctx, pathB, codec)
	if err != nil {
		t.Fatalf("store pathB with same content: %v", err)
	}
	if !resB.AlreadyStored {
		t.Fatalf("expected second deduplicated store to report AlreadyStored=true")
	}
	if resA.FileID != resB.FileID {
		t.Fatalf("expected deduplicated stores to share logical_file_id, got %d vs %d", resA.FileID, resB.FileID)
	}

	_, err = StoreFileWithStorageContextAndCodecResult(sgctx, pathA, codec)
	if err != nil {
		t.Fatalf("re-store same path same content: %v", err)
	}

	var physicalRows int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM physical_file WHERE logical_file_id = $1`, resA.FileID).Scan(&physicalRows); err != nil {
		t.Fatalf("count physical_file rows for logical_file: %v", err)
	}
	if physicalRows != 2 {
		t.Fatalf("expected 2 physical_file rows for deduplicated logical file, got %d", physicalRows)
	}

	var refCount int64
	if err := dbconn.QueryRow(`SELECT ref_count FROM logical_file WHERE id = $1`, resA.FileID).Scan(&refCount); err != nil {
		t.Fatalf("read logical_file ref_count: %v", err)
	}
	if refCount != 2 {
		t.Fatalf("expected logical_file.ref_count=2 for two distinct paths, got %d", refCount)
	}

	if err := os.WriteFile(pathA, []byte("different-content-v2"), 0o600); err != nil {
		t.Fatalf("overwrite pathA content: %v", err)
	}

	_, err = StoreFileWithStorageContextAndCodecResult(sgctx, pathA, codec)
	if err == nil {
		t.Fatalf("expected same-path different-content store to fail without replace")
	}

	if err := dbconn.QueryRow(`SELECT ref_count FROM logical_file WHERE id = $1`, resA.FileID).Scan(&refCount); err != nil {
		t.Fatalf("read logical_file ref_count after failed conflict store: %v", err)
	}
	if refCount != 2 {
		t.Fatalf("expected logical_file.ref_count to remain 2 after failed same-path conflict, got %d", refCount)
	}
}

func TestStoreFileReplacePolicyRetargetsSamePathDifferentContent(t *testing.T) {
	t.Setenv("COLDKEEP_REUSE_SEMANTIC_VALIDATION", "off")

	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	sgctx := StorageContext{
		DB:           dbconn,
		Writer:       container.NewSimulatedWriter(container.GetContainerMaxSize()),
		ContainerDir: t.TempDir(),
	}

	codec, err := blocks.ParseCodec("plain")
	if err != nil {
		t.Fatalf("parse plain codec: %v", err)
	}

	tmpDir := t.TempDir()
	pathA := filepath.Join(tmpDir, "same-path.txt")
	if err := os.WriteFile(pathA, []byte("content-v1"), 0o600); err != nil {
		t.Fatalf("write initial content: %v", err)
	}

	first, err := StoreFileWithStorageContextAndCodecResultWithPolicy(sgctx, pathA, codec, false)
	if err != nil {
		t.Fatalf("initial store: %v", err)
	}

	if err := os.WriteFile(pathA, []byte("content-v2"), 0o600); err != nil {
		t.Fatalf("overwrite with new content: %v", err)
	}

	second, err := StoreFileWithStorageContextAndCodecResultWithPolicy(sgctx, pathA, codec, true)
	if err != nil {
		t.Fatalf("store with replace policy: %v", err)
	}
	if first.FileID == second.FileID {
		t.Fatalf("expected replace to retarget path to different logical file")
	}

	var mappedLogicalID int64
	if err := dbconn.QueryRow(`SELECT logical_file_id FROM physical_file WHERE path = $1`, pathA).Scan(&mappedLogicalID); err != nil {
		t.Fatalf("read physical mapping after replace: %v", err)
	}
	if mappedLogicalID != second.FileID {
		t.Fatalf("expected path mapped to new logical file %d, got %d", second.FileID, mappedLogicalID)
	}

	var oldRefCount, newRefCount int64
	if err := dbconn.QueryRow(`SELECT ref_count FROM logical_file WHERE id = $1`, first.FileID).Scan(&oldRefCount); err != nil {
		t.Fatalf("read old logical ref_count: %v", err)
	}
	if err := dbconn.QueryRow(`SELECT ref_count FROM logical_file WHERE id = $1`, second.FileID).Scan(&newRefCount); err != nil {
		t.Fatalf("read new logical ref_count: %v", err)
	}
	if oldRefCount != 0 {
		t.Fatalf("expected old logical ref_count=0 after replace, got %d", oldRefCount)
	}
	if newRefCount != 1 {
		t.Fatalf("expected new logical ref_count=1 after replace, got %d", newRefCount)
	}
}

func TestStoreFileNormalizesRelativeAndAbsolutePathsToSingleIdentity(t *testing.T) {
	t.Setenv("COLDKEEP_REUSE_SEMANTIC_VALIDATION", "off")

	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	defer func() { _ = dbconn.Close() }()

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	codec, err := blocks.ParseCodec("plain")
	if err != nil {
		t.Fatalf("parse plain codec: %v", err)
	}

	workDir := t.TempDir()
	fileName := "same.txt"
	absPath := filepath.Join(workDir, fileName)
	if err := os.WriteFile(absPath, []byte("same-content"), 0o600); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	originalWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	defer func() { _ = os.Chdir(originalWD) }()
	if err := os.Chdir(workDir); err != nil {
		t.Fatalf("chdir work dir: %v", err)
	}

	sgctx := StorageContext{
		DB:           dbconn,
		Writer:       container.NewSimulatedWriter(container.GetContainerMaxSize()),
		ContainerDir: t.TempDir(),
	}

	first, err := StoreFileWithStorageContextAndCodecResult(sgctx, fileName, codec)
	if err != nil {
		t.Fatalf("store with relative path: %v", err)
	}

	second, err := StoreFileWithStorageContextAndCodecResult(sgctx, "./"+fileName, codec)
	if err != nil {
		t.Fatalf("store with dot-relative path: %v", err)
	}

	third, err := StoreFileWithStorageContextAndCodecResult(sgctx, absPath, codec)
	if err != nil {
		t.Fatalf("store with absolute path: %v", err)
	}

	if first.Path != absPath || second.Path != absPath || third.Path != absPath {
		t.Fatalf("expected all normalized paths to equal %q, got first=%q second=%q third=%q", absPath, first.Path, second.Path, third.Path)
	}

	var physicalCount int64
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM physical_file WHERE logical_file_id = $1`, first.FileID).Scan(&physicalCount); err != nil {
		t.Fatalf("count physical mappings for logical file: %v", err)
	}
	if physicalCount != 1 {
		t.Fatalf("expected one physical_file identity after path normalization, got %d", physicalCount)
	}

	var refCount int64
	if err := dbconn.QueryRow(`SELECT ref_count FROM logical_file WHERE id = $1`, first.FileID).Scan(&refCount); err != nil {
		t.Fatalf("read logical ref_count: %v", err)
	}
	if refCount != 1 {
		t.Fatalf("expected logical ref_count=1 after repeated normalized path stores, got %d", refCount)
	}
}
