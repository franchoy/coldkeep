package storage

import (
	"crypto/sha256"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/db"
	verifypkg "github.com/franchoy/coldkeep/internal/verify"
)

func TestCKV11316015PostgresOpenLocalStorageRepairAndRecovery(t *testing.T) {
	if strings.TrimSpace(os.Getenv("COLDKEEP_TEST_DB")) == "" {
		t.Skip("set COLDKEEP_TEST_DB=1 with DB_* settings to run PostgreSQL repair coverage")
	}

	adminDatabase := strings.TrimSpace(os.Getenv("COLDKEEP_TEST_DB_MAINTENANCE"))
	if adminDatabase == "" {
		adminDatabase = "postgres"
	}
	adminConnection, err := db.BuildPostgresConnStringFromEnv(adminDatabase)
	if err != nil {
		t.Fatalf("build PostgreSQL maintenance connection: %v", err)
	}
	admin, err := sql.Open("postgres", adminConnection)
	if err != nil {
		t.Fatalf("open PostgreSQL maintenance connection: %v", err)
	}
	defer func() { _ = admin.Close() }()
	if err := admin.Ping(); err != nil {
		t.Fatalf("ping PostgreSQL maintenance database: %v", err)
	}

	databaseName := fmt.Sprintf("coldkeep_ck015_%d_%d", os.Getpid(), time.Now().UnixNano())
	if _, err := admin.Exec("CREATE DATABASE " + databaseName); err != nil {
		t.Fatalf("create PostgreSQL CK-015 scratch database %s: %v", databaseName, err)
	}
	defer func() {
		if _, err := admin.Exec(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()`, databaseName); err != nil {
			t.Errorf("terminate CK-015 scratch sessions: %v", err)
		}
		if _, err := admin.Exec("DROP DATABASE IF EXISTS " + databaseName); err != nil {
			t.Errorf("drop PostgreSQL CK-015 scratch database %s: %v", databaseName, err)
		}
	}()

	t.Setenv("DB_NAME", databaseName)
	t.Setenv("COLDKEEP_DB_AUTO_BOOTSTRAP", "true")
	t.Setenv("COLDKEEP_REUSE_SEMANTIC_VALIDATION", "always")
	t.Setenv("COLDKEEP_COMPRESSION", "zstd")
	t.Setenv("COLDKEEP_COMPRESSION_LEVEL", "3")

	containersDir := t.TempDir()
	storageContext, err := OpenLocalStorage(containersDir)
	if err != nil {
		t.Fatalf("open PostgreSQL local storage: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = storageContext.Close()
		}
	}()

	left := bytesForCKV11316015("postgres-left", 64*1024)
	right := bytesForCKV11316015("postgres-right", 64*1024)
	payload := append(append([]byte(nil), left...), right...)
	storageContext.Chunker = scriptedChunker{
		version:  chunk.VersionV1SimpleRolling,
		payloads: [][]byte{left, right},
	}
	inputDir := t.TempDir()
	originalPath := filepath.Join(inputDir, "postgres-original.bin")
	duplicatePath := filepath.Join(inputDir, "postgres-duplicate.bin")
	if err := os.WriteFile(originalPath, payload, 0o600); err != nil {
		t.Fatalf("write PostgreSQL original source: %v", err)
	}
	if err := os.WriteFile(duplicatePath, payload, 0o600); err != nil {
		t.Fatalf("write PostgreSQL duplicate source: %v", err)
	}

	stored, err := StoreFileWithStorageContextAndCodecResult(storageContext, originalPath, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("store PostgreSQL repair fixture: %v", err)
	}
	if stored.AlreadyStored || stored.FileID <= 0 {
		t.Fatalf("unexpected PostgreSQL initial Store result: %+v", stored)
	}
	beforeRecipe := ckV11316015QueryStrings(t, storageContext.DB,
		`SELECT CAST(chunk_id AS TEXT) || '|' || CAST(chunk_order AS TEXT) FROM file_chunk WHERE logical_file_id = $1 ORDER BY chunk_order`, stored.FileID)

	var filename string
	if err := storageContext.DB.QueryRow(`
		SELECT c.filename
		FROM file_chunk fc
		JOIN chunk_block_refs r ON r.chunk_id = fc.chunk_id
		JOIN storage_blocks sb ON sb.id = r.block_id
		JOIN container c ON c.id = sb.container_id
		WHERE fc.logical_file_id = $1
		ORDER BY fc.chunk_order LIMIT 1`, stored.FileID).Scan(&filename); err != nil {
		t.Fatalf("resolve PostgreSQL required container: %v", err)
	}
	requiredPath := filepath.Join(containersDir, filename)
	holdingPath := filepath.Join(t.TempDir(), filename)
	originalBytes, err := os.ReadFile(requiredPath)
	if err != nil {
		t.Fatalf("read PostgreSQL required container: %v", err)
	}
	originalHash := sha256.Sum256(originalBytes)
	if err := os.Rename(requiredPath, holdingPath); err != nil {
		t.Fatalf("temporarily remove PostgreSQL required container: %v", err)
	}
	if _, err := os.Stat(requiredPath); !os.IsNotExist(err) {
		t.Fatalf("PostgreSQL required container remains visible: %v", err)
	}

	repaired, err := StoreFileWithStorageContextAndCodecResult(storageContext, duplicatePath, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("repair PostgreSQL completed object: %v", err)
	}
	if repaired.FileID != stored.FileID || repaired.AlreadyStored {
		t.Fatalf("unexpected PostgreSQL repair result: %+v", repaired)
	}
	afterRecipe := ckV11316015QueryStrings(t, storageContext.DB,
		`SELECT CAST(chunk_id AS TEXT) || '|' || CAST(chunk_order AS TEXT) FROM file_chunk WHERE logical_file_id = $1 ORDER BY chunk_order`, stored.FileID)
	if !reflect.DeepEqual(beforeRecipe, afterRecipe) {
		t.Fatalf("PostgreSQL repair changed ordered recipe: before=%v after=%v", beforeRecipe, afterRecipe)
	}

	fixture := &ckV11316015Fixture{
		repo: &TestRepository{
			DB: storageContext.DB, Storage: storageContext, ContainersDir: containersDir,
		},
		originalPath: originalPath, duplicatePath: duplicatePath, fileID: stored.FileID, payload: payload,
	}
	assertCKV11316015Restore(t, fixture, originalPath, "postgres-original-after-repair.bin")
	assertCKV11316015Restore(t, fixture, duplicatePath, "postgres-duplicate-after-repair.bin")

	if err := os.Rename(holdingPath, requiredPath); err != nil {
		t.Fatalf("replace exact PostgreSQL retired container bytes: %v", err)
	}
	replacedBytes, err := os.ReadFile(requiredPath)
	if err != nil {
		t.Fatalf("read replaced PostgreSQL retired container: %v", err)
	}
	if len(replacedBytes) != len(originalBytes) || sha256.Sum256(replacedBytes) != originalHash {
		t.Fatalf("replaced PostgreSQL retired container differs from exact original bytes")
	}
	if err := verifypkg.VerifySystemFullWithContainersDir(storageContext.DB, containersDir); err != nil {
		t.Fatalf("verify PostgreSQL published repair: %v", err)
	}

	if err := storageContext.Close(); err != nil {
		t.Fatalf("close PostgreSQL storage before recovery reopen: %v", err)
	}
	closed = true
	reopened, err := OpenLocalStorage(containersDir)
	if err != nil {
		t.Fatalf("reopen PostgreSQL local storage through production recovery: %v", err)
	}
	storageContext = reopened
	closed = false
	storageContext.Chunker = scriptedChunker{
		version:  chunk.VersionV1SimpleRolling,
		payloads: [][]byte{left, right},
	}
	fixture.repo.DB = storageContext.DB
	fixture.repo.Storage = storageContext
	assertCKV11316015Restore(t, fixture, originalPath, "postgres-original-after-reopen.bin")

	var published int
	if err := storageContext.DB.QueryRow(`SELECT COUNT(*) FROM store_repair_attempt WHERE logical_file_id = $1 AND status = 'PUBLISHED'`, stored.FileID).Scan(&published); err != nil {
		t.Fatalf("count PostgreSQL published repair attempts: %v", err)
	}
	if published != 1 {
		t.Fatalf("PostgreSQL published repair attempts=%d, want 1", published)
	}
}
