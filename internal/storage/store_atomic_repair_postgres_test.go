package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
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
	filestate "github.com/franchoy/coldkeep/internal/status"
	verifypkg "github.com/franchoy/coldkeep/internal/verify"
	"github.com/lib/pq"
)

var errCKV11316015PublicationChunkLockMissing = errors.New("repair publication did not hold the affected chunk row lock")

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
	var logicalRetryBefore int64
	if err := storageContext.DB.QueryRow(`SELECT retry_count FROM logical_file WHERE id = $1`, stored.FileID).Scan(&logicalRetryBefore); err != nil {
		t.Fatalf("query PostgreSQL logical retry before repair: %v", err)
	}
	chunkRetriesBefore := make(map[int64]int64)
	rows, err := storageContext.DB.Query(`
		SELECT DISTINCT c.id, c.retry_count
		FROM chunk c JOIN file_chunk fc ON fc.chunk_id = c.id
		WHERE fc.logical_file_id = $1`, stored.FileID)
	if err != nil {
		t.Fatalf("query PostgreSQL chunk retries before repair: %v", err)
	}
	for rows.Next() {
		var chunkID, retryCount int64
		if err := rows.Scan(&chunkID, &retryCount); err != nil {
			_ = rows.Close()
			t.Fatalf("scan PostgreSQL chunk retry before repair: %v", err)
		}
		chunkRetriesBefore[chunkID] = retryCount
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatalf("iterate PostgreSQL chunk retries before repair: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close PostgreSQL chunk retries before repair: %v", err)
	}

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
	var logicalRetryAfter int64
	if err := storageContext.DB.QueryRow(`SELECT retry_count FROM logical_file WHERE id = $1`, stored.FileID).Scan(&logicalRetryAfter); err != nil {
		t.Fatalf("query PostgreSQL logical retry after repair: %v", err)
	}
	if logicalRetryAfter != logicalRetryBefore+1 {
		t.Fatalf("PostgreSQL logical retry=%d->%d, want exactly +1", logicalRetryBefore, logicalRetryAfter)
	}
	for chunkID, before := range chunkRetriesBefore {
		var after int64
		if err := storageContext.DB.QueryRow(`SELECT retry_count FROM chunk WHERE id = $1`, chunkID).Scan(&after); err != nil {
			t.Fatalf("query PostgreSQL chunk %d retry after repair: %v", chunkID, err)
		}
		if after != before+1 {
			t.Fatalf("PostgreSQL repaired chunk %d retry=%d->%d, want exactly +1", chunkID, before, after)
		}
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

func TestCKV11316015PostgresRepairPublisherLocksChunkBeforeAuthorityMutation(t *testing.T) {
	repo := newCKV11316015PostgresRepository(t)
	fixture := newCKV11316015FixtureWithRepository(t, false, blocks.CodecPlain, repo)
	chunkID := ckV11316015FirstRecipeChunkID(t, repo.DB, fixture.fileID)
	if _, err := repo.DB.Exec(`UPDATE chunk SET status = $1 WHERE id = $2`, filestate.ChunkAborted, chunkID); err != nil {
		t.Fatalf("prepare publisher-first ABORTED chunk: %v", err)
	}

	lockObserved := false
	restoreHooks := InstallTestStoreInterleavingHooks(&repo.Storage, func(ctx context.Context, event TestStoreInterleavingHookEvent) error {
		if event.Event != TestStoreInterleavingEventAfterRepairRetirement {
			return nil
		}
		probe, err := repo.DB.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin publisher-first lock probe: %w", err)
		}
		defer func() { _ = probe.Rollback() }()
		var lockedID int64
		err = probe.QueryRowContext(ctx, `SELECT id FROM chunk WHERE id = $1 FOR UPDATE NOWAIT`, chunkID).Scan(&lockedID)
		if err == nil {
			return errCKV11316015PublicationChunkLockMissing
		}
		var pqErr *pq.Error
		if !errors.As(err, &pqErr) || pqErr.Code != "55P03" {
			return fmt.Errorf("probe publisher-first chunk lock: %w", err)
		}
		lockObserved = true
		return nil
	})
	defer restoreHooks()

	result, err := StoreFileWithStorageContextAndCodecResult(repo.Storage, fixture.duplicatePath, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("publisher-first repair: %v", err)
	}
	if result.FileID != fixture.fileID || result.AlreadyStored {
		t.Fatalf("publisher-first repair result=%+v", result)
	}
	if !lockObserved {
		t.Fatal("publisher-first repair never demonstrated the affected chunk row lock")
	}
}

func TestCKV11316015PostgresRepairCompetitorWinsChunkLockBeforePublication(t *testing.T) {
	repo := newCKV11316015PostgresRepository(t)
	fixture := newCKV11316015FixtureWithRepository(t, false, blocks.CodecPlain, repo)
	chunkID := ckV11316015FirstRecipeChunkID(t, repo.DB, fixture.fileID)
	if _, err := repo.DB.Exec(`UPDATE chunk SET status = $1 WHERE id = $2`, filestate.ChunkAborted, chunkID); err != nil {
		t.Fatalf("prepare competitor-first ABORTED chunk: %v", err)
	}
	var logicalBefore, chunkBefore int64
	if err := repo.DB.QueryRow(`SELECT retry_count FROM logical_file WHERE id = $1`, fixture.fileID).Scan(&logicalBefore); err != nil {
		t.Fatalf("query competitor-first logical retry: %v", err)
	}
	if err := repo.DB.QueryRow(`SELECT retry_count FROM chunk WHERE id = $1`, chunkID).Scan(&chunkBefore); err != nil {
		t.Fatalf("query competitor-first chunk retry: %v", err)
	}

	competitorConn, err := repo.DB.Conn(context.Background())
	if err != nil {
		t.Fatalf("reserve competitor connection: %v", err)
	}
	defer func() { _ = competitorConn.Close() }()
	var competitorPID int
	if err := competitorConn.QueryRowContext(context.Background(), `SELECT pg_backend_pid()`).Scan(&competitorPID); err != nil {
		t.Fatalf("query competitor backend pid: %v", err)
	}
	competitorTx, err := competitorConn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin competitor-first claim: %v", err)
	}
	competitorCommitted := false
	defer func() {
		if !competitorCommitted {
			_ = competitorTx.Rollback()
		}
	}()
	if _, err := competitorTx.Exec(`UPDATE chunk SET status = $1, retry_count = retry_count + 1 WHERE id = $2 AND status = $3`,
		filestate.ChunkProcessing, chunkID, filestate.ChunkAborted); err != nil {
		t.Fatalf("claim competitor-first chunk: %v", err)
	}

	type storeOutcome struct {
		result StoreFileResult
		err    error
	}
	storeDone := make(chan storeOutcome, 1)
	go func() {
		result, storeErr := StoreFileWithStorageContextAndCodecResult(repo.Storage, fixture.duplicatePath, blocks.CodecPlain)
		storeDone <- storeOutcome{result: result, err: storeErr}
	}()

	waitCtx, cancelWait := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelWait()
	if err := ckV11316015WaitForPostgresLockWaiter(waitCtx, repo.DB, competitorPID); err != nil {
		t.Fatalf("publisher did not block behind competitor chunk ownership: %v", err)
	}

	terminalDone := make(chan error, 1)
	go func() {
		_, terminalErr := repo.DB.Exec(`UPDATE chunk SET status = $1 WHERE id = $2`,
			filestate.ChunkCompleted, chunkID)
		terminalDone <- terminalErr
	}()
	if err := competitorTx.Commit(); err != nil {
		t.Fatalf("commit competitor-first claim: %v", err)
	}
	competitorCommitted = true

	select {
	case err := <-terminalDone:
		if err != nil {
			t.Fatalf("complete competitor-first chunk: %v", err)
		}
	case <-waitCtx.Done():
		t.Fatalf("competitor-first terminal transition did not finish: %v", waitCtx.Err())
	}
	var outcome storeOutcome
	select {
	case outcome = <-storeDone:
	case <-waitCtx.Done():
		t.Fatalf("competitor-first Store did not converge: %v", waitCtx.Err())
	}
	if outcome.err != nil {
		t.Fatalf("competitor-first Store: %v", outcome.err)
	}
	if outcome.result.FileID != fixture.fileID || !outcome.result.AlreadyStored {
		t.Fatalf("competitor-first Store did not converge to healthy reuse: %+v", outcome.result)
	}
	var logicalAfter, chunkAfter int64
	if err := repo.DB.QueryRow(`SELECT retry_count FROM logical_file WHERE id = $1`, fixture.fileID).Scan(&logicalAfter); err != nil {
		t.Fatalf("query competitor-first logical retry after convergence: %v", err)
	}
	if err := repo.DB.QueryRow(`SELECT retry_count FROM chunk WHERE id = $1`, chunkID).Scan(&chunkAfter); err != nil {
		t.Fatalf("query competitor-first chunk retry after convergence: %v", err)
	}
	if logicalAfter != logicalBefore || chunkAfter != chunkBefore+1 {
		t.Fatalf("competitor-first retry accounting logical=%d->%d chunk=%d->%d", logicalBefore, logicalAfter, chunkBefore, chunkAfter)
	}
	ckV11316015AssertNoLiveRepairState(t, repo.DB, fixture.fileID)
}

func newCKV11316015PostgresRepository(t *testing.T) *TestRepository {
	t.Helper()
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
	if err := admin.Ping(); err != nil {
		_ = admin.Close()
		t.Fatalf("ping PostgreSQL maintenance database: %v", err)
	}
	databaseName := fmt.Sprintf("coldkeep_ck015_lock_%d_%d", os.Getpid(), time.Now().UnixNano())
	if _, err := admin.Exec("CREATE DATABASE " + databaseName); err != nil {
		_ = admin.Close()
		t.Fatalf("create PostgreSQL CK-015 lock database %s: %v", databaseName, err)
	}
	t.Setenv("DB_NAME", databaseName)
	t.Setenv("COLDKEEP_DB_AUTO_BOOTSTRAP", "true")
	t.Setenv("COLDKEEP_REUSE_SEMANTIC_VALIDATION", "always")
	t.Setenv("COLDKEEP_COMPRESSION", "none")
	containersDir := t.TempDir()
	storageContext, err := OpenLocalStorage(containersDir)
	if err != nil {
		_, _ = admin.Exec("DROP DATABASE IF EXISTS " + databaseName)
		_ = admin.Close()
		t.Fatalf("open PostgreSQL lock-test storage: %v", err)
	}
	t.Cleanup(func() {
		_ = storageContext.Close()
		_, _ = admin.Exec(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()`, databaseName)
		_, _ = admin.Exec("DROP DATABASE IF EXISTS " + databaseName)
		_ = admin.Close()
	})
	return &TestRepository{
		DB: storageContext.DB, Storage: storageContext, ContainersDir: containersDir,
	}
}

func ckV11316015WaitForPostgresLockWaiter(ctx context.Context, dbconn *sql.DB, excludedPID int) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		if err := dbconn.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_stat_activity
				WHERE datname = current_database()
				  AND pid <> pg_backend_pid()
				  AND pid <> $1
				  AND wait_event_type = 'Lock'
			)`, excludedPID).Scan(&waiting); err != nil {
			return err
		}
		if waiting {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
