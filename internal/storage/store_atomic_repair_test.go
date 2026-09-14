package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/container"
	dbpkg "github.com/franchoy/coldkeep/internal/db"
	filestate "github.com/franchoy/coldkeep/internal/status"
	verifypkg "github.com/franchoy/coldkeep/internal/verify"
	"github.com/mattn/go-sqlite3"
)

var errCKV11316015InjectedRepairFailure = errors.New("CK-V11316-015 injected repair failure before publication")

type ckV11316015Fixture struct {
	repo          *TestRepository
	originalPath  string
	duplicatePath string
	fileID        int64
	containerPath string
	holdingPath   string
	payload       []byte
	codec         blocks.Codec
}

const ckV11316015InitialLookupQuery = "SELECT id, status FROM logical_file WHERE file_hash = $1 AND total_size = $2"

type ckV11316015LookupResponse uint8

const (
	ckV11316015LookupOperationalError ckV11316015LookupResponse = iota + 1
	ckV11316015LookupPartialScanError
	ckV11316015LookupNoRows
)

type ckV11316015LookupControl struct {
	mu             sync.Mutex
	armed          bool
	response       ckV11316015LookupResponse
	responseErr    error
	partialID      int64
	consumed       int
	lookupMatches  int
	fallbackClaims int
	observing      bool
}

func (control *ckV11316015LookupControl) arm(response ckV11316015LookupResponse, responseErr error, partialID int64) {
	control.mu.Lock()
	defer control.mu.Unlock()
	control.armed = true
	control.response = response
	control.responseErr = responseErr
	control.partialID = partialID
	control.observing = true
}

func (control *ckV11316015LookupControl) counts() (consumed, lookupMatches, fallbackClaims int) {
	control.mu.Lock()
	defer control.mu.Unlock()
	return control.consumed, control.lookupMatches, control.fallbackClaims
}

func (control *ckV11316015LookupControl) queryResponse(query string) (driver.Rows, error, bool) {
	normalized := strings.Join(strings.Fields(query), " ")
	control.mu.Lock()
	defer control.mu.Unlock()
	if control.observing && strings.HasPrefix(normalized, "INSERT INTO logical_file") {
		control.fallbackClaims++
	}
	if normalized != ckV11316015InitialLookupQuery {
		return nil, nil, false
	}
	if control.observing || control.armed {
		control.lookupMatches++
	}
	if !control.armed {
		return nil, nil, false
	}
	control.armed = false
	control.consumed++
	switch control.response {
	case ckV11316015LookupOperationalError:
		return nil, control.responseErr, true
	case ckV11316015LookupPartialScanError:
		return &ckV11316015InjectedRows{
			columns: []string{"id", "status"},
			values:  [][]driver.Value{{control.partialID, nil}},
		}, nil, true
	case ckV11316015LookupNoRows:
		return &ckV11316015InjectedRows{columns: []string{"id", "status"}}, nil, true
	default:
		return nil, fmt.Errorf("unknown CK-V11316-015 lookup response %d", control.response), true
	}
}

func (control *ckV11316015LookupControl) observeExec(query string) {
	normalized := strings.Join(strings.Fields(query), " ")
	control.mu.Lock()
	defer control.mu.Unlock()
	if control.observing && strings.HasPrefix(normalized, "INSERT INTO logical_file") {
		control.fallbackClaims++
	}
}

type ckV11316015InjectedRows struct {
	columns []string
	values  [][]driver.Value
	next    int
}

func (rows *ckV11316015InjectedRows) Columns() []string { return rows.columns }
func (rows *ckV11316015InjectedRows) Close() error      { return nil }
func (rows *ckV11316015InjectedRows) Next(dest []driver.Value) error {
	if rows.next >= len(rows.values) {
		return io.EOF
	}
	copy(dest, rows.values[rows.next])
	rows.next++
	return nil
}

type ckV11316015Connector struct {
	underlying driver.Connector
	control    *ckV11316015LookupControl
}

func (connector *ckV11316015Connector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := connector.underlying.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &ckV11316015Conn{Conn: conn, control: connector.control}, nil
}

func (connector *ckV11316015Connector) Driver() driver.Driver {
	return connector.underlying.Driver()
}

type ckV11316015DriverConnector struct {
	driver driver.Driver
	dsn    string
}

func (connector *ckV11316015DriverConnector) Connect(context.Context) (driver.Conn, error) {
	return connector.driver.Open(connector.dsn)
}

func (connector *ckV11316015DriverConnector) Driver() driver.Driver { return connector.driver }

type ckV11316015Conn struct {
	driver.Conn
	control *ckV11316015LookupControl
}

func (conn *ckV11316015Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if rows, err, intercepted := conn.control.queryResponse(query); intercepted {
		return rows, err
	}
	if queryer, ok := conn.Conn.(driver.QueryerContext); ok {
		return queryer.QueryContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

func (conn *ckV11316015Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	conn.control.observeExec(query)
	if execer, ok := conn.Conn.(driver.ExecerContext); ok {
		return execer.ExecContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

func (conn *ckV11316015Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if preparer, ok := conn.Conn.(driver.ConnPrepareContext); ok {
		return preparer.PrepareContext(ctx, query)
	}
	return conn.Prepare(query)
}

func (conn *ckV11316015Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if beginner, ok := conn.Conn.(driver.ConnBeginTx); ok {
		return beginner.BeginTx(ctx, opts)
	}
	return nil, fmt.Errorf("CK-V11316-015 lookup wrapper requires driver.ConnBeginTx")
}

func (conn *ckV11316015Conn) Ping(ctx context.Context) error {
	if pinger, ok := conn.Conn.(driver.Pinger); ok {
		return pinger.Ping(ctx)
	}
	return nil
}

func (conn *ckV11316015Conn) ResetSession(ctx context.Context) error {
	if resetter, ok := conn.Conn.(driver.SessionResetter); ok {
		return resetter.ResetSession(ctx)
	}
	return nil
}

func (conn *ckV11316015Conn) IsValid() bool {
	if validator, ok := conn.Conn.(driver.Validator); ok {
		return validator.IsValid()
	}
	return true
}

func (conn *ckV11316015Conn) CheckNamedValue(value *driver.NamedValue) error {
	if checker, ok := conn.Conn.(driver.NamedValueChecker); ok {
		return checker.CheckNamedValue(value)
	}
	return driver.ErrSkip
}

func TestCKV11316015InitialLookupOperationalErrorStopsStoreBeforeFallbackSQLite(t *testing.T) {
	errLookup := errors.New("CK-V11316-015 injected initial SQLite lookup failure")
	ckV11316015RunSQLiteLookupErrorTest(t, ckV11316015LookupOperationalError, errLookup)
}

func TestCKV11316015InitialLookupPartialScanErrorStopsStoreBeforeFallbackSQLite(t *testing.T) {
	ckV11316015RunSQLiteLookupErrorTest(t, ckV11316015LookupPartialScanError, nil)
}

func TestCKV11316015InitialLookupErrNoRowsPreservesNewObjectStoreSQLite(t *testing.T) {
	repo, control, _ := newCKV11316015WrappedSQLiteRepository(t)
	input := filepath.Join(t.TempDir(), "lookup-no-rows.bin")
	payload := bytesForCKV11316015("lookup-no-rows", 64*1024)
	if err := os.WriteFile(input, payload, 0o600); err != nil {
		t.Fatalf("write no-row control source: %v", err)
	}
	control.arm(ckV11316015LookupNoRows, nil, 0)
	result, err := StoreFileWithStorageContextAndCodecResult(repo.Storage, input, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("exact no-row response must preserve new-object Store: %v", err)
	}
	if result.AlreadyStored || result.FileID <= 0 {
		t.Fatalf("no-row control result=%+v, want one new logical object", result)
	}
	consumed, matches, fallback := control.counts()
	if consumed != 1 || matches != 1 || fallback != 1 {
		t.Fatalf("no-row routing counts consumed=%d matches=%d fallback=%d, want 1/1/1", consumed, matches, fallback)
	}
	var mappings int
	if err := repo.DB.QueryRow(`SELECT COUNT(*) FROM physical_file WHERE path = $1 AND logical_file_id = $2`, input, result.FileID).Scan(&mappings); err != nil {
		t.Fatalf("count no-row control mapping: %v", err)
	}
	if mappings != 1 {
		t.Fatalf("no-row control mappings=%d, want exactly 1", mappings)
	}
}

func TestCKV11316015InitialLookupSupportedStatusRoutingSQLite(t *testing.T) {
	t.Run("completed", func(t *testing.T) {
		repo, control, _ := newCKV11316015WrappedSQLiteRepository(t)
		fixture := newCKV11316015FixtureWithRepository(t, false, blocks.CodecPlain, repo)
		control.mu.Lock()
		control.observing = true
		control.mu.Unlock()
		result, err := StoreFileWithStorageContextAndCodecResult(repo.Storage, fixture.duplicatePath, blocks.CodecPlain)
		if err != nil || result.FileID != fixture.fileID || !result.AlreadyStored {
			t.Fatalf("COMPLETED routing result=%+v err=%v", result, err)
		}
		_, matches, fallback := control.counts()
		if matches != 1 || fallback != 0 {
			t.Fatalf("COMPLETED routing matches=%d fallback=%d, want 1/0", matches, fallback)
		}
	})

	t.Run("aborted", func(t *testing.T) {
		repo, control, _ := newCKV11316015WrappedSQLiteRepository(t)
		fixture := newCKV11316015FixtureWithRepository(t, false, blocks.CodecPlain, repo)
		if _, err := repo.DB.Exec(`UPDATE logical_file SET status = $1 WHERE id = $2`, filestate.LogicalFileAborted, fixture.fileID); err != nil {
			t.Fatalf("prepare ABORTED routing control: %v", err)
		}
		control.mu.Lock()
		control.observing = true
		control.mu.Unlock()
		result, err := StoreFileWithStorageContextAndCodecResult(repo.Storage, fixture.duplicatePath, blocks.CodecPlain)
		if err != nil || result.FileID != fixture.fileID || result.AlreadyStored {
			t.Fatalf("ABORTED routing result=%+v err=%v", result, err)
		}
		_, matches, fallback := control.counts()
		if matches != 1 || fallback == 0 {
			t.Fatalf("ABORTED routing matches=%d fallback=%d, want successful lookup and delegated claim", matches, fallback)
		}
	})

	t.Run("processing", func(t *testing.T) {
		repo, control, _ := newCKV11316015WrappedSQLiteRepository(t)
		fixture := newCKV11316015FixtureWithRepository(t, false, blocks.CodecPlain, repo)
		if _, err := repo.DB.Exec(`UPDATE logical_file SET status = $1 WHERE id = $2`, filestate.LogicalFileProcessing, fixture.fileID); err != nil {
			t.Fatalf("prepare PROCESSING routing control: %v", err)
		}
		control.mu.Lock()
		control.observing = true
		control.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		result, err := StoreFileWithStorageContextAndCodecResultContext(ctx, repo.Storage, fixture.duplicatePath, blocks.CodecPlain)
		if !errors.Is(err, context.DeadlineExceeded) || result.AlreadyStored {
			t.Fatalf("PROCESSING routing result=%+v err=%v, want bounded established wait", result, err)
		}
		_, matches, fallback := control.counts()
		if matches != 1 || fallback == 0 {
			t.Fatalf("PROCESSING routing matches=%d fallback=%d, want successful lookup and delegated claim", matches, fallback)
		}
	})
}

func TestCKV11316015FailedPackedRepairPreservesAuthoritativeGraph(t *testing.T) {
	fixture := newCKV11316015Fixture(t, false)
	before := ckV11316015AuthoritativeSnapshot(t, fixture.repo.DB, fixture.fileID)
	fixture.moveRequiredContainer(t)

	restoreHooks := InstallTestStoreInterleavingHooks(&fixture.repo.Storage, func(_ context.Context, event TestStoreInterleavingHookEvent) error {
		if event.Event == TestStoreInterleavingEventBeforeMarkChunkForRebuild {
			return errCKV11316015InjectedRepairFailure
		}
		return nil
	})
	defer restoreHooks()

	result, err := StoreFileWithStorageContextAndCodecResult(fixture.repo.Storage, fixture.duplicatePath, blocks.CodecPlain)
	if !errors.Is(err, errCKV11316015InjectedRepairFailure) {
		t.Fatalf("packed RED harness did not reach its exact injected repair failure: result=%+v err=%v", result, err)
	}
	if result.AlreadyStored {
		t.Fatalf("failed packed repair reported AlreadyStored=true: %+v", result)
	}
	after := ckV11316015AuthoritativeSnapshot(t, fixture.repo.DB, fixture.fileID)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("CK-V11316-015 failed packed repair mutated authoritative graph\nbefore=%v\nafter=%v", before, after)
	}
}

func TestCKV11316015FailedLegacyRepairPreservesAuthoritativeGraph(t *testing.T) {
	fixture := newCKV11316015Fixture(t, true)
	before := ckV11316015AuthoritativeSnapshot(t, fixture.repo.DB, fixture.fileID)
	fixture.moveRequiredContainer(t)

	restoreHooks := InstallTestStoreInterleavingHooks(&fixture.repo.Storage, func(_ context.Context, event TestStoreInterleavingHookEvent) error {
		if event.Event == TestStoreInterleavingEventBeforeMarkChunkForRebuild {
			return errCKV11316015InjectedRepairFailure
		}
		return nil
	})
	defer restoreHooks()

	result, err := StoreFileWithStorageContextAndCodecResult(fixture.repo.Storage, fixture.duplicatePath, blocks.CodecPlain)
	if !errors.Is(err, errCKV11316015InjectedRepairFailure) {
		t.Fatalf("legacy RED harness did not reach its exact injected repair failure: result=%+v err=%v", result, err)
	}
	if result.AlreadyStored {
		t.Fatalf("failed legacy repair reported AlreadyStored=true: %+v", result)
	}
	after := ckV11316015AuthoritativeSnapshot(t, fixture.repo.DB, fixture.fileID)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("CK-V11316-015 failed legacy repair mutated authoritative graph\nbefore=%v\nafter=%v", before, after)
	}
}

func TestCKV11316015FailureAfterDurablePreparationPreservesAndRecovers(t *testing.T) {
	fixture := newCKV11316015Fixture(t, false)
	before := ckV11316015AuthoritativeSnapshot(t, fixture.repo.DB, fixture.fileID)
	fixture.moveRequiredContainer(t)

	restoreHooks := InstallTestStoreInterleavingHooks(&fixture.repo.Storage, func(_ context.Context, event TestStoreInterleavingHookEvent) error {
		if event.Event == TestStoreInterleavingEventBeforeRepairPublication {
			return errCKV11316015InjectedRepairFailure
		}
		return nil
	})
	defer restoreHooks()

	result, err := StoreFileWithStorageContextAndCodecResult(fixture.repo.Storage, fixture.duplicatePath, blocks.CodecPlain)
	if !errors.Is(err, errCKV11316015InjectedRepairFailure) {
		t.Fatalf("durable-preparation failure seam not reached: result=%+v err=%v", result, err)
	}
	after := ckV11316015AuthoritativeSnapshot(t, fixture.repo.DB, fixture.fileID)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("post-preparation failure mutated authoritative graph\nbefore=%v\nafter=%v", before, after)
	}

	var attemptID int64
	var attemptStatus, stagingFilename string
	var quarantine bool
	if err := fixture.repo.DB.QueryRow(`
		SELECT a.id, a.status, c.filename, c.quarantine
		FROM store_repair_attempt a
		JOIN store_repair_container rc ON rc.attempt_id = a.id
		JOIN container c ON c.id = rc.container_id
		WHERE a.logical_file_id = $1
		ORDER BY a.id DESC LIMIT 1`, fixture.fileID,
	).Scan(&attemptID, &attemptStatus, &stagingFilename, &quarantine); err != nil {
		t.Fatalf("load failed durable repair state: %v", err)
	}
	if attemptStatus != "ABORTED" || !quarantine {
		t.Fatalf("failed staged repair state=(%s, quarantine=%t), want ABORTED quarantined", attemptStatus, quarantine)
	}
	stagingPath := filepath.Join(fixture.repo.ContainersDir, stagingFilename)
	if _, err := os.Stat(stagingPath); err != nil {
		t.Fatalf("durable attempt-owned staging file missing before recovery: %v", err)
	}

	fixture.restoreRequiredContainer(t)
	assertCKV11316015Restore(t, fixture, fixture.originalPath, "original-after-failed-preparation.bin")
}

func TestCKV11316015PublicationRollbackRestoresTentativelyRetiredGraph(t *testing.T) {
	fixture := newCKV11316015Fixture(t, false)
	before := ckV11316015AuthoritativeSnapshot(t, fixture.repo.DB, fixture.fileID)
	fixture.moveRequiredContainer(t)

	restoreHooks := InstallTestStoreInterleavingHooks(&fixture.repo.Storage, func(_ context.Context, event TestStoreInterleavingHookEvent) error {
		if event.Event == TestStoreInterleavingEventAfterRepairRetirement {
			return errCKV11316015InjectedRepairFailure
		}
		return nil
	})
	defer restoreHooks()

	result, err := StoreFileWithStorageContextAndCodecResult(fixture.repo.Storage, fixture.duplicatePath, blocks.CodecPlain)
	if !errors.Is(err, errCKV11316015InjectedRepairFailure) {
		t.Fatalf("publication rollback seam not reached: result=%+v err=%v", result, err)
	}
	after := ckV11316015AuthoritativeSnapshot(t, fixture.repo.DB, fixture.fileID)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("rolled-back publication changed authoritative graph\nbefore=%v\nafter=%v", before, after)
	}
	fixture.restoreRequiredContainer(t)
	assertCKV11316015Restore(t, fixture, fixture.originalPath, "original-after-publication-rollback.bin")
}

func TestCKV11316015SharedPackedRepairPublishesWithoutHarmingOtherMembers(t *testing.T) {
	fixture := newCKV11316015Fixture(t, false)
	unrelatedPath, unrelatedPayload := seedCKV11316015UnrelatedSharedLogical(t, fixture)
	beforeRecipe := ckV11316015QueryStrings(t, fixture.repo.DB,
		`SELECT chunk_id || '|' || chunk_order FROM file_chunk WHERE logical_file_id = $1 ORDER BY chunk_order`, fixture.fileID)
	fixture.moveRequiredContainer(t)

	result, err := StoreFileWithStorageContextAndCodecResult(fixture.repo.Storage, fixture.duplicatePath, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("shared packed repair must replace the complete physical unit instead of refusing one member: %v", err)
	}
	if result.FileID != fixture.fileID || result.AlreadyStored {
		t.Fatalf("shared packed repair returned unexpected identity: %+v", result)
	}
	afterRecipe := ckV11316015QueryStrings(t, fixture.repo.DB,
		`SELECT chunk_id || '|' || chunk_order FROM file_chunk WHERE logical_file_id = $1 ORDER BY chunk_order`, fixture.fileID)
	if !reflect.DeepEqual(beforeRecipe, afterRecipe) {
		t.Fatalf("repair replaced the authoritative ordered recipe: before=%v after=%v", beforeRecipe, afterRecipe)
	}
	assertCKV11316015Restore(t, fixture, fixture.originalPath, "original-after-repair.bin")
	assertCKV11316015Restore(t, fixture, fixture.duplicatePath, "duplicate-after-repair.bin")
	assertCKV11316015StoredPathBytes(t, fixture.repo.Storage, unrelatedPath, unrelatedPayload, "unrelated-shared-after-repair.bin")
	fixture.restoreRequiredContainer(t)
	if err := verifypkg.VerifyRepository(fixture.repo.DB, fixture.repo.ContainersDir); err != nil {
		t.Fatalf("published repair must preserve exact active-plus-retired packed membership: %v", err)
	}
}

func TestCKV11316015RepeatedAbortedChunkRepairsOneEntity(t *testing.T) {
	t.Setenv("COLDKEEP_REUSE_SEMANTIC_VALIDATION", "always")
	left := bytesForCKV11316015("repeated-left", 64*1024)
	right := bytesForCKV11316015("repeated-right", 64*1024)
	payload := append(append(append([]byte(nil), left...), left...), right...)
	repo := NewTestRepository(t)
	repo.Storage.Chunker = scriptedChunker{
		version:  chunk.VersionV1SimpleRolling,
		payloads: [][]byte{left, left, right},
	}
	inputDir := t.TempDir()
	originalPath := filepath.Join(inputDir, "repeated-original.bin")
	duplicatePath := filepath.Join(inputDir, "repeated-duplicate.bin")
	if err := os.WriteFile(originalPath, payload, 0o600); err != nil {
		t.Fatalf("write repeated original: %v", err)
	}
	if err := os.WriteFile(duplicatePath, payload, 0o600); err != nil {
		t.Fatalf("write repeated duplicate: %v", err)
	}
	stored, err := StoreFileWithStorageContextAndCodecResult(repo.Storage, originalPath, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("store repeated recipe: %v", err)
	}
	recipeBefore := ckV11316015QueryStrings(t, repo.DB,
		`SELECT chunk_id || '|' || chunk_order FROM file_chunk WHERE logical_file_id = $1 ORDER BY chunk_order`, stored.FileID)
	if len(recipeBefore) != 3 {
		t.Fatalf("repeated recipe entries=%v, want three", recipeBefore)
	}
	var repeatedID, repeatedAgainID, unchangedID int64
	rows, err := repo.DB.Query(`SELECT chunk_id FROM file_chunk WHERE logical_file_id = $1 ORDER BY chunk_order`, stored.FileID)
	if err != nil {
		t.Fatalf("query repeated recipe IDs: %v", err)
	}
	ids := []*int64{&repeatedID, &repeatedAgainID, &unchangedID}
	for index := 0; rows.Next(); index++ {
		if index >= len(ids) {
			_ = rows.Close()
			t.Fatal("repeated recipe returned too many rows")
		}
		if err := rows.Scan(ids[index]); err != nil {
			_ = rows.Close()
			t.Fatalf("scan repeated recipe ID: %v", err)
		}
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close repeated recipe rows: %v", err)
	}
	if repeatedID != repeatedAgainID || repeatedID == unchangedID {
		t.Fatalf("recipe IDs=(%d,%d,%d), want first two repeated and third distinct", repeatedID, repeatedAgainID, unchangedID)
	}

	var logicalBefore, repeatedBefore, unchangedBefore int64
	if err := repo.DB.QueryRow(`SELECT retry_count FROM logical_file WHERE id = $1`, stored.FileID).Scan(&logicalBefore); err != nil {
		t.Fatalf("query logical retry before repeated repair: %v", err)
	}
	if err := repo.DB.QueryRow(`SELECT retry_count FROM chunk WHERE id = $1`, repeatedID).Scan(&repeatedBefore); err != nil {
		t.Fatalf("query repeated chunk retry before repair: %v", err)
	}
	if err := repo.DB.QueryRow(`SELECT retry_count FROM chunk WHERE id = $1`, unchangedID).Scan(&unchangedBefore); err != nil {
		t.Fatalf("query unchanged chunk retry before repair: %v", err)
	}
	unchangedPlacement := ckV11316015QueryStrings(t, repo.DB,
		`SELECT block_id || '|' || offset_in_block || '|' || size_in_block FROM chunk_block_refs WHERE chunk_id = $1`, unchangedID)
	if _, err := repo.DB.Exec(`UPDATE chunk SET status = $1 WHERE id = $2`, filestate.ChunkAborted, repeatedID); err != nil {
		t.Fatalf("mark repeated chunk aborted: %v", err)
	}

	result, err := StoreFileWithStorageContextAndCodecResult(repo.Storage, duplicatePath, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("repair repeated ABORTED chunk: %v", err)
	}
	if result.FileID != stored.FileID || result.AlreadyStored {
		t.Fatalf("repeated repair result=%+v", result)
	}
	var logicalAfter, repeatedAfter, unchangedAfter int64
	var repeatedStatus string
	if err := repo.DB.QueryRow(`SELECT retry_count FROM logical_file WHERE id = $1`, stored.FileID).Scan(&logicalAfter); err != nil {
		t.Fatalf("query logical retry after repeated repair: %v", err)
	}
	if err := repo.DB.QueryRow(`SELECT status, retry_count FROM chunk WHERE id = $1`, repeatedID).Scan(&repeatedStatus, &repeatedAfter); err != nil {
		t.Fatalf("query repeated chunk after repair: %v", err)
	}
	if err := repo.DB.QueryRow(`SELECT retry_count FROM chunk WHERE id = $1`, unchangedID).Scan(&unchangedAfter); err != nil {
		t.Fatalf("query unchanged chunk after repair: %v", err)
	}
	if logicalAfter != logicalBefore+1 || repeatedAfter != repeatedBefore+1 || unchangedAfter != unchangedBefore {
		t.Fatalf("retry counts logical=%d->%d repeated=%d->%d unchanged=%d->%d",
			logicalBefore, logicalAfter, repeatedBefore, repeatedAfter, unchangedBefore, unchangedAfter)
	}
	if repeatedStatus != filestate.ChunkCompleted {
		t.Fatalf("repeated repaired status=%s, want COMPLETED", repeatedStatus)
	}
	recipeAfter := ckV11316015QueryStrings(t, repo.DB,
		`SELECT chunk_id || '|' || chunk_order FROM file_chunk WHERE logical_file_id = $1 ORDER BY chunk_order`, stored.FileID)
	if !reflect.DeepEqual(recipeBefore, recipeAfter) {
		t.Fatalf("repeated repair changed recipe: before=%v after=%v", recipeBefore, recipeAfter)
	}
	if got := ckV11316015QueryStrings(t, repo.DB,
		`SELECT block_id || '|' || offset_in_block || '|' || size_in_block FROM chunk_block_refs WHERE chunk_id = $1`, unchangedID); !reflect.DeepEqual(got, unchangedPlacement) {
		t.Fatalf("repeated repair changed healthy placement: before=%v after=%v", unchangedPlacement, got)
	}
	var stagedRows int
	if err := repo.DB.QueryRow(`
		SELECT COUNT(*) FROM store_repair_chunk rc
		JOIN store_repair_attempt ra ON ra.id = rc.attempt_id
		WHERE ra.logical_file_id = $1 AND ra.status = 'PUBLISHED'`, stored.FileID).Scan(&stagedRows); err != nil {
		t.Fatalf("count repeated repair staging rows: %v", err)
	}
	if stagedRows != 0 {
		t.Fatalf("status-only repeated repair staged rows=%d, want zero", stagedRows)
	}
	fixture := &ckV11316015Fixture{
		repo: repo, originalPath: originalPath, duplicatePath: duplicatePath,
		fileID: stored.FileID, payload: payload,
	}
	assertCKV11316015Restore(t, fixture, duplicatePath, "repeated-after-repair.bin")
}

func seedCKV11316015UnrelatedSharedLogical(t *testing.T, fixture *ckV11316015Fixture) (string, []byte) {
	t.Helper()
	sharedPayload := append([]byte(nil), fixture.payload[len(fixture.payload)/2:]...)
	digest := sha256.Sum256(sharedPayload)
	var chunkID int64
	if err := fixture.repo.DB.QueryRow(`
		SELECT chunk_id FROM file_chunk
		WHERE logical_file_id = $1 AND chunk_order = 1`, fixture.fileID,
	).Scan(&chunkID); err != nil {
		t.Fatalf("load shared chunk for unrelated logical file: %v", err)
	}
	var logicalID int64
	if err := fixture.repo.DB.QueryRow(`
		INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		VALUES ('unrelated-shared.bin', $1, $2, $3, 1, $4)
		RETURNING id`, int64(len(sharedPayload)), hex.EncodeToString(digest[:]),
		filestate.LogicalFileCompleted, chunk.VersionV1SimpleRolling,
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert unrelated shared logical file: %v", err)
	}
	unrelatedPath := filepath.Join(filepath.Dir(fixture.originalPath), "unrelated-shared.bin")
	tx, err := fixture.repo.DB.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin unrelated shared mapping transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1,$2,0)`, logicalID, chunkID); err != nil {
		t.Fatalf("insert unrelated shared recipe: %v", err)
	}
	if _, err := tx.Exec(`INSERT INTO physical_file (path, logical_file_id, is_metadata_complete) VALUES ($1,$2,0)`, unrelatedPath, logicalID); err != nil {
		t.Fatalf("insert unrelated shared mapping: %v", err)
	}
	if _, err := tx.Exec(`UPDATE chunk SET live_ref_count = live_ref_count + 1 WHERE id = $1`, chunkID); err != nil {
		t.Fatalf("increment unrelated shared chunk liveness: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit unrelated shared logical file: %v", err)
	}
	assertCKV11316015StoredPathBytes(t, fixture.repo.Storage, unrelatedPath, sharedPayload, "unrelated-shared-healthy.bin")
	return unrelatedPath, sharedPayload
}

func assertCKV11316015StoredPathBytes(t *testing.T, sgctx StorageContext, storedPath string, want []byte, name string) {
	t.Helper()
	destination := filepath.Join(t.TempDir(), name)
	if _, err := RestoreFileByStoredPathWithStorageContextResultOptions(sgctx, storedPath, RestoreOptions{
		Overwrite:       true,
		DestinationMode: RestoreDestinationOverride,
		Destination:     destination,
		NoMetadata:      true,
	}); err != nil {
		t.Fatalf("restore stored path %s: %v", storedPath, err)
	}
	got, err := os.ReadFile(destination)
	if err != nil {
		t.Fatalf("read restored stored path %s: %v", storedPath, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("restored stored path %s differs: got=%d want=%d", storedPath, len(got), len(want))
	}
}

func TestCKV11316015ConcurrentRepairsPublishOneAuthoritativeReplacement(t *testing.T) {
	fixture := newCKV11316015Fixture(t, false)
	fixture.repo.DB.SetMaxOpenConns(1)
	fixture.repo.DB.SetMaxIdleConns(1)
	thirdPath := filepath.Join(filepath.Dir(fixture.duplicatePath), "third.bin")
	if err := os.WriteFile(thirdPath, fixture.payload, 0o600); err != nil {
		t.Fatalf("write third repair source: %v", err)
	}
	fixture.moveRequiredContainer(t)

	paths := []string{fixture.duplicatePath, thirdPath}
	results := make([]StoreFileResult, len(paths))
	errs := make([]error, len(paths))
	workerContexts := make([]StorageContext, len(paths))
	workerWriters := make([]*container.LocalWriter, len(paths))
	for index := range paths {
		workerContexts[index] = ckV11316015ConcurrentStorageContext(fixture)
		writer, ok := workerContexts[index].Writer.(*container.LocalWriter)
		if !ok || writer == nil {
			t.Fatalf("concurrent repair worker %d writer=%T, want non-nil *container.LocalWriter", index, workerContexts[index].Writer)
		}
		workerWriters[index] = writer
		if workerContexts[index].DB != fixture.repo.DB {
			t.Fatalf("concurrent repair worker %d does not share fixture database", index)
		}
		if workerContexts[index].EffectiveContainerDir() != fixture.repo.ContainersDir || writer.Dir() != fixture.repo.ContainersDir {
			t.Fatalf("concurrent repair worker %d container directories=(context=%q writer=%q), want %q",
				index, workerContexts[index].EffectiveContainerDir(), writer.Dir(), fixture.repo.ContainersDir)
		}
	}
	if workerWriters[0] == workerWriters[1] {
		t.Fatal("concurrent repair workers share one LocalWriter")
	}
	start := make(chan struct{})
	var workers sync.WaitGroup
	for index := range paths {
		workers.Add(1)
		go func(index int, storeContext StorageContext) {
			defer workers.Done()
			<-start
			results[index], errs[index] = StoreFileWithStorageContextAndCodecResult(storeContext, paths[index], blocks.CodecPlain)
		}(index, workerContexts[index])
	}
	close(start)
	workers.Wait()
	for index, writer := range workerWriters {
		if _, _, active := writer.ActiveContainerState(); active {
			t.Fatalf("concurrent repair worker %d retained an active writer after Store cleanup", index)
		}
	}
	for index, err := range errs {
		if err != nil {
			t.Fatalf("concurrent repair %d failed: %v", index, err)
		}
		if results[index].FileID != fixture.fileID {
			t.Fatalf("concurrent repair %d changed logical identity: %+v", index, results[index])
		}
		assertCKV11316015Restore(t, fixture, paths[index], fmt.Sprintf("concurrent-%d.bin", index))
	}
	var published int
	if err := fixture.repo.DB.QueryRow(`SELECT COUNT(*) FROM store_repair_attempt WHERE logical_file_id = $1 AND status = 'PUBLISHED'`, fixture.fileID).Scan(&published); err != nil {
		t.Fatalf("count published concurrent repairs: %v", err)
	}
	if published != 1 {
		t.Fatalf("published repair attempts=%d, want exactly one", published)
	}
	fixture.restoreRequiredContainer(t)
	if err := verifypkg.VerifyRepository(fixture.repo.DB, fixture.repo.ContainersDir); err != nil {
		t.Fatalf("concurrent repair membership verification: %v", err)
	}
}

func TestCKV11316015ProcessingCompletionWaitsOutsideRepairLockAndReuses(t *testing.T) {
	fixture := newCKV11316015ConcurrentFixture(t)
	t.Setenv("COLDKEEP_REUSE_SEMANTIC_VALIDATION", "suspicious")
	chunkID := ckV11316015FirstRecipeChunkID(t, fixture.repo.DB, fixture.fileID)
	var logicalRetryBefore, chunkRetryBefore int64
	if err := fixture.repo.DB.QueryRow(`SELECT retry_count FROM logical_file WHERE id = $1`, fixture.fileID).Scan(&logicalRetryBefore); err != nil {
		t.Fatalf("query logical retry before contention: %v", err)
	}
	if err := fixture.repo.DB.QueryRow(`SELECT retry_count FROM chunk WHERE id = $1`, chunkID).Scan(&chunkRetryBefore); err != nil {
		t.Fatalf("query chunk retry before contention: %v", err)
	}
	if _, err := fixture.repo.DB.Exec(`UPDATE chunk SET status = $1 WHERE id = $2`, filestate.ChunkProcessing, chunkID); err != nil {
		t.Fatalf("mark recipe chunk PROCESSING: %v", err)
	}

	type outcome struct {
		result StoreFileResult
		err    error
	}
	start := make(chan struct{})
	outcomes := make(chan outcome, 2)
	paths := []string{fixture.duplicatePath, filepath.Join(filepath.Dir(fixture.duplicatePath), "same-logical-third.bin")}
	if err := os.WriteFile(paths[1], fixture.payload, 0o600); err != nil {
		t.Fatalf("write same-logical contender: %v", err)
	}
	for _, path := range paths {
		storeContext := ckV11316015ConcurrentStorageContext(fixture)
		go func(path string, storeContext StorageContext) {
			<-start
			result, err := StoreFileWithStorageContextAndCodecResult(storeContext, path, blocks.CodecPlain)
			outcomes <- outcome{result: result, err: err}
		}(path, storeContext)
	}
	close(start)

	select {
	case got := <-outcomes:
		t.Fatalf("same-logical Store returned before PROCESSING became terminal: result=%+v err=%v", got.result, got.err)
	case <-time.After(75 * time.Millisecond):
	}
	if _, err := fixture.repo.DB.Exec(`UPDATE chunk SET status = $1 WHERE id = $2`, filestate.ChunkCompleted, chunkID); err != nil {
		t.Fatalf("complete competing chunk claim: %v", err)
	}
	for range paths {
		select {
		case got := <-outcomes:
			if got.err != nil || got.result.FileID != fixture.fileID || !got.result.AlreadyStored {
				t.Fatalf("same-logical contention did not converge to healthy reuse: result=%+v err=%v", got.result, got.err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("same-logical contention deadlocked")
		}
	}
	var logicalRetryAfter, chunkRetryAfter int64
	if err := fixture.repo.DB.QueryRow(`SELECT retry_count FROM logical_file WHERE id = $1`, fixture.fileID).Scan(&logicalRetryAfter); err != nil {
		t.Fatalf("query logical retry after contention: %v", err)
	}
	if err := fixture.repo.DB.QueryRow(`SELECT retry_count FROM chunk WHERE id = $1`, chunkID).Scan(&chunkRetryAfter); err != nil {
		t.Fatalf("query chunk retry after contention: %v", err)
	}
	if logicalRetryAfter != logicalRetryBefore || chunkRetryAfter != chunkRetryBefore {
		t.Fatalf("healthy winner gained waiter retry increments: logical=%d->%d chunk=%d->%d", logicalRetryBefore, logicalRetryAfter, chunkRetryBefore, chunkRetryAfter)
	}
	ckV11316015AssertNoLiveRepairState(t, fixture.repo.DB, fixture.fileID)
}

func TestCKV11316015SharedChunkHealingBetweenValidationAndPlanReclassifies(t *testing.T) {
	fixture := newCKV11316015ConcurrentFixture(t)
	sharedChunkID := ckV11316015FirstRecipeChunkID(t, fixture.repo.DB, fixture.fileID)
	var logicalRetryBefore, chunkRetryBefore int64
	if err := fixture.repo.DB.QueryRow(`SELECT retry_count FROM logical_file WHERE id = $1`, fixture.fileID).Scan(&logicalRetryBefore); err != nil {
		t.Fatalf("query logical retry before pre-plan healing: %v", err)
	}
	if err := fixture.repo.DB.QueryRow(`SELECT retry_count FROM chunk WHERE id = $1`, sharedChunkID).Scan(&chunkRetryBefore); err != nil {
		t.Fatalf("query chunk retry before pre-plan healing: %v", err)
	}
	if _, err := fixture.repo.DB.Exec(`UPDATE chunk SET status = $1, retry_count = retry_count + 1 WHERE id = $2`, filestate.ChunkProcessing, sharedChunkID); err != nil {
		t.Fatalf("claim shared chunk for competing Store: %v", err)
	}
	loserStorage := ckV11316015ConcurrentStorageContext(fixture)
	var validationOnce sync.Once
	restoreLoserHooks := InstallTestStoreInterleavingHooks(&loserStorage, func(_ context.Context, event TestStoreInterleavingHookEvent) error {
		if event.Event == TestStoreInterleavingEventAfterCompletedRepairValidationRejected {
			var transitionErr error
			validationOnce.Do(func() {
				_, transitionErr = fixture.repo.DB.Exec(`UPDATE chunk SET status = $1 WHERE id = $2`, filestate.ChunkCompleted, sharedChunkID)
			})
			return transitionErr
		}
		return nil
	})
	defer restoreLoserHooks()

	loserResult, loserErr := StoreFileWithStorageContextAndCodecResult(loserStorage, fixture.duplicatePath, blocks.CodecPlain)
	if loserErr != nil {
		t.Fatalf("healed loser did not reclassify to healthy reuse: %v", loserErr)
	}
	if loserResult.FileID != fixture.fileID || !loserResult.AlreadyStored {
		t.Fatalf("healed loser result=%+v, want healthy reuse of %d", loserResult, fixture.fileID)
	}
	writer, ok := loserStorage.Writer.(*container.LocalWriter)
	if !ok || writer == nil {
		t.Fatalf("convergence loser writer=%T", loserStorage.Writer)
	}
	if _, _, active := writer.ActiveContainerState(); active {
		t.Fatal("convergence loser retained an active writer")
	}
	var logicalRetryAfter, chunkRetryAfter int64
	if err := fixture.repo.DB.QueryRow(`SELECT retry_count FROM logical_file WHERE id = $1`, fixture.fileID).Scan(&logicalRetryAfter); err != nil {
		t.Fatalf("query logical retry after pre-plan healing: %v", err)
	}
	if err := fixture.repo.DB.QueryRow(`SELECT retry_count FROM chunk WHERE id = $1`, sharedChunkID).Scan(&chunkRetryAfter); err != nil {
		t.Fatalf("query chunk retry after pre-plan healing: %v", err)
	}
	if logicalRetryAfter != logicalRetryBefore || chunkRetryAfter != chunkRetryBefore+1 {
		t.Fatalf("pre-plan healing retry accounting logical=%d->%d chunk=%d->%d", logicalRetryBefore, logicalRetryAfter, chunkRetryBefore, chunkRetryAfter)
	}
	ckV11316015AssertNoLiveRepairState(t, fixture.repo.DB, fixture.fileID)
	assertCKV11316015Restore(t, fixture, fixture.duplicatePath, "convergence-loser.bin")
}

func TestCKV11316015ZeroEntityReclassifiesFreshAbortedState(t *testing.T) {
	fixture := newCKV11316015ConcurrentFixture(t)
	chunkID := ckV11316015FirstRecipeChunkID(t, fixture.repo.DB, fixture.fileID)
	var logicalBefore, chunkBefore int64
	if err := fixture.repo.DB.QueryRow(`SELECT retry_count FROM logical_file WHERE id = $1`, fixture.fileID).Scan(&logicalBefore); err != nil {
		t.Fatalf("query logical retry before zero-to-aborted: %v", err)
	}
	if err := fixture.repo.DB.QueryRow(`SELECT retry_count FROM chunk WHERE id = $1`, chunkID).Scan(&chunkBefore); err != nil {
		t.Fatalf("query chunk retry before zero-to-aborted: %v", err)
	}
	if _, err := fixture.repo.DB.Exec(`UPDATE chunk SET status = $1, retry_count = retry_count + 1 WHERE id = $2`, filestate.ChunkProcessing, chunkID); err != nil {
		t.Fatalf("claim zero-to-aborted chunk: %v", err)
	}
	storageContext := ckV11316015ConcurrentStorageContext(fixture)
	var validationOnce, zeroOnce sync.Once
	restoreHooks := InstallTestStoreInterleavingHooks(&storageContext, func(_ context.Context, event TestStoreInterleavingHookEvent) error {
		var hookErr error
		switch event.Event {
		case TestStoreInterleavingEventAfterCompletedRepairValidationRejected:
			validationOnce.Do(func() {
				_, hookErr = fixture.repo.DB.Exec(`UPDATE chunk SET status = $1 WHERE id = $2`, filestate.ChunkCompleted, chunkID)
			})
		case TestStoreInterleavingEventAfterCompletedRepairZeroEntity:
			zeroOnce.Do(func() {
				_, hookErr = fixture.repo.DB.Exec(`UPDATE chunk SET status = $1 WHERE id = $2`, filestate.ChunkAborted, chunkID)
			})
		}
		return hookErr
	})
	defer restoreHooks()

	result, err := StoreFileWithStorageContextAndCodecResult(storageContext, fixture.duplicatePath, blocks.CodecPlain)
	if err != nil || result.FileID != fixture.fileID || result.AlreadyStored {
		t.Fatalf("zero-to-aborted did not build a fresh repair: result=%+v err=%v", result, err)
	}
	var logicalAfter, chunkAfter int64
	var status string
	if err := fixture.repo.DB.QueryRow(`SELECT retry_count FROM logical_file WHERE id = $1`, fixture.fileID).Scan(&logicalAfter); err != nil {
		t.Fatalf("query logical retry after zero-to-aborted: %v", err)
	}
	if err := fixture.repo.DB.QueryRow(`SELECT status, retry_count FROM chunk WHERE id = $1`, chunkID).Scan(&status, &chunkAfter); err != nil {
		t.Fatalf("query chunk after zero-to-aborted: %v", err)
	}
	if status != filestate.ChunkCompleted || logicalAfter != logicalBefore+1 || chunkAfter != chunkBefore+2 {
		t.Fatalf("zero-to-aborted state=%s logical=%d->%d chunk=%d->%d", status, logicalBefore, logicalAfter, chunkBefore, chunkAfter)
	}
	ckV11316015AssertNoLiveRepairState(t, fixture.repo.DB, fixture.fileID)
	assertCKV11316015Restore(t, fixture, fixture.duplicatePath, "zero-to-aborted.bin")
}

func TestCKV11316015OperationalValidationErrorDoesNotEnterConvergence(t *testing.T) {
	fixture := newCKV11316015ConcurrentFixture(t)
	t.Setenv("COLDKEEP_REUSE_SEMANTIC_VALIDATION", "invalid-zero-signature-mode")
	storageContext := ckV11316015ConcurrentStorageContext(fixture)
	zeroCount := 0
	restoreHooks := InstallTestStoreInterleavingHooks(&storageContext, func(_ context.Context, event TestStoreInterleavingHookEvent) error {
		if event.Event == TestStoreInterleavingEventAfterCompletedRepairZeroEntity {
			zeroCount++
		}
		return nil
	})
	defer restoreHooks()
	before := ckV11316015AuthoritativeSnapshot(t, fixture.repo.DB, fixture.fileID)
	_, err := StoreFileWithStorageContextAndCodecResult(storageContext, fixture.duplicatePath, blocks.CodecPlain)
	if err == nil || !strings.Contains(err.Error(), "invalid-zero-signature-mode") {
		t.Fatalf("operational validation error=%v, want configuration cause", err)
	}
	if zeroCount != 0 {
		t.Fatalf("operational validation entered zero convergence %d times", zeroCount)
	}
	after := ckV11316015AuthoritativeSnapshot(t, fixture.repo.DB, fixture.fileID)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("operational validation failure mutated authority\nbefore=%v\nafter=%v", before, after)
	}
}

func TestCKV11316015StableZeroStateMachineRequiresBackoffAndThirdClassification(t *testing.T) {
	var stability completedRepairZeroStability
	displaced := &completedRepairZeroDecisionError{signature: "canonical-a", freshCause: errors.New("fresh-a")}
	if got := advanceCompletedRepairZeroStability(&stability, displaced); got != completedRepairZeroRestartImmediate {
		t.Fatalf("first zero action=%d, want immediate restart", got)
	}
	if got := advanceCompletedRepairZeroStability(&stability, displaced); got != completedRepairZeroRestartAfterBackoff {
		t.Fatalf("second zero action=%d, want bounded backoff", got)
	}
	stability.backoffUsed = true
	if got := advanceCompletedRepairZeroStability(&stability, displaced); got != completedRepairZeroStableFailClosed {
		t.Fatalf("third stable zero action=%d, want fail closed", got)
	}

	changed := &completedRepairZeroDecisionError{signature: "canonical-b", freshCause: errors.New("fresh-b")}
	if got := advanceCompletedRepairZeroStability(&stability, changed); got != completedRepairZeroRestartAfterBackoff {
		t.Fatalf("changed current state action=%d, want bounded reclassification", got)
	}
	if stability.count != 1 || stability.backoffUsed {
		t.Fatalf("changed current state stability=%+v, want reset signature count", stability)
	}

	changed.concurrentEvidence = true
	if got := advanceCompletedRepairZeroStability(&stability, changed); got != completedRepairZeroRestartAfterBackoff {
		t.Fatalf("concurrent evidence action=%d, want bounded reclassification", got)
	}
}

func TestCKV11316015SharedProcessingChunkWaitersConvergeWithoutDeadlock(t *testing.T) {
	fixture := newCKV11316015ConcurrentFixture(t)
	t.Setenv("COLDKEEP_REUSE_SEMANTIC_VALIDATION", "suspicious")
	unrelatedPath, unrelatedPayload := seedCKV11316015UnrelatedSharedLogical(t, fixture)
	if err := os.WriteFile(unrelatedPath, unrelatedPayload, 0o600); err != nil {
		t.Fatalf("write unrelated shared Store source: %v", err)
	}
	var unrelatedID, sharedChunkID int64
	if err := fixture.repo.DB.QueryRow(`SELECT logical_file_id FROM physical_file WHERE path = $1`, unrelatedPath).Scan(&unrelatedID); err != nil {
		t.Fatalf("load unrelated shared logical identity: %v", err)
	}
	if err := fixture.repo.DB.QueryRow(`SELECT chunk_id FROM file_chunk WHERE logical_file_id = $1`, unrelatedID).Scan(&sharedChunkID); err != nil {
		t.Fatalf("load shared chunk identity: %v", err)
	}
	if _, err := fixture.repo.DB.Exec(`UPDATE chunk SET status = $1 WHERE id = $2`, filestate.ChunkProcessing, sharedChunkID); err != nil {
		t.Fatalf("mark shared chunk PROCESSING: %v", err)
	}

	type outcome struct {
		result StoreFileResult
		err    error
	}
	outcomes := make(chan outcome, 2)
	unrelatedStorage := ckV11316015ConcurrentStorageContext(fixture)
	unrelatedStorage.Chunker = scriptedChunker{
		version:  chunk.VersionV1SimpleRolling,
		payloads: [][]byte{unrelatedPayload},
	}
	stores := []struct {
		storage StorageContext
		path    string
	}{
		{storage: ckV11316015ConcurrentStorageContext(fixture), path: fixture.duplicatePath},
		{storage: unrelatedStorage, path: unrelatedPath},
	}
	for _, store := range stores {
		go func(store struct {
			storage StorageContext
			path    string
		}) {
			result, err := StoreFileWithStorageContextAndCodecResult(store.storage, store.path, blocks.CodecPlain)
			outcomes <- outcome{result: result, err: err}
		}(store)
	}
	select {
	case got := <-outcomes:
		t.Fatalf("shared-chunk Store returned before PROCESSING became terminal: result=%+v err=%v", got.result, got.err)
	case <-time.After(75 * time.Millisecond):
	}
	if _, err := fixture.repo.DB.Exec(`UPDATE chunk SET status = $1 WHERE id = $2`, filestate.ChunkCompleted, sharedChunkID); err != nil {
		t.Fatalf("complete shared competing chunk claim: %v", err)
	}
	wantIDs := map[int64]bool{fixture.fileID: false, unrelatedID: false}
	for range wantIDs {
		select {
		case got := <-outcomes:
			if got.err != nil || !got.result.AlreadyStored {
				t.Fatalf("shared-chunk contention did not converge to healthy reuse: result=%+v err=%v", got.result, got.err)
			}
			if _, ok := wantIDs[got.result.FileID]; !ok {
				t.Fatalf("shared-chunk contention returned unexpected logical identity: %+v", got.result)
			}
			wantIDs[got.result.FileID] = true
		case <-time.After(5 * time.Second):
			t.Fatal("shared-chunk contention deadlocked")
		}
	}
	for fileID, seen := range wantIDs {
		if !seen {
			t.Fatalf("shared-chunk logical object %d did not converge", fileID)
		}
		ckV11316015AssertNoLiveRepairState(t, fixture.repo.DB, fileID)
	}
}

func TestCKV11316015ProcessingAbortWaiterPublishesOneRepair(t *testing.T) {
	fixture := newCKV11316015ConcurrentFixture(t)
	chunkID := ckV11316015FirstRecipeChunkID(t, fixture.repo.DB, fixture.fileID)
	var logicalBefore, chunkBefore int64
	if err := fixture.repo.DB.QueryRow(`SELECT retry_count FROM logical_file WHERE id = $1`, fixture.fileID).Scan(&logicalBefore); err != nil {
		t.Fatalf("query logical retry before aborted contention: %v", err)
	}
	if err := fixture.repo.DB.QueryRow(`SELECT retry_count FROM chunk WHERE id = $1`, chunkID).Scan(&chunkBefore); err != nil {
		t.Fatalf("query chunk retry before aborted contention: %v", err)
	}
	if _, err := fixture.repo.DB.Exec(`UPDATE chunk SET status = $1, retry_count = retry_count + 1 WHERE id = $2`, filestate.ChunkProcessing, chunkID); err != nil {
		t.Fatalf("claim competing chunk retry: %v", err)
	}
	type outcome struct {
		result StoreFileResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := StoreFileWithStorageContextAndCodecResult(fixture.repo.Storage, fixture.duplicatePath, blocks.CodecPlain)
		done <- outcome{result: result, err: err}
	}()
	select {
	case got := <-done:
		t.Fatalf("aborted-contention Store returned before terminal state: result=%+v err=%v", got.result, got.err)
	case <-time.After(75 * time.Millisecond):
	}
	if _, err := fixture.repo.DB.Exec(`UPDATE chunk SET status = $1 WHERE id = $2`, filestate.ChunkAborted, chunkID); err != nil {
		t.Fatalf("abort competing chunk claim: %v", err)
	}
	select {
	case got := <-done:
		if got.err != nil || got.result.FileID != fixture.fileID || got.result.AlreadyStored {
			t.Fatalf("aborted contention did not converge to one repair: result=%+v err=%v", got.result, got.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("aborted contention repair deadlocked")
	}
	var status string
	var logicalAfter, chunkAfter int64
	if err := fixture.repo.DB.QueryRow(`SELECT retry_count FROM logical_file WHERE id = $1`, fixture.fileID).Scan(&logicalAfter); err != nil {
		t.Fatalf("query logical retry after aborted contention: %v", err)
	}
	if err := fixture.repo.DB.QueryRow(`SELECT status, retry_count FROM chunk WHERE id = $1`, chunkID).Scan(&status, &chunkAfter); err != nil {
		t.Fatalf("query chunk after aborted contention: %v", err)
	}
	if status != filestate.ChunkCompleted || logicalAfter != logicalBefore+1 || chunkAfter != chunkBefore+2 {
		t.Fatalf("aborted contention state=%s logical=%d->%d chunk=%d->%d, want competitor +1 then repair +1", status, logicalBefore, logicalAfter, chunkBefore, chunkAfter)
	}
	ckV11316015AssertNoLiveRepairState(t, fixture.repo.DB, fixture.fileID)
}

func TestCKV11316015ProcessingContentionCancellationAndTimeoutAreMutationFree(t *testing.T) {
	t.Run("cancellation", func(t *testing.T) {
		fixture := newCKV11316015Fixture(t, false)
		chunkID := ckV11316015FirstRecipeChunkID(t, fixture.repo.DB, fixture.fileID)
		if _, err := fixture.repo.DB.Exec(`UPDATE chunk SET status = $1 WHERE id = $2`, filestate.ChunkProcessing, chunkID); err != nil {
			t.Fatalf("mark cancellation chunk PROCESSING: %v", err)
		}
		before := ckV11316015AuthoritativeSnapshot(t, fixture.repo.DB, fixture.fileID)
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(50*time.Millisecond, cancel)
		_, err := StoreFileWithStorageContextAndCodecResultContext(ctx, fixture.repo.Storage, fixture.duplicatePath, blocks.CodecPlain)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("contention cancellation error=%v, want context.Canceled", err)
		}
		after := ckV11316015AuthoritativeSnapshot(t, fixture.repo.DB, fixture.fileID)
		if !reflect.DeepEqual(before, after) {
			t.Fatalf("contention cancellation mutated authoritative state\nbefore=%v\nafter=%v", before, after)
		}
	})

	t.Run("timeout", func(t *testing.T) {
		fixture := newCKV11316015Fixture(t, false)
		chunkID := ckV11316015FirstRecipeChunkID(t, fixture.repo.DB, fixture.fileID)
		if _, err := fixture.repo.DB.Exec(`UPDATE chunk SET status = $1 WHERE id = $2`, filestate.ChunkProcessing, chunkID); err != nil {
			t.Fatalf("mark timeout chunk PROCESSING: %v", err)
		}
		before := ckV11316015AuthoritativeSnapshot(t, fixture.repo.DB, fixture.fileID)
		oldChunkWait, oldMaxPoll, oldMaxWait := chunkWaitingtime, maxClaimPollingWait, maxClaimWaitDuration
		chunkWaitingtime, maxClaimPollingWait, maxClaimWaitDuration = 5*time.Millisecond, 10*time.Millisecond, 40*time.Millisecond
		t.Cleanup(func() {
			chunkWaitingtime, maxClaimPollingWait, maxClaimWaitDuration = oldChunkWait, oldMaxPoll, oldMaxWait
		})
		started := time.Now()
		_, err := StoreFileWithStorageContextAndCodecResult(fixture.repo.Storage, fixture.duplicatePath, blocks.CodecPlain)
		if err == nil || !strings.Contains(err.Error(), "timeout waiting for chunk") {
			t.Fatalf("contention timeout error=%v, want bounded chunk timeout", err)
		}
		if elapsed := time.Since(started); elapsed > time.Second {
			t.Fatalf("contention timeout was not bounded: %s", elapsed)
		}
		after := ckV11316015AuthoritativeSnapshot(t, fixture.repo.DB, fixture.fileID)
		if !reflect.DeepEqual(before, after) {
			t.Fatalf("contention timeout mutated authoritative state\nbefore=%v\nafter=%v", before, after)
		}
	})
}

func TestCKV11316015PublicationDisplacementCleansBeforeWaitingAndRevalidates(t *testing.T) {
	fixture := newCKV11316015ConcurrentFixture(t)
	chunkID := ckV11316015FirstRecipeChunkID(t, fixture.repo.DB, fixture.fileID)
	fixture.moveRequiredContainer(t)
	if _, err := fixture.repo.DB.Exec(`UPDATE chunk SET status = $1 WHERE id = $2`, filestate.ChunkAborted, chunkID); err != nil {
		t.Fatalf("prepare displaced repair chunk: %v", err)
	}
	var logicalBefore, chunkBefore int64
	if err := fixture.repo.DB.QueryRow(`SELECT retry_count FROM logical_file WHERE id = $1`, fixture.fileID).Scan(&logicalBefore); err != nil {
		t.Fatalf("query displaced logical retry: %v", err)
	}
	if err := fixture.repo.DB.QueryRow(`SELECT retry_count FROM chunk WHERE id = $1`, chunkID).Scan(&chunkBefore); err != nil {
		t.Fatalf("query displaced chunk retry: %v", err)
	}

	displaced := make(chan struct{})
	competitorDone := make(chan error, 1)
	go func() {
		<-displaced
		time.Sleep(75 * time.Millisecond)
		_, err := fixture.repo.DB.Exec(`UPDATE chunk SET status = $1 WHERE id = $2`, filestate.ChunkAborted, chunkID)
		competitorDone <- err
	}()
	var once sync.Once
	restoreHooks := InstallTestStoreInterleavingHooks(&fixture.repo.Storage, func(_ context.Context, event TestStoreInterleavingHookEvent) error {
		if event.Event != TestStoreInterleavingEventBeforeRepairPublication {
			return nil
		}
		var hookErr error
		once.Do(func() {
			_, hookErr = fixture.repo.DB.Exec(`UPDATE chunk SET status = $1, retry_count = retry_count + 1 WHERE id = $2`, filestate.ChunkProcessing, chunkID)
			close(displaced)
		})
		return hookErr
	})
	defer restoreHooks()

	result, err := StoreFileWithStorageContextAndCodecResult(fixture.repo.Storage, fixture.duplicatePath, blocks.CodecPlain)
	if competitorErr := <-competitorDone; competitorErr != nil {
		t.Fatalf("terminate displaced competitor: %v", competitorErr)
	}
	if err != nil || result.FileID != fixture.fileID || result.AlreadyStored {
		t.Fatalf("publication displacement did not revalidate and repair: result=%+v err=%v", result, err)
	}
	var logicalAfter, chunkAfter int64
	if err := fixture.repo.DB.QueryRow(`SELECT retry_count FROM logical_file WHERE id = $1`, fixture.fileID).Scan(&logicalAfter); err != nil {
		t.Fatalf("query displaced logical retry after repair: %v", err)
	}
	if err := fixture.repo.DB.QueryRow(`SELECT retry_count FROM chunk WHERE id = $1`, chunkID).Scan(&chunkAfter); err != nil {
		t.Fatalf("query displaced chunk retry after repair: %v", err)
	}
	if logicalAfter != logicalBefore+1 || chunkAfter != chunkBefore+2 {
		t.Fatalf("displaced retry accounting logical=%d->%d chunk=%d->%d", logicalBefore, logicalAfter, chunkBefore, chunkAfter)
	}
	var attempts, liveAttempts, quarantinedRepairContainers int
	if err := fixture.repo.DB.QueryRow(`SELECT COUNT(*) FROM store_repair_attempt WHERE logical_file_id = $1`, fixture.fileID).Scan(&attempts); err != nil {
		t.Fatalf("count displacement repair attempts: %v", err)
	}
	if err := fixture.repo.DB.QueryRow(`SELECT COUNT(*) FROM store_repair_attempt WHERE logical_file_id = $1 AND status IN ('PREPARING','READY')`, fixture.fileID).Scan(&liveAttempts); err != nil {
		t.Fatalf("count live displacement repair attempts: %v", err)
	}
	if err := fixture.repo.DB.QueryRow(`SELECT COUNT(*) FROM container WHERE filename LIKE 'container_repair_%' AND quarantine = TRUE`).Scan(&quarantinedRepairContainers); err != nil {
		t.Fatalf("count losing quarantined repair containers: %v", err)
	}
	if attempts != 1 || liveAttempts != 0 || quarantinedRepairContainers != 0 {
		t.Fatalf("losing publication state remains: attempts=%d live=%d quarantined_containers=%d", attempts, liveAttempts, quarantinedRepairContainers)
	}
	assertCKV11316015Restore(t, fixture, fixture.duplicatePath, "after-displacement-repair.bin")
	fixture.restoreRequiredContainer(t)
}

func ckV11316015FirstRecipeChunkID(t *testing.T, dbconn *sql.DB, fileID int64) int64 {
	t.Helper()
	var chunkID int64
	if err := dbconn.QueryRow(`SELECT chunk_id FROM file_chunk WHERE logical_file_id = $1 ORDER BY chunk_order LIMIT 1`, fileID).Scan(&chunkID); err != nil {
		t.Fatalf("load first recipe chunk: %v", err)
	}
	return chunkID
}

func ckV11316015ConcurrentStorageContext(fixture *ckV11316015Fixture) StorageContext {
	storageContext := fixture.repo.Storage
	storageContext.Writer = container.NewLocalWriterWithDirAndDB(
		fixture.repo.ContainersDir, container.GetContainerMaxSize(), fixture.repo.DB,
	)
	return storageContext
}

func ckV11316015AssertNoLiveRepairState(t *testing.T, dbconn *sql.DB, fileID int64) {
	t.Helper()
	var processing, liveAttempts int
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM chunk WHERE status = $1 AND id IN (SELECT chunk_id FROM file_chunk WHERE logical_file_id = $2)`, filestate.ChunkProcessing, fileID).Scan(&processing); err != nil {
		t.Fatalf("count PROCESSING repair chunks: %v", err)
	}
	if err := dbconn.QueryRow(`SELECT COUNT(*) FROM store_repair_attempt WHERE logical_file_id = $1 AND status IN ('PREPARING','READY')`, fileID).Scan(&liveAttempts); err != nil {
		t.Fatalf("count live repair attempts: %v", err)
	}
	if processing != 0 || liveAttempts != 0 {
		t.Fatalf("live repair state remains: processing_chunks=%d live_attempts=%d", processing, liveAttempts)
	}
}

func TestCKV11316015RepairPublicationRefusesActiveRestorePins(t *testing.T) {
	fixture := newCKV11316015Fixture(t, false)
	fixture.repo.DB.SetMaxOpenConns(1)
	fixture.repo.DB.SetMaxIdleConns(1)
	before := ckV11316015AuthoritativeSnapshot(t, fixture.repo.DB, fixture.fileID)
	pinned := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	ConfigureRestoreTestHooksForTesting(&fixture.repo.Storage, func(_ *sql.DB, _ int64) error {
		once.Do(func() { close(pinned) })
		<-release
		return nil
	}, nil)

	restoreDestination := filepath.Join(t.TempDir(), "pinned-restore.bin")
	restoreDone := make(chan error, 1)
	go func() {
		_, err := RestoreFileWithStorageContextResultOptions(fixture.repo.Storage, fixture.fileID, restoreDestination, RestoreOptions{
			Overwrite:  true,
			NoMetadata: true,
		})
		restoreDone <- err
	}()
	<-pinned
	fixture.moveRequiredContainer(t)

	result, err := StoreFileWithStorageContextAndCodecResult(fixture.repo.Storage, fixture.duplicatePath, blocks.CodecPlain)
	if err == nil || !strings.Contains(err.Error(), "requires zero restore pins") {
		t.Fatalf("repair publication with active Restore pins result=%+v err=%v", result, err)
	}
	close(release)
	if err := <-restoreDone; err != nil {
		t.Fatalf("already-pinned Restore failed after physical removal: %v", err)
	}
	got, err := os.ReadFile(restoreDestination)
	if err != nil || !reflect.DeepEqual(got, fixture.payload) {
		t.Fatalf("already-pinned Restore bytes differ: read_err=%v got=%d want=%d", err, len(got), len(fixture.payload))
	}
	after := ckV11316015AuthoritativeSnapshot(t, fixture.repo.DB, fixture.fileID)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("pin-refused repair mutated authoritative graph\nbefore=%v\nafter=%v", before, after)
	}
	fixture.restoreRequiredContainer(t)
	assertCKV11316015Restore(t, fixture, fixture.originalPath, "after-pin-refusal.bin")
}

func TestCKV11316015SuccessfulRepairBackendCodecCompressionLayoutMatrix(t *testing.T) {
	modes := []struct {
		name        string
		codec       blocks.Codec
		compression string
		legacy      bool
	}{
		{name: "packed-plain-none", codec: blocks.CodecPlain, compression: "none"},
		{name: "packed-plain-zstd", codec: blocks.CodecPlain, compression: "zstd"},
		{name: "packed-aes-none", codec: blocks.CodecAESGCM, compression: "none"},
		{name: "packed-aes-zstd", codec: blocks.CodecAESGCM, compression: "zstd"},
		{name: "legacy-plain-none", codec: blocks.CodecPlain, compression: "none", legacy: true},
		{name: "legacy-plain-zstd", codec: blocks.CodecPlain, compression: "zstd", legacy: true},
		{name: "legacy-aes-none", codec: blocks.CodecAESGCM, compression: "none", legacy: true},
		{name: "legacy-aes-zstd", codec: blocks.CodecAESGCM, compression: "zstd", legacy: true},
	}
	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			t.Setenv("COLDKEEP_COMPRESSION", mode.compression)
			t.Setenv("COLDKEEP_COMPRESSION_LEVEL", "3")
			if mode.codec == blocks.CodecAESGCM {
				t.Setenv("COLDKEEP_KEY", strings.Repeat("ab", 32))
			}
			fixture := newCKV11316015FixtureWithCodec(t, mode.legacy, mode.codec)
			beforeRecipe := ckV11316015QueryStrings(t, fixture.repo.DB,
				`SELECT chunk_id || '|' || chunk_order FROM file_chunk WHERE logical_file_id = $1 ORDER BY chunk_order`, fixture.fileID)
			fixture.moveRequiredContainer(t)
			result, err := StoreFileWithStorageContextAndCodecResult(fixture.repo.Storage, fixture.duplicatePath, mode.codec)
			if err != nil {
				t.Fatalf("repair matrix Store: %v", err)
			}
			if result.FileID != fixture.fileID || result.AlreadyStored {
				t.Fatalf("repair matrix result=%+v", result)
			}
			afterRecipe := ckV11316015QueryStrings(t, fixture.repo.DB,
				`SELECT chunk_id || '|' || chunk_order FROM file_chunk WHERE logical_file_id = $1 ORDER BY chunk_order`, fixture.fileID)
			if !reflect.DeepEqual(beforeRecipe, afterRecipe) {
				t.Fatalf("repair matrix changed recipe: before=%v after=%v", beforeRecipe, afterRecipe)
			}
			assertCKV11316015Restore(t, fixture, fixture.originalPath, "matrix-original.bin")
			assertCKV11316015Restore(t, fixture, fixture.duplicatePath, "matrix-duplicate.bin")
			fixture.restoreRequiredContainer(t)
			if err := verifypkg.VerifySystemFullWithContainersDir(fixture.repo.DB, fixture.repo.ContainersDir); err != nil {
				t.Fatalf("repair matrix full Verify: %v", err)
			}
		})
	}
}

func TestCKV11316015SchemaV17ProvidesDurableCopyOnWriteRepairState(t *testing.T) {
	repo := NewTestRepository(t)
	var version int
	if err := repo.DB.QueryRow(`SELECT MAX(catalog_version) FROM schema_version`).Scan(&version); err != nil {
		t.Fatalf("read schema version: %v", err)
	}
	if version != 17 {
		t.Fatalf("CK-V11316-015 requires schema version 17, got %d", version)
	}

	wantTables := []string{
		"store_repair_attempt",
		"store_repair_container",
		"store_repair_block",
		"store_repair_chunk",
		"retired_chunk_block_ref",
		"retired_legacy_block_extent",
	}
	for _, table := range wantTables {
		var count int
		if err := repo.DB.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = $1`, table).Scan(&count); err != nil {
			t.Fatalf("inspect schema-v17 table %s: %v", table, err)
		}
		if count != 1 {
			t.Fatalf("schema-v17 durable repair table %s is absent", table)
		}
	}

	var pkColumns string
	rows, err := repo.DB.Query(`PRAGMA table_info(retired_legacy_block_extent)`)
	if err != nil {
		t.Fatalf("inspect retired legacy extent identity: %v", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var cid, notNull, pk int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &pk); err != nil {
			t.Fatalf("scan retired legacy extent identity: %v", err)
		}
		if pk > 0 {
			pkColumns += fmt.Sprintf("%d:%s;", pk, name)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate retired legacy extent identity: %v", err)
	}
	if pkColumns != "1:container_id;2:block_offset;" {
		t.Fatalf("retired legacy extent identity=%q, want physical (container_id, block_offset)", pkColumns)
	}
}

func newCKV11316015WrappedSQLiteRepository(t *testing.T) (*TestRepository, *ckV11316015LookupControl, string) {
	t.Helper()
	databasePath := filepath.Join(t.TempDir(), "ck015-initial-lookup.sqlite")
	control := &ckV11316015LookupControl{}
	base := &ckV11316015DriverConnector{
		driver: &sqlite3.SQLiteDriver{},
		dsn:    "file:" + databasePath + "?_busy_timeout=5000&_journal_mode=WAL&_foreign_keys=on",
	}
	dbconn := sql.OpenDB(&ckV11316015Connector{underlying: base, control: control})
	dbconn.SetMaxOpenConns(8)
	if err := dbpkg.RunMigrations(dbconn); err != nil {
		_ = dbconn.Close()
		t.Fatalf("migrate wrapped SQLite lookup database: %v", err)
	}
	tx, err := dbconn.Begin()
	if err != nil {
		_ = dbconn.Close()
		t.Fatalf("begin wrapped SQLite repository configuration: %v", err)
	}
	if err := SetDefaultCompression(tx, "none"); err != nil {
		_ = tx.Rollback()
		_ = dbconn.Close()
		t.Fatalf("set wrapped SQLite compression: %v", err)
	}
	if err := tx.Commit(); err != nil {
		_ = dbconn.Close()
		t.Fatalf("commit wrapped SQLite repository configuration: %v", err)
	}
	containersDir := t.TempDir()
	storageContext := StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn),
		ContainerDir: containersDir,
	}
	repo := &TestRepository{DB: dbconn, Storage: storageContext, ContainersDir: containersDir}
	t.Cleanup(func() { _ = repo.Storage.Close() })
	return repo, control, databasePath
}

func ckV11316015RunSQLiteLookupErrorTest(t *testing.T, response ckV11316015LookupResponse, sentinel error) {
	t.Helper()
	repo, control, databasePath := newCKV11316015WrappedSQLiteRepository(t)
	fixture := newCKV11316015FixtureWithRepository(t, false, blocks.CodecPlain, repo)
	ckV11316015PrepareLookupErrorAuthority(t, repo.DB, fixture.fileID)
	before := ckV11316015LookupAuthoritySnapshot(t, repo.DB, fixture.fileID)
	originalContainer, err := os.ReadFile(fixture.containerPath)
	if err != nil {
		t.Fatalf("read required container before lookup fault: %v", err)
	}
	originalHash := sha256.Sum256(originalContainer)
	fixture.moveRequiredContainer(t)
	beforeFiles := ckV11316015FilesystemSnapshot(t, repo.ContainersDir)
	control.arm(response, sentinel, fixture.fileID)

	result, storeErr := StoreFileWithStorageContextAndCodecResult(repo.Storage, fixture.duplicatePath, blocks.CodecPlain)
	if response == ckV11316015LookupOperationalError {
		if !errors.Is(storeErr, sentinel) {
			t.Fatalf("initial lookup cause not propagated: result=%+v err=%v", result, storeErr)
		}
	} else {
		if storeErr == nil || errors.Is(storeErr, sentinel) || !strings.Contains(storeErr.Error(), "converting NULL to string") {
			t.Fatalf("partial Scan did not return the real destination conversion error: result=%+v err=%v", result, storeErr)
		}
	}
	if result.AlreadyStored {
		t.Fatalf("failed initial lookup reported AlreadyStored=true: %+v", result)
	}
	consumed, matches, fallback := control.counts()
	if consumed != 1 || matches != 1 || fallback != 0 {
		t.Fatalf("lookup fault counts consumed=%d matches=%d fallback=%d, want 1/1/0", consumed, matches, fallback)
	}
	after := ckV11316015LookupAuthoritySnapshot(t, repo.DB, fixture.fileID)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("lookup fault mutated authoritative state\nbefore=%v\nafter=%v", before, after)
	}
	afterFiles := ckV11316015FilesystemSnapshot(t, repo.ContainersDir)
	if !reflect.DeepEqual(beforeFiles, afterFiles) {
		t.Fatalf("lookup fault created or changed payload artifacts\nbefore=%v\nafter=%v", beforeFiles, afterFiles)
	}
	held, err := os.ReadFile(fixture.holdingPath)
	if err != nil {
		t.Fatalf("read exact held container after lookup fault: %v", err)
	}
	if len(held) != len(originalContainer) || sha256.Sum256(held) != originalHash {
		t.Fatal("held required container changed during failed lookup")
	}
	fixture.restoreRequiredContainer(t)
	restored, err := os.ReadFile(fixture.containerPath)
	if err != nil || len(restored) != len(originalContainer) || sha256.Sum256(restored) != originalHash {
		t.Fatalf("restored required container is not byte-exact: err=%v", err)
	}
	assertCKV11316015Restore(t, fixture, fixture.originalPath, "lookup-fault-immediate.bin")

	if err := repo.Storage.Close(); err != nil {
		t.Fatalf("close wrapped SQLite repository before reopen: %v", err)
	}
	reopened, err := sql.Open("sqlite3", "file:"+databasePath+"?_busy_timeout=5000&_journal_mode=WAL&_foreign_keys=on")
	if err != nil {
		t.Fatalf("reopen SQLite lookup database: %v", err)
	}
	reopened.SetMaxOpenConns(8)
	repo.DB = reopened
	repo.Storage = StorageContext{
		DB:           reopened,
		Writer:       container.NewLocalWriterWithDirAndDB(repo.ContainersDir, container.GetContainerMaxSize(), reopened),
		ContainerDir: repo.ContainersDir,
	}
	assertCKV11316015Restore(t, fixture, fixture.originalPath, "lookup-fault-after-reopen.bin")
}

func ckV11316015PrepareLookupErrorAuthority(t *testing.T, dbconn *sql.DB, fileID int64) {
	t.Helper()
	if _, err := dbconn.Exec(`UPDATE logical_file SET retry_count = 17 WHERE id = $1`, fileID); err != nil {
		t.Fatalf("set recognizable logical retry: %v", err)
	}
	if _, err := dbconn.Exec(`UPDATE chunk SET retry_count = 23 WHERE id IN (SELECT chunk_id FROM file_chunk WHERE logical_file_id = $1)`, fileID); err != nil {
		t.Fatalf("set recognizable chunk retries: %v", err)
	}
	snapshotID := fmt.Sprintf("ck015-lookup-%d", fileID)
	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type, label) VALUES ($1, CURRENT_TIMESTAMP, 'full', 'CK-015 lookup fault')`, snapshotID); err != nil {
		t.Fatalf("insert lookup-fault snapshot: %v", err)
	}
	var pathID int64
	if err := dbconn.QueryRow(`INSERT INTO snapshot_path (path) VALUES ($1) RETURNING id`, "snapshot/"+snapshotID).Scan(&pathID); err != nil {
		t.Fatalf("insert lookup-fault snapshot path: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id, size) SELECT $1, $2, id, total_size FROM logical_file WHERE id = $3`, snapshotID, pathID, fileID); err != nil {
		t.Fatalf("insert lookup-fault snapshot membership: %v", err)
	}
}

func ckV11316015LookupAuthoritySnapshot(t *testing.T, dbconn *sql.DB, fileID int64) []string {
	t.Helper()
	snapshot := append([]string(nil), ckV11316015AuthoritativeSnapshot(t, dbconn, fileID)...)
	queries := []string{
		`SELECT CAST(sf.id AS TEXT) || '|' || sf.snapshot_id || '|' || CAST(sf.path_id AS TEXT) || '|' || CAST(sf.logical_file_id AS TEXT) || '|' || COALESCE(CAST(sf.size AS TEXT), '') || '|' || sp.path FROM snapshot_file sf JOIN snapshot_path sp ON sp.id = sf.path_id WHERE sf.logical_file_id = $1 ORDER BY sf.id`,
		`SELECT s.id || '|' || s.type || '|' || COALESCE(s.label, '') || '|' || COALESCE(s.parent_id, '') FROM snapshot s JOIN snapshot_file sf ON sf.snapshot_id = s.id WHERE sf.logical_file_id = $1 ORDER BY s.id`,
		`SELECT CAST(c.id AS TEXT) || '|' || c.filename || '|' || CAST(c.sealed AS TEXT) || '|' || CAST(c.sealing AS TEXT) || '|' || CAST(c.quarantine AS TEXT) || '|' || CAST(c.current_size AS TEXT) FROM container c WHERE c.id IN (SELECT sb.container_id FROM storage_blocks sb JOIN chunk_block_refs r ON r.block_id = sb.id JOIN file_chunk fc ON fc.chunk_id = r.chunk_id WHERE fc.logical_file_id = $1 UNION SELECT b.container_id FROM blocks b JOIN file_chunk fc ON fc.chunk_id = b.chunk_id WHERE fc.logical_file_id = $1) ORDER BY c.id`,
		`SELECT CAST(id AS TEXT) || '|' || status || '|' || source_file_hash || '|' || CAST(source_total_size AS TEXT) FROM store_repair_attempt WHERE logical_file_id = $1 ORDER BY id`,
		`SELECT CAST(rc.attempt_id AS TEXT) || '|' || CAST(rc.container_id AS TEXT) || '|' || rc.status FROM store_repair_container rc JOIN store_repair_attempt a ON a.id = rc.attempt_id WHERE a.logical_file_id = $1 ORDER BY rc.attempt_id, rc.container_id`,
		`SELECT CAST(rb.attempt_id AS TEXT) || '|' || CAST(rb.block_ordinal AS TEXT) || '|' || CAST(rb.container_id AS TEXT) FROM store_repair_block rb JOIN store_repair_attempt a ON a.id = rb.attempt_id WHERE a.logical_file_id = $1 ORDER BY rb.attempt_id, rb.block_ordinal`,
		`SELECT CAST(rc.attempt_id AS TEXT) || '|' || CAST(rc.chunk_id AS TEXT) || '|' || CAST(rc.chunk_order AS TEXT) FROM store_repair_chunk rc JOIN store_repair_attempt a ON a.id = rc.attempt_id WHERE a.logical_file_id = $1 ORDER BY rc.attempt_id, rc.chunk_order`,
	}
	for _, query := range queries {
		snapshot = append(snapshot, ckV11316015QueryStrings(t, dbconn, query, fileID)...)
		snapshot = append(snapshot, "--lookup-boundary--")
	}
	return snapshot
}

func ckV11316015FilesystemSnapshot(t *testing.T, root string) []string {
	t.Helper()
	var snapshot []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		payload, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(payload)
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		snapshot = append(snapshot, fmt.Sprintf("%s|%o|%d|%x", filepath.ToSlash(relative), info.Mode().Perm(), len(payload), digest))
		return nil
	})
	if err != nil {
		t.Fatalf("snapshot container filesystem: %v", err)
	}
	sort.Strings(snapshot)
	return snapshot
}

func newCKV11316015Fixture(t *testing.T, legacyOnly bool) *ckV11316015Fixture {
	return newCKV11316015FixtureWithCodec(t, legacyOnly, blocks.CodecPlain)
}

func newCKV11316015FixtureWithCodec(t *testing.T, legacyOnly bool, codec blocks.Codec) *ckV11316015Fixture {
	t.Helper()
	return newCKV11316015FixtureWithRepository(t, legacyOnly, codec, NewTestRepository(t))
}

func newCKV11316015ConcurrentFixture(t *testing.T) *ckV11316015Fixture {
	t.Helper()
	databasePath := filepath.Join(t.TempDir(), "concurrent-repair.sqlite")
	dbconn, err := sql.Open("sqlite3", "file:"+databasePath+"?_busy_timeout=5000&_journal_mode=WAL&_foreign_keys=on")
	if err != nil {
		t.Fatalf("open concurrent repair SQLite database: %v", err)
	}
	dbconn.SetMaxOpenConns(8)
	if err := dbpkg.RunMigrations(dbconn); err != nil {
		_ = dbconn.Close()
		t.Fatalf("migrate concurrent repair SQLite database: %v", err)
	}
	tx, err := dbconn.Begin()
	if err != nil {
		_ = dbconn.Close()
		t.Fatalf("begin concurrent repair repository configuration: %v", err)
	}
	if err := SetDefaultCompression(tx, "none"); err != nil {
		_ = tx.Rollback()
		_ = dbconn.Close()
		t.Fatalf("set concurrent repair compression: %v", err)
	}
	if err := tx.Commit(); err != nil {
		_ = dbconn.Close()
		t.Fatalf("commit concurrent repair repository configuration: %v", err)
	}
	containersDir := t.TempDir()
	storageContext := StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn),
		ContainerDir: containersDir,
	}
	repo := &TestRepository{DB: dbconn, Storage: storageContext, ContainersDir: containersDir}
	t.Cleanup(func() { _ = repo.Storage.Close() })
	return newCKV11316015FixtureWithRepository(t, false, blocks.CodecPlain, repo)
}

func newCKV11316015FixtureWithRepository(t *testing.T, legacyOnly bool, codec blocks.Codec, repo *TestRepository) *ckV11316015Fixture {
	t.Helper()
	t.Setenv("COLDKEEP_REUSE_SEMANTIC_VALIDATION", "always")
	left := bytesForCKV11316015("left", 64*1024)
	right := bytesForCKV11316015("right", 64*1024)
	payload := append(append([]byte(nil), left...), right...)

	repo.Storage.Chunker = scriptedChunker{
		version:  chunk.VersionV1SimpleRolling,
		payloads: [][]byte{left, right},
	}
	inputDir := t.TempDir()
	originalPath := filepath.Join(inputDir, "original.bin")
	duplicatePath := filepath.Join(inputDir, "duplicate.bin")
	if err := os.WriteFile(originalPath, payload, 0o600); err != nil {
		t.Fatalf("write original repair fixture: %v", err)
	}
	if err := os.WriteFile(duplicatePath, payload, 0o600); err != nil {
		t.Fatalf("write duplicate repair fixture: %v", err)
	}
	var fileID int64
	if legacyOnly {
		fileID = seedCKV11316015LegacyFixture(t, repo, originalPath, payload, [][]byte{left, right}, codec)
	} else {
		stored, err := StoreFileWithStorageContextAndCodecResult(repo.Storage, originalPath, codec)
		if err != nil {
			t.Fatalf("store healthy repair fixture: %v", err)
		}
		if stored.AlreadyStored || stored.FileID <= 0 {
			t.Fatalf("unexpected initial Store result: %+v", stored)
		}
		fileID = stored.FileID
	}

	var filename string
	placementJoin := `JOIN chunk_block_refs r ON r.chunk_id = fc.chunk_id JOIN storage_blocks sb ON sb.id = r.block_id JOIN container ctr ON ctr.id = sb.container_id`
	if legacyOnly {
		placementJoin = `JOIN blocks b ON b.chunk_id = fc.chunk_id JOIN container ctr ON ctr.id = b.container_id`
	}
	query := `SELECT ctr.filename FROM file_chunk fc ` + placementJoin + ` WHERE fc.logical_file_id = $1 ORDER BY fc.chunk_order LIMIT 1`
	if err := repo.DB.QueryRow(query, fileID).Scan(&filename); err != nil {
		t.Fatalf("resolve required repair container: %v", err)
	}
	containerPath := filepath.Join(repo.ContainersDir, filename)
	holdingPath := filepath.Join(t.TempDir(), filename)
	fixture := &ckV11316015Fixture{
		repo: repo, originalPath: originalPath, duplicatePath: duplicatePath,
		fileID: fileID, containerPath: containerPath, holdingPath: holdingPath, payload: payload,
		codec: codec,
	}
	assertCKV11316015Restore(t, fixture, originalPath, "healthy.bin")
	return fixture
}

func seedCKV11316015LegacyFixture(
	t *testing.T,
	repo *TestRepository,
	originalPath string,
	payload []byte,
	chunkPayloads [][]byte,
	codec blocks.Codec,
) int64 {
	t.Helper()
	fileDigest := sha256.Sum256(payload)
	var fileID int64
	if err := repo.DB.QueryRow(`
		INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		VALUES ($1, $2, $3, $4, 1, $5)
		RETURNING id`,
		filepath.Base(originalPath), int64(len(payload)), hex.EncodeToString(fileDigest[:]),
		filestate.LogicalFileCompleted, chunk.VersionV1SimpleRolling,
	).Scan(&fileID); err != nil {
		t.Fatalf("insert legacy logical file: %v", err)
	}

	transformer, err := blocks.GetBlockTransformer(codec)
	if err != nil {
		t.Fatalf("create legacy block transformer: %v", err)
	}
	writer, ok := repo.Storage.Writer.(payloadStatefulWriter)
	if !ok {
		t.Fatalf("legacy fixture writer %T does not expose payload append state", repo.Storage.Writer)
	}
	tx, err := repo.DB.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin legacy fixture transaction: %v", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	blockRepo := &blocks.Repository{DB: repo.DB}
	for order, chunkPayload := range chunkPayloads {
		chunkDigest := sha256.Sum256(chunkPayload)
		chunkHash := hex.EncodeToString(chunkDigest[:])
		var chunkID int64
		if err := tx.QueryRow(`
			INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version)
			VALUES ($1, $2, $3, 1, $4)
			RETURNING id`,
			chunkHash, int64(len(chunkPayload)), filestate.ChunkCompleted, chunk.VersionV1SimpleRolling,
		).Scan(&chunkID); err != nil {
			t.Fatalf("insert legacy chunk %d: %v", order, err)
		}
		if _, err := tx.Exec(`
			INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order)
			VALUES ($1, $2, $3)`, fileID, chunkID, order); err != nil {
			t.Fatalf("insert legacy recipe entry %d: %v", order, err)
		}
		placement, _, err := storeChunkAsPlainBlockWithWriter(
			context.Background(), tx, blockRepo, writer,
			chunkID, chunkHash, chunkPayload, transformer,
		)
		if err != nil {
			t.Fatalf("persist genuine legacy chunk %d: %v", order, err)
		}
		if err := container.UpdateContainerSize(tx, placement.ContainerID, placement.NewContainerSize); err != nil {
			t.Fatalf("update legacy container size for chunk %d: %v", order, err)
		}
	}
	if _, err := tx.Exec(`
		INSERT INTO physical_file (path, logical_file_id, is_metadata_complete)
		VALUES ($1, $2, 0)`, originalPath, fileID); err != nil {
		t.Fatalf("insert legacy physical mapping: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit genuine legacy fixture: %v", err)
	}
	committed = true
	acknowledgeWriterAppendCommitted(writer)
	return fileID
}

func (f *ckV11316015Fixture) moveRequiredContainer(t *testing.T) {
	t.Helper()
	if err := os.Rename(f.containerPath, f.holdingPath); err != nil {
		t.Fatalf("move required container out of service: %v", err)
	}
	t.Cleanup(func() {
		if _, err := os.Stat(f.holdingPath); err == nil {
			_ = os.Rename(f.holdingPath, f.containerPath)
		}
	})
	if _, err := os.Stat(f.containerPath); !os.IsNotExist(err) {
		t.Fatalf("required container must be absent, stat error=%v", err)
	}
}

func (f *ckV11316015Fixture) restoreRequiredContainer(t *testing.T) {
	t.Helper()
	if err := os.Rename(f.holdingPath, f.containerPath); err != nil {
		t.Fatalf("restore original required container bytes: %v", err)
	}
}

func assertCKV11316015Restore(t *testing.T, fixture *ckV11316015Fixture, storedPath, name string) {
	t.Helper()
	destination := filepath.Join(t.TempDir(), name)
	_, err := RestoreFileByStoredPathWithStorageContextResultOptions(fixture.repo.Storage, storedPath, RestoreOptions{
		Overwrite:       true,
		DestinationMode: RestoreDestinationOverride,
		Destination:     destination,
		NoMetadata:      true,
	})
	if err != nil {
		t.Fatalf("restore %s: %v", storedPath, err)
	}
	got, err := os.ReadFile(destination)
	if err != nil {
		t.Fatalf("read restored file: %v", err)
	}
	if !reflect.DeepEqual(got, fixture.payload) {
		t.Fatalf("restored bytes differ: got=%d want=%d", len(got), len(fixture.payload))
	}
}

func ckV11316015AuthoritativeSnapshot(t *testing.T, dbconn *sql.DB, fileID int64) []string {
	t.Helper()
	queries := []string{
		`SELECT CAST(id AS TEXT) || '|' || status || '|' || CAST(retry_count AS TEXT) || '|' || CAST(ref_count AS TEXT) FROM logical_file WHERE id = $1`,
		`SELECT CAST(logical_file_id AS TEXT) || '|' || CAST(chunk_id AS TEXT) || '|' || CAST(chunk_order AS TEXT) FROM file_chunk WHERE logical_file_id = $1 ORDER BY chunk_order`,
		`SELECT CAST(id AS TEXT) || '|' || status || '|' || CAST(retry_count AS TEXT) || '|' || CAST(live_ref_count AS TEXT) || '|' || CAST(pin_count AS TEXT) FROM chunk WHERE id IN (SELECT chunk_id FROM file_chunk WHERE logical_file_id = $1) ORDER BY id`,
		`SELECT CAST(id AS TEXT) || '|' || CAST(chunk_id AS TEXT) || '|' || CAST(container_id AS TEXT) || '|' || CAST(block_offset AS TEXT) FROM blocks WHERE chunk_id IN (SELECT chunk_id FROM file_chunk WHERE logical_file_id = $1) ORDER BY id`,
		`SELECT CAST(r.chunk_id AS TEXT) || '|' || CAST(r.block_id AS TEXT) || '|' || CAST(r.offset_in_block AS TEXT) || '|' || CAST(r.size_in_block AS TEXT) FROM chunk_block_refs r WHERE r.chunk_id IN (SELECT chunk_id FROM file_chunk WHERE logical_file_id = $1) ORDER BY r.chunk_id, r.block_id`,
		`SELECT CAST(sb.id AS TEXT) || '|' || CAST(sb.container_id AS TEXT) || '|' || CAST(sb.container_offset AS TEXT) || '|' || CAST(sb.stored_size AS TEXT) FROM storage_blocks sb WHERE sb.id IN (SELECT block_id FROM chunk_block_refs WHERE chunk_id IN (SELECT chunk_id FROM file_chunk WHERE logical_file_id = $1)) ORDER BY sb.id`,
		`SELECT path || '|' || CAST(logical_file_id AS TEXT) FROM physical_file WHERE logical_file_id = $1 ORDER BY path`,
	}
	var snapshot []string
	for _, query := range queries {
		snapshot = append(snapshot, ckV11316015QueryStrings(t, dbconn, query, fileID)...)
		snapshot = append(snapshot, "--")
	}
	return snapshot
}

func ckV11316015QueryStrings(t *testing.T, dbconn *sql.DB, query string, args ...any) []string {
	t.Helper()
	rows, err := dbconn.Query(query, args...)
	if err != nil {
		t.Fatalf("query authoritative state: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var result []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			t.Fatalf("scan authoritative state: %v", err)
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate authoritative state: %v", err)
	}
	return result
}

func bytesForCKV11316015(label string, size int) []byte {
	result := make([]byte, 0, size)
	for counter := 0; len(result) < size; counter++ {
		digest := sha256.Sum256([]byte(fmt.Sprintf("%s-%d", label, counter)))
		result = append(result, digest[:]...)
	}
	return result[:size]
}
