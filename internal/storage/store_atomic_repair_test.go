package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/container"
	filestate "github.com/franchoy/coldkeep/internal/status"
	verifypkg "github.com/franchoy/coldkeep/internal/verify"
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
	start := make(chan struct{})
	var workers sync.WaitGroup
	for index := range paths {
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			<-start
			results[index], errs[index] = StoreFileWithStorageContextAndCodecResult(fixture.repo.Storage, paths[index], blocks.CodecPlain)
		}(index)
	}
	close(start)
	workers.Wait()
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
	if err := repo.DB.QueryRow(`SELECT MAX(version) FROM schema_version`).Scan(&version); err != nil {
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

func newCKV11316015Fixture(t *testing.T, legacyOnly bool) *ckV11316015Fixture {
	return newCKV11316015FixtureWithCodec(t, legacyOnly, blocks.CodecPlain)
}

func newCKV11316015FixtureWithCodec(t *testing.T, legacyOnly bool, codec blocks.Codec) *ckV11316015Fixture {
	t.Helper()
	t.Setenv("COLDKEEP_REUSE_SEMANTIC_VALIDATION", "always")
	left := bytesForCKV11316015("left", 64*1024)
	right := bytesForCKV11316015("right", 64*1024)
	payload := append(append([]byte(nil), left...), right...)

	repo := NewTestRepository(t)
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
		`SELECT id || '|' || status || '|' || retry_count || '|' || ref_count FROM logical_file WHERE id = $1`,
		`SELECT logical_file_id || '|' || chunk_id || '|' || chunk_order FROM file_chunk WHERE logical_file_id = $1 ORDER BY chunk_order`,
		`SELECT id || '|' || status || '|' || retry_count || '|' || live_ref_count || '|' || pin_count FROM chunk WHERE id IN (SELECT chunk_id FROM file_chunk WHERE logical_file_id = $1) ORDER BY id`,
		`SELECT id || '|' || chunk_id || '|' || container_id || '|' || block_offset FROM blocks WHERE chunk_id IN (SELECT chunk_id FROM file_chunk WHERE logical_file_id = $1) ORDER BY id`,
		`SELECT r.chunk_id || '|' || r.block_id || '|' || r.offset_in_block || '|' || r.size_in_block FROM chunk_block_refs r WHERE r.chunk_id IN (SELECT chunk_id FROM file_chunk WHERE logical_file_id = $1) ORDER BY r.chunk_id, r.block_id`,
		`SELECT sb.id || '|' || sb.container_id || '|' || sb.container_offset || '|' || sb.stored_size FROM storage_blocks sb WHERE sb.id IN (SELECT block_id FROM chunk_block_refs WHERE chunk_id IN (SELECT chunk_id FROM file_chunk WHERE logical_file_id = $1)) ORDER BY sb.id`,
		`SELECT path || '|' || logical_file_id FROM physical_file WHERE logical_file_id = $1 ORDER BY path`,
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
