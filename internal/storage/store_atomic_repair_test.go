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
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/chunk"
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

func TestCKV11316015SharedPackedRepairPublishesWithoutHarmingOtherMembers(t *testing.T) {
	fixture := newCKV11316015Fixture(t, false)
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
	stored, err := StoreFileWithStorageContextAndCodecResult(repo.Storage, originalPath, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("store healthy repair fixture: %v", err)
	}
	if stored.AlreadyStored || stored.FileID <= 0 {
		t.Fatalf("unexpected initial Store result: %+v", stored)
	}

	if legacyOnly {
		if _, err := repo.DB.Exec(`DELETE FROM chunk_block_refs WHERE chunk_id IN (SELECT chunk_id FROM file_chunk WHERE logical_file_id = $1)`, stored.FileID); err != nil {
			t.Fatalf("convert fixture to legacy-only placement: %v", err)
		}
		if _, err := repo.DB.Exec(`DELETE FROM storage_blocks`); err != nil {
			t.Fatalf("remove packed metadata from legacy fixture: %v", err)
		}
	}

	var filename string
	placementJoin := `JOIN chunk_block_refs r ON r.chunk_id = fc.chunk_id JOIN storage_blocks sb ON sb.id = r.block_id JOIN container ctr ON ctr.id = sb.container_id`
	if legacyOnly {
		placementJoin = `JOIN blocks b ON b.chunk_id = fc.chunk_id JOIN container ctr ON ctr.id = b.container_id`
	}
	query := `SELECT ctr.filename FROM file_chunk fc ` + placementJoin + ` WHERE fc.logical_file_id = $1 ORDER BY fc.chunk_order LIMIT 1`
	if err := repo.DB.QueryRow(query, stored.FileID).Scan(&filename); err != nil {
		t.Fatalf("resolve required repair container: %v", err)
	}
	containerPath := filepath.Join(repo.ContainersDir, filename)
	holdingPath := filepath.Join(t.TempDir(), filename)
	fixture := &ckV11316015Fixture{
		repo: repo, originalPath: originalPath, duplicatePath: duplicatePath,
		fileID: stored.FileID, containerPath: containerPath, holdingPath: holdingPath, payload: payload,
	}
	assertCKV11316015Restore(t, fixture, originalPath, "healthy.bin")
	return fixture
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
