package observability

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/db"
	_ "github.com/mattn/go-sqlite3"
)

func openInspectTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	return dbconn
}

func TestInspectLogicalFileBasic(t *testing.T) {
	dbconn := openInspectTestDB(t)

	res, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`,
		"notes.txt", 123, "hash-1", "COMPLETED", "v2-fastcdc",
	)
	if err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}
	fileID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("last insert id: %v", err)
	}

	chunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`,
		"chunk-1", 64, "COMPLETED", 1, "v2-fastcdc",
	)
	if err != nil {
		t.Fatalf("insert chunk: %v", err)
	}
	chunkID, err := chunkRes.LastInsertId()
	if err != nil {
		t.Fatalf("chunk last insert id: %v", err)
	}

	if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES (?, ?, ?)`, fileID, chunkID, 0); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	fixedNow := time.Date(2026, time.April, 26, 13, 0, 0, 0, time.UTC)
	svc := newServiceForTest(dbconn, func() time.Time { return fixedNow })

	entityID := strconv.FormatInt(fileID, 10)
	result, err := svc.Inspect(context.Background(), EntityFile, entityID, InspectOptions{})
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}

	if result.GeneratedAtUTC != fixedNow {
		t.Fatalf("generated_at_utc mismatch: got %s want %s", result.GeneratedAtUTC, fixedNow)
	}
	if result.EntityType != EntityLogicalFile || result.EntityID != entityID {
		t.Fatalf("unexpected entity: type=%s id=%s", result.EntityType, result.EntityID)
	}
	if got := result.Summary["chunk_count"]; got != int64(1) {
		t.Fatalf("expected chunk_count=1, got %v", got)
	}
	if got := result.Summary["file_id"]; got != fileID {
		t.Fatalf("expected file_id=%d, got %v", fileID, got)
	}
	if got := result.Summary["original_name"]; got != "notes.txt" {
		t.Fatalf("expected original_name=notes.txt, got %v", got)
	}
	if got := result.Summary["status"]; got != "COMPLETED" {
		t.Fatalf("expected status=COMPLETED, got %v", got)
	}
	if got := result.Summary["chunker_version"]; got != "v2-fastcdc" {
		t.Fatalf("expected chunker_version=v2-fastcdc, got %v", got)
	}
}

func TestInspectRejectsUnsupportedEntity(t *testing.T) {
	svc := newServiceForTest(nil, nil)

	_, err := svc.Inspect(context.Background(), EntityPhysicalFile, "7", InspectOptions{})
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrUnsupportedEntity) {
		t.Fatalf("expected ErrUnsupportedEntity, got %v", err)
	}
}

func TestInspectRepositoryBasic(t *testing.T) {
	dbconn := openInspectTestDB(t)
	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES (?, ?, ?)`, "s1", time.Now().UTC(), "full"); err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
	svc := newServiceForTest(dbconn, nil)

	result, err := svc.Inspect(context.Background(), EntityRepository, "", InspectOptions{})
	if err != nil {
		t.Fatalf("Inspect repository: %v", err)
	}

	if result.EntityType != EntityRepository {
		t.Fatalf("expected repository entity type, got %s", result.EntityType)
	}
	if result.EntityID != "repository" {
		t.Fatalf("expected repository id, got %q", result.EntityID)
	}
	if got := result.Summary["total_files"]; got != int64(0) {
		t.Fatalf("expected total_files=0, got %v", got)
	}
	if got := result.Summary["total_chunks"]; got != int64(0) {
		t.Fatalf("expected total_chunks=0, got %v", got)
	}
	if got := result.Summary["total_snapshots"]; got != int64(1) {
		t.Fatalf("expected total_snapshots=1, got %v", got)
	}
}

func TestInspectSnapshotBasic(t *testing.T) {
	dbconn := openInspectTestDB(t)
	fileRes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`,
		"readme.md", 345, "hash-snap", "COMPLETED", "v2-fastcdc",
	)
	if err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}
	fileID, err := fileRes.LastInsertId()
	if err != nil {
		t.Fatalf("logical_file last insert id: %v", err)
	}

	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type, label) VALUES (?, ?, ?, ?)`, "snap-10", time.Now().UTC(), "full", "release"); err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES (?)`, "readme.md"); err != nil {
		t.Fatalf("insert snapshot_path: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id, size) VALUES (?, ?, ?, ?)`, "snap-10", 1, fileID, 345); err != nil {
		t.Fatalf("insert snapshot_file: %v", err)
	}

	svc := newServiceForTest(dbconn, nil)
	result, err := svc.Inspect(context.Background(), EntitySnapshot, "snap-10", InspectOptions{})
	if err != nil {
		t.Fatalf("Inspect snapshot: %v", err)
	}

	if result.EntityType != EntitySnapshot || result.EntityID != "snap-10" {
		t.Fatalf("unexpected snapshot entity: type=%s id=%s", result.EntityType, result.EntityID)
	}
	if got := result.Summary["type"]; got != "full" {
		t.Fatalf("expected snapshot type full, got %v", got)
	}
	if got := result.Summary["label"]; got != "release" {
		t.Fatalf("expected snapshot label release, got %v", got)
	}
	if got := result.Summary["logical_file_count"]; got != int64(1) {
		t.Fatalf("expected logical_file_count=1, got %v", got)
	}
	if got := result.Summary["total_size_bytes"]; got != int64(345) {
		t.Fatalf("expected total_size_bytes=345, got %v", got)
	}
}

func TestInspectChunkBasic(t *testing.T) {
	dbconn := openInspectTestDB(t)

	chunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "chunk-x", 99, "COMPLETED", 2, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert chunk: %v", err)
	}
	chunkID, err := chunkRes.LastInsertId()
	if err != nil {
		t.Fatalf("chunk last insert id: %v", err)
	}

	ctrRes, err := dbconn.Exec(`INSERT INTO container (filename, current_size, max_size, quarantine) VALUES (?, ?, ?, ?)`, "ctr_x.bin", 512, 1024, 0)
	if err != nil {
		t.Fatalf("insert container: %v", err)
	}
	containerID, err := ctrRes.LastInsertId()
	if err != nil {
		t.Fatalf("container last insert id: %v", err)
	}

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		chunkID, "plain", 1, 99, 99, containerID, 0,
	); err != nil {
		t.Fatalf("insert blocks: %v", err)
	}

	svc := newServiceForTest(dbconn, nil)
	result, err := svc.Inspect(context.Background(), EntityChunk, strconv.FormatInt(chunkID, 10), InspectOptions{})
	if err != nil {
		t.Fatalf("Inspect chunk: %v", err)
	}

	if result.EntityType != EntityChunk {
		t.Fatalf("expected chunk entity type, got %s", result.EntityType)
	}
	if got := result.Summary["chunk_hash"]; got != "chunk-x" {
		t.Fatalf("expected chunk hash chunk-x, got %v", got)
	}
	if got := result.Summary["container_id"]; got != containerID {
		t.Fatalf("expected container_id=%d, got %v", containerID, got)
	}
	if got := result.Summary["size_bytes"]; got != int64(99) {
		t.Fatalf("expected size_bytes=99, got %v", got)
	}
}

func TestInspectContainerBasic(t *testing.T) {
	dbconn := openInspectTestDB(t)

	res, err := dbconn.Exec(`INSERT INTO container (filename, sealed, sealing, current_size, max_size, quarantine) VALUES (?, ?, ?, ?, ?, ?)`, "ctr_y.bin", 1, 0, 256, 1024, 1)
	if err != nil {
		t.Fatalf("insert container: %v", err)
	}
	containerID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("container last insert id: %v", err)
	}

	svc := newServiceForTest(dbconn, nil)
	result, err := svc.Inspect(context.Background(), EntityContainer, strconv.FormatInt(containerID, 10), InspectOptions{})
	if err != nil {
		t.Fatalf("Inspect container: %v", err)
	}

	if result.EntityType != EntityContainer {
		t.Fatalf("expected container entity type, got %s", result.EntityType)
	}
	if got := result.Summary["filename"]; got != "ctr_y.bin" {
		t.Fatalf("expected filename ctr_y.bin, got %v", got)
	}
	if got := result.Summary["sealed"]; got != true {
		t.Fatalf("expected sealed=true, got %v", got)
	}
	if got := result.Summary["quarantine"]; got != true {
		t.Fatalf("expected quarantine=true, got %v", got)
	}
	if got := result.Summary["size_bytes"]; got != int64(256) {
		t.Fatalf("expected size_bytes=256, got %v", got)
	}
	if got := result.Summary["chunk_count"]; got != int64(0) {
		t.Fatalf("expected chunk_count=0, got %v", got)
	}
}

func TestInspectSnapshotIncludesForwardRelations(t *testing.T) {
	dbconn := openInspectTestDB(t)

	fileRes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`,
		"alpha.txt", 10, "h-a", "COMPLETED", "v2-fastcdc",
	)
	if err != nil {
		t.Fatalf("insert file: %v", err)
	}
	fileID, _ := fileRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES (?, ?, ?)`, "snap-rel", time.Now().UTC(), "full"); err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES (?)`, "alpha.txt"); err != nil {
		t.Fatalf("insert snapshot path: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES (?, ?, ?)`, "snap-rel", 1, fileID); err != nil {
		t.Fatalf("insert snapshot file: %v", err)
	}

	svc := newServiceForTest(dbconn, nil)
	result, err := svc.Inspect(context.Background(), EntitySnapshot, "snap-rel", InspectOptions{Relations: true})
	if err != nil {
		t.Fatalf("Inspect snapshot with relations: %v", err)
	}
	if len(result.Relations) != 1 {
		t.Fatalf("expected 1 relation, got %d", len(result.Relations))
	}
	if rel := result.Relations[0]; rel.TargetType != EntityLogicalFile || rel.TargetID != strconv.FormatInt(fileID, 10) || rel.Direction != RelationOutgoing {
		t.Fatalf("unexpected relation: %+v", rel)
	}
}

func TestInspectLogicalFileIncludesForwardAndReverseRelations(t *testing.T) {
	dbconn := openInspectTestDB(t)

	fileRes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`,
		"beta.txt", 20, "h-b", "COMPLETED", "v2-fastcdc",
	)
	if err != nil {
		t.Fatalf("insert file: %v", err)
	}
	fileID, _ := fileRes.LastInsertId()

	chunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "chunk-beta", 20, "COMPLETED", 1, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert chunk: %v", err)
	}
	chunkID, _ := chunkRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES (?, ?, ?)`, fileID, chunkID, 0); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES (?, ?, ?)`, "42", time.Now().UTC(), "full"); err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES (?)`, "beta.txt"); err != nil {
		t.Fatalf("insert snapshot_path: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES (?, ?, ?)`, "42", 1, fileID); err != nil {
		t.Fatalf("insert snapshot_file: %v", err)
	}

	svc := newServiceForTest(dbconn, nil)
	result, err := svc.Inspect(
		context.Background(),
		EntityLogicalFile,
		strconv.FormatInt(fileID, 10),
		InspectOptions{Relations: true, Reverse: true},
	)
	if err != nil {
		t.Fatalf("Inspect logical file with relations: %v", err)
	}
	if len(result.Relations) != 2 {
		t.Fatalf("expected 2 relations, got %d", len(result.Relations))
	}
}

func TestInspectChunkIncludesForwardAndReverseRelations(t *testing.T) {
	dbconn := openInspectTestDB(t)

	fileRes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`,
		"gamma.txt", 30, "h-g", "COMPLETED", "v2-fastcdc",
	)
	if err != nil {
		t.Fatalf("insert file: %v", err)
	}
	fileID, _ := fileRes.LastInsertId()

	chunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "chunk-gamma", 30, "COMPLETED", 1, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert chunk: %v", err)
	}
	chunkID, _ := chunkRes.LastInsertId()

	ctrRes, err := dbconn.Exec(`INSERT INTO container (filename, current_size, max_size, quarantine) VALUES (?, ?, ?, ?)`, "ctr_g.bin", 256, 1024, 0)
	if err != nil {
		t.Fatalf("insert container: %v", err)
	}
	containerID, _ := ctrRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES (?, ?, ?)`, fileID, chunkID, 0); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}
	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		chunkID, "plain", 1, 30, 30, containerID, 0,
	); err != nil {
		t.Fatalf("insert block: %v", err)
	}

	svc := newServiceForTest(dbconn, nil)
	result, err := svc.Inspect(
		context.Background(),
		EntityChunk,
		strconv.FormatInt(chunkID, 10),
		InspectOptions{Relations: true, Reverse: true},
	)
	if err != nil {
		t.Fatalf("Inspect chunk with relations: %v", err)
	}
	if len(result.Relations) != 2 {
		t.Fatalf("expected 2 relations, got %d", len(result.Relations))
	}
}

func TestInspectContainerIncludesReverseRelations(t *testing.T) {
	dbconn := openInspectTestDB(t)

	chunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "chunk-ctr", 11, "COMPLETED", 1, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert chunk: %v", err)
	}
	chunkID, _ := chunkRes.LastInsertId()

	ctrRes, err := dbconn.Exec(`INSERT INTO container (filename, current_size, max_size, quarantine) VALUES (?, ?, ?, ?)`, "ctr_r.bin", 128, 1024, 0)
	if err != nil {
		t.Fatalf("insert container: %v", err)
	}
	containerID, _ := ctrRes.LastInsertId()

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		chunkID, "plain", 1, 11, 11, containerID, 0,
	); err != nil {
		t.Fatalf("insert block: %v", err)
	}

	svc := newServiceForTest(dbconn, nil)
	result, err := svc.Inspect(
		context.Background(),
		EntityContainer,
		strconv.FormatInt(containerID, 10),
		InspectOptions{Reverse: true},
	)
	if err != nil {
		t.Fatalf("Inspect container with reverse relations: %v", err)
	}
	if len(result.Relations) != 1 {
		t.Fatalf("expected 1 relation, got %d", len(result.Relations))
	}
	if rel := result.Relations[0]; rel.TargetType != EntityChunk || rel.TargetID != strconv.FormatInt(chunkID, 10) || rel.Direction != RelationIncoming {
		t.Fatalf("unexpected reverse relation: %+v", rel)
	}
}

func TestInspectRepositoryIncludesAggregateAndSnapshotRelations(t *testing.T) {
	dbconn := openInspectTestDB(t)
	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES (?, ?, ?)`, "snap-a", time.Now().UTC(), "full"); err != nil {
		t.Fatalf("insert snapshot a: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES (?, ?, ?)`, "snap-b", time.Now().UTC().Add(time.Minute), "full"); err != nil {
		t.Fatalf("insert snapshot b: %v", err)
	}

	svc := newServiceForTest(dbconn, nil)
	result, err := svc.Inspect(context.Background(), EntityRepository, "", InspectOptions{Relations: true, Limit: 10})
	if err != nil {
		t.Fatalf("Inspect repository with relations: %v", err)
	}
	if len(result.Relations) < 6 {
		t.Fatalf("expected at least 6 relations (4 aggregates + snapshots), got %d", len(result.Relations))
	}
}

func TestInspectMissingFileReturnsEntityNotFound(t *testing.T) {
	dbconn := openInspectTestDB(t)
	svc := newServiceForTest(dbconn, nil)

	_, err := svc.Inspect(context.Background(), EntityFile, "999", InspectOptions{})
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestInspectRejectsInvalidLogicalFileID(t *testing.T) {
	dbconn := openInspectTestDB(t)
	svc := newServiceForTest(dbconn, nil)

	ids := []string{"", "abc", "0", "-1"}
	for _, id := range ids {
		t.Run(id, func(t *testing.T) {
			_, err := svc.Inspect(context.Background(), EntityLogicalFile, id, InspectOptions{})
			if err == nil {
				t.Fatalf("expected error for id %q", id)
			}
			if !errors.Is(err, ErrInvalidTarget) {
				t.Fatalf("expected ErrInvalidTarget for id %q, got %v", id, err)
			}
		})
	}
}

func TestNormalizeInspectOptionsDefaultsToSummaryOnlyWithDefaultLimit(t *testing.T) {
	opts := normalizeInspectOptions(InspectOptions{})

	if opts.Deep {
		t.Fatal("expected deep=false by default")
	}
	if opts.Relations {
		t.Fatal("expected relations=false by default")
	}
	if opts.Reverse {
		t.Fatal("expected reverse=false by default")
	}
	if opts.Limit != DefaultInspectLimit {
		t.Fatalf("expected default limit %d, got %d", DefaultInspectLimit, opts.Limit)
	}
}

func TestNormalizeInspectOptionsDeepImpliesRelations(t *testing.T) {
	opts := normalizeInspectOptions(InspectOptions{Deep: true, Limit: 5})

	if !opts.Deep {
		t.Fatal("expected deep=true")
	}
	if !opts.Relations {
		t.Fatal("expected relations=true when deep=true")
	}
	if opts.Limit != 5 {
		t.Fatalf("expected explicit limit to be preserved, got %d", opts.Limit)
	}
}

func TestNormalizeInspectOptionsClampsLimit(t *testing.T) {
	tooLarge := normalizeInspectOptions(InspectOptions{Limit: MaxInspectLimit + 1})
	if tooLarge.Limit != MaxInspectLimit {
		t.Fatalf("expected clamped limit %d, got %d", MaxInspectLimit, tooLarge.Limit)
	}

	nonPositive := normalizeInspectOptions(InspectOptions{Limit: 0})
	if nonPositive.Limit != DefaultInspectLimit {
		t.Fatalf("expected default limit %d for non-positive input, got %d", DefaultInspectLimit, nonPositive.Limit)
	}
}
