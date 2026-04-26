package observability

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"reflect"
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
	if rel := result.Relations[0]; rel.TargetType != EntityChunk || rel.TargetID != strconv.FormatInt(chunkID, 10) || rel.Direction != RelationIncoming || rel.Type != "referenced_by" {
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

func TestInspectSnapshotDeepTraversalExpandsRecursively(t *testing.T) {
	dbconn := openInspectTestDB(t)

	fileRes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`,
		"deep.txt", 50, "h-deep", "COMPLETED", "v2-fastcdc",
	)
	if err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}
	fileID, _ := fileRes.LastInsertId()

	chunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "chunk-deep", 50, "COMPLETED", 1, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert chunk: %v", err)
	}
	chunkID, _ := chunkRes.LastInsertId()

	ctrRes, err := dbconn.Exec(`INSERT INTO container (filename, current_size, max_size, quarantine) VALUES (?, ?, ?, ?)`, "ctr_deep.bin", 256, 1024, 0)
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
		chunkID, "plain", 1, 50, 50, containerID, 0,
	); err != nil {
		t.Fatalf("insert blocks: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES (?, ?, ?)`, "1", time.Now().UTC(), "full"); err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES (?)`, "deep.txt"); err != nil {
		t.Fatalf("insert snapshot_path: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES (?, ?, ?)`, "1", 1, fileID); err != nil {
		t.Fatalf("insert snapshot_file: %v", err)
	}

	svc := newServiceForTest(dbconn, nil)
	result, err := svc.Inspect(context.Background(), EntitySnapshot, "1", InspectOptions{Deep: true, Limit: 10})
	if err != nil {
		t.Fatalf("Inspect snapshot deep: %v", err)
	}
	if len(result.Relations) != 3 {
		t.Fatalf("expected 3 deep relations, got %d", len(result.Relations))
	}

	depthByTarget := map[string]int{}
	for _, rel := range result.Relations {
		if rel.Type != "references" || rel.Direction != RelationOutgoing {
			t.Fatalf("expected references/outgoing relation, got %+v", rel)
		}
		depthRaw, ok := rel.Metadata["depth"]
		if !ok {
			t.Fatalf("expected depth metadata, got %+v", rel)
		}
		depth, ok := depthRaw.(int)
		if !ok {
			t.Fatalf("expected depth metadata to be int, got %T", depthRaw)
		}
		depthByTarget[string(rel.TargetType)+":"+rel.TargetID] = depth
	}

	if depthByTarget[string(EntityLogicalFile)+":"+strconv.FormatInt(fileID, 10)] != 1 {
		t.Fatalf("expected logical file at depth 1, got %+v", depthByTarget)
	}
	if depthByTarget[string(EntityChunk)+":"+strconv.FormatInt(chunkID, 10)] != 2 {
		t.Fatalf("expected chunk at depth 2, got %+v", depthByTarget)
	}
	if depthByTarget[string(EntityContainer)+":"+strconv.FormatInt(containerID, 10)] != 3 {
		t.Fatalf("expected container at depth 3, got %+v", depthByTarget)
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

func TestInspectSnapshotRelations(t *testing.T) {
	dbconn := openInspectTestDB(t)

	fileRes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`, "snap-rel.txt", 12, "h-snap-rel", "COMPLETED", "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}
	fileID, _ := fileRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES (?, ?, ?)`, "snap-rel-1", time.Now().UTC(), "full"); err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES (?)`, "snap-rel.txt"); err != nil {
		t.Fatalf("insert snapshot_path: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES (?, ?, ?)`, "snap-rel-1", 1, fileID); err != nil {
		t.Fatalf("insert snapshot_file: %v", err)
	}

	svc := newServiceForTest(dbconn, nil)
	result, err := svc.Inspect(context.Background(), EntitySnapshot, "snap-rel-1", InspectOptions{Relations: true})
	if err != nil {
		t.Fatalf("Inspect snapshot relations: %v", err)
	}
	if len(result.Relations) != 1 {
		t.Fatalf("expected 1 relation, got %d", len(result.Relations))
	}
	rel := result.Relations[0]
	if rel.Type != "references" || rel.Direction != RelationOutgoing || rel.TargetType != EntityLogicalFile || rel.TargetID != strconv.FormatInt(fileID, 10) {
		t.Fatalf("unexpected relation: %+v", rel)
	}
}

func TestInspectLogicalFileForwardRelations(t *testing.T) {
	dbconn := openInspectTestDB(t)

	fileRes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`, "forward.txt", 14, "h-forward", "COMPLETED", "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}
	fileID, _ := fileRes.LastInsertId()

	chunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "chunk-forward", 14, "COMPLETED", 1, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert chunk: %v", err)
	}
	chunkID, _ := chunkRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES (?, ?, ?)`, fileID, chunkID, 0); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	svc := newServiceForTest(dbconn, nil)
	result, err := svc.Inspect(context.Background(), EntityLogicalFile, strconv.FormatInt(fileID, 10), InspectOptions{Relations: true})
	if err != nil {
		t.Fatalf("Inspect logical file forward relations: %v", err)
	}
	if len(result.Relations) != 1 {
		t.Fatalf("expected 1 relation, got %d", len(result.Relations))
	}
	rel := result.Relations[0]
	if rel.Type != "references" || rel.Direction != RelationOutgoing || rel.TargetType != EntityChunk || rel.TargetID != strconv.FormatInt(chunkID, 10) {
		t.Fatalf("unexpected relation: %+v", rel)
	}
}

func TestInspectLogicalFileReverseRelations(t *testing.T) {
	dbconn := openInspectTestDB(t)

	fileRes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`, "reverse.txt", 22, "h-reverse", "COMPLETED", "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}
	fileID, _ := fileRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES (?, ?, ?)`, "99", time.Now().UTC(), "full"); err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES (?)`, "reverse.txt"); err != nil {
		t.Fatalf("insert snapshot_path: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES (?, ?, ?)`, "99", 1, fileID); err != nil {
		t.Fatalf("insert snapshot_file: %v", err)
	}

	svc := newServiceForTest(dbconn, nil)
	result, err := svc.Inspect(context.Background(), EntityLogicalFile, strconv.FormatInt(fileID, 10), InspectOptions{Reverse: true})
	if err != nil {
		t.Fatalf("Inspect logical file reverse relations: %v", err)
	}
	if len(result.Relations) != 1 {
		t.Fatalf("expected 1 relation, got %d", len(result.Relations))
	}
	rel := result.Relations[0]
	if rel.Type != "referenced_by" || rel.Direction != RelationIncoming || rel.TargetType != EntitySnapshot || rel.TargetID != "99" {
		t.Fatalf("unexpected relation: %+v", rel)
	}
}

func TestInspectChunkReverseRelations(t *testing.T) {
	dbconn := openInspectTestDB(t)

	fileRes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`, "chunk-rev.txt", 18, "h-chunk-rev", "COMPLETED", "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}
	fileID, _ := fileRes.LastInsertId()

	chunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "chunk-rev", 18, "COMPLETED", 1, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert chunk: %v", err)
	}
	chunkID, _ := chunkRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES (?, ?, ?)`, fileID, chunkID, 0); err != nil {
		t.Fatalf("insert file_chunk: %v", err)
	}

	svc := newServiceForTest(dbconn, nil)
	result, err := svc.Inspect(context.Background(), EntityChunk, strconv.FormatInt(chunkID, 10), InspectOptions{Reverse: true})
	if err != nil {
		t.Fatalf("Inspect chunk reverse relations: %v", err)
	}
	if len(result.Relations) != 1 {
		t.Fatalf("expected 1 relation, got %d", len(result.Relations))
	}
	rel := result.Relations[0]
	if rel.Type != "referenced_by" || rel.Direction != RelationIncoming || rel.TargetType != EntityLogicalFile || rel.TargetID != strconv.FormatInt(fileID, 10) {
		t.Fatalf("unexpected relation: %+v", rel)
	}
}

func TestInspectDeepTraversalLimited(t *testing.T) {
	dbconn := openInspectTestDB(t)

	fileRes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`, "deep-limited.txt", 60, "h-deep-limited", "COMPLETED", "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}
	fileID, _ := fileRes.LastInsertId()

	chunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "chunk-deep-limited", 60, "COMPLETED", 1, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert chunk: %v", err)
	}
	chunkID, _ := chunkRes.LastInsertId()

	ctrRes, err := dbconn.Exec(`INSERT INTO container (filename, current_size, max_size, quarantine) VALUES (?, ?, ?, ?)`, "ctr_deep_limited.bin", 256, 1024, 0)
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
		chunkID, "plain", 1, 60, 60, containerID, 0,
	); err != nil {
		t.Fatalf("insert blocks: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES (?, ?, ?)`, "3", time.Now().UTC(), "full"); err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES (?)`, "deep-limited.txt"); err != nil {
		t.Fatalf("insert snapshot_path: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES (?, ?, ?)`, "3", 1, fileID); err != nil {
		t.Fatalf("insert snapshot_file: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	svc := newServiceForTest(dbconn, nil)
	result, err := svc.Inspect(ctx, EntitySnapshot, "3", InspectOptions{Deep: true, Limit: 2})
	if err != nil {
		t.Fatalf("Inspect deep limited: %v", err)
	}

	if len(result.Relations) > 2 {
		t.Fatalf("expected relation limit <= 2, got %d", len(result.Relations))
	}

	seen := make(map[string]struct{}, len(result.Relations))
	for _, rel := range result.Relations {
		key := string(rel.TargetType) + ":" + rel.TargetID
		if _, exists := seen[key]; exists {
			t.Fatalf("duplicate relation suggests traversal issue: %s", key)
		}
		seen[key] = struct{}{}
	}
}

func TestInspectDeterministicOutput(t *testing.T) {
	dbconn := openInspectTestDB(t)

	fileRes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`, "det.txt", 33, "h-det", "COMPLETED", "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}
	fileID, _ := fileRes.LastInsertId()

	chunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "chunk-det", 33, "COMPLETED", 1, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert chunk: %v", err)
	}
	chunkID, _ := chunkRes.LastInsertId()

	ctrRes, err := dbconn.Exec(`INSERT INTO container (filename, current_size, max_size, quarantine) VALUES (?, ?, ?, ?)`, "ctr_det.bin", 128, 1024, 0)
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
		chunkID, "plain", 1, 33, 33, containerID, 0,
	); err != nil {
		t.Fatalf("insert blocks: %v", err)
	}

	fixedNow := time.Date(2026, time.April, 26, 18, 0, 0, 0, time.UTC)
	svc := newServiceForTest(dbconn, func() time.Time { return fixedNow })

	r1, err := svc.Inspect(context.Background(), EntityChunk, strconv.FormatInt(chunkID, 10), InspectOptions{Relations: true, Reverse: true, Limit: 10})
	if err != nil {
		t.Fatalf("first inspect: %v", err)
	}
	r2, err := svc.Inspect(context.Background(), EntityChunk, strconv.FormatInt(chunkID, 10), InspectOptions{Relations: true, Reverse: true, Limit: 10})
	if err != nil {
		t.Fatalf("second inspect: %v", err)
	}

	b1, err := json.Marshal(r1)
	if err != nil {
		t.Fatalf("marshal first: %v", err)
	}
	b2, err := json.Marshal(r2)
	if err != nil {
		t.Fatalf("marshal second: %v", err)
	}
	if !bytes.Equal(b1, b2) {
		t.Fatalf("expected deterministic output\nfirst:  %s\nsecond: %s", string(b1), string(b2))
	}
}

func TestInspectInvalidEntity(t *testing.T) {
	svc := newServiceForTest(nil, nil)
	_, err := svc.Inspect(context.Background(), EntityPhysicalFile, "1", InspectOptions{})
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrUnsupportedEntity) {
		t.Fatalf("expected ErrUnsupportedEntity, got %v", err)
	}
}

func TestInspectInvalidID(t *testing.T) {
	dbconn := openInspectTestDB(t)
	svc := newServiceForTest(dbconn, nil)

	for _, tc := range []struct {
		entity EntityType
		id     string
	}{
		{entity: EntityLogicalFile, id: "0"},
		{entity: EntityChunk, id: "abc"},
		{entity: EntityContainer, id: "-2"},
		{entity: EntitySnapshot, id: ""},
	} {
		t.Run(string(tc.entity)+"/"+tc.id, func(t *testing.T) {
			_, err := svc.Inspect(context.Background(), tc.entity, tc.id, InspectOptions{})
			if err == nil {
				t.Fatalf("expected invalid target error for entity=%s id=%q", tc.entity, tc.id)
			}
			if !errors.Is(err, ErrInvalidTarget) {
				t.Fatalf("expected ErrInvalidTarget, got %v", err)
			}
		})
	}
}

func TestInspectNotFound(t *testing.T) {
	dbconn := openInspectTestDB(t)
	svc := newServiceForTest(dbconn, nil)

	_, err := svc.Inspect(context.Background(), EntityChunk, "999", InspectOptions{})
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestInspectDoesNotMutateState(t *testing.T) {
	dbconn := openInspectTestDB(t)

	fileRes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`, "immut.txt", 77, "h-immut", "COMPLETED", "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}
	fileID, _ := fileRes.LastInsertId()

	chunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "chunk-immut", 77, "COMPLETED", 1, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert chunk: %v", err)
	}
	chunkID, _ := chunkRes.LastInsertId()

	ctrRes, err := dbconn.Exec(`INSERT INTO container (filename, current_size, max_size, quarantine) VALUES (?, ?, ?, ?)`, "ctr_immut.bin", 256, 1024, 0)
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
		chunkID, "plain", 1, 77, 77, containerID, 0,
	); err != nil {
		t.Fatalf("insert blocks: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES (?, ?, ?)`, "7", time.Now().UTC(), "full"); err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES (?)`, "immut.txt"); err != nil {
		t.Fatalf("insert snapshot_path: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES (?, ?, ?)`, "7", 1, fileID); err != nil {
		t.Fatalf("insert snapshot_file: %v", err)
	}

	countTables := []string{"logical_file", "chunk", "container", "file_chunk", "blocks", "snapshot", "snapshot_file", "snapshot_path", "repository_config"}
	before := make(map[string]int64, len(countTables))
	for _, table := range countTables {
		var c int64
		if err := dbconn.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&c); err != nil {
			t.Fatalf("count before %s: %v", table, err)
		}
		before[table] = c
	}

	svc := newServiceForTest(dbconn, nil)
	_, err = svc.Inspect(context.Background(), EntitySnapshot, "7", InspectOptions{Relations: true, Deep: true, Reverse: true, Limit: 10})
	if err != nil {
		t.Fatalf("Inspect should not mutate (snapshot): %v", err)
	}
	_, err = svc.Inspect(context.Background(), EntityLogicalFile, strconv.FormatInt(fileID, 10), InspectOptions{Relations: true, Deep: true, Reverse: true, Limit: 10})
	if err != nil {
		t.Fatalf("Inspect should not mutate (logical file): %v", err)
	}
	_, err = svc.Inspect(context.Background(), EntityChunk, strconv.FormatInt(chunkID, 10), InspectOptions{Relations: true, Deep: true, Reverse: true, Limit: 10})
	if err != nil {
		t.Fatalf("Inspect should not mutate (chunk): %v", err)
	}

	after := make(map[string]int64, len(countTables))
	for _, table := range countTables {
		var c int64
		if err := dbconn.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&c); err != nil {
			t.Fatalf("count after %s: %v", table, err)
		}
		after[table] = c
	}

	if !reflect.DeepEqual(before, after) {
		t.Fatalf("inspect mutated repository state\nbefore: %+v\nafter:  %+v", before, after)
	}
}

func TestInspectChunkForwardRelationsIncludesContainer(t *testing.T) {
	dbconn := openInspectTestDB(t)

	chunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "chunk-fwd-ctr", 88, "COMPLETED", 1, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert chunk: %v", err)
	}
	chunkID, _ := chunkRes.LastInsertId()

	ctrRes, err := dbconn.Exec(`INSERT INTO container (filename, current_size, max_size, quarantine) VALUES (?, ?, ?, ?)`, "ctr_fwd.bin", 512, 1024, 0)
	if err != nil {
		t.Fatalf("insert container: %v", err)
	}
	containerID, _ := ctrRes.LastInsertId()

	if _, err := dbconn.Exec(
		`INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, container_id, block_offset) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		chunkID, "plain", 1, 88, 88, containerID, 0,
	); err != nil {
		t.Fatalf("insert blocks: %v", err)
	}

	svc := newServiceForTest(dbconn, nil)
	result, err := svc.Inspect(context.Background(), EntityChunk, strconv.FormatInt(chunkID, 10), InspectOptions{Relations: true})
	if err != nil {
		t.Fatalf("Inspect chunk with relations: %v", err)
	}
	if len(result.Relations) != 1 {
		t.Fatalf("expected 1 forward relation, got %d: %+v", len(result.Relations), result.Relations)
	}
	rel := result.Relations[0]
	if rel.Type != "references" || rel.Direction != RelationOutgoing || rel.TargetType != EntityContainer || rel.TargetID != strconv.FormatInt(containerID, 10) {
		t.Fatalf("unexpected chunk→container relation: %+v", rel)
	}
}

func TestInspectNoFSWrites(t *testing.T) {
	dbconn := openInspectTestDB(t)

	fileRes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`, "fswrite.txt", 55, "h-fswrite", "COMPLETED", "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}
	fileID, _ := fileRes.LastInsertId()

	// snapshot with numeric id for deep traversal
	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES (?, ?, ?)`, "50", time.Now().UTC(), "full"); err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES (?)`, "fswrite.txt"); err != nil {
		t.Fatalf("insert snapshot_path: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES (?, ?, ?)`, "50", 1, fileID); err != nil {
		t.Fatalf("insert snapshot_file: %v", err)
	}

	workDir := t.TempDir()

	// Record files present before
	entriesBefore := dirEntries(t, workDir)

	svc := newServiceForTest(dbconn, nil)
	_, err = svc.Inspect(context.Background(), EntitySnapshot, "50", InspectOptions{Relations: true, Deep: true, Limit: 10})
	if err != nil {
		t.Fatalf("Inspect snapshot: %v", err)
	}
	_, err = svc.Inspect(context.Background(), EntityLogicalFile, strconv.FormatInt(fileID, 10), InspectOptions{Relations: true, Reverse: true})
	if err != nil {
		t.Fatalf("Inspect logical file: %v", err)
	}

	entriesAfter := dirEntries(t, workDir)
	if len(entriesAfter) != len(entriesBefore) {
		t.Fatalf("inspect wrote files to workdir: before=%v after=%v", entriesBefore, entriesAfter)
	}
}

func dirEntries(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir %s: %v", dir, err)
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	return names
}
