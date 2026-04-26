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

func TestInspectFileReturnsStableModel(t *testing.T) {
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
}

func TestInspectUnsupportedTarget(t *testing.T) {
	svc := newServiceForTest(nil, nil)

	_, err := svc.Inspect(context.Background(), EntityContainer, "7", InspectOptions{})
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrUnsupportedEntity) {
		t.Fatalf("expected ErrUnsupportedEntity, got %v", err)
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
