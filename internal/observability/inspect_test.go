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
