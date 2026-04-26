package graph

import (
	"context"
	"strings"
	"testing"
)

func TestGetReverseReferencesChunkToLogicalFiles(t *testing.T) {
	dbconn := openGraphTestDB(t)
	svc := NewService(dbconn)

	lfARes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`, "a.txt", 10, "lf-a", "COMPLETED", "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert logical_file a: %v", err)
	}
	lfAID, _ := lfARes.LastInsertId()
	lfBRes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`, "b.txt", 10, "lf-b", "COMPLETED", "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert logical_file b: %v", err)
	}
	lfBID, _ := lfBRes.LastInsertId()

	chunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "chunk-shared", 5, "COMPLETED", 2, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert chunk: %v", err)
	}
	chunkID, _ := chunkRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES (?, ?, ?), (?, ?, ?)`, lfAID, chunkID, 0, lfBID, chunkID, 0); err != nil {
		t.Fatalf("insert file_chunk rows: %v", err)
	}

	references, err := svc.GetReverseReferences(context.Background(), NodeID{Type: EntityChunk, ID: chunkID})
	if err != nil {
		t.Fatalf("GetReverseReferences: %v", err)
	}
	if len(references) != 2 {
		t.Fatalf("expected 2 reverse refs, got %d", len(references))
	}
	if references[0].Type != EntityLogicalFile || references[1].Type != EntityLogicalFile {
		t.Fatalf("expected logical_file refs, got %+v", references)
	}
	if references[0].ID != lfAID || references[1].ID != lfBID {
		t.Fatalf("expected deterministic id order [%d,%d], got %+v", lfAID, lfBID, references)
	}
}

func TestGetReverseReferencesLogicalFileToSnapshots(t *testing.T) {
	dbconn := openGraphTestDB(t)
	svc := NewService(dbconn)

	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES (?, CURRENT_TIMESTAMP, ?), (?, CURRENT_TIMESTAMP, ?)`, "1", "full", "2", "full"); err != nil {
		t.Fatalf("insert snapshots: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES (?)`, "docs/a.txt"); err != nil {
		t.Fatalf("insert snapshot_path: %v", err)
	}
	lfRes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`, "a.txt", 10, "lf-a", "COMPLETED", "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}
	lfID, _ := lfRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES (?, (SELECT id FROM snapshot_path WHERE path = ?), ?), (?, (SELECT id FROM snapshot_path WHERE path = ?), ?)`, "1", "docs/a.txt", lfID, "2", "docs/a.txt", lfID); err != nil {
		t.Fatalf("insert snapshot_file rows: %v", err)
	}

	references, err := svc.GetReverseReferences(context.Background(), NodeID{Type: EntityLogicalFile, ID: lfID})
	if err != nil {
		t.Fatalf("GetReverseReferences: %v", err)
	}
	if len(references) != 2 {
		t.Fatalf("expected 2 reverse refs, got %d", len(references))
	}
	if references[0] != (NodeID{Type: EntitySnapshot, ID: 1}) || references[1] != (NodeID{Type: EntitySnapshot, ID: 2}) {
		t.Fatalf("unexpected snapshot refs: %+v", references)
	}
}

func TestGetReverseReferencesRejectsNonNumericSnapshotID(t *testing.T) {
	dbconn := openGraphTestDB(t)
	svc := NewService(dbconn)

	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES (?, CURRENT_TIMESTAMP, ?)`, "snap-a", "full"); err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES (?)`, "docs/a.txt"); err != nil {
		t.Fatalf("insert snapshot_path: %v", err)
	}
	lfRes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`, "a.txt", 10, "lf-a", "COMPLETED", "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}
	lfID, _ := lfRes.LastInsertId()
	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES (?, (SELECT id FROM snapshot_path WHERE path = ?), ?)`, "snap-a", "docs/a.txt", lfID); err != nil {
		t.Fatalf("insert snapshot_file row: %v", err)
	}

	_, err = svc.GetReverseReferences(context.Background(), NodeID{Type: EntityLogicalFile, ID: lfID})
	if err == nil {
		t.Fatal("expected parse error")
	}
	if !strings.Contains(err.Error(), "parse snapshot id") {
		t.Fatalf("unexpected error: %v", err)
	}
}
