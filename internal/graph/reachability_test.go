package graph

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	idb "github.com/franchoy/coldkeep/internal/db"
	_ "github.com/mattn/go-sqlite3"
)

func openGraphTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })

	if err := idb.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	return dbconn
}

func TestGetReachableChunksFromSnapshots(t *testing.T) {
	dbconn := openGraphTestDB(t)
	svc := NewService(dbconn)

	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES (?, CURRENT_TIMESTAMP, ?)`, "1", "full"); err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES (?)`, "docs/a.txt"); err != nil {
		t.Fatalf("insert snapshot_path: %v", err)
	}

	lfRes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`, "a.txt", 100, "lf-hash", "COMPLETED", "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert logical_file: %v", err)
	}
	logicalFileID, err := lfRes.LastInsertId()
	if err != nil {
		t.Fatalf("logical_file last insert id: %v", err)
	}

	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES (?, (SELECT id FROM snapshot_path WHERE path = ?), ?)`, "1", "docs/a.txt", logicalFileID); err != nil {
		t.Fatalf("insert snapshot_file: %v", err)
	}

	chunkARes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "chunk-a", 40, "COMPLETED", 1, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert chunk a: %v", err)
	}
	chunkAID, err := chunkARes.LastInsertId()
	if err != nil {
		t.Fatalf("chunk a last insert id: %v", err)
	}

	chunkBRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "chunk-b", 60, "COMPLETED", 1, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert chunk b: %v", err)
	}
	chunkBID, err := chunkBRes.LastInsertId()
	if err != nil {
		t.Fatalf("chunk b last insert id: %v", err)
	}

	if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES (?, ?, ?), (?, ?, ?)`, logicalFileID, chunkAID, 0, logicalFileID, chunkBID, 1); err != nil {
		t.Fatalf("insert file_chunk rows: %v", err)
	}

	reachable, err := svc.GetReachableChunks(context.Background(), []int64{1})
	if err != nil {
		t.Fatalf("GetReachableChunks: %v", err)
	}

	if len(reachable) != 2 {
		t.Fatalf("expected 2 reachable chunks, got %d", len(reachable))
	}
	if _, ok := reachable[chunkAID]; !ok {
		t.Fatalf("expected chunk %d to be reachable", chunkAID)
	}
	if _, ok := reachable[chunkBID]; !ok {
		t.Fatalf("expected chunk %d to be reachable", chunkBID)
	}
}

func TestGetReachableChunksDeduplicatesSharedChunks(t *testing.T) {
	dbconn := openGraphTestDB(t)
	svc := NewService(dbconn)

	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES (?, CURRENT_TIMESTAMP, ?), (?, CURRENT_TIMESTAMP, ?)`, "1", "full", "2", "full"); err != nil {
		t.Fatalf("insert snapshots: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES (?), (?)`, "docs/a.txt", "docs/b.txt"); err != nil {
		t.Fatalf("insert snapshot_path rows: %v", err)
	}

	lfARes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`, "a.txt", 100, "lf-a", "COMPLETED", "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert logical_file a: %v", err)
	}
	lfAID, _ := lfARes.LastInsertId()
	lfBRes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`, "b.txt", 100, "lf-b", "COMPLETED", "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert logical_file b: %v", err)
	}
	lfBID, _ := lfBRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES (?, (SELECT id FROM snapshot_path WHERE path = ?), ?), (?, (SELECT id FROM snapshot_path WHERE path = ?), ?)`, "1", "docs/a.txt", lfAID, "2", "docs/b.txt", lfBID); err != nil {
		t.Fatalf("insert snapshot_file rows: %v", err)
	}

	chunkRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "shared-chunk", 50, "COMPLETED", 2, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert shared chunk: %v", err)
	}
	sharedChunkID, _ := chunkRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES (?, ?, ?), (?, ?, ?)`, lfAID, sharedChunkID, 0, lfBID, sharedChunkID, 0); err != nil {
		t.Fatalf("insert file_chunk rows: %v", err)
	}

	reachable, err := svc.GetReachableChunks(context.Background(), []int64{1, 2})
	if err != nil {
		t.Fatalf("GetReachableChunks: %v", err)
	}

	if len(reachable) != 1 {
		t.Fatalf("expected 1 deduplicated reachable chunk, got %d", len(reachable))
	}
	if _, ok := reachable[sharedChunkID]; !ok {
		t.Fatalf("expected shared chunk %d to be reachable", sharedChunkID)
	}
}

func TestGetReachableChunks(t *testing.T) {
	dbconn := openGraphTestDB(t)
	svc := NewService(dbconn)

	if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES (?, CURRENT_TIMESTAMP, ?), (?, CURRENT_TIMESTAMP, ?)`, "1", "full", "2", "full"); err != nil {
		t.Fatalf("insert snapshots: %v", err)
	}
	if _, err := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES (?), (?)`, "docs/a.txt", "docs/b.txt"); err != nil {
		t.Fatalf("insert snapshot_path rows: %v", err)
	}

	lfARes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`, "a.txt", 100, "lf-a", "COMPLETED", "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert logical_file a: %v", err)
	}
	lfAID, _ := lfARes.LastInsertId()
	lfBRes, err := dbconn.Exec(`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`, "b.txt", 100, "lf-b", "COMPLETED", "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert logical_file b: %v", err)
	}
	lfBID, _ := lfBRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES (?, (SELECT id FROM snapshot_path WHERE path = ?), ?), (?, (SELECT id FROM snapshot_path WHERE path = ?), ?)`, "1", "docs/a.txt", lfAID, "2", "docs/b.txt", lfBID); err != nil {
		t.Fatalf("insert snapshot_file rows: %v", err)
	}

	chunkARes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "chunk-a", 40, "COMPLETED", 1, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert chunk a: %v", err)
	}
	chunkAID, _ := chunkARes.LastInsertId()
	chunkBRes, err := dbconn.Exec(`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`, "chunk-b", 50, "COMPLETED", 1, "v2-fastcdc")
	if err != nil {
		t.Fatalf("insert chunk b: %v", err)
	}
	chunkBID, _ := chunkBRes.LastInsertId()

	if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES (?, ?, ?), (?, ?, ?)`, lfAID, chunkAID, 0, lfBID, chunkBID, 0); err != nil {
		t.Fatalf("insert file_chunk rows: %v", err)
	}

	allReachable, err := svc.GetReachableChunks(context.Background(), []int64{1, 2})
	if err != nil {
		t.Fatalf("GetReachableChunks (before delete): %v", err)
	}
	if len(allReachable) != 2 {
		t.Fatalf("expected 2 reachable chunks before delete, got %d", len(allReachable))
	}

	if _, err := dbconn.Exec(`DELETE FROM snapshot_file WHERE snapshot_id = ?`, "2"); err != nil {
		t.Fatalf("delete snapshot_file rows: %v", err)
	}
	if _, err := dbconn.Exec(`DELETE FROM snapshot WHERE id = ?`, "2"); err != nil {
		t.Fatalf("delete snapshot row: %v", err)
	}

	reachableAfterDelete, err := svc.GetReachableChunks(context.Background(), []int64{1, 2})
	if err != nil {
		t.Fatalf("GetReachableChunks (after delete): %v", err)
	}
	if len(reachableAfterDelete) != 1 {
		t.Fatalf("expected 1 reachable chunk after delete, got %d", len(reachableAfterDelete))
	}
	if _, ok := reachableAfterDelete[chunkAID]; !ok {
		t.Fatalf("expected chunk %d to remain reachable", chunkAID)
	}
	if _, ok := reachableAfterDelete[chunkBID]; ok {
		t.Fatalf("expected chunk %d to be unreachable after delete", chunkBID)
	}
}

func TestGetReachableChunksMatchesLegacyAcrossRandomizedFixtures(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	for iter := 0; iter < 25; iter++ {
		dbconn := openGraphTestDB(t)
		svc := NewService(dbconn)

		snapshotCount := 2 + rng.Intn(4) // 2..5
		logicalCount := 3 + rng.Intn(6)  // 3..8
		chunkCount := 4 + rng.Intn(8)    // 4..11

		for i := 1; i <= snapshotCount; i++ {
			if _, err := dbconn.Exec(`INSERT INTO snapshot (id, created_at, type) VALUES (?, CURRENT_TIMESTAMP, ?)`, fmt.Sprintf("%d", i), "full"); err != nil {
				t.Fatalf("iter=%d insert snapshot %d: %v", iter, i, err)
			}
		}

		logicalIDs := make([]int64, 0, logicalCount)
		for i := 0; i < logicalCount; i++ {
			res, err := dbconn.Exec(
				`INSERT INTO logical_file (original_name, total_size, file_hash, status, chunker_version) VALUES (?, ?, ?, ?, ?)`,
				fmt.Sprintf("f-%d-%d.txt", iter, i),
				100+i,
				fmt.Sprintf("lf-%d-%d", iter, i),
				"COMPLETED",
				"v2-fastcdc",
			)
			if err != nil {
				t.Fatalf("iter=%d insert logical_file %d: %v", iter, i, err)
			}
			id, err := res.LastInsertId()
			if err != nil {
				t.Fatalf("iter=%d logical_file last insert id %d: %v", iter, i, err)
			}
			logicalIDs = append(logicalIDs, id)
		}

		chunkIDs := make([]int64, 0, chunkCount)
		for i := 0; i < chunkCount; i++ {
			res, err := dbconn.Exec(
				`INSERT INTO chunk (chunk_hash, size, status, live_ref_count, chunker_version) VALUES (?, ?, ?, ?, ?)`,
				fmt.Sprintf("chunk-%d-%d", iter, i),
				16+i,
				"COMPLETED",
				1,
				"v2-fastcdc",
			)
			if err != nil {
				t.Fatalf("iter=%d insert chunk %d: %v", iter, i, err)
			}
			id, err := res.LastInsertId()
			if err != nil {
				t.Fatalf("iter=%d chunk last insert id %d: %v", iter, i, err)
			}
			chunkIDs = append(chunkIDs, id)
		}

		for lfIdx, logicalID := range logicalIDs {
			assignmentCount := 1 + rng.Intn(minInt(4, len(chunkIDs)))
			perm := rng.Perm(len(chunkIDs))
			for order := 0; order < assignmentCount; order++ {
				chunkID := chunkIDs[perm[order]]
				if _, err := dbconn.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES (?, ?, ?)`, logicalID, chunkID, order); err != nil {
					t.Fatalf("iter=%d insert file_chunk lfIdx=%d order=%d: %v", iter, lfIdx, order, err)
				}
			}
		}

		for snapshotID := 1; snapshotID <= snapshotCount; snapshotID++ {
			if _, err := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES (?)`, fmt.Sprintf("docs/iter-%d-snap-%d", iter, snapshotID)); err != nil {
				t.Fatalf("iter=%d insert snapshot_path for snapshot %d: %v", iter, snapshotID, err)
			}

			assignmentCount := 1 + rng.Intn(minInt(3, len(logicalIDs)))
			perm := rng.Perm(len(logicalIDs))
			for i := 0; i < assignmentCount; i++ {
				logicalID := logicalIDs[perm[i]]
				path := fmt.Sprintf("docs/iter-%d-snap-%d-file-%d", iter, snapshotID, i)
				if _, err := dbconn.Exec(`INSERT INTO snapshot_path (path) VALUES (?)`, path); err != nil {
					t.Fatalf("iter=%d insert snapshot_file path snapshot=%d idx=%d: %v", iter, snapshotID, i, err)
				}
				if _, err := dbconn.Exec(`INSERT INTO snapshot_file (snapshot_id, path_id, logical_file_id) VALUES (?, (SELECT id FROM snapshot_path WHERE path = ?), ?)`, fmt.Sprintf("%d", snapshotID), path, logicalID); err != nil {
					t.Fatalf("iter=%d insert snapshot_file snapshot=%d idx=%d: %v", iter, snapshotID, i, err)
				}
			}
		}

		queryCount := 1 + rng.Intn(snapshotCount)
		perm := rng.Perm(snapshotCount)
		snapshotIDs := make([]int64, 0, queryCount)
		for i := 0; i < queryCount; i++ {
			snapshotIDs = append(snapshotIDs, int64(perm[i]+1))
		}

		graphReachable, err := svc.GetReachableChunks(context.Background(), snapshotIDs)
		if err != nil {
			t.Fatalf("iter=%d graph reachable: %v", iter, err)
		}

		legacyReachable, err := legacyReachableChunksForSnapshots(context.Background(), dbconn, snapshotIDs)
		if err != nil {
			t.Fatalf("iter=%d legacy reachable: %v", iter, err)
		}

		if len(graphReachable) != len(legacyReachable) {
			t.Fatalf("iter=%d mismatch sizes graph=%d legacy=%d snapshotIDs=%v", iter, len(graphReachable), len(legacyReachable), snapshotIDs)
		}
		for id := range legacyReachable {
			if _, ok := graphReachable[id]; !ok {
				t.Fatalf("iter=%d graph missing chunk=%d snapshotIDs=%v", iter, id, snapshotIDs)
			}
		}
		for id := range graphReachable {
			if _, ok := legacyReachable[id]; !ok {
				t.Fatalf("iter=%d graph has extra chunk=%d snapshotIDs=%v", iter, id, snapshotIDs)
			}
		}
	}
}

func legacyReachableChunksForSnapshots(ctx context.Context, dbconn *sql.DB, snapshotIDs []int64) (map[int64]struct{}, error) {
	if len(snapshotIDs) == 0 {
		return map[int64]struct{}{}, nil
	}

	placeholders := make([]string, len(snapshotIDs))
	args := make([]any, len(snapshotIDs))
	for i, id := range snapshotIDs {
		placeholders[i] = "?"
		args[i] = fmt.Sprintf("%d", id)
	}

	query := fmt.Sprintf(`
		SELECT DISTINCT fc.chunk_id
		FROM snapshot_file sf
		JOIN file_chunk fc ON fc.logical_file_id = sf.logical_file_id
		WHERE sf.snapshot_id IN (%s)
		ORDER BY fc.chunk_id
	`, strings.Join(placeholders, ","))

	rows, err := dbconn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[int64]struct{})
	for rows.Next() {
		var chunkID int64
		if err := rows.Scan(&chunkID); err != nil {
			return nil, err
		}
		out[chunkID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
