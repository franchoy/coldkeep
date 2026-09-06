package maintenance

import (
	"context"
	"database/sql"
	"testing"

	"github.com/franchoy/coldkeep/internal/retention"
)

func TestStoreRepairGCSweepsRetiredPackedMembershipBeforeBlock(t *testing.T) {
	dbconn := openPackedGCUnitDB(t)
	logicalID, attemptID := seedPublishedRepairAttemptForGC(t, dbconn, "retired-packed")
	_ = logicalID
	containerResult, err := dbconn.Exec(`
		INSERT INTO container (filename, sealed, quarantine, current_size, max_size)
		VALUES ('retired-packed.bin', TRUE, FALSE, 80, 1024)`)
	if err != nil {
		t.Fatalf("insert retired packed container: %v", err)
	}
	containerID, _ := containerResult.LastInsertId()
	blockResult, err := dbconn.Exec(`
		INSERT INTO storage_blocks
		 (format_version, codec, plaintext_size, compression_codec, compression_level,
		  compressed_size, stored_size, container_id, container_offset, block_hash,
		  compression_ratio, payload_hash, compressed_hash, physical_hash)
		VALUES (1, 'none', 16, 'none', NULL, 16, 16, $1, 64, x'01', 1.0, '01', x'01', x'01')`, containerID)
	if err != nil {
		t.Fatalf("insert retired packed block: %v", err)
	}
	blockID, _ := blockResult.LastInsertId()
	if _, err := dbconn.Exec(`
		INSERT INTO retired_chunk_block_ref
		 (block_id, embedded_chunk_id, offset_in_block, size_in_block, repair_attempt_id)
		VALUES ($1, 9191, 0, 16, $2)`, blockID, attemptID); err != nil {
		t.Fatalf("insert retired packed membership: %v", err)
	}

	if err := SweepUnreachableChunks(context.Background(), dbconn, containerID); err != nil {
		t.Fatalf("sweep retired packed block: %v", err)
	}
	assertRepairGCCount(t, dbconn, `SELECT COUNT(*) FROM storage_blocks WHERE id = $1`, blockID, 0)
	assertRepairGCCount(t, dbconn, `SELECT COUNT(*) FROM retired_chunk_block_ref WHERE block_id = $1`, blockID, 0)
	assertRepairGCCount(t, dbconn, `SELECT COUNT(*) FROM store_repair_attempt WHERE id = $1`, attemptID, 0)
}

func TestStoreRepairGCSweepsRetiredLegacyExtentAndStatsCountItDead(t *testing.T) {
	dbconn := openPackedGCUnitDB(t)
	_, attemptID := seedPublishedRepairAttemptForGC(t, dbconn, "retired-legacy")
	containerResult, err := dbconn.Exec(`
		INSERT INTO container (filename, sealed, quarantine, current_size, max_size)
		VALUES ('retired-legacy.bin', TRUE, FALSE, 80, 1024)`)
	if err != nil {
		t.Fatalf("insert retired legacy container: %v", err)
	}
	containerID, _ := containerResult.LastInsertId()
	if _, err := dbconn.Exec(`
		INSERT INTO retired_legacy_block_extent
		 (container_id, block_offset, stored_size, plaintext_size, codec, format_version,
		  historical_block_id, historical_chunk_id, repair_attempt_id)
		VALUES ($1, 64, 16, 16, 'plain', 1, 81, 82, $2)`, containerID, attemptID); err != nil {
		t.Fatalf("insert retired legacy extent: %v", err)
	}

	records, _, deadBytes, healthyDeadBytes, err := collectPhysicalStorageStats(
		context.Background(), dbconn, &retention.ProtectedStorageSet{
			ProtectedCompletedChunkIDs: map[int64]struct{}{},
			ProtectedPackedBlockIDs:    map[int64]struct{}{},
		})
	if err != nil {
		t.Fatalf("collect retired legacy stats: %v", err)
	}
	if len(records) != 1 || records[0].DeadBytes != 16 || deadBytes != 16 || healthyDeadBytes != 16 {
		t.Fatalf("retired legacy stats records=%+v dead=%d healthy_dead=%d", records, deadBytes, healthyDeadBytes)
	}

	if err := SweepUnreachableChunks(context.Background(), dbconn, containerID); err != nil {
		t.Fatalf("sweep retired legacy extent: %v", err)
	}
	assertRepairGCCount(t, dbconn, `SELECT COUNT(*) FROM retired_legacy_block_extent WHERE container_id = $1`, containerID, 0)
	assertRepairGCCount(t, dbconn, `SELECT COUNT(*) FROM store_repair_attempt WHERE id = $1`, attemptID, 0)
}

func seedPublishedRepairAttemptForGC(t *testing.T, dbconn interface {
	QueryRow(string, ...any) *sql.Row
}, suffix string) (int64, int64) {
	t.Helper()
	var logicalID int64
	if err := dbconn.QueryRow(`
		INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		VALUES ($1, 1, $2, 'COMPLETED', 0, 'v2-fastcdc') RETURNING id`, suffix, "hash-"+suffix,
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert repair GC logical file: %v", err)
	}
	var attemptID int64
	if err := dbconn.QueryRow(`
		INSERT INTO store_repair_attempt
		 (logical_file_id, source_file_hash, source_total_size, recipe_fingerprint, status)
		VALUES ($1, $2, 1, 'fingerprint', 'PUBLISHED') RETURNING id`, logicalID, "hash-"+suffix,
	).Scan(&attemptID); err != nil {
		t.Fatalf("insert published repair attempt: %v", err)
	}
	return logicalID, attemptID
}

func assertRepairGCCount(t *testing.T, dbconn interface {
	QueryRow(string, ...any) *sql.Row
}, query string, id, want int64) {
	t.Helper()
	var got int64
	if err := dbconn.QueryRow(query, id).Scan(&got); err != nil {
		t.Fatalf("count repair GC state: %v", err)
	}
	if got != want {
		t.Fatalf("repair GC count=%d, want %d for %q", got, want, query)
	}
}
