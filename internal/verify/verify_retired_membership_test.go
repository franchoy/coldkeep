package verify

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	filestate "github.com/franchoy/coldkeep/internal/status"
)

func TestVerifyRetiredPackedMembershipCompletesEncodedDirectory(t *testing.T) {
	dbconn, containersDir, blockID, chunkIDs := seedRetiredMembershipFixture(t)
	retireVerifyPackedMember(t, dbconn, blockID, chunkIDs[0])
	if err := VerifyRepository(dbconn, containersDir); err != nil {
		t.Fatalf("active plus retired membership must exactly explain the packed directory: %v", err)
	}
}

func TestVerifyRetiredPackedMembershipRejectsUnexplainedEncodedMember(t *testing.T) {
	dbconn, containersDir, blockID, chunkIDs := seedRetiredMembershipFixture(t)
	retireVerifyPackedMember(t, dbconn, blockID, chunkIDs[0])
	if _, err := dbconn.Exec(`DELETE FROM chunk_block_refs WHERE block_id = $1 AND chunk_id = $2`, blockID, chunkIDs[1]); err != nil {
		t.Fatalf("remove membership to create unexplained directory entry: %v", err)
	}
	err := VerifyRepository(dbconn, containersDir)
	if err == nil || !strings.Contains(err.Error(), "not in chunk_block_refs or retired membership") {
		t.Fatalf("expected unexplained encoded-member failure, got %v", err)
	}
}

func TestVerifyRetiredPackedMembershipRejectsActiveRetiredOverlap(t *testing.T) {
	dbconn, containersDir, blockID, chunkIDs := seedRetiredMembershipFixture(t)
	insertVerifyRetiredMember(t, dbconn, blockID, chunkIDs[0])
	err := VerifyRepository(dbconn, containersDir)
	if err == nil || !strings.Contains(err.Error(), "conflicting active/retired packed offsets") {
		t.Fatalf("expected active/retired physical overlap failure, got %v", err)
	}
}

func TestVerifyRetiredPackedMembershipRejectsDuplicateEmbeddedID(t *testing.T) {
	dbconn, containersDir, blockID, chunkIDs := seedRetiredMembershipFixture(t)
	insertVerifyRetiredMember(t, dbconn, blockID, chunkIDs[0])
	if _, err := dbconn.Exec(`UPDATE retired_chunk_block_ref SET offset_in_block = offset_in_block + 1 WHERE block_id = $1 AND embedded_chunk_id = $2`, blockID, chunkIDs[0]); err != nil {
		t.Fatalf("separate duplicate embedded ID from its active physical offset: %v", err)
	}
	err := VerifyRepository(dbconn, containersDir)
	if err == nil || !strings.Contains(err.Error(), "duplicate or overlapping active/retired packed entries") {
		t.Fatalf("expected duplicate active/retired embedded-ID failure, got %v", err)
	}
}

func TestVerifyRetiredPackedMembershipRejectsInvalidRange(t *testing.T) {
	dbconn, containersDir, blockID, chunkIDs := seedRetiredMembershipFixture(t)
	retireVerifyPackedMember(t, dbconn, blockID, chunkIDs[0])
	if _, err := dbconn.Exec(`UPDATE retired_chunk_block_ref SET offset_in_block = 9223372036854770000 WHERE block_id = $1`, blockID); err != nil {
		t.Fatalf("create invalid retired packed range: %v", err)
	}
	err := VerifyRepository(dbconn, containersDir)
	if err == nil || !strings.Contains(err.Error(), "retired_chunk_block_ref range exceeds plaintext_size") {
		t.Fatalf("expected invalid retired-range failure, got %v", err)
	}
}

func TestVerifyContainerPayloadOccupancyIncludesRetiredLegacyExtent(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()
	containerID, attemptID := seedVerifyRetiredLegacyExtent(t, dbconn, 64, 16, 80)
	_ = containerID
	_ = attemptID
	if err := verifyContainerPayloadOccupancyContext(context.Background(), dbconn); err != nil {
		t.Fatalf("retired legacy extent must account for payload occupancy: %v", err)
	}
}

func TestVerifyContainerPayloadOccupancyRejectsRetiredOverlap(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()
	containerID, attemptID := seedVerifyRetiredLegacyExtent(t, dbconn, 64, 16, 80)
	if _, err := dbconn.Exec(`
		INSERT INTO retired_legacy_block_extent
		 (container_id, block_offset, stored_size, plaintext_size, codec, format_version,
		  historical_block_id, historical_chunk_id, repair_attempt_id)
		VALUES ($1, 72, 8, 8, 'plain', 1, 102, 202, $2)`, containerID, attemptID); err != nil {
		t.Fatalf("insert overlapping retired legacy extent: %v", err)
	}
	err := verifyContainerPayloadOccupancyContext(context.Background(), dbconn)
	if err == nil || !strings.Contains(err.Error(), "gap or overlap") {
		t.Fatalf("expected retired legacy overlap failure, got %v", err)
	}
}

func TestVerifyContainerPayloadOccupancyRejectsUnexplainedGap(t *testing.T) {
	dbconn := openVerifyTestDB(t)
	defer func() { _ = dbconn.Close() }()
	seedVerifyRetiredLegacyExtent(t, dbconn, 65, 15, 80)
	err := verifyContainerPayloadOccupancyContext(context.Background(), dbconn)
	if err == nil || !strings.Contains(err.Error(), "gap or overlap") {
		t.Fatalf("expected unexplained payload gap failure, got %v", err)
	}
}

func seedVerifyRetiredLegacyExtent(t *testing.T, dbconn *sql.DB, offset, size, currentSize int64) (int64, int64) {
	t.Helper()
	var logicalID int64
	if err := dbconn.QueryRow(`
		INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		VALUES ('retired-legacy', 1, 'retired-legacy-hash', 'COMPLETED', 0, 'v1-simple-rolling') RETURNING id`,
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert retired legacy logical file: %v", err)
	}
	var attemptID int64
	if err := dbconn.QueryRow(`
		INSERT INTO store_repair_attempt
		 (logical_file_id, source_file_hash, source_total_size, recipe_fingerprint, status)
		VALUES ($1, 'source', 1, 'fingerprint', 'PUBLISHED') RETURNING id`, logicalID,
	).Scan(&attemptID); err != nil {
		t.Fatalf("insert retired legacy attempt: %v", err)
	}
	var containerID int64
	if err := dbconn.QueryRow(`
		INSERT INTO container (filename, sealed, quarantine, current_size, max_size)
		VALUES ('retired-legacy.bin', TRUE, FALSE, $1, 1024) RETURNING id`, currentSize,
	).Scan(&containerID); err != nil {
		t.Fatalf("insert retired legacy container: %v", err)
	}
	if _, err := dbconn.Exec(`
		INSERT INTO retired_legacy_block_extent
		 (container_id, block_offset, stored_size, plaintext_size, codec, format_version,
		  historical_block_id, historical_chunk_id, repair_attempt_id)
		VALUES ($1,$2,$3,$3,'plain',1,101,201,$4)`, containerID, offset, size, attemptID); err != nil {
		t.Fatalf("insert retired legacy extent: %v", err)
	}
	return containerID, attemptID
}

func seedRetiredMembershipFixture(t *testing.T) (*sql.DB, string, int64, []int64) {
	t.Helper()
	dbconn := openVerifyTestDB(t)
	t.Cleanup(func() { _ = dbconn.Close() })
	containersDir := t.TempDir()
	blockID, chunkIDs := seedVerifyPackedBlockFixture(t, dbconn, containersDir,
		[][]byte{[]byte("retired-member"), []byte("active-member")}, nil)
	return dbconn, containersDir, blockID, chunkIDs
}

func retireVerifyPackedMember(t *testing.T, dbconn *sql.DB, blockID, chunkID int64) {
	t.Helper()
	insertVerifyRetiredMember(t, dbconn, blockID, chunkID)
	if _, err := dbconn.Exec(`DELETE FROM chunk_block_refs WHERE block_id = $1 AND chunk_id = $2`, blockID, chunkID); err != nil {
		t.Fatalf("retire active packed membership: %v", err)
	}
}

func insertVerifyRetiredMember(t *testing.T, dbconn *sql.DB, blockID, chunkID int64) {
	t.Helper()
	var offset, size int64
	if err := dbconn.QueryRow(`SELECT offset_in_block, size_in_block FROM chunk_block_refs WHERE block_id = $1 AND chunk_id = $2`, blockID, chunkID).Scan(&offset, &size); err != nil {
		t.Fatalf("load active membership for retirement: %v", err)
	}
	var logicalID int64
	if err := dbconn.QueryRow(`
		INSERT INTO logical_file (original_name, total_size, file_hash, status, ref_count, chunker_version)
		VALUES ($1, 0, $2, $3, 0, 'v1-simple-rolling') RETURNING id`,
		fmt.Sprintf("retired-%d-%d", blockID, chunkID), fmt.Sprintf("retired-hash-%d-%d", blockID, chunkID), filestate.LogicalFileCompleted,
	).Scan(&logicalID); err != nil {
		t.Fatalf("insert retired-membership attempt owner: %v", err)
	}
	var attemptID int64
	if err := dbconn.QueryRow(`
		INSERT INTO store_repair_attempt
		 (logical_file_id, source_file_hash, source_total_size, recipe_fingerprint, status)
		VALUES ($1, $2, 0, $3, 'PUBLISHED') RETURNING id`,
		logicalID, fmt.Sprintf("source-%d", chunkID), fmt.Sprintf("fingerprint-%d", chunkID),
	).Scan(&attemptID); err != nil {
		t.Fatalf("insert retired-membership repair attempt: %v", err)
	}
	if _, err := dbconn.Exec(`
		INSERT INTO retired_chunk_block_ref
		 (block_id, embedded_chunk_id, offset_in_block, size_in_block, repair_attempt_id)
		VALUES ($1,$2,$3,$4,$5)`, blockID, chunkID, offset, size, attemptID); err != nil {
		t.Fatalf("insert retired packed membership: %v", err)
	}
}
