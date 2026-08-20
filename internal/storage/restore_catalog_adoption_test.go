package storage

import (
	"bytes"
	"context"
	"database/sql"
	"reflect"
	"testing"

	"github.com/franchoy/coldkeep/internal/catalog"
	filestate "github.com/franchoy/coldkeep/internal/status"
)

func TestCatalogRestorePlanPreservesLegacyQueryExecutionMetadata(t *testing.T) {
	for _, tc := range []struct {
		name   string
		packed bool
	}{
		{name: "legacy"},
		{name: "packed", packed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dbconn := setupStep8DB(t)
			defer func() { _ = dbconn.Close() }()
			containersDir := t.TempDir()
			chunks := []restoreChunkSeed{
				insertChunkForRestore(t, dbconn, []byte("catalog-adoption-a"), "v1-simple-rolling"),
				insertChunkForRestore(t, dbconn, []byte("catalog-adoption-b"), "v1-simple-rolling"),
			}
			fileID := insertLogicalFileForRestore(t, dbconn, tc.name+".bin", chunks, "v1-simple-rolling")
			if tc.packed {
				insertPackedStorageBlock(t, dbconn, containersDir, "packed.ckc", chunks)
			} else {
				containerID := insertContainerWithPayload(t, dbconn, containersDir, "legacy.ckc", chunks[0].payload)
				insertLegacyBlocksRows(t, dbconn, containerID, chunks)
			}

			legacyRows := loadLegacyRestoreRowsForParity(t, dbconn, fileID)
			plan, err := catalog.NewServiceFromSQL(dbconn).LoadRestorePlanMetadata(context.Background(), catalog.RestorePlanInput{Selector: catalog.RestoreByFileID, FileID: fileID})
			if err != nil {
				t.Fatalf("load catalog restore plan: %v", err)
			}
			catalogRows, pinnedIDs, err := restoreRowsFromCatalogPlan(plan)
			if err != nil {
				t.Fatalf("map catalog restore plan: %v", err)
			}
			assertRestoreExecutionMetadataParity(t, legacyRows, catalogRows)

			wantIDs := []int64{chunks[0].id, chunks[1].id}
			if !reflect.DeepEqual(pinnedIDs, wantIDs) {
				t.Fatalf("pinned IDs=%v want=%v", pinnedIDs, wantIDs)
			}
			_, _, _, actualPinned, err := pinLogicalFileRestoreChunks(dbconn, fileID)
			if err != nil {
				t.Fatalf("pin catalog restore plan: %v", err)
			}
			if !reflect.DeepEqual(actualPinned, wantIDs) {
				t.Fatalf("actual pinned IDs=%v want=%v", actualPinned, wantIDs)
			}
			if err := unpinRestoreChunks(dbconn, actualPinned); err != nil {
				t.Fatalf("unpin catalog restore plan: %v", err)
			}
			for _, id := range wantIDs {
				var count int64
				if err := dbconn.QueryRow(`SELECT pin_count FROM chunk WHERE id=$1`, id).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != 0 {
					t.Fatalf("chunk %d pin_count=%d after unpin", id, count)
				}
			}
		})
	}
}

func loadLegacyRestoreRowsForParity(t *testing.T, dbconn *sql.DB, fileID int64) []restoreChunkRow {
	t.Helper()
	rows, err := dbconn.Query(`
SELECT fc.chunk_order, COALESCE(b.block_offset,0), COALESCE(b.plaintext_size,c.size),
       COALESCE(b.stored_size,c.size), c.chunk_hash, sb.block_hash,
       sb.compressed_hash, sb.physical_hash, c.chunker_version, c.size,
       COALESCE(b.codec,'plain'), COALESCE(b.format_version,1), b.nonce,
       COALESCE(b.container_id,0), ctr.filename, c.status, ctr.max_size, c.id
FROM file_chunk fc
JOIN chunk c ON c.id=fc.chunk_id
LEFT JOIN blocks b ON b.chunk_id=c.id
LEFT JOIN chunk_block_refs r ON r.chunk_id=c.id
LEFT JOIN storage_blocks sb ON sb.id=r.block_id
LEFT JOIN container ctr ON ctr.id=COALESCE(b.container_id,sb.container_id)
WHERE fc.logical_file_id=$1 AND c.status=$2
ORDER BY fc.chunk_order`, fileID, filestate.ChunkCompleted)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	got := make([]restoreChunkRow, 0)
	for rows.Next() {
		var row restoreChunkRow
		if err := rows.Scan(&row.chunkOrder, &row.blockOffset, &row.plaintextSize,
			&row.storedSize, &row.expectedChunkHash, &row.blockHash,
			&row.compressedHash, &row.physicalHash, &row.chunkerVersion,
			&row.chunkSize, &row.blocksCodec, &row.blocksFormatVersion,
			&row.blocksNonce, &row.blocksContainerID, &row.filename,
			&row.chunkStatus, &row.maxSize, &row.chunkID); err != nil {
			t.Fatal(err)
		}
		got = append(got, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return got
}

func assertRestoreExecutionMetadataParity(t *testing.T, oldRows, newRows []restoreChunkRow) {
	t.Helper()
	if len(oldRows) != len(newRows) {
		t.Fatalf("row count old=%d new=%d", len(oldRows), len(newRows))
	}
	for i := range oldRows {
		old, current := oldRows[i], newRows[i]
		if old.chunkOrder != current.chunkOrder || old.chunkID != current.chunkID ||
			old.expectedChunkHash != current.expectedChunkHash || old.chunkerVersion != current.chunkerVersion ||
			old.chunkSize != current.chunkSize || old.chunkStatus != current.chunkStatus ||
			old.filename != current.filename || old.maxSize != current.maxSize ||
			!bytes.Equal(old.blockHash, current.blockHash) ||
			!bytes.Equal(old.compressedHash, current.compressedHash) ||
			!bytes.Equal(old.physicalHash, current.physicalHash) {
			t.Fatalf("execution metadata mismatch row=%d old=%+v new=%+v", i, old, current)
		}
		if len(old.blockHash) == 0 && (old.blockOffset != current.blockOffset ||
			old.plaintextSize != current.plaintextSize || old.storedSize != current.storedSize ||
			old.blocksCodec != current.blocksCodec || old.blocksFormatVersion != current.blocksFormatVersion ||
			old.blocksContainerID != current.blocksContainerID || !bytes.Equal(old.blocksNonce, current.blocksNonce)) {
			t.Fatalf("legacy execution metadata mismatch row=%d old=%+v new=%+v", i, old, current)
		}
	}
}
