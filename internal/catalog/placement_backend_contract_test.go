package catalog_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/franchoy/coldkeep/internal/catalog"
	"github.com/franchoy/coldkeep/internal/testutil/backendtest"
)

const (
	placementLogicalID   int64 = 51
	placementEmptyID     int64 = 52
	placementMalformedID int64 = 53
)

func TestCatalogContractChunkPlacementsAcrossBackends(t *testing.T) {
	backendtest.ForEach(t, backendtest.Options{}, func(t *testing.T, backend backendtest.Backend) {
		seedPlacementContractFixture(t, backend)
		svc := catalog.NewServiceFromSQL(backend.DB)
		before := catalogStateCounts(t, backend.DB)

		first, err := svc.LoadChunkPlacements(context.Background(), placementLogicalID)
		if err != nil {
			t.Fatalf("LoadChunkPlacements: %v", err)
		}
		second, err := svc.LoadChunkPlacements(context.Background(), placementLogicalID)
		if err != nil {
			t.Fatalf("LoadChunkPlacements repeated: %v", err)
		}
		if !reflect.DeepEqual(first, second) {
			t.Fatalf("placements not deterministic: first=%+v second=%+v", first, second)
		}
		assertPlacementContractRows(t, first)
		assertPlacementParityWithRestoreQueries(t, backend, first)

		empty, err := svc.LoadChunkPlacements(context.Background(), placementEmptyID)
		if err != nil || len(empty) != 0 || empty == nil {
			t.Fatalf("zero-length placements: got=%#v err=%v", empty, err)
		}
		if got, err := svc.LoadChunkPlacements(context.Background(), 999999); got != nil || !catalog.IsCode(err, catalog.ErrorNotFound) {
			t.Fatalf("missing logical file: got=%+v err=%v", got, err)
		}
		if got, err := svc.LoadChunkPlacements(context.Background(), 0); got != nil || !catalog.IsCode(err, catalog.ErrorInvalidArgument) {
			t.Fatalf("invalid ID: got=%+v err=%v", got, err)
		}
		if got, err := svc.LoadChunkPlacements(context.Background(), placementMalformedID); got != nil || !catalog.IsCode(err, catalog.ErrorInvariantViolation) {
			t.Fatalf("missing placement: got=%+v err=%v", got, err)
		}

		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		if got, err := svc.LoadChunkPlacements(cancelled, placementLogicalID); got != nil || !catalog.IsCode(err, catalog.ErrorCancelled) || !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled placements: got=%+v err=%v", got, err)
		}
		if after := catalogStateCounts(t, backend.DB); after != before {
			t.Fatalf("placement reads mutated catalog: before=%+v after=%+v", before, after)
		}
	})
}

func seedPlacementContractFixture(t *testing.T, backend backendtest.Backend) {
	t.Helper()
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := backend.DB.ExecContext(context.Background(), query, args...); err != nil {
			t.Fatalf("seed placement fixture: %v\nquery: %s", err, query)
		}
	}
	for _, row := range []struct {
		id, size   int64
		name, hash string
	}{
		{placementLogicalID, 20, "mixed.bin", "file-hash-mixed"},
		{placementEmptyID, 0, "empty.bin", "file-hash-empty"},
		{placementMalformedID, 1, "malformed.bin", "file-hash-malformed"},
	} {
		exec(`INSERT INTO logical_file (id, original_name, total_size, file_hash, ref_count, chunker_version, status) VALUES ($1,$2,$3,$4,1,'v2-fastcdc','COMPLETED')`, row.id, row.name, row.size, row.hash)
	}
	for _, row := range []struct {
		id       int64
		filename string
		hash     any
	}{
		{301, "legacy.ckc", "legacy-container-hash"},
		{302, "packed.ckc", nil},
	} {
		exec(`INSERT INTO container (id, filename, sealed, sealing, container_hash, quarantine, current_size, max_size) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, row.id, row.filename, true, false, row.hash, false, int64(500), int64(4096))
	}
	for _, row := range []struct {
		id, size int64
		hash     string
	}{
		{101, 9, "chunk-hash-legacy"},
		{102, 11, "chunk-hash-packed"},
		{103, 1, "chunk-hash-malformed"},
	} {
		exec(`INSERT INTO chunk (id, chunk_hash, size, status, chunker_version) VALUES ($1,$2,$3,'COMPLETED','v2-fastcdc')`, row.id, row.hash, row.size)
	}
	exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1,101,0)`, placementLogicalID)
	exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1,102,1)`, placementLogicalID)
	exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1,103,0)`, placementMalformedID)
	exec(`INSERT INTO blocks (id, chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset) VALUES (201,101,'plain',1,9,9,NULL,301,64)`)
	// Packed writes retain this companion legacy row. The catalog must choose the
	// packed reference below and return one placement form, not both.
	exec(`INSERT INTO blocks (id, chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset) VALUES (202,102,'plain',1,11,11,NULL,302,120)`)
	exec(`INSERT INTO storage_blocks (id, format_version, codec, plaintext_size, compression_codec, compression_level, compressed_size, stored_size, container_id, container_offset, block_hash, compressed_hash, physical_hash) VALUES (401,1,'none',100,'none',NULL,NULL,80,302,80,$1,$2,$3)`, []byte{1, 2, 3}, []byte{4, 5}, []byte{6, 7})
	exec(`INSERT INTO chunk_block_refs (chunk_id, block_id, offset_in_block, size_in_block) VALUES (102,401,20,11)`)
}

func assertPlacementContractRows(t *testing.T, got []catalog.ChunkPlacementRef) {
	t.Helper()
	if len(got) != 2 {
		t.Fatalf("placements len=%d got=%+v", len(got), got)
	}
	legacy := got[0]
	if legacy.ChunkOrder != 0 || legacy.ChunkID != 101 || legacy.Kind != catalog.PlacementLegacy || legacy.Legacy == nil || legacy.Packed != nil {
		t.Fatalf("legacy placement: %+v", legacy)
	}
	if legacy.Legacy.BlockID != 201 || legacy.Legacy.Container.ID != 301 || legacy.Legacy.ContainerOffset != 64 || legacy.Legacy.Container.Filename != "legacy.ckc" || legacy.Legacy.Container.ContainerHash != "legacy-container-hash" {
		t.Fatalf("legacy metadata: %+v", legacy.Legacy)
	}
	packed := got[1]
	if packed.ChunkOrder != 1 || packed.ChunkID != 102 || packed.Kind != catalog.PlacementPacked || packed.Packed == nil || packed.Legacy != nil {
		t.Fatalf("packed placement: %+v", packed)
	}
	if packed.Packed.BlockID != 401 || packed.Packed.Container.ID != 302 || packed.Packed.ContainerOffset != 80 || packed.Packed.OffsetInBlock != 20 || packed.Packed.SizeInBlock != 11 {
		t.Fatalf("packed metadata: %+v", packed.Packed)
	}
	if !bytes.Equal(packed.Packed.BlockHash, []byte{1, 2, 3}) || !bytes.Equal(packed.Packed.CompressedHash, []byte{4, 5}) || !bytes.Equal(packed.Packed.PhysicalHash, []byte{6, 7}) {
		t.Fatalf("packed hashes: %+v", packed.Packed)
	}
}

func assertPlacementParityWithRestoreQueries(t *testing.T, backend backendtest.Backend, got []catalog.ChunkPlacementRef) {
	t.Helper()
	rows, err := backend.DB.QueryContext(context.Background(), `
SELECT fc.chunk_order, c.id, c.chunk_hash, c.size, c.chunker_version, c.status,
       COALESCE(b.block_offset,0), COALESCE(b.plaintext_size,c.size),
       COALESCE(b.stored_size,c.size), COALESCE(b.codec,'plain'),
       COALESCE(b.format_version,1), b.nonce, COALESCE(b.container_id,0),
       ctr.filename, ctr.max_size, sb.block_hash, sb.compressed_hash, sb.physical_hash
FROM file_chunk fc
JOIN chunk c ON c.id=fc.chunk_id
LEFT JOIN blocks b ON b.chunk_id=c.id
LEFT JOIN chunk_block_refs r ON r.chunk_id=c.id
LEFT JOIN storage_blocks sb ON sb.id=r.block_id
LEFT JOIN container ctr ON ctr.id=COALESCE(b.container_id,sb.container_id)
WHERE fc.logical_file_id=$1 AND c.status='COMPLETED'
ORDER BY fc.chunk_order`, placementLogicalID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	index := 0
	for rows.Next() {
		var order, chunkID, chunkSize, offset, plaintext, stored, format, containerID, maxSize int64
		var chunkHash, version, status, codec, filename string
		var nonce, blockHash, compressedHash, physicalHash []byte
		if err := rows.Scan(&order, &chunkID, &chunkHash, &chunkSize, &version, &status, &offset, &plaintext, &stored, &codec, &format, &nonce, &containerID, &filename, &maxSize, &blockHash, &compressedHash, &physicalHash); err != nil {
			t.Fatal(err)
		}
		placement := got[index]
		if placement.ChunkOrder != order || placement.ChunkID != chunkID || placement.ChunkHash != chunkHash || placement.ChunkSize != chunkSize || placement.ChunkerVersion != version || placement.ChunkStatus != status {
			t.Fatalf("restore recipe parity row=%d placement=%+v", index, placement)
		}
		if placement.Kind == catalog.PlacementLegacy {
			if placement.Legacy.ContainerOffset != offset || placement.Legacy.PlaintextSize != plaintext || placement.Legacy.StoredSize != stored || placement.Legacy.Codec != codec || int64(placement.Legacy.FormatVersion) != format || placement.Legacy.Container.ID != containerID || placement.Legacy.Container.Filename != filename || placement.Legacy.Container.MaxSize != maxSize || !bytes.Equal(placement.Legacy.Nonce, nonce) {
				t.Fatalf("legacy restore parity: %+v", placement.Legacy)
			}
		} else if !bytes.Equal(placement.Packed.BlockHash, blockHash) || !bytes.Equal(placement.Packed.CompressedHash, compressedHash) || !bytes.Equal(placement.Packed.PhysicalHash, physicalHash) {
			t.Fatalf("packed restore hash parity: %+v", placement.Packed)
		}
		index++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if index != len(got) {
		t.Fatalf("restore parity rows=%d placements=%d", index, len(got))
	}
}
