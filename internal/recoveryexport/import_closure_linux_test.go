package recoveryexport

import (
	"context"
	"fmt"
	"github.com/franchoy/coldkeep/v2/internal/blocks"
	"os"
	"path/filepath"
	"testing"
)

func TestRecoveryImportFullHistoricalClosure(t *testing.T) {
	f := exportFixture(t)
	execSQL(t, f.db, `INSERT INTO store_repair_attempt(id,logical_file_id,source_file_hash,source_total_size,recipe_fingerprint,status)
 SELECT 1,id,file_hash,total_size,file_hash,'PUBLISHED' FROM logical_file WHERE id=2`)
	p := payloadPath(t, f)
	data, e := os.ReadFile(p)
	must(t, e)
	digest := fmt.Sprintf("%x", blocks.ComputeBlockHash(data))
	execSQL(t, f.db, `INSERT INTO store_repair_container(attempt_id,container_id,physical_size,physical_hash,status) VALUES(1,1,$1,$2,'PUBLISHED')`, len(data), digest)
	execSQL(t, f.db, `INSERT INTO store_repair_block(attempt_id,block_ordinal,format_version,codec,plaintext_size,compression_codec,compression_level,compressed_size,stored_size,container_id,container_offset,block_hash,compression_ratio,payload_hash,compressed_hash,physical_hash)
 SELECT 1,0,format_version,codec,plaintext_size,compression_codec,compression_level,compressed_size,stored_size,container_id,container_offset,block_hash,compression_ratio,payload_hash,compressed_hash,physical_hash FROM storage_blocks WHERE id=1`)
	execSQL(t, f.db, `INSERT INTO store_repair_chunk(attempt_id,chunk_id,chunk_order,block_ordinal,offset_in_block,size_in_block) VALUES(1,1,0,0,0,4)`)
	must(t, os.WriteFile(filepath.Join(f.root, "retired-packed.bin"), data, 0600))
	execSQL(t, f.db, `INSERT INTO container(id,filename,current_size,max_size) SELECT 200,'retired-packed.bin',current_size,max_size FROM container WHERE id=1`)
	execSQL(t, f.db, `INSERT INTO storage_blocks(id,format_version,codec,plaintext_size,compression_codec,compression_level,compressed_size,stored_size,container_id,container_offset,block_hash,compression_ratio,payload_hash,compressed_hash,physical_hash)
 SELECT 200,format_version,codec,plaintext_size,compression_codec,compression_level,compressed_size,stored_size,200,container_offset,block_hash,compression_ratio,payload_hash,compressed_hash,physical_hash FROM storage_blocks WHERE id=1`)
	execSQL(t, f.db, `INSERT INTO retired_chunk_block_ref(block_id,embedded_chunk_id,offset_in_block,size_in_block,repair_attempt_id) VALUES(200,1,0,4,1)`)
	addLegacy(t, f, true)
	legacy, e := os.ReadFile(filepath.Join(f.root, "legacy.bin"))
	must(t, e)
	must(t, os.WriteFile(filepath.Join(f.root, "retired-legacy.bin"), legacy, 0600))
	execSQL(t, f.db, `INSERT INTO container(id,filename,current_size,max_size) SELECT 201,'retired-legacy.bin',current_size,max_size FROM container WHERE id=100`)
	execSQL(t, f.db, `INSERT INTO retired_legacy_block_extent(container_id,block_offset,stored_size,plaintext_size,codec,format_version,nonce,historical_block_id,historical_chunk_id,repair_attempt_id)
 SELECT 201,block_offset,stored_size,plaintext_size,codec,format_version,nonce,199,199,1 FROM blocks WHERE id=100`)

	// Retain a pin-only chunk without inventing a file recipe for it.
	execSQL(t, f.db, "DELETE FROM physical_file WHERE logical_file_id=3; DELETE FROM file_chunk WHERE logical_file_id=3; DELETE FROM logical_file WHERE id=3; UPDATE chunk SET pin_count=1,live_ref_count=0 WHERE id=2")
	execSQL(t, f.db, "INSERT INTO snapshot(id,created_at,type) VALUES('closure-history',NOW(),'full'); INSERT INTO snapshot_path(id,path) VALUES(90,'old/file.txt'); INSERT INTO snapshot_file(snapshot_id,path_id,logical_file_id,size) VALUES('closure-history',90,2,4); DELETE FROM physical_file WHERE logical_file_id=2; UPDATE logical_file SET ref_count=0 WHERE id=2; UPDATE chunk SET live_ref_count=0 WHERE id=1")
	o := exportOptions(t)
	_, e = exportPreserved(t, f, o, nil)
	must(t, e)
	d, open := importTarget(t, f)
	r, e := importPreserved(t, f, d, importOptionsFor(t, o), nil)
	must(t, e)
	if !r.Ready || len(r.Receipt.Objects) != 4 {
		t.Fatal("closure missing", r)
	}
	if metadataSemantics(t, open()) != metadataSemantics(t, f.db) {
		t.Fatal("full historical metadata changed")
	}
	_, e = Check(context.Background(), o.Destination, o.Limits)
	must(t, e)
	t.Log("D9 four whole objects; pin-only chunk; snapshot-only recipe; repair journal; retired packed and legacy extents all admitted unchanged")
}
