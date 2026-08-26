package engine

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	filestate "github.com/franchoy/coldkeep/internal/status"
	"github.com/franchoy/coldkeep/internal/storage"
	storagecompression "github.com/franchoy/coldkeep/internal/storage/compression"
)

type verifyExecutionFixture struct {
	db           *sql.DB
	engine       *DefaultEngine
	containerDir string
	fileID       int64
	chunkID      int64
	blockID      int64
	containerID  int64
}

func newVerifyExecutionFixture(t *testing.T, compression string) verifyExecutionFixture {
	t.Helper()
	t.Setenv("COLDKEEP_COMPRESSION", compression)
	if compression == storagecompression.CompressionZstd {
		t.Setenv("COLDKEEP_COMPRESSION_LEVEL", "3")
	} else {
		t.Setenv("COLDKEEP_COMPRESSION_LEVEL", "")
	}

	dbconn := newEngineTestDB(t)
	containersDir := t.TempDir()
	writer := container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn)
	storageContext := storage.StorageContext{DB: dbconn, Writer: writer, ContainerDir: containersDir}

	input := filepath.Join(t.TempDir(), "verify-result.txt")
	if err := os.WriteFile(input, []byte("phase-4 execution-derived verification payload"), 0o600); err != nil {
		t.Fatalf("write verify fixture: %v", err)
	}
	stored, err := storage.StoreFileWithStorageContextAndCodecResult(storageContext, input, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("store verify fixture: %v", err)
	}
	if err := writer.FinalizeContainer(); err != nil {
		t.Fatalf("finalize verify fixture: %v", err)
	}

	var chunkID, blockID, containerID int64
	if err := dbconn.QueryRow(`
		SELECT fc.chunk_id, r.block_id, sb.container_id
		FROM file_chunk fc
		JOIN chunk_block_refs r ON r.chunk_id = fc.chunk_id
		JOIN storage_blocks sb ON sb.id = r.block_id
		WHERE fc.logical_file_id = $1
		ORDER BY fc.chunk_order
		LIMIT 1
	`, stored.FileID).Scan(&chunkID, &blockID, &containerID); err != nil {
		t.Fatalf("load verify fixture identities: %v", err)
	}

	eng, err := New(Config{DB: dbconn, ContainerDir: containersDir})
	if err != nil {
		t.Fatalf("new verify engine: %v", err)
	}
	return verifyExecutionFixture{
		db: dbconn, engine: eng, containerDir: containersDir,
		fileID: stored.FileID, chunkID: chunkID, blockID: blockID, containerID: containerID,
	}
}

func requireVerifyResult(t *testing.T, got, want VerifyResult) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("VerifyResult = %+v, want %+v", got, want)
	}
}

func packedNoneVerifyResult() VerifyResult {
	return VerifyResult{
		BlocksChecked: 1, PhysicalHashChecked: 1, CompressedHashChecked: 1,
		LogicalHashChecked: 1, CompressedBlocksChecked: 0,
	}
}

func TestVerifyResultFastCountsOnlyExecutedStages(t *testing.T) {
	fixture := newVerifyExecutionFixture(t, storagecompression.CompressionNone)

	got, err := fixture.engine.Verify(context.Background(), VerifyRequest{Level: "fast", Target: "system"})
	if err != nil {
		t.Fatalf("fast system verify: %v", err)
	}
	requireVerifyResult(t, got, packedNoneVerifyResult())

	got, err = fixture.engine.Verify(context.Background(), VerifyRequest{Level: "fast", Target: "file", FileID: int(fixture.fileID)})
	if err != nil {
		t.Fatalf("fast file verify: %v", err)
	}
	requireVerifyResult(t, got, VerifyResult{})
}

func TestVerifyResultStandardCountsOnlyExecutedStages(t *testing.T) {
	fixture := newVerifyExecutionFixture(t, storagecompression.CompressionNone)

	got, err := fixture.engine.Verify(context.Background(), VerifyRequest{Level: "standard", Target: "system"})
	if err != nil {
		t.Fatalf("standard system verify: %v", err)
	}
	requireVerifyResult(t, got, packedNoneVerifyResult())

	got, err = fixture.engine.Verify(context.Background(), VerifyRequest{Level: "standard", Target: "file", FileID: int(fixture.fileID)})
	if err != nil {
		t.Fatalf("standard file verify: %v", err)
	}
	requireVerifyResult(t, got, VerifyResult{})
}

func TestVerifyResultFullCountsOnlyExecutedStages(t *testing.T) {
	fixture := newVerifyExecutionFixture(t, storagecompression.CompressionNone)

	got, err := fixture.engine.Verify(context.Background(), VerifyRequest{Level: "full", Target: "system"})
	if err != nil {
		t.Fatalf("full system verify: %v", err)
	}
	requireVerifyResult(t, got, packedNoneVerifyResult())

	got, err = fixture.engine.Verify(context.Background(), VerifyRequest{Level: "full", Target: "file", FileID: int(fixture.fileID)})
	if err != nil {
		t.Fatalf("full file verify: %v", err)
	}
	requireVerifyResult(t, got, VerifyResult{})
}

func TestVerifyResultDeepCountsExecutedPayloadChecks(t *testing.T) {
	fixture := newVerifyExecutionFixture(t, storagecompression.CompressionNone)
	for _, target := range []string{"system", "file"} {
		request := VerifyRequest{Level: "deep", Target: target}
		if target == "file" {
			request.FileID = int(fixture.fileID)
		}
		got, err := fixture.engine.Verify(context.Background(), request)
		if err != nil {
			t.Fatalf("deep %s verify: %v", target, err)
		}
		requireVerifyResult(t, got, packedNoneVerifyResult())
	}
}

func TestVerifyResultDoesNotCountQuarantinedOrSkippedObjects(t *testing.T) {
	fixture := newVerifyExecutionFixture(t, storagecompression.CompressionNone)
	if _, err := fixture.db.Exec(`UPDATE container SET quarantine = TRUE WHERE id = $1`, fixture.containerID); err != nil {
		t.Fatalf("quarantine fixture container: %v", err)
	}
	got, err := fixture.engine.Verify(context.Background(), VerifyRequest{Level: "fast", Target: "system"})
	if err != nil {
		t.Fatalf("fast system verify with skipped quarantine: %v", err)
	}
	requireVerifyResult(t, got, VerifyResult{})
}

func TestVerifyResultDeduplicatesRepeatedBlockReferences(t *testing.T) {
	fixture := newVerifyExecutionFixture(t, storagecompression.CompressionNone)
	if _, err := fixture.db.Exec(`INSERT INTO file_chunk (logical_file_id, chunk_id, chunk_order) VALUES ($1, $2, 1)`, fixture.fileID, fixture.chunkID); err != nil {
		t.Fatalf("insert repeated file chunk reference: %v", err)
	}
	if _, err := fixture.db.Exec(`UPDATE chunk SET live_ref_count = live_ref_count + 1 WHERE id = $1`, fixture.chunkID); err != nil {
		t.Fatalf("update repeated chunk ref count: %v", err)
	}
	got, err := fixture.engine.Verify(context.Background(), VerifyRequest{Level: "deep", Target: "file", FileID: int(fixture.fileID)})
	if err != nil {
		t.Fatalf("deep file verify with repeated block reference: %v", err)
	}
	requireVerifyResult(t, got, packedNoneVerifyResult())
}

func TestVerifyResultMixedRepositoryAccounting(t *testing.T) {
	fixture := newVerifyExecutionFixture(t, storagecompression.CompressionNone)
	_ = seedEngineVerifyLegacyBlock(t, fixture.db, fixture.containerDir, []byte("phase-4 legacy payload"))

	got, err := fixture.engine.Verify(context.Background(), VerifyRequest{Level: "standard", Target: "system"})
	if err != nil {
		t.Fatalf("standard mixed system verify: %v", err)
	}
	requireVerifyResult(t, got, VerifyResult{
		BlocksChecked: 2, PhysicalHashChecked: 1, CompressedHashChecked: 1,
		LogicalHashChecked: 2, CompressedBlocksChecked: 0,
	})
}

func TestVerifyResultLegacyOnlyRepositoryAccounting(t *testing.T) {
	dbconn := newEngineTestDB(t)
	containersDir := t.TempDir()
	_ = seedEngineVerifyLegacyBlock(t, dbconn, containersDir, []byte("phase-4 legacy-only payload"))
	eng, err := New(Config{DB: dbconn, ContainerDir: containersDir})
	if err != nil {
		t.Fatalf("new legacy-only verify engine: %v", err)
	}

	got, err := eng.Verify(context.Background(), VerifyRequest{Level: "standard", Target: "system"})
	if err != nil {
		t.Fatalf("standard legacy-only system verify: %v", err)
	}
	requireVerifyResult(t, got, VerifyResult{BlocksChecked: 1, LogicalHashChecked: 1})
}

func TestVerifyResultCountsCompressedPayloadOnlyAfterSuccessfulDecompression(t *testing.T) {
	fixture := newVerifyExecutionFixture(t, storagecompression.CompressionZstd)
	got, err := fixture.engine.Verify(context.Background(), VerifyRequest{Level: "standard", Target: "system"})
	if err != nil {
		t.Fatalf("standard compressed system verify: %v", err)
	}
	requireVerifyResult(t, got, VerifyResult{
		BlocksChecked: 1, PhysicalHashChecked: 1, CompressedHashChecked: 1,
		LogicalHashChecked: 1, CompressedBlocksChecked: 1,
	})
}

func TestVerifyFailureReturnsZeroPublicResult(t *testing.T) {
	fixture := newVerifyExecutionFixture(t, storagecompression.CompressionNone)
	badHash := make([]byte, sha256.Size)
	if _, err := fixture.db.Exec(`UPDATE storage_blocks SET block_hash = $1 WHERE id = $2`, badHash, fixture.blockID); err != nil {
		t.Fatalf("corrupt logical block hash: %v", err)
	}
	got, err := fixture.engine.Verify(context.Background(), VerifyRequest{Level: "standard", Target: "system"})
	if err == nil {
		t.Fatal("verification unexpectedly succeeded")
	}
	requireVerifyResult(t, got, VerifyResult{})
}

func seedEngineVerifyLegacyBlock(t *testing.T, dbconn *sql.DB, containersDir string, payload []byte) int64 {
	t.Helper()
	sum := sha256.Sum256(payload)
	hash := hex.EncodeToString(sum[:])
	var chunkID int64
	if err := dbconn.QueryRow(`
		INSERT INTO chunk (chunk_hash, size, status, live_ref_count, pin_count, retry_count, chunker_version)
		VALUES ($1, $2, $3, 0, 0, 0, 'v1-simple-rolling') RETURNING id
	`, hash, int64(len(payload)), filestate.ChunkCompleted).Scan(&chunkID); err != nil {
		t.Fatalf("insert legacy chunk: %v", err)
	}
	transformer, err := blocks.GetBlockTransformer(blocks.CodecPlain)
	if err != nil {
		t.Fatalf("get legacy transformer: %v", err)
	}
	encoded, err := transformer.Encode(context.Background(), blocks.EncodeInput{ChunkID: chunkID, ChunkHash: hash, Plaintext: payload})
	if err != nil {
		t.Fatalf("encode legacy payload: %v", err)
	}
	writer := container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn)
	tx, err := dbconn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin legacy transaction: %v", err)
	}
	placement, err := writer.AppendPayload(tx, encoded.Payload)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("append legacy payload: %v", err)
	}
	if err := container.UpdateContainerSize(tx, placement.ContainerID, placement.NewContainerSize); err != nil {
		_ = tx.Rollback()
		t.Fatalf("update legacy container size: %v", err)
	}
	var blockID int64
	if err := tx.QueryRow(`
		INSERT INTO blocks (chunk_id, codec, format_version, plaintext_size, stored_size, nonce, container_id, block_offset)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id
	`, chunkID, string(encoded.Descriptor.Codec), encoded.Descriptor.FormatVersion,
		encoded.Descriptor.PlaintextSize, encoded.Descriptor.StoredSize, encoded.Descriptor.Nonce,
		placement.ContainerID, placement.Offset).Scan(&blockID); err != nil {
		_ = tx.Rollback()
		t.Fatalf("insert legacy block: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit legacy block: %v", err)
	}
	if err := writer.FinalizeContainer(); err != nil {
		t.Fatalf("finalize legacy container: %v", err)
	}
	return blockID
}
