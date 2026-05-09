package verify

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	storagecompression "github.com/franchoy/coldkeep/internal/storage/compression"
)

type staticContainerReader struct {
	payload []byte
	err     error
}

func (r staticContainerReader) ReadStoredPayload(_ context.Context, _ BlockStorageMetadata) ([]byte, error) {
	if r.err != nil {
		return nil, r.err
	}
	return append([]byte(nil), r.payload...), nil
}

func buildPipelineEncodedBytes(t *testing.T, payload []byte) []byte {
	t.Helper()
	encoded, err := blocks.EncodeBlock(&blocks.EncodedBlock{
		Header: blocks.BlockHeader{
			Magic:         blocks.BlockMagicV1,
			Version:       blocks.BlockFormatVersionV1,
			Codec:         blocks.BlockCodecNoneV1,
			ChunkCount:    1,
			PlaintextSize: uint64(len(payload)),
		},
		Entries: []blocks.ChunkEntry{{ChunkID: 1, Offset: 0, Size: uint64(len(payload))}},
		Payload: payload,
	})
	if err != nil {
		t.Fatalf("EncodeBlock: %v", err)
	}
	return encoded
}

func TestVerifyStoredBlockUncompressedPasses(t *testing.T) {
	logicalPayload := buildPipelineEncodedBytes(t, []byte("verify-pipeline-uncompressed"))
	meta := BlockStorageMetadata{
		BlockID:          101,
		ContainerID:      11,
		ContainerOffset:  64,
		ContainerName:    "container_001.ck",
		ContainerMaxSize: 1 << 20,
		FormatVersion:    1,
		Codec:            "none",
		PlaintextSize:    int64(len(logicalPayload)),
		StoredSize:       int64(len(logicalPayload)),
		CompressionCodec: "none",
		LogicalHash:      blocks.HashLogical(logicalPayload),
		CompressedHash:   blocks.HashCompressed(logicalPayload),
		PhysicalHash:     blocks.HashPhysical(logicalPayload),
	}

	verified, err := VerifyStoredBlock(context.Background(), meta, staticContainerReader{payload: logicalPayload})
	if err != nil {
		t.Fatalf("VerifyStoredBlock(uncompressed): %v", err)
	}
	if !bytes.Equal(verified.LogicalPayload, logicalPayload) {
		t.Fatalf("logical payload mismatch")
	}
	if verified.DecodedBlock == nil {
		t.Fatalf("decoded block should not be nil")
	}
}

func TestVerifyStoredBlockCompressedZstdPasses(t *testing.T) {
	logicalPayload := buildPipelineEncodedBytes(t, []byte("verify-pipeline-zstd-compressed-payload"))
	zstd, err := storagecompression.NewZstdCompressor(3)
	if err != nil {
		t.Fatalf("NewZstdCompressor: %v", err)
	}
	compressedPayload, err := zstd.Compress(logicalPayload)
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}

	level := 3
	meta := BlockStorageMetadata{
		BlockID:          202,
		ContainerID:      22,
		ContainerOffset:  128,
		ContainerName:    "container_002.ck",
		ContainerMaxSize: 1 << 20,
		FormatVersion:    1,
		Codec:            "none",
		PlaintextSize:    int64(len(logicalPayload)),
		StoredSize:       int64(len(compressedPayload)),
		CompressionCodec: "zstd",
		CompressionLevel: &level,
		LogicalHash:      blocks.HashLogical(logicalPayload),
		CompressedHash:   blocks.HashCompressed(compressedPayload),
		PhysicalHash:     blocks.HashPhysical(compressedPayload),
	}

	verified, err := VerifyStoredBlock(context.Background(), meta, staticContainerReader{payload: compressedPayload})
	if err != nil {
		t.Fatalf("VerifyStoredBlock(zstd): %v", err)
	}
	if !bytes.Equal(verified.LogicalPayload, logicalPayload) {
		t.Fatalf("logical payload mismatch after decompression")
	}
	if verified.DecodedBlock == nil {
		t.Fatalf("decoded block should not be nil")
	}
}

func TestVerifyStoredBlockDecompressionControlledByPerBlockMetadata(t *testing.T) {
	logicalPayload := buildPipelineEncodedBytes(t, []byte("verify-metadata-controls-decompression"))
	zstd, err := storagecompression.NewZstdCompressor(3)
	if err != nil {
		t.Fatalf("NewZstdCompressor: %v", err)
	}
	compressedPayload, err := zstd.Compress(logicalPayload)
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}

	meta := BlockStorageMetadata{
		BlockID:          707,
		ContainerID:      77,
		ContainerOffset:  4096,
		ContainerName:    "container_metadata_control.ck",
		ContainerMaxSize: 1 << 20,
		FormatVersion:    1,
		Codec:            "none",
		PlaintextSize:    int64(len(logicalPayload)),
		StoredSize:       int64(len(compressedPayload)),
		CompressionCodec: "none", // intentional mismatch: payload is compressed
		LogicalHash:      blocks.HashLogical(logicalPayload),
		CompressedHash:   blocks.HashCompressed(compressedPayload),
		PhysicalHash:     blocks.HashPhysical(compressedPayload),
	}

	_, err = VerifyStoredBlock(context.Background(), meta, staticContainerReader{payload: compressedPayload})
	if err == nil {
		t.Fatal("expected verify failure when metadata disables decompression for compressed payload")
	}
	var vf *VerifyFailure
	if !errors.As(err, &vf) {
		t.Fatalf("expected VerifyFailure, got: %v", err)
	}
	if vf.Stage != VerifyStageDecompress {
		t.Fatalf("expected decompress stage failure when metadata disables decompression for compressed payload, got stage=%q err=%v", vf.Stage, err)
	}
	if vf.Category != verifyErrMetadataInvalid {
		t.Fatalf("expected metadata_invalid category when decompression is skipped by metadata, got category=%q err=%v", vf.Category, err)
	}
}

func TestVerifyStoredBlockLegacyMissingPhysicalAndCompressedHashesPasses(t *testing.T) {
	logicalPayload := buildPipelineEncodedBytes(t, []byte("legacy-missing-hashes"))
	meta := BlockStorageMetadata{
		BlockID:          303,
		ContainerID:      33,
		ContainerOffset:  256,
		ContainerName:    "container_legacy.ck",
		ContainerMaxSize: 1 << 20,
		FormatVersion:    1,
		Codec:            "none",
		PlaintextSize:    int64(len(logicalPayload)),
		StoredSize:       int64(len(logicalPayload)),
		CompressionCodec: "none",
		LogicalHash:      blocks.HashLogical(logicalPayload),
		// Legacy row simulation: new hash columns absent.
		CompressedHash: nil,
		PhysicalHash:   nil,
	}

	verified, err := VerifyStoredBlock(context.Background(), meta, staticContainerReader{payload: logicalPayload})
	if err != nil {
		t.Fatalf("VerifyStoredBlock(legacy missing hashes): %v", err)
	}
	if verified.DecodedBlock == nil {
		t.Fatalf("decoded block should not be nil")
	}
}

func TestVerifyStoredBlockLogicalHashRequired(t *testing.T) {
	logicalPayload := buildPipelineEncodedBytes(t, []byte("logical-hash-required"))
	meta := BlockStorageMetadata{
		BlockID:          404,
		ContainerID:      44,
		ContainerOffset:  512,
		ContainerName:    "container_required.ck",
		ContainerMaxSize: 1 << 20,
		FormatVersion:    1,
		Codec:            "none",
		PlaintextSize:    int64(len(logicalPayload)),
		StoredSize:       int64(len(logicalPayload)),
		CompressionCodec: "none",
		LogicalHash:      nil,
		CompressedHash:   nil,
		PhysicalHash:     nil,
	}

	_, err := VerifyStoredBlock(context.Background(), meta, staticContainerReader{payload: logicalPayload})
	if err == nil {
		t.Fatalf("expected VerifyStoredBlock to fail when logical hash is missing")
	}
	if !strings.Contains(err.Error(), verifyErrBlockHashMismatch) {
		t.Fatalf("expected block hash mismatch category, got: %v", err)
	}
}

func TestVerifyStoredBlockOrderingPrefersPhysicalBeforeCompressedAndLogical(t *testing.T) {
	logicalPayload := buildPipelineEncodedBytes(t, []byte("verify-order-physical-first"))
	zstd, err := storagecompression.NewZstdCompressor(3)
	if err != nil {
		t.Fatalf("NewZstdCompressor: %v", err)
	}
	compressedPayload, err := zstd.Compress(logicalPayload)
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}

	level := 3
	meta := BlockStorageMetadata{
		BlockID:          505,
		ContainerID:      55,
		ContainerOffset:  1024,
		ContainerName:    "container_order.ck",
		ContainerMaxSize: 1 << 20,
		FormatVersion:    1,
		Codec:            "none",
		PlaintextSize:    int64(len(logicalPayload)),
		StoredSize:       int64(len(compressedPayload)),
		CompressionCodec: "zstd",
		CompressionLevel: &level,
		LogicalHash:      bytes.Repeat([]byte{0x11}, 32),
		CompressedHash:   bytes.Repeat([]byte{0x22}, 32),
		PhysicalHash:     bytes.Repeat([]byte{0x33}, 32),
	}

	_, err = VerifyStoredBlock(context.Background(), meta, staticContainerReader{payload: compressedPayload})
	if err == nil {
		t.Fatal("expected VerifyStoredBlock to fail")
	}
	var vf *VerifyFailure
	if !errors.As(err, &vf) {
		t.Fatalf("expected VerifyFailure, got: %v", err)
	}
	if vf.Stage != VerifyStagePhysicalPayload {
		t.Fatalf("expected first failing stage %q, got %q", VerifyStagePhysicalPayload, vf.Stage)
	}
	if vf.Category != verifyErrPhysicalHashMismatch {
		t.Fatalf("expected first failing category %q, got %q", verifyErrPhysicalHashMismatch, vf.Category)
	}
}

func TestVerifyStoredBlockOrderingPrefersCompressedBeforeLogical(t *testing.T) {
	logicalPayload := buildPipelineEncodedBytes(t, []byte("verify-order-compressed-first"))
	zstd, err := storagecompression.NewZstdCompressor(3)
	if err != nil {
		t.Fatalf("NewZstdCompressor: %v", err)
	}
	compressedPayload, err := zstd.Compress(logicalPayload)
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}

	level := 3
	meta := BlockStorageMetadata{
		BlockID:          606,
		ContainerID:      66,
		ContainerOffset:  2048,
		ContainerName:    "container_order2.ck",
		ContainerMaxSize: 1 << 20,
		FormatVersion:    1,
		Codec:            "none",
		PlaintextSize:    int64(len(logicalPayload)),
		StoredSize:       int64(len(compressedPayload)),
		CompressionCodec: "zstd",
		CompressionLevel: &level,
		LogicalHash:      bytes.Repeat([]byte{0x44}, 32),
		CompressedHash:   bytes.Repeat([]byte{0x55}, 32),
		PhysicalHash:     blocks.HashPhysical(compressedPayload),
	}

	_, err = VerifyStoredBlock(context.Background(), meta, staticContainerReader{payload: compressedPayload})
	if err == nil {
		t.Fatal("expected VerifyStoredBlock to fail")
	}
	var vf *VerifyFailure
	if !errors.As(err, &vf) {
		t.Fatalf("expected VerifyFailure, got: %v", err)
	}
	if vf.Stage != VerifyStageCompressedHash {
		t.Fatalf("expected first failing stage %q, got %q", VerifyStageCompressedHash, vf.Stage)
	}
	if vf.Category != verifyErrCompressedHashMismatch {
		t.Fatalf("expected first failing category %q, got %q", verifyErrCompressedHashMismatch, vf.Category)
	}
}
