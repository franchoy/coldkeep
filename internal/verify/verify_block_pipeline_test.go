package verify

import (
	"bytes"
	"context"
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
