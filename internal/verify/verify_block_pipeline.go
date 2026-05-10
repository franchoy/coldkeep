package verify

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	storagecompression "github.com/franchoy/coldkeep/internal/storage/compression"
)

// BlockStorageMetadata carries the persisted block metadata required to verify
// and decode a packed storage block from container bytes.
type BlockStorageMetadata struct {
	BlockID          int64
	ContainerID      int64
	ContainerOffset  int64
	ContainerName    string
	ContainerMaxSize int64

	FormatVersion    int64
	Codec            string
	PlaintextSize    int64
	CompressedSize   *int64
	StoredSize       int64
	CompressionCodec string
	CompressionLevel *int

	LogicalHash    []byte
	CompressedHash []byte
	PhysicalHash   []byte
}

// ContainerReader reads the persisted payload bytes for a block from storage.
type ContainerReader interface {
	ReadStoredPayload(ctx context.Context, meta BlockStorageMetadata) ([]byte, error)
}

// FilesystemContainerReader loads container payload bytes from the local
// container directory.
type FilesystemContainerReader struct {
	ContainersDir string
}

// ReadStoredPayload reads the exact stored payload bytes for one packed block.
func (r FilesystemContainerReader) ReadStoredPayload(_ context.Context, meta BlockStorageMetadata) ([]byte, error) {
	path := filepath.Join(r.ContainersDir, meta.ContainerName)
	fc, err := container.OpenReadOnlyContainer(path, meta.ContainerMaxSize)
	if err != nil {
		return nil, err
	}
	defer func() { _ = fc.Close() }()

	return container.ReadPayloadAt(fc, meta.ContainerOffset, meta.StoredSize)
}

// VerifiedBlock is the successful output of the staged block verification
// pipeline.
type VerifiedBlock struct {
	LogicalPayload []byte
	DecodedBlock   *blocks.EncodedBlock
	Metadata       BlockStorageMetadata
}

const (
	verifyBlockMinCompressionLevel = 1
	verifyBlockMaxCompressionLevel = 9
)

// VerifyStoredBlock executes the packed-block verification pipeline in stage
// order and returns the decoded logical block on success.
func VerifyStoredBlock(ctx context.Context, meta BlockStorageMetadata, reader ContainerReader) (*VerifiedBlock, error) {
	if reader == nil {
		metaErr := verifyBlockFailureMeta(VerifyStagePhysicalPayload, meta.BlockID, meta.ContainerID, meta.ContainerOffset)
		return nil, verifyStageError(verifyErrPhysicalMissing, metaErr, "verifyBlockPayloads: nil container reader", nil)
	}

	// 1) Read physical payload.
	storedBytes, err := reader.ReadStoredPayload(ctx, meta)
	if err != nil {
		metaErr := verifyBlockFailureMeta(VerifyStagePhysicalPayload, meta.BlockID, meta.ContainerID, meta.ContainerOffset)
		return nil, verifyStageError(verifyErrPhysicalMissing, metaErr, "verifyBlockPayloads: read stored payload", err)
	}

	loc := verifyBlockLocation{blockID: meta.BlockID, containerID: meta.ContainerID, offset: meta.ContainerOffset}

	// 2) Verify physical_hash if present.
	payloads := blockStagePayloads{
		storedBytes: storedBytes,
		hashes: blocks.BlockHashes{
			LogicalHash:    meta.LogicalHash,
			CompressedHash: meta.CompressedHash,
			PhysicalHash:   meta.PhysicalHash,
		},
	}
	if err := verifyPhysicalPayloadStage(ctx, loc, payloads); err != nil {
		return nil, err
	}

	// 3) Decrypt if needed.
	codec, err := resolveVerifyStorageBlockCodec(meta.Codec)
	if err != nil {
		metaErr := verifyBlockFailureMeta(VerifyStageDecrypt, meta.BlockID, meta.ContainerID, meta.ContainerOffset)
		return nil, verifyStageError(verifyErrMetadataInvalid, metaErr, "verifyBlockPayloads: invalid storage codec metadata", err)
	}

	transformer, err := blocks.GetBlockTransformer(codec)
	if err != nil {
		metaErr := verifyBlockFailureMeta(VerifyStageDecrypt, meta.BlockID, meta.ContainerID, meta.ContainerOffset)
		return nil, verifyStageError(verifyErrMetadataInvalid, metaErr, "verifyBlockPayloads: load transformer", err)
	}

	decodePayload := storedBytes
	var descriptorNonce []byte
	if codec == blocks.CodecAESGCM {
		if len(storedBytes) <= packedStorageBlockAESGCMNonceSize {
			metaErr := verifyBlockFailureMeta(VerifyStageDecrypt, meta.BlockID, meta.ContainerID, meta.ContainerOffset)
			return nil, verifyStageError(verifyErrMetadataInvalid, metaErr, "verifyBlockPayloads: aes-gcm payload too small for nonce prefix", nil)
		}
		descriptorNonce = append([]byte(nil), storedBytes[:packedStorageBlockAESGCMNonceSize]...)
		decodePayload = storedBytes[packedStorageBlockAESGCMNonceSize:]
	}

	preDecompressionPayload, err := transformer.Decode(ctx, blocks.DecodeInput{
		Descriptor: blocks.Descriptor{
			ChunkID:       0,
			Codec:         codec,
			FormatVersion: int(meta.FormatVersion),
			PlaintextSize: meta.PlaintextSize,
			StoredSize:    meta.StoredSize,
			Nonce:         descriptorNonce,
			ContainerID:   0,
			BlockOffset:   meta.ContainerOffset,
		},
		Payload: decodePayload,
	})
	if err != nil {
		metaErr := verifyBlockFailureMeta(VerifyStageDecrypt, meta.BlockID, meta.ContainerID, meta.ContainerOffset)
		return nil, verifyStageError(verifyErrMetadataInvalid, metaErr, "verifyBlockPayloads: decrypt/transform decode failed", err)
	}

	// 4) Verify compressed_hash if present.
	payloads.compressedBytes = preDecompressionPayload
	if err := verifyCompressedPayloadStage(ctx, loc, payloads); err != nil {
		return nil, err
	}

	if meta.CompressedSize != nil && *meta.CompressedSize > 0 && int64(len(preDecompressionPayload)) != *meta.CompressedSize {
		metaErr := verifyBlockFailureMeta(VerifyStageDecompress, meta.BlockID, meta.ContainerID, meta.ContainerOffset)
		return nil, verifyStageError(verifyErrMetadataInvalid, metaErr, fmt.Sprintf("verifyBlockPayloads: compressed size mismatch metadata=%d decoded=%d", *meta.CompressedSize, len(preDecompressionPayload)), nil)
	}

	// 5) Decompress if needed.
	compressionCodec := strings.TrimSpace(strings.ToLower(meta.CompressionCodec))
	if compressionCodec == "" {
		compressionCodec = storagecompression.CompressionNone
	}
	if err := validateVerifyBlockCompressionMetadata(compressionCodec, meta.CompressionLevel); err != nil {
		metaErr := verifyBlockFailureMeta(VerifyStageDecompress, meta.BlockID, meta.ContainerID, meta.ContainerOffset)
		return nil, verifyStageError(verifyErrMetadataInvalid, metaErr, "verifyBlockPayloads: invalid compression metadata contract", err)
	}

	var compressor storagecompression.Compressor
	if compressionCodec == storagecompression.CompressionZstd {
		level := *meta.CompressionLevel
		comp, err := storagecompression.NewZstdCompressor(level)
		if err != nil {
			metaErr := verifyBlockFailureMeta(VerifyStageDecompress, meta.BlockID, meta.ContainerID, meta.ContainerOffset)
			return nil, verifyStageError(verifyErrMetadataInvalid, metaErr, fmt.Sprintf("verifyBlockPayloads: initialize compression codec=%s level=%d", compressionCodec, level), err)
		}
		compressor = comp
	} else {
		comp, err := storagecompression.Lookup(compressionCodec)
		if err != nil {
			metaErr := verifyBlockFailureMeta(VerifyStageDecompress, meta.BlockID, meta.ContainerID, meta.ContainerOffset)
			return nil, verifyStageError(verifyErrMetadataInvalid, metaErr, fmt.Sprintf("verifyBlockPayloads: resolve compression codec=%s", compressionCodec), err)
		}
		compressor = comp
	}

	logicalPayload, err := compressor.Decompress(preDecompressionPayload, meta.PlaintextSize)
	if err != nil {
		metaErr := verifyBlockFailureMeta(VerifyStageDecompress, meta.BlockID, meta.ContainerID, meta.ContainerOffset)
		return nil, verifyStageError(verifyErrMetadataInvalid, metaErr, fmt.Sprintf("verifyBlockPayloads: decompress codec=%s", compressionCodec), err)
	}
	if int64(len(logicalPayload)) != meta.PlaintextSize {
		metaErr := verifyBlockFailureMeta(VerifyStageDecompress, meta.BlockID, meta.ContainerID, meta.ContainerOffset)
		return nil, verifyStageError(verifyErrMetadataInvalid, metaErr, fmt.Sprintf("verifyBlockPayloads: plaintext size mismatch metadata=%d decoded=%d", meta.PlaintextSize, len(logicalPayload)), nil)
	}

	// 6) Verify logical block hash.
	payloads.plaintextEncoded = logicalPayload
	if err := verifyLogicalPayloadStage(ctx, loc, meta.LogicalHash, payloads); err != nil {
		return nil, err
	}

	// 7) Decode logical block.
	decoded, err := blocks.DecodeBlock(logicalPayload)
	if err != nil {
		metaErr := verifyBlockFailureMeta(VerifyStageBlockDecode, meta.BlockID, meta.ContainerID, meta.ContainerOffset)
		return nil, verifyStageError(verifyErrUnsupportedBlock, metaErr, "verifyBlockPayloads: decode logical block", err)
	}

	// 8) Validate chunk layout/counts.
	if err := validateDecodedChunkLayout(decoded); err != nil {
		metaErr := verifyBlockFailureMeta(VerifyStageChunkRefs, meta.BlockID, meta.ContainerID, meta.ContainerOffset)
		return nil, verifyStageError(verifyErrUnsupportedBlock, metaErr, "verifyBlockPayloads: decoded chunk layout invalid", err)
	}

	return &VerifiedBlock{LogicalPayload: logicalPayload, DecodedBlock: decoded, Metadata: meta}, nil
}

func validateDecodedChunkLayout(decoded *blocks.EncodedBlock) error {
	if decoded == nil {
		return fmt.Errorf("decoded block is nil")
	}
	if decoded.Header.ChunkCount == 0 {
		return fmt.Errorf("decoded chunk count must be > 0")
	}
	if int(decoded.Header.ChunkCount) != len(decoded.Entries) {
		return fmt.Errorf("decoded chunk count mismatch header=%d entries=%d", decoded.Header.ChunkCount, len(decoded.Entries))
	}
	if decoded.Header.PlaintextSize != uint64(len(decoded.Payload)) {
		return fmt.Errorf("decoded payload size mismatch header=%d payload=%d", decoded.Header.PlaintextSize, len(decoded.Payload))
	}

	expectedOffset := uint64(0)
	for i, entry := range decoded.Entries {
		if entry.Size == 0 {
			return fmt.Errorf("chunk entry index=%d has zero size", i)
		}
		if entry.Offset != expectedOffset {
			return fmt.Errorf("chunk entry index=%d offset mismatch expected=%d got=%d", i, expectedOffset, entry.Offset)
		}
		chunkSlice, err := blocks.SliceChunkFromPayload(decoded.Payload, entry)
		if err != nil {
			return fmt.Errorf("chunk entry index=%d out of bounds: %w", i, err)
		}
		if len(chunkSlice) != int(entry.Size) {
			return fmt.Errorf("chunk entry index=%d size mismatch entry=%d actual=%d", i, entry.Size, len(chunkSlice))
		}
		expectedOffset += entry.Size
	}
	if expectedOffset != uint64(len(decoded.Payload)) {
		return fmt.Errorf("chunk entries do not fully cover payload expected=%d payload=%d", expectedOffset, len(decoded.Payload))
	}

	return nil
}

func validateVerifyBlockCompressionMetadata(codec string, level *int) error {
	switch codec {
	case storagecompression.CompressionNone:
		if level != nil {
			return fmt.Errorf("compression_codec=%s requires compression_level=NULL", storagecompression.CompressionNone)
		}
		return nil
	case storagecompression.CompressionZstd:
		if level == nil {
			return fmt.Errorf("compression_codec=%s requires compression_level in [%d,%d]", storagecompression.CompressionZstd, verifyBlockMinCompressionLevel, verifyBlockMaxCompressionLevel)
		}
		if *level < verifyBlockMinCompressionLevel || *level > verifyBlockMaxCompressionLevel {
			return fmt.Errorf("compression_codec=%s has invalid compression_level=%d (expected [%d,%d])", storagecompression.CompressionZstd, *level, verifyBlockMinCompressionLevel, verifyBlockMaxCompressionLevel)
		}
		return nil
	default:
		return fmt.Errorf("unsupported compression_codec=%q (expected %q or %q)", codec, storagecompression.CompressionNone, storagecompression.CompressionZstd)
	}
}
