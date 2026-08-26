package verify

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/catalog"
	"github.com/franchoy/coldkeep/internal/container"
)

type filePlacementVerifyState struct {
	containersDir string
	packedBlocks  map[int64]*VerifiedBlock
}

func (s *filePlacementVerifyState) verify(ctx context.Context, placement catalog.ChunkPlacementRef) error {
	switch placement.Kind {
	case catalog.PlacementLegacy:
		return s.verifyLegacy(ctx, placement)
	case catalog.PlacementPacked:
		return s.verifyPacked(ctx, placement)
	default:
		return fmt.Errorf("unsupported placement kind %q", placement.Kind)
	}
}

func (s *filePlacementVerifyState) verifyLegacy(ctx context.Context, placement catalog.ChunkPlacementRef) error {
	legacy := placement.Legacy
	if legacy == nil {
		return fmt.Errorf("legacy placement metadata is missing")
	}
	fullPath, err := container.SafeContainerPath(s.containersDir, legacy.Container.Filename)
	if err != nil {
		return fmt.Errorf("invalid container filename %q: %w", legacy.Container.Filename, err)
	}
	fc, err := container.OpenReadOnlyContainer(fullPath, legacy.Container.MaxSize)
	if err != nil {
		return fmt.Errorf("open container %q: %w", fullPath, err)
	}
	defer func() { _ = fc.Close() }()
	payload, err := container.ReadPayloadAt(fc, legacy.ContainerOffset, legacy.StoredSize)
	if err != nil {
		return fmt.Errorf("read legacy block %d: %w", legacy.BlockID, err)
	}
	codec := blocks.Codec(legacy.Codec)
	transformer, err := blocks.GetBlockTransformer(codec)
	if err != nil {
		return fmt.Errorf("get transformer for codec %q: %w", legacy.Codec, err)
	}
	plaintext, err := transformer.Decode(ctx, blocks.DecodeInput{
		ChunkHash: placement.ChunkHash,
		Descriptor: blocks.Descriptor{
			ChunkID:       placement.ChunkID,
			Codec:         codec,
			FormatVersion: legacy.FormatVersion,
			PlaintextSize: legacy.PlaintextSize,
			StoredSize:    legacy.StoredSize,
			Nonce:         legacy.Nonce,
			ContainerID:   legacy.Container.ID,
			BlockOffset:   legacy.ContainerOffset,
		},
		Payload: payload,
	})
	if err != nil {
		return fmt.Errorf("decode legacy block %d: %w", legacy.BlockID, err)
	}
	if err := verifyPlacementChunkBytes(placement, plaintext); err != nil {
		return err
	}
	observeLegacyVerificationStage(ctx, legacy.BlockID, verificationObservedLogicalHash)
	observeLegacyVerificationStage(ctx, legacy.BlockID, verificationObservedBlockComplete)
	return nil
}

func (s *filePlacementVerifyState) verifyPacked(ctx context.Context, placement catalog.ChunkPlacementRef) error {
	packed := placement.Packed
	if packed == nil {
		return fmt.Errorf("packed placement metadata is missing")
	}
	verified, err := s.loadPackedBlock(ctx, packed)
	if err != nil {
		return err
	}
	match, err := findPackedChunkEntry(verified, packed.BlockID, placement.ChunkID)
	if err != nil {
		return err
	}
	if err := validatePackedChunkRange(*match, *packed, placement.ChunkID); err != nil {
		return err
	}
	chunkBytes, err := blocks.SliceChunkFromPayload(verified.DecodedBlock.Payload, *match)
	if err != nil {
		return fmt.Errorf("slice packed block %d chunk %d: %w", packed.BlockID, placement.ChunkID, err)
	}
	return verifyPlacementChunkBytes(placement, chunkBytes)
}

func (s *filePlacementVerifyState) loadPackedBlock(ctx context.Context, packed *catalog.PackedChunkPlacement) (*VerifiedBlock, error) {
	verified := s.packedBlocks[packed.BlockID]
	if verified == nil {
		var err error
		verified, err = VerifyStoredBlock(ctx, BlockStorageMetadata{
			BlockID: packed.BlockID, ContainerID: packed.Container.ID,
			ContainerOffset: packed.ContainerOffset, ContainerName: packed.Container.Filename,
			ContainerMaxSize: packed.Container.MaxSize, FormatVersion: int64(packed.FormatVersion),
			Codec: packed.Codec, PlaintextSize: packed.PlaintextSize,
			CompressedSize: packed.CompressedSize, StoredSize: packed.StoredSize,
			CompressionCodec: packed.CompressionCodec, CompressionLevel: packed.CompressionLevel,
			LogicalHash: packed.BlockHash, CompressedHash: packed.CompressedHash,
			PhysicalHash: packed.PhysicalHash,
		}, FilesystemContainerReader{ContainersDir: s.containersDir})
		if err != nil {
			return nil, fmt.Errorf("verify packed block %d: %w", packed.BlockID, err)
		}
		s.packedBlocks[packed.BlockID] = verified
	}
	if verified.DecodedBlock == nil {
		return nil, fmt.Errorf("packed block %d decoded block is nil", packed.BlockID)
	}
	return verified, nil
}

func findPackedChunkEntry(verified *VerifiedBlock, blockID, chunkID int64) (*blocks.ChunkEntry, error) {
	var match *blocks.ChunkEntry
	for i := range verified.DecodedBlock.Entries {
		entry := &verified.DecodedBlock.Entries[i]
		if entry.ChunkID != uint64(chunkID) {
			continue
		}
		if match != nil {
			return nil, fmt.Errorf("packed block %d contains duplicate table entries for chunk %d", blockID, chunkID)
		}
		match = entry
	}
	if match == nil {
		return nil, fmt.Errorf("packed block %d has no table entry for chunk %d", blockID, chunkID)
	}
	return match, nil
}

func validatePackedChunkRange(entry blocks.ChunkEntry, packed catalog.PackedChunkPlacement, chunkID int64) error {
	if entry.Offset != uint64(packed.OffsetInBlock) || entry.Size != uint64(packed.SizeInBlock) {
		return fmt.Errorf("packed block %d chunk %d table/range mismatch table=(%d,%d) placement=(%d,%d)", packed.BlockID, chunkID, entry.Offset, entry.Size, packed.OffsetInBlock, packed.SizeInBlock)
	}
	return nil
}

func verifyPlacementChunkBytes(placement catalog.ChunkPlacementRef, payload []byte) error {
	if int64(len(payload)) != placement.ChunkSize {
		return fmt.Errorf("chunk %d size mismatch expected=%d got=%d", placement.ChunkID, placement.ChunkSize, len(payload))
	}
	sum := sha256.Sum256(payload)
	computed := hex.EncodeToString(sum[:])
	if computed != placement.ChunkHash {
		return fmt.Errorf("chunk %d corrupted: expected %s got %s", placement.ChunkID, placement.ChunkHash, computed)
	}
	return nil
}
