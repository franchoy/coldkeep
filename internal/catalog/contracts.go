package catalog

import (
	"fmt"
	"sort"
	"strings"
)

func ValidateRestorePlanInput(input RestorePlanInput) error {
	valid := false
	switch input.Selector {
	case RestoreByFileID:
		valid = input.FileID > 0 && input.StoredPath == "" && input.SnapshotID == "" && input.SnapshotPath == ""
	case RestoreByStoredPath:
		valid = input.FileID == 0 && input.StoredPath != "" && input.SnapshotID == "" && input.SnapshotPath == ""
	case RestoreBySnapshotPath:
		valid = input.FileID == 0 && input.StoredPath == "" && input.SnapshotID != "" && input.SnapshotPath != ""
	}
	if valid {
		return nil
	}
	return NewError(ErrorInvalidArgument, "load restore plan", "exactly_one_restore_selector", "restore selector must contain exactly one valid file_id, stored_path, or snapshot_path target", nil)
}

// NormalizeGCPlanInput validates, deduplicates, and sorts IDs without mutating input.
func NormalizeGCPlanInput(input GCPlanInput) (GCPlanInput, error) {
	seen := make(map[string]struct{}, len(input.ExcludeSnapshotIDs))
	for _, id := range input.ExcludeSnapshotIDs {
		if strings.TrimSpace(id) == "" {
			return GCPlanInput{}, NewError(ErrorInvalidArgument, "load GC plan", "nonempty_snapshot_id", "excluded snapshot ID must not be empty", nil)
		}
		seen[id] = struct{}{}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return GCPlanInput{ExcludeSnapshotIDs: ids}, nil
}

// ValidateChunkPlacement enforces the tagged union and shared range invariants.
func ValidateChunkPlacement(placement ChunkPlacementRef) error {
	if placement.ChunkOrder < 0 || placement.ChunkID <= 0 || placement.ChunkHash == "" || placement.ChunkSize <= 0 || placement.ChunkStatus == "" {
		return invalidPlacement(placement, "chunk identity and order must be complete")
	}
	if strings.TrimSpace(placement.ChunkerVersion) == "" {
		return invalidPlacement(placement, "chunk has empty chunker_version (repository corruption or incomplete migration)")
	}
	switch placement.Kind {
	case PlacementLegacy:
		if placement.Legacy == nil || placement.Packed != nil {
			return invalidPlacement(placement, "legacy placement must contain only legacy metadata")
		}
		if placement.Legacy.BlockID <= 0 || placement.Legacy.Codec == "" || placement.Legacy.FormatVersion <= 0 || placement.Legacy.PlaintextSize <= 0 || placement.Legacy.StoredSize <= 0 || !validContainerRead(placement.Legacy.Container, placement.Legacy.ContainerOffset, placement.Legacy.StoredSize) {
			return invalidPlacement(placement, "legacy block and container bounds must be valid")
		}
		if !validLegacyCodec(placement.Legacy.Codec, placement.Legacy.Nonce) {
			return invalidPlacement(placement, "legacy codec and nonce must be valid")
		}
	case PlacementPacked:
		if placement.Packed == nil || placement.Legacy != nil {
			return invalidPlacement(placement, "packed placement must contain only packed metadata")
		}
		if placement.Packed.BlockID <= 0 || placement.Packed.Codec == "" || placement.Packed.FormatVersion <= 0 || placement.Packed.PlaintextSize <= 0 || placement.Packed.CompressionCodec == "" || placement.Packed.StoredSize <= 0 || placement.Packed.OffsetInBlock < 0 || placement.Packed.SizeInBlock <= 0 || !validContainerRead(placement.Packed.Container, placement.Packed.ContainerOffset, placement.Packed.StoredSize) {
			return invalidPlacement(placement, "packed block, segment, and container metadata must be valid")
		}
		if !validPackedTransforms(placement.Packed) {
			return invalidPlacement(placement, "packed codec and compression metadata must be valid")
		}
	default:
		return invalidPlacement(placement, "placement kind must be legacy or packed")
	}
	return nil
}

func validLegacyCodec(codec string, nonce []byte) bool {
	switch codec {
	case "plain":
		return len(nonce) == 0
	case "aes-gcm":
		return len(nonce) == 12
	default:
		return false
	}
}

func validPackedTransforms(placement *PackedChunkPlacement) bool {
	if placement == nil || (placement.Codec != "none" && placement.Codec != "aes-gcm") {
		return false
	}
	switch placement.CompressionCodec {
	case "none":
		return placement.CompressionLevel == nil && (placement.CompressedSize == nil || *placement.CompressedSize > 0)
	case "zstd":
		return placement.CompressionLevel != nil && *placement.CompressionLevel >= 1 && *placement.CompressionLevel <= 9 && placement.CompressedSize != nil && *placement.CompressedSize > 0
	default:
		return false
	}
}

func validContainerRead(container ContainerPlacementRef, offset, size int64) bool {
	if container.ID <= 0 || container.Filename == "" || offset < 0 || size <= 0 || container.CurrentSize < 0 || container.MaxSize <= 0 {
		return false
	}
	return true
}

func invalidPlacement(placement ChunkPlacementRef, message string) error {
	return NewError(ErrorInvariantViolation, "load chunk placements", "exactly_one_valid_placement_per_chunk", fmt.Sprintf("chunk %d: %s", placement.ChunkID, message), nil)
}
