package catalog

import "context"

// LoadChunkPlacements implements PlacementCatalog.
//
// Phase 6 implements the strict packed/legacy union defined in types.go.
func (s *Service) LoadChunkPlacements(ctx context.Context, logicalFileID int64) ([]ChunkPlacementRef, error) {
	_, _ = ctx, logicalFileID
	return nil, ErrNotImplemented
}
