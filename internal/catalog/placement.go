package catalog

import "context"

// LoadChunkPlacements implements PlacementCatalog.
//
// Deferred to Phase 7/8 (restore/store migration). Placement metadata must
// unify packed (storage_blocks / chunk_block_refs) and legacy (blocks) roots
// without privileging either representation. That duality is safely expressible
// only after the restore and store migrations are underway. Callers must not
// rely on this method until it is implemented.
func (s *Service) LoadChunkPlacements(ctx context.Context, logicalFileID int64) ([]ChunkPlacementRef, error) {
	_, _ = ctx, logicalFileID
	return nil, ErrNotImplemented
}
