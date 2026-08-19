package catalog

import "context"

// LoadSnapshotGraph implements SnapshotGraphCatalog.
//
// Phase 5 implements the backend-neutral graph contract defined in types.go.
func (s *Service) LoadSnapshotGraph(ctx context.Context) (*SnapshotGraph, error) {
	_ = ctx
	return nil, ErrNotImplemented
}
