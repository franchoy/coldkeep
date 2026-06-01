package catalog

import "context"

// LoadSnapshotGraph implements SnapshotGraphCatalog.
//
// Deferred to Phase 5/6 (snapshot/GC migration). The snapshot graph traversal
// depends on snapshot reachability semantics being fully understood and tested
// before it is expressed through a catalog contract. Callers must not rely on
// this method until it is implemented.
func (s *Service) LoadSnapshotGraph(ctx context.Context) (*SnapshotGraph, error) {
	_ = ctx
	return nil, ErrNotImplemented
}
