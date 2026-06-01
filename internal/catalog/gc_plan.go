package catalog

import "context"

// LoadGCPlanMetadata implements GCPlanCatalog.
//
// Deferred to Phase 6 (GC migration). The GC plan must be available before
// "GC must never delete reachable data" can be tested at the engine/catalog
// boundary. Callers must not rely on this method until it is implemented.
func (s *Service) LoadGCPlanMetadata(ctx context.Context, input GCPlanInput) (*GCPlanMetadata, error) {
	_, _ = ctx, input
	return nil, ErrNotImplemented
}
