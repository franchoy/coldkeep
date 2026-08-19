package catalog

import "context"

// LoadGCPlanMetadata implements GCPlanCatalog.
//
// Phase 9 implements and adopts deterministic reachability metadata.
func (s *Service) LoadGCPlanMetadata(ctx context.Context, input GCPlanInput) (*GCPlanMetadata, error) {
	_, _ = ctx, input
	return nil, ErrNotImplemented
}
