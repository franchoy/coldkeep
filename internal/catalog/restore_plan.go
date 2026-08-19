package catalog

import "context"

// LoadRestorePlanMetadata implements RestorePlanCatalog.
//
// Phase 7 implements selector resolution and immutable recipe construction.
func (s *Service) LoadRestorePlanMetadata(ctx context.Context, input RestorePlanInput) (*RestorePlanMetadata, error) {
	_, _ = ctx, input
	return nil, ErrNotImplemented
}
