package catalog

import "context"

// LoadRestorePlanMetadata implements RestorePlanCatalog.
//
// Deferred to Phase 7 (restore migration). The restore plan must be available
// before "restore must not write outside the intended destination" can be
// enforced at the engine/catalog boundary. Callers must not rely on this
// method until it is implemented.
func (s *Service) LoadRestorePlanMetadata(ctx context.Context, input RestorePlanInput) (*RestorePlanMetadata, error) {
	_, _ = ctx, input
	return nil, ErrNotImplemented
}
