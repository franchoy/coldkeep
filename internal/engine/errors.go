package engine

import "errors"

// IsUnsupported classifies only active unsupported engine modes already
// represented by ErrNotImplemented.
//
// It does not classify validation, domain, invariant, runtime, catalog, or
// storage failures. It also does not imply deferred candidate-only surfaces are
// active. The helper remains intentionally narrow.
func IsUnsupported(err error) bool {
	return errors.Is(err, ErrNotImplemented)
}
