package catalog

import "errors"

// IsDeferred classifies catalog deferred surfaces represented by
// catalog.ErrNotImplemented.
//
// It is intentionally narrow for v1.13.2. It does not mean deferred catalog
// methods are implemented. It does not classify engine unsupported errors,
// validation errors, domain errors, invariant errors, runtime errors, or
// storage failures. The helper preserves errors.Is compatibility.
func IsDeferred(err error) bool {
	return errors.Is(err, ErrNotImplemented)
}
