package catalog_test

import (
	"testing"

	"github.com/franchoy/coldkeep/internal/testutil/backendtest"
)

// forEachCatalogBackend delegates fixture lifecycle to the reusable harness.
// PostgreSQL remains optional locally and is not required-CI executed until
// Phase 4 provisions it for the internal package suite.
func forEachCatalogBackend(t *testing.T, fn func(t *testing.T, backend backendtest.Backend)) {
	t.Helper()
	backendtest.ForEach(t, backendtest.Options{}, fn)
}
