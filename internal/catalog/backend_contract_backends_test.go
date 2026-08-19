package catalog_test

import (
	"testing"

	"github.com/franchoy/coldkeep/internal/testutil/backendtest"
)

// forEachCatalogBackend delegates fixture lifecycle to the reusable harness.
// PostgreSQL remains optional locally. Required CI is configured to execute
// its PostgreSQL subtests in correctness-matrix's plain-codec package-contract
// step.
func forEachCatalogBackend(t *testing.T, fn func(t *testing.T, backend backendtest.Backend)) {
	t.Helper()
	backendtest.ForEach(t, backendtest.Options{}, fn)
}
