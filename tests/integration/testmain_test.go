package main

import (
	"os"
	"testing"

	testutils "github.com/franchoy/coldkeep/tests/utils"
)

func TestMain(m *testing.M) {
	os.Exit(testutils.RunWithIsolatedPostgresDB("integration", m))
}
