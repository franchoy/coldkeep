package main

import (
	"os"
	"testing"

	testutils "github.com/franchoy/coldkeep/v2/tests/utils"
)

func TestMain(m *testing.M) {
	os.Exit(testutils.RunWithIsolatedPostgresDB("adversarial", m))
}
