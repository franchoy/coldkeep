package container

import "testing"

// mustNoErr fails the test immediately if err is non-nil.
func mustNoErr(t *testing.T, err error, msg string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", msg, err)
	}
}
