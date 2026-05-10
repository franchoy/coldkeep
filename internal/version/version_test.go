package version

import "testing"

func TestStringReturnsSemverFromConstants(t *testing.T) {
	if got, want := String(), "1.9.0"; got != want {
		t.Fatalf("String() mismatch: got=%q want=%q", got, want)
	}
}
