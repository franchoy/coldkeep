package version

import "testing"

func TestStringReturnsSemverFromConstants(t *testing.T) {
	if got, want := String(), "1.10.16"; got != want {
		t.Fatalf("String() mismatch: got=%q want=%q", got, want)
	}
}
