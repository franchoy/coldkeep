package version

import "testing"

func TestStringReturnsSemverFromConstants(t *testing.T) {
	if got, want := String(), "1.13.12"; got != want {
		t.Fatalf("String() mismatch: got=%q want=%q", got, want)
	}
}
