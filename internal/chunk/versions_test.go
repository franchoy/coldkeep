package chunk

import "testing"

func TestIsWellFormedVersion(t *testing.T) {
	tests := []struct {
		name   string
		input  Version
		expect bool
	}{
		{name: "default version", input: VersionV1SimpleRolling, expect: true},
		{name: "future unknown version", input: Version("v9-future-cdc"), expect: true},
		{name: "single suffix", input: Version("v2-fastcdc"), expect: true},
		{name: "empty", input: Version(""), expect: false},
		{name: "spaces only", input: Version("   "), expect: false},
		{name: "missing v prefix", input: Version("1-simple-rolling"), expect: false},
		{name: "uppercase V", input: Version("V1-simple-rolling"), expect: false},
		{name: "non numeric major", input: Version("vX-simple-rolling"), expect: false},
		{name: "missing suffix", input: Version("v1"), expect: false},
		{name: "invalid char", input: Version("v1-simple_rolling"), expect: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsWellFormedVersion(tt.input); got != tt.expect {
				t.Fatalf("IsWellFormedVersion(%q) = %v, want %v", tt.input, got, tt.expect)
			}
		})
	}
}
