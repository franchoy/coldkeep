package engine

import (
	"testing"

	"github.com/franchoy/coldkeep/internal/verify"
)

// Compile-time check: DefaultEngine satisfies the Engine interface.
var _ Engine = (*DefaultEngine)(nil)

func TestNewRequiresDB(t *testing.T) {
	_, err := New(Config{})
	if err == nil {
		t.Fatal("New with nil DB: want error, got nil")
	}
}

func TestVerifyLevelFromString(t *testing.T) {
	tests := []struct {
		input   string
		want    verify.VerifyLevel
		wantErr bool
	}{
		{"", verify.VerifyStandard, false},
		{"standard", verify.VerifyStandard, false},
		{"fast", verify.VerifyFast, false},
		{"full", verify.VerifyFull, false},
		{"deep", verify.VerifyDeep, false},
		{"bad", 0, true},
		{"STANDARD", 0, true},
	}
	for _, tc := range tests {
		t.Run("input="+tc.input, func(t *testing.T) {
			got, err := verifyLevelFromString(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("verifyLevelFromString(%q): err=%v, wantErr=%v", tc.input, err, tc.wantErr)
			}
			if !tc.wantErr && got != tc.want {
				t.Errorf("verifyLevelFromString(%q) = %v, want %v", tc.input, got, tc.want)
			}
		})
	}
}
