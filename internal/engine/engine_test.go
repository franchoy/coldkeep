package engine

import (
	"testing"

	"github.com/franchoy/coldkeep/internal/observability"
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

func TestValidateInspectRequest(t *testing.T) {
	tests := []struct {
		name    string
		req     InspectRequest
		wantErr bool
	}{
		{"repository no id", InspectRequest{Entity: observability.EntityRepository}, false},
		{"repository id ignored", InspectRequest{Entity: observability.EntityRepository, EntityID: "99"}, false},
		{"file positive int", InspectRequest{Entity: observability.EntityFile, EntityID: "42"}, false},
		{"file zero", InspectRequest{Entity: observability.EntityFile, EntityID: "0"}, true},
		{"file negative", InspectRequest{Entity: observability.EntityFile, EntityID: "-1"}, true},
		{"file non-numeric", InspectRequest{Entity: observability.EntityFile, EntityID: "abc"}, true},
		{"file empty id", InspectRequest{Entity: observability.EntityFile, EntityID: ""}, true},
		{"file whitespace id", InspectRequest{Entity: observability.EntityFile, EntityID: "  "}, true},
		{"logical_file positive int", InspectRequest{Entity: observability.EntityLogicalFile, EntityID: "1"}, false},
		{"physical_file positive int", InspectRequest{Entity: observability.EntityPhysicalFile, EntityID: "1"}, false},
		{"chunk positive int", InspectRequest{Entity: observability.EntityChunk, EntityID: "7"}, false},
		{"container positive int", InspectRequest{Entity: observability.EntityContainer, EntityID: "3"}, false},
		{"snapshot string id", InspectRequest{Entity: observability.EntitySnapshot, EntityID: "snap-123"}, false},
		{"snapshot empty id", InspectRequest{Entity: observability.EntitySnapshot, EntityID: ""}, true},
		{"snapshot whitespace id", InspectRequest{Entity: observability.EntitySnapshot, EntityID: "  "}, true},
		{"unknown entity", InspectRequest{Entity: observability.EntityType("bogus")}, true},
		{"empty entity", InspectRequest{Entity: observability.EntityType("")}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateInspectRequest(tc.req)
			if (err != nil) != tc.wantErr {
				t.Errorf("validateInspectRequest(%+v) error = %v, wantErr %v", tc.req, err, tc.wantErr)
			}
		})
	}
}

func TestValidateVerifyRequest(t *testing.T) {
	tests := []struct {
		name    string
		target  string
		fileID  int
		wantErr bool
	}{
		{"system", "system", 0, false},
		{"file positive id", "file", 1, false},
		{"file large id", "file", 99999, false},
		{"file zero id", "file", 0, true},
		{"file negative id", "file", -1, true},
		{"unknown target", "bogus", 0, true},
		{"empty target", "", 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateVerifyRequest(tc.target, tc.fileID)
			if (err != nil) != tc.wantErr {
				t.Errorf("validateVerifyRequest(%q, %d) error = %v, wantErr %v", tc.target, tc.fileID, err, tc.wantErr)
			}
		})
	}
}
