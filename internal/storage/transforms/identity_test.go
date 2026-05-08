package transforms_test

import (
	"bytes"
	"testing"

	"github.com/franchoy/coldkeep/internal/storage/transforms"
)

func TestIdentityTransform_RoundTrip(t *testing.T) {
	tr := &transforms.IdentityTransform{}

	if tr.Name() != "none" {
		t.Fatalf("expected name %q, got %q", "none", tr.Name())
	}

	input := []byte("hello coldkeep")

	encoded, err := tr.Encode(input)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if !bytes.Equal(encoded, input) {
		t.Fatalf("Encode modified bytes unexpectedly")
	}

	decoded, err := tr.Decode(encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !bytes.Equal(decoded, input) {
		t.Fatalf("Decode did not recover original bytes")
	}
}

func TestIdentityTransform_EmptyInput(t *testing.T) {
	tr := &transforms.IdentityTransform{}

	encoded, err := tr.Encode([]byte{})
	if err != nil {
		t.Fatalf("Encode empty: %v", err)
	}
	decoded, err := tr.Decode(encoded)
	if err != nil {
		t.Fatalf("Decode empty: %v", err)
	}
	if !bytes.Equal(decoded, []byte{}) {
		t.Fatalf("expected empty bytes, got %v", decoded)
	}
}

func TestIdentityTransform_NilInput(t *testing.T) {
	tr := &transforms.IdentityTransform{}

	encoded, err := tr.Encode(nil)
	if err != nil {
		t.Fatalf("Encode nil: %v", err)
	}
	decoded, err := tr.Decode(encoded)
	if err != nil {
		t.Fatalf("Decode nil: %v", err)
	}
	if len(decoded) != 0 {
		t.Fatalf("expected nil/empty, got %v", decoded)
	}
}

// Compile-time interface check.
var _ transforms.Transform = (*transforms.IdentityTransform)(nil)
