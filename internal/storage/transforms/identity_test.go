package transforms_test

import (
	"bytes"
	"crypto/rand"
	"errors"
	"testing"

	"github.com/franchoy/coldkeep/internal/storage/transforms"
	"github.com/franchoy/coldkeep/internal/storage/transforms/aesgcm"
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

// ---------------------------------------------------------------------------
// TransformPipeline tests
// ---------------------------------------------------------------------------

func TestPipeline_Empty_IsIdentity(t *testing.T) {
	p := transforms.NewTransformPipeline()
	if p.Len() != 0 {
		t.Fatalf("expected Len 0, got %d", p.Len())
	}

	input := []byte("no-op pipeline")
	enc, err := p.Encode(input)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if !bytes.Equal(enc, input) {
		t.Fatalf("empty pipeline Encode changed bytes")
	}
	dec, err := p.Decode(enc)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !bytes.Equal(dec, input) {
		t.Fatalf("empty pipeline Decode changed bytes")
	}
}

func TestPipeline_SingleIdentity_RoundTrip(t *testing.T) {
	p := transforms.NewTransformPipeline(&transforms.IdentityTransform{})
	input := []byte("single identity stage")

	enc, err := p.Encode(input)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	dec, err := p.Decode(enc)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !bytes.Equal(dec, input) {
		t.Fatalf("round-trip failed")
	}
}

func TestPipeline_MultipleIdentity_RoundTrip(t *testing.T) {
	p := transforms.NewTransformPipeline(
		&transforms.IdentityTransform{},
		&transforms.IdentityTransform{},
		&transforms.IdentityTransform{},
	)
	if p.Len() != 3 {
		t.Fatalf("expected Len 3, got %d", p.Len())
	}
	input := []byte("three identity stages")
	enc, _ := p.Encode(input)
	dec, err := p.Decode(enc)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !bytes.Equal(dec, input) {
		t.Fatalf("round-trip failed")
	}
}

func TestPipeline_AESGCMStage_RoundTrip(t *testing.T) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("generate key: %v", err)
	}
	p := transforms.NewTransformPipeline(&aesgcm.AESGCMTransform{Key: key})
	input := []byte("encrypted pipeline stage")

	enc, err := p.Encode(input)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if bytes.Equal(enc, input) {
		t.Fatal("expected encrypted output to differ from plaintext")
	}
	dec, err := p.Decode(enc)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !bytes.Equal(dec, input) {
		t.Fatalf("round-trip failed")
	}
}

func TestPipeline_DecodeReverses_EncodeOrder(t *testing.T) {
	// Use a marker transform that records call order to verify symmetry.
	var order []string

	makeStage := func(name string) transforms.Transform {
		return &recordingTransform{
			name:   name,
			record: &order,
		}
	}

	p := transforms.NewTransformPipeline(makeStage("A"), makeStage("B"), makeStage("C"))

	order = nil
	if _, err := p.Encode([]byte("x")); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if !bytes.Equal([]byte(order[0]+order[1]+order[2]), []byte("ABC")) {
		t.Fatalf("expected encode order A,B,C got %v", order)
	}

	order = nil
	if _, err := p.Decode([]byte("x")); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !bytes.Equal([]byte(order[0]+order[1]+order[2]), []byte("CBA")) {
		t.Fatalf("expected decode order C,B,A got %v", order)
	}
}

func TestPipeline_EncodeError_Propagates(t *testing.T) {
	p := transforms.NewTransformPipeline(&errorTransform{})
	_, err := p.Encode([]byte("x"))
	if err == nil {
		t.Fatal("expected error from failing stage, got nil")
	}
}

func TestPipeline_DecodeError_Propagates(t *testing.T) {
	p := transforms.NewTransformPipeline(&errorTransform{})
	_, err := p.Decode([]byte("x"))
	if err == nil {
		t.Fatal("expected error from failing stage, got nil")
	}
}

// recordingTransform appends its name to a shared slice on each call and
// passes bytes through unchanged. Used to verify call order.
type recordingTransform struct {
	name   string
	record *[]string
}

func (r *recordingTransform) Name() string { return r.name }
func (r *recordingTransform) Encode(input []byte) ([]byte, error) {
	*r.record = append(*r.record, r.name)
	return input, nil
}
func (r *recordingTransform) Decode(input []byte) ([]byte, error) {
	*r.record = append(*r.record, r.name)
	return input, nil
}

// errorTransform always returns an error from both Encode and Decode.
type errorTransform struct{}

func (e *errorTransform) Name() string { return "error" }
func (e *errorTransform) Encode(_ []byte) ([]byte, error) {
	return nil, errors.New("intentional encode error")
}
func (e *errorTransform) Decode(_ []byte) ([]byte, error) {
	return nil, errors.New("intentional decode error")
}
