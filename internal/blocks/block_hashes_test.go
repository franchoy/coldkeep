package blocks

import (
	"bytes"
	"crypto/sha256"
	"testing"
)

func TestHashLogicalMatchesComputeBlockHash(t *testing.T) {
	data := []byte("encoded plaintext block bytes for testing")
	got := HashLogical(data)
	want := ComputeBlockHash(data)
	if !bytes.Equal(got, want) {
		t.Fatalf("HashLogical != ComputeBlockHash: got %x want %x", got, want)
	}
}

func TestHashLogicalReturnsSHA256(t *testing.T) {
	data := []byte("hello coldkeep")
	got := HashLogical(data)
	sum := sha256.Sum256(data)
	if !bytes.Equal(got, sum[:]) {
		t.Fatalf("HashLogical: got %x want %x", got, sum[:])
	}
}

func TestHashCompressedEqualsHashLogicalWhenNoneCodec(t *testing.T) {
	// Phase 2 invariant: when compression is disabled the pre-encryption
	// payload is the plaintext encoded bytes, so CompressedHash == LogicalHash.
	data := []byte("plaintext block data, no compression")
	if !bytes.Equal(HashLogical(data), HashCompressed(data)) {
		t.Fatal("Phase 2 invariant violated: HashCompressed != HashLogical for same input")
	}
}

func TestAllHashHelpersReturn32Bytes(t *testing.T) {
	data := []byte("test payload")
	for name, fn := range map[string]func([]byte) []byte{
		"HashLogical":    HashLogical,
		"HashCompressed": HashCompressed,
		"HashPhysical":   HashPhysical,
	} {
		result := fn(data)
		if len(result) != 32 {
			t.Errorf("%s: expected 32-byte digest, got %d bytes", name, len(result))
		}
	}
}

func TestHashHelpersEmptyInput(t *testing.T) {
	// sha256 of empty input is deterministic; all three helpers must agree on empty.
	empty := []byte{}
	want := sha256.Sum256(empty)
	for name, fn := range map[string]func([]byte) []byte{
		"HashLogical":    HashLogical,
		"HashCompressed": HashCompressed,
		"HashPhysical":   HashPhysical,
	} {
		got := fn(empty)
		if !bytes.Equal(got, want[:]) {
			t.Errorf("%s(empty): got %x want %x", name, got, want)
		}
	}
}

func TestBlockHashesStruct(t *testing.T) {
	data := []byte("block payload")
	h := BlockHashes{
		LogicalHash:    HashLogical(data),
		CompressedHash: HashCompressed(data),
		PhysicalHash:   HashPhysical(data),
	}
	if len(h.LogicalHash) != 32 || len(h.CompressedHash) != 32 || len(h.PhysicalHash) != 32 {
		t.Fatal("BlockHashes: one or more fields are not 32 bytes")
	}
	// Phase 2 invariant: LogicalHash == CompressedHash when same input
	if !bytes.Equal(h.LogicalHash, h.CompressedHash) {
		t.Fatal("BlockHashes: LogicalHash != CompressedHash for same data (Phase 2 invariant)")
	}
}
