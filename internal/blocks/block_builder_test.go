package blocks

import (
	"bytes"
	"testing"
)

func TestBlockBuilderBuildMultiChunk(t *testing.T) {
	var b BlockBuilder

	if err := b.AddChunk(11, []byte("hello")); err != nil {
		t.Fatalf("add first chunk: %v", err)
	}
	if err := b.AddChunk(22, []byte("-")); err != nil {
		t.Fatalf("add second chunk: %v", err)
	}
	if err := b.AddChunk(33, []byte("world")); err != nil {
		t.Fatalf("add third chunk: %v", err)
	}

	encoded, hash, err := b.Build()
	if err != nil {
		t.Fatalf("build block: %v", err)
	}

	if encoded.Header.ChunkCount != 3 {
		t.Fatalf("unexpected chunk count: got %d want 3", encoded.Header.ChunkCount)
	}
	if encoded.Header.PlaintextSize != uint64(len("hello-world")) {
		t.Fatalf("unexpected plaintext size: got %d want %d", encoded.Header.PlaintextSize, len("hello-world"))
	}

	if len(encoded.Entries) != 3 {
		t.Fatalf("unexpected entries len: got %d want 3", len(encoded.Entries))
	}
	if encoded.Entries[0].Offset != 0 || encoded.Entries[0].Size != 5 {
		t.Fatalf("unexpected first entry: %+v", encoded.Entries[0])
	}
	if encoded.Entries[1].Offset != 5 || encoded.Entries[1].Size != 1 {
		t.Fatalf("unexpected second entry: %+v", encoded.Entries[1])
	}
	if encoded.Entries[2].Offset != 6 || encoded.Entries[2].Size != 5 {
		t.Fatalf("unexpected third entry: %+v", encoded.Entries[2])
	}

	expectedPayload := []byte("hello-world")
	if !bytes.Equal(encoded.Payload, expectedPayload) {
		t.Fatalf("payload mismatch: got %q want %q", encoded.Payload, expectedPayload)
	}

	serialized, err := EncodePackedBlockV1(encoded.Entries, encoded.Payload)
	if err != nil {
		t.Fatalf("re-encode serialized block: %v", err)
	}
	if !bytes.Equal(hash, serialized.BlockHash) {
		t.Fatal("hash mismatch: expected hash of serialized plaintext encoded block")
	}
}

func TestBlockBuilderBuildEmpty(t *testing.T) {
	var b BlockBuilder
	encoded, hash, err := b.Build()
	if err != nil {
		t.Fatalf("build empty block: %v", err)
	}
	if encoded.Header.ChunkCount != 0 {
		t.Fatalf("unexpected empty chunk count: got %d want 0", encoded.Header.ChunkCount)
	}
	if encoded.Header.PlaintextSize != 0 {
		t.Fatalf("unexpected empty plaintext size: got %d want 0", encoded.Header.PlaintextSize)
	}
	if len(encoded.Payload) != 0 {
		t.Fatalf("unexpected payload length: got %d want 0", len(encoded.Payload))
	}
	if len(hash) == 0 {
		t.Fatal("expected non-empty hash for encoded header-only block")
	}
}
