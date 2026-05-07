package blocks

import (
	"bytes"
	"errors"
	"testing"
)

func TestBlockBuilderBuildMultiChunk(t *testing.T) {
	b := NewBlockBuilder(64)

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
	b := NewBlockBuilder(64)
	_, _, err := b.Build()
	if err == nil {
		t.Fatal("expected build empty block to fail")
	}
}

func TestBlockBuilderCanFitAndAddRules(t *testing.T) {
	b := NewBlockBuilder(8)

	if !b.CanFit(8) {
		t.Fatal("expected empty builder to fit exact target-size chunk")
	}
	if !b.CanFit(9) {
		t.Fatal("expected empty builder to allow oversized chunk to be written alone")
	}

	if err := b.Add(PendingChunk{ChunkID: 1, Data: []byte("abc"), Size: 3}); err != nil {
		t.Fatalf("add first chunk: %v", err)
	}
	if b.CanFit(9) {
		t.Fatal("did not expect oversized chunk to fit non-empty builder")
	}
	if err := b.Add(PendingChunk{ChunkID: 2, Data: []byte("012345678"), Size: 9}); !errors.Is(err, ErrBlockBuilderCannotFit) {
		t.Fatalf("expected ErrBlockBuilderCannotFit, got: %v", err)
	}
}

func TestBlockBuilderRejectsZeroSizeChunk(t *testing.T) {
	b := NewBlockBuilder(32)
	err := b.Add(PendingChunk{ChunkID: 1, Data: []byte{}, Size: 0})
	if !errors.Is(err, ErrBlockBuilderZeroChunkSize) {
		t.Fatalf("expected ErrBlockBuilderZeroChunkSize, got: %v", err)
	}
}

func TestBlockBuilderOversizedChunkAlone(t *testing.T) {
	b := NewBlockBuilder(4)
	err := b.Add(PendingChunk{ChunkID: 1, Data: []byte("oversized"), Size: int64(len("oversized"))})
	if err != nil {
		t.Fatalf("expected oversized chunk to be allowed when builder empty, got: %v", err)
	}

	enc, _, err := b.Build()
	if err != nil {
		t.Fatalf("build oversized-alone block: %v", err)
	}
	if enc.Header.ChunkCount != 1 {
		t.Fatalf("expected one chunk in oversized-alone block, got %d", enc.Header.ChunkCount)
	}
	if string(enc.Payload) != "oversized" {
		t.Fatalf("unexpected payload: got %q", string(enc.Payload))
	}
}

func TestBlockBuilderReset(t *testing.T) {
	b := NewBlockBuilder(64)
	if err := b.Add(PendingChunk{ChunkID: 1, Data: []byte("abc"), Size: 3}); err != nil {
		t.Fatalf("add before reset: %v", err)
	}
	if b.Empty() {
		t.Fatal("builder should be non-empty after Add")
	}

	b.Reset()
	if !b.Empty() {
		t.Fatal("builder should be empty after Reset")
	}

	if err := b.Add(PendingChunk{ChunkID: 2, Data: []byte("xy"), Size: 2}); err != nil {
		t.Fatalf("add after reset: %v", err)
	}
}

func TestBlockBuilderFlushRuleByTargetOverflow(t *testing.T) {
	b := NewBlockBuilder(10)
	if err := b.Add(PendingChunk{ChunkID: 1, Data: []byte("1234567"), Size: 7}); err != nil {
		t.Fatalf("add seed chunk: %v", err)
	}

	if !b.ShouldFlushBeforeAdd(4) {
		t.Fatal("expected flush when current_size + next_size exceeds target")
	}
	if b.ShouldFlushBeforeAdd(3) {
		t.Fatal("did not expect flush when current_size + next_size equals target")
	}
}

func TestBlockBuilderFlushRuleOversizedChunkMustBeAlone(t *testing.T) {
	b := NewBlockBuilder(8)
	if err := b.Add(PendingChunk{ChunkID: 1, Data: []byte("abc"), Size: 3}); err != nil {
		t.Fatalf("add seed chunk: %v", err)
	}

	if !b.ShouldFlushBeforeAdd(9) {
		t.Fatal("expected flush when next chunk is oversized and builder is non-empty")
	}

	b.Reset()
	if b.ShouldFlushBeforeAdd(9) {
		t.Fatal("did not expect pre-add flush on empty builder for oversized chunk")
	}
	if !b.CanFit(9) {
		t.Fatal("expected empty builder to accept oversized chunk alone")
	}
}

func TestBlockBuilderFlushRuleAtOperationEnd(t *testing.T) {
	b := NewBlockBuilder(16)
	if b.ShouldFlushAtEnd() {
		t.Fatal("did not expect end flush for empty builder")
	}

	if err := b.Add(PendingChunk{ChunkID: 1, Data: []byte("data"), Size: 4}); err != nil {
		t.Fatalf("add chunk: %v", err)
	}
	if !b.ShouldFlushAtEnd() {
		t.Fatal("expected end flush when builder has pending chunks")
	}
}
