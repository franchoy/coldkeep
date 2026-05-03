package blocks

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestEncodeBlockSerializesHeaderEntriesPayloadLittleEndian(t *testing.T) {
	b := &EncodedBlock{
		Header: BlockHeader{
			Magic:         BlockMagicV1,
			Version:       BlockFormatVersionV1,
			Codec:         BlockCodecNoneV1,
			ChunkCount:    2,
			PlaintextSize: 3,
		},
		Entries: []ChunkEntry{
			{ChunkID: 10, Offset: 0, Size: 1},
			{ChunkID: 20, Offset: 1, Size: 2},
		},
		Payload: []byte("abc"),
	}

	raw, err := EncodeBlock(b)
	if err != nil {
		t.Fatalf("encode block: %v", err)
	}

	if got := binary.LittleEndian.Uint32(raw[0:4]); got != BlockMagicV1 {
		t.Fatalf("magic mismatch: got %x want %x", got, BlockMagicV1)
	}
	if got := binary.LittleEndian.Uint16(raw[4:6]); got != BlockFormatVersionV1 {
		t.Fatalf("version mismatch: got %d want %d", got, BlockFormatVersionV1)
	}
	if got := binary.LittleEndian.Uint16(raw[6:8]); got != BlockCodecNoneV1 {
		t.Fatalf("codec mismatch: got %d want %d", got, BlockCodecNoneV1)
	}
	if got := binary.LittleEndian.Uint32(raw[8:12]); got != 2 {
		t.Fatalf("chunk_count mismatch: got %d want 2", got)
	}
	if got := binary.LittleEndian.Uint64(raw[12:20]); got != 3 {
		t.Fatalf("plaintext_size mismatch: got %d want 3", got)
	}

	decoded, err := DecodePackedBlockV1(raw)
	if err != nil {
		t.Fatalf("decode encoded bytes: %v", err)
	}
	if !bytes.Equal(decoded.Payload, []byte("abc")) {
		t.Fatalf("payload mismatch: got %q want %q", decoded.Payload, "abc")
	}
}

func TestEncodeBlockRejectsInconsistentShape(t *testing.T) {
	_, err := EncodeBlock(&EncodedBlock{
		Header: BlockHeader{ChunkCount: 2, PlaintextSize: 1},
		Entries: []ChunkEntry{
			{ChunkID: 1, Offset: 0, Size: 1},
		},
		Payload: []byte("x"),
	})
	if err == nil {
		t.Fatal("expected error for inconsistent chunk_count")
	}
}

func TestEncodeDecodePackedBlockV1RoundTripFromChunks(t *testing.T) {
	encoded, err := EncodePackedBlockV1FromChunks([]PackedChunk{
		{ChunkID: 101, Data: []byte("hello")},
		{ChunkID: 102, Data: []byte("-")},
		{ChunkID: 103, Data: []byte("world")},
	})
	if err != nil {
		t.Fatalf("encode from chunks: %v", err)
	}

	if encoded.Header.Magic != BlockMagicV1 {
		t.Fatalf("unexpected magic: got %x want %x", encoded.Header.Magic, BlockMagicV1)
	}
	if encoded.Header.Version != BlockFormatVersionV1 {
		t.Fatalf("unexpected version: got %d want %d", encoded.Header.Version, BlockFormatVersionV1)
	}
	if encoded.Header.Codec != BlockCodecNoneV1 {
		t.Fatalf("unexpected codec: got %d want %d", encoded.Header.Codec, BlockCodecNoneV1)
	}
	if encoded.Header.ChunkCount != 3 {
		t.Fatalf("unexpected chunk count: got %d want 3", encoded.Header.ChunkCount)
	}

	decoded, err := DecodePackedBlockV1(encoded.Bytes)
	if err != nil {
		t.Fatalf("decode block: %v", err)
	}

	if decoded.Header.ChunkCount != 3 {
		t.Fatalf("unexpected decoded chunk count: got %d want 3", decoded.Header.ChunkCount)
	}

	expectedPayload := []byte("hello-world")
	if !bytes.Equal(decoded.Payload, expectedPayload) {
		t.Fatalf("payload mismatch: got %q want %q", decoded.Payload, expectedPayload)
	}

	first, err := SliceChunkFromPayload(decoded.Payload, decoded.Entries[0])
	if err != nil {
		t.Fatalf("slice first chunk: %v", err)
	}
	if !bytes.Equal(first, []byte("hello")) {
		t.Fatalf("first chunk mismatch: got %q want %q", first, "hello")
	}

	third, err := SliceChunkFromPayload(decoded.Payload, decoded.Entries[2])
	if err != nil {
		t.Fatalf("slice third chunk: %v", err)
	}
	if !bytes.Equal(third, []byte("world")) {
		t.Fatalf("third chunk mismatch: got %q want %q", third, "world")
	}
}

func TestEncodePackedBlockV1IsDeterministic(t *testing.T) {
	entries := []ChunkEntry{
		{ChunkID: 1, Offset: 0, Size: 3},
		{ChunkID: 2, Offset: 3, Size: 2},
	}
	payload := []byte("abcde")

	enc1, err := EncodePackedBlockV1(entries, payload)
	if err != nil {
		t.Fatalf("encode first: %v", err)
	}
	enc2, err := EncodePackedBlockV1(entries, payload)
	if err != nil {
		t.Fatalf("encode second: %v", err)
	}

	if !bytes.Equal(enc1.Bytes, enc2.Bytes) {
		t.Fatal("expected deterministic encoded bytes")
	}
	if !bytes.Equal(enc1.BlockHash, enc2.BlockHash) {
		t.Fatal("expected deterministic block hash")
	}
}

func TestDecodePackedBlockV1RejectsCorruptedMagic(t *testing.T) {
	enc, err := EncodePackedBlockV1([]ChunkEntry{{ChunkID: 1, Offset: 0, Size: 1}}, []byte("a"))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	corrupted := append([]byte(nil), enc.Bytes...)
	corrupted[0] ^= 0xFF

	if _, err := DecodePackedBlockV1(corrupted); err == nil {
		t.Fatal("expected decode error for corrupted magic")
	}
}

func TestDecodePackedBlockV1RejectsInvalidEntryBounds(t *testing.T) {
	_, err := EncodePackedBlockV1([]ChunkEntry{{ChunkID: 1, Offset: 10, Size: 1}}, []byte("abc"))
	if err == nil {
		t.Fatal("expected encode error for invalid entry bounds")
	}
}

func TestHashPlaintextEncodedBlockMatchesEncodedResult(t *testing.T) {
	enc, err := EncodePackedBlockV1([]ChunkEntry{{ChunkID: 7, Offset: 0, Size: 4}}, []byte("data"))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	got := HashPlaintextEncodedBlock(enc.Bytes)
	if !bytes.Equal(got, enc.BlockHash) {
		t.Fatal("block hash mismatch: expected hash over plaintext encoded block bytes")
	}
}
