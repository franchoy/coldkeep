package blocks

import (
	"bytes"
	"encoding/binary"
	"errors"
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

func TestEncodeBlockRejectsUnsupportedHeaderConstants(t *testing.T) {
	_, err := EncodeBlock(&EncodedBlock{
		Header: BlockHeader{
			Magic:         0,
			Version:       999,
			Codec:         42,
			ChunkCount:    1,
			PlaintextSize: 1,
		},
		Entries: []ChunkEntry{{ChunkID: 1, Offset: 0, Size: 1}},
		Payload: []byte("x"),
	})
	if err == nil {
		t.Fatal("expected unsupported-header encode error")
	}
	if !errors.Is(err, ErrBlockFormatUnsupported) {
		t.Fatalf("expected ErrBlockFormatUnsupported, got: %v", err)
	}
}

func TestDecodeBlockReadsHeaderTableAndPayload(t *testing.T) {
	enc, err := EncodePackedBlockV1FromChunks([]PackedChunk{
		{ChunkID: 101, Data: []byte("aa")},
		{ChunkID: 202, Data: []byte("bbb")},
	})
	if err != nil {
		t.Fatalf("encode block: %v", err)
	}

	b, err := DecodeBlock(enc.Bytes)
	if err != nil {
		t.Fatalf("decode block: %v", err)
	}

	if b.Header.Magic != BlockMagicV1 {
		t.Fatalf("unexpected magic: got %x want %x", b.Header.Magic, BlockMagicV1)
	}
	if b.Header.Version != BlockFormatVersionV1 {
		t.Fatalf("unexpected version: got %d want %d", b.Header.Version, BlockFormatVersionV1)
	}
	if len(b.Entries) != 2 {
		t.Fatalf("unexpected entries len: got %d want 2", len(b.Entries))
	}

	if got := b.GetChunk(0); !bytes.Equal(got, []byte("aa")) {
		t.Fatalf("chunk[0] mismatch: got %q want %q", got, "aa")
	}
	if got := b.GetChunk(1); !bytes.Equal(got, []byte("bbb")) {
		t.Fatalf("chunk[1] mismatch: got %q want %q", got, "bbb")
	}
}

func TestDecodeBlockRejectsUnsupportedVersion(t *testing.T) {
	enc, err := EncodePackedBlockV1FromChunks([]PackedChunk{{ChunkID: 1, Data: []byte("x")}})
	if err != nil {
		t.Fatalf("encode block: %v", err)
	}

	raw := append([]byte(nil), enc.Bytes...)
	binary.LittleEndian.PutUint16(raw[4:6], 999)

	if _, err := DecodeBlock(raw); err == nil {
		t.Fatal("expected unsupported-version decode error")
	}
}

func TestDecodeBlockRejectsInvalidChunkCount(t *testing.T) {
	raw := make([]byte, blockHeaderSizeV1)
	writeBlockHeader(raw, BlockHeader{
		Magic:         BlockMagicV1,
		Version:       BlockFormatVersionV1,
		Codec:         BlockCodecNoneV1,
		ChunkCount:    0,
		PlaintextSize: 0,
	})

	if _, err := DecodeBlock(raw); err == nil || !errors.Is(err, ErrBlockFormatInvalidCount) {
		t.Fatalf("expected invalid-count decode error, got: %v", err)
	}
}

func TestDecodeBlockRejectsPayloadLengthMismatch(t *testing.T) {
	b := &EncodedBlock{
		Header: BlockHeader{
			Magic:         BlockMagicV1,
			Version:       BlockFormatVersionV1,
			Codec:         BlockCodecNoneV1,
			ChunkCount:    1,
			PlaintextSize: 4,
		},
		Entries: []ChunkEntry{{ChunkID: 1, Offset: 0, Size: 4}},
		Payload: []byte("data"),
	}

	raw, err := EncodeBlock(b)
	if err != nil {
		t.Fatalf("encode block: %v", err)
	}

	truncated := raw[:len(raw)-1]
	if _, err := DecodeBlock(truncated); err == nil {
		t.Fatal("expected payload-length mismatch decode error")
	}
}

func TestDecodeBlockRejectsInvalidOffsets(t *testing.T) {
	enc, err := EncodePackedBlockV1FromChunks([]PackedChunk{{ChunkID: 1, Data: []byte("abc")}})
	if err != nil {
		t.Fatalf("encode block: %v", err)
	}

	raw := append([]byte(nil), enc.Bytes...)
	// Corrupt first entry size from 3 -> 4 so offset+size exceeds plaintext_size.
	binary.LittleEndian.PutUint64(raw[36:44], 4)

	if _, err := DecodeBlock(raw); err == nil {
		t.Fatal("expected invalid-offset decode error")
	}
}

func TestStep8SingleChunkEncodeDecodeCompareBytes(t *testing.T) {
	original := []byte("single-chunk-payload")
	enc, err := EncodePackedBlockV1FromChunks([]PackedChunk{{ChunkID: 9001, Data: original}})
	if err != nil {
		t.Fatalf("encode single chunk: %v", err)
	}

	decoded, err := DecodeBlock(enc.Bytes)
	if err != nil {
		t.Fatalf("decode single chunk: %v", err)
	}
	got := decoded.GetChunk(0)
	if !bytes.Equal(got, original) {
		t.Fatalf("single chunk roundtrip mismatch: got %q want %q", got, original)
	}
}

func TestStep8MultipleChunksOrderAndSlicing(t *testing.T) {
	chunks := []PackedChunk{
		{ChunkID: 1, Data: []byte("a")},
		{ChunkID: 2, Data: []byte("bb")},
		{ChunkID: 3, Data: []byte("ccc")},
		{ChunkID: 4, Data: []byte("dddd")},
		{ChunkID: 5, Data: []byte("eeeee")},
		{ChunkID: 6, Data: []byte("ffffff")},
	}

	enc, err := EncodePackedBlockV1FromChunks(chunks)
	if err != nil {
		t.Fatalf("encode multi-chunk: %v", err)
	}

	decoded, err := DecodeBlock(enc.Bytes)
	if err != nil {
		t.Fatalf("decode multi-chunk: %v", err)
	}

	if len(decoded.Entries) != len(chunks) {
		t.Fatalf("entries len mismatch: got %d want %d", len(decoded.Entries), len(chunks))
	}

	for i, ch := range chunks {
		if decoded.Entries[i].ChunkID != ch.ChunkID {
			t.Fatalf("chunk order mismatch at %d: got id=%d want id=%d", i, decoded.Entries[i].ChunkID, ch.ChunkID)
		}
		if got := decoded.GetChunk(i); !bytes.Equal(got, ch.Data) {
			t.Fatalf("chunk slice mismatch at %d: got %q want %q", i, got, ch.Data)
		}
	}
}

func TestStep8BoundarySizesZeroChunksFails(t *testing.T) {
	if _, err := EncodePackedBlockV1FromChunks(nil); err == nil {
		t.Fatal("expected zero-chunk encode to fail")
	}
}

func TestStep8BoundarySizesMaxAndUneven(t *testing.T) {
	maxPayload := bytes.Repeat([]byte{'x'}, 1024*1024)
	enc, err := EncodePackedBlockV1(
		[]ChunkEntry{{ChunkID: 1, Offset: 0, Size: uint64(len(maxPayload))}},
		maxPayload,
	)
	if err != nil {
		t.Fatalf("encode max-size-ish block: %v", err)
	}
	decoded, err := DecodeBlock(enc.Bytes)
	if err != nil {
		t.Fatalf("decode max-size-ish block: %v", err)
	}
	if got := decoded.GetChunk(0); !bytes.Equal(got, maxPayload) {
		t.Fatal("max-size chunk mismatch")
	}

	uneven := []PackedChunk{
		{ChunkID: 10, Data: []byte("a")},
		{ChunkID: 11, Data: bytes.Repeat([]byte{'b'}, 17)},
		{ChunkID: 12, Data: []byte("ccc")},
	}
	enc2, err := EncodePackedBlockV1FromChunks(uneven)
	if err != nil {
		t.Fatalf("encode uneven block: %v", err)
	}
	decoded2, err := DecodeBlock(enc2.Bytes)
	if err != nil {
		t.Fatalf("decode uneven block: %v", err)
	}
	for i := range uneven {
		if got := decoded2.GetChunk(i); !bytes.Equal(got, uneven[i].Data) {
			t.Fatalf("uneven chunk mismatch at %d", i)
		}
	}
}

func TestStep8CorruptionCasesFailDecode(t *testing.T) {
	enc, err := EncodePackedBlockV1FromChunks([]PackedChunk{{ChunkID: 1, Data: []byte("payload")}})
	if err != nil {
		t.Fatalf("encode block: %v", err)
	}

	badMagic := append([]byte(nil), enc.Bytes...)
	badMagic[0] ^= 0xFF
	if _, err := DecodeBlock(badMagic); err == nil {
		t.Fatal("expected bad-magic decode failure")
	}

	truncated := enc.Bytes[:len(enc.Bytes)-1]
	if _, err := DecodeBlock(truncated); err == nil {
		t.Fatal("expected truncated-payload decode failure")
	}

	invalidOffset := append([]byte(nil), enc.Bytes...)
	binary.LittleEndian.PutUint64(invalidOffset[28:36], 9999) // first entry offset
	if _, err := DecodeBlock(invalidOffset); err == nil {
		t.Fatal("expected invalid-offset decode failure")
	}
}

func TestStep8HashValidationRecomputeMatches(t *testing.T) {
	enc, err := EncodePackedBlockV1FromChunks([]PackedChunk{
		{ChunkID: 1, Data: []byte("hash")},
		{ChunkID: 2, Data: []byte("-validation")},
	})
	if err != nil {
		t.Fatalf("encode block: %v", err)
	}
	recomputed := ComputeBlockHash(enc.Bytes)
	if !bytes.Equal(recomputed, enc.BlockHash) {
		t.Fatal("recomputed hash mismatch")
	}
}

func TestGetChunkReturnsNilForInvalidIndexOrBounds(t *testing.T) {
	b := &EncodedBlock{
		Header: BlockHeader{Magic: BlockMagicV1, Version: BlockFormatVersionV1, Codec: BlockCodecNoneV1, ChunkCount: 1, PlaintextSize: 3},
		Entries: []ChunkEntry{
			{ChunkID: 1, Offset: 0, Size: 5}, // invalid against payload len=3
		},
		Payload: []byte("abc"),
	}

	if got := b.GetChunk(-1); got != nil {
		t.Fatalf("expected nil for negative index, got %q", got)
	}
	if got := b.GetChunk(1); got != nil {
		t.Fatalf("expected nil for out-of-range index, got %q", got)
	}
	if got := b.GetChunk(0); got != nil {
		t.Fatalf("expected nil for invalid bounds, got %q", got)
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

func TestEncodePackedBlockV1RejectsZeroSizeChunk(t *testing.T) {
	_, err := EncodePackedBlockV1([]ChunkEntry{{ChunkID: 1, Offset: 0, Size: 0}}, []byte{})
	if err == nil {
		t.Fatal("expected encode error for zero-size chunk entry")
	}
}

func TestEncodePackedBlockV1RejectsFirstOffsetNotZero(t *testing.T) {
	_, err := EncodePackedBlockV1([]ChunkEntry{{ChunkID: 1, Offset: 1, Size: 1}}, []byte("ab"))
	if err == nil {
		t.Fatal("expected encode error for first offset != 0")
	}
}

func TestEncodePackedBlockV1RejectsGapBetweenEntries(t *testing.T) {
	_, err := EncodePackedBlockV1(
		[]ChunkEntry{
			{ChunkID: 1, Offset: 0, Size: 1},
			{ChunkID: 2, Offset: 2, Size: 1},
		},
		[]byte("abc"),
	)
	if err == nil {
		t.Fatal("expected encode error for gap between entries")
	}
}

func TestEncodePackedBlockV1RejectsOverlapEntries(t *testing.T) {
	_, err := EncodePackedBlockV1(
		[]ChunkEntry{
			{ChunkID: 1, Offset: 0, Size: 2},
			{ChunkID: 2, Offset: 1, Size: 1},
		},
		[]byte("abc"),
	)
	if err == nil {
		t.Fatal("expected encode error for overlapping entries")
	}
}

func TestEncodePackedBlockV1RejectsFinalEndNotEqualPayloadSize(t *testing.T) {
	_, err := EncodePackedBlockV1(
		[]ChunkEntry{{ChunkID: 1, Offset: 0, Size: 1}},
		[]byte("ab"),
	)
	if err == nil {
		t.Fatal("expected encode error when final entry end != payload size")
	}
}

func TestHashPlaintextEncodedBlockMatchesEncodedResult(t *testing.T) {
	enc, err := EncodePackedBlockV1([]ChunkEntry{{ChunkID: 7, Offset: 0, Size: 4}}, []byte("data"))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	got := ComputeBlockHash(enc.Bytes)
	if !bytes.Equal(got, enc.BlockHash) {
		t.Fatal("block hash mismatch: expected hash over plaintext encoded block bytes")
	}
}

func TestComputeBlockHashTargetsPlaintextEncodedBytes(t *testing.T) {
	enc, err := EncodePackedBlockV1FromChunks([]PackedChunk{{ChunkID: 1, Data: []byte("hello")}})
	if err != nil {
		t.Fatalf("encode from chunks: %v", err)
	}

	plainHash := ComputeBlockHash(enc.Bytes)
	if !bytes.Equal(plainHash, enc.BlockHash) {
		t.Fatal("expected block hash to match plaintext encoded bytes")
	}

	// Simulate post-encoding transformation bytes (e.g. encrypted representation)
	// and ensure hash target stays plaintext encoded bytes.
	transformed := append([]byte(nil), enc.Bytes...)
	for i := range transformed {
		transformed[i] ^= 0xA5
	}
	transformedHash := ComputeBlockHash(transformed)
	if bytes.Equal(transformedHash, plainHash) {
		t.Fatal("expected transformed-bytes hash to differ from plaintext-encoded hash")
	}
}
func validOneChunkEncodedBlock() *EncodedBlock {
	return &EncodedBlock{
		Header: BlockHeader{
			Magic:         BlockMagicV1,
			Version:       BlockFormatVersionV1,
			Codec:         BlockCodecNoneV1,
			ChunkCount:    1,
			PlaintextSize: 5,
		},
		Entries:  []ChunkEntry{{ChunkID: 1, Offset: 0, Size: 5}},
		Payload:  []byte("hello"),
	}
}
func TestVerifyBlockHashValidPasses(t *testing.T) {
b := validOneChunkEncodedBlock()
encoded, err := EncodeBlock(b)
if err != nil {
t.Fatalf("encode: %v", err)
}
hash := ComputeBlockHash(encoded)
if err := VerifyBlockHash(encoded, hash); err != nil {
t.Fatalf("expected nil error, got %v", err)
}
}

func TestVerifyBlockHashWrongHashFails(t *testing.T) {
b := validOneChunkEncodedBlock()
encoded, err := EncodeBlock(b)
if err != nil {
t.Fatalf("encode: %v", err)
}
wrong := make([]byte, 32)
if err := VerifyBlockHash(encoded, wrong); !errors.Is(err, ErrBlockHashMismatch) {
t.Fatalf("expected ErrBlockHashMismatch, got %v", err)
}
}

func TestVerifyBlockHashNilOrEmptyExpectedFails(t *testing.T) {
b := validOneChunkEncodedBlock()
encoded, err := EncodeBlock(b)
if err != nil {
t.Fatalf("encode: %v", err)
}
for _, tc := range [][]byte{nil, {}} {
if err := VerifyBlockHash(encoded, tc); !errors.Is(err, ErrBlockHashExpectedNil) {
			t.Fatalf("expected ErrBlockHashExpectedNil, got %v", err)
		}
	}
}

func TestSliceChunkFromPayloadOverflowRejected(t *testing.T) {
	payload := []byte("hello")
	// Offset + Size wraps around uint64.
	overflowEntry := ChunkEntry{ChunkID: 1, Offset: ^uint64(0), Size: 2}
	if _, err := SliceChunkFromPayload(payload, overflowEntry); !errors.Is(err, ErrBlockFormatInvalidLayout) {
		t.Fatalf("expected ErrBlockFormatInvalidLayout on overflow, got %v", err)
	}
}
