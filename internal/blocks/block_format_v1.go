package blocks

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	// CKBL in ASCII.
	BlockMagicV1 uint32 = 0x434B424C

	BlockFormatVersionV1 uint16 = 1
	BlockCodecNoneV1     uint16 = 0

	blockHeaderSizeV1 = 20
	chunkEntrySizeV1  = 24

	// BlockHashAlgorithm is the current project hash algorithm used for
	// plaintext encoded block hashing. v1.8 policy hashes encoded plaintext
	// block bytes (not encrypted bytes).
	BlockHashAlgorithm = "sha256"
)

var (
	ErrBlockFormatTooSmall      = errors.New("block format: payload too small for header")
	ErrBlockFormatUnsupported   = errors.New("block format: unsupported header values")
	ErrBlockFormatInvalidLayout = errors.New("block format: invalid table/payload layout")
	ErrBlockFormatInvalidCount  = errors.New("block format: invalid chunk_count")
	ErrBlockFormatEmptyBlock    = errors.New("block format: empty block is not allowed")
	ErrNilEncodedBlock          = errors.New("block format: nil encoded block")
)

// BlockHeader is the fixed-size binary block header for v1 format.
//
// Layout (20 bytes):
// - magic          uint32
// - version        uint16
// - codec          uint16
// - chunk_count    uint32
// - plaintext_size uint64
type BlockHeader struct {
	Magic         uint32
	Version       uint16
	Codec         uint16
	ChunkCount    uint32
	PlaintextSize uint64
}

// ChunkEntry is one chunk segment entry in the v1 chunk table.
//
// Layout (24 bytes):
// - chunk_id uint64
// - offset   uint64
// - size     uint64
type ChunkEntry struct {
	ChunkID uint64
	Offset  uint64
	Size    uint64
}

// EncodedBlock is the in-memory representation of a decoded/constructed
// plaintext block format payload in v1 layout.
type EncodedBlock struct {
	Header  BlockHeader
	Entries []ChunkEntry
	Payload []byte
}

// GetChunk returns one chunk slice by table index from the payload.
// Invalid indexes or invalid entry bounds return nil.
func (b *EncodedBlock) GetChunk(i int) []byte {
	if b == nil || i < 0 || i >= len(b.Entries) {
		return nil
	}
	entry := b.Entries[i]
	end := entry.Offset + entry.Size
	if end < entry.Offset || end > uint64(len(b.Payload)) {
		return nil
	}
	return b.Payload[entry.Offset:end]
}

// PackedChunk is an encode helper input for building payload and chunk table
// deterministically from an ordered chunk sequence.
type PackedChunk struct {
	ChunkID uint64
	Data    []byte
}

// PackedBlockV1 remains as a compatibility alias during Phase 2 rollout.
type PackedBlockV1 = EncodedBlock

// EncodedPackedBlockV1 is the result of encoding v1 block bytes.
// BlockHash is mandatory and computed from plaintext encoded block bytes.
type EncodedPackedBlockV1 struct {
	Bytes     []byte
	BlockHash []byte
	Header    BlockHeader
	Entries   []ChunkEntry
}

// EncodeBlock serializes one in-memory encoded block using deterministic v1
// binary layout with little-endian fields:
// | HEADER | CHUNK_TABLE | PAYLOAD |
func EncodeBlock(b *EncodedBlock) ([]byte, error) {
	if b == nil {
		return nil, ErrNilEncodedBlock
	}
	if b.Header.ChunkCount == 0 {
		return nil, ErrBlockFormatEmptyBlock
	}
	if b.Header.ChunkCount != uint32(len(b.Entries)) {
		return nil, ErrBlockFormatInvalidLayout
	}
	if b.Header.PlaintextSize != uint64(len(b.Payload)) {
		return nil, ErrBlockFormatInvalidLayout
	}
	if err := validateChunkEntries(b.Entries, b.Header.PlaintextSize); err != nil {
		return nil, err
	}

	tableSize := uint64(len(b.Entries)) * chunkEntrySizeV1
	encoded := make([]byte, int(uint64(blockHeaderSizeV1)+tableSize+b.Header.PlaintextSize))

	writeBlockHeader(encoded[:blockHeaderSizeV1], b.Header)

	offset := blockHeaderSizeV1
	for _, entry := range b.Entries {
		writeChunkEntry(encoded[offset:offset+chunkEntrySizeV1], entry)
		offset += chunkEntrySizeV1
	}

	copy(encoded[offset:], b.Payload)
	return encoded, nil
}

// ComputeBlockHash returns block hash over plaintext encoded block bytes.
//
// IMPORTANT: hash target is encoded plaintext block bytes, before encryption.
func ComputeBlockHash(encoded []byte) []byte {
	sum := sha256.Sum256(encoded)
	return sum[:]
}

// HashPlaintextEncodedBlock remains as compatibility alias.
func HashPlaintextEncodedBlock(encoded []byte) []byte {
	return ComputeBlockHash(encoded)
}

// EncodePackedBlockV1FromChunks builds a v1 encoded block from an ordered chunk
// sequence, creating chunk table segmentation and payload deterministically.
func EncodePackedBlockV1FromChunks(chunks []PackedChunk) (*EncodedPackedBlockV1, error) {
	if len(chunks) == 0 {
		return nil, ErrBlockFormatEmptyBlock
	}

	entries := make([]ChunkEntry, 0, len(chunks))
	payloadSize := uint64(0)

	for _, ch := range chunks {
		entry := ChunkEntry{
			ChunkID: ch.ChunkID,
			Offset:  payloadSize,
			Size:    uint64(len(ch.Data)),
		}
		entries = append(entries, entry)
		payloadSize += entry.Size
	}

	payload := make([]byte, 0, int(payloadSize))
	for _, ch := range chunks {
		payload = append(payload, ch.Data...)
	}

	return EncodePackedBlockV1(entries, payload)
}

// EncodePackedBlockV1 encodes block bytes using deterministic v1 layout:
// | HEADER | CHUNK_TABLE | PAYLOAD |
func EncodePackedBlockV1(entries []ChunkEntry, payload []byte) (*EncodedPackedBlockV1, error) {
	if len(entries) == 0 {
		return nil, ErrBlockFormatEmptyBlock
	}

	header := BlockHeader{
		Magic:         BlockMagicV1,
		Version:       BlockFormatVersionV1,
		Codec:         BlockCodecNoneV1,
		ChunkCount:    uint32(len(entries)),
		PlaintextSize: uint64(len(payload)),
	}

	encoded, err := EncodeBlock(&EncodedBlock{
		Header:  header,
		Entries: entries,
		Payload: payload,
	})
	if err != nil {
		return nil, err
	}

	return &EncodedPackedBlockV1{
		Bytes:     encoded,
		BlockHash: ComputeBlockHash(encoded),
		Header:    header,
		Entries:   append([]ChunkEntry(nil), entries...),
	}, nil
}

// DecodePackedBlockV1 parses and validates encoded v1 block bytes.
func DecodePackedBlockV1(encoded []byte) (*PackedBlockV1, error) {
	return DecodeBlock(encoded)
}

// DecodeBlock parses and validates v1 encoded block bytes into in-memory
// representation.
func DecodeBlock(data []byte) (*EncodedBlock, error) {
	if len(data) < blockHeaderSizeV1 {
		return nil, ErrBlockFormatTooSmall
	}

	header := readBlockHeader(data[:blockHeaderSizeV1])
	if header.Magic != BlockMagicV1 || header.Version != BlockFormatVersionV1 || header.Codec != BlockCodecNoneV1 {
		return nil, fmt.Errorf("%w: magic=%x version=%d codec=%d", ErrBlockFormatUnsupported, header.Magic, header.Version, header.Codec)
	}
	if header.ChunkCount == 0 {
		return nil, ErrBlockFormatInvalidCount
	}

	_, totalMin, err := validateChunkCountSane(header.ChunkCount, len(data))
	if err != nil {
		return nil, err
	}
	if uint64(len(data)) < totalMin {
		return nil, ErrBlockFormatInvalidLayout
	}

	payload := data[int(totalMin):]
	if uint64(len(payload)) != header.PlaintextSize {
		return nil, ErrBlockFormatInvalidLayout
	}

	entries := make([]ChunkEntry, int(header.ChunkCount))
	offset := blockHeaderSizeV1
	for i := 0; i < int(header.ChunkCount); i++ {
		entries[i] = readChunkEntry(data[offset : offset+chunkEntrySizeV1])
		offset += chunkEntrySizeV1
	}

	if err := validateChunkEntries(entries, header.PlaintextSize); err != nil {
		return nil, err
	}

	return &EncodedBlock{
		Header:  header,
		Entries: entries,
		Payload: payload,
	}, nil
}

func validateChunkCountSane(chunkCount uint32, totalDataLen int) (tableSize uint64, totalMin uint64, err error) {
	totalLen := uint64(totalDataLen)
	tableSize = uint64(chunkCount) * chunkEntrySizeV1
	totalMin = uint64(blockHeaderSizeV1) + tableSize

	if totalMin < uint64(blockHeaderSizeV1) || totalMin < tableSize {
		return 0, 0, ErrBlockFormatInvalidCount
	}
	if totalMin > totalLen {
		return 0, 0, ErrBlockFormatInvalidCount
	}

	maxEntriesByBytes := (totalLen - uint64(blockHeaderSizeV1)) / chunkEntrySizeV1
	if uint64(chunkCount) > maxEntriesByBytes {
		return 0, 0, ErrBlockFormatInvalidCount
	}

	return tableSize, totalMin, nil
}

// SliceChunkFromPayload returns chunk bytes for one table entry.
func SliceChunkFromPayload(payload []byte, entry ChunkEntry) ([]byte, error) {
	end := entry.Offset + entry.Size
	if end > uint64(len(payload)) {
		return nil, ErrBlockFormatInvalidLayout
	}
	return payload[entry.Offset:end], nil
}

func validateChunkEntries(entries []ChunkEntry, payloadSize uint64) error {
	expectedOffset := uint64(0)
	for _, entry := range entries {
		if entry.Size == 0 {
			return ErrBlockFormatInvalidLayout
		}
		if entry.Offset != expectedOffset {
			return ErrBlockFormatInvalidLayout
		}
		end := entry.Offset + entry.Size
		if end < entry.Offset || end > payloadSize {
			return ErrBlockFormatInvalidLayout
		}
		expectedOffset = end
	}

	if expectedOffset != payloadSize {
		return ErrBlockFormatInvalidLayout
	}
	return nil
}

func writeBlockHeader(dst []byte, h BlockHeader) {
	binary.LittleEndian.PutUint32(dst[0:4], h.Magic)
	binary.LittleEndian.PutUint16(dst[4:6], h.Version)
	binary.LittleEndian.PutUint16(dst[6:8], h.Codec)
	binary.LittleEndian.PutUint32(dst[8:12], h.ChunkCount)
	binary.LittleEndian.PutUint64(dst[12:20], h.PlaintextSize)
}

func readBlockHeader(src []byte) BlockHeader {
	return BlockHeader{
		Magic:         binary.LittleEndian.Uint32(src[0:4]),
		Version:       binary.LittleEndian.Uint16(src[4:6]),
		Codec:         binary.LittleEndian.Uint16(src[6:8]),
		ChunkCount:    binary.LittleEndian.Uint32(src[8:12]),
		PlaintextSize: binary.LittleEndian.Uint64(src[12:20]),
	}
}

func writeChunkEntry(dst []byte, e ChunkEntry) {
	binary.LittleEndian.PutUint64(dst[0:8], e.ChunkID)
	binary.LittleEndian.PutUint64(dst[8:16], e.Offset)
	binary.LittleEndian.PutUint64(dst[16:24], e.Size)
}

func readChunkEntry(src []byte) ChunkEntry {
	return ChunkEntry{
		ChunkID: binary.LittleEndian.Uint64(src[0:8]),
		Offset:  binary.LittleEndian.Uint64(src[8:16]),
		Size:    binary.LittleEndian.Uint64(src[16:24]),
	}
}
