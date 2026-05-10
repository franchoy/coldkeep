package storage

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	storagecompression "github.com/franchoy/coldkeep/internal/storage/compression"
)

type orderingSpyCompressor struct {
	compressOut []byte
	compressIn  []byte
}

func (s *orderingSpyCompressor) Codec() string {
	return storagecompression.CompressionZstd
}

func (s *orderingSpyCompressor) Compress(input []byte) ([]byte, error) {
	s.compressIn = append([]byte(nil), input...)
	return append([]byte(nil), s.compressOut...), nil
}

func (s *orderingSpyCompressor) Decompress(_ []byte, _ int64) ([]byte, error) {
	return nil, errors.New("not used in write-path ordering test")
}

type orderingSpyTransformer struct {
	codec         blocks.Codec
	nonce         []byte
	encodeOut     []byte
	encodeInput   []byte
	decodeInvoked bool
}

func (s *orderingSpyTransformer) Encode(_ context.Context, in blocks.EncodeInput) (*blocks.TransformedBlock, error) {
	s.encodeInput = append([]byte(nil), in.Plaintext...)
	out := append([]byte(nil), s.encodeOut...)
	return &blocks.TransformedBlock{
		Descriptor: blocks.Descriptor{Codec: s.codec, Nonce: append([]byte(nil), s.nonce...)},
		Payload:    out,
	}, nil
}

func (s *orderingSpyTransformer) Decode(_ context.Context, _ blocks.DecodeInput) ([]byte, error) {
	s.decodeInvoked = true
	return nil, errors.New("not used in write-path ordering test")
}

func TestApplyPackedBlockTransformsWriteOrderCompressionThenEncryptionThenPhysicalHash(t *testing.T) {
	logical := []byte("logical-encoded-payload-bytes")
	logicalHash := blocks.HashLogical(logical)
	metadata := blocks.TransformMetadata{
		PayloadHash:      hex.EncodeToString(logicalHash),
		CompressionCodec: packedStorageBlockCodecNone,
		CompressionRatio: 1.0,
	}
	enc := packedBlockEncoded{
		plaintextEncoded: logical,
		blockHash:        logicalHash,
		metadata:         metadata,
	}

	compressor := &orderingSpyCompressor{compressOut: []byte("cmp")}
	nonce := bytes.Repeat([]byte{0xAB}, packedStorageBlockAESGCMNonceSize)
	transformer := &orderingSpyTransformer{
		codec:     blocks.CodecAESGCM,
		nonce:     nonce,
		encodeOut: []byte("ciphertext"),
	}
	level := 3

	tr, err := applyPackedBlockTransforms(
		context.Background(),
		transformer,
		storeRuntimeCompression{compressor: compressor, codec: storagecompression.CompressionZstd, level: &level},
		enc,
	)
	if err != nil {
		t.Fatalf("applyPackedBlockTransforms: %v", err)
	}

	if !bytes.Equal(compressor.compressIn, logical) {
		t.Fatalf("expected compressor input to be logical payload, got=%x want=%x", compressor.compressIn, logical)
	}
	if !bytes.Equal(transformer.encodeInput, compressor.compressOut) {
		t.Fatalf("expected encryption input to be compressed payload, got=%x want=%x", transformer.encodeInput, compressor.compressOut)
	}

	expectedCompressedHash := blocks.HashCompressed(compressor.compressOut)
	if !bytes.Equal(tr.compressedHash, expectedCompressedHash) {
		t.Fatalf("expected compressed_hash over pre-encryption payload")
	}

	expectedStored := append(append([]byte(nil), nonce...), transformer.encodeOut...)
	if !bytes.Equal(tr.storedPayload, expectedStored) {
		t.Fatalf("expected stored payload nonce||ciphertext, got=%x want=%x", tr.storedPayload, expectedStored)
	}
	expectedPhysicalHash := blocks.HashPhysical(expectedStored)
	if !bytes.Equal(tr.physicalHash, expectedPhysicalHash) {
		t.Fatalf("expected physical_hash over persisted payload")
	}

	if tr.storageCodec != string(blocks.CodecAESGCM) {
		t.Fatalf("expected storage codec %q, got %q", blocks.CodecAESGCM, tr.storageCodec)
	}
	if tr.compressionLvl == nil || *tr.compressionLvl != 3 {
		t.Fatalf("expected compression level 3, got %+v", tr.compressionLvl)
	}
	if tr.metadata.CompressionCodec != storagecompression.CompressionZstd {
		t.Fatalf("expected metadata compression codec zstd, got %q", tr.metadata.CompressionCodec)
	}
}

func TestApplyPackedBlockTransformsStoreIfSmallerFallbackCanonical(t *testing.T) {
	logical := []byte("logical-block-bytes")
	logicalHash := blocks.HashLogical(logical)
	enc := packedBlockEncoded{
		plaintextEncoded: logical,
		blockHash:        logicalHash,
		metadata: blocks.TransformMetadata{
			PayloadHash:      hex.EncodeToString(logicalHash),
			CompressionCodec: packedStorageBlockCodecNone,
			CompressionRatio: 1.0,
		},
	}

	compressor := &orderingSpyCompressor{compressOut: append([]byte(nil), logical...)}
	compressor.compressOut = append(compressor.compressOut, 0xFF) // force expansion
	transformer := &orderingSpyTransformer{codec: blocks.CodecPlain, encodeOut: logical}
	level := 3

	tr, err := applyPackedBlockTransforms(
		context.Background(),
		transformer,
		storeRuntimeCompression{compressor: compressor, codec: storagecompression.CompressionZstd, level: &level},
		enc,
	)
	if err != nil {
		t.Fatalf("applyPackedBlockTransforms: %v", err)
	}

	if !bytes.Equal(transformer.encodeInput, logical) {
		t.Fatalf("expected store-if-smaller fallback to pass logical bytes to encryption stage, got=%x want=%x", transformer.encodeInput, logical)
	}
	if tr.metadata.CompressionCodec != storagecompression.CompressionNone {
		t.Fatalf("expected fallback metadata codec=none, got %q", tr.metadata.CompressionCodec)
	}
	if tr.compressionLvl != nil {
		t.Fatalf("expected fallback compression level to be nil, got %+v", tr.compressionLvl)
	}
	if tr.compressedSize != int64(len(logical)) {
		t.Fatalf("expected fallback compressed_size to equal plaintext size, got compressed=%d plaintext=%d", tr.compressedSize, len(logical))
	}
	if !bytes.Equal(tr.compressedHash, blocks.HashCompressed(logical)) {
		t.Fatalf("expected compressed_hash to be computed from uncompressed logical bytes after fallback")
	}
}
