// Package gzip provides a compression Transform using standard-library gzip.
//
// GzipTransform implements the transforms.Transform interface and compresses
// block payloads using compress/gzip. It is inserted as transform[0] before
// any encryption transform so that the write path is:
//
//	logical block → compress → encrypt → persisted payload
//
// and the read path is:
//
//	persisted payload → decrypt → decompress → logical block
//
// The codec name "gzip" is used as the stable identifier stored in the
// storage_blocks.compression_codec column.
//
// The compression level is configurable. Level 0 (gzip.NoCompression)
// disables compression while keeping the code path active; it is recommended
// only for testing. The production default is gzip.DefaultCompression (-1).
package gzip

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
)

// Name is the stable string identifier used in storage metadata.
const Name = "gzip"

// Compression level constants re-exported from compress/gzip for caller convenience.
const (
	NoCompression      = gzip.NoCompression      // 0
	BestSpeed          = gzip.BestSpeed          // 1
	BestCompression    = gzip.BestCompression    // 9
	DefaultCompression = gzip.DefaultCompression // -1
	HuffmanOnly        = gzip.HuffmanOnly        // -2
)

// GzipTransform compresses/decompresses block payloads using gzip.
//
// Level controls the trade-off between speed and compression ratio.
// Valid values: NoCompression (0), BestSpeed (1) … BestCompression (9),
// DefaultCompression (-1), HuffmanOnly (-2).
type GzipTransform struct {
	// Level is the gzip compression level. Use DefaultCompression (-1) when
	// not sure; it is gzip's internal default (equivalent to level 6).
	Level int
}

// New returns a GzipTransform with the given compression level.
func New(level int) *GzipTransform {
	return &GzipTransform{Level: level}
}

// NewDefault returns a GzipTransform using gzip.DefaultCompression.
func NewDefault() *GzipTransform {
	return &GzipTransform{Level: DefaultCompression}
}

// Name returns the stable codec identifier "gzip".
func (t *GzipTransform) Name() string { return Name }

// Encode compresses input using gzip and returns the compressed bytes.
// Returns an error if the level is out of range or if the writer fails.
func (t *GzipTransform) Encode(input []byte) ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(len(input)/2 + 512) // pre-allocate a rough estimate

	w, err := gzip.NewWriterLevel(&buf, t.Level)
	if err != nil {
		return nil, fmt.Errorf("gzip: create writer (level=%d): %w", t.Level, err)
	}

	if _, err := w.Write(input); err != nil {
		return nil, fmt.Errorf("gzip: write: %w", err)
	}

	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("gzip: close writer: %w", err)
	}

	return buf.Bytes(), nil
}

// Decode decompresses a gzip-compressed block and returns the original bytes.
// Returns an error if the input is not valid gzip data.
func (t *GzipTransform) Decode(input []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(input))
	if err != nil {
		return nil, fmt.Errorf("gzip: create reader: %w", err)
	}
	defer func() { _ = r.Close() }()

	out, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("gzip: read: %w", err)
	}

	return out, nil
}
