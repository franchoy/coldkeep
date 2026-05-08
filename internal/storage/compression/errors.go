package compression

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrUnsupportedCompressionCodec = errors.New("unsupported compression codec")
	ErrCompressionFailed           = errors.New("compression failed")
	ErrDecompressionFailed         = errors.New("decompression failed")
	ErrCompressionSizeMismatch     = errors.New("decompression size mismatch")
)

// Backward-compatible alias kept from Step 3.1.
var ErrUnsupportedCompression = ErrUnsupportedCompressionCodec

// CompressionError carries storage-engine-safe diagnostics for compression
// operations. It never includes payload bytes.
type CompressionError struct {
	Kind         error
	BlockID      int64
	Codec        string
	ExpectedSize int64
	ActualSize   int64
	Cause        error
}

func (e *CompressionError) Error() string {
	parts := []string{fmt.Sprintf("%s", e.kindLabel())}
	parts = append(parts, fmt.Sprintf("block_id=%d", e.BlockID))
	parts = append(parts, fmt.Sprintf("compression_codec=%q", e.Codec))
	if e.ExpectedSize >= 0 {
		parts = append(parts, fmt.Sprintf("expected_size=%d", e.ExpectedSize))
	}
	if e.ActualSize >= 0 {
		parts = append(parts, fmt.Sprintf("actual_size=%d", e.ActualSize))
	}
	if e.Cause != nil {
		parts = append(parts, fmt.Sprintf("cause=%v", e.Cause))
	}
	return strings.Join(parts, " ")
}

func (e *CompressionError) Unwrap() error {
	if e.Cause == nil {
		return e.Kind
	}
	return errors.Join(e.Kind, e.Cause)
}

func (e *CompressionError) kindLabel() string {
	switch {
	case errors.Is(e.Kind, ErrUnsupportedCompressionCodec):
		return ErrUnsupportedCompressionCodec.Error()
	case errors.Is(e.Kind, ErrCompressionFailed):
		return ErrCompressionFailed.Error()
	case errors.Is(e.Kind, ErrDecompressionFailed):
		return ErrDecompressionFailed.Error()
	case errors.Is(e.Kind, ErrCompressionSizeMismatch):
		return ErrCompressionSizeMismatch.Error()
	default:
		if e.Kind != nil {
			return e.Kind.Error()
		}
		return "compression error"
	}
}

func newCompressionError(kind error, blockID int64, codec string, expectedSize, actualSize int64, cause error) error {
	return &CompressionError{
		Kind:         kind,
		BlockID:      blockID,
		Codec:        codec,
		ExpectedSize: expectedSize,
		ActualSize:   actualSize,
		Cause:        cause,
	}
}
