package main

import (
	"compress/gzip"
	"io"
	"os"

	"github.com/klauspost/compress/zstd"
)

type CompressionType string

const (
	CompressionNone CompressionType = "none"
	CompressionGzip CompressionType = "gzip"
	CompressionZstd CompressionType = "zstd"
)

//
// ===================== COMPRESS =====================
//

func CompressFile(path string, algo CompressionType) (string, int64, error) {

	// No compression
	if algo == CompressionNone {
		info, err := os.Stat(path)
		if err != nil {
			return "", 0, err
		}
		return path, info.Size(), nil
	}

	input, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer input.Close()

	outputPath := path + "." + string(algo)

	output, err := os.Create(outputPath)
	if err != nil {
		return "", 0, err
	}
	defer output.Close()

	var writer io.WriteCloser

	switch algo {
	case CompressionGzip:
		writer = gzip.NewWriter(output)

	case CompressionZstd:
		encoder, err := zstd.NewWriter(output)
		if err != nil {
			return "", 0, err
		}
		writer = encoder

	default:
		return "", 0, nil
	}

	_, err = io.Copy(writer, input)
	if err != nil {
		writer.Close()
		return "", 0, err
	}

	writer.Close()

	// Remove original uncompressed container
	err = os.Remove(path)
	if err != nil {
		return "", 0, err
	}

	info, err := os.Stat(outputPath)
	if err != nil {
		return "", 0, err
	}

	return outputPath, info.Size(), nil
}

//
// ===================== DECOMPRESS =====================
//

// zstd wrapper to satisfy io.ReadCloser
type zstdReadCloser struct {
	decoder *zstd.Decoder
	file    *os.File
}

func (z *zstdReadCloser) Read(p []byte) (int, error) {
	return z.decoder.Read(p)
}

func (z *zstdReadCloser) Close() error {
	z.decoder.Close()
	return z.file.Close()
}

func OpenDecompressionReader(path string, algo CompressionType) (io.ReadCloser, error) {

	switch algo {

	case CompressionNone:
		return os.Open(path)

	case CompressionGzip:
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}

		gr, err := gzip.NewReader(f)
		if err != nil {
			f.Close()
			return nil, err
		}

		// Return wrapper so closing also closes file
		return struct {
			io.Reader
			io.Closer
		}{
			Reader: gr,
			Closer: f,
		}, nil

	case CompressionZstd:
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}

		decoder, err := zstd.NewReader(f)
		if err != nil {
			f.Close()
			return nil, err
		}

		return &zstdReadCloser{
			decoder: decoder,
			file:    f,
		}, nil

	default:
		return os.Open(path)
	}
}
