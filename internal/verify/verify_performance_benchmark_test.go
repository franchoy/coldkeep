package verify

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"math/rand"
	"runtime"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/db"
	storagecompression "github.com/franchoy/coldkeep/internal/storage/compression"
	_ "github.com/mattn/go-sqlite3"
)

type verifyBenchmarkCase struct {
	name             string
	encryptionCodec  blocks.Codec
	compressionCodec string
	envKeyHex        string
}

func openVerifyBenchmarkDB(b *testing.B) *sql.DB {
	b.Helper()

	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		b.Fatalf("open sqlite db: %v", err)
	}
	dbconn.SetMaxOpenConns(1)
	dbconn.SetMaxIdleConns(1)
	if err := db.RunMigrations(dbconn); err != nil {
		_ = dbconn.Close()
		b.Fatalf("run migrations: %v", err)
	}
	return dbconn
}

func seedVerifyBenchmarkRepository(
	b *testing.B,
	dbconn *sql.DB,
	containersDir string,
	encryptionCodec blocks.Codec,
	compressionCodec string,
	blockCount int,
	chunksPerBlock int,
	chunkSize int,
) (int64, int64, int64) {
	b.Helper()

	var totalLogicalBytes int64
	var totalPhysicalBytes int64
	var seededBlocks int64

	for blockIdx := 0; blockIdx < blockCount; blockIdx++ {
		chunkPayloads := make([][]byte, 0, chunksPerBlock)
		for chunkIdx := 0; chunkIdx < chunksPerBlock; chunkIdx++ {
			seed := int64(1000 + blockIdx*137 + chunkIdx*17)
			chunkPayloads = append(chunkPayloads, makeVerifyBenchmarkChunk(seed, chunkSize))
		}

		_, _ = seedVerifyCompressedPackedBlockFixture(
			b,
			dbconn,
			containersDir,
			chunkPayloads,
			encryptionCodec,
			compressionCodec,
		)
	}

	if err := dbconn.QueryRow(`
		SELECT
			COUNT(*),
			COALESCE(SUM(plaintext_size), 0),
			COALESCE(SUM(stored_size), 0)
		FROM storage_blocks
	`).Scan(&seededBlocks, &totalLogicalBytes, &totalPhysicalBytes); err != nil {
		b.Fatalf("query benchmark storage block totals: %v", err)
	}

	if seededBlocks <= 0 {
		b.Fatal("benchmark fixture seeded zero blocks")
	}
	if totalLogicalBytes <= 0 || totalPhysicalBytes <= 0 {
		b.Fatalf("invalid benchmark byte totals logical=%d physical=%d", totalLogicalBytes, totalPhysicalBytes)
	}

	return seededBlocks, totalLogicalBytes, totalPhysicalBytes
}

func makeVerifyBenchmarkChunk(seed int64, size int) []byte {
	rng := rand.New(rand.NewSource(seed))
	out := make([]byte, size)

	for i := 0; i < len(out); i++ {
		// Make data mostly structured/compressible with sparse deterministic noise
		// so compressed verify paths show meaningful physical-size differences.
		if i%64 == 0 {
			out[i] = byte(rng.Intn(256))
			continue
		}
		out[i] = byte('A' + ((i / 32) % 20))
	}
	return out
}

func bytesToMB(bytes int64) float64 {
	return float64(bytes) / (1024.0 * 1024.0)
}

func BenchmarkVerifyPerformanceSanity(b *testing.B) {
	cases := []verifyBenchmarkCase{
		{
			name:             "uncompressed_unencrypted",
			encryptionCodec:  blocks.CodecPlain,
			compressionCodec: storagecompression.CompressionNone,
		},
		{
			name:             "uncompressed_encrypted",
			encryptionCodec:  blocks.CodecAESGCM,
			compressionCodec: storagecompression.CompressionNone,
			envKeyHex:        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		},
		{
			name:             "compressed_unencrypted",
			encryptionCodec:  blocks.CodecPlain,
			compressionCodec: storagecompression.CompressionZstd,
		},
		{
			name:             "compressed_encrypted",
			encryptionCodec:  blocks.CodecAESGCM,
			compressionCodec: storagecompression.CompressionZstd,
			envKeyHex:        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			if tc.envKeyHex != "" {
				b.Setenv("COLDKEEP_KEY", tc.envKeyHex)
			}

			dbconn := openVerifyBenchmarkDB(b)
			defer func() { _ = dbconn.Close() }()

			containersDir := b.TempDir()
			const (
				blockCount     = 64
				chunksPerBlock = 4
				chunkSize      = 64 * 1024
			)
			seededBlocks, logicalBytesPerVerify, physicalBytesPerVerify := seedVerifyBenchmarkRepository(
				b,
				dbconn,
				containersDir,
				tc.encryptionCodec,
				tc.compressionCodec,
				blockCount,
				chunksPerBlock,
				chunkSize,
			)

			if err := verifyBlockPayloads(dbconn, containersDir); err != nil {
				b.Fatalf("warm verify before benchmark: %v", err)
			}

			oldLogWriter := log.Writer()
			log.SetOutput(io.Discard)
			defer log.SetOutput(oldLogWriter)

			b.ReportAllocs()
			b.SetBytes(logicalBytesPerVerify)

			var mem runtime.MemStats
			var heapPeakBytes uint64

			b.ResetTimer()
			started := time.Now()
			for i := 0; i < b.N; i++ {
				if err := verifyBlockPayloads(dbconn, containersDir); err != nil {
					b.Fatalf("verify benchmark iteration=%d: %v", i, err)
				}
				if i%16 == 0 {
					runtime.ReadMemStats(&mem)
					if mem.HeapInuse > heapPeakBytes {
						heapPeakBytes = mem.HeapInuse
					}
				}
			}
			elapsed := time.Since(started)
			b.StopTimer()

			runtime.ReadMemStats(&mem)
			if mem.HeapInuse > heapPeakBytes {
				heapPeakBytes = mem.HeapInuse
			}

			if elapsed > 0 {
				totalRuns := float64(b.N)
				seconds := elapsed.Seconds()
				b.ReportMetric((float64(seededBlocks)*totalRuns)/seconds, "blocks/s")
				b.ReportMetric(bytesToMB(logicalBytesPerVerify)*totalRuns/seconds, "logical_MB/s")
				b.ReportMetric(bytesToMB(physicalBytesPerVerify)*totalRuns/seconds, "physical_MB/s")
			}
			b.ReportMetric(float64(heapPeakBytes)/(1024.0*1024.0), "heap_peak_MB")

			b.Logf(
				"verify perf sanity case=%s blocks=%d logical_mb=%.2f physical_mb=%.2f",
				tc.name,
				seededBlocks,
				bytesToMB(logicalBytesPerVerify),
				bytesToMB(physicalBytesPerVerify),
			)
		})
	}
}

func TestVerifyPerformanceSanityFixtureNonEmpty(t *testing.T) {
	cases := []struct {
		name             string
		encryptionCodec  blocks.Codec
		compressionCodec string
	}{
		{name: "uncompressed_unencrypted", encryptionCodec: blocks.CodecPlain, compressionCodec: storagecompression.CompressionNone},
		{name: "uncompressed_encrypted", encryptionCodec: blocks.CodecAESGCM, compressionCodec: storagecompression.CompressionNone},
		{name: "compressed_unencrypted", encryptionCodec: blocks.CodecPlain, compressionCodec: storagecompression.CompressionZstd},
		{name: "compressed_encrypted", encryptionCodec: blocks.CodecAESGCM, compressionCodec: storagecompression.CompressionZstd},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if tc.encryptionCodec == blocks.CodecAESGCM {
				t.Setenv("COLDKEEP_KEY", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
			}

			dbconn := openVerifyTestDB(t)
			defer func() { _ = dbconn.Close() }()

			containersDir := t.TempDir()
			var totalBlocks int64
			var totalLogicalBytes int64
			var totalPhysicalBytes int64

			for blockIdx := 0; blockIdx < 4; blockIdx++ {
				chunkPayloads := [][]byte{
					makeVerifyBenchmarkChunk(int64(500+blockIdx*7), 8*1024),
					makeVerifyBenchmarkChunk(int64(600+blockIdx*11), 8*1024),
				}
				_, _ = seedVerifyCompressedPackedBlockFixture(t, dbconn, containersDir, chunkPayloads, tc.encryptionCodec, tc.compressionCodec)
			}

			if err := dbconn.QueryRow(`
				SELECT COUNT(*), COALESCE(SUM(plaintext_size), 0), COALESCE(SUM(stored_size), 0)
				FROM storage_blocks
			`).Scan(&totalBlocks, &totalLogicalBytes, &totalPhysicalBytes); err != nil {
				t.Fatalf("query fixture totals: %v", err)
			}

			if totalBlocks <= 0 {
				t.Fatalf("expected seeded blocks > 0 for case=%s", tc.name)
			}
			if totalLogicalBytes <= 0 || totalPhysicalBytes <= 0 {
				t.Fatalf("expected non-zero logical and physical bytes for case=%s logical=%d physical=%d", tc.name, totalLogicalBytes, totalPhysicalBytes)
			}

			if err := verifyBlockPayloads(dbconn, containersDir); err != nil {
				t.Fatalf("verify fixture must pass for case=%s: %v", tc.name, err)
			}

			t.Logf(
				"verify perf fixture %s: blocks=%d logical_mb=%.2f physical_mb=%.2f",
				tc.name,
				totalBlocks,
				bytesToMB(totalLogicalBytes),
				bytesToMB(totalPhysicalBytes),
			)
		})
	}
}

func TestVerifyPerformanceSanityCaseNaming(t *testing.T) {
	cases := []verifyBenchmarkCase{
		{name: "uncompressed_unencrypted", encryptionCodec: blocks.CodecPlain, compressionCodec: storagecompression.CompressionNone},
		{name: "uncompressed_encrypted", encryptionCodec: blocks.CodecAESGCM, compressionCodec: storagecompression.CompressionNone},
		{name: "compressed_unencrypted", encryptionCodec: blocks.CodecPlain, compressionCodec: storagecompression.CompressionZstd},
		{name: "compressed_encrypted", encryptionCodec: blocks.CodecAESGCM, compressionCodec: storagecompression.CompressionZstd},
	}

	seen := make(map[string]struct{}, len(cases))
	for _, tc := range cases {
		if tc.name == "" {
			t.Fatal("benchmark case name must be non-empty")
		}
		if _, ok := seen[tc.name]; ok {
			t.Fatalf("duplicate benchmark case name: %s", tc.name)
		}
		seen[tc.name] = struct{}{}

		compressionLabel := map[string]string{
			storagecompression.CompressionNone: "uncompressed",
			storagecompression.CompressionZstd: "compressed",
		}[tc.compressionCodec]
		if compressionLabel == "" {
			t.Fatalf("unexpected compression codec in benchmark case %s: %s", tc.name, tc.compressionCodec)
		}
		encryptionLabel := map[blocks.Codec]string{blocks.CodecPlain: "unencrypted", blocks.CodecAESGCM: "encrypted"}[tc.encryptionCodec]
		if encryptionLabel == "" {
			t.Fatalf("unexpected encryption codec in benchmark case %s: %s", tc.name, tc.encryptionCodec)
		}

		expected := fmt.Sprintf("%s_%s", compressionLabel, encryptionLabel)
		if tc.name != expected {
			t.Fatalf("benchmark case naming drift: got=%s want=%s", tc.name, expected)
		}
	}
}
