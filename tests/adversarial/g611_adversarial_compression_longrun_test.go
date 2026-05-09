package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/storage"
	storagecompression "github.com/franchoy/coldkeep/internal/storage/compression"
	"github.com/franchoy/coldkeep/internal/verify"
	testutils "github.com/franchoy/coldkeep/tests/utils"
	"github.com/franchoy/coldkeep/tests/utils/testgate"
)

// Step 6.11 — Add Long-Run / Adversarial Compression Tests
//
// Compression-adversarial risks covered:
//   - memory leaks / unbounded growth under repeated loops
//   - decompressor edge cases and panic safety
//   - fragmentation/lifecycle stress in larger repositories
//   - corruption handling and partial-container truncation

type step611Stored struct {
	fileID int64
	hash   string
}

func TestStep611CompressionStressStoreRestoreVerifyCycles(t *testing.T) {
	testgate.RequireDB(t)

	for _, codec := range []blocks.Codec{blocks.CodecPlain, blocks.CodecAESGCM} {
		codec := codec
		t.Run(string(codec), func(t *testing.T) {
			dbconn, tmp, writer := setupStep611Env(t, codec)
			defer dbconn.Close()

			setCompressionStep611(t, dbconn, storagecompression.CompressionZstd)

			rng := rand.New(rand.NewSource(61101))
			stored := make([]step611Stored, 0, 72)

			// Large repository stress seed.
			for i := 0; i < 48; i++ {
				size := 220*1024 + (i%9)*41*1024
				var payload []byte
				if i%3 == 0 {
					payload = step611CompressiblePayload(size)
				} else {
					payload = step611PseudoRandomPayload(size, uint64(61100+i))
				}
				stored = append(stored, storeStep611(t, dbconn, writer, tmp, fmt.Sprintf("seed-%03d.bin", i), payload, codec))
			}

			for round := 0; round < 12; round++ {
				var payload []byte
				size := 300*1024 + (round%7)*57*1024
				if round%2 == 0 {
					payload = step611CompressiblePayload(size)
				} else {
					payload = step611PseudoRandomPayload(size, uint64(71000+round))
				}
				stored = append(stored, storeStep611(t, dbconn, writer, tmp, fmt.Sprintf("round-%02d.bin", round), payload, codec))

				for k := 0; k < 4; k++ {
					idx := rng.Intn(len(stored))
					assertRestoreNoPanicStep611(t, dbconn, tmp, stored[idx], fmt.Sprintf("round-%02d-restore-%02d.bin", round, k))
				}

				assertVerifyNoPanicStep611(t, func() error {
					return maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyFull)
				})

				if round%3 == 2 {
					if _, err := maintenance.RunGCWithContainersDirResult(true, container.ContainersDir); err != nil {
						t.Fatalf("gc dry-run round %d: %v", round, err)
					}
					if _, err := maintenance.RunGCWithContainersDirResult(false, container.ContainersDir); err != nil {
						t.Fatalf("gc real-run round %d: %v", round, err)
					}
				}
			}
		})
	}
}

func TestStep611CompressionCorruptionAndTruncationAlwaysDetectedSafely(t *testing.T) {
	testgate.RequireDB(t)

	for _, codec := range []blocks.Codec{blocks.CodecPlain, blocks.CodecAESGCM} {
		codec := codec
		t.Run(string(codec)+"-corruption", func(t *testing.T) {
			dbconn, tmp, writer := setupStep611Env(t, codec)
			defer dbconn.Close()

			setCompressionStep611(t, dbconn, storagecompression.CompressionZstd)

			for i := 0; i < 10; i++ {
				payload := step611PseudoRandomPayload(260*1024+i*31*1024, uint64(80000+i))
				_ = storeStep611(t, dbconn, writer, tmp, fmt.Sprintf("corrupt-seed-%02d.bin", i), payload, codec)
			}

			assertVerifyNoPanicStep611(t, func() error {
				return maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyFull)
			})

			for i := 0; i < 5; i++ {
				testutils.CorruptFirstCompletedChunkByte(t, dbconn, container.ContainersDir)
			}

			assertVerifyNoPanicStep611(t, func() error {
				err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyFull)
				if err == nil {
					t.Fatalf("expected verify full to fail after corruption injection")
				}
				return nil
			})
		})

		t.Run(string(codec)+"-truncation", func(t *testing.T) {
			dbconn, tmp, writer := setupStep611Env(t, codec)
			defer dbconn.Close()

			setCompressionStep611(t, dbconn, storagecompression.CompressionZstd)

			stored := make([]step611Stored, 0, 6)
			for i := 0; i < 6; i++ {
				payload := step611CompressiblePayload(340*1024 + i*44*1024)
				stored = append(stored, storeStep611(t, dbconn, writer, tmp, fmt.Sprintf("truncate-seed-%02d.bin", i), payload, codec))
			}

			_ = stored
			containerPath, truncatedSize := locateAndComputeTruncationStep611(t, dbconn, container.ContainersDir)
			if err := os.Truncate(containerPath, truncatedSize); err != nil {
				t.Fatalf("truncate container: %v", err)
			}

			assertVerifyNoPanicStep611(t, func() error {
				err := maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyFull)
				if err == nil {
					t.Fatalf("expected verify full to fail after partial container truncation")
				}
				return nil
			})
		})
	}
}

func TestStep611CompressionLongRunMemoryGrowthBounded(t *testing.T) {
	testgate.RequireDB(t)
	testgate.RequireLongRun(t)

	dbconn, tmp, writer := setupStep611Env(t, blocks.CodecPlain)
	defer dbconn.Close()

	setCompressionStep611(t, dbconn, storagecompression.CompressionZstd)

	runtime.GC()
	baseline := step611CurrentHeapAlloc()
	peak := baseline

	kept := make([]step611Stored, 0, 64)
	for round := 0; round < 60; round++ {
		var payload []byte
		size := 180*1024 + (round%11)*33*1024
		if round%4 == 0 {
			payload = step611CompressiblePayload(size)
		} else {
			payload = step611PseudoRandomPayload(size, uint64(92000+round))
		}

		stored := storeStep611(t, dbconn, writer, tmp, fmt.Sprintf("long-%03d.bin", round), payload, blocks.CodecPlain)
		kept = append(kept, stored)

		assertRestoreNoPanicStep611(t, dbconn, tmp, stored, fmt.Sprintf("long-restore-%03d.bin", round))

		if round%5 == 0 {
			assertVerifyNoPanicStep611(t, func() error {
				return maintenance.VerifyCommandWithContainersDir(container.ContainersDir, "system", 0, verify.VerifyFull)
			})
		}
		if round%6 == 5 {
			if _, err := maintenance.RunGCWithContainersDirResult(false, container.ContainersDir); err != nil {
				t.Fatalf("long-run gc round %d: %v", round, err)
			}
		}

		if round%4 == 3 {
			runtime.GC()
			now := step611CurrentHeapAlloc()
			if now > peak {
				peak = now
			}
		}
	}

	runtime.GC()
	finalAlloc := step611CurrentHeapAlloc()
	if finalAlloc > peak {
		peak = finalAlloc
	}

	limit := int64(math.Max(float64(baseline*6), float64(baseline+192*1024*1024)))
	if peak > limit {
		t.Fatalf("observed unbounded memory growth risk: baseline=%d peak=%d limit=%d", baseline, peak, limit)
	}
	if finalAlloc > baseline*4+96*1024*1024 {
		t.Fatalf("final heap did not converge after long run: baseline=%d final=%d", baseline, finalAlloc)
	}
}

func setupStep611Env(t *testing.T, codec blocks.Codec) (*sql.DB, string, container.ContainerWriter) {
	t.Helper()

	tmp := t.TempDir()
	origContainers := container.ContainersDir
	container.ContainersDir = filepath.Join(tmp, "containers")
	t.Cleanup(func() { container.ContainersDir = origContainers })

	if err := os.MkdirAll(container.ContainersDir, 0o755); err != nil {
		t.Fatalf("mkdir containers: %v", err)
	}
	t.Setenv("COLDKEEP_STORAGE_DIR", container.ContainersDir)
	testutils.ResetStorage(t)

	if codec == blocks.CodecAESGCM {
		testutils.SetTestAESGCMKey(t)
	} else {
		t.Setenv("COLDKEEP_KEY", "")
	}

	dbconn, err := db.ConnectDB()
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	testutils.ApplySchema(t, dbconn)
	testutils.ResetDB(t, dbconn)

	writer := container.NewLocalWriterWithDirAndDB(container.ContainersDir, container.GetContainerMaxSize(), dbconn)
	return dbconn, tmp, writer
}

func setCompressionStep611(t *testing.T, dbconn *sql.DB, compression string) {
	t.Helper()

	t.Setenv("COLDKEEP_COMPRESSION", compression)
	if compression == storagecompression.CompressionZstd {
		t.Setenv("COLDKEEP_COMPRESSION_LEVEL", "3")
	}

	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin compression tx: %v", err)
	}
	if err := storage.SetDefaultCompression(tx, compression); err != nil {
		t.Fatalf("set default compression: %v", err)
	}
	if compression == storagecompression.CompressionZstd {
		if err := storage.SetDefaultCompressionLevel(tx, 3); err != nil {
			t.Fatalf("set default compression level: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit compression tx: %v", err)
	}
}

func storeStep611(
	t *testing.T,
	dbconn *sql.DB,
	writer container.ContainerWriter,
	tmp string,
	name string,
	payload []byte,
	codec blocks.Codec,
) step611Stored {
	t.Helper()

	path := filepath.Join(tmp, name)
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("write payload %s: %v", name, err)
	}

	sgctx := storage.StorageContext{
		DB:           dbconn,
		Writer:       writer,
		ContainerDir: container.ContainersDir,
		Chunker:      chunk.DefaultChunker(),
	}

	res, err := storage.StoreFileWithStorageContextAndCodecResult(sgctx, path, codec)
	if err != nil {
		t.Fatalf("store %s: %v", name, err)
	}

	return step611Stored{fileID: res.FileID, hash: step611SHA256Hex(payload)}
}

func assertRestoreNoPanicStep611(t *testing.T, dbconn *sql.DB, tmp string, f step611Stored, outName string) {
	t.Helper()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("restore panicked: %v", r)
		}
	}()

	outPath := filepath.Join(tmp, outName)
	if err := storage.RestoreFileWithDB(dbconn, f.fileID, outPath); err != nil {
		t.Fatalf("restore file_id=%d: %v", f.fileID, err)
	}

	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read restored file_id=%d: %v", f.fileID, err)
	}
	if step611SHA256Hex(data) != f.hash {
		t.Fatalf("restored hash mismatch for file_id=%d", f.fileID)
	}
}

func assertVerifyNoPanicStep611(t *testing.T, fn func() error) {
	t.Helper()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("verify panicked: %v", r)
		}
	}()
	if err := fn(); err != nil {
		t.Fatalf("verify failed: %v", err)
	}
}

func locateAndComputeTruncationStep611(t *testing.T, dbconn *sql.DB, containersDir string) (string, int64) {
	t.Helper()

	var filename string
	var storedSize int64
	var blockOffset int64
	err := dbconn.QueryRow(`
		SELECT ctr.filename, b.stored_size, b.block_offset
		FROM chunk c
		JOIN blocks b ON b.chunk_id = c.id
		JOIN container ctr ON ctr.id = b.container_id
		WHERE c.status = 'COMPLETED'
		ORDER BY c.id ASC
		LIMIT 1
	`).Scan(&filename, &storedSize, &blockOffset)
	if err == sql.ErrNoRows {
		err = dbconn.QueryRow(`
			SELECT ctr.filename, sb.stored_size, sb.container_offset
			FROM chunk c
			JOIN chunk_block_refs r ON r.chunk_id = c.id
			JOIN storage_blocks sb ON sb.id = r.block_id
			JOIN container ctr ON ctr.id = sb.container_id
			WHERE c.status = 'COMPLETED'
			ORDER BY c.id ASC
			LIMIT 1
		`).Scan(&filename, &storedSize, &blockOffset)
	}
	if err != nil {
		t.Fatalf("query truncation target: %v", err)
	}

	containerPath := filepath.Join(containersDir, filename)
	info, err := os.Stat(containerPath)
	if err != nil {
		t.Fatalf("stat container for truncation: %v", err)
	}

	truncateBy := int64(64)
	if storedSize > 256 {
		truncateBy = 128
	}
	truncatedSize := info.Size() - truncateBy
	if truncatedSize <= blockOffset {
		truncatedSize = info.Size() - 16
	}
	if truncatedSize <= 0 {
		t.Fatalf("invalid truncation target size=%d for container=%s", truncatedSize, containerPath)
	}

	return containerPath, truncatedSize
}

func step611CurrentHeapAlloc() int64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.HeapAlloc)
}

func step611SHA256Hex(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func step611CompressiblePayload(size int) []byte {
	if size <= 0 {
		return nil
	}
	p := make([]byte, size)
	pat := []byte("step-611-compressible-pattern-")
	for i := range p {
		p[i] = pat[i%len(pat)]
	}
	return p
}

func step611PseudoRandomPayload(size int, seed uint64) []byte {
	if size <= 0 {
		return nil
	}
	p := make([]byte, size)
	x := seed ^ 0x9E3779B97F4A7C15
	for i := range p {
		x ^= x << 7
		x ^= x >> 9
		x ^= x << 8
		p[i] = byte(x)
	}
	return p
}
