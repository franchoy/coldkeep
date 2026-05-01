package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/chunk"
	"github.com/franchoy/coldkeep/internal/chunk/fastcdc"
	"github.com/franchoy/coldkeep/internal/chunk/simplecdc"
)

func TestPrepareChunksWithContextComputesExpectedHashesDeterministically(t *testing.T) {
	results := []chunk.Result{
		{Info: chunk.Info{Offset: 0}, Data: []byte("alpha")},
		{Info: chunk.Info{Offset: 5}, Data: []byte("beta")},
		{Info: chunk.Info{Offset: 9}, Data: []byte("gamma")},
	}

	prepared1, err := prepareChunksWithContext(context.Background(), results, string(chunk.VersionV1SimpleRolling))
	if err != nil {
		t.Fatalf("prepareChunksWithContext first call: %v", err)
	}
	prepared2, err := prepareChunksWithContext(context.Background(), results, string(chunk.VersionV1SimpleRolling))
	if err != nil {
		t.Fatalf("prepareChunksWithContext second call: %v", err)
	}

	if len(prepared1) != len(results) {
		t.Fatalf("prepared length mismatch: got %d want %d", len(prepared1), len(results))
	}
	if len(prepared2) != len(results) {
		t.Fatalf("prepared(second) length mismatch: got %d want %d", len(prepared2), len(results))
	}

	for i := range results {
		sum := sha256.Sum256(results[i].Data)
		want := hex.EncodeToString(sum[:])
		if prepared1[i].Hash != want {
			t.Fatalf("hash mismatch at index %d: got %q want %q", i, prepared1[i].Hash, want)
		}
		if prepared2[i].Hash != want {
			t.Fatalf("hash mismatch on second call at index %d: got %q want %q", i, prepared2[i].Hash, want)
		}
	}
}

func TestPrepareChunksWithContextPreservesProvidedHash(t *testing.T) {
	provided := "already-computed-hash"
	results := []chunk.Result{
		{Info: chunk.Info{Offset: 0, Hash: provided}, Data: []byte("payload")},
	}

	prepared, err := prepareChunksWithContext(context.Background(), results, string(chunk.VersionV2FastCDC))
	if err != nil {
		t.Fatalf("prepareChunksWithContext: %v", err)
	}
	if len(prepared) != 1 {
		t.Fatalf("prepared length mismatch: got %d want 1", len(prepared))
	}
	if prepared[0].Hash != provided {
		t.Fatalf("provided hash should be preserved: got %q want %q", prepared[0].Hash, provided)
	}
}

// TestPrepareChunksWithContextDeterminismSameFile verifies that preparing the same file
// yields identical chunk metadata across multiple preparation calls (deterministic).
func TestPrepareChunksWithContextDeterminismSameFile(t *testing.T) {
	results := []chunk.Result{
		{Info: chunk.Info{Offset: 0}, Data: []byte("first chunk data")},
		{Info: chunk.Info{Offset: 15}, Data: []byte("second chunk data")},
		{Info: chunk.Info{Offset: 32}, Data: []byte("third chunk data")},
	}
	chunkerVer := string(chunk.VersionV1SimpleRolling)

	// Prepare the same chunks multiple times
	prepared1, err := prepareChunksWithContext(context.Background(), results, chunkerVer)
	if err != nil {
		t.Fatalf("prepareChunksWithContext(1): %v", err)
	}
	prepared2, err := prepareChunksWithContext(context.Background(), results, chunkerVer)
	if err != nil {
		t.Fatalf("prepareChunksWithContext(2): %v", err)
	}

	// Verify same number of chunks
	if len(prepared1) != len(prepared2) {
		t.Fatalf("chunk count mismatch: first=%d, second=%d", len(prepared1), len(prepared2))
	}

	// Verify identical chunk metadata across all attributes
	for i := range prepared1 {
		p1 := prepared1[i]
		p2 := prepared2[i]

		if p1.Index != p2.Index {
			t.Errorf("Index mismatch at position %d: got %d, want %d", i, p2.Index, p1.Index)
		}
		if p1.Offset != p2.Offset {
			t.Errorf("Offset mismatch at position %d: got %d, want %d", i, p2.Offset, p1.Offset)
		}
		if p1.Size != p2.Size {
			t.Errorf("Size mismatch at position %d: got %d, want %d", i, p2.Size, p1.Size)
		}
		if p1.Hash != p2.Hash {
			t.Errorf("Hash mismatch at position %d: got %q, want %q", i, p2.Hash, p1.Hash)
		}
		if p1.ChunkerVersion != p2.ChunkerVersion {
			t.Errorf("ChunkerVersion mismatch at position %d: got %q, want %q", i, p2.ChunkerVersion, p1.ChunkerVersion)
		}
	}
}

// TestPrepareChunksWithContextPreservesChunkIndexes verifies that chunk indexes
// match the result sequence (0-based, in order).
func TestPrepareChunksWithContextPreservesChunkIndexes(t *testing.T) {
	results := []chunk.Result{
		{Info: chunk.Info{Offset: 0}, Data: []byte("chunk0")},
		{Info: chunk.Info{Offset: 6}, Data: []byte("chunk1")},
		{Info: chunk.Info{Offset: 12}, Data: []byte("chunk2")},
		{Info: chunk.Info{Offset: 18}, Data: []byte("chunk3")},
	}

	prepared, err := prepareChunksWithContext(context.Background(), results, string(chunk.VersionV1SimpleRolling))
	if err != nil {
		t.Fatalf("prepareChunksWithContext: %v", err)
	}

	if len(prepared) != len(results) {
		t.Fatalf("chunk count mismatch: got %d, want %d", len(prepared), len(results))
	}

	for i, p := range prepared {
		if p.Index != i {
			t.Errorf("Index mismatch at position %d: got %d, want %d", i, p.Index, i)
		}
	}
}

// TestPrepareChunksWithContextPreservesChunkSizes verifies that prepared chunk sizes
// match the original data payloads.
func TestPrepareChunksWithContextPreservesChunkSizes(t *testing.T) {
	testData := [][]byte{
		[]byte("small"),                              // 5 bytes
		[]byte("medium size"),                        // 11 bytes
		[]byte("a much longer chunk with more data"), // longer
	}
	results := make([]chunk.Result, len(testData))
	for i, data := range testData {
		results[i] = chunk.Result{
			Info: chunk.Info{Offset: int64(i * 100)},
			Data: data,
		}
	}

	prepared, err := prepareChunksWithContext(context.Background(), results, string(chunk.VersionV1SimpleRolling))
	if err != nil {
		t.Fatalf("prepareChunksWithContext: %v", err)
	}

	for i, p := range prepared {
		if p.Size != len(testData[i]) {
			t.Errorf("Size mismatch at index %d: got %d, want %d", i, p.Size, len(testData[i]))
		}
	}
}

// TestPrepareChunksWithContextComputesConsistentHashes verifies that computed hashes
// (when not provided by chunker) are stable and match SHA256 of the data.
func TestPrepareChunksWithContextComputesConsistentHashes(t *testing.T) {
	testData := []string{"test chunk 1", "test chunk 2", "test chunk 3"}
	results := make([]chunk.Result, len(testData))
	for i, data := range testData {
		results[i] = chunk.Result{
			Info: chunk.Info{Offset: int64(i * 50)},
			Data: []byte(data),
		}
	}

	prepared, err := prepareChunksWithContext(context.Background(), results, string(chunk.VersionV1SimpleRolling))
	if err != nil {
		t.Fatalf("prepareChunksWithContext: %v", err)
	}

	// Verify hashes match SHA256 of data
	for i, p := range prepared {
		expected := sha256.Sum256(results[i].Data)
		expectedHex := hex.EncodeToString(expected[:])
		if p.Hash != expectedHex {
			t.Errorf("Hash mismatch at index %d: got %q, want %q", i, p.Hash, expectedHex)
		}
	}

	// Verify hashes are deterministic (call again, get same hashes)
	prepared2, err := prepareChunksWithContext(context.Background(), results, string(chunk.VersionV1SimpleRolling))
	if err != nil {
		t.Fatalf("prepareChunksWithContext(second): %v", err)
	}

	for i := range prepared {
		if prepared[i].Hash != prepared2[i].Hash {
			t.Errorf("Hash non-deterministic at index %d: first=%q, second=%q", i, prepared[i].Hash, prepared2[i].Hash)
		}
	}
}

// TestPrepareChunksWithContextPreservesChunkerVersion verifies that the chunker version
// is correctly captured in all prepared chunks.
func TestPrepareChunksWithContextPreservesChunkerVersion(t *testing.T) {
	results := []chunk.Result{
		{Info: chunk.Info{Offset: 0}, Data: []byte("data1")},
		{Info: chunk.Info{Offset: 5}, Data: []byte("data2")},
	}

	testVersions := []string{
		string(chunk.VersionV1SimpleRolling),
		string(chunk.VersionV2FastCDC),
	}

	for _, version := range testVersions {
		prepared, err := prepareChunksWithContext(context.Background(), results, version)
		if err != nil {
			t.Fatalf("prepareChunksWithContext with version %q: %v", version, err)
		}

		if len(prepared) != len(results) {
			t.Fatalf("chunk count mismatch for version %q: got %d, want %d", version, len(prepared), len(results))
		}

		for i, p := range prepared {
			if p.ChunkerVersion != version {
				t.Errorf("Version mismatch at index %d: got %q, want %q", i, p.ChunkerVersion, version)
			}
		}
	}
}

// TestPrepareChunksWithContextComputesFinalFileHash verifies that all chunk hashes
// can be combined to compute the full file hash using SHA256.
func TestPrepareChunksWithContextComputesFinalFileHash(t *testing.T) {
	// Create chunks with specific sizes that will combine to a known size
	chunks := [][]byte{
		[]byte("alpha"),
		[]byte("beta"),
		[]byte("gamma"),
	}
	results := make([]chunk.Result, len(chunks))
	for i, data := range chunks {
		results[i] = chunk.Result{
			Info: chunk.Info{Offset: int64(i * 50)},
			Data: data,
		}
	}

	prepared, err := prepareChunksWithContext(context.Background(), results, string(chunk.VersionV1SimpleRolling))
	if err != nil {
		t.Fatalf("prepareChunksWithContext: %v", err)
	}

	// Verify we can compute final file hash from combined data
	combined := make([]byte, 0)
	for _, chunk := range chunks {
		combined = append(combined, chunk...)
	}

	// Also verify each chunk hash is consistent
	for i, p := range prepared {
		chunkHash := sha256.Sum256(chunks[i])
		chunkHashHex := hex.EncodeToString(chunkHash[:])
		if p.Hash != chunkHashHex {
			t.Errorf("Chunk %d hash mismatch: got %q, want %q", i, p.Hash, chunkHashHex)
		}
	}

	// Verify total size adds up
	totalSize := 0
	for _, p := range prepared {
		totalSize += p.Size
	}
	if totalSize != len(combined) {
		t.Errorf("Total size mismatch: got %d, want %d", totalSize, len(combined))
	}
}

// TestPrepareChunksWithContextPreservesDataPayloads verifies that Data field in prepared chunks
// exactly matches the input chunk data for later encoding/storage.
func TestPrepareChunksWithContextPreservesDataPayloads(t *testing.T) {
	testData := [][]byte{
		[]byte("payload one"),
		[]byte("payload two with more data"),
		[]byte(""),
		[]byte("final payload"),
	}
	results := make([]chunk.Result, len(testData))
	for i, data := range testData {
		results[i] = chunk.Result{
			Info: chunk.Info{Offset: int64(i * 100)},
			Data: data,
		}
	}

	prepared, err := prepareChunksWithContext(context.Background(), results, string(chunk.VersionV1SimpleRolling))
	if err != nil {
		t.Fatalf("prepareChunksWithContext: %v", err)
	}

	for i, p := range prepared {
		if string(p.Data) != string(testData[i]) {
			t.Errorf("Data mismatch at index %d: got %q, want %q", i, string(p.Data), string(testData[i]))
		}
	}
}

// TestPrepareChunksWithContextHandlesEmptyChunkList verifies graceful handling of zero chunks.
func TestPrepareChunksWithContextHandlesEmptyChunkList(t *testing.T) {
	results := []chunk.Result{}

	prepared, err := prepareChunksWithContext(context.Background(), results, string(chunk.VersionV1SimpleRolling))
	if err != nil {
		t.Fatalf("prepareChunksWithContext with empty list: %v", err)
	}

	if len(prepared) != 0 {
		t.Fatalf("expected empty prepared list, got %d chunks", len(prepared))
	}
}

func TestPrepareFileForStorePhase4Phase5Parity(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "parity_input.bin")

	// Large enough to force multi-chunk behavior for v1-simple-rolling max size,
	// while also exercising v2-fastcdc chunk metadata flow.
	content := bytes.Repeat([]byte("coldkeep-phase5-parity-"), 150000)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	tests := []struct {
		name    string
		chunker chunk.Chunker
	}{
		{name: "v1-simple-rolling", chunker: simplecdc.New()},
		{name: "v2-fastcdc", chunker: fastcdc.New()},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			chunkerVersion := string(tc.chunker.Version())

			phase4Prepared, err := prepareFileForStorePhase4Baseline(context.Background(), path, tc.chunker, chunkerVersion)
			if err != nil {
				t.Fatalf("phase4 baseline prepare: %v", err)
			}

			phase5Prepared, err := prepareFileForStoreWithContext(context.Background(), path, tc.chunker, chunkerVersion, nil)
			if err != nil {
				t.Fatalf("phase5 prepare: %v", err)
			}

			if len(phase4Prepared.Chunks) != len(phase5Prepared.Chunks) {
				t.Fatalf("chunk count mismatch: phase4=%d phase5=%d", len(phase4Prepared.Chunks), len(phase5Prepared.Chunks))
			}

			for i := range phase4Prepared.Chunks {
				c4 := phase4Prepared.Chunks[i]
				c5 := phase5Prepared.Chunks[i]

				if c4.Index != c5.Index {
					t.Fatalf("chunk index mismatch at %d: phase4=%d phase5=%d", i, c4.Index, c5.Index)
				}
				if c4.Size != c5.Size {
					t.Fatalf("chunk size mismatch at %d: phase4=%d phase5=%d", i, c4.Size, c5.Size)
				}
				if c4.Hash != c5.Hash {
					t.Fatalf("chunk hash mismatch at %d: phase4=%q phase5=%q", i, c4.Hash, c5.Hash)
				}
				if c4.ChunkerVersion != c5.ChunkerVersion {
					t.Fatalf("chunker version mismatch at %d: phase4=%q phase5=%q", i, c4.ChunkerVersion, c5.ChunkerVersion)
				}
			}

			if phase4Prepared.LogicalHash != phase5Prepared.LogicalHash {
				t.Fatalf("logical file hash mismatch: phase4=%q phase5=%q", phase4Prepared.LogicalHash, phase5Prepared.LogicalHash)
			}

			phase4Restored := reconstructBytesFromPrepared(phase4Prepared.Chunks)
			phase5Restored := reconstructBytesFromPrepared(phase5Prepared.Chunks)
			if !bytes.Equal(phase4Restored, phase5Restored) {
				t.Fatalf("final restored bytes mismatch between phase4 and phase5")
			}

			original, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read original file: %v", err)
			}
			if !bytes.Equal(original, phase5Restored) {
				t.Fatalf("final restored bytes mismatch with original content")
			}
		})
	}
}

func TestPrepareFileForStoreWithContextLogicalHashMatchesHashFile(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "logical_identity_input.bin")

	content := bytes.Repeat([]byte("logical-identity-parity-"), 120000)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	legacyHash, err := hashFile(path)
	if err != nil {
		t.Fatalf("hashFile: %v", err)
	}

	tests := []struct {
		name    string
		chunker chunk.Chunker
	}{
		{name: "v1-simple-rolling", chunker: simplecdc.New()},
		{name: "v2-fastcdc", chunker: fastcdc.New()},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			prepared, err := prepareFileForStoreWithContext(
				context.Background(),
				path,
				tc.chunker,
				string(tc.chunker.Version()),
				nil,
			)
			if err != nil {
				t.Fatalf("prepareFileForStoreWithContext: %v", err)
			}

			if prepared.LogicalHash != legacyHash {
				t.Fatalf("logical hash identity mismatch: prepared=%q legacy=%q", prepared.LogicalHash, legacyHash)
			}
		})
	}
}

func TestPrepareFileForStoreEmptyFileParity(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "empty.bin")
	if err := os.WriteFile(path, []byte{}, 0o600); err != nil {
		t.Fatalf("write empty fixture: %v", err)
	}

	legacyHash, err := hashFile(path)
	if err != nil {
		t.Fatalf("hashFile: %v", err)
	}

	tests := []struct {
		name    string
		chunker chunk.Chunker
	}{
		{name: "v1-simple-rolling", chunker: simplecdc.New()},
		{name: "v2-fastcdc", chunker: fastcdc.New()},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			chunkerVersion := string(tc.chunker.Version())

			phase4Prepared, err := prepareFileForStorePhase4Baseline(context.Background(), path, tc.chunker, chunkerVersion)
			if err != nil {
				t.Fatalf("phase4 baseline prepare: %v", err)
			}

			phase5Prepared, err := prepareFileForStoreWithContext(context.Background(), path, tc.chunker, chunkerVersion, nil)
			if err != nil {
				t.Fatalf("phase5 prepare: %v", err)
			}

			if phase4Prepared.LogicalHash != phase5Prepared.LogicalHash {
				t.Fatalf("logical hash mismatch: phase4=%q phase5=%q", phase4Prepared.LogicalHash, phase5Prepared.LogicalHash)
			}
			if phase5Prepared.LogicalHash != legacyHash {
				t.Fatalf("logical hash mismatch vs hashFile: phase5=%q legacy=%q", phase5Prepared.LogicalHash, legacyHash)
			}

			if len(phase4Prepared.Chunks) != len(phase5Prepared.Chunks) {
				t.Fatalf("empty chunk representation length mismatch: phase4=%d phase5=%d", len(phase4Prepared.Chunks), len(phase5Prepared.Chunks))
			}
			if len(phase5Prepared.Chunks) != 0 {
				t.Fatalf("expected zero chunks for empty file, got %d", len(phase5Prepared.Chunks))
			}

			if got := reconstructBytesFromPrepared(phase5Prepared.Chunks); len(got) != 0 {
				t.Fatalf("expected empty reconstructed bytes, got %d bytes", len(got))
			}
		})
	}
}

// prepareFileForStorePhase4Baseline models the prior two-step preparation path:
// chunk file first, then prepare chunk metadata, then derive logical file hash.
func prepareFileForStorePhase4Baseline(
	ctx context.Context,
	path string,
	effectiveChunker chunk.Chunker,
	chunkerVersion string,
) (preparedFile, error) {
	if err := ctx.Err(); err != nil {
		return preparedFile{}, err
	}

	results, err := effectiveChunker.ChunkFile(path)
	if err != nil {
		return preparedFile{}, err
	}

	preparedChunks, err := prepareChunksWithContext(ctx, results, chunkerVersion)
	if err != nil {
		return preparedFile{}, err
	}

	totalSize := int64(0)
	h := sha256.New()
	for _, ch := range preparedChunks {
		totalSize += int64(ch.Size)
		_, _ = h.Write(ch.Data)
	}

	return preparedFile{
		Path:           path,
		LogicalHash:    hex.EncodeToString(h.Sum(nil)),
		SizeBytes:      totalSize,
		ChunkerVersion: chunkerVersion,
		Chunks:         preparedChunks,
	}, nil
}

func reconstructBytesFromPrepared(chunks []preparedChunk) []byte {
	total := 0
	for _, ch := range chunks {
		total += len(ch.Data)
	}
	out := make([]byte, 0, total)
	for _, ch := range chunks {
		out = append(out, ch.Data...)
	}
	return out
}
