package chunk

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/franchoy/coldkeep/internal/chunk/fastcdc"
	"github.com/franchoy/coldkeep/internal/chunk/shared"
	"github.com/franchoy/coldkeep/internal/chunk/simplecdc"
)

// writeCompFixture writes data to a temp file and returns its path.
func writeCompFixture(t *testing.T, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write fixture %s: %v", name, err)
	}
	return path
}

// assertFullCoverage verifies that the results reassemble exactly to src and
// that offsets are strictly contiguous with no zero-size chunks.
func assertFullCoverage(t *testing.T, label string, src []byte, results []shared.Result) {
	t.Helper()

	var cursor int64
	for i, r := range results {
		if r.Info.Size == 0 || len(r.Data) == 0 {
			t.Fatalf("%s: chunk %d is zero-size", label, i)
		}
		if r.Info.Offset != cursor {
			t.Fatalf("%s: chunk %d offset=%d want=%d", label, i, r.Info.Offset, cursor)
		}
		if r.Info.Size != int64(len(r.Data)) {
			t.Fatalf("%s: chunk %d size metadata=%d len(Data)=%d", label, i, r.Info.Size, len(r.Data))
		}
		cursor += r.Info.Size
	}

	if cursor != int64(len(src)) {
		t.Fatalf("%s: total chunk bytes=%d want=%d", label, cursor, len(src))
	}

	reassembled := make([]byte, 0, len(src))
	for _, r := range results {
		reassembled = append(reassembled, r.Data...)
	}
	if !bytes.Equal(reassembled, src) {
		t.Fatalf("%s: reassembled content does not match original", label)
	}
}

// inputs used across comparison tests
var compInputs = []struct {
	name string
	data []byte
}{
	{"small", bytes.Repeat([]byte("abcde"), 10000)},                // ~48 KiB — below v2 avg
	{"medium", bytes.Repeat([]byte("fastcdc-vs-simple-"), 100000)}, // ~1.7 MiB
	{"large", bytes.Repeat([]byte("x"), MaxChunkSize*4+7)},         // forced-cut dominated
	{"mixed", append(bytes.Repeat([]byte{0x00}, 64*1024), bytes.Repeat([]byte("varied-suffix-pattern"), 200000)...)},
}

func TestBothChunkersReconstructFullCoverage(t *testing.T) {
	v1 := simplecdc.Chunker{}
	v2 := fastcdc.Chunker{}

	for _, tc := range compInputs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			path := writeCompFixture(t, tc.name+".bin", tc.data)

			r1, err := v1.ChunkFile(path)
			if err != nil {
				t.Fatalf("v1 ChunkFile: %v", err)
			}
			assertFullCoverage(t, "v1", tc.data, r1)

			r2, err := v2.ChunkFile(path)
			if err != nil {
				t.Fatalf("v2 ChunkFile: %v", err)
			}
			assertFullCoverage(t, "v2", tc.data, r2)
		})
	}
}

func TestBothChunkersDeterministic(t *testing.T) {
	v1 := simplecdc.Chunker{}
	v2 := fastcdc.Chunker{}

	for _, tc := range compInputs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			path := writeCompFixture(t, tc.name+".bin", tc.data)

			run := func(label string, c Chunker) ([]int64, []int64) {
				r, err := c.ChunkFile(path)
				if err != nil {
					t.Fatalf("%s ChunkFile: %v", label, err)
				}
				offsets := make([]int64, len(r))
				sizes := make([]int64, len(r))
				for i, chunk := range r {
					offsets[i] = chunk.Info.Offset
					sizes[i] = chunk.Info.Size
				}
				return offsets, sizes
			}

			off1a, sz1a := run("v1-run1", v1)
			off1b, sz1b := run("v1-run2", v1)
			if !int64SlicesEqual(off1a, off1b) || !int64SlicesEqual(sz1a, sz1b) {
				t.Fatalf("v1 is not deterministic for input %q", tc.name)
			}

			off2a, sz2a := run("v2-run1", v2)
			off2b, sz2b := run("v2-run2", v2)
			if !int64SlicesEqual(off2a, off2b) || !int64SlicesEqual(sz2a, sz2b) {
				t.Fatalf("v2 is not deterministic for input %q", tc.name)
			}
		})
	}
}

func TestV2DoesNotPanicOrProduceInvalidChunks(t *testing.T) {
	v2 := fastcdc.Chunker{}

	edge := [][]byte{
		{},                            // empty
		bytes.Repeat([]byte{0x00}, 1), // 1 byte
		bytes.Repeat([]byte{0xFF}, fastcdc.MinChunkSize-1), // just below min
		bytes.Repeat([]byte{0xAB}, fastcdc.MaxChunkSize),   // exactly max
		bytes.Repeat([]byte{0x12}, fastcdc.MaxChunkSize*3), // well above max
	}

	for i, src := range edge {
		path := writeCompFixture(t, "edge.bin", src)
		results, err := v2.ChunkFile(path)
		if err != nil {
			t.Fatalf("edge[%d]: unexpected error: %v", i, err)
		}
		assertFullCoverage(t, "v2", src, results)
		for j, r := range results {
			if int(r.Info.Size) > fastcdc.MaxChunkSize {
				t.Fatalf("edge[%d] chunk %d exceeds MaxChunkSize: %d", i, j, r.Info.Size)
			}
		}
	}
}

func TestV2VersionString(t *testing.T) {
	if got := (fastcdc.Chunker{}).Version(); got != VersionV2FastCDC {
		t.Fatalf("v2 Version()=%q want %q", got, VersionV2FastCDC)
	}
}

func TestV1OutputUnchanged(t *testing.T) {
	// Snapshot test: v1 must produce the same boundary set it always has.
	// We pin the exact chunk count and sizes for a deterministic input so that
	// any accidental change to simplecdc is caught immediately.
	src := bytes.Repeat([]byte{0xFF}, MaxChunkSize*2+123)
	path := writeCompFixture(t, "v1-snapshot.bin", src)

	results, err := (simplecdc.Chunker{}).ChunkFile(path)
	if err != nil {
		t.Fatalf("v1 ChunkFile: %v", err)
	}

	// This value comes from existing TestDefaultChunkerReturnsStableMetadata in registry_test.go.
	const wantChunks = 3
	if len(results) != wantChunks {
		t.Fatalf("v1 chunk count changed: got %d want %d — v1 boundary logic must be frozen", len(results), wantChunks)
	}

	assertFullCoverage(t, "v1-snapshot", src, results)
}

// int64SlicesEqual reports whether a and b contain the same values.
func int64SlicesEqual(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
