package storage

// These tests validate host filesystem behavior and platform-sensitive path
// forms for the restore engine.
//
// They do not claim native Windows semantics until executed on Windows CI.

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/pathsafe"
)

// TestRestoreCrossPlatformDestinationPathForms verifies that restore
// succeeds and produces byte-identical output for platform-sensitive
// destination path forms.
//
// Destination paths are constructed with filepath.Join / filepath.Clean,
// which resolves dot segments and repeated separators on the host OS.
// This documents host semantics; true Windows semantics require Windows CI.
func TestRestoreCrossPlatformDestinationPathForms(t *testing.T) {
	t.Parallel()

	payload := []byte("coldkeep-phase3-crossplatform-restore-payload")

	cases := []struct {
		name     string
		buildDst func(root string) string
		notes    string
	}{
		{
			name: "nested relative destination",
			buildDst: func(root string) string {
				return filepath.Join(root, "sub", "dir", "restored.bin")
			},
			notes: "restore into a nested directory that does not yet exist",
		},
		{
			name: "deeply nested destination",
			buildDst: func(root string) string {
				return filepath.Join(root, "a", "b", "c", "d", "restored.bin")
			},
			notes: "restore into a deeply nested directory",
		},
		{
			name: "dot segment in destination cleaned by filepath.Join",
			buildDst: func(root string) string {
				// filepath.Join resolves dot segments before the OS call.
				// Result is root/y/restored.bin on host.
				return filepath.Join(root, "x", "..", "y", "restored.bin")
			},
			notes: "filepath.Join cleans dot segments; result is root/y/restored.bin on host",
		},
		{
			name: "repeated separator cleaned by filepath.Clean",
			buildDst: func(root string) string {
				// filepath.Clean collapses double separators on the host OS.
				raw := root + string(os.PathSeparator) + string(os.PathSeparator) + "subdir" + string(os.PathSeparator) + "restored.bin"
				return filepath.Clean(raw)
			},
			notes: "filepath.Clean collapses repeated separators on host",
		},
		{
			name: "path with underscore and digit names",
			buildDst: func(root string) string {
				return filepath.Join(root, "restore_dir_01", "file_01.bin")
			},
			notes: "underscore and digit names are portable across platforms",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Each subtest owns its own repository to avoid SQLite :memory:
			// connection-pool sharing across parallel subtests.
			repo := NewTestRepository(t)
			srcFile := filepath.Join(t.TempDir(), "source.bin")
			mustNoErr(t, os.WriteFile(filepath.Clean(srcFile), payload, 0o600), "write source file")
			storeResult, err := StoreFileWithStorageContextAndCodecResult(repo.Storage, srcFile, blocks.CodecPlain)
			mustNoErr(t, err, "store file")

			outRoot := t.TempDir()
			outPath := tc.buildDst(outRoot)

			_, err = RestoreFileWithStorageContextResultOptions(
				repo.Storage,
				storeResult.FileID,
				outPath,
				RestoreOptions{Overwrite: true},
			)
			if err != nil {
				t.Fatalf("restore failed: %v; notes: %s", err, tc.notes)
			}

			got, err := os.ReadFile(outPath)
			if err != nil {
				t.Fatalf("read restored file %q: %v; notes: %s", outPath, err, tc.notes)
			}
			if !bytes.Equal(got, payload) {
				t.Fatalf("restored bytes mismatch: got %q want %q; notes: %s", got, payload, tc.notes)
			}
		})
	}
}

// TestRestoreCrossPlatformRejectsTraversalDestination verifies that the
// pathsafe.SafeJoin guardrail — used by restore callers to build output
// paths under a trusted root — rejects traversal-containing paths before
// they reach the restore engine.
//
// The restore engine itself accepts any absolute output path; root
// containment is the caller's responsibility.  pathsafe.SafeJoin is the
// canonical mechanism for enforcing that containment.
//
// This test documents the safety contract on all host platforms.
func TestRestoreCrossPlatformRejectsTraversalDestination(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	traversalCases := []struct {
		name  string
		rel   string
		notes string
	}{
		{
			name:  "parent traversal one level",
			rel:   "../escape.bin",
			notes: "single-level traversal must not escape the restore root",
		},
		{
			name:  "parent traversal multi level",
			rel:   "../../escape.bin",
			notes: "multi-level traversal must not escape the restore root",
		},
		{
			name:  "nested traversal",
			rel:   "subdir/../../escape.bin",
			notes: "traversal inside nested path must not escape the restore root",
		},
		{
			name:  "windows drive path as destination component",
			rel:   `C:\outside\escape.bin`,
			notes: "Windows drive path must not be accepted as a safe relative destination component",
		},
		{
			name:  "absolute destination component",
			rel:   "/etc/passwd",
			notes: "absolute path must not be accepted as a relative destination component",
		},
	}

	for _, tc := range traversalCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := pathsafe.SafeJoin(root, tc.rel)
			if err == nil {
				t.Fatalf("pathsafe.SafeJoin accepted dangerous destination %q; notes: %s",
					tc.rel, tc.notes)
			}
		})
	}

	// Verify a safe relative path is accepted, confirming the guardrail
	// allows legitimate restore destinations.
	safeDst, err := pathsafe.SafeJoin(root, "safe/restore/output.bin")
	if err != nil {
		t.Fatalf("pathsafe.SafeJoin rejected safe destination: %v", err)
	}
	if !strings.HasPrefix(safeDst, root) {
		t.Fatalf("safe destination escaped root: root=%q dst=%q", root, safeDst)
	}
}

// TestRestoreCrossPlatformBytesRemainDeterministic verifies that multiple
// store-and-restore round trips produce byte-identical output regardless of
// payload content or length.
//
// These tests validate host filesystem behavior.  They do not claim native
// Windows semantics until executed on Windows CI.
func TestRestoreCrossPlatformBytesRemainDeterministic(t *testing.T) {
	t.Parallel()

	payloads := []struct {
		name    string
		content []byte
		notes   string
	}{
		{
			name:    "short ascii payload",
			content: []byte("hello"),
			notes:   "single-word payload",
		},
		{
			name:    "empty payload",
			content: []byte{},
			notes:   "zero-byte file must restore with zero bytes",
		},
		{
			name:    "binary payload",
			content: []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD},
			notes:   "binary content with NUL bytes must survive round-trip intact",
		},
		{
			name:    "newline only payload",
			content: []byte("\n"),
			notes:   "single newline payload; relevant for line-ending normalization checks",
		},
		{
			name:    "mixed line endings payload",
			content: []byte("line1\r\nline2\nline3\r\n"),
			notes:   "mixed CRLF/LF must not be normalized during restore",
		},
		{
			name:    "repeated pattern payload",
			content: bytes.Repeat([]byte("coldkeep"), 128),
			notes:   "repeated pattern to exercise multi-chunk boundary handling",
		},
	}

	for _, p := range payloads {
		p := p
		t.Run(p.name, func(t *testing.T) {
			t.Parallel()

			repo := NewTestRepository(t)

			srcFile := filepath.Join(t.TempDir(), "source.bin")
			mustNoErr(t, os.WriteFile(filepath.Clean(srcFile), p.content, 0o600), "write source file")

			storeResult, err := StoreFileWithStorageContextAndCodecResult(repo.Storage, srcFile, blocks.CodecPlain)
			mustNoErr(t, err, "store file")

			outPath := filepath.Join(t.TempDir(), "restored.bin")
			_, err = RestoreFileWithStorageContextResultOptions(
				repo.Storage,
				storeResult.FileID,
				outPath,
				RestoreOptions{Overwrite: true},
			)
			mustNoErr(t, err, "restore file")

			got, err := os.ReadFile(outPath) // #nosec G304 -- nosemgrep: outPath is filepath.Join(t.TempDir(), literal); no user input
			mustNoErr(t, err, "read restored file")

			if !bytes.Equal(got, p.content) {
				t.Fatalf("restored bytes mismatch: got len=%d want len=%d; notes: %s",
					len(got), len(p.content), p.notes)
			}
		})
	}
}
