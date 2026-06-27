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

// restoreDestinationPathFormCases drives TestRestoreCrossPlatformDestinationPathForms.
var restoreDestinationPathFormCases = []struct {
	name     string
	buildDst func(root string) string
	notes    string
}{
	{
		"nested relative destination",
		func(root string) string { return filepath.Join(root, "sub", "dir", "restored.bin") },
		"restore into a nested directory that does not yet exist",
	},
	{
		"deeply nested destination",
		func(root string) string { return filepath.Join(root, "a", "b", "c", "d", "restored.bin") },
		"restore into a deeply nested directory",
	},
	{
		"dot segment in destination cleaned by filepath.Join",
		func(root string) string {
			// filepath.Join resolves dot segments before the OS call.
			return filepath.Join(root, "x", "..", "y", "restored.bin")
		},
		"filepath.Join cleans dot segments; result is root/y/restored.bin on host",
	},
	{
		"repeated separator cleaned by filepath.Clean",
		func(root string) string {
			// filepath.Clean collapses double separators on the host OS.
			raw := root + string(os.PathSeparator) + string(os.PathSeparator) + "subdir" + string(os.PathSeparator) + "restored.bin"
			return filepath.Clean(raw)
		},
		"filepath.Clean collapses repeated separators on host",
	},
	{
		"path with underscore and digit names",
		func(root string) string { return filepath.Join(root, "restore_dir_01", "file_01.bin") },
		"underscore and digit names are portable across platforms",
	},
}

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

	for _, tc := range restoreDestinationPathFormCases {
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

			outRoot := realTempDir(t)
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

			got, err := os.ReadFile(outPath) // nosemgrep -- #nosec G304: outPath built from t.TempDir(); no user input
			if err != nil {
				t.Fatalf("read restored file %q: %v; notes: %s", outPath, err, tc.notes)
			}
			if !bytes.Equal(got, payload) {
				t.Fatalf("restored bytes mismatch: got %q want %q; notes: %s", got, payload, tc.notes)
			}
		})
	}
}

// restoreTraversalRejectedCases drives the rejection loop in
// TestRestoreCrossPlatformRejectsTraversalDestination.
var restoreTraversalRejectedCases = []struct {
	name  string
	rel   string
	notes string
}{
	{"parent traversal one level", "../escape.bin", "single-level traversal must not escape the restore root"},
	{"parent traversal multi level", "../../escape.bin", "multi-level traversal must not escape the restore root"},
	{"nested traversal", "subdir/../../escape.bin", "traversal inside nested path must not escape the restore root"},
	{"windows drive path as destination component", `C:\outside\escape.bin`, "Windows drive path must not be accepted as a safe relative destination component"},
	{"absolute destination component", "/etc/passwd", "absolute path must not be accepted as a relative destination component"},
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

	for _, tc := range restoreTraversalRejectedCases {
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

// restoreDeterministicPayloadCases drives TestRestoreCrossPlatformBytesRemainDeterministic.
var restoreDeterministicPayloadCases = []struct {
	name    string
	content []byte
	notes   string
}{
	{"short ascii payload", []byte("hello"), "single-word payload"},
	{"empty payload", []byte{}, "zero-byte file must restore with zero bytes"},
	{"binary payload", []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD}, "binary content with NUL bytes must survive round-trip intact"},
	{"newline only payload", []byte("\n"), "single newline payload; relevant for line-ending normalization checks"},
	{"mixed line endings payload", []byte("line1\r\nline2\nline3\r\n"), "mixed CRLF/LF must not be normalized during restore"},
	{"repeated pattern payload", bytes.Repeat([]byte("coldkeep"), 128), "repeated pattern to exercise multi-chunk boundary handling"},
}

// TestRestoreCrossPlatformBytesRemainDeterministic verifies that multiple
// store-and-restore round trips produce byte-identical output regardless of
// payload content or length.
//
// These tests validate host filesystem behavior.  They do not claim native
// Windows semantics until executed on Windows CI.
func TestRestoreCrossPlatformBytesRemainDeterministic(t *testing.T) {
	t.Parallel()

	for _, p := range restoreDeterministicPayloadCases {
		p := p
		t.Run(p.name, func(t *testing.T) {
			t.Parallel()

			repo := NewTestRepository(t)

			srcFile := filepath.Join(t.TempDir(), "source.bin")
			mustNoErr(t, os.WriteFile(filepath.Clean(srcFile), p.content, 0o600), "write source file")

			storeResult, err := StoreFileWithStorageContextAndCodecResult(repo.Storage, srcFile, blocks.CodecPlain)
			mustNoErr(t, err, "store file")

			outPath := filepath.Join(realTempDir(t), "restored.bin")
			_, err = RestoreFileWithStorageContextResultOptions(
				repo.Storage,
				storeResult.FileID,
				outPath,
				RestoreOptions{Overwrite: true},
			)
			mustNoErr(t, err, "restore file")

			got, err := os.ReadFile(outPath) // nosemgrep -- #nosec G304: outPath is filepath.Join(t.TempDir(), literal); no user input
			mustNoErr(t, err, "read restored file")

			if !bytes.Equal(got, p.content) {
				t.Fatalf("restored bytes mismatch: got len=%d want len=%d; notes: %s",
					len(got), len(p.content), p.notes)
			}
		})
	}
}
