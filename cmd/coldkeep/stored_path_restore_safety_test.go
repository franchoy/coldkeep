package main

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/blocks"
	"github.com/franchoy/coldkeep/internal/container"
	dbpkg "github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/storage"

	_ "github.com/mattn/go-sqlite3"
)

type cliStoredPathRestoreFixture struct {
	db      *sql.DB
	storage storage.StorageContext
	stored  storage.StoreFileResult
	payload []byte
}

func newCLIStoredPathRestoreFixture(t *testing.T, payload []byte) cliStoredPathRestoreFixture {
	t.Helper()

	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	if err := dbpkg.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}
	containersDir := t.TempDir()
	sgctx := storage.StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn),
		ContainerDir: containersDir,
	}
	t.Cleanup(func() { _ = sgctx.Close() })

	inputPath := filepath.Join(t.TempDir(), "cli-stored-path-restore-source.bin")
	if err := os.WriteFile(inputPath, payload, 0o600); err != nil {
		t.Fatalf("write restore source: %v", err)
	}
	stored, err := storage.StoreFileWithStorageContextAndCodecResult(sgctx, inputPath, blocks.CodecPlain)
	if err != nil {
		t.Fatalf("store restore fixture: %v", err)
	}
	return cliStoredPathRestoreFixture{db: dbconn, storage: sgctx, stored: stored, payload: payload}
}

func installStoredPathRestoreRepo(t *testing.T, fixture cliStoredPathRestoreFixture) {
	t.Helper()

	originalLoad := loadDefaultStorageContextPhase
	t.Cleanup(func() { loadDefaultStorageContextPhase = originalLoad })
	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		return fixture.storage, nil
	}
}

func updateCLIStoredPathMapping(t *testing.T, fixture cliStoredPathRestoreFixture, storedPath string) {
	t.Helper()

	if _, err := fixture.db.Exec(`UPDATE physical_file SET path = $1 WHERE logical_file_id = $2`, storedPath, fixture.stored.FileID); err != nil {
		t.Fatalf("update stored path mapping: %v", err)
	}
}

func requireCLIPathAbsent(t *testing.T, path string) {
	t.Helper()

	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("expected path %q to be absent, stat=%v", path, err)
	}
}

func requireCLIFileBytes(t *testing.T, path string, want []byte) {
	t.Helper()

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %q: %v", path, err)
	}
	if string(got) != string(want) {
		t.Fatalf("bytes mismatch for %q: got=%q want=%q", path, string(got), string(want))
	}
}

func requireCLINoRestoreTempFiles(t *testing.T, dir string) {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir %q: %v", dir, err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".coldkeep-restore-") {
			t.Fatalf("unexpected restore temp file left behind: %q", filepath.Join(dir, entry.Name()))
		}
	}
}

func TestRunRestoreCommandStoredPathPrefixTraversalRejectionSafety(t *testing.T) {
	fixture := newCLIStoredPathRestoreFixture(t, []byte("cli-prefix-traversal"))
	installStoredPathRestoreRepo(t, fixture)
	updateCLIStoredPathMapping(t, fixture, "../escape.bin")

	prefixRoot := t.TempDir()
	outsidePath := filepath.Join(filepath.Dir(prefixRoot), "escape.bin")
	err := runRestoreCommand(parsedCommandLine{
		method: "restore",
		flags: map[string][]string{
			"stored-path": {"../escape.bin"},
			"mode":        {"prefix"},
			"destination": {prefixRoot},
		},
	}, outputModeText)
	if err == nil {
		t.Fatal("expected traversal restore to fail")
	}

	requireCLIPathAbsent(t, outsidePath)
	requireCLINoRestoreTempFiles(t, prefixRoot)
}

func TestRunRestoreCommandStoredPathRejectsSymlinkedPrefixRootSafety(t *testing.T) {
	fixture := newCLIStoredPathRestoreFixture(t, []byte("cli-prefix-symlink-root"))
	installStoredPathRestoreRepo(t, fixture)

	realRoot := t.TempDir()
	symlinkRoot := filepath.Join(t.TempDir(), "restore-root-link")
	if err := os.Symlink(realRoot, symlinkRoot); err != nil {
		t.Skipf("symlink unavailable on this platform/environment: %v", err)
	}

	err := runRestoreCommand(parsedCommandLine{
		method: "restore",
		flags: map[string][]string{
			"stored-path": {fixture.stored.Path},
			"mode":        {"prefix"},
			"destination": {symlinkRoot},
			"overwrite":   {""},
		},
	}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("expected symlink-root rejection, got %v", err)
	}

	requireCLINoRestoreTempFiles(t, realRoot)
}

func TestRunRestoreCommandStoredPathOverwriteFalsePreservesExistingDestination(t *testing.T) {
	fixture := newCLIStoredPathRestoreFixture(t, []byte("cli-overwrite-false"))
	installStoredPathRestoreRepo(t, fixture)

	overridePath := filepath.Join(t.TempDir(), "override-existing.bin")
	sentinel := []byte("existing-cli-sentinel")
	if err := os.WriteFile(overridePath, sentinel, 0o600); err != nil {
		t.Fatalf("write sentinel: %v", err)
	}

	err := runRestoreCommand(parsedCommandLine{
		method: "restore",
		flags: map[string][]string{
			"stored-path": {fixture.stored.Path},
			"mode":        {"override"},
			"destination": {overridePath},
		},
	}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "output file already exists") {
		t.Fatalf("expected overwrite=false conflict, got %v", err)
	}

	requireCLIFileBytes(t, overridePath, sentinel)
	requireCLINoRestoreTempFiles(t, filepath.Dir(overridePath))
}

func TestRunRestoreCommandStoredPathOverwriteTrueJSONParity(t *testing.T) {
	fixture := newCLIStoredPathRestoreFixture(t, []byte("cli-overwrite-true"))
	installStoredPathRestoreRepo(t, fixture)

	overridePath := filepath.Join(t.TempDir(), "override-out.bin")
	if err := os.WriteFile(overridePath, []byte("old-bytes"), 0o600); err != nil {
		t.Fatalf("write old bytes: %v", err)
	}

	output := captureStdout(t, func() {
		err := runRestoreCommand(parsedCommandLine{
			method: "restore",
			flags: map[string][]string{
				"stored-path": {fixture.stored.Path},
				"mode":        {"override"},
				"destination": {overridePath},
				"overwrite":   {""},
				"output":      {"json"},
			},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runRestoreCommand: %v", err)
		}
	})

	var payload map[string]any
	line := strings.TrimSpace(output)
	if err := json.Unmarshal([]byte(line), &payload); err != nil {
		t.Fatalf("decode JSON output: %v\n%s", err, output)
	}
	data := payload["data"].(map[string]any)
	if data["stored_path"] != fixture.stored.Path || data["output_path"] != overridePath {
		t.Fatalf("unexpected output data: %v", data)
	}
	if data["file_id"] != float64(fixture.stored.FileID) || data["restored_hash"] != fixture.stored.FileHash || data["mode"] != "override" {
		t.Fatalf("unexpected restore fields: %v", data)
	}
	if _, ok := data["perf_spans"].([]any); !ok {
		t.Fatalf("expected perf_spans array, got %T", data["perf_spans"])
	}

	requireCLIFileBytes(t, overridePath, fixture.payload)
	requireCLINoRestoreTempFiles(t, filepath.Dir(overridePath))
}

func TestRunRestoreCommandStoredPathMissingMappingCreatesNoOutput(t *testing.T) {
	fixture := newCLIStoredPathRestoreFixture(t, []byte("cli-missing-mapping"))
	installStoredPathRestoreRepo(t, fixture)

	overridePath := filepath.Join(t.TempDir(), "missing-mapping.bin")
	err := runRestoreCommand(parsedCommandLine{
		method: "restore",
		flags: map[string][]string{
			"stored-path": {"/missing/mapping.bin"},
			"mode":        {"override"},
			"destination": {overridePath},
			"overwrite":   {""},
		},
	}, outputModeText)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected missing mapping error, got %v", err)
	}

	requireCLIPathAbsent(t, overridePath)
	requireCLINoRestoreTempFiles(t, filepath.Dir(overridePath))
}

func TestRunRestoreCommandStoredPathNoMetadataParity(t *testing.T) {
	fixture := newCLIStoredPathRestoreFixture(t, []byte("cli-no-metadata"))
	installStoredPathRestoreRepo(t, fixture)

	if _, err := fixture.db.Exec(`UPDATE physical_file SET is_metadata_complete = FALSE, mode = NULL, mtime = NULL, uid = NULL, gid = NULL WHERE logical_file_id = $1`, fixture.stored.FileID); err != nil {
		t.Fatalf("mark metadata incomplete: %v", err)
	}

	overridePath := filepath.Join(t.TempDir(), "no-metadata.bin")
	output := captureStdout(t, func() {
		err := runRestoreCommand(parsedCommandLine{
			method: "restore",
			flags: map[string][]string{
				"stored-path": {fixture.stored.Path},
				"mode":        {"override"},
				"destination": {overridePath},
				"overwrite":   {""},
				"no-metadata": {""},
			},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runRestoreCommand: %v", err)
		}
	})

	for _, want := range []string{
		"File restored successfully: " + overridePath,
		"FileID: " + strconv.FormatInt(fixture.stored.FileID, 10),
		"SHA256: " + fixture.stored.FileHash,
		"Hint: " + doctorOperationalHint,
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, output)
		}
	}
	requireCLIFileBytes(t, overridePath, fixture.payload)
}
