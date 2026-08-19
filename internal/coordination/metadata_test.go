package coordination

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestOwnerMetadataPublicationReadAndSensitiveFieldBoundary(t *testing.T) {
	prepared := mustPreparedControlNamespace(t)
	owner := mustMetadataOwner(t, prepared.Identity, OperationSnapshotCreate, testOwnerStart)

	if err := publishOwnerMetadata(prepared, owner); err != nil {
		t.Fatalf("publishOwnerMetadata: %v", err)
	}
	raw, err := os.ReadFile(prepared.OwnerMetadataPath)
	if err != nil {
		t.Fatalf("read published owner metadata: %v", err)
	}
	if !json.Valid(raw) {
		t.Fatalf("published owner metadata is not complete JSON: %s", raw)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("decode owner metadata fields: %v", err)
	}
	allowed := map[string]bool{
		"schema_version": true,
		"pid":            true,
		"operation":      true,
		"started_at":     true,
		"hostname":       true,
		"executable":     true,
		"version":        true,
		"identity_hash":  true,
		"mode":           true,
	}
	for field := range fields {
		if !allowed[field] {
			t.Fatalf("published owner metadata contains unexpected field %q: %s", field, raw)
		}
	}
	for _, field := range []string{"schema_version", "pid", "operation", "started_at", "version", "identity_hash", "mode"} {
		if _, exists := fields[field]; !exists {
			t.Fatalf("published owner metadata is missing field %q: %s", field, raw)
		}
	}
	for _, forbidden := range []string{
		prepared.Identity.CanonicalPath,
		"configured-secret-repository",
		"postgres://user:password@database.example/coldkeep",
		"secret-user-name",
		"--restore-destination=/private/output",
		"source_path",
		"working_directory",
	} {
		if bytes.Contains(raw, []byte(forbidden)) {
			t.Fatalf("published owner metadata contains sensitive value %q: %s", forbidden, raw)
		}
	}

	read, err := readOwnerMetadata(prepared)
	if err != nil {
		t.Fatalf("readOwnerMetadata: %v", err)
	}
	assertOwnerEqual(t, read, owner)
	if runtime.GOOS != "windows" {
		info, err := os.Stat(prepared.OwnerMetadataPath)
		if err != nil {
			t.Fatalf("stat owner metadata: %v", err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("owner metadata mode=%#o want=0600", info.Mode().Perm())
		}
	}
	assertOnlyOwnerMetadataArtifact(t, prepared)
}

func TestOwnerMetadataPublicationReplacesCompleteRecord(t *testing.T) {
	prepared := mustPreparedControlNamespace(t)
	first := mustMetadataOwner(t, prepared.Identity, OperationStore, testOwnerStart)
	second := mustMetadataOwner(t, prepared.Identity, OperationVerify, testOwnerStart.Add(time.Minute))

	if err := publishOwnerMetadata(prepared, first); err != nil {
		t.Fatalf("publish first owner: %v", err)
	}
	if err := publishOwnerMetadata(prepared, second); err != nil {
		t.Fatalf("publish second owner: %v", err)
	}
	read, err := readOwnerMetadata(prepared)
	if err != nil {
		t.Fatalf("read replacement owner: %v", err)
	}
	assertOwnerEqual(t, read, second)
	assertOnlyOwnerMetadataArtifact(t, prepared)
}

func TestOwnerMetadataPublicationRejectsMismatchedIdentity(t *testing.T) {
	prepared := mustPreparedControlNamespace(t)
	otherIdentity := mustIdentity(t, t.TempDir())
	owner := mustMetadataOwner(t, otherIdentity, OperationStore, testOwnerStart)

	if err := publishOwnerMetadata(prepared, owner); err == nil {
		t.Fatal("expected owner identity mismatch")
	}
	if _, err := os.Lstat(prepared.OwnerMetadataPath); !os.IsNotExist(err) {
		t.Fatalf("mismatched owner metadata was published, stat err=%v", err)
	}
}

func TestOwnerMetadataPublicationRejectsSymlinkDestination(t *testing.T) {
	prepared := mustPreparedControlNamespace(t)
	owner := mustMetadataOwner(t, prepared.Identity, OperationStore, testOwnerStart)
	outsidePath := filepath.Join(t.TempDir(), "outside-owner.json")
	outsideData := []byte("outside target must remain unchanged")
	if err := os.WriteFile(outsidePath, outsideData, 0o600); err != nil {
		t.Fatalf("write outside target: %v", err)
	}
	if err := os.Symlink(outsidePath, prepared.OwnerMetadataPath); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}

	err := publishOwnerMetadata(prepared, owner)
	if err == nil {
		t.Fatal("expected symlink destination rejection")
	}
	assertMetadataErrorIsNotOwnership(t, err)
	info, statErr := os.Lstat(prepared.OwnerMetadataPath)
	if statErr != nil {
		t.Fatalf("lstat rejected owner metadata symlink: %v", statErr)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("owner metadata destination mode=%v want symlink", info.Mode())
	}
	gotOutside, readErr := os.ReadFile(outsidePath)
	if readErr != nil {
		t.Fatalf("read outside target: %v", readErr)
	}
	if !bytes.Equal(gotOutside, outsideData) {
		t.Fatalf("outside target changed: got=%q want=%q", gotOutside, outsideData)
	}
	assertOnlyOwnerMetadataArtifact(t, prepared)
}

func TestOwnerMetadataPublicationRejectsDirectoryDestination(t *testing.T) {
	prepared := mustPreparedControlNamespace(t)
	owner := mustMetadataOwner(t, prepared.Identity, OperationStore, testOwnerStart)
	if err := os.Mkdir(prepared.OwnerMetadataPath, 0o700); err != nil {
		t.Fatalf("create owner metadata directory: %v", err)
	}

	err := publishOwnerMetadata(prepared, owner)
	if err == nil {
		t.Fatal("expected directory destination rejection")
	}
	assertMetadataErrorIsNotOwnership(t, err)
	info, statErr := os.Lstat(prepared.OwnerMetadataPath)
	if statErr != nil {
		t.Fatalf("lstat rejected owner metadata directory: %v", statErr)
	}
	if !info.IsDir() {
		t.Fatalf("owner metadata destination mode=%v want directory", info.Mode())
	}
	assertOnlyOwnerMetadataArtifact(t, prepared)
}

func TestOwnerMetadataReadMissingIsNonAuthoritative(t *testing.T) {
	prepared := mustPreparedControlNamespace(t)
	_, err := readOwnerMetadata(prepared)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("read missing metadata error=%v want fs.ErrNotExist", err)
	}
	assertMetadataErrorIsNotOwnership(t, err)
}

func TestOwnerMetadataReadRejectsMalformedAndUnsupportedData(t *testing.T) {
	prepared := mustPreparedControlNamespace(t)
	for name, data := range map[string][]byte{
		"malformed":          []byte(`{"schema_version":`),
		"unsupported-schema": validOwnerWithSchemaVersion(t, prepared.Identity, 2),
	} {
		t.Run(name, func(t *testing.T) {
			if err := os.WriteFile(prepared.OwnerMetadataPath, data, 0o600); err != nil {
				t.Fatalf("write owner metadata fixture: %v", err)
			}
			if _, err := readOwnerMetadata(prepared); err == nil {
				t.Fatal("expected diagnostic metadata error")
			} else {
				assertMetadataErrorIsNotOwnership(t, err)
			}
		})
	}
}

func TestOwnerMetadataReadRejectsOversizedRegularFile(t *testing.T) {
	prepared := mustPreparedControlNamespace(t)
	oversized := bytes.Repeat([]byte{'x'}, maxOwnerMetadataSize+1)
	if err := os.WriteFile(prepared.OwnerMetadataPath, oversized, 0o600); err != nil {
		t.Fatalf("write oversized owner metadata: %v", err)
	}

	_, err := readOwnerMetadata(prepared)
	if err == nil {
		t.Fatal("expected oversized owner metadata error")
	}
	assertMetadataErrorIsNotOwnership(t, err)
	info, statErr := os.Lstat(prepared.OwnerMetadataPath)
	if statErr != nil {
		t.Fatalf("oversized owner metadata was removed: %v", statErr)
	}
	if !info.Mode().IsRegular() {
		t.Fatalf("oversized owner metadata mode=%v want regular", info.Mode())
	}
}

func TestOwnerMetadataRemovalExistingAndAbsent(t *testing.T) {
	prepared := mustPreparedControlNamespace(t)
	owner := mustMetadataOwner(t, prepared.Identity, OperationGarbageCollect, testOwnerStart)
	if err := publishOwnerMetadata(prepared, owner); err != nil {
		t.Fatalf("publish owner: %v", err)
	}

	if err := removeOwnerMetadata(prepared); err != nil {
		t.Fatalf("remove existing owner metadata: %v", err)
	}
	if _, err := os.Lstat(prepared.OwnerMetadataPath); !os.IsNotExist(err) {
		t.Fatalf("owner metadata remains after removal, stat err=%v", err)
	}
	if err := removeOwnerMetadata(prepared); err != nil {
		t.Fatalf("remove absent owner metadata: %v", err)
	}
	if _, err := os.Lstat(prepared.LockArtifactPath); !os.IsNotExist(err) {
		t.Fatalf("metadata removal touched lock artifact, stat err=%v", err)
	}
}

func mustPreparedControlNamespace(t *testing.T) PreparedControlNamespace {
	t.Helper()
	prepared, err := PrepareControlNamespace(filepath.Join(t.TempDir(), "containers"))
	if err != nil {
		t.Fatalf("PrepareControlNamespace: %v", err)
	}
	return prepared
}

func mustMetadataOwner(t *testing.T, identity Identity, operation Operation, startedAt time.Time) Owner {
	t.Helper()
	owner, err := NewOwner(operation, identity, "1.13.11", startedAt)
	if err != nil {
		t.Fatalf("NewOwner: %v", err)
	}
	owner.Hostname = "host.example"
	owner.Executable = "coldkeep"
	return owner
}

func assertOwnerEqual(t *testing.T, got, want Owner) {
	t.Helper()
	if got.SchemaVersion != want.SchemaVersion ||
		got.PID != want.PID ||
		got.Operation != want.Operation ||
		!got.StartedAt.Equal(want.StartedAt) ||
		got.Hostname != want.Hostname ||
		got.Executable != want.Executable ||
		got.Version != want.Version ||
		got.IdentityHash != want.IdentityHash ||
		got.Mode != want.Mode {
		t.Fatalf("owner mismatch got=%+v want=%+v", got, want)
	}
}

func assertOnlyOwnerMetadataArtifact(t *testing.T, prepared PreparedControlNamespace) {
	t.Helper()
	entries, err := os.ReadDir(prepared.ControlDirectory)
	if err != nil {
		t.Fatalf("read control directory: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != OwnerMetadataName {
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Fatalf("unexpected control artifacts after publication: %s", strings.Join(names, ", "))
	}
}

func assertMetadataErrorIsNotOwnership(t *testing.T, err error) {
	t.Helper()
	for _, sentinel := range []error{
		ErrRepositoryBusy,
		ErrRepositoryLockUnsupported,
		ErrRepositoryIdentityInvalid,
		ErrNestedRepositoryAcquisition,
	} {
		if errors.Is(err, sentinel) {
			t.Fatalf("diagnostic metadata error impersonates ownership error %v: %v", sentinel, err)
		}
	}
}

func validOwnerWithSchemaVersion(t *testing.T, identity Identity, version int) []byte {
	t.Helper()
	owner := mustMetadataOwner(t, identity, OperationRestore, testOwnerStart)
	encoded, err := EncodeOwner(owner)
	if err != nil {
		t.Fatalf("EncodeOwner: %v", err)
	}
	updated := strings.Replace(string(encoded), `"schema_version":1`, `"schema_version":`+strconv.Itoa(version), 1)
	if updated == string(encoded) {
		t.Fatal("owner schema fixture did not change")
	}
	return []byte(updated)
}
