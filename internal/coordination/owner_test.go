package coordination

import (
	"bytes"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOwnerMetadataRoundTripAndSensitiveFieldBoundary(t *testing.T) {
	identity := mustIdentity(t, t.TempDir())
	startedAt := time.Date(2026, time.July, 25, 12, 30, 45, 123, time.FixedZone("test", 2*60*60))
	owner, err := NewOwner(OperationSnapshotRestore, identity, "1.13.11", startedAt)
	if err != nil {
		t.Fatalf("NewOwner: %v", err)
	}
	encoded, err := EncodeOwner(owner)
	if err != nil {
		t.Fatalf("EncodeOwner: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatalf("decode field map: %v", err)
	}
	for _, name := range []string{
		"schema_version",
		"pid",
		"operation",
		"started_at",
		"version",
		"identity_hash",
		"mode",
	} {
		if _, ok := fields[name]; !ok {
			t.Fatalf("owner metadata missing stable field %q: %s", name, encoded)
		}
	}
	if !bytes.Contains(encoded, []byte(`"started_at":"2026-07-25T10:30:45.000000123Z"`)) {
		t.Fatalf("owner timestamp is not serialized in UTC: %s", encoded)
	}

	for _, forbidden := range []string{
		identity.CanonicalPath,
		"DB_PASSWORD",
		"postgres://",
		"source_path",
		"destination_path",
		"user_name",
	} {
		if forbidden != "" && bytes.Contains(encoded, []byte(forbidden)) {
			t.Fatalf("owner metadata contains forbidden value %q: %s", forbidden, encoded)
		}
	}

	decoded, err := DecodeOwner(encoded)
	if err != nil {
		t.Fatalf("DecodeOwner: %v", err)
	}
	if decoded.SchemaVersion != OwnerMetadataSchemaVersion ||
		decoded.PID != owner.PID ||
		decoded.Operation != OperationSnapshotRestore ||
		decoded.Mode != ModeExclusive ||
		decoded.Version != "1.13.11" ||
		decoded.IdentityHash != identity.Hash {
		t.Fatalf("unexpected decoded owner: %+v", decoded)
	}
	if decoded.StartedAt.Location() != time.UTC || !decoded.StartedAt.Equal(startedAt) {
		t.Fatalf("owner start time not normalized to UTC: %v", decoded.StartedAt)
	}
}

func TestOwnerMetadataOptionalFieldsAndExecutableBasename(t *testing.T) {
	identity := mustIdentity(t, t.TempDir())
	owner, err := NewOwner(OperationVerify, identity, "1.13.11", testOwnerStart)
	if err != nil {
		t.Fatalf("NewOwner: %v", err)
	}
	owner.Hostname = ""
	owner.Executable = ""
	encoded, err := EncodeOwner(owner)
	if err != nil {
		t.Fatalf("EncodeOwner without optional fields: %v", err)
	}
	if bytes.Contains(encoded, []byte(`"hostname"`)) || bytes.Contains(encoded, []byte(`"executable"`)) {
		t.Fatalf("empty optional fields were serialized: %s", encoded)
	}

	owner.Executable = filepath.Join("private", "coldkeep")
	if _, err := EncodeOwner(owner); err == nil {
		t.Fatal("expected executable path to be rejected")
	}
}

func TestOwnerMetadataRejectsUnknownOperation(t *testing.T) {
	identity := mustIdentity(t, t.TempDir())
	if _, err := NewOwner(Operation("unknown"), identity, "1.13.11", testOwnerStart); err == nil {
		t.Fatal("expected unknown owner operation to fail")
	}
}

func TestOwnerMetadataRejectsMalformedAndUnknownFields(t *testing.T) {
	identity := mustIdentity(t, t.TempDir())
	owner, err := NewOwner(OperationVerify, identity, "1.13.11", time.Now())
	if err != nil {
		t.Fatalf("NewOwner: %v", err)
	}
	encoded, err := EncodeOwner(owner)
	if err != nil {
		t.Fatalf("EncodeOwner: %v", err)
	}

	withUnknown := strings.TrimSuffix(string(encoded), "}") + `,"repository_path":"/secret"}`
	if _, err := DecodeOwner([]byte(withUnknown)); err == nil {
		t.Fatal("expected unknown owner metadata field to fail")
	}
	if _, err := DecodeOwner([]byte(`{"schema_version":1}`)); err == nil {
		t.Fatal("expected incomplete owner metadata to fail")
	}
	unknownVersion := strings.Replace(string(encoded), `"schema_version":1`, `"schema_version":2`, 1)
	if _, err := DecodeOwner([]byte(unknownVersion)); err == nil {
		t.Fatal("expected unknown owner metadata version to fail")
	}
}

func TestCoordinationStableErrorSentinelsRemainDiscoverable(t *testing.T) {
	for _, sentinel := range []error{
		ErrRepositoryBusy,
		ErrRepositoryLockUnsupported,
		ErrRepositoryIdentityInvalid,
		ErrNestedRepositoryAcquisition,
	} {
		wrapped := errors.Join(errors.New("outer"), sentinel)
		if !errors.Is(wrapped, sentinel) {
			t.Fatalf("sentinel %v was not discoverable", sentinel)
		}
	}
}
