package coordination

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const OwnerMetadataSchemaVersion = 1

// Owner is non-authoritative diagnostic metadata for a native lease.
// It intentionally contains no repository path, DSN, credentials, user name,
// command arguments, source path, or destination path.
type Owner struct {
	SchemaVersion int       `json:"schema_version"`
	PID           int       `json:"pid"`
	Operation     Operation `json:"operation"`
	StartedAt     time.Time `json:"started_at"`
	Hostname      string    `json:"hostname,omitempty"`
	Executable    string    `json:"executable,omitempty"`
	Version       string    `json:"version"`
	IdentityHash  string    `json:"identity_hash"`
	Mode          Mode      `json:"mode"`
}

// NewOwner builds the diagnostic record for an exclusive acquisition.
func NewOwner(operation Operation, identity Identity, version string, startedAt time.Time) (Owner, error) {
	if err := ValidateIdentity(identity); err != nil {
		return Owner{}, err
	}
	if !isCanonicalOperation(operation) {
		return Owner{}, fmt.Errorf("coordination: unsupported owner operation %q", operation)
	}
	if strings.TrimSpace(version) == "" {
		return Owner{}, fmt.Errorf("coordination: owner version is required")
	}
	if startedAt.IsZero() {
		return Owner{}, fmt.Errorf("coordination: owner start time is required")
	}
	hostname, _ := os.Hostname()
	return Owner{
		SchemaVersion: OwnerMetadataSchemaVersion,
		PID:           os.Getpid(),
		Operation:     operation,
		StartedAt:     startedAt.UTC(),
		Hostname:      strings.TrimSpace(hostname),
		Executable:    filepath.Base(os.Args[0]),
		Version:       strings.TrimSpace(version),
		IdentityHash:  identity.Hash,
		Mode:          ModeExclusive,
	}, nil
}

// EncodeOwner validates and deterministically serializes diagnostic metadata.
func EncodeOwner(owner Owner) ([]byte, error) {
	if err := ValidateOwner(owner); err != nil {
		return nil, err
	}
	owner.StartedAt = owner.StartedAt.UTC()
	return json.Marshal(owner)
}

// DecodeOwner strictly decodes one diagnostic metadata object.
func DecodeOwner(data []byte) (Owner, error) {
	var owner Owner
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&owner); err != nil {
		return Owner{}, fmt.Errorf("coordination: decode owner metadata: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return Owner{}, fmt.Errorf("coordination: decode owner metadata: trailing JSON value")
		}
		return Owner{}, fmt.Errorf("coordination: decode owner metadata: %w", err)
	}
	if err := ValidateOwner(owner); err != nil {
		return Owner{}, err
	}
	owner.StartedAt = owner.StartedAt.UTC()
	return owner, nil
}

// ValidateOwner validates required, non-sensitive diagnostic fields.
func ValidateOwner(owner Owner) error {
	if owner.SchemaVersion != OwnerMetadataSchemaVersion {
		return fmt.Errorf("coordination: unsupported owner metadata schema %d", owner.SchemaVersion)
	}
	if owner.PID <= 0 {
		return fmt.Errorf("coordination: owner PID must be positive")
	}
	if !isCanonicalOperation(owner.Operation) {
		return fmt.Errorf("coordination: unsupported owner operation %q", owner.Operation)
	}
	if owner.StartedAt.IsZero() {
		return fmt.Errorf("coordination: owner start time is required")
	}
	if strings.TrimSpace(owner.Version) == "" {
		return fmt.Errorf("coordination: owner version is required")
	}
	if len(owner.IdentityHash) != sha256HexLength || !isLowerHex(owner.IdentityHash) {
		return fmt.Errorf("coordination: owner identity hash must be lowercase SHA-256")
	}
	if owner.Mode != ModeExclusive {
		return fmt.Errorf("coordination: owner mode must be %q", ModeExclusive)
	}
	if strings.ContainsAny(owner.Hostname, "\r\n") || strings.ContainsAny(owner.Executable, "\r\n") {
		return fmt.Errorf("coordination: owner metadata contains a line break")
	}
	if owner.Executable != "" && filepath.Base(owner.Executable) != owner.Executable {
		return fmt.Errorf("coordination: owner executable must be a basename")
	}
	return nil
}

const sha256HexLength = 64

func isLowerHex(value string) bool {
	for _, r := range value {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}
