package testutils

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const diagnosticDirEnv = "COLDKEEP_TEST_DIAGNOSTIC_DIR"

var diagnosticNameSanitizer = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

type PreservedIsolatedDBManifest struct {
	Kind         string    `json:"kind"`
	TimestampUTC time.Time `json:"timestamp_utc"`
	PackageLabel string    `json:"package_label"`
	DatabaseName string    `json:"database_name"`
}

func DiagnosticDir() string {
	return strings.TrimSpace(os.Getenv(diagnosticDirEnv))
}

func DiagnosticManifestEnabled() bool {
	return PreserveFailureStateEnabled() && DiagnosticDir() != ""
}

func WriteDiagnosticJSON(prefix string, payload any) (string, error) {
	if !DiagnosticManifestEnabled() {
		return "", nil
	}
	safePrefix := sanitizeDiagnosticName(prefix)
	path, err := prepareDiagnosticManifestPath(safePrefix)
	if err != nil {
		return "", err
	}
	data, err := marshalDiagnosticPayload(safePrefix, payload)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return "", fmt.Errorf("write diagnostic manifest %s: %w", path, err)
	}
	return path, nil
}

func prepareDiagnosticManifestPath(safePrefix string) (string, error) {
	dir := DiagnosticDir()
	info, err := os.Stat(dir)
	switch {
	case err == nil && !info.IsDir():
		return "", fmt.Errorf("%s=%q is not a directory", diagnosticDirEnv, dir)
	case err != nil && !os.IsNotExist(err):
		return "", fmt.Errorf("stat %s=%q: %w", diagnosticDirEnv, dir, err)
	}

	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("mkdir %s=%q: %w", diagnosticDirEnv, dir, err)
	}
	filename := fmt.Sprintf("%s-%s.json", safePrefix, time.Now().UTC().Format("20060102T150405.000000000Z"))
	return filepath.Join(dir, filename), nil
}

func marshalDiagnosticPayload(safePrefix string, payload any) ([]byte, error) {
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal diagnostic payload %q: %w", safePrefix, err)
	}
	data = append(data, '\n')
	return data, nil
}

func WritePreservedIsolatedDBManifest(packageLabel, dbName string) (string, error) {
	return WriteDiagnosticJSON("isolated-postgres-db", PreservedIsolatedDBManifest{
		Kind:         "isolated_postgres_db",
		TimestampUTC: time.Now().UTC(),
		PackageLabel: packageLabel,
		DatabaseName: dbName,
	})
}

func sanitizeDiagnosticName(name string) string {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return "diagnostic"
	}
	safe := diagnosticNameSanitizer.ReplaceAllString(trimmed, "-")
	safe = strings.Trim(safe, "-.")
	if safe == "" {
		return "diagnostic"
	}
	return safe
}
