package testutils

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteDiagnosticJSONDisabledWithoutPreservation(t *testing.T) {
	t.Setenv(preserveFailureStateEnv, "")
	t.Setenv(diagnosticDirEnv, t.TempDir())

	path, err := WriteDiagnosticJSON("g6", map[string]any{"ok": true})
	if err != nil {
		t.Fatalf("WriteDiagnosticJSON returned error: %v", err)
	}
	if path != "" {
		t.Fatalf("expected no manifest path when preservation disabled, got %q", path)
	}
}

func TestWriteDiagnosticJSONWritesManifestWhenEnabled(t *testing.T) {
	diagDir := t.TempDir()
	t.Setenv(preserveFailureStateEnv, "1")
	t.Setenv(diagnosticDirEnv, diagDir)
	t.Setenv("DB_PASSWORD", "secret-password")
	t.Setenv("COLDKEEP_KEY", "secret-key")

	path, err := WriteDiagnosticJSON("g6 same file", map[string]any{
		"test_name": "TestG6",
		"db_name":   "coldkeep_adversarial_g6_123",
	})
	if err != nil {
		t.Fatalf("WriteDiagnosticJSON returned error: %v", err)
	}
	if path == "" {
		t.Fatal("expected diagnostic path when preservation enabled")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if strings.Contains(string(data), "secret-password") || strings.Contains(string(data), "secret-key") {
		t.Fatalf("manifest leaked secrets: %s", data)
	}

	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatalf("unmarshal manifest: %v", err)
	}
	if payload["test_name"] != "TestG6" {
		t.Fatalf("unexpected manifest payload: %+v", payload)
	}
}

func TestWriteDiagnosticJSONFailsClearlyForFileDiagnosticDir(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(filePath, []byte("x"), 0o600); err != nil {
		t.Fatalf("write diagnostic dir fixture: %v", err)
	}
	t.Setenv(preserveFailureStateEnv, "1")
	t.Setenv(diagnosticDirEnv, filePath)

	path, err := WriteDiagnosticJSON("g6", map[string]any{"ok": true})
	if err == nil || !strings.Contains(err.Error(), "is not a directory") {
		t.Fatalf("expected directory error, got path=%q err=%v", path, err)
	}
}

func TestWritePreservedIsolatedDBManifestWritesExpectedIdentifiers(t *testing.T) {
	diagDir := t.TempDir()
	t.Setenv(preserveFailureStateEnv, "1")
	t.Setenv(diagnosticDirEnv, diagDir)

	path, err := WritePreservedIsolatedDBManifest("adversarial", "coldkeep_adversarial_1")
	if err != nil {
		t.Fatalf("WritePreservedIsolatedDBManifest returned error: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}

	var payload PreservedIsolatedDBManifest
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatalf("unmarshal manifest: %v", err)
	}
	if payload.PackageLabel != "adversarial" || payload.DatabaseName != "coldkeep_adversarial_1" {
		t.Fatalf("unexpected preserved DB manifest: %+v", payload)
	}
}
