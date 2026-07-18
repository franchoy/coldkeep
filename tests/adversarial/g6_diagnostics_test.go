package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	testutils "github.com/franchoy/coldkeep/tests/utils"
)

func TestWriteConcurrentInvariantManifestG6DisabledWithoutPreservation(t *testing.T) {
	t.Setenv("COLDKEEP_TEST_PRESERVE_FAILURE_STATE", "")
	t.Setenv("COLDKEEP_TEST_DIAGNOSTIC_DIR", t.TempDir())

	writeConcurrentInvariantManifestG6(t, g6ChunkFailureDiagnosticManifest{
		Kind:         "g6_concurrent_store_failure",
		TestName:     "TestDisabled",
		TimestampUTC: time.Now().UTC(),
	})

	entries, err := os.ReadDir(testutils.DiagnosticDir())
	if err != nil {
		t.Fatalf("read diagnostic dir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected no manifest files when preservation disabled, got %d", len(entries))
	}
}

func TestWriteConcurrentInvariantManifestG6WritesExpectedIdentifiers(t *testing.T) {
	diagDir := t.TempDir()
	t.Setenv("COLDKEEP_TEST_PRESERVE_FAILURE_STATE", "1")
	t.Setenv("COLDKEEP_TEST_DIAGNOSTIC_DIR", diagDir)
	t.Setenv("DB_PASSWORD", "secret-password")
	t.Setenv("COLDKEEP_KEY", "secret-key")
	writeConcurrentInvariantManifestG6(t, expectedG6DiagnosticManifest())
	data := readSingleG6DiagnosticManifest(t, diagDir)
	assertSafeG6DiagnosticPayload(t, data)
	assertExpectedG6DiagnosticManifest(t, decodeG6DiagnosticManifest(t, data))
}

func expectedG6DiagnosticManifest() g6ChunkFailureDiagnosticManifest {
	return g6ChunkFailureDiagnosticManifest{
		Kind:                 "g6_concurrent_store_failure",
		TestName:             "TestAdversarialG6/plain",
		TimestampUTC:         time.Now().UTC(),
		Backend:              "postgres",
		OuterJobCodec:        "aes-gcm",
		InnerSubtestCodec:    "plain",
		IsolatedDatabaseName: "coldkeep_adversarial_g6_123",
		OffendingChunkHash:   "abc123",
		StoreResults: []g6StoreOperationResult{
			{Worker: 0, FileID: 1},
			{Worker: 1, Error: "boom"},
		},
	}
}

func readSingleG6DiagnosticManifest(t *testing.T, diagDir string) []byte {
	t.Helper()
	entries, err := os.ReadDir(diagDir)
	if err != nil {
		t.Fatalf("read diagnostic dir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected one manifest file, got %d", len(entries))
	}
	data, err := os.ReadFile(filepath.Join(diagDir, entries[0].Name()))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	return data
}

func assertSafeG6DiagnosticPayload(t *testing.T, data []byte) {
	t.Helper()
	if string(data) == "" {
		t.Fatal("expected non-empty manifest")
	}
	if json.Valid(data) == false {
		t.Fatalf("expected valid JSON manifest, got: %s", data)
	}
	if containsSecretG6(string(data)) {
		t.Fatalf("manifest leaked secret material: %s", data)
	}
}

func decodeG6DiagnosticManifest(t *testing.T, data []byte) g6ChunkFailureDiagnosticManifest {
	t.Helper()
	var manifest g6ChunkFailureDiagnosticManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatalf("unmarshal manifest: %v", err)
	}
	return manifest
}

func assertExpectedG6DiagnosticManifest(t *testing.T, manifest g6ChunkFailureDiagnosticManifest) {
	t.Helper()
	if manifest.TestName != "TestAdversarialG6/plain" || manifest.IsolatedDatabaseName != "coldkeep_adversarial_g6_123" {
		t.Fatalf("unexpected manifest identifiers: %+v", manifest)
	}
	if len(manifest.StoreResults) != 2 || manifest.StoreResults[1].Error != "boom" {
		t.Fatalf("unexpected store results: %+v", manifest.StoreResults)
	}
}

func containsSecretG6(s string) bool {
	return strings.Contains(s, "secret-password") || strings.Contains(s, "secret-key")
}
