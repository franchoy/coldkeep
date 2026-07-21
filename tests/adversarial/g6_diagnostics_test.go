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
			{
				Worker:         0,
				FileID:         1,
				LifecycleTrace: []string{"event=store_reuse_validation_failed file_id=1"},
				StartedUTC:     time.Date(2026, 7, 21, 5, 41, 0, 0, time.UTC),
				FinishedUTC:    time.Date(2026, 7, 21, 5, 41, 1, 0, time.UTC),
			},
			{Worker: 1, Error: "boom"},
		},
		PackedBlocks: []g6PackedBlockRecord{{
			BlockID:            7,
			PhysicalHash:       "expected-physical-hash",
			ActualPhysicalHash: "actual-physical-hash",
			Members:            []g6PackedBlockMember{{ChunkID: 3, ChunkHash: "abc123"}},
			EncodedMembers:     []g6EncodedBlockMember{{ChunkID: 3, Offset: 0, Size: 64}},
		}},
		PhysicalFiles: []g6PhysicalFileRecord{{ID: 9, Path: "/tmp/input", LogicalFileID: 1}},
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
	if len(manifest.StoreResults[0].LifecycleTrace) != 1 || !strings.Contains(manifest.StoreResults[0].LifecycleTrace[0], "store_reuse_validation_failed") {
		t.Fatalf("unexpected lifecycle trace: %+v", manifest.StoreResults[0].LifecycleTrace)
	}
	if manifest.StoreResults[0].StartedUTC.IsZero() || !manifest.StoreResults[0].FinishedUTC.After(manifest.StoreResults[0].StartedUTC) {
		t.Fatalf("unexpected store operation timestamps: %+v", manifest.StoreResults[0])
	}
	if len(manifest.PackedBlocks) != 1 || len(manifest.PackedBlocks[0].Members) != 1 || manifest.PackedBlocks[0].Members[0].ChunkID != 3 || len(manifest.PackedBlocks[0].EncodedMembers) != 1 {
		t.Fatalf("unexpected packed block diagnostics: %+v", manifest.PackedBlocks)
	}
	if len(manifest.PhysicalFiles) != 1 || manifest.PhysicalFiles[0].ID != 9 || manifest.PhysicalFiles[0].LogicalFileID != 1 {
		t.Fatalf("unexpected physical-file diagnostics: %+v", manifest.PhysicalFiles)
	}
}

func TestG6ChunkIDPatternAcceptsVerifierFormats(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input string
		want  string
	}{
		{name: "space", input: "chunk 3 has both mappings", want: "3"},
		{name: "equals", input: "encoded entry missing chunk=17 offset=0", want: "17"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			matches := g6ChunkIDPattern.FindStringSubmatch(tc.input)
			if len(matches) != 2 || matches[1] != tc.want {
				t.Fatalf("unexpected chunk match for %q: %v", tc.input, matches)
			}
		})
	}
}

func TestFilterG6LifecycleTraceKeepsAllowedEventsAndRedactsSecrets(t *testing.T) {
	env := map[string]string{
		"COLDKEEP_KEY": "secret-key",
		"DB_PASSWORD":  "secret-password",
	}
	output := strings.Join([]string{
		"ordinary CLI output secret-key",
		"2026/07/21 event=store_reuse_validation_failed file_id=1 error=secret-key",
		"2026/07/21 event=chunk_reuse_validation_failed chunk_id=3 error=secret-password",
		"2026/07/21 event=store_chunk_reclaim action=write_rebuild chunk_id=3",
		"unrelated event=restore_block_read action=start",
	}, "\n")

	trace := filterG6LifecycleTrace(output, env)
	if len(trace) != 3 {
		t.Fatalf("expected three lifecycle events, got %d: %v", len(trace), trace)
	}
	joined := strings.Join(trace, "\n")
	if containsSecretG6(joined) {
		t.Fatalf("lifecycle trace leaked secret material: %s", joined)
	}
	if !strings.Contains(joined, "[REDACTED]") || strings.Contains(joined, "restore_block_read") {
		t.Fatalf("unexpected filtered lifecycle trace: %s", joined)
	}
}

func TestAttachG6ActualPhysicalHashRejectsInvalidBounds(t *testing.T) {
	for _, tc := range []struct {
		name   string
		record g6PackedBlockRecord
	}{
		{
			name:   "negative size",
			record: g6PackedBlockRecord{StoredSize: -1},
		},
		{
			name:   "negative offset",
			record: g6PackedBlockRecord{ContainerOffset: -1},
		},
		{
			name: "past container maximum",
			record: g6PackedBlockRecord{
				ContainerOffset:  90,
				StoredSize:       11,
				ContainerMaxSize: 100,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record := tc.record
			attachG6ActualPhysicalHash(&record)
			if record.ActualPhysicalHashError == "" {
				t.Fatal("expected invalid bounds to be recorded as a diagnostic error")
			}
		})
	}
}

func containsSecretG6(s string) bool {
	return strings.Contains(s, "secret-password") || strings.Contains(s, "secret-key")
}
