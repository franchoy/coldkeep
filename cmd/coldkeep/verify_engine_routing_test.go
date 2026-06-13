package main

import (
	"database/sql"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/storage"
	"github.com/franchoy/coldkeep/internal/verify"
)

func TestVerifySystemEngineRoutingJSON(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)

	origLoad := loadDefaultStorageContextPhase
	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		return storage.StorageContext{DB: dbconn}, nil
	}
	t.Cleanup(func() { loadDefaultStorageContextPhase = origLoad })

	origVerify := verifyCommandPhase
	verifyCalled := false
	verifyCommandPhase = func(_ *sql.DB, target string, fileID int, level verify.VerifyLevel) error {
		verifyCalled = true
		if target != "system" {
			t.Fatalf("expected target system, got %q", target)
		}
		if fileID != 0 {
			t.Fatalf("expected fileID 0 for system verify, got %d", fileID)
		}
		if level != verify.VerifyFast {
			t.Fatalf("expected fast verify level, got %v", level)
		}
		return nil
	}
	t.Cleanup(func() { verifyCommandPhase = origVerify })

	origSummary := verifySummaryPhase
	summaryCalled := false
	verifySummaryPhase = func(_ *sql.DB, target string, fileID int64) (verifyOutputSummary, error) {
		summaryCalled = true
		if target != "system" {
			t.Fatalf("expected summary target system, got %q", target)
		}
		if fileID != 0 {
			t.Fatalf("expected summary fileID 0 for system verify, got %d", fileID)
		}
		return verifyOutputSummary{
			BlocksChecked:           10,
			PhysicalHashChecked:     7,
			CompressedHashChecked:   5,
			LogicalHashChecked:      10,
			CompressedBlocksChecked: 3,
		}, nil
	}
	t.Cleanup(func() { verifySummaryPhase = origSummary })

	output := captureStdout(t, func() {
		err := runVerifyCommand(parsedCommandLine{
			method:      "verify",
			positionals: []string{"system"},
			flags:       map[string][]string{"output": {"json"}, "fast": {""}},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runVerifyCommand: %v", err)
		}
	})

	if !verifyCalled {
		t.Fatalf("expected verifyCommandPhase to be called")
	}
	if !summaryCalled {
		t.Fatalf("expected verifySummaryPhase to be called")
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse JSON: %v output=%q", err, output)
	}
	if got := payload["command"]; got != "verify" {
		t.Fatalf("expected command=verify, got %v", got)
	}
	if got := payload["target"]; got != "system" {
		t.Fatalf("expected target system, got %v", got)
	}
	if got := payload["verify"]; got != "ok" {
		t.Fatalf("expected verify=ok, got %v", got)
	}
	if got := int(payload["blocks_checked"].(float64)); got != 10 {
		t.Fatalf("expected blocks_checked 10, got %d", got)
	}
}

func TestVerifySystemEngineRoutingText(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)

	origLoad := loadDefaultStorageContextPhase
	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		return storage.StorageContext{DB: dbconn}, nil
	}
	t.Cleanup(func() { loadDefaultStorageContextPhase = origLoad })

	origVerify := verifyCommandPhase
	verifyCalled := false
	verifyCommandPhase = func(_ *sql.DB, target string, fileID int, level verify.VerifyLevel) error {
		verifyCalled = true
		if target != "system" {
			t.Fatalf("expected target system, got %q", target)
		}
		if fileID != 0 {
			t.Fatalf("expected fileID 0 for system verify, got %d", fileID)
		}
		if level != verify.VerifyStandard {
			t.Fatalf("expected standard verify level, got %v", level)
		}
		return nil
	}
	t.Cleanup(func() { verifyCommandPhase = origVerify })

	origSummary := verifySummaryPhase
	summaryCalled := false
	verifySummaryPhase = func(_ *sql.DB, target string, fileID int64) (verifyOutputSummary, error) {
		summaryCalled = true
		if target != "system" {
			t.Fatalf("expected summary target system, got %q", target)
		}
		if fileID != 0 {
			t.Fatalf("expected summary fileID 0 for system verify, got %d", fileID)
		}
		return verifyOutputSummary{
			BlocksChecked:           12,
			PhysicalHashChecked:     8,
			CompressedHashChecked:   5,
			LogicalHashChecked:      12,
			CompressedBlocksChecked: 4,
		}, nil
	}
	t.Cleanup(func() { verifySummaryPhase = origSummary })

	output := captureStdout(t, func() {
		err := runVerifyCommand(parsedCommandLine{
			method:      "verify",
			positionals: []string{"system"},
			flags:       map[string][]string{},
		}, outputModeText)
		if err != nil {
			t.Fatalf("runVerifyCommand: %v", err)
		}
	})

	if !verifyCalled {
		t.Fatalf("expected verifyCommandPhase to be called")
	}
	if !summaryCalled {
		t.Fatalf("expected verifySummaryPhase to be called")
	}

	for _, want := range []string{
		"verify ok",
		"blocks_checked: 12",
		"physical_hash_checked: 8",
		"compressed_hash_checked: 5",
		"logical_hash_checked: 12",
		"compressed_blocks_checked: 4",
		"Hint: " + doctorOperationalHint,
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("expected verify text output to contain %q, got output:\n%s", want, output)
		}
	}
}

func TestVerifyFileEngineRoutingJSON(t *testing.T) {
	dbconn := openSnapshotRoutingDB(t)

	origLoad := loadDefaultStorageContextPhase
	loadDefaultStorageContextPhase = func() (storage.StorageContext, error) {
		return storage.StorageContext{DB: dbconn}, nil
	}
	t.Cleanup(func() { loadDefaultStorageContextPhase = origLoad })

	origVerify := verifyCommandPhase
	verifyCalled := false
	verifyCommandPhase = func(_ *sql.DB, target string, fileID int, level verify.VerifyLevel) error {
		verifyCalled = true
		if target != "file" {
			t.Fatalf("expected target file, got %q", target)
		}
		if fileID != 42 {
			t.Fatalf("expected fileID 42 for file verify, got %d", fileID)
		}
		if level != verify.VerifyStandard {
			t.Fatalf("expected standard verify level, got %v", level)
		}
		return nil
	}
	t.Cleanup(func() { verifyCommandPhase = origVerify })

	origSummary := verifySummaryPhase
	summaryCalled := false
	verifySummaryPhase = func(_ *sql.DB, target string, fileID int64) (verifyOutputSummary, error) {
		summaryCalled = true
		if target != "file" {
			t.Fatalf("expected summary target file, got %q", target)
		}
		if fileID != 42 {
			t.Fatalf("expected summary fileID 42 for file verify, got %d", fileID)
		}
		return verifyOutputSummary{BlocksChecked: 1}, nil
	}
	t.Cleanup(func() { verifySummaryPhase = origSummary })

	output := captureStdout(t, func() {
		err := runVerifyCommand(parsedCommandLine{
			method:      "verify",
			positionals: []string{"file", "42"},
			flags:       map[string][]string{"output": {"json"}},
		}, outputModeJSON)
		if err != nil {
			t.Fatalf("runVerifyCommand: %v", err)
		}
	})

	if !verifyCalled {
		t.Fatalf("expected verifyCommandPhase to be called")
	}
	if !summaryCalled {
		t.Fatalf("expected verifySummaryPhase to be called")
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &payload); err != nil {
		t.Fatalf("parse JSON: %v output=%q", err, output)
	}
	if got := payload["target"]; got != "file" {
		t.Fatalf("expected target file, got %v", got)
	}
	if got := int(payload["file_id"].(float64)); got != 42 {
		t.Fatalf("expected file_id 42, got %d", got)
	}
}

func TestVerifyFileIDInt(t *testing.T) {
	maxInt := int64(math.MaxInt64)
	wantMax := math.MaxInt
	if strconv.IntSize == 32 {
		maxInt = math.MaxInt32
		wantMax = math.MaxInt32
	}

	tests := []struct {
		name    string
		fileID  int64
		want    int
		wantErr string
	}{
		{name: "positive in range", fileID: 42, want: 42},
		{name: "zero rejected", fileID: 0, wantErr: "Invalid fileID"},
		{name: "negative rejected", fileID: -1, wantErr: "Invalid fileID"},
		{name: "max int allowed", fileID: maxInt, want: wantMax},
	}

	if maxInt < math.MaxInt64 {
		tests = append(tests, struct {
			name    string
			fileID  int64
			want    int
			wantErr string
		}{
			name:    "overflow rejected",
			fileID:  maxInt + 1,
			wantErr: "exceeds platform int range",
		})
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := verifyFileIDInt(tc.fileID)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("verifyFileIDInt(%d) error = %v, want substring %q", tc.fileID, err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("verifyFileIDInt(%d) returned error: %v", tc.fileID, err)
			}
			if got != tc.want {
				t.Fatalf("verifyFileIDInt(%d) = %d, want %d", tc.fileID, got, tc.want)
			}
		})
	}
}

func TestRunVerifyCommandRejectsOversizedFileIDBeforeRouting(t *testing.T) {
	maxInt := int64(math.MaxInt64)
	if strconv.IntSize == 32 {
		maxInt = math.MaxInt32
	}
	if maxInt == math.MaxInt64 {
		t.Skip("current platform int is 64-bit; no larger signed int64 fileID exists")
	}

	origVerify := verifyCommandPhase
	verifyCalled := false
	verifyCommandPhase = func(_ *sql.DB, target string, fileID int, level verify.VerifyLevel) error {
		verifyCalled = true
		return nil
	}
	t.Cleanup(func() { verifyCommandPhase = origVerify })

	err := runVerifyCommand(parsedCommandLine{
		method:      "verify",
		positionals: []string{"file", strconv.FormatInt(maxInt+1, 10)},
		flags:       map[string][]string{},
	}, outputModeText)
	if err == nil {
		t.Fatal("expected usage error for oversized verify fileID")
	}
	if verifyCalled {
		t.Fatal("verifyCommandPhase should not be called for oversized fileID")
	}
	if got := classifyExitCode(err); got != exitUsage {
		t.Fatalf("expected usage exit code %d, got %d", exitUsage, got)
	}
	if !strings.Contains(err.Error(), "exceeds platform int range") {
		t.Fatalf("expected oversized fileID message, got: %v", err)
	}
}
