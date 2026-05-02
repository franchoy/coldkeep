package iodebug

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func resetIOCountersTestState(t *testing.T) {
	t.Helper()
	enabledOnce = sync.Once{}
	enabled = false
	containerAppendCount.Store(0)
	fsyncCount.Store(0)
	containerOpenCount.Store(0)
	containerCloseCount.Store(0)
	bytesWritten.Store(0)
	bytesRead.Store(0)
	snapshotMetadataWrites.Store(0)
}

func TestCountersDisabledNoop(t *testing.T) {
	resetIOCountersTestState(t)
	t.Setenv(envIOCountersFile, "")

	StartOperation()
	IncContainerAppend()
	IncFsync()
	IncContainerOpen()
	IncContainerClose()
	IncSnapshotMetadataWrite()
	AddBytesWritten(10)
	AddBytesRead(5)

	if got := Snapshot(); got != (Counters{}) {
		t.Fatalf("expected zero snapshot when disabled, got=%+v", got)
	}
	if err := FlushProcessCounters("benchmark", "run"); err != nil {
		t.Fatalf("FlushProcessCounters disabled returned error: %v", err)
	}
}

func TestCountersEnabledSnapshotAndFlush(t *testing.T) {
	resetIOCountersTestState(t)
	outPath := filepath.Join(t.TempDir(), "io-counters.jsonl")
	t.Setenv(envIOCountersFile, outPath)

	StartOperation()
	IncContainerAppend()
	IncFsync()
	IncContainerOpen()
	IncContainerClose()
	IncSnapshotMetadataWrite()
	AddBytesWritten(99)
	AddBytesRead(7)

	snap := Snapshot()
	if snap.ContainerAppendCount != 1 || snap.FsyncCount != 1 || snap.ContainerOpenCount != 1 || snap.ContainerCloseCount != 1 || snap.SnapshotMetadataWrites != 1 || snap.BytesWritten != 99 || snap.BytesRead != 7 {
		t.Fatalf("unexpected snapshot counters: %+v", snap)
	}

	if err := FlushProcessCounters("benchmark", "run"); err != nil {
		t.Fatalf("FlushProcessCounters error: %v", err)
	}

	raw, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read flushed counters: %v", err)
	}
	var decoded struct {
		Command    string `json:"command"`
		Subcommand string `json:"subcommand"`
		Counters
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode flushed counters: %v raw=%q", err, string(raw))
	}
	if decoded.Command != "benchmark" || decoded.Subcommand != "run" {
		t.Fatalf("unexpected command metadata: %+v", decoded)
	}
	if decoded.BytesWritten != 99 || decoded.BytesRead != 7 || decoded.ContainerAppendCount != 1 {
		t.Fatalf("unexpected flushed counters: %+v", decoded)
	}
}
