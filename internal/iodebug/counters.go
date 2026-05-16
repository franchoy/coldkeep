package iodebug

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
)

const envIOCountersFile = "COLDKEEP_IO_COUNTERS_FILE"

type Counters struct {
	ContainerAppendCount   int64 `json:"container_append_count"`
	FsyncCount             int64 `json:"fsync_count"`
	ContainerOpenCount     int64 `json:"container_open_count"`
	ContainerCloseCount    int64 `json:"container_close_count"`
	BytesWritten           int64 `json:"bytes_written"`
	BytesRead              int64 `json:"bytes_read"`
	SnapshotMetadataWrites int64 `json:"snapshot_metadata_write_count"`
	BlockDecodeCount       int64 `json:"block_decode_count"`
}

type processRecord struct {
	Command    string `json:"command"`
	Subcommand string `json:"subcommand,omitempty"`
	Counters
}

var (
	enabledOnce sync.Once
	enabled     bool

	containerAppendCount   atomic.Int64
	fsyncCount             atomic.Int64
	containerOpenCount     atomic.Int64
	containerCloseCount    atomic.Int64
	bytesWritten           atomic.Int64
	bytesRead              atomic.Int64
	snapshotMetadataWrites atomic.Int64
	blockDecodeCount       atomic.Int64
)

// StartOperation resets in-process metrics so captured counters are scoped to
// one command execution rather than cumulative process lifetime totals.
func StartOperation() {
	if !isEnabled() {
		return
	}
	containerAppendCount.Store(0)
	fsyncCount.Store(0)
	containerOpenCount.Store(0)
	containerCloseCount.Store(0)
	bytesWritten.Store(0)
	bytesRead.Store(0)
	snapshotMetadataWrites.Store(0)
	blockDecodeCount.Store(0)
}

func isEnabled() bool {
	enabledOnce.Do(func() {
		enabled = strings.TrimSpace(os.Getenv(envIOCountersFile)) != ""
	})
	return enabled
}

func IncContainerAppend() {
	if !isEnabled() {
		return
	}
	containerAppendCount.Add(1)
}

func IncFsync() {
	if !isEnabled() {
		return
	}
	fsyncCount.Add(1)
}

func IncContainerOpen() {
	if !isEnabled() {
		return
	}
	containerOpenCount.Add(1)
}

func IncContainerClose() {
	if !isEnabled() {
		return
	}
	containerCloseCount.Add(1)
}

func IncSnapshotMetadataWrite() {
	if !isEnabled() {
		return
	}
	snapshotMetadataWrites.Add(1)
}

func AddBytesWritten(n int64) {
	if !isEnabled() || n <= 0 {
		return
	}
	bytesWritten.Add(n)
}

func AddBytesRead(n int64) {
	if !isEnabled() || n <= 0 {
		return
	}
	bytesRead.Add(n)
}

func IncBlockDecode() {
	if !isEnabled() {
		return
	}
	blockDecodeCount.Add(1)
}

func Snapshot() Counters {
	return Counters{
		ContainerAppendCount:   containerAppendCount.Load(),
		FsyncCount:             fsyncCount.Load(),
		ContainerOpenCount:     containerOpenCount.Load(),
		ContainerCloseCount:    containerCloseCount.Load(),
		BytesWritten:           bytesWritten.Load(),
		BytesRead:              bytesRead.Load(),
		SnapshotMetadataWrites: snapshotMetadataWrites.Load(),
		BlockDecodeCount:       blockDecodeCount.Load(),
	}
}

func FlushProcessCounters(command string, subcommand string) error {
	if !isEnabled() {
		return nil
	}
	path, err := parseIOCountersPath(os.Getenv(envIOCountersFile))
	if err != nil {
		return err
	}
	if path == "" {
		return nil
	}

	record := processRecord{
		Command:    strings.TrimSpace(command),
		Subcommand: strings.TrimSpace(subcommand),
		Counters:   Snapshot(),
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return err
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	if _, err := f.Write(append(encoded, '\n')); err != nil {
		return err
	}
	return nil
}

func parseIOCountersPath(raw string) (string, error) {
	path := strings.TrimSpace(raw)
	if path == "" {
		return "", nil
	}
	if strings.ContainsRune(path, '\x00') {
		return "", fmt.Errorf("%s must not contain NUL byte", envIOCountersFile)
	}
	return path, nil
}
