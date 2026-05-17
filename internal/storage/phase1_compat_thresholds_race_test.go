//go:build race

package storage

const (
	// storeThroughputMinMBps is the minimum acceptable store throughput (MB/s)
	// for race-enabled CI. The lower floor avoids false failures from race
	// detector overhead while still catching complete breakage.
	storeThroughputMinMBps = 1.0

	// restoreThroughputMinMBps is the minimum acceptable restore throughput for
	// race-enabled CI.
	restoreThroughputMinMBps = 1.0
)
