//go:build !race

package storage

const (
	// storeThroughputMinMBps is the minimum acceptable store throughput (MB/s)
	// for non-race test runs. This restores the stricter steady-state floor that
	// would be too flaky under the race detector.
	storeThroughputMinMBps = 5.0

	// restoreThroughputMinMBps is the minimum acceptable restore throughput for
	// non-race test runs.
	restoreThroughputMinMBps = 5.0
)
