package benchmark

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics captures benchmark run measurements.
type Metrics struct {
	Duration       time.Duration
	FilesProcessed int
	BytesProcessed int64
	ThroughputMBps float64
}

type metricsAccumulator struct {
	files atomic.Int64
	bytes atomic.Int64
}

var (
	measureMu         sync.Mutex
	activeAccumulator *metricsAccumulator
	errNestedMeasure  = fmt.Errorf("nested Measure calls are not supported")
)

// Measure executes fn, captures elapsed time, and computes throughput.
//
// Throughput is calculated as:
// MB/s = bytes / duration
//
// The fn can report processed counters via RecordProcessed.
func Measure(fn func() error) (Metrics, error) {
	if fn == nil {
		return Metrics{}, fmt.Errorf("measure function cannot be nil")
	}

	acc := &metricsAccumulator{}
	if err := activateAccumulator(acc); err != nil {
		return Metrics{}, err
	}

	start := time.Now()
	err := fn()
	duration := time.Since(start)
	deactivateAccumulator(acc)

	bytesProcessed := acc.bytes.Load()
	metrics := Metrics{
		Duration:       duration,
		FilesProcessed: int(acc.files.Load()),
		BytesProcessed: bytesProcessed,
		ThroughputMBps: throughputMBps(bytesProcessed, duration),
	}

	return metrics, err
}

// RecordProcessed increments files and bytes counters for the active Measure call.
// Calls made outside Measure are ignored.
func RecordProcessed(files int, bytes int64) {
	measureMu.Lock()
	acc := activeAccumulator
	measureMu.Unlock()
	if acc == nil {
		return
	}

	if files > 0 {
		acc.files.Add(int64(files))
	}
	if bytes > 0 {
		acc.bytes.Add(bytes)
	}
}

func activateAccumulator(acc *metricsAccumulator) error {
	measureMu.Lock()
	defer measureMu.Unlock()
	if activeAccumulator != nil {
		return errNestedMeasure
	}
	activeAccumulator = acc
	return nil
}

func deactivateAccumulator(acc *metricsAccumulator) {
	measureMu.Lock()
	defer measureMu.Unlock()
	if activeAccumulator == acc {
		activeAccumulator = nil
	}
}

func throughputMBps(bytes int64, duration time.Duration) float64 {
	if bytes <= 0 || duration <= 0 {
		return 0
	}
	mb := float64(bytes) / (1024 * 1024)
	seconds := duration.Seconds()
	if seconds <= 0 {
		return 0
	}
	return mb / seconds
}
