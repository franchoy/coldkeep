package benchmark

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics captures comprehensive benchmark run measurements across five categories:
// Storage, Throughput, CPU, Memory, and Structural metrics.
//
// Storage Metrics distinguish logical input size from compressed and stored sizes,
// enabling analysis of compression effectiveness and storage transform overhead.
//
// Throughput Metrics track MB/s for store, restore, and verify operations separately,
// allowing identification of CPU-intensive transformations.
//
// CPU Metrics segment CPU time by operation phase to identify compression overhead.
//
// Memory Metrics monitor peak heap usage and allocation churn from transforms.
//
// Structural Metrics track compression decisions (blocks compressed vs not, fallbacks)
// for repositories with mixed stored/uncompressed content.
type Metrics struct {
	// Core operation metrics
	Duration       time.Duration
	FilesProcessed int
	BytesProcessed int64
	ThroughputMBps float64

	// Storage Metrics: distinguish logical vs physical data sizes
	LogicalBytes     int64   // input data size (uncompressed)
	CompressedBytes  int64   // size after compression (before storage overhead)
	StoredBytes      int64   // final data on disk (storage + encryption + overhead)
	CompressionRatio float64 // CompressedBytes / LogicalBytes, stable across runs
	PhysicalRatio    float64 // StoredBytes / LogicalBytes, includes all overhead

	// Throughput Metrics: operation-specific throughput
	StoreMBps   float64 // MB/s during store operation
	RestoreMBps float64 // MB/s during restore operation
	VerifyMBps  float64 // MB/s during verify operation

	// CPU Metrics: time spent in each operation phase
	CompressionCPUTime time.Duration // CPU time for compression phase
	RestoreCPUTime     time.Duration // CPU time for restore/decompression
	VerifyCPUTime      time.Duration // CPU time for verify operation

	// Memory Metrics: heap and allocation pressure
	PeakMemoryBytes int64 // peak heap usage during operation
	AllocationCount int64 // number of allocations (alloc churn indicator)

	// Structural Metrics: compression effectiveness tracking
	CompressedBlocks       int64 // blocks where compression was applied
	UncompressedBlocks     int64 // blocks stored as-is (compression ineffective or skipped)
	StoreIfSmallerFallback int64 // instances where uncompressed fallback was used
}

type metricsAccumulator struct {
	// Core counters
	files atomic.Int64
	bytes atomic.Int64

	// Storage counters
	logicalBytes     atomic.Int64
	compressedBytes  atomic.Int64
	storedBytes      atomic.Int64
	compressionRatio atomic.Value // float64
	physicalRatio    atomic.Value // float64

	// Throughput
	storeMBps   atomic.Value // float64
	restoreMBps atomic.Value // float64
	verifyMBps  atomic.Value // float64

	// CPU time
	compressionCPUTime atomic.Int64 // nanoseconds
	restoreCPUTime     atomic.Int64 // nanoseconds
	verifyCPUTime      atomic.Int64 // nanoseconds

	// Memory
	peakMemoryBytes atomic.Int64
	allocationCount atomic.Int64

	// Structural
	compressedBlocks       atomic.Int64
	uncompressedBlocks     atomic.Int64
	storeIfSmallerFallback atomic.Int64
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
// The fn can report processed counters via RecordProcessed, RecordStorage, RecordCPU,
// RecordMemory, and RecordStructural.
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

	// Extract ratio values with safe defaults
	compressionRatio := 0.0
	if v, ok := acc.compressionRatio.Load().(float64); ok {
		compressionRatio = v
	}
	physicalRatio := 0.0
	if v, ok := acc.physicalRatio.Load().(float64); ok {
		physicalRatio = v
	}

	// Extract throughput values with safe defaults
	storeMBps := 0.0
	if v, ok := acc.storeMBps.Load().(float64); ok {
		storeMBps = v
	}
	restoreMBps := 0.0
	if v, ok := acc.restoreMBps.Load().(float64); ok {
		restoreMBps = v
	}
	verifyMBps := 0.0
	if v, ok := acc.verifyMBps.Load().(float64); ok {
		verifyMBps = v
	}

	metrics := Metrics{
		Duration:       duration,
		FilesProcessed: int(acc.files.Load()),
		BytesProcessed: bytesProcessed,
		ThroughputMBps: throughputMBps(bytesProcessed, duration),

		// Storage
		LogicalBytes:     acc.logicalBytes.Load(),
		CompressedBytes:  acc.compressedBytes.Load(),
		StoredBytes:      acc.storedBytes.Load(),
		CompressionRatio: compressionRatio,
		PhysicalRatio:    physicalRatio,

		// Throughput
		StoreMBps:   storeMBps,
		RestoreMBps: restoreMBps,
		VerifyMBps:  verifyMBps,

		// CPU
		CompressionCPUTime: time.Duration(acc.compressionCPUTime.Load()),
		RestoreCPUTime:     time.Duration(acc.restoreCPUTime.Load()),
		VerifyCPUTime:      time.Duration(acc.verifyCPUTime.Load()),

		// Memory
		PeakMemoryBytes: acc.peakMemoryBytes.Load(),
		AllocationCount: acc.allocationCount.Load(),

		// Structural
		CompressedBlocks:       acc.compressedBlocks.Load(),
		UncompressedBlocks:     acc.uncompressedBlocks.Load(),
		StoreIfSmallerFallback: acc.storeIfSmallerFallback.Load(),
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

// RecordStorage records storage metrics: logical, compressed, and final stored sizes.
// Ratios are computed internally for stability across runs.
// Calls made outside Measure are ignored.
func RecordStorage(logicalBytes, compressedBytes, storedBytes int64) {
	measureMu.Lock()
	acc := activeAccumulator
	measureMu.Unlock()
	if acc == nil {
		return
	}

	if logicalBytes > 0 {
		acc.logicalBytes.Add(logicalBytes)
	}
	if compressedBytes > 0 {
		acc.compressedBytes.Add(compressedBytes)
	}
	if storedBytes > 0 {
		acc.storedBytes.Add(storedBytes)
	}

	// Compute and update ratios
	totalLogical := acc.logicalBytes.Load()
	if totalLogical > 0 {
		totalCompressed := acc.compressedBytes.Load()
		totalStored := acc.storedBytes.Load()
		acc.compressionRatio.Store(float64(totalCompressed) / float64(totalLogical))
		acc.physicalRatio.Store(float64(totalStored) / float64(totalLogical))
	}
}

// RecordThroughput records operation-specific throughput in MB/s.
// Calls made outside Measure are ignored.
func RecordThroughput(operationType string, mbps float64) {
	measureMu.Lock()
	acc := activeAccumulator
	measureMu.Unlock()
	if acc == nil {
		return
	}

	if mbps < 0 {
		return
	}

	switch operationType {
	case "store":
		acc.storeMBps.Store(mbps)
	case "restore":
		acc.restoreMBps.Store(mbps)
	case "verify":
		acc.verifyMBps.Store(mbps)
	}
}

// RecordCPU records CPU time spent in each operation phase.
// Duration should be the CPU time (not wall-clock time) for accurate overhead calculation.
// Calls made outside Measure are ignored.
func RecordCPU(operationType string, cpuTime time.Duration) {
	measureMu.Lock()
	acc := activeAccumulator
	measureMu.Unlock()
	if acc == nil {
		return
	}

	if cpuTime <= 0 {
		return
	}

	switch operationType {
	case "compression":
		acc.compressionCPUTime.Add(cpuTime.Nanoseconds())
	case "restore":
		acc.restoreCPUTime.Add(cpuTime.Nanoseconds())
	case "verify":
		acc.verifyCPUTime.Add(cpuTime.Nanoseconds())
	}
}

// RecordMemory records peak heap usage and allocation count.
// Calls made outside Measure are ignored.
func RecordMemory(peakMemoryBytes int64, allocations int64) {
	measureMu.Lock()
	acc := activeAccumulator
	measureMu.Unlock()
	if acc == nil {
		return
	}

	if peakMemoryBytes > 0 {
		// Keep the maximum peak
		currentPeak := acc.peakMemoryBytes.Load()
		for peakMemoryBytes > currentPeak && !acc.peakMemoryBytes.CompareAndSwap(currentPeak, peakMemoryBytes) {
			currentPeak = acc.peakMemoryBytes.Load()
		}
	}

	if allocations > 0 {
		acc.allocationCount.Add(allocations)
	}
}

// RecordStructural records compression effectiveness: blocks compressed, not compressed, and fallbacks.
// Calls made outside Measure are ignored.
func RecordStructural(compressedBlocks, uncompressedBlocks, storeIfSmallerFallbacks int64) {
	measureMu.Lock()
	acc := activeAccumulator
	measureMu.Unlock()
	if acc == nil {
		return
	}

	if compressedBlocks > 0 {
		acc.compressedBlocks.Add(compressedBlocks)
	}
	if uncompressedBlocks > 0 {
		acc.uncompressedBlocks.Add(uncompressedBlocks)
	}
	if storeIfSmallerFallbacks > 0 {
		acc.storeIfSmallerFallback.Add(storeIfSmallerFallbacks)
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
