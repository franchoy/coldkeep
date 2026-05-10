package benchmark

import (
	"errors"
	"math"
	"testing"
	"time"
)

func TestMeasureCollectsCountersAndThroughput(t *testing.T) {
	metrics, err := Measure(func() error {
		RecordProcessed(3, 3*1024*1024)
		time.Sleep(5 * time.Millisecond)
		return nil
	})
	if err != nil {
		t.Fatalf("Measure returned error: %v", err)
	}
	if metrics.Duration <= 0 {
		t.Fatalf("expected positive duration, got=%v", metrics.Duration)
	}
	if metrics.FilesProcessed != 3 {
		t.Fatalf("files processed mismatch: got=%d want=3", metrics.FilesProcessed)
	}
	if metrics.BytesProcessed != 3*1024*1024 {
		t.Fatalf("bytes processed mismatch: got=%d want=%d", metrics.BytesProcessed, 3*1024*1024)
	}
	if metrics.ThroughputMBps <= 0 {
		t.Fatalf("expected positive throughput, got=%f", metrics.ThroughputMBps)
	}
}

func TestMeasureReturnsFunctionErrorWithMetrics(t *testing.T) {
	wantErr := errors.New("boom")
	metrics, err := Measure(func() error {
		RecordProcessed(1, 1024)
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped error %v, got %v", wantErr, err)
	}
	if metrics.FilesProcessed != 1 || metrics.BytesProcessed != 1024 {
		t.Fatalf("metrics should still be populated on error, got=%+v", metrics)
	}
}

func TestMeasureZeroBytesYieldsZeroThroughput(t *testing.T) {
	metrics, err := Measure(func() error {
		RecordProcessed(5, 0)
		return nil
	})
	if err != nil {
		t.Fatalf("Measure returned error: %v", err)
	}
	if metrics.ThroughputMBps != 0 {
		t.Fatalf("expected zero throughput, got=%f", metrics.ThroughputMBps)
	}
}

func TestMeasureRejectsNilFunction(t *testing.T) {
	_, err := Measure(nil)
	if err == nil {
		t.Fatal("expected error for nil function")
	}
}

func TestRecordProcessedOutsideMeasureIsNoop(t *testing.T) {
	RecordProcessed(100, 9999)

	metrics, err := Measure(func() error {
		return nil
	})
	if err != nil {
		t.Fatalf("Measure returned error: %v", err)
	}
	if metrics.FilesProcessed != 0 || metrics.BytesProcessed != 0 {
		t.Fatalf("expected zero counters when nothing recorded in-measure, got=%+v", metrics)
	}
	if math.Signbit(metrics.ThroughputMBps) {
		t.Fatalf("throughput should not be negative, got=%f", metrics.ThroughputMBps)
	}
}

// ====== Storage Metrics Tests ======

func TestRecordStorageTracksLogicalCompressedStored(t *testing.T) {
	metrics, err := Measure(func() error {
		logicalBytes := int64(10 * 1024 * 1024)   // 10 MB uncompressed
		compressedBytes := int64(3 * 1024 * 1024) // 3 MB compressed
		storedBytes := int64(3*1024*1024 + 4096)  // 3 MB + 4KB overhead
		RecordStorage(logicalBytes, compressedBytes, storedBytes)
		return nil
	})
	if err != nil {
		t.Fatalf("Measure returned error: %v", err)
	}

	if metrics.LogicalBytes != 10*1024*1024 {
		t.Fatalf("logical bytes mismatch: got=%d want=%d", metrics.LogicalBytes, 10*1024*1024)
	}
	if metrics.CompressedBytes != 3*1024*1024 {
		t.Fatalf("compressed bytes mismatch: got=%d want=%d", metrics.CompressedBytes, 3*1024*1024)
	}
	if metrics.StoredBytes != 3*1024*1024+4096 {
		t.Fatalf("stored bytes mismatch: got=%d want=%d", metrics.StoredBytes, 3*1024*1024+4096)
	}

	// Verify compression ratio: 3MB / 10MB = 0.3
	expectedCompRatio := 0.3
	if math.Abs(metrics.CompressionRatio-expectedCompRatio) > 0.001 {
		t.Fatalf("compression ratio mismatch: got=%f want=%f", metrics.CompressionRatio, expectedCompRatio)
	}

	// Verify physical ratio: (3MB+4KB) / 10MB = 0.300390625
	expectedPhysRatio := float64(3*1024*1024+4096) / float64(10*1024*1024)
	if math.Abs(metrics.PhysicalRatio-expectedPhysRatio) > 0.001 {
		t.Fatalf("physical ratio mismatch: got=%f want=%f", metrics.PhysicalRatio, expectedPhysRatio)
	}
}

func TestStorageRatiosStableAcrossMultipleCalls(t *testing.T) {
	metrics, err := Measure(func() error {
		// First batch
		RecordStorage(int64(5*1024*1024), int64(1536*1024), int64(1540*1024))
		// Second batch (same compression ratio)
		RecordStorage(int64(5*1024*1024), int64(1536*1024), int64(1540*1024))
		return nil
	})
	if err != nil {
		t.Fatalf("Measure returned error: %v", err)
	}

	if metrics.LogicalBytes != 10*1024*1024 {
		t.Fatalf("total logical bytes mismatch: got=%d", metrics.LogicalBytes)
	}
	if metrics.CompressedBytes != 3072*1024 {
		t.Fatalf("total compressed bytes mismatch: got=%d", metrics.CompressedBytes)
	}

	// Ratio should be stable: 3072KB / 10MB = 0.3
	expectedRatio := float64(3072*1024) / float64(10*1024*1024)
	if math.Abs(metrics.CompressionRatio-expectedRatio) > 0.001 {
		t.Fatalf("compression ratio not stable: got=%f want=%f", metrics.CompressionRatio, expectedRatio)
	}
}

func TestStorageMetricsDistinguishLogicalFromPhysical(t *testing.T) {
	metrics, err := Measure(func() error {
		// High compression with overhead: 100MB → 10MB compressed + 1MB overhead = 11MB stored
		RecordStorage(int64(100*1024*1024), int64(10*1024*1024), int64(11*1024*1024))
		return nil
	})
	if err != nil {
		t.Fatalf("Measure returned error: %v", err)
	}

	if metrics.LogicalBytes != 100*1024*1024 {
		t.Fatalf("logical bytes should be 100MB")
	}
	if metrics.CompressionRatio == metrics.PhysicalRatio {
		t.Fatalf("compression ratio and physical ratio should differ due to overhead: comp=%f phys=%f",
			metrics.CompressionRatio, metrics.PhysicalRatio)
	}
	// Physical ratio should be greater than compression ratio due to overhead
	if metrics.PhysicalRatio <= metrics.CompressionRatio {
		t.Fatalf("physical ratio should be greater than compression ratio (due to overhead): comp=%f phys=%f",
			metrics.CompressionRatio, metrics.PhysicalRatio)
	}
}

// ====== Throughput Metrics Tests ======

func TestRecordThroughputTracksOperationSpecificMBps(t *testing.T) {
	tests := []struct {
		op       string
		mbps     float64
		validate func(m Metrics) bool
	}{
		{"store", 500.0, func(m Metrics) bool { return m.StoreMBps == 500.0 }},
		{"restore", 400.0, func(m Metrics) bool { return m.RestoreMBps == 400.0 }},
		{"verify", 600.0, func(m Metrics) bool { return m.VerifyMBps == 600.0 }},
	}

	for _, tt := range tests {
		t.Run(tt.op, func(t *testing.T) {
			metrics, err := Measure(func() error {
				RecordThroughput(tt.op, tt.mbps)
				return nil
			})
			if err != nil {
				t.Fatalf("Measure returned error: %v", err)
			}
			if !tt.validate(metrics) {
				t.Fatalf("throughput validation failed for %s: got=%+v", tt.op, metrics)
			}
		})
	}
}

func TestThroughputMetricsAreIndependent(t *testing.T) {
	metrics, err := Measure(func() error {
		RecordThroughput("store", 500.0)
		RecordThroughput("restore", 400.0)
		RecordThroughput("verify", 600.0)
		return nil
	})
	if err != nil {
		t.Fatalf("Measure returned error: %v", err)
	}

	if metrics.StoreMBps != 500.0 || metrics.RestoreMBps != 400.0 || metrics.VerifyMBps != 600.0 {
		t.Fatalf("all throughputs should be independent: got %+v", metrics)
	}
}

// ====== CPU Metrics Tests ======

func TestRecordCPUTracksCPUTimeByPhase(t *testing.T) {
	tests := []struct {
		phase string
		dur   time.Duration
		check func(m Metrics) bool
	}{
		{"compression", 100 * time.Millisecond, func(m Metrics) bool { return m.CompressionCPUTime == 100*time.Millisecond }},
		{"restore", 80 * time.Millisecond, func(m Metrics) bool { return m.RestoreCPUTime == 80*time.Millisecond }},
		{"verify", 50 * time.Millisecond, func(m Metrics) bool { return m.VerifyCPUTime == 50*time.Millisecond }},
	}

	for _, tt := range tests {
		t.Run(tt.phase, func(t *testing.T) {
			metrics, err := Measure(func() error {
				RecordCPU(tt.phase, tt.dur)
				return nil
			})
			if err != nil {
				t.Fatalf("Measure returned error: %v", err)
			}
			if !tt.check(metrics) {
				t.Fatalf("CPU time validation failed for %s: got=%+v", tt.phase, metrics)
			}
		})
	}
}

func TestCPUMetricsAccumulate(t *testing.T) {
	metrics, err := Measure(func() error {
		RecordCPU("compression", 50*time.Millisecond)
		RecordCPU("compression", 50*time.Millisecond)
		RecordCPU("restore", 40*time.Millisecond)
		return nil
	})
	if err != nil {
		t.Fatalf("Measure returned error: %v", err)
	}

	if metrics.CompressionCPUTime != 100*time.Millisecond {
		t.Fatalf("compression CPU time should accumulate: got=%v want=%v", metrics.CompressionCPUTime, 100*time.Millisecond)
	}
	if metrics.RestoreCPUTime != 40*time.Millisecond {
		t.Fatalf("restore CPU time should be 40ms: got=%v", metrics.RestoreCPUTime)
	}
}

// ====== Memory Metrics Tests ======

func TestRecordMemoryTracksPeakAndAllocations(t *testing.T) {
	metrics, err := Measure(func() error {
		RecordMemory(100*1024*1024, 5000) // 100 MB peak, 5000 allocations
		return nil
	})
	if err != nil {
		t.Fatalf("Measure returned error: %v", err)
	}

	if metrics.PeakMemoryBytes != 100*1024*1024 {
		t.Fatalf("peak memory mismatch: got=%d want=%d", metrics.PeakMemoryBytes, 100*1024*1024)
	}
	if metrics.AllocationCount != 5000 {
		t.Fatalf("allocation count mismatch: got=%d want=%d", metrics.AllocationCount, 5000)
	}
}

func TestMemoryMetricsTrackMaximumPeak(t *testing.T) {
	metrics, err := Measure(func() error {
		RecordMemory(50*1024*1024, 1000)  // 50 MB
		RecordMemory(100*1024*1024, 2000) // 100 MB - should be kept as max
		RecordMemory(75*1024*1024, 1500)  // 75 MB - should not overwrite max
		return nil
	})
	if err != nil {
		t.Fatalf("Measure returned error: %v", err)
	}

	if metrics.PeakMemoryBytes != 100*1024*1024 {
		t.Fatalf("peak memory should be maximum: got=%d want=%d", metrics.PeakMemoryBytes, 100*1024*1024)
	}
	if metrics.AllocationCount != 4500 {
		t.Fatalf("allocation count should accumulate: got=%d want=%d", metrics.AllocationCount, 4500)
	}
}

// ====== Structural Metrics Tests ======

func TestRecordStructuralTracksCompressionDecisions(t *testing.T) {
	metrics, err := Measure(func() error {
		// Track compression decisions: 1000 blocks compressed, 200 uncompressed, 50 fallbacks
		RecordStructural(1000, 200, 50)
		return nil
	})
	if err != nil {
		t.Fatalf("Measure returned error: %v", err)
	}

	if metrics.CompressedBlocks != 1000 {
		t.Fatalf("compressed blocks mismatch: got=%d want=%d", metrics.CompressedBlocks, 1000)
	}
	if metrics.UncompressedBlocks != 200 {
		t.Fatalf("uncompressed blocks mismatch: got=%d want=%d", metrics.UncompressedBlocks, 200)
	}
	if metrics.StoreIfSmallerFallback != 50 {
		t.Fatalf("fallback count mismatch: got=%d want=%d", metrics.StoreIfSmallerFallback, 50)
	}
}

func TestStructuralMetricsWorkForMixedRepositories(t *testing.T) {
	metrics, err := Measure(func() error {
		// Batch 1: newly stored files (all compressible)
		RecordStructural(500, 0, 0)
		// Batch 2: pre-compressed files (none benefit from further compression)
		RecordStructural(0, 300, 0)
		// Batch 3: store-if-smaller analysis on mixed files
		RecordStructural(200, 0, 35)
		return nil
	})
	if err != nil {
		t.Fatalf("Measure returned error: %v", err)
	}

	totalBlocks := metrics.CompressedBlocks + metrics.UncompressedBlocks
	if totalBlocks != 1000 {
		t.Fatalf("total blocks should be 1000: got=%d", totalBlocks)
	}
	if metrics.StoreIfSmallerFallback != 35 {
		t.Fatalf("total fallbacks should be 35: got=%d", metrics.StoreIfSmallerFallback)
	}
}

// ====== Stability and Understandability Tests ======

func TestMetricsStableAcrossMultipleRuns(t *testing.T) {
	runs := 3
	var results []Metrics

	for i := 0; i < runs; i++ {
		metrics, err := Measure(func() error {
			// Simulated deterministic workload
			RecordProcessed(100, 100*1024*1024)
			RecordStorage(100*1024*1024, 30*1024*1024, 31*1024*1024)
			RecordThroughput("store", 500.0)
			RecordStructural(100, 0, 0)
			return nil
		})
		if err != nil {
			t.Fatalf("run %d: Measure failed: %v", i, err)
		}
		results = append(results, metrics)
	}

	// Verify metrics are consistent across runs
	for i := 1; i < len(results); i++ {
		if results[i].LogicalBytes != results[0].LogicalBytes {
			t.Fatalf("logical bytes not stable: run 0=%d, run %d=%d",
				results[0].LogicalBytes, i, results[i].LogicalBytes)
		}
		if math.Abs(results[i].CompressionRatio-results[0].CompressionRatio) > 0.0001 {
			t.Fatalf("compression ratio not stable: run 0=%f, run %d=%f",
				results[0].CompressionRatio, i, results[i].CompressionRatio)
		}
		if results[i].CompressedBlocks != results[0].CompressedBlocks {
			t.Fatalf("compressed blocks not stable: run 0=%d, run %d=%d",
				results[0].CompressedBlocks, i, results[i].CompressedBlocks)
		}
	}
}

func TestMetricsAreUnderstandable(t *testing.T) {
	metrics, err := Measure(func() error {
		RecordProcessed(50, 50*1024*1024)
		RecordStorage(50*1024*1024, 15*1024*1024, 16*1024*1024)
		RecordThroughput("store", 256.0)
		RecordCPU("compression", 100*time.Millisecond)
		RecordMemory(64*1024*1024, 2000)
		RecordStructural(100, 0, 0)
		return nil
	})
	if err != nil {
		t.Fatalf("Measure returned error: %v", err)
	}

	// Verify all metrics are present and meaningful
	if metrics.LogicalBytes <= 0 {
		t.Fatal("logical bytes should be positive and understandable")
	}
	if metrics.CompressionRatio <= 0 || metrics.CompressionRatio > 1.0 {
		t.Fatalf("compression ratio should be between 0-1: got=%f", metrics.CompressionRatio)
	}
	if metrics.PhysicalRatio <= 0 || metrics.PhysicalRatio > 1.0 {
		t.Fatalf("physical ratio should be between 0-1: got=%f", metrics.PhysicalRatio)
	}
	if metrics.StoreMBps <= 0 {
		t.Fatal("store throughput should be positive")
	}
	if metrics.CompressionCPUTime <= 0 {
		t.Fatal("compression CPU time should be positive")
	}
	if metrics.PeakMemoryBytes <= 0 {
		t.Fatal("peak memory should be positive")
	}
	if metrics.CompressedBlocks <= 0 {
		t.Fatal("compressed blocks count should be positive and understandable")
	}
}

func TestRecordOutsideMeasureIgnoresAllMetrics(t *testing.T) {
	// Call all record functions outside Measure
	RecordStorage(1000, 500, 600)
	RecordThroughput("store", 500.0)
	RecordCPU("compression", 100*time.Millisecond)
	RecordMemory(100*1024*1024, 1000)
	RecordStructural(100, 50, 10)

	// Measure should start clean
	metrics, err := Measure(func() error {
		return nil
	})
	if err != nil {
		t.Fatalf("Measure returned error: %v", err)
	}

	if metrics.LogicalBytes != 0 || metrics.CompressedBytes != 0 ||
		metrics.StoredBytes != 0 || metrics.StoreMBps != 0 ||
		metrics.CompressionCPUTime != 0 || metrics.PeakMemoryBytes != 0 ||
		metrics.CompressedBlocks != 0 {
		t.Fatalf("metrics should all be zero when nothing recorded in Measure: got=%+v", metrics)
	}
}
