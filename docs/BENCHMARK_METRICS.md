# Step 6.4 — Benchmark Metrics Specification

Status: ✅ Implemented and Validated

## Overview

The benchmark metrics system tracks storage efficiency, CPU tradeoffs, and transform overhead for Coldkeep compression benchmarks. Metrics are organized into five categories that collectively provide visibility into compression effectiveness and system performance.

## Five Metric Categories

### 1. Storage Metrics

Track logical input data size through each transformation stage, enabling analysis of compression effectiveness and storage transform overhead.

| Metric | Type | Unit | Purpose |
|--------|------|------|---------|
| `LogicalBytes` | int64 | bytes | Original uncompressed input data size |
| `CompressedBytes` | int64 | bytes | Size after compression (before storage overhead) |
| `StoredBytes` | int64 | bytes | Final data on disk (includes encryption, metadata) |
| `CompressionRatio` | float64 | ratio | `CompressedBytes / LogicalBytes` (stable 0.0-1.0) |
| `PhysicalRatio` | float64 | ratio | `StoredBytes / LogicalBytes` (includes overhead) |

**Use Cases:**
- `CompressionRatio < PhysicalRatio` indicates storage transform overhead
- Both metrics stable across benchmark runs for deterministic validation
- Distinguish highly compressible (ratio < 0.5) from adversarial data

**Recording:**
```go
RecordStorage(
    100*1024*1024,  // logical bytes: 100 MB uncompressed
    30*1024*1024,   // compressed bytes: 30 MB after compression
    31*1024*1024,   // stored bytes: +1MB overhead on disk
)
```

### 2. Throughput Metrics

Operation-specific throughput in MB/s, allowing identification of CPU-intensive transformations.

| Metric | Type | Unit | Purpose |
|--------|------|------|---------|
| `StoreMBps` | float64 | MB/s | Store operation throughput |
| `RestoreMBps` | float64 | MB/s | Restore/decompression throughput |
| `VerifyMBps` | float64 | MB/s | Verify operation throughput |
| `ThroughputMBps` | float64 | MB/s | Overall operation throughput |

**Use Cases:**
- Compare `StoreMBps` vs `RestoreMBps` to identify compression CPU cost
- Sequential recovery: typically RestoreMBps > StoreMBps due to compression overhead
- Verify operations usually fastest (no transformation)

**Recording:**
```go
RecordThroughput("store", 256.0)    // 256 MB/s store speed
RecordThroughput("restore", 512.0)  // 512 MB/s restore speed (faster)
RecordThroughput("verify", 1024.0)  // 1024 MB/s verify (no transform)
```

### 3. CPU Metrics

CPU time segmented by operation phase, identifying compression overhead.

| Metric | Type | Unit | Purpose |
|--------|------|------|---------|
| `CompressionCPUTime` | duration | nanoseconds | CPU time for compression phase |
| `RestoreCPUTime` | duration | nanoseconds | CPU time for restore/decompression |
| `VerifyCPUTime` | duration | nanoseconds | CPU time for verify operation |

**Use Cases:**
- Compression CPU time typically highest (compression is CPU-intensive)
- Restore CPU time inversely correlates with compression ratio
- Analyze CPU/throughput ratio to identify bottlenecks

**Recording:**
```go
RecordCPU("compression", 150*time.Millisecond)
RecordCPU("restore", 75*time.Millisecond)
RecordCPU("verify", 20*time.Millisecond)
```

### 4. Memory Metrics

Heap and allocation pressure from compression transforms.

| Metric | Type | Unit | Purpose |
|--------|------|------|---------|
| `PeakMemoryBytes` | int64 | bytes | Maximum heap usage during operation |
| `AllocationCount` | int64 | count | Number of allocations (alloc churn) |

**Use Cases:**
- Detect memory spikes from compression buffering
- Allocation count indicates GC pressure
- Important for resource-constrained environments

**Recording:**
```go
RecordMemory(
    256*1024*1024,  // peak memory: 256 MB
    5000,           // allocation count: 5000 allocations
)
```

### 5. Structural Metrics

Compression effectiveness tracking for repositories with mixed stored/uncompressed content.

| Metric | Type | Unit | Purpose |
|--------|------|------|---------|
| `CompressedBlocks` | int64 | blocks | Blocks where compression was applied |
| `UncompressedBlocks` | int64 | blocks | Blocks stored as-is (compression ineffective) |
| `StoreIfSmallerFallback` | int64 | instances | Times uncompressed fallback used |

**Use Cases:**
- Mixed repositories: some pre-compressed, some highly compressible
- Measure effectiveness: `CompressedBlocks / (CompressedBlocks + UncompressedBlocks)`
- Fallback count identifies compression failures (data incompressible)

**Recording:**
```go
RecordStructural(
    1000,  // 1000 blocks successfully compressed
    200,   // 200 blocks stored uncompressed
    50,    // 50 store-if-smaller fallbacks
)
```

## Properties

### ✔ Metrics Stable Across Runs

All metrics are **deterministic** for fixed workloads:
- Compression ratio repeatable: same data yields same compression ratio
- CPU time stable: measured for identical operations
- Structure metrics stable: same blocks processed identically

### ✔ Metrics Understandable

Each metric has clear semantics:
- **Ratios bounded 0.0-1.0**: easy to reason about
- **Operation types explicit**: store/restore/verify clearly named
- **Physical vs logical sizes**: disambiguation prevents confusion
- **Block statistics concrete**: countable, observable events

### ✔ Metrics Distinguish Logical vs Physical Sizes

Separation enables visibility into transform overhead:
```
LogicalBytes = 100 MB (input)
CompressedBytes = 30 MB (after compression)
StoredBytes = 31 MB (with overhead)

CompressionRatio = 0.30 (30% of original)
PhysicalRatio = 0.31 (31% of original)
Overhead = PhysicalRatio - CompressionRatio = 0.01 (1%)
```

### ✔ Metrics Work for Mixed Repositories

Structural metrics support repositories with:
- **Highly compressible files**: source code, JSON, logs
- **Pre-compressed content**: JPEG, MP4, ZIP, already compressed
- **Adversarial data**: random bytes, encrypted blobs

Example flow:
1. Store highly compressible files → CompressedBlocks++
2. Store JPEG (already compressed) → UncompressedBlocks++
3. Analyze JSON where compression makes it larger → StoreIfSmallerFallback++

## API Reference

### Recording Functions

All recording functions are **no-op outside Measure()** calls.

```go
// Core: files and bytes processed
RecordProcessed(files int, bytes int64)

// Storage: distinguish logical from physical sizes
RecordStorage(logicalBytes, compressedBytes, storedBytes int64)

// Throughput: operation-specific MB/s
RecordThroughput(operationType string, mbps float64)
// operationType: "store", "restore", "verify"

// CPU: clock time by phase
RecordCPU(operationType string, cpuTime time.Duration)
// operationType: "compression", "restore", "verify"

// Memory: peak heap and alloc churn
RecordMemory(peakMemoryBytes int64, allocations int64)

// Structural: compression decision tracking
RecordStructural(compressedBlocks, uncompressedBlocks, fallbacks int64)
```

### Metrics Struct

```go
type Metrics struct {
    // Core metrics
    Duration       time.Duration
    FilesProcessed int
    BytesProcessed int64
    ThroughputMBps float64

    // Storage: logical vs physical distinction
    LogicalBytes       int64
    CompressedBytes    int64
    StoredBytes        int64
    CompressionRatio   float64
    PhysicalRatio      float64

    // Throughput: operation-specific
    StoreMBps   float64
    RestoreMBps float64
    VerifyMBps  float64

    // CPU: by phase
    CompressionCPUTime time.Duration
    RestoreCPUTime     time.Duration
    VerifyCPUTime      time.Duration

    // Memory: heap and alloc pressure
    PeakMemoryBytes int64
    AllocationCount int64

    // Structural: compression decision tracking
    CompressedBlocks       int64
    UncompressedBlocks     int64
    StoreIfSmallerFallback int64
}
```

## Usage Example

```go
import (
    "time"
    "github.com/franchoy/coldkeep/internal/benchmark"
)

metrics, err := benchmark.Measure(func() error {
    // Simulate store operation
    benchmark.RecordProcessed(100, 100*1024*1024)

    // Record storage transform (100 MB → 30 MB compressed + 1 MB overhead)
    benchmark.RecordStorage(
        100*1024*1024,  // logical
        30*1024*1024,   // compressed
        31*1024*1024,   // stored
    )

    // Record operation throughput
    benchmark.RecordThroughput("store", 256.0)  // 256 MB/s

    // Record CPU time
    benchmark.RecordCPU("compression", 150*time.Millisecond)

    // Record memory usage
    benchmark.RecordMemory(256*1024*1024, 5000)

    // Record compression decisions for mixed repository
    benchmark.RecordStructural(
        1000,  // 1000 blocks compressed effectively
        200,   // 200 blocks stored as-is
        50,    // 50 store-if-smaller fallbacks
    )

    // Run actual benchmark operation
    return runStoreOperation()
})

if err != nil {
    return err
}

// Analyze results
fmt.Printf("Compression Ratio: %.2f\n", metrics.CompressionRatio)
fmt.Printf("Physical Ratio: %.2f\n", metrics.PhysicalRatio)
fmt.Printf("Store Throughput: %.1f MB/s\n", metrics.StoreMBps)
fmt.Printf("Effective Compression: %.1f%%\n", 
    float64(metrics.CompressedBlocks)*100/float64(metrics.CompressedBlocks+metrics.UncompressedBlocks))
```

## Testing

### Validation Tests

**Storage Metrics:**
- ✅ `TestRecordStorageTracksLogicalCompressedStored`: tracks all three sizes
- ✅ `TestStorageRatiosStableAcrossMultipleCalls`: ratios stable across runs
- ✅ `TestStorageMetricsDistinguishLogicalFromPhysical`: overhead visible

**Throughput Metrics:**
- ✅ `TestRecordThroughputTracksOperationSpecificMBps`: per-operation tracking
- ✅ `TestThroughputMetricsAreIndependent`: operations don't interfere

**CPU Metrics:**
- ✅ `TestRecordCPUTracksCPUTimeByPhase`: time tracking by phase
- ✅ `TestCPUMetricsAccumulate`: times accumulate correctly

**Memory Metrics:**
- ✅ `TestRecordMemoryTracksPeakAndAllocations`: peak and count tracked
- ✅ `TestMemoryMetricsTrackMaximumPeak`: tracks maximal peak

**Structural Metrics:**
- ✅ `TestRecordStructuralTracksCompressionDecisions`: compression tracking
- ✅ `TestStructuralMetricsWorkForMixedRepositories`: mixed repo support

**Stability & Understandability:**
- ✅ `TestMetricsStableAcrossMultipleRuns`: deterministic for fixed workloads
- ✅ `TestMetricsAreUnderstandable`: all fields meaningful and bounded
- ✅ `TestRecordOutsideMeasureIgnoresAllMetrics`: safe no-op outside Measure

## Integration Points

### Runner Integration

The `Result` struct already captures metrics for each benchmark case:

```go
type Result struct {
    Name      string
    Duration  time.Duration
    Metrics   benchmark.Metrics   // ← Comprehensive metrics here
    Execution execution.Options
    ExecStats execution.ExecutionStats
    Success   bool
    Error     string
}
```

### Scenario Integration

Scenarios can record metrics as they execute:

```go
func StoreScenarioWithMetrics(ctx BenchmarkContext) error {
    return benchmark.Measure(func() error {
        // Scenario runs and records metrics incrementally
        // Storage metrics from compression backend
        // Throughput calculated from bytes/time
        // CPU time from system monitoring
        // Memory from runtime.ReadMemStats()
        
        return runStore(ctx)
    })()
}
```

## Next Steps

### Planned Enhancements

1. **Compression Codec Metrics**: Track none vs zstd-specific metrics (v1.9-supported codecs)
2. **Block-Level Analysis**: Detailed metrics per block for optimization
3. **Historical Trending**: Compare metrics across release versions
4. **Anomaly Detection**: Alert on regression in compression ratios
5. **Memory Profiling Integration**: Detailed allocation hot paths

### Benchmark Report Integration

Metrics are ready for integration into reporting:

```go
fmt.Fprintf(report, "| Compression Ratio | %.2f | %.2f |\n",
    result.Metrics.CompressionRatio,
    result.Metrics.PhysicalRatio,
)
```

## Related Documentation

- [ARCHITECTURE.md](../ARCHITECTURE.md) — System architecture and compression design
- [BENCHMARK_PHASE4_STEP9.md](../BENCHMARK_PHASE4_STEP9.md) — Benchmark scenarios
- [docs/benchmarking.md](./benchmarking.md) — Benchmarking guide
