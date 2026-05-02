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
