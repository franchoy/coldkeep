package observability

import (
	"context"
	"testing"
	"time"

	"github.com/franchoy/coldkeep/internal/maintenance"
)

func TestStatsMapsMaintenanceResultToStableModel(t *testing.T) {
	fixedNow := time.Date(2026, time.April, 26, 10, 0, 0, 0, time.UTC)

	svc := NewService(
		WithNowFunc(func() time.Time { return fixedNow }),
		WithStatsRunner(func() (*maintenance.StatsResult, error) {
			return &maintenance.StatsResult{
				TotalFiles:               7,
				CompletedFiles:           5,
				ProcessingFiles:          1,
				AbortedFiles:             1,
				TotalLogicalSizeBytes:    700,
				CompletedSizeBytes:       500,
				EstimatedDedupRatioPct:   42.5,
				ActiveWriteChunker:       "v2-fastcdc",
				TotalChunks:              11,
				CompletedChunks:          9,
				CompletedChunkBytes:      450,
				ChunkCountsByVersion:     map[string]int64{"v2-fastcdc": 11},
				ChunkBytesByVersion:      map[string]int64{"v2-fastcdc": 450},
				TotalChunkReferences:     30,
				UniqueReferencedChunks:   11,
				TotalContainers:          3,
				HealthyContainers:        2,
				QuarantineContainers:     1,
				TotalContainerBytes:      900,
				HealthyContainerBytes:    800,
				QuarantineContainerBytes: 100,
				LiveBlockBytes:           400,
				DeadBlockBytes:           50,
				FragmentationRatioPct:    6.25,
				Containers: []maintenance.ContainerStatRecord{
					{
						ID:           10,
						Filename:     "container_10.bin",
						TotalBytes:   400,
						LiveBytes:    300,
						DeadBytes:    100,
						Quarantine:   false,
						LiveRatioPct: 75,
					},
				},
				SnapshotRetention: maintenance.SnapshotRetentionStats{
					CurrentOnlyLogicalFiles:        1,
					CurrentOnlyBytes:               10,
					SnapshotReferencedLogicalFiles: 3,
					SnapshotReferencedBytes:        60,
					SnapshotOnlyLogicalFiles:       2,
					SnapshotOnlyBytes:              50,
					SharedLogicalFiles:             1,
					SharedBytes:                    10,
				},
			}, nil
		}),
	)

	result, err := svc.Stats(context.Background())
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}

	if result.GeneratedAtUTC != fixedNow {
		t.Fatalf("generated_at_utc mismatch: got %s want %s", result.GeneratedAtUTC, fixedNow)
	}
	if result.Repository.ActiveWriteChunker != "v2-fastcdc" {
		t.Fatalf("unexpected active_write_chunker: %q", result.Repository.ActiveWriteChunker)
	}
	if result.Logical.TotalFiles != 7 || result.Logical.CompletedFiles != 5 {
		t.Fatalf("unexpected logical stats: %+v", result.Logical)
	}
	if result.Chunks.TotalReferences != 30 || result.Chunks.UniqueReferenced != 11 {
		t.Fatalf("unexpected chunk refs: %+v", result.Chunks)
	}
	if len(result.Containers.Records) != 1 {
		t.Fatalf("expected one container record, got %d", len(result.Containers.Records))
	}
	if result.Physical.TotalPhysicalFiles != 0 {
		t.Fatalf("expected phase-1 default physical total=0, got %d", result.Physical.TotalPhysicalFiles)
	}
	if result.Retention.SnapshotOnlyLogicalFiles != 2 {
		t.Fatalf("unexpected retention stats: %+v", result.Retention)
	}
}

func TestStatsHandlesNilMaintenanceResult(t *testing.T) {
	svc := NewService(
		WithNowFunc(func() time.Time {
			return time.Date(2026, time.April, 26, 11, 0, 0, 0, time.UTC)
		}),
		WithStatsRunner(func() (*maintenance.StatsResult, error) {
			return nil, nil
		}),
	)

	result, err := svc.Stats(context.Background())
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if result.GeneratedAtUTC.IsZero() {
		t.Fatal("expected generated_at_utc to be populated")
	}
	if result.Chunks.CountsByVersion != nil {
		t.Fatal("expected zero-value result when maintenance payload is nil")
	}
}
