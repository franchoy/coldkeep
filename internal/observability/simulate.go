package observability

import (
	"context"
	"fmt"
	"time"

	"github.com/franchoy/coldkeep/internal/gc"
)

const SimulationKindGC = "gc"

func (s *Service) Simulate(ctx context.Context, opts SimulationOptions) (*SimulationResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	switch opts.Kind {
	case SimulationKindGC:
		return s.simulateGC(ctx, opts)
	default:
		return nil, fmt.Errorf("%w: unsupported simulation kind %q", ErrInvalidTarget, opts.Kind)
	}
}

func (s *Service) simulateGC(ctx context.Context, opts SimulationOptions) (*SimulationResult, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("observability service requires non-nil db")
	}

	plan, err := gc.BuildPlan(ctx, s.db, gc.PlanOptions{
		AssumeDeletedSnapshots: opts.AssumeDeletedSnapshots,
	})
	if err != nil {
		return nil, fmt.Errorf("gc simulation: build plan: %w", err)
	}

	impacts := make([]ContainerSimulationImpact, len(plan.AffectedContainers))
	for i, c := range plan.AffectedContainers {
		impacts[i] = ContainerSimulationImpact{
			ContainerID:        c.ContainerID,
			Filename:           c.Filename,
			TotalBytes:         c.TotalBytes,
			LiveBytesAfterGC:   c.LiveBytesAfterGC,
			ReclaimableBytes:   c.ReclaimableBytes,
			ReclaimableChunks:  c.ReclaimableChunks,
			TotalChunks:        c.TotalChunks,
			FullyReclaimable:   c.FullyReclaimable,
			RequiresCompaction: c.RequiresCompaction,
		}
	}
	warnings := make([]ObservationWarning, len(plan.Warnings))
	for i, warning := range plan.Warnings {
		warnings[i] = ObservationWarning{Code: warning.Code, Message: warning.Message}
	}
	deletedSnapshots := append([]string(nil), opts.AssumeDeletedSnapshots...)
	var generatedAt time.Time

	result := &SimulationResult{
		GeneratedAtUTC: generatedAt,
		Kind:           SimulationKindGC,
		Exact:          true,
		Mutated:        false,
		Summary: map[string]any{
			"total_chunks":                 plan.TotalChunks,
			"reachable_chunks":             plan.ReachableChunks,
			"unreachable_chunks":           plan.Summary.UnreachableChunks,
			"logically_reclaimable_bytes":  plan.Summary.LogicallyReclaimableBytes,
			"physically_reclaimable_bytes": plan.Summary.PhysicallyReclaimableBytes,
			"fully_reclaimable_containers": plan.Summary.FullyReclaimableContainers,
			"partially_dead_containers":    plan.Summary.PartiallyDeadContainers,
			"affected_containers":          len(plan.AffectedContainers),
		},
		GC: &GCSimulationResult{
			GeneratedAtUTC: generatedAt,
			Kind:           SimulationKindGC,
			Exact:          true,
			Mutated:        false,
			Assumptions: GCSimulationAssumptions{
				DeletedSnapshots: deletedSnapshots,
			},
			Summary: GCSimulationSummary{
				ReachableChunks:            plan.ReachableChunks,
				UnreachableChunks:          plan.Summary.UnreachableChunks,
				LogicallyReclaimableBytes:  plan.Summary.LogicallyReclaimableBytes,
				PhysicallyReclaimableBytes: plan.Summary.PhysicallyReclaimableBytes,
				FullyReclaimableContainers: plan.Summary.FullyReclaimableContainers,
				PartiallyDeadContainers:    plan.Summary.PartiallyDeadContainers,
			},
			Containers: impacts,
			Warnings:   warnings,
		},
		Warnings: warnings,
	}

	return result, nil
}
