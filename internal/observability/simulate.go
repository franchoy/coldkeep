package observability

import (
	"context"
	"fmt"

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
			ContainerID:       c.ContainerID,
			Filename:          c.Filename,
			TotalBytes:        c.TotalBytes,
			ReclaimableBytes:  c.ReclaimableBytes,
			ReclaimableChunks: c.ReclaimableChunks,
			WouldDeleteFile:   c.WouldDeleteFile,
		}
	}

	result := &SimulationResult{
		GeneratedAtUTC: s.now(),
		Kind:           SimulationKindGC,
		Exact:          true,
		Mutated:        false,
		Summary: map[string]any{
			"total_chunks":        plan.TotalChunks,
			"reachable_chunks":    plan.ReachableChunks,
			"unreachable_chunks":  plan.UnreachableChunks,
			"reclaimable_bytes":   plan.ReclaimableBytes,
			"affected_containers": len(plan.AffectedContainers),
		},
		GC: &GCSimulationResult{
			TotalChunks:        plan.TotalChunks,
			ReachableChunks:    plan.ReachableChunks,
			UnreachableChunks:  plan.UnreachableChunks,
			ReclaimableBytes:   plan.ReclaimableBytes,
			AffectedContainers: impacts,
		},
	}

	return result, nil
}
