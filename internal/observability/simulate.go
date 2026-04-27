package observability

import (
	"context"
	"fmt"
	"time"

	"github.com/franchoy/coldkeep/internal/gc"
	"github.com/franchoy/coldkeep/internal/graph"
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
	emitTrace(opts.Trace, TraceEvent{
		Step:    "simulate.gc.start",
		Message: "starting gc simulation",
		Metadata: map[string]any{
			"assumed_deleted_snapshots": len(opts.AssumeDeletedSnapshots),
		},
	})

	rootMetadata := map[string]any{
		"excluded_snapshots": len(opts.AssumeDeletedSnapshots),
	}
	if s != nil && s.graph != nil {
		if roots, rootsErr := s.graph.GCRoots(ctx, graph.GCRootOptions{ExcludeSnapshots: opts.AssumeDeletedSnapshots}); rootsErr == nil {
			rootMetadata["root_count"] = len(roots)
		}
	}
	emitTrace(opts.Trace, TraceEvent{
		Step:     "simulate.gc.roots.load",
		Message:  "loading gc roots",
		Metadata: rootMetadata,
	})

	for _, snapshotID := range opts.AssumeDeletedSnapshots {
		emitTrace(opts.Trace, TraceEvent{
			Step:     "simulate.gc.assumption.exclude_snapshot",
			Entity:   "snapshot",
			EntityID: snapshotID,
			Message:  "excluding snapshot from simulation roots",
		})
	}

	emitTrace(opts.Trace, TraceEvent{
		Step:    "simulate.gc.mark.start",
		Message: "starting reachable chunk mark traversal",
	})

	plan, err := gc.BuildPlan(ctx, s.db, gc.PlanOptions{
		AssumeDeletedSnapshots: opts.AssumeDeletedSnapshots,
	})
	if err != nil {
		return nil, fmt.Errorf("gc simulation: build plan: %w", err)
	}
	emitTrace(opts.Trace, TraceEvent{
		Step:    "simulate.gc.mark.complete",
		Message: "reachable chunk set computed",
		Metadata: map[string]any{
			"reachable_chunks": plan.ReachableChunks,
		},
	})
	emitTrace(opts.Trace, TraceEvent{
		Step:    "simulate.gc.unreachable.compute",
		Message: "computed unreachable chunk set and logical reclaimability",
		Metadata: map[string]any{
			"unreachable_chunks":          plan.Summary.UnreachableChunks,
			"logically_reclaimable_bytes": plan.Summary.LogicallyReclaimableBytes,
		},
	})

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
	emitTrace(opts.Trace, TraceEvent{
		Step:    "simulate.gc.container_impact.compute",
		Message: "computed per-container reclaim impact",
		Metadata: map[string]any{
			"affected_containers":          len(plan.AffectedContainers),
			"fully_reclaimable_containers": plan.Summary.FullyReclaimableContainers,
			"partially_dead_containers":    plan.Summary.PartiallyDeadContainers,
		},
	})
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
	emitTrace(opts.Trace, TraceEvent{
		Step:    "simulate.gc.complete",
		Message: "completed gc simulation",
		Metadata: map[string]any{
			"reachable_chunks":             result.GC.Summary.ReachableChunks,
			"unreachable_chunks":           result.GC.Summary.UnreachableChunks,
			"affected_containers":          len(result.GC.Containers),
			"warnings":                     len(result.GC.Warnings),
			"physically_reclaimable_bytes": result.GC.Summary.PhysicallyReclaimableBytes,
		},
	})

	return result, nil
}
