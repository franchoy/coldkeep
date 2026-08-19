package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/franchoy/coldkeep/internal/invariants"
	"github.com/franchoy/coldkeep/internal/maintenance"
)

func (e *DefaultEngine) Repair(ctx context.Context, req RepairRequest) (RepairResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	result := RepairResult{Targets: make([]RepairTargetResult, 0, len(req.Targets))}
	if len(req.Targets) == 0 {
		return result, TranslateErrorAs("repair", ErrorInvalidArgument, fmt.Errorf("repair requires at least one target"))
	}
	if e == nil || e.config.DB == nil {
		return result, TranslateError("repair", fmt.Errorf("repair requires injected database"))
	}

	seen := make(map[RepairTarget]struct{}, len(req.Targets))
	for _, raw := range req.Targets {
		if err := ctx.Err(); err != nil {
			finalizeRepairSummary(&result)
			return result, TranslateError("repair", err)
		}
		item, executable := prepareRepairTarget(raw, seen)
		if !executable {
			appendRepairTarget(&result, item)
			continue
		}
		seen[item.Target] = struct{}{}
		item, executionErr := e.executeRepairTarget(ctx, item)
		appendRepairTarget(&result, item)
		if executionErr != nil && (errors.Is(executionErr, context.Canceled) || errors.Is(executionErr, context.DeadlineExceeded)) {
			return result, TranslateError("repair", executionErr)
		}
		if req.FailFast && item.Status == BatchItemFailed {
			break
		}
	}
	return result, nil
}

func prepareRepairTarget(raw string, seen map[RepairTarget]struct{}) (RepairTargetResult, bool) {
	normalized := strings.TrimSpace(raw)
	if normalized == "" {
		return RepairTargetResult{
			RawTarget: raw, Status: BatchItemFailed,
			Message: fmt.Sprintf("invalid repair target %q", raw),
		}, false
	}
	target := RepairTarget(normalized)
	if target != RepairTargetRefCounts && target != RepairTargetChunkLiveRefCounts {
		return RepairTargetResult{
			RawTarget: raw, Status: BatchItemFailed,
			Message: fmt.Sprintf("unknown repair target %q", raw),
		}, false
	}
	if _, duplicate := seen[target]; duplicate {
		return RepairTargetResult{
			RawTarget: normalized, Target: target, Status: BatchItemSkipped,
			Message: "duplicate target",
		}, false
	}
	return RepairTargetResult{RawTarget: normalized, Target: target}, true
}

func (e *DefaultEngine) executeRepairTarget(ctx context.Context, item RepairTargetResult) (RepairTargetResult, error) {
	var err error
	switch item.Target {
	case RepairTargetRefCounts:
		var outcome maintenance.RepairLogicalRefCountsResult
		outcome, err = maintenance.RepairLogicalRefCountsResultWithDBContext(ctx, e.config.DB)
		if err == nil {
			item.ScannedRows = outcome.ScannedLogicalFiles
			item.UpdatedRows = outcome.UpdatedLogicalFiles
			item.OrphanRows = outcome.OrphanPhysicalFileRows
			item.Message = fmt.Sprintf(
				"repaired scanned_logical_files=%d updated_logical_files=%d orphan_physical_file_rows=%d",
				outcome.ScannedLogicalFiles, outcome.UpdatedLogicalFiles, outcome.OrphanPhysicalFileRows,
			)
		}
	case RepairTargetChunkLiveRefCounts:
		var outcome maintenance.RepairChunkLiveRefCountsResult
		outcome, err = maintenance.RepairChunkLiveRefCountsResultWithDBContext(ctx, e.config.DB)
		if err == nil {
			item.ScannedRows = outcome.ScannedChunks
			item.UpdatedRows = outcome.UpdatedChunks
			item.Message = fmt.Sprintf("repaired scanned_chunks=%d updated_chunks=%d", outcome.ScannedChunks, outcome.UpdatedChunks)
		}
	default:
		err = fmt.Errorf("unknown repair target %q", item.Target)
	}
	if err == nil {
		item.Status = BatchItemOK
		return item, nil
	}

	item.Status = BatchItemFailed
	item.Message = fmt.Sprintf("repair %s failed: %v", item.Target, err)
	if code, ok := invariants.Code(err); ok {
		item.InvariantCode = code
		item.RecommendedAction = invariants.RecommendedActionForCode(code)
	}
	return item, err
}

func appendRepairTarget(result *RepairResult, item RepairTargetResult) {
	result.Targets = append(result.Targets, item)
	switch item.Status {
	case BatchItemOK:
		result.Summary.OK++
	case BatchItemFailed:
		result.Summary.Failed++
	case BatchItemSkipped:
		result.Summary.Skipped++
	}
}

func finalizeRepairSummary(result *RepairResult) {
	result.Summary = BatchSummary{}
	for _, item := range result.Targets {
		switch item.Status {
		case BatchItemOK:
			result.Summary.OK++
		case BatchItemFailed:
			result.Summary.Failed++
		case BatchItemSkipped:
			result.Summary.Skipped++
		}
	}
}
