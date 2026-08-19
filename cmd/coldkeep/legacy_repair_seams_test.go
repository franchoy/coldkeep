package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/invariants"
	"github.com/franchoy/coldkeep/internal/maintenance"
)

var repairLogicalRefCountsPhase = maintenance.RepairLogicalRefCountsResultRun
var repairChunkLiveRefCountsPhase = maintenance.RepairChunkLiveRefCountsResultRun
var productionRepairCommandEngine = newRepairCommandEngine
var productionRepairDBConnector = connectRepairDBPhase

func init() {
	connectRepairDBPhase = func() (*sql.DB, error) { return sql.Open("sqlite3", ":memory:") }
	newRepairCommandEngine = func(*sql.DB) (engine.Engine, error) {
		return legacyRepairTestEngine{}, nil
	}
}

type legacyRepairTestEngine struct{ engine.Engine }

func (legacyRepairTestEngine) Repair(ctx context.Context, req engine.RepairRequest) (engine.RepairResult, error) {
	result := engine.RepairResult{Targets: make([]engine.RepairTargetResult, 0, len(req.Targets))}
	seen := make(map[engine.RepairTarget]struct{}, len(req.Targets))
	for _, raw := range req.Targets {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		normalized := strings.TrimSpace(raw)
		item := engine.RepairTargetResult{RawTarget: normalized, Target: engine.RepairTarget(normalized)}
		if normalized == "" {
			item.RawTarget = raw
			item.Target = ""
			item.Status = engine.BatchItemFailed
			item.Message = fmt.Sprintf("invalid repair target %q", raw)
			appendLegacyRepairResult(&result, item)
			continue
		}
		if item.Target != engine.RepairTargetRefCounts && item.Target != engine.RepairTargetChunkLiveRefCounts {
			item.RawTarget = raw
			item.Target = ""
			item.Status = engine.BatchItemFailed
			item.Message = fmt.Sprintf("unknown repair target %q", raw)
			appendLegacyRepairResult(&result, item)
			continue
		}
		if _, duplicate := seen[item.Target]; duplicate {
			item.Status = engine.BatchItemSkipped
			item.Message = "duplicate target"
			appendLegacyRepairResult(&result, item)
			continue
		}
		seen[item.Target] = struct{}{}

		var err error
		switch item.Target {
		case engine.RepairTargetRefCounts:
			var value maintenance.RepairLogicalRefCountsResult
			value, err = repairLogicalRefCountsPhase()
			if err == nil {
				item.ScannedRows, item.UpdatedRows, item.OrphanRows = value.ScannedLogicalFiles, value.UpdatedLogicalFiles, value.OrphanPhysicalFileRows
				item.Message = fmt.Sprintf("repaired scanned_logical_files=%d updated_logical_files=%d orphan_physical_file_rows=%d", value.ScannedLogicalFiles, value.UpdatedLogicalFiles, value.OrphanPhysicalFileRows)
			}
		case engine.RepairTargetChunkLiveRefCounts:
			var value maintenance.RepairChunkLiveRefCountsResult
			value, err = repairChunkLiveRefCountsPhase()
			if err == nil {
				item.ScannedRows, item.UpdatedRows = value.ScannedChunks, value.UpdatedChunks
				item.Message = fmt.Sprintf("repaired scanned_chunks=%d updated_chunks=%d", value.ScannedChunks, value.UpdatedChunks)
			}
		}
		if err == nil {
			item.Status = engine.BatchItemOK
		} else {
			item.Status = engine.BatchItemFailed
			item.Message = fmt.Sprintf("repair %s failed: %v", item.Target, err)
			if code, ok := invariants.Code(err); ok {
				item.InvariantCode = code
				item.RecommendedAction = invariants.RecommendedActionForCode(code)
			}
		}
		appendLegacyRepairResult(&result, item)
		if req.FailFast && item.Status == engine.BatchItemFailed {
			break
		}
	}
	return result, nil
}

func appendLegacyRepairResult(result *engine.RepairResult, item engine.RepairTargetResult) {
	result.Targets = append(result.Targets, item)
	switch item.Status {
	case engine.BatchItemOK:
		result.Summary.OK++
	case engine.BatchItemFailed:
		result.Summary.Failed++
	case engine.BatchItemSkipped:
		result.Summary.Skipped++
	}
}
