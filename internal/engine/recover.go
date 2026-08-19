package engine

import (
	"context"
	"fmt"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/recovery"
)

// Recover executes real corrective recovery. It intentionally has no dry-run
// mode: startup and explicit recovery share this exact operation.
func (e *DefaultEngine) Recover(ctx context.Context, _ RecoverRequest) (RecoverResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return RecoverResult{}, TranslateError("recover", err)
	}
	if e == nil || e.config.DB == nil {
		return RecoverResult{}, TranslateErrorAs("recover", ErrorRecoveryFailed, fmt.Errorf("recover requires injected database"))
	}
	containersDir := e.config.ContainerDir
	if containersDir == "" {
		containersDir = container.ContainersDir
	}
	report, err := recovery.SystemRecoveryReportWithDBContext(ctx, e.config.DB, containersDir)
	result := recoverResultFromReport(report)
	if err != nil {
		return result, TranslateErrorAs("recover", ErrorRecoveryFailed, err)
	}
	return result, nil
}

func recoverResultFromReport(report recovery.Report) RecoverResult {
	return RecoverResult{
		AbortedLogicalFiles: report.AbortedLogicalFiles, AbortedChunks: report.AbortedChunks,
		QuarantinedMissing: report.QuarantinedMissing, QuarantinedCorruptTail: report.QuarantinedCorruptTail,
		QuarantinedOrphan: report.QuarantinedOrphan, SkippedDirEntries: report.SkippedDirEntries,
		CheckedContainerRecord: report.CheckedContainerRecord, CheckedDiskFiles: report.CheckedDiskFiles,
		SealingCompleted: report.SealingCompleted, SealingQuarantined: report.SealingQuarantined,
	}
}
