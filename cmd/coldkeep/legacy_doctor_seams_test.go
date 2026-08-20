package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/recovery"
	"github.com/franchoy/coldkeep/internal/verify"
)

var doctorRecoveryPhase = runRecoveryThroughEngine
var doctorSchemaVersionPhase = db.QueryCurrentSchemaVersion
var doctorVerifyPhase = maintenance.VerifyCommandWithContainersDir
var doctorSystemAuditPhase = maintenance.CollectSystemAuditSummary

func init() {
	connectDoctorDBPhase = func() (*sql.DB, error) { return sql.Open("sqlite3", ":memory:") }
	newDoctorCommandEngine = func(*sql.DB, string) (engine.Engine, error) {
		return legacyDoctorTestEngine{}, nil
	}
}

type legacyDoctorTestEngine struct{ engine.Engine }

func (legacyDoctorTestEngine) Doctor(_ context.Context, req engine.DoctorRequest) (engine.DoctorResult, error) {
	level, err := legacyDoctorVerifyLevel(req.VerifyLevel)
	if err != nil {
		return engine.DoctorResult{}, err
	}
	result := engine.DoctorResult{VerifyLevel: req.VerifyLevel}
	recoveryReport, err := doctorRecoveryPhase(container.ContainersDir)
	result.Recovery = recoverResultForDoctorTest(recoveryReport)
	if err != nil {
		result.RecoveryStatus, result.FailedStage = "error", engine.DoctorStageRecovery
		return result, fmt.Errorf("doctor recovery phase failed: %w", err)
	}
	result.RecoveryStatus = "ok"
	result.SchemaVersion, err = doctorSchemaVersionPhase()
	if err != nil {
		result.SchemaStatus, result.FailedStage = "error", engine.DoctorStageSchema
		return result, fmt.Errorf("doctor schema/version check failed: %w", err)
	}
	result.SchemaStatus = "ok"
	if err := doctorVerifyPhase(container.ContainersDir, "system", 0, level); err != nil {
		result.VerifyStatus, result.FailedStage = "error", engine.DoctorStageVerify
		return result, fmt.Errorf("doctor verify phase failed: %w", err)
	}
	result.VerifyStatus = "ok"
	audit, err := doctorSystemAuditPhase()
	if err != nil {
		result.FailedStage = engine.DoctorStageAudit
		return result, fmt.Errorf("doctor audit summary phase failed: %w", err)
	}
	result.PhysicalAudit = engine.DoctorPhysicalAudit{
		OrphanPhysicalFileRows:    audit.Physical.OrphanPhysicalFileRows,
		LogicalRefCountMismatches: audit.Physical.LogicalRefCountMismatches,
		NegativeLogicalRefCounts:  audit.Physical.NegativeLogicalRefCounts,
	}
	result.SnapshotAudit = engine.DoctorSnapshotAudit{
		SnapshotFileRows:               audit.Snapshot.SnapshotFileRows,
		OrphanSnapshotPathRefs:         audit.Snapshot.OrphanSnapshotPathRefs,
		DuplicateSnapshotPathPairs:     audit.Snapshot.DuplicateSnapshotPathPairs,
		SnapshotReferencedLogicalFiles: audit.Snapshot.SnapshotReferencedLogicalFiles,
		SnapshotOnlyLogicalFiles:       audit.Snapshot.SnapshotOnlyLogicalFiles,
		SharedLogicalFiles:             audit.Snapshot.SharedLogicalFiles,
		OrphanSnapshotLogicalRefs:      audit.Snapshot.OrphanSnapshotLogicalRefs,
		InvalidLifecycleStates:         audit.Snapshot.InvalidSnapshotLifecycleStates,
		RetainedMissingChunkGraph:      audit.Snapshot.RetainedMissingChunkGraph,
	}
	return result, nil
}

func legacyDoctorVerifyLevel(level string) (verify.VerifyLevel, error) {
	switch level {
	case "standard":
		return verify.VerifyStandard, nil
	case "full":
		return verify.VerifyFull, nil
	case "deep":
		return verify.VerifyDeep, nil
	default:
		return 0, fmt.Errorf("invalid doctor verify level %q", level)
	}
}

func recoverResultForDoctorTest(report recovery.Report) engine.RecoverResult {
	return engine.RecoverResult{
		AbortedLogicalFiles: report.AbortedLogicalFiles, AbortedChunks: report.AbortedChunks,
		QuarantinedMissing: report.QuarantinedMissing, QuarantinedCorruptTail: report.QuarantinedCorruptTail,
		QuarantinedOrphan: report.QuarantinedOrphan, SkippedDirEntries: report.SkippedDirEntries,
		CheckedContainerRecord: report.CheckedContainerRecord, CheckedDiskFiles: report.CheckedDiskFiles,
		SealingCompleted: report.SealingCompleted, SealingQuarantined: report.SealingQuarantined,
	}
}
