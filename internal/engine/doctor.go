package engine

import (
	"context"
	"fmt"

	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/invariants"
	"github.com/franchoy/coldkeep/internal/maintenance"
)

func (e *DefaultEngine) Doctor(ctx context.Context, req DoctorRequest) (_ DoctorResult, outErr error) {
	defer func() { outErr = TranslateError("doctor", outErr) }()
	if ctx == nil {
		ctx = context.Background()
	}
	level, err := normalizeDoctorVerifyLevel(req.VerifyLevel)
	if err != nil {
		return DoctorResult{}, TranslateErrorAs("doctor", ErrorInvalidArgument, err)
	}
	result := DoctorResult{VerifyLevel: level}
	if err := ctx.Err(); err != nil {
		return result, TranslateError("doctor", err)
	}
	if e == nil || e.config.DB == nil {
		return result, doctorStageError(ErrorOperationFailed, "doctor requires injected database", fmt.Errorf("doctor requires injected database"))
	}

	recoveryResult, err := e.runDoctorRecovery(ctx)
	result.Recovery = recoveryResult
	if err != nil {
		result.RecoveryStatus = "error"
		result.FailedStage = DoctorStageRecovery
		code := ErrorRecoveryFailed
		if CodeOf(err) == ErrorCancelled {
			code = ErrorCancelled
		}
		return result, doctorStageError(code, fmt.Sprintf("doctor recovery phase failed: %v", err), err)
	}
	result.RecoveryStatus = "ok"

	schemaVersion, err := e.runDoctorSchema()
	if err != nil {
		result.SchemaStatus = "error"
		result.FailedStage = DoctorStageSchema
		return result, doctorStageError(ErrorOperationFailed, fmt.Sprintf("doctor schema/version check failed: %v", err), err)
	}
	result.SchemaVersion = schemaVersion
	result.SchemaStatus = "ok"

	if err := e.runDoctorVerification(ctx, level); err != nil {
		result.VerifyStatus = "error"
		result.FailedStage = DoctorStageVerify
		code := ErrorVerificationFailed
		if CodeOf(err) == ErrorCancelled {
			code = ErrorCancelled
		}
		return result, doctorStageError(code, fmt.Sprintf("doctor verify phase failed: %v", err), err)
	}
	result.VerifyStatus = "ok"

	physicalAudit, snapshotAudit, err := e.runDoctorAudit()
	if err != nil {
		result.FailedStage = DoctorStageAudit
		return result, doctorStageError(ErrorVerificationFailed, fmt.Sprintf("doctor audit summary phase failed: %v", err), err)
	}
	result.PhysicalAudit = physicalAudit
	result.SnapshotAudit = snapshotAudit
	return result, nil
}

func (e *DefaultEngine) runDoctorRecovery(ctx context.Context) (RecoverResult, error) {
	if e.doctorRecover != nil {
		return e.doctorRecover(ctx)
	}
	return e.Recover(ctx, RecoverRequest{})
}

func (e *DefaultEngine) runDoctorSchema() (int64, error) {
	if e.doctorSchema != nil {
		return e.doctorSchema(e.config.DB)
	}
	return db.CurrentSchemaVersion(e.config.DB)
}

func (e *DefaultEngine) runDoctorVerification(ctx context.Context, level string) error {
	if e.doctorVerify != nil {
		return e.doctorVerify(ctx, level)
	}
	_, err := e.Verify(ctx, VerifyRequest{Target: "system", Level: level})
	return err
}

func (e *DefaultEngine) runDoctorAudit() (DoctorPhysicalAudit, DoctorSnapshotAudit, error) {
	if e.doctorAudit != nil {
		return e.doctorAudit(e.config.DB)
	}
	audit, err := maintenance.CollectSystemAuditSummaryWithDB(e.config.DB)
	if err != nil {
		return DoctorPhysicalAudit{}, DoctorSnapshotAudit{}, err
	}
	return DoctorPhysicalAudit{
			OrphanPhysicalFileRows:    audit.Physical.OrphanPhysicalFileRows,
			LogicalRefCountMismatches: audit.Physical.LogicalRefCountMismatches,
			NegativeLogicalRefCounts:  audit.Physical.NegativeLogicalRefCounts,
		}, DoctorSnapshotAudit{
			SnapshotFileRows:               audit.Snapshot.SnapshotFileRows,
			OrphanSnapshotPathRefs:         audit.Snapshot.OrphanSnapshotPathRefs,
			DuplicateSnapshotPathPairs:     audit.Snapshot.DuplicateSnapshotPathPairs,
			SnapshotReferencedLogicalFiles: audit.Snapshot.SnapshotReferencedLogicalFiles,
			SnapshotOnlyLogicalFiles:       audit.Snapshot.SnapshotOnlyLogicalFiles,
			SharedLogicalFiles:             audit.Snapshot.SharedLogicalFiles,
			OrphanSnapshotLogicalRefs:      audit.Snapshot.OrphanSnapshotLogicalRefs,
			InvalidLifecycleStates:         audit.Snapshot.InvalidSnapshotLifecycleStates,
			RetainedMissingChunkGraph:      audit.Snapshot.RetainedMissingChunkGraph,
		}, nil
}

func normalizeDoctorVerifyLevel(level string) (string, error) {
	switch level {
	case "":
		return "standard", nil
	case "standard", "full", "deep":
		return level, nil
	default:
		return "", fmt.Errorf("invalid doctor verify level %q: must be standard, full, or deep", level)
	}
}

func doctorStageError(code ErrorCode, message string, cause error) error {
	invariantCode := ""
	if value, ok := invariants.Code(cause); ok {
		invariantCode = value
	}
	return NewError(code, "doctor", message, invariantCode, cause)
}
