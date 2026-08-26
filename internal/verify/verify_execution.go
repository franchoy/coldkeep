package verify

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/franchoy/coldkeep/internal/db"
)

// VerifySystemWithExecutionResult runs the selected system verification and
// returns only evidence emitted by successful stages in this invocation.
func VerifySystemWithExecutionResult(dbconn *sql.DB, containersDir string, level VerifyLevel) (ExecutionResult, error) {
	return VerifySystemWithExecutionResultContext(context.Background(), dbconn, containersDir, level)
}

// VerifySystemWithExecutionResultContext runs system verification with caller
// cancellation while retaining invocation-local execution evidence.
func VerifySystemWithExecutionResultContext(parent context.Context, dbconn *sql.DB, containersDir string, level VerifyLevel) (ExecutionResult, error) {
	ctx, cancel := db.NewOperationContext(parent)
	defer cancel()
	ledger := newVerificationExecutionLedger()
	var err error
	switch level {
	case VerifyFast:
		err = verifySystemFastWithContainersDirContext(ctx, dbconn, containersDir, ledger)
	case VerifyStandard:
		err = verifySystemStandardWithContainersDirContext(ctx, dbconn, containersDir, ledger)
	case VerifyFull:
		err = verifySystemFullWithContainersDirContext(ctx, dbconn, containersDir, ledger)
	case VerifyDeep:
		err = verifySystemDeepWithContainersDirContext(ctx, dbconn, containersDir, ledger)
	default:
		return ExecutionResult{}, fmt.Errorf("invalid system verify level: %d", level)
	}
	if err != nil {
		return ExecutionResult{}, err
	}
	return ledger.result(), nil
}

// VerifyFileWithExecutionResult runs the selected file verification and
// returns only evidence emitted by successful stages in this invocation.
func VerifyFileWithExecutionResult(dbconn *sql.DB, fileID int, containersDir string, level VerifyLevel) (ExecutionResult, error) {
	return VerifyFileWithExecutionResultContext(context.Background(), dbconn, fileID, containersDir, level)
}

// VerifyFileWithExecutionResultContext runs file verification with caller
// cancellation while retaining invocation-local execution evidence.
func VerifyFileWithExecutionResultContext(parent context.Context, dbconn *sql.DB, fileID int, containersDir string, level VerifyLevel) (ExecutionResult, error) {
	ctx, cancel := db.NewOperationContext(parent)
	defer cancel()
	ledger := newVerificationExecutionLedger()
	var err error
	switch level {
	case VerifyFast, VerifyStandard:
		err = verifyFileStandardWithContainersDirContext(ctx, dbconn, fileID, containersDir, ledger)
	case VerifyFull:
		err = verifyFileFullWithContainersDirContext(ctx, dbconn, fileID, containersDir, ledger)
	case VerifyDeep:
		err = verifyFileDeepWithContainersDirContext(ctx, dbconn, fileID, containersDir, ledger)
	default:
		return ExecutionResult{}, fmt.Errorf("invalid file verify level: %d", level)
	}
	if err != nil {
		return ExecutionResult{}, err
	}
	return ledger.result(), nil
}
