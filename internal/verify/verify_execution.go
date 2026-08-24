package verify

import (
	"database/sql"
	"fmt"
)

// VerifySystemWithExecutionResult runs the selected system verification and
// returns only evidence emitted by successful stages in this invocation.
func VerifySystemWithExecutionResult(dbconn *sql.DB, containersDir string, level VerifyLevel) (ExecutionResult, error) {
	ledger := newVerificationExecutionLedger()
	var err error
	switch level {
	case VerifyFast:
		err = verifySystemFastWithContainersDir(dbconn, containersDir, ledger)
	case VerifyStandard:
		err = verifySystemStandardWithContainersDir(dbconn, containersDir, ledger)
	case VerifyFull:
		err = verifySystemFullWithContainersDir(dbconn, containersDir, ledger)
	case VerifyDeep:
		err = verifySystemDeepWithContainersDir(dbconn, containersDir, ledger)
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
	ledger := newVerificationExecutionLedger()
	var err error
	switch level {
	case VerifyFast, VerifyStandard:
		err = verifyFileStandardWithContainersDir(dbconn, fileID, containersDir, ledger)
	case VerifyFull:
		err = verifyFileFullWithContainersDir(dbconn, fileID, containersDir, ledger)
	case VerifyDeep:
		err = verifyFileDeepWithContainersDir(dbconn, fileID, containersDir, ledger)
	default:
		return ExecutionResult{}, fmt.Errorf("invalid file verify level: %d", level)
	}
	if err != nil {
		return ExecutionResult{}, err
	}
	return ledger.result(), nil
}
