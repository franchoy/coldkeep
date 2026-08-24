package maintenance

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/verify"
)

type SystemAuditSummary struct {
	Physical verify.PhysicalFileIntegritySummary
	Snapshot verify.SnapshotReachabilityIntegritySummary
}

func CollectSystemAuditSummary() (SystemAuditSummary, error) {
	dbconn, err := db.ConnectDB()
	if err != nil {
		return SystemAuditSummary{}, fmt.Errorf("failed to connect to database: %w", err)
	}
	defer func() { _ = dbconn.Close() }()

	return CollectSystemAuditSummaryWithDB(dbconn)
}

// CollectSystemAuditSummaryWithDB collects the Doctor audit summaries through
// a caller-owned database connection.
func CollectSystemAuditSummaryWithDB(dbconn *sql.DB) (SystemAuditSummary, error) {
	physical, err := verify.CheckPhysicalFileGraphIntegrity(dbconn)
	if err != nil {
		return SystemAuditSummary{}, err
	}

	snapshot, err := verify.CheckSnapshotReachabilityIntegrity(dbconn)
	if err != nil {
		return SystemAuditSummary{}, err
	}

	return SystemAuditSummary{
		Physical: physical,
		Snapshot: snapshot,
	}, nil
}

func VerifyCommandWithContainersDir(containersDir string, target string, fileID int, verifyLevel verify.VerifyLevel) error {

	dbconn, err := db.ConnectDB()
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer func() { _ = dbconn.Close() }()

	return VerifyCommandWithDBAndContainersDir(dbconn, containersDir, target, fileID, verifyLevel)
}

// VerifyCommandWithDBAndContainersDir runs verification using the provided
// database connection instead of opening a new one. The caller owns the
// connection lifetime.
//
// This is the database-aware entry point used by the engine facade so that
// engine-owned dependencies are honored. VerifyCommandWithContainersDir wraps
// this function for callers that rely on the global database connection.
func VerifyCommandWithDBAndContainersDir(dbconn *sql.DB, containersDir string, target string, fileID int, verifyLevel verify.VerifyLevel) error {
	_, err := VerifyCommandWithDBAndContainersDirResult(dbconn, containersDir, target, fileID, verifyLevel)
	return err
}

// VerifyCommandWithDBAndContainersDirResult preserves the error-only command
// API while providing invocation-local verification evidence to Engine.
func VerifyCommandWithDBAndContainersDirResult(dbconn *sql.DB, containersDir string, target string, fileID int, verifyLevel verify.VerifyLevel) (verify.ExecutionResult, error) {
	switch target {
	case "system":
		return verifySystemResult(dbconn, containersDir, verifyLevel)
	case "file":
		return verifyFileResult(dbconn, containersDir, fileID, verifyLevel)
	default:
		return verify.ExecutionResult{}, fmt.Errorf("invalid target for verify command: %s", target)
	}
}

func verifySystem(dbconn *sql.DB, containersDir string, verifyLevel verify.VerifyLevel) error {
	_, err := verifySystemResult(dbconn, containersDir, verifyLevel)
	return err
}

func verifySystemResult(dbconn *sql.DB, containersDir string, verifyLevel verify.VerifyLevel) (verify.ExecutionResult, error) {
	if verifyLevel < verify.VerifyFast || verifyLevel > verify.VerifyDeep {
		return verify.ExecutionResult{}, fmt.Errorf("invalid system verify level: %d", verifyLevel)
	}

	result, err := verify.VerifySystemWithExecutionResult(dbconn, containersDir, verifyLevel)
	if err != nil {
		return verify.ExecutionResult{}, fmt.Errorf("system %s verification failed: %w", verifyLevelLabel(verifyLevel), err)
	}
	return result, nil
}

func verifyFile(dbconn *sql.DB, containersDir string, fileID int, verifyLevel verify.VerifyLevel) error {
	_, err := verifyFileResult(dbconn, containersDir, fileID, verifyLevel)
	return err
}

func verifyFileResult(dbconn *sql.DB, containersDir string, fileID int, verifyLevel verify.VerifyLevel) (verify.ExecutionResult, error) {
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	// Verify that the file ID exists before dispatching to deeper validation.
	var exists bool
	err := dbconn.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM logical_file WHERE id = $1)", fileID).Scan(&exists)
	if err != nil {
		return verify.ExecutionResult{}, fmt.Errorf("failed to check if file exists: %w", err)
	}
	if !exists {
		return verify.ExecutionResult{}, fmt.Errorf("file with ID %d does not exist", fileID)
	}

	if verifyLevel < verify.VerifyFast || verifyLevel > verify.VerifyDeep {
		return verify.ExecutionResult{}, fmt.Errorf("invalid file verify level: %d", verifyLevel)
	}
	result, err := verify.VerifyFileWithExecutionResult(dbconn, fileID, containersDir, verifyLevel)
	if err != nil {
		return verify.ExecutionResult{}, fmt.Errorf("file %s verification failed: %w", verifyLevelLabel(verifyLevel), err)
	}
	return result, nil
}

func verifyLevelLabel(level verify.VerifyLevel) string {
	switch level {
	case verify.VerifyFast:
		return "fast"
	case verify.VerifyStandard:
		return "standard"
	case verify.VerifyFull:
		return "full"
	case verify.VerifyDeep:
		return "deep"
	default:
		return "invalid"
	}
}
