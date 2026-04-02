package maintenance

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/verify"
)

func VerifyCommand(target string, fileID int, verifyLevel verify.VerifyLevel) error {
	return VerifyCommandWithContainersDir(container.ContainersDir, target, fileID, verifyLevel)
}

func VerifyCommandWithContainersDir(containersDir string, target string, fileID int, verifyLevel verify.VerifyLevel) error {

	dbconn, err := db.ConnectDB()
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer func() { _ = dbconn.Close() }()

	switch target {
	case "system":
		return verifySystem(dbconn, containersDir, verifyLevel)
	case "file":
		return verifyFile(dbconn, containersDir, fileID, verifyLevel)
	default:
		return fmt.Errorf("invalid target for verify command: %s", target)
	}
}

func verifySystem(dbconn *sql.DB, containersDir string, verifyLevel verify.VerifyLevel) error {

	switch verifyLevel {
	case verify.VerifyStandard:
		if err := verify.VerifySystemStandardWithContainersDir(dbconn, containersDir); err != nil {
			return fmt.Errorf("system standard verification failed: %w", err)
		}
	case verify.VerifyFull:
		if err := verify.VerifySystemFullWithContainersDir(dbconn, containersDir); err != nil {
			return fmt.Errorf("system full verification failed: %w", err)
		}
	case verify.VerifyDeep:
		if err := verify.VerifySystemDeepWithContainersDir(dbconn, containersDir); err != nil {
			return fmt.Errorf("system deep verification failed: %w", err)
		}
	default:
		return fmt.Errorf("invalid system verify level: %d", verifyLevel)
	}

	return nil
}

func verifyFile(dbconn *sql.DB, containersDir string, fileID int, verifyLevel verify.VerifyLevel) error {
	ctx, cancel := db.NewOperationContext(context.Background())
	defer cancel()

	// Verify that the file ID exists before dispatching to deeper validation.
	var exists bool
	err := dbconn.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM logical_file WHERE id = $1)", fileID).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check if file exists: %w", err)
	}
	if !exists {
		return fmt.Errorf("file with ID %d does not exist", fileID)
	}

	switch verifyLevel {
	case verify.VerifyStandard:
		if err := verify.VerifyFileStandardWithContainersDir(dbconn, fileID, containersDir); err != nil {
			return fmt.Errorf("file standard verification failed: %w", err)
		}
	case verify.VerifyFull:
		if err := verify.VerifyFileFullWithContainersDir(dbconn, fileID, containersDir); err != nil {
			return fmt.Errorf("file full verification failed: %w", err)
		}
	case verify.VerifyDeep:
		if err := verify.VerifyFileDeepWithContainersDir(dbconn, fileID, containersDir); err != nil {
			return fmt.Errorf("file deep verification failed: %w", err)
		}
	default:
		return fmt.Errorf("invalid file verify level: %d", verifyLevel)
	}

	return nil
}
