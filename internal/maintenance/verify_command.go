package maintenance

import (
	"database/sql"
	"fmt"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/verify"
)

//verify system --standard
//verify system --full
//verify system --deep

//verify file <id> --standard
//verify file <id> --full
//verify file <id> --deep

func VerifyCommand(target string, fileId int, verifyLevel verify.VerifyLevel) error {
	return VerifyCommandWithContainersDir(container.ContainersDir, target, fileId, verifyLevel)
}

func VerifyCommandWithContainersDir(containersDir string, target string, fileId int, verifyLevel verify.VerifyLevel) error {

	dbconn, err := db.ConnectDB()
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer func() { _ = dbconn.Close() }()

	switch target {
	case "system":
		return verifySystem(dbconn, containersDir, verifyLevel)
	case "file":
		return verifyFile(dbconn, containersDir, fileId, verifyLevel)
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

func verifyFile(dbconn *sql.DB, containersDir string, fileId int, verifyLevel verify.VerifyLevel) error {

	//verify that the file id exists
	var exists bool
	err := dbconn.QueryRow("SELECT EXISTS(SELECT 1 FROM logical_file WHERE id = $1)", fileId).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check if file exists: %w", err)
	}
	if !exists {
		return fmt.Errorf("file with ID %d does not exist", fileId)
	}

	switch verifyLevel {
	case verify.VerifyStandard:
		if err := verify.VerifyFileStandardWithContainersDir(dbconn, fileId, containersDir); err != nil {
			return fmt.Errorf("file standard verification failed: %w", err)
		}
	case verify.VerifyFull:
		if err := verify.VerifyFileFullWithContainersDir(dbconn, fileId, containersDir); err != nil {
			return fmt.Errorf("file full verification failed: %w", err)
		}
	case verify.VerifyDeep:
		if err := verify.VerifyFileDeepWithContainersDir(dbconn, fileId, containersDir); err != nil {
			return fmt.Errorf("file deep verification failed: %w", err)
		}
	default:
		return fmt.Errorf("invalid file verify level: %d", verifyLevel)
	}

	return nil
}
