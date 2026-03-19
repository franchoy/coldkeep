package maintenance

import (
	"database/sql"
	"fmt"

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

	dbconn, err := db.ConnectDB()
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer dbconn.Close()

	switch target {
	case "system":
		return verifySystem(dbconn, verifyLevel)
	case "file":
		return verifyFile(dbconn, fileId, verifyLevel)
	default:
		return fmt.Errorf("invalid target for verify command: %s", target)
	}
}

func verifySystem(dbconn *sql.DB, verifyLevel verify.VerifyLevel) error {

	switch verifyLevel {
	case verify.VerifyStandard:
		if err := verify.VerifySystemStandard(dbconn); err != nil {
			return fmt.Errorf("system standard verification failed: %w", err)
		}
	case verify.VerifyFull:
		if err := verify.VerifySystemFull(dbconn); err != nil {
			return fmt.Errorf("system full verification failed: %w", err)
		}
	case verify.VerifyDeep:
		if err := verify.VerifySystemDeep(dbconn); err != nil {
			return fmt.Errorf("system deep verification failed: %w", err)
		}
	default:
		return fmt.Errorf("invalid system verify level: %d", verifyLevel)
	}

	return nil
}

func verifyFile(dbconn *sql.DB, fileId int, verifyLevel verify.VerifyLevel) error {

	//verify that the file id exists
	var exists bool
	err := dbconn.QueryRow("SELECT EXISTS(SELECT 1 FROM file WHERE id = ?)", fileId).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check if file exists: %w", err)
	}
	if !exists {
		return fmt.Errorf("file with ID %d does not exist", fileId)
	}

	switch verifyLevel {
	case verify.VerifyStandard:
		if err := verify.VerifyFileStandard(dbconn, fileId); err != nil {
			return fmt.Errorf("file standard verification failed: %w", err)
		}
	case verify.VerifyFull:
		if err := verify.VerifyFileFull(dbconn, fileId); err != nil {
			return fmt.Errorf("file full verification failed: %w", err)
		}
	case verify.VerifyDeep:
		if err := verify.VerifyFileDeep(dbconn, fileId); err != nil {
			return fmt.Errorf("file deep verification failed: %w", err)
		}
	default:
		return fmt.Errorf("invalid file verify level: %d", verifyLevel)
	}

	return nil
}
