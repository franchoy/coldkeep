package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/utils_env"
	_ "github.com/mattn/go-sqlite3"
)

type StorageContextType string

type StorageContext struct {
	DB           *sql.DB
	Writer       container.ContainerWriter
	ContainerDir string
	TempDBPath   string
}

func (s StorageContext) EffectiveContainerDir() string {
	if strings.TrimSpace(s.ContainerDir) != "" {
		return s.ContainerDir
	}
	return container.ContainersDir
}

// Close releases storage context resources.
// For simulated mode, it also removes the temporary sqlite DB file.
// Close is safe to call multiple times (idempotent).
// Writer finalization is always attempted first; in simulated mode this is a
// logical reset (no physical fsync/close), while local writers close handles.
// Note: some store paths also finalize writers per file operation; this call is
// intentionally tolerant as a defensive final ownership boundary.
func (s *StorageContext) Close() error {
	var errs []error

	if s.Writer != nil {
		if err := s.Writer.FinalizeContainer(); err != nil {
			errs = append(errs, fmt.Errorf("finalize container writer: %w", err))
		}
		s.Writer = nil
	}

	if s.DB != nil {
		if err := s.DB.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close DB: %w", err))
		}
		s.DB = nil
	}

	if s.TempDBPath != "" {
		if err := os.Remove(s.TempDBPath); err != nil {
			if !os.IsNotExist(err) {
				errs = append(errs, fmt.Errorf("remove temp DB file %s: %w", s.TempDBPath, err))
			} else {
				s.TempDBPath = ""
			}
		} else {
			s.TempDBPath = ""
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

func (s *StorageContext) IsSimulated() bool {
	_, ok := s.Writer.(*container.SimulatedWriter)
	return ok
}

const (
	LocalStorage     StorageContextType = "local"
	SimulatedStorage StorageContextType = "simulated"
	NasStorage       StorageContextType = "nas"
	S3Storage        StorageContextType = "s3"
)

func ParseStorageContext(value string) (StorageContext, error) {
	value = strings.TrimSpace(strings.ToLower(value))

	switch StorageContextType(value) {
	case LocalStorage:
		//connect to local DB and initialize local container writer
		dbconn, err := db.ConnectDB()
		if err != nil {
			return StorageContext{}, fmt.Errorf("failed to connect to local DB: %w", err)
		}

		writer := container.NewLocalWriterWithDirAndDB(container.ContainersDir, container.GetContainerMaxSize(), dbconn)
		if writer == nil {
			_ = dbconn.Close()
			return StorageContext{}, fmt.Errorf("writer cannot be nil")
		}
		return StorageContext{
			DB:           dbconn, // metadata store (required)
			Writer:       writer,
			ContainerDir: container.ContainersDir,
		}, nil
	case SimulatedStorage:
		// create isolated sqlite DB file and initialize simulated container writer
		tempDBFile, err := os.CreateTemp("", "coldkeep-sim-*.db")
		if err != nil {
			return StorageContext{}, fmt.Errorf("failed to create simulated DB file: %w", err)
		}
		if err := tempDBFile.Close(); err != nil {
			return StorageContext{}, fmt.Errorf("failed to close simulated DB file: %w", err)
		}

		sqliteDB, err := sql.Open("sqlite3", tempDBFile.Name())
		if err != nil {
			_ = os.Remove(tempDBFile.Name())
			return StorageContext{}, fmt.Errorf("failed to create simulated DB: %w", err)
		}

		if err := db.ApplySQLiteSessionPragmas(sqliteDB); err != nil {
			_ = sqliteDB.Close()
			_ = os.Remove(tempDBFile.Name())
			return StorageContext{}, fmt.Errorf("failed to configure simulated DB: %w", err)
		}

		pingCtx, cancel := db.NewOperationContext(context.Background())
		defer cancel()

		if err := sqliteDB.PingContext(pingCtx); err != nil {
			_ = sqliteDB.Close()
			_ = os.Remove(tempDBFile.Name())
			return StorageContext{}, fmt.Errorf("failed to ping simulated DB: %w", err)
		}

		if err := db.RunMigrations(sqliteDB); err != nil {
			_ = sqliteDB.Close()
			_ = os.Remove(tempDBFile.Name())
			return StorageContext{}, fmt.Errorf("failed to init simulated DB: %w", err)
		}

		writer := container.NewSimulatedWriter(container.GetContainerMaxSize())
		if writer == nil {
			_ = sqliteDB.Close()
			_ = os.Remove(tempDBFile.Name())
			return StorageContext{}, fmt.Errorf("writer cannot be nil")
		}

		return StorageContext{
			DB:           sqliteDB,
			Writer:       writer,
			ContainerDir: container.ContainersDir,
			TempDBPath:   tempDBFile.Name(),
		}, nil
	case NasStorage:
		return StorageContext{}, fmt.Errorf("NAS storage context is not yet implemented")
		// return StorageContext{
		// 	DB:     nil, // NAS storage may not require a DB connection
		// 	Writer: containers.NewNASContainerWriter(),
		// }, nil
	case S3Storage:
		return StorageContext{}, fmt.Errorf("S3 storage context is not yet implemented")
		// return StorageContext{
		// 	DB:     nil, // S3 storage may not require a DB connection
		// 	Writer: containers.NewS3ContainerWriter(),
		// }, nil
	default:
		return StorageContext{}, fmt.Errorf("unsupported storage context: %s", value)
	}
}

// LoadDefaultStorageContext resolves storage context from env with a secure default.
// Precedence: env (COLDKEEP_STORAGE_CONTEXT) -> default (local).
func LoadDefaultStorageContext() (StorageContext, error) {
	value := utils_env.GetenvOrDefault("COLDKEEP_STORAGE_CONTEXT", string(LocalStorage))
	if strings.TrimSpace(value) == "" {
		value = string(LocalStorage)
	}
	return ParseStorageContext(value)
}
