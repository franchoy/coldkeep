// Package application owns runtime composition of configured database,
// storage, and engine dependencies. It deliberately does not acquire the
// repository coordination lease or choose a different backend default.
package application

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/storage"
)

// Request describes the dependencies needed by one application operation.
type Request struct {
	Operation      string
	ContainerDir   string
	RequireStorage bool
}

// Session owns exactly one configured runtime connection/context and its
// injected engine. Close must be called by the opener.
type Session struct {
	engine  engine.Engine
	db      *sql.DB
	storage *storage.StorageContext
}

// Open constructs one engine using the existing configured backend and
// storage resolution. Backend selection and startup coordination remain with
// their established owners.
func Open(req Request) (*Session, error) {
	if req.RequireStorage {
		storageContext, err := storage.LoadDefaultStorageContext()
		if err != nil {
			return nil, err
		}
		containerDir := storageContext.EffectiveContainerDir()
		if strings.TrimSpace(req.ContainerDir) != "" {
			containerDir = req.ContainerDir
		}
		eng, err := engine.New(engine.Config{
			DB: storageContext.DB, ContainerDir: containerDir,
			StoreContext: &storageContext,
		})
		if err != nil {
			_ = storageContext.Close()
			return nil, err
		}
		return &Session{engine: eng, storage: &storageContext}, nil
	}

	dbconn, err := db.ConnectDB()
	if err != nil {
		return nil, err
	}
	eng, err := engine.New(engine.Config{DB: dbconn, ContainerDir: req.ContainerDir})
	if err != nil {
		_ = dbconn.Close()
		return nil, err
	}
	return &Session{engine: eng, db: dbconn}, nil
}

// Engine returns the injected headless engine.
func (s *Session) Engine() engine.Engine {
	if s == nil {
		return nil
	}
	return s.engine
}

// OperationContext applies the established repository-operation timeout
// without exposing database helpers to command code.
func (s *Session) OperationContext(parent context.Context) (context.Context, context.CancelFunc) {
	return db.NewOperationContext(parent)
}

// Close releases the session-owned resources.
func (s *Session) Close() error {
	if s == nil {
		return nil
	}
	var closeErr error
	if s.storage != nil {
		closeErr = s.storage.Close()
		s.storage = nil
	}
	if s.db != nil {
		closeErr = errors.Join(closeErr, s.db.Close())
		s.db = nil
	}
	s.engine = nil
	return closeErr
}
