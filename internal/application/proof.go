package application

import (
	"context"
	"database/sql"
	"errors"

	"github.com/franchoy/coldkeep/v2/internal/engine"
	"github.com/franchoy/coldkeep/v2/internal/storage"
)

// ProofOperations composes the three engine operations used by the fixture
// daemon. Database and storage handles are borrowed: the caller retains their
// lifetime, repository lease, and operation serialization. There is no Close
// method and no default-target discovery or additional connection pool.
type ProofOperations struct {
	engine proofEngine
}

type proofEngine interface {
	Verify(context.Context, engine.VerifyRequest) (engine.VerifyResult, error)
	SnapshotShow(context.Context, engine.SnapshotShowRequest) (engine.SnapshotShowResult, error)
	SnapshotRestore(context.Context, engine.SnapshotRestoreRequest) (engine.SnapshotRestoreResult, error)
}

// NewProofOperations constructs the engine over the exact supplied handles.
// Engine construction retains the ordinary catalog admission checks.
func NewProofOperations(database *sql.DB, store *storage.StorageContext, containerDir string) (*ProofOperations, error) {
	if database == nil || store == nil || store.DB != database || containerDir == "" {
		return nil, errors.New("proof operations require explicit matching database and storage handles")
	}
	eng, err := engine.New(engine.Config{DB: database, StoreContext: store, ContainerDir: containerDir})
	if err != nil {
		return nil, err
	}
	return &ProofOperations{engine: eng}, nil
}

// Verify performs the fixture's existing deep system verification.
func (p *ProofOperations) Verify(ctx context.Context) error {
	_, err := p.engine.Verify(ctx, engine.VerifyRequest{Level: "deep", Target: "system"})
	return err
}

// ProofSnapshotRequest selects exact snapshot bytes and a positive row limit.
type ProofSnapshotRequest struct {
	SnapshotID string
	Limit      int
}

// ProofSnapshot contains only the bounded history fields consumed by the daemon.
type ProofSnapshot struct {
	Label string
	Files []ProofSnapshotFile
}

type ProofSnapshotFile struct {
	StoredPath string
	Size       *int64
}

func (p *ProofOperations) Snapshot(ctx context.Context, req ProofSnapshotRequest) (ProofSnapshot, error) {
	if req.Limit <= 0 {
		return ProofSnapshot{}, errors.New("proof snapshot requires a positive limit")
	}
	history, err := p.engine.SnapshotShow(ctx, engine.SnapshotShowRequest{
		SnapshotID: req.SnapshotID, Query: engine.SnapshotQuery{Limit: req.Limit},
	})
	if err != nil {
		return ProofSnapshot{}, err
	}
	result := ProofSnapshot{Label: history.Snapshot.Label, Files: make([]ProofSnapshotFile, len(history.Files))}
	for i, file := range history.Files {
		result.Files[i] = ProofSnapshotFile{StoredPath: file.StoredPath, Size: file.Size}
	}
	return result, nil
}

// ProofRestoreRequest selects one exact historical path and the caller-owned
// destination. Metadata policy and destination mode are fixed by this facade.
type ProofRestoreRequest struct {
	SnapshotID      string
	StoredPath      string
	DestinationRoot string
}

type ProofRestoreResult struct {
	RestoredFiles int64
	WarningCount  int
}

func (p *ProofOperations) Restore(ctx context.Context, req ProofRestoreRequest) (ProofRestoreResult, error) {
	result, err := p.engine.SnapshotRestore(ctx, engine.SnapshotRestoreRequest{
		SnapshotID: req.SnapshotID, Paths: []string{req.StoredPath},
		Destination: engine.SnapshotRestoreDestination{Mode: engine.SnapshotRestoreDestinationPrefix, Path: req.DestinationRoot},
		Metadata:    engine.SnapshotRestoreMetadataStrict,
	})
	return ProofRestoreResult{RestoredFiles: result.RestoredFiles, WarningCount: len(result.Warnings)}, err
}
