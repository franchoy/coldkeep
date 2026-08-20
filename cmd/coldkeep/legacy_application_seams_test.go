package main

// This file contains the pre-v1.13.12 construction seams used by the frozen
// CLI compatibility suite. Production command code cannot see these symbols:
// normal builds compose dependencies only through internal/application.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/franchoy/coldkeep/internal/application"
	"github.com/franchoy/coldkeep/internal/batch"
	"github.com/franchoy/coldkeep/internal/db"
	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/snapshot"
	"github.com/franchoy/coldkeep/internal/storage"
)

var loadDefaultStorageContextPhase = storage.LoadDefaultStorageContext
var createSnapshotPhase = snapshot.CreateSnapshotWithOptions
var restoreSnapshotPhase = snapshot.RestoreSnapshot
var deleteSnapshotPhase = snapshot.DeleteSnapshot
var snapshotDeleteLineagePreviewPhase = snapshot.LoadDeleteLineagePreview

func loadSnapshotDeleteLineagePreview(ctx context.Context, dbconn *sql.DB, snapshotID string) (*snapshotDeleteLineagePreview, error) {
	return snapshot.LoadDeleteLineagePreview(ctx, dbconn, snapshotID)
}

var connectListSearchDBPhase = db.ConnectDB
var connectRepairDBPhase = db.ConnectDB
var connectRecoveryDBPhase = db.ConnectDB
var connectDoctorDBPhase = db.ConnectDB

var newCommandEngine = func(dbconn *sql.DB, containerDir string) (engine.Engine, error) {
	return engine.New(engine.Config{DB: dbconn, ContainerDir: containerDir})
}
var newSnapshotRestoreCommandEngine = func(sgctx storage.StorageContext) (engine.Engine, error) {
	return engine.New(engine.Config{DB: sgctx.DB, ContainerDir: sgctx.EffectiveContainerDir(), StoreContext: &sgctx})
}
var newStoreFolderCommandEngine = func(sgctx storage.StorageContext) (engine.Engine, error) {
	return engine.New(engine.Config{DB: sgctx.DB, ContainerDir: sgctx.EffectiveContainerDir(), StoreContext: &sgctx})
}
var newConfigurationCommandEngine = func(sgctx storage.StorageContext) (engine.Engine, error) {
	return engine.New(engine.Config{DB: sgctx.DB, ContainerDir: sgctx.EffectiveContainerDir()})
}
var newSnapshotReadCommandEngine = func(sgctx storage.StorageContext) (engine.Engine, error) {
	return engine.New(engine.Config{DB: sgctx.DB, ContainerDir: sgctx.EffectiveContainerDir()})
}
var newVerifyCommandEngine = func(sgctx storage.StorageContext) (engine.Engine, error) {
	return engine.New(engine.Config{DB: sgctx.DB, ContainerDir: sgctx.EffectiveContainerDir()})
}
var newObservabilityCommandEngine = func(sgctx storage.StorageContext) (engine.Engine, error) {
	return engine.New(engine.Config{DB: sgctx.DB, ContainerDir: sgctx.EffectiveContainerDir()})
}
var newRepairCommandEngine = func(dbconn *sql.DB) (engine.Engine, error) {
	return engine.New(engine.Config{DB: dbconn})
}
var newRecoveryCommandEngine = func(dbconn *sql.DB, containersDir string) (engine.Engine, error) {
	return engine.New(engine.Config{DB: dbconn, ContainerDir: containersDir})
}
var newDoctorCommandEngine = func(dbconn *sql.DB, containersDir string) (engine.Engine, error) {
	return engine.New(engine.Config{DB: dbconn, ContainerDir: containersDir})
}

type legacyApplicationSession struct {
	engine engine.Engine
	close  func() error
}

func (s *legacyApplicationSession) Engine() engine.Engine { return s.engine }
func (s *legacyApplicationSession) OperationContext(parent context.Context) (context.Context, context.CancelFunc) {
	return db.NewOperationContext(parent)
}
func (s *legacyApplicationSession) Close() error {
	if s == nil || s.close == nil {
		return nil
	}
	close := s.close
	s.close = nil
	return close()
}

func init() {
	openApplicationSessionPhase = openLegacyApplicationSession
}

func openLegacyApplicationSession(req application.Request) (commandSession, error) {
	if !req.RequireStorage {
		return openLegacyDBSession(req)
	}
	sgctx, err := loadDefaultStorageContextPhase()
	if err != nil {
		return nil, err
	}
	var eng engine.Engine
	switch req.Operation {
	case "config":
		eng, err = newConfigurationCommandEngine(sgctx)
	case "store":
		if reflect.ValueOf(storeByFilePhase).Pointer() != reflect.ValueOf(productionStoreByFilePhase).Pointer() {
			eng = legacyPerItemEngine{Engine: nil, storage: &sgctx}
		} else {
			eng, err = newStoreFolderCommandEngine(sgctx)
		}
	case "store-folder":
		eng, err = newStoreFolderCommandEngine(sgctx)
	case "snapshot-list", "snapshot-show", "snapshot-stats", "snapshot-diff":
		eng, err = newSnapshotReadCommandEngine(sgctx)
	case "snapshot-restore":
		eng, err = newSnapshotRestoreCommandEngine(sgctx)
	case "verify":
		eng, err = newVerifyCommandEngine(sgctx)
	case "stats", "inspect", "simulate-gc":
		eng, err = newObservabilityCommandEngine(sgctx)
	default:
		eng, err = newCommandEngine(sgctx.DB, effectiveLegacyContainerDir(sgctx, req.ContainerDir))
		if err == nil && ((req.Operation == "restore" && reflect.ValueOf(restoreByIDPhase).Pointer() != reflect.ValueOf(productionRestoreByIDPhase).Pointer()) ||
			(req.Operation == "remove" && reflect.ValueOf(removeByIDPhase).Pointer() != reflect.ValueOf(productionRemoveByIDPhase).Pointer())) {
			eng = legacyPerItemEngine{Engine: eng, storage: &sgctx}
		}
	}
	if err != nil {
		_ = sgctx.Close()
		return nil, err
	}
	return &legacyApplicationSession{engine: eng, close: sgctx.Close}, nil
}

func openLegacyDBSession(req application.Request) (commandSession, error) {
	connector := connectListSearchDBPhase
	switch req.Operation {
	case "repair":
		connector = connectRepairDBPhase
	case "recovery":
		connector = connectRecoveryDBPhase
	case "doctor":
		connector = connectDoctorDBPhase
	}
	dbconn, err := connector()
	if err != nil {
		return nil, err
	}
	var eng engine.Engine
	switch req.Operation {
	case "repair":
		eng, err = newRepairCommandEngine(dbconn)
	case "recovery":
		eng, err = newRecoveryCommandEngine(dbconn, req.ContainerDir)
	case "doctor":
		eng, err = newDoctorCommandEngine(dbconn, req.ContainerDir)
	default:
		eng, err = newCommandEngine(dbconn, req.ContainerDir)
	}
	if err != nil {
		_ = dbconn.Close()
		return nil, err
	}
	return &legacyApplicationSession{engine: eng, close: dbconn.Close}, nil
}

func effectiveLegacyContainerDir(sgctx storage.StorageContext, override string) string {
	if strings.TrimSpace(override) != "" {
		return override
	}
	return sgctx.EffectiveContainerDir()
}

var storeByFilePhase = func(sgctx *storage.StorageContext, path, codecName string) (storage.StoreFileResult, error) {
	if sgctx == nil || sgctx.DB == nil {
		return storage.StoreFileResult{}, fmt.Errorf("store: storage context DB is required")
	}
	eng, err := engine.New(engine.Config{DB: sgctx.DB, ContainerDir: sgctx.EffectiveContainerDir(), StoreContext: sgctx})
	if err != nil {
		return storage.StoreFileResult{}, err
	}
	res, err := eng.Store(context.Background(), engine.StoreRequest{SourcePath: path, Codec: strings.TrimSpace(codecName)})
	if err != nil {
		return storage.StoreFileResult{}, err
	}
	return storage.StoreFileResult{FileID: res.LogicalFileID, FileHash: res.FileHash, Path: res.StoredPath, AlreadyStored: res.AlreadyStored}, nil
}

var removeByIDPhase = func(sgctx *storage.StorageContext, fileID int64, dryRun bool) batch.ItemResult {
	if sgctx == nil || sgctx.DB == nil {
		return batch.ItemResult{ID: fileID, Status: batch.ResultFailed, Message: "remove: storage context DB is required"}
	}
	eng, err := engine.New(engine.Config{DB: sgctx.DB, ContainerDir: sgctx.EffectiveContainerDir()})
	if err != nil {
		return batch.ItemResult{ID: fileID, Status: batch.ResultFailed, Message: err.Error()}
	}
	res, err := eng.Remove(context.Background(), engine.RemoveRequest{FileIDs: []int64{fileID}, DryRun: dryRun, FailFast: true})
	if err != nil || len(res.Items) != 1 {
		if err == nil {
			err = fmt.Errorf("remove: expected one item result, got %d", len(res.Items))
		}
		return batch.ItemResult{ID: fileID, Status: batch.ResultFailed, Message: err.Error()}
	}
	item := res.Items[0]
	if item.Status == engine.BatchItemFailed {
		return batch.ItemResult{ID: fileID, Status: batch.ResultFailed, Message: item.Error, InvariantCode: item.InvariantCode, RecommendedAction: item.RecommendedAction}
	}
	if dryRun {
		return batch.ItemResult{ID: fileID, Status: batch.ResultPlanned, Message: "would remove"}
	}
	return batch.ItemResult{ID: fileID, Status: batch.ResultSuccess, Message: fmt.Sprintf("removed mappings=%d", item.RemovedChunkAssociations)}
}

var restoreByIDPhase = func(sgctx *storage.StorageContext, fileID int64, outputDir string, overwrite bool, dryRun bool) (storage.RestoreFileResult, error) {
	if sgctx == nil || sgctx.DB == nil {
		return storage.RestoreFileResult{}, fmt.Errorf("restore: storage context DB is required")
	}
	eng, err := engine.New(engine.Config{DB: sgctx.DB, ContainerDir: sgctx.EffectiveContainerDir()})
	if err != nil {
		return storage.RestoreFileResult{}, err
	}
	res, err := eng.Restore(context.Background(), engine.RestoreRequest{FileIDs: []int64{fileID}, DestinationRoot: outputDir, Overwrite: overwrite, DryRun: dryRun, FailFast: true})
	if err != nil || len(res.Items) != 1 {
		if err == nil {
			err = fmt.Errorf("restore: expected one item result, got %d", len(res.Items))
		}
		return storage.RestoreFileResult{}, err
	}
	item := res.Items[0]
	if item.Status == engine.BatchItemFailed {
		return storage.RestoreFileResult{}, errors.New(item.Error)
	}
	return storage.RestoreFileResult{FileID: fileID, OriginalName: item.OriginalName, OutputPath: item.DestinationPath, RestoredHash: item.RestoredHash}, nil
}

var productionStoreByFilePhase = storeByFilePhase
var productionRemoveByIDPhase = removeByIDPhase
var productionRestoreByIDPhase = restoreByIDPhase

type legacyPerItemEngine struct {
	engine.Engine
	storage *storage.StorageContext
}

func (e legacyPerItemEngine) Store(_ context.Context, req engine.StoreRequest) (engine.StoreResult, error) {
	result, err := storeByFilePhase(e.storage, req.SourcePath, req.Codec)
	return engine.StoreResult{
		LogicalFileID: result.FileID, FileHash: result.FileHash,
		StoredPath: result.Path, AlreadyStored: result.AlreadyStored,
	}, err
}

func (e legacyPerItemEngine) Remove(_ context.Context, req engine.RemoveRequest) (engine.RemoveResult, error) {
	result := engine.RemoveResult{DryRun: req.DryRun}
	for _, fileID := range req.FileIDs {
		legacy := removeByIDPhase(e.storage, fileID, req.DryRun)
		item := engine.RemoveItemResult{FileID: fileID}
		switch legacy.Status {
		case batch.ResultSuccess:
			item.Status = engine.BatchItemOK
			_, _ = fmt.Sscanf(legacy.Message, "removed mappings=%d", &item.RemovedChunkAssociations)
		case batch.ResultPlanned:
			item.Status = engine.BatchItemOK
		default:
			item.Status, item.Error = engine.BatchItemFailed, legacy.Message
			item.InvariantCode, item.RecommendedAction = legacy.InvariantCode, legacy.RecommendedAction
		}
		result.Items = append(result.Items, item)
		if item.Status == engine.BatchItemFailed {
			result.Summary.Failed++
			if req.FailFast {
				break
			}
		} else {
			result.Summary.OK++
		}
	}
	return result, nil
}

func (e legacyPerItemEngine) Restore(_ context.Context, req engine.RestoreRequest) (engine.RestoreResult, error) {
	result := engine.RestoreResult{DryRun: req.DryRun}
	for _, fileID := range req.FileIDs {
		legacy, err := restoreByIDPhase(e.storage, fileID, req.DestinationRoot, req.Overwrite, req.DryRun)
		item := engine.RestoreItemResult{
			FileID: fileID, OriginalName: legacy.OriginalName,
			DestinationPath: legacy.OutputPath, RestoredHash: legacy.RestoredHash,
			Status: engine.BatchItemOK,
		}
		if err != nil {
			item.Status, item.Error = engine.BatchItemFailed, err.Error()
		}
		result.Items = append(result.Items, item)
		if item.Status == engine.BatchItemFailed {
			result.Summary.Failed++
			if req.FailFast {
				break
			}
		} else {
			result.Summary.OK++
		}
	}
	return result, nil
}
