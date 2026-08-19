package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/franchoy/coldkeep/internal/engine"
	"github.com/franchoy/coldkeep/internal/storage"
	"github.com/franchoy/coldkeep/internal/verify"
)

type verifyOutputSummary struct {
	BlocksChecked           int64
	PhysicalHashChecked     int64
	CompressedHashChecked   int64
	LogicalHashChecked      int64
	CompressedBlocksChecked int64
}

var verifyCommandPhase = func(dbconn *sql.DB, target string, fileID int, level verify.VerifyLevel) error {
	eng, err := engine.New(engine.Config{DB: dbconn})
	if err != nil {
		return err
	}
	_, err = eng.Verify(context.Background(), engine.VerifyRequest{Target: target, FileID: fileID, Level: verifyLevelToString(level)})
	return err
}

var verifySummaryPhase = func(_ *sql.DB, _ string, _ int64) (verifyOutputSummary, error) {
	return verifyOutputSummary{}, nil
}

var productionVerifyCommandEngine = newVerifyCommandEngine

func init() {
	newVerifyCommandEngine = func(sgctx storage.StorageContext) (engine.Engine, error) {
		return legacyVerifyTestEngine{db: sgctx.DB}, nil
	}
}

type legacyVerifyTestEngine struct {
	engine.Engine
	db *sql.DB
}

func (e legacyVerifyTestEngine) Verify(_ context.Context, req engine.VerifyRequest) (engine.VerifyResult, error) {
	level, err := legacyVerifyLevel(req.Level)
	if err != nil {
		return engine.VerifyResult{}, err
	}
	if err := verifyCommandPhase(e.db, req.Target, req.FileID, level); err != nil {
		return engine.VerifyResult{}, err
	}
	summary, err := verifySummaryPhase(e.db, req.Target, int64(req.FileID))
	if err != nil {
		return engine.VerifyResult{}, err
	}
	return engine.VerifyResult{
		BlocksChecked: summary.BlocksChecked, PhysicalHashChecked: summary.PhysicalHashChecked,
		CompressedHashChecked: summary.CompressedHashChecked, LogicalHashChecked: summary.LogicalHashChecked,
		CompressedBlocksChecked: summary.CompressedBlocksChecked,
	}, nil
}

func legacyVerifyLevel(value string) (verify.VerifyLevel, error) {
	switch value {
	case "fast":
		return verify.VerifyFast, nil
	case "", "standard":
		return verify.VerifyStandard, nil
	case "full":
		return verify.VerifyFull, nil
	case "deep":
		return verify.VerifyDeep, nil
	default:
		return 0, fmt.Errorf("unknown verify level %q", value)
	}
}
