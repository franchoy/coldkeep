package observability

import (
	"context"
	"database/sql"
	"time"

	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/storage"
)

type Service struct {
	now                      func() time.Time
	statsRunner              func() (*maintenance.StatsResult, error)
	storageLoader            func() (storage.StorageContext, error)
	inspectLogicalFileReader func(*sql.DB, int64) (storage.LogicalFileInspectInfo, error)
	simulationRunner         func(ctx context.Context, target SimulationTarget) (SimulationResult, error)
}

func NewService(opts ...ServiceOption) *Service {
	svc := &Service{
		now:                      time.Now,
		statsRunner:              maintenance.RunStatsResult,
		storageLoader:            storage.LoadDefaultStorageContext,
		inspectLogicalFileReader: storage.GetLogicalFileInspectInfoWithDB,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(svc)
		}
	}
	if svc.simulationRunner == nil {
		svc.simulationRunner = svc.defaultSimulationRunner
	}
	return svc
}

var defaultService = NewService()

func Stats(ctx context.Context) (StatsResult, error) {
	return defaultService.Stats(ctx)
}

func Inspect(ctx context.Context, target InspectTarget) (InspectResult, error) {
	return defaultService.Inspect(ctx, target)
}

func Simulate(ctx context.Context, target SimulationTarget) (SimulationResult, error) {
	return defaultService.Simulate(ctx, target)
}
