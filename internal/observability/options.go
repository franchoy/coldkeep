package observability

import (
	"context"
	"database/sql"
	"time"

	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/storage"
)

type StatsOptions struct {
	IncludeContainers bool
}

type InspectOptions struct {
	Deep      bool
	Relations bool
	Reverse   bool
	Limit     int
}

type SimulationOptions struct {
	Kind string
}

type InspectTarget struct {
	EntityType EntityType
	EntityID   string
}

type SimulationTarget struct {
	Kind     string
	Metadata map[string]any
}

type ServiceOption func(*Service)

func WithNowFunc(fn func() time.Time) ServiceOption {
	return func(s *Service) {
		if fn != nil {
			s.now = fn
		}
	}
}

func WithStatsRunner(fn func() (*maintenance.StatsResult, error)) ServiceOption {
	return func(s *Service) {
		if fn != nil {
			s.statsRunner = fn
		}
	}
}

func WithStorageLoader(fn func() (storage.StorageContext, error)) ServiceOption {
	return func(s *Service) {
		if fn != nil {
			s.storageLoader = fn
		}
	}
}

func WithInspectLogicalFileReader(fn func(dbconn *sql.DB, fileID int64) (storage.LogicalFileInspectInfo, error)) ServiceOption {
	return func(s *Service) {
		if fn != nil {
			s.inspectLogicalFileReader = fn
		}
	}
}

func WithSimulationRunner(fn func(ctx context.Context, target SimulationTarget) (SimulationResult, error)) ServiceOption {
	return func(s *Service) {
		if fn != nil {
			s.simulationRunner = fn
		}
	}
}
