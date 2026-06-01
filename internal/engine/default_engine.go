package engine

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/maintenance"
	"github.com/franchoy/coldkeep/internal/observability"
	"github.com/franchoy/coldkeep/internal/verify"
)

// Config holds configuration for a DefaultEngine.
//
// Database backend selection (SQLite vs PostgreSQL) is not decided here;
// the caller is responsible for opening the correct backend and providing
// the connection. Config fields will expand as wrapper-only implementations
// require additional dependencies.
type Config struct {
	// DB is the active database connection.
	// The caller is responsible for the connection lifetime.
	DB *sql.DB
	// ContainerDir is the path to the coldkeep containers directory.
	// Defaults to container.ContainersDir if empty.
	ContainerDir string
}

// DefaultEngine is the canonical Engine implementation.
//
// Phase 2: wrapper-only. All methods delegate to existing domain packages.
// No business logic is moved; the engine is a thin delegation layer.
type DefaultEngine struct {
	config Config
	obs    *observability.Service
}

// New returns a new DefaultEngine with the given configuration.
// Returns an error if DB is nil or if the observability service cannot be
// initialized.
func New(cfg Config) (*DefaultEngine, error) {
	if cfg.DB == nil {
		return nil, fmt.Errorf("engine: Config.DB is required")
	}
	obs, err := observability.NewService(cfg.DB)
	if err != nil {
		return nil, fmt.Errorf("engine: observability service: %w", err)
	}
	return &DefaultEngine{config: cfg, obs: obs}, nil
}

func (e *DefaultEngine) Stats(ctx context.Context, req StatsRequest) (StatsResult, error) {
	r, err := e.obs.Stats(ctx, observability.StatsOptions{
		IncludeContainers: req.IncludeContainers,
		Trace:             req.Trace,
	})
	if err != nil {
		return StatsResult{}, err
	}
	return StatsResult{Raw: r}, nil
}

func (e *DefaultEngine) Inspect(ctx context.Context, req InspectRequest) (InspectResult, error) {
	if err := validateInspectRequest(req); err != nil {
		return InspectResult{}, err
	}
	r, err := e.obs.Inspect(ctx, req.Entity, req.EntityID, req.Options)
	if err != nil {
		return InspectResult{}, err
	}
	return InspectResult{Raw: r}, nil
}

func (e *DefaultEngine) Verify(ctx context.Context, req VerifyRequest) (VerifyResult, error) {
	level, err := verifyLevelFromString(req.Level)
	if err != nil {
		return VerifyResult{}, err
	}
	target := req.Target
	if target == "" {
		target = "system"
	}
	if err := validateVerifyRequest(target, req.FileID); err != nil {
		return VerifyResult{}, err
	}
	containerDir := e.config.ContainerDir
	if containerDir == "" {
		containerDir = container.ContainersDir
	}
	if err := maintenance.VerifyCommandWithDBAndContainersDir(e.config.DB, containerDir, target, req.FileID, level); err != nil {
		return VerifyResult{}, err
	}
	return VerifyResult{}, nil
}

// validateInspectRequest returns an error if req contains an unrecognized entity
// type or an invalid/missing entity ID for that type. This duplicates the CLI
// validation so correctness does not depend solely on the CLI parsing path.
func validateInspectRequest(req InspectRequest) error {
	switch req.Entity {
	case observability.EntityRepository:
		// EntityRepository is the only entity that requires no ID.
		return nil
	case observability.EntitySnapshot:
		if strings.TrimSpace(req.EntityID) == "" {
			return fmt.Errorf("engine: entity ID is required for %s", req.Entity)
		}
		return nil
	case observability.EntityFile, observability.EntityLogicalFile, observability.EntityPhysicalFile,
		observability.EntityChunk, observability.EntityContainer:
		id := strings.TrimSpace(req.EntityID)
		if id == "" {
			return fmt.Errorf("engine: entity ID is required for %s", req.Entity)
		}
		n, err := strconv.ParseInt(id, 10, 64)
		if err != nil || n <= 0 {
			return fmt.Errorf("engine: %s ID must be a positive integer, got %q", req.Entity, req.EntityID)
		}
		return nil
	default:
		return fmt.Errorf("engine: unknown inspect entity %q", req.Entity)
	}
}

// validateVerifyRequest returns an error if target is not a recognized verify
// target, or if the file ID is non-positive when target is "file".
func validateVerifyRequest(target string, fileID int) error {
	switch target {
	case "system":
		return nil
	case "file":
		if fileID <= 0 {
			return fmt.Errorf("engine: file ID must be positive for verify file, got %d", fileID)
		}
		return nil
	default:
		return fmt.Errorf("engine: unknown verify target %q: must be system or file", target)
	}
}

// verifyLevelFromString maps the Level string from VerifyRequest to the
// internal verify.VerifyLevel type.
func verifyLevelFromString(s string) (verify.VerifyLevel, error) {
	switch s {
	case "fast":
		return verify.VerifyFast, nil
	case "", "standard":
		return verify.VerifyStandard, nil
	case "full":
		return verify.VerifyFull, nil
	case "deep":
		return verify.VerifyDeep, nil
	default:
		return 0, fmt.Errorf("unknown verify level %q: must be fast, standard, full, or deep", s)
	}
}
