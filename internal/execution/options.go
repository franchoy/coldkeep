package execution

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

const envStoreFolderWorkers = "COLDKEEP_STORE_FOLDER_WORKERS"

// Options defines execution policy knobs used by command flows.
type Options struct {
	StoreFolderWorkers int
	PipelineDepth      int
	Deterministic      bool
}

// ExecutionStats captures aggregated execution diagnostics for one run.
// This shape is intentionally aggregate-only so it remains stable for
// benchmarks and CI output.
type ExecutionStats struct {
	TotalFilesProcessed int
	TotalBytesProcessed int64
	WorkersUsed         int
}

// DefaultOptions returns v1.6-equivalent execution behavior.
func DefaultOptions() Options {
	return Options{
		StoreFolderWorkers: 1,
		PipelineDepth:      1,
		Deterministic:      true,
	}
}

// Normalize clamps invalid or unset numeric values to safe defaults.
func (o Options) Normalize() Options {
	if o.StoreFolderWorkers <= 0 {
		o.StoreFolderWorkers = 1
	}
	if o.PipelineDepth <= 0 {
		o.PipelineDepth = 1
	}
	return o
}

// Validate ensures options are explicitly valid.
func (o Options) Validate() error {
	if o.StoreFolderWorkers < 1 {
		return fmt.Errorf("store folder workers must be >= 1")
	}
	if o.PipelineDepth < 1 {
		return fmt.Errorf("pipeline depth must be >= 1")
	}
	return nil
}

// FromEnv applies supported environment overrides to base options.
// Invalid values are rejected to avoid silently enabling risky behavior.
func FromEnv(base Options) (Options, error) {
	opts := base.Normalize()

	raw := strings.TrimSpace(os.Getenv(envStoreFolderWorkers))
	if raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			return Options{}, fmt.Errorf("invalid %s value %q: %w", envStoreFolderWorkers, raw, err)
		}
		if n < 1 {
			return Options{}, fmt.Errorf("invalid %s value %q: must be >= 1", envStoreFolderWorkers, raw)
		}
		opts.StoreFolderWorkers = n
	}

	if err := opts.Validate(); err != nil {
		return Options{}, err
	}
	return opts, nil
}
