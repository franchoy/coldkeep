package execution

import "fmt"

// Options defines execution policy knobs used by command flows.
type Options struct {
	StoreFolderWorkers int
	PipelineDepth      int
	Deterministic      bool
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
