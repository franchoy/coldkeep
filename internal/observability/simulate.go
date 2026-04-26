package observability

import (
	"context"
	"fmt"
)

const SimulationKindGC = "gc"

func (s *Service) Simulate(ctx context.Context, opts SimulationOptions) (*SimulationResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	switch opts.Kind {
	case SimulationKindGC:
		return nil, fmt.Errorf("gc simulation not implemented yet")
	default:
		return nil, fmt.Errorf("%w: unsupported simulation kind %q", ErrInvalidTarget, opts.Kind)
	}
}
