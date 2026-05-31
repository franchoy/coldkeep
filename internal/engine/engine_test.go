package engine_test

import (
	"context"
	"errors"
	"testing"

	"github.com/franchoy/coldkeep/internal/engine"
)

// Compile-time check: DefaultEngine satisfies the Engine interface.
var _ engine.Engine = (*engine.DefaultEngine)(nil)

func TestDefaultEngineReturnsErrNotImplemented(t *testing.T) {
	e := engine.New(engine.Config{RepositoryRoot: "/tmp/test"})
	ctx := context.Background()

	if _, err := e.Stats(ctx, engine.StatsRequest{}); !errors.Is(err, engine.ErrNotImplemented) {
		t.Errorf("Stats: got %v, want ErrNotImplemented", err)
	}
	if _, err := e.Inspect(ctx, engine.InspectRequest{FileID: 1}); !errors.Is(err, engine.ErrNotImplemented) {
		t.Errorf("Inspect: got %v, want ErrNotImplemented", err)
	}
	if _, err := e.Verify(ctx, engine.VerifyRequest{Level: "standard"}); !errors.Is(err, engine.ErrNotImplemented) {
		t.Errorf("Verify: got %v, want ErrNotImplemented", err)
	}
}
