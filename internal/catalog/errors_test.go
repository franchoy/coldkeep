package catalog_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/franchoy/coldkeep/internal/catalog"
	"github.com/franchoy/coldkeep/internal/engine"
)

func TestErrNotImplementedRemainsCatalogDeferredSentinel(t *testing.T) {
	if !errors.Is(catalog.ErrNotImplemented, catalog.ErrNotImplemented) {
		t.Fatal("expected catalog.ErrNotImplemented to remain errors.Is compatible with itself")
	}
	if !catalog.IsDeferred(catalog.ErrNotImplemented) {
		t.Fatal("expected catalog.ErrNotImplemented to classify as deferred")
	}
}

func TestIsDeferredRecognizesWrappedErrNotImplemented(t *testing.T) {
	err := fmt.Errorf("wrapped catalog deferred method: %w", catalog.ErrNotImplemented)
	if !errors.Is(err, catalog.ErrNotImplemented) {
		t.Fatalf("expected wrapped error to remain catalog.ErrNotImplemented-compatible, got %v", err)
	}
	if !catalog.IsDeferred(err) {
		t.Fatalf("expected wrapped catalog.ErrNotImplemented to classify as deferred, got %v", err)
	}
}

func TestIsDeferredRejectsUnrelatedErrors(t *testing.T) {
	for _, err := range []error{
		nil,
		errors.New("plain unrelated error"),
		fmt.Errorf("wrapped unrelated: %w", errors.New("other")),
		fmt.Errorf("catalog: list snapshots: query failed"),
	} {
		if catalog.IsDeferred(err) {
			t.Fatalf("expected unrelated error to not classify as deferred: %v", err)
		}
	}
}

func TestCatalogAndEngineErrNotImplementedRemainDistinct(t *testing.T) {
	if catalog.IsDeferred(engine.ErrNotImplemented) {
		t.Fatalf("expected engine.ErrNotImplemented to not classify as catalog deferred")
	}
	if engine.IsUnsupported(catalog.ErrNotImplemented) {
		t.Fatalf("expected catalog.ErrNotImplemented to not classify as engine unsupported")
	}
}
